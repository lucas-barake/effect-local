import * as PeerRelayClientRuntime from "@lucas-barake/effect-local-sql/PeerRelayClientRuntime"
import type * as PeerRelayOutbox from "@lucas-barake/effect-local-sql/PeerRelayOutbox"
import * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Sink from "effect/Sink"
import * as Stream from "effect/Stream"
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError"
import * as PeerRpcObservability from "./internal/peerRpcObservability.js"
import * as PeerRpc from "./PeerRpc.js"
import type * as PeerRpcError from "./PeerRpcError.js"

const unavailable = () =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.StorageUnavailable({ cause: new Error("RPC peer connection unavailable") })
  })

const protocolFailure = (observed: string) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.ProtocolMismatch({
      expected: "valid RPC peer exchange",
      observed
    })
  })

const validateDocuments = (
  documents: ReadonlyArray<PeerSession.SelectedDocument>,
  definition: ReplicaDefinition.Any
) =>
  Effect.suspend(() => {
    for (const entry of documents) {
      if (definition.documents.byName.get(entry.document.name) !== entry.document) {
        return Effect.fail(protocolFailure(entry.document.name))
      }
    }
    return Effect.void
  })

const mapError = (error: PeerRpcError.PeerRpcError | RpcClientError) => {
  if (error._tag === "RpcClientError") return unavailable()
  switch (error._tag) {
    case "RequestCapacityExceeded":
    case "SessionUnavailable":
    case "SessionOverloaded":
    case "ServerUnavailable":
      return unavailable()
    case "AuthenticationFailure":
    case "AccessDenied":
    case "UnsupportedVersion":
    case "PeerMismatch":
    case "DefinitionMismatch":
    case "InvalidRequest":
    case "RequestLimitExceeded":
    // The wire error is fieldless, so the local side knows only that the remote lineage no longer
    // matches, never which document or which lineage. The tag is carried in `observed` rather than
    // rebuilt into a `DocumentLineageChanged` reason, whose `documentId` and lineages would have to
    // be invented here. Like every other permanent rejection this is not retryable.
    case "DocumentLineageChanged":
      return protocolFailure(error._tag)
  }
}

export const isRetryable = (error: ReplicaError.ReplicaError) => error.reason._tag === "StorageUnavailable"

const adapterResult = (exit: Exit.Exit<unknown, ReplicaError.ReplicaError>) => {
  if (Exit.isSuccess(exit)) return "Success" as const
  const error = PeerRpcObservability.failure(exit)
  return error !== undefined && (
      error.reason._tag === "ProtocolMismatch" ||
      error.reason._tag === "QuotaExceeded" ||
      // Peer caused: a rewritten lineage is a rejection of the exchange, not an adapter fault.
      error.reason._tag === "DocumentLineageChanged"
    )
    ? "ProtocolRejected" as const
    : "Failure" as const
}

const adapterAcknowledgeResult = (
  success: "Acknowledged" | "DeadLettered"
) =>
(exit: Exit.Exit<unknown, ReplicaError.ReplicaError>) => {
  if (Exit.isSuccess(exit)) return success
  const error = PeerRpcObservability.failure(exit)
  if (error === undefined) return "Failure" as const
  switch (error.reason._tag) {
    case "ProtocolMismatch":
    case "DocumentLineageChanged":
      return "ProtocolRejected" as const
    case "QuotaExceeded":
      return "CapacityRejected" as const
    case "StorageUnavailable":
      return "Unavailable" as const
    default:
      return "Failure" as const
  }
}

export interface Options {
  readonly expectedLocal: PeerSyncEnvelope.RelayPeerPrincipal
  readonly senderReplicaIncarnation: Identity.ReplicaIncarnation
  readonly expectedRelayPeerId: Identity.PeerId
  readonly remote: {
    readonly subjectId: string
    readonly peerId: Identity.PeerId
  }
  readonly documents: ReadonlyArray<PeerSession.SelectedDocument>
  readonly definition: ReplicaDefinition.Any
  readonly receiptRetentionMillis: number
  readonly senderRetryHorizonMillis: number
  readonly replayBatchSize: number
}

const samePrincipal = (
  left: PeerSyncEnvelope.RelayPeerPrincipal,
  right: PeerSyncEnvelope.RelayPeerPrincipal
) =>
  left.tenantId === right.tenantId &&
  left.subjectId === right.subjectId &&
  left.peerId === right.peerId

const validateRelayOptions = (options: Options) =>
  Effect.suspend(() => {
    for (
      const [name, value] of [
        ["receipt retention", options.receiptRetentionMillis],
        ["sender retry horizon", options.senderRetryHorizonMillis]
      ] as const
    ) {
      if (
        !Number.isSafeInteger(value) ||
        value <= 0 ||
        value > PeerRpc.maximumNegotiatedDurationMillis
      ) {
        return Effect.fail(protocolFailure(`valid ${name}`))
      }
    }
    if (!Number.isSafeInteger(options.replayBatchSize) || options.replayBatchSize <= 0) {
      return Effect.fail(protocolFailure("valid replay batch size"))
    }
    return Effect.void
  })

const validateStoredMessage = (
  event: PeerRpc.StoredMessage,
  options: Options,
  crypto: Crypto.Crypto,
  limits: ReplicaLimits.Values
) =>
  Effect.gen(function*() {
    const expectedRecipient: PeerSyncEnvelope.RelayPeerPrincipal = {
      ...options.expectedLocal
    }
    const expectedSender: PeerSyncEnvelope.RelayPeerPrincipal = {
      tenantId: options.expectedLocal.tenantId,
      subjectId: options.remote.subjectId,
      peerId: options.remote.peerId
    }
    if (
      event.relayPeerId !== options.expectedRelayPeerId ||
      !samePrincipal(event.sender, expectedSender) ||
      !samePrincipal(event.recipient, expectedRecipient)
    ) {
      return yield* protocolFailure("relay delivery endpoint")
    }
    const selected = options.documents.some((entry) =>
      entry.document.name === event.document.documentType &&
      entry.documentId === event.document.documentId
    )
    if (!selected) return yield* protocolFailure("selected relay document")
    const decoded = yield* PeerSyncEnvelope.decodeSyncEnvelope(
      event.payload,
      limits
    ).pipe(Effect.provideService(Crypto.Crypto, crypto))
    const digest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope({
      domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
      version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
      expectedLocal: expectedSender,
      remote: expectedRecipient,
      relayPeerId: event.relayPeerId,
      relayMessageId: event.relayMessageId,
      protocolVersion: PeerRpc.protocolVersion,
      payloadVersion: event.payloadVersion,
      senderReplicaIncarnation: event.sender.replicaIncarnation,
      senderConnectionEpoch: event.sender.connectionEpoch,
      senderSequence: event.sender.sequence,
      document: event.document,
      lineage: decoded.lineage,
      writerProvenance: event.writerProvenance,
      messageHash: event.messageHash,
      payload: event.payload
    }).pipe(Effect.provideService(Crypto.Crypto, crypto))
    if (digest !== event.outerEnvelopeDigest) {
      return yield* protocolFailure("relay outer envelope digest")
    }
  })

export const layer = (
  client: PeerRpc.RpcClient,
  options: Options
) =>
  Layer.effect(
    PeerTransport.PeerTransport,
    Effect.gen(function*() {
      const runtime = yield* PeerRelayClientRuntime.PeerRelayClientRuntime
      const crypto = yield* Crypto.Crypto
      const limits = yield* ReplicaLimits.ReplicaLimits
      const endpoint = {
        expectedLocal: options.expectedLocal,
        remote: {
          tenantId: options.expectedLocal.tenantId,
          subjectId: options.remote.subjectId,
          peerId: options.remote.peerId
        },
        relayPeerId: options.expectedRelayPeerId
      } as const

      return {
        capabilities: { lineageAware: true },
        connect: (connectOptions) =>
          PeerRpcObservability.observe({
            effect: Effect.gen(function*() {
              yield* validateDocuments(options.documents, options.definition)
              yield* validateRelayOptions(options)
              if (connectOptions.peerId !== options.remote.peerId) {
                return yield* protocolFailure("configured remote peer")
              }
              yield* runtime.health
              yield* runtime.validateConnectionConfiguration({
                replicaIncarnation: options.senderReplicaIncarnation,
                retryHorizonMillis: options.senderRetryHorizonMillis,
                replayBatchSize: options.replayBatchSize
              })
              const pendingHorizon = yield* runtime.maximumPendingHorizon(endpoint)
              if (
                pendingHorizon !== null &&
                options.senderRetryHorizonMillis < pendingHorizon
              ) {
                return yield* protocolFailure("sender retry horizon covering pending relay outbox")
              }
              const advertisedRetryHorizon = Math.max(
                options.senderRetryHorizonMillis,
                pendingHorizon ?? 0
              )
              const parentScope = yield* Scope.Scope
              return yield* Effect.uninterruptibleMask((restore) =>
                Effect.gen(function*() {
                  const lifetimeScope = yield* Scope.fork(parentScope, "sequential")
                  const connectionScope = yield* Scope.make("parallel")
                  const stateLock = yield* Semaphore.make(1)
                  const closeCompleted = yield* Deferred.make<void>()
                  const fatalCause = yield* Deferred.make<
                    Cause.Cause<ReplicaError.ReplicaError>
                  >()
                  const activeDrained = yield* Deferred.make<void>()
                  const interruptOnClose = Deferred.await(closeCompleted).pipe(
                    Effect.andThen(Effect.interrupt)
                  )
                  let closing = false
                  let activeUses = 0
                  const awaitFatal = Deferred.await(fatalCause).pipe(
                    Effect.flatMap(Effect.failCause)
                  )
                  const closeConnection = (exit: Exit.Exit<unknown, unknown>) =>
                    Effect.sync(() => {
                      if (closing) return false
                      closing = true
                      return true
                    }).pipe(
                      stateLock.withPermit,
                      Effect.flatMap((owner) =>
                        owner
                          ? Scope.close(connectionScope, exit).pipe(
                            Effect.ensuring(Deferred.succeed(closeCompleted, undefined))
                          )
                          : Deferred.await(closeCompleted)
                      ),
                      Effect.uninterruptible
                    )
                  const closeWithExit = (exit: Exit.Exit<unknown, unknown>) =>
                    closeConnection(exit).pipe(
                      Effect.ensuring(Scope.close(lifetimeScope, exit))
                    )
                  const releaseUse = stateLock.withPermit(
                    Effect.gen(function*() {
                      activeUses -= 1
                      if (
                        activeUses === 0 &&
                        (yield* Deferred.isDone(fatalCause))
                      ) {
                        yield* Deferred.succeed(activeDrained, undefined)
                      }
                    })
                  )
                  const beginUse = stateLock.withPermit(
                    Effect.gen(function*() {
                      if (yield* Deferred.isDone(fatalCause)) {
                        return yield* awaitFatal
                      }
                      if (closing) return yield* unavailable()
                      activeUses += 1
                      return releaseUse
                    })
                  )
                  yield* Scope.addFinalizerExit(lifetimeScope, closeConnection)
                  yield* runtime.awaitFatal.pipe(
                    Effect.exit,
                    Effect.flatMap((exit) =>
                      Exit.isFailure(exit) &&
                        !Cause.hasInterruptsOnly(exit.cause)
                        ? Deferred.succeed(fatalCause, exit.cause)
                        : Effect.void
                    ),
                    Effect.forkIn(lifetimeScope, { startImmediately: true })
                  )

                  return yield* restore(Effect.gen(function*() {
                    const openCompleted = yield* Deferred.make<
                      Exit.Exit<
                        readonly [
                          ReadonlyArray<PeerRpc.OpenEvent>,
                          Stream.Stream<
                            PeerRpc.OpenEvent,
                            ReplicaError.ReplicaError
                          >
                        ],
                        ReplicaError.ReplicaError
                      >
                    >()
                    const openRequest = client.Open({
                      protocolVersion: PeerRpc.protocolVersion,
                      expectedRelayPeerId: options.expectedRelayPeerId,
                      expectedLocal: options.expectedLocal,
                      senderReplicaIncarnation: options.senderReplicaIncarnation,
                      remote: options.remote,
                      documents: options.documents.map((entry) => ({
                        documentType: entry.document.name,
                        documentId: entry.documentId
                      })),
                      receiptRetentionMillis: options.receiptRetentionMillis,
                      senderRetryHorizonMillis: advertisedRetryHorizon
                    }, { streamBufferSize: 1 }).pipe(
                      Stream.mapError(mapError),
                      Stream.peel(Sink.take<PeerRpc.OpenEvent>(1)),
                      Effect.provideService(Scope.Scope, connectionScope),
                      Effect.onExit((exit) => Deferred.succeed(openCompleted, exit).pipe(Effect.asVoid))
                    )
                    const openFiber = yield* stateLock.withPermit(
                      Effect.suspend(() =>
                        closing
                          ? Effect.fail(unavailable())
                          : Effect.forkIn(openRequest, connectionScope)
                      )
                    )
                    const [first, remainder] = yield* Effect.raceFirst(
                      awaitFatal,
                      Deferred.await(openCompleted).pipe(
                        Effect.onInterrupt(() => Fiber.interrupt(openFiber)),
                        Effect.flatten
                      )
                    )
                    const handshake = first[0]
                    if (
                      handshake === undefined ||
                      handshake._tag !== "Opened" ||
                      handshake.protocolVersion !== PeerRpc.protocolVersion ||
                      handshake.remotePeerId !== options.remote.peerId ||
                      !samePrincipal(handshake.authenticatedLocal, options.expectedLocal)
                    ) {
                      return yield* protocolFailure("valid relay handshake")
                    }
                    yield* stateLock.withPermit(
                      Effect.gen(function*() {
                        if (closing) return yield* unavailable()
                        if (yield* Deferred.isDone(fatalCause)) {
                          return yield* awaitFatal
                        }
                      })
                    )

                    const sendLock = yield* Semaphore.make(1)
                    const terminalLock = yield* Semaphore.make(1)
                    const callWithinConnection = <A,>(
                      effect: Effect.Effect<A, ReplicaError.ReplicaError>,
                      lock: Semaphore.Semaphore
                    ) =>
                      Effect.uninterruptibleMask((restoreCall) =>
                        Effect.gen(function*() {
                          const release = yield* beginUse
                          const completed = yield* Deferred.make<
                            Exit.Exit<A, ReplicaError.ReplicaError>
                          >()
                          const fiber = yield* Effect.raceFirst(
                            awaitFatal,
                            lock.withPermit(effect)
                          ).pipe(
                            Effect.onExit((exit) => Deferred.succeed(completed, exit).pipe(Effect.asVoid)),
                            Effect.ensuring(release),
                            Effect.forkIn(connectionScope, { startImmediately: true })
                          )
                          return [fiber, completed] as const
                        }).pipe(
                          Effect.flatMap(([fiber, completed]) =>
                            Deferred.await(completed).pipe(
                              restoreCall,
                              Effect.flatten,
                              Effect.onInterrupt(() => Fiber.interrupt(fiber))
                            )
                          )
                        )
                      )

                    const pushEntry = (
                      entry: PeerRelayOutbox.Entry
                    ) =>
                      client.Push({
                        sessionId: handshake.sessionId,
                        relayMessageId: entry.relayMessageId,
                        payload: entry.payload
                      }).pipe(
                        Effect.mapError(mapError),
                        Effect.andThen(runtime.markCustody({
                          relayMessageId: entry.relayMessageId,
                          outerEnvelopeDigest: entry.outerEnvelopeDigest
                        }))
                      )

                    const replay = Effect.gen(function*() {
                      while (true) {
                        const entries = yield* runtime.dueForEndpoint({
                          ...endpoint,
                          maximum: options.replayBatchSize
                        })
                        if (entries.length === 0) return
                        for (const entry of entries) yield* pushEntry(entry)
                      }
                    })
                    yield* callWithinConnection(replay, sendLock)

                    const send = (message: Uint8Array) =>
                      PeerRpcObservability.observe({
                        effect: callWithinConnection(
                          runtime.admit({
                            ...endpoint,
                            payload: message,
                            retryHorizonMillis: options.senderRetryHorizonMillis
                          }).pipe(
                            Effect.flatMap((admission) =>
                              admission._tag === "PendingRelayCustody"
                                ? pushEntry(admission)
                                : Effect.void
                            )
                          ),
                          sendLock
                        ),
                        operation: "AdapterPush",
                        spanName: "effect_local_rpc.adapter.relay_push",
                        attributes: { "rpc.payload_bytes": message.byteLength },
                        result: adapterResult
                      })

                    const terminalCall = (
                      effect: Effect.Effect<void, ReplicaError.ReplicaError>
                    ) =>
                      callWithinConnection(
                        effect,
                        terminalLock
                      ).pipe(
                        Effect.onExitIf(Exit.isFailure, closeWithExit)
                      )

                    const acknowledged = Stream.scoped(
                      Stream.fromEffect(
                        Effect.acquireRelease(beginUse, (release) => release)
                      ).pipe(
                        Stream.flatMap(() =>
                          remainder.pipe(
                            Stream.mapEffect((event) =>
                              event._tag !== "StoredMessage"
                                ? Effect.fail(protocolFailure(event._tag))
                                : validateStoredMessage(event, options, crypto, limits).pipe(
                                  Effect.as(
                                    {
                                      message: event.payload,
                                      identity: {
                                        relayMessageId: event.relayMessageId,
                                        relayPeerId: event.relayPeerId,
                                        senderTenantId: event.sender.tenantId,
                                        senderSubjectId: event.sender.subjectId,
                                        senderPeerId: event.sender.peerId,
                                        senderReplicaIncarnation: event.sender.replicaIncarnation,
                                        messageHash: event.messageHash,
                                        outerEnvelopeDigest: event.outerEnvelopeDigest
                                      },
                                      receiptRetentionMillis: options.receiptRetentionMillis,
                                      acknowledge: terminalCall(
                                        PeerRpcObservability.observeRelay({
                                          effect: client.Acknowledge({
                                            sessionId: handshake.sessionId,
                                            relayMessageId: event.relayMessageId,
                                            claimToken: event.claimToken,
                                            messageHash: event.messageHash
                                          }).pipe(
                                            Effect.mapError(mapError),
                                            Effect.andThen(runtime.signalReceiptPrune)
                                          ),
                                          operation: "AdapterAcknowledge",
                                          direction: "Receive",
                                          facts: () => ({
                                            bytes: event.payload.byteLength,
                                            items: 1,
                                            version: event.payloadVersion
                                          }),
                                          result: adapterAcknowledgeResult("Acknowledged")
                                        })
                                      ),
                                      reject: (reason: PeerTransport.PermanentRejectReason) =>
                                        terminalCall(
                                          PeerRpcObservability.observeRelay({
                                            effect: client.Reject({
                                              sessionId: handshake.sessionId,
                                              relayMessageId: event.relayMessageId,
                                              claimToken: event.claimToken,
                                              messageHash: event.messageHash,
                                              reason
                                            }).pipe(Effect.mapError(mapError)),
                                            operation: "AdapterAcknowledge",
                                            direction: "Receive",
                                            facts: () => ({
                                              bytes: event.payload.byteLength,
                                              items: 1,
                                              version: event.payloadVersion
                                            }),
                                            result: adapterAcknowledgeResult("DeadLettered")
                                          })
                                        )
                                    } satisfies PeerTransport.AcknowledgedDelivery
                                  )
                                )
                            ),
                            Stream.interruptWhen(awaitFatal),
                            Stream.interruptWhen(interruptOnClose)
                          )
                        )
                      )
                    )

                    yield* awaitFatal.pipe(
                      Effect.catchCause((cause) =>
                        stateLock.withPermit(
                          Effect.gen(function*() {
                            if (activeUses === 0) {
                              yield* Deferred.succeed(activeDrained, undefined)
                            }
                          })
                        ).pipe(
                          Effect.andThen(Deferred.await(activeDrained)),
                          Effect.andThen(closeConnection(Exit.failCause(cause)))
                        )
                      ),
                      Effect.forkIn(lifetimeScope, { startImmediately: true })
                    )

                    return {
                      peerId: handshake.remotePeerId,
                      relayPeerId: options.expectedRelayPeerId,
                      relayEndpoint: {
                        expectedLocal: options.expectedLocal,
                        remote: {
                          tenantId: options.expectedLocal.tenantId,
                          ...options.remote
                        },
                        relayPeerId: options.expectedRelayPeerId
                      },
                      capabilities: { lineageAware: true },
                      receive: acknowledged,
                      send,
                      close: closeWithExit(Exit.void)
                    }
                  })).pipe(Effect.onExitIf(Exit.isFailure, closeWithExit))
                })
              )
            }),
            operation: "AdapterOpen",
            spanName: "effect_local_rpc.adapter.relay_open",
            attributes: { "rpc.selected_documents": options.documents.length },
            result: adapterResult
          })
      }
    })
  )

export const makeSession = (
  client: PeerRpc.RpcClient,
  options: Options
) =>
  PeerSession.makeLive({
    peerId: options.remote.peerId,
    documents: options.documents
  }).pipe(
    Effect.provide(layer(client, options))
  )
