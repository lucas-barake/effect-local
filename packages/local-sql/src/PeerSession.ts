import type * as Document from "@lucas-barake/effect-local/Document"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import * as CommandDeliveryStore from "./CommandDeliveryStore.js"
import * as CommitPublisher from "./CommitPublisher.js"
import * as DocumentEntity from "./DocumentEntity.js"
import * as PeerConnectionStatus from "./PeerConnectionStatus.js"
import * as PeerRelayReceiptLimits from "./PeerRelayReceiptLimits.js"
import * as PeerSync from "./PeerSync.js"
import * as PeerSyncEnvelope from "./PeerSyncEnvelope.js"
import * as ReplicaGate from "./ReplicaGate.js"

export interface SelectedDocument {
  readonly document: Document.Any
  readonly documentId: Identity.DocumentId
}

export interface PeerSession {
  readonly peerId: Identity.PeerId
  readonly connectionEpoch: string
  readonly markDirty: (documentId: Identity.DocumentId) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly flush: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly observedByPeer: (documentId: Identity.DocumentId) => Effect.Effect<boolean>
  readonly durableConfirmation: (
    documentId: Identity.DocumentId
  ) => Effect.Effect<boolean, ReplicaError.ReplicaError>
}

export interface SupervisedPeerSession extends PeerSession {
  readonly awaitDisconnect: Effect.Effect<never, ReplicaError.ReplicaError>
}

class RelayProtocolInvalid extends Schema.TaggedErrorClass<RelayProtocolInvalid>(
  "@lucas-barake/effect-local-sql/PeerSession/RelayProtocolInvalid"
)("RelayProtocolInvalid", {}) {}

const key = (documentType: string, documentId: Identity.DocumentId) => `${documentType}:${documentId}`

const supervise = (
  failTerminal: (error: ReplicaError.ReplicaError) => Effect.Effect<void>,
  effect: Effect.Effect<void, ReplicaError.ReplicaError>
) =>
  effect.pipe(
    Effect.tapError(failTerminal),
    Effect.catchCauseIf(
      (cause) => !Cause.hasInterruptsOnly(cause),
      (cause) =>
        failTerminal(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({ cause })
          })
        )
    )
  )

interface OpenedSession {
  readonly session: SupervisedPeerSession
  readonly disconnect: Effect.Effect<void>
  readonly failTerminal: (error: ReplicaError.ReplicaError) => Effect.Effect<void>
}

const makeWithTerminal = (
  options: {
    readonly peerId: Identity.PeerId
    readonly documents: ReadonlyArray<SelectedDocument>
  },
  entity: (
    documentId: Identity.DocumentId
  ) => Effect.Effect<ReturnType<Effect.Success<typeof DocumentEntity.DocumentEntity.client>>>,
  terminalFailure: Deferred.Deferred<never, ReplicaError.ReplicaError>
): Effect.Effect<
  OpenedSession,
  ReplicaError.ReplicaError,
  | Scope.Scope
  | CommandDeliveryStore.CommandDeliveryStore
  | CommitPublisher.CommitPublisher
  | Crypto.Crypto
  | PeerConnectionStatus.Reporter
  | PeerTransport.PeerTransport
  | PeerSync.PeerSync
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
> => {
  let cleanupOnError: Effect.Effect<void> = Effect.void
  return Effect.gen(function*() {
    const deliveries = yield* CommandDeliveryStore.CommandDeliveryStore
    const gate = yield* ReplicaGate.ReplicaGate
    const publisher = yield* CommitPublisher.CommitPublisher
    const limits = yield* ReplicaLimits.ReplicaLimits
    const transport = yield* PeerTransport.PeerTransport
    const sync = yield* PeerSync.PeerSync
    const crypto = yield* Crypto.Crypto
    const relayReceiptLimits = yield* Effect.serviceOption(PeerRelayReceiptLimits.PeerRelayReceiptLimits)
    const decodeRelay = (bytes: Uint8Array) =>
      PeerSyncEnvelope.decodeSyncEnvelope(bytes, limits).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.catchReason(
          "ReplicaError",
          "ProtocolMismatch",
          () => Effect.fail(new RelayProtocolInvalid())
        )
      )
    const selected = new Set(options.documents.map((entry) => key(entry.document.name, entry.documentId)))
    const selectedDocumentIds = new Set(options.documents.map((entry) => entry.documentId))
    if (
      selected.size !== options.documents.length ||
      selectedDocumentIds.size !== options.documents.length
    ) {
      return yield* new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "unique selected documents",
          observed: String(options.documents.length)
        })
      })
    }
    // An ordinary dependency, not `Effect.serviceOption`. This is the only writer of peer connection
    // status, so a graph that provides the reader to its observers and builds the session outside
    // that context would compile and then report every peer as `Disconnected` forever, with nothing
    // to indicate the wiring was wrong. That is the same failure the reader side already refuses.
    const reporter = yield* PeerConnectionStatus.Reporter
    const attempt = yield* reporter.connecting(options.peerId).pipe(
      Effect.tap((attempt) =>
        Effect.sync(() => {
          cleanupOnError = attempt.disconnected
        })
      ),
      Effect.uninterruptible
    )
    const transitionGate = yield* Semaphore.make(1)
    const admissionOpen = yield* Ref.make(true)
    const lifetime = yield* Effect.scope
    const disconnect = transitionGate.withPermit(
      Ref.set(admissionOpen, false).pipe(Effect.andThen(attempt.disconnected))
    )
    const reportConnected = transitionGate.withPermit(
      Effect.gen(function*() {
        if (!(yield* Ref.get(admissionOpen)) || (yield* Deferred.isDone(terminalFailure))) return
        yield* attempt.connected
      })
    )
    const { connection, session } = yield* Effect.acquireUseRelease(
      Scope.make(),
      (scope) =>
        Effect.gen(function*() {
          const permit = yield* gate.shared.pipe(Effect.provideService(Scope.Scope, scope))
          const connection = yield* transport.connect({ replicaId: permit.replicaId, peerId: options.peerId })
          if (connection.peerId !== options.peerId) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: options.peerId,
                observed: connection.peerId
              })
            })
          }
          const session = yield* sync.open(connection.peerId)
          if (session.peerId !== connection.peerId) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: connection.peerId,
                observed: session.peerId
              })
            })
          }
          if (session.replicaIncarnation !== permit.incarnation) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: String(permit.incarnation),
                observed: String(session.replicaIncarnation)
              })
            })
          }
          return { connection, session }
        }),
      Scope.close
    )
    const dirty = yield* Ref.make(new Map<Identity.DocumentId, number>())
    const observed = yield* Ref.make(
      new Map(
        options.documents.map((entry) => [entry.documentId, { value: false, revision: 0 }])
      )
    )
    // Documents this session has permanently stopped synchronizing. A lineage change is scoped to
    // the one document it names, so it belongs here and not in `terminalFailure`.
    const refused = yield* Ref.make(new Set<Identity.DocumentId>())
    const remoteEpoch = yield* Ref.make<string | null>(null)
    const active = yield* Ref.make(true)
    const teardown = yield* Deferred.make<void>()
    const sendLock = yield* Semaphore.make(1)
    const flushLock = yield* Semaphore.make(1)
    const flushRequests = yield* Queue.dropping<void>(1)
    const scheduled = yield* Ref.make(new Map<number, PeerSync.Outbound>())
    const syncLocks = new Map(options.documents.map((entry) => [entry.documentId, Semaphore.makeUnsafe(1)]))
    const failTerminal = (error: ReplicaError.ReplicaError) =>
      transitionGate.withPermit(Effect.gen(function*() {
        if (yield* Deferred.isDone(terminalFailure)) return
        yield* Ref.set(admissionOpen, false)
        yield* attempt.disconnected
        yield* sendLock.withPermit(connection.close).pipe(
          Effect.forkIn(lifetime, { startImmediately: true, uninterruptible: true })
        )
        yield* Deferred.fail(terminalFailure, error)
      }))

    const selectedById = (documentId: Identity.DocumentId) => {
      const entry = options.documents.find((candidate) => candidate.documentId === documentId)
      return entry === undefined
        ? Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "selected document",
              observed: documentId
            })
          })
        )
        : Effect.succeed(entry)
    }

    /**
     * Retires one document from this session instead of failing the session.
     *
     * A lineage change is permanent, but it is permanent for exactly the document it names. Routing
     * it into `terminalFailure` like every other failure would let an authorized peer forge one
     * lineage on one document and stop every other selected document from synchronizing, because
     * the supervisor treats the resulting non retryable failure as a reason to stop reconnecting.
     *
     * Clearing the dirty entry and recording the id are both required: the record is what keeps a
     * later `markDirty` from putting the document back into the flush loop.
     */
    const refuse = (
      documentId: Identity.DocumentId,
      reason: ReplicaError.DocumentLineageChanged
    ) =>
      Ref.update(refused, (current) => new Set(current).add(documentId)).pipe(
        Effect.andThen(Ref.update(dirty, (values) => {
          if (!values.has(documentId)) return values
          const next = new Map(values)
          next.delete(documentId)
          return next
        })),
        Effect.andThen(
          Effect.logWarning("Peer session refused one document after its lineage changed").pipe(
            Effect.annotateLogs({
              documentId,
              peerId: connection.peerId,
              localLineage: reason.localLineage,
              remoteLineage: reason.remoteLineage
            })
          )
        )
      )

    const send = (outbound: PeerSync.Outbound) =>
      Effect.raceFirst(
        Deferred.await(teardown),
        Effect.gen(function*() {
          if (!(yield* Ref.get(active))) return
          const entry = yield* selectedById(outbound.documentId)
          const bytes = yield* PeerSyncEnvelope.encodeSyncEnvelope({
            connectionEpoch: session.connectionEpoch,
            sequence: outbound.sendSequence,
            documentId: outbound.documentId,
            documentType: entry.document.name,
            messageHash: outbound.messageHash,
            message: outbound.message,
            // From the queued row, never re-read from the document. The row records the lineage the
            // message was generated under, and a rewrite between generation and send must not
            // relabel it.
            lineage: outbound.lineage,
            writerProvenance: outbound.writerProvenance
          })
          yield* Effect.scoped(Effect.gen(function*() {
            const permit = yield* gate.shared
            if (permit.incarnation !== session.replicaIncarnation) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.ProtocolMismatch({
                  expected: String(session.replicaIncarnation),
                  observed: String(permit.incarnation)
                })
              })
            }
            yield* sendLock.withPermit(Effect.gen(function*() {
              if (!(yield* Ref.get(active))) return
              yield* connection.send(bytes).pipe(
                Effect.timeout(limits.maxPeerSendMillis),
                Effect.catchTag("TimeoutError", (cause) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.StorageUnavailable({
                        cause
                      })
                    })
                  ))
              )
              if (yield* Ref.get(active)) {
                yield* sync.markSent(session, outbound.sendSequence, outbound.messageHash)
              }
            }))
          }))
        })
      )

    const schedule = (outbound: PeerSync.Outbound) =>
      Ref.update(scheduled, (current) => new Map(current).set(outbound.sendSequence, outbound))

    const withSyncLock = <A, E, R,>(
      documentId: Identity.DocumentId,
      effect: Effect.Effect<A, E, R>
    ) => syncLocks.get(documentId)!.withPermit(effect)

    const drainOutbox = (afterSend: ReadonlyMap<number, Effect.Effect<void>>) =>
      Effect.gen(function*() {
        const pending = yield* Effect.raceFirst(
          Deferred.await(teardown).pipe(Effect.as([] as const)),
          sync.pending(session)
        )
        const scheduledNow = yield* Ref.getAndSet(scheduled, new Map())
        const bySequence = new Map(pending.map((outbound) => [outbound.sendSequence, outbound]))
        for (const [sendSequence, outbound] of scheduledNow) bySequence.set(sendSequence, outbound)
        const ordered = [...bySequence.values()].toSorted((left, right) => left.sendSequence - right.sendSequence)
        for (let index = 0; index < ordered.length; index++) {
          const outbound = ordered[index]!
          yield* send(outbound).pipe(
            Effect.onError(() =>
              Ref.update(scheduled, (current) => {
                const next = new Map(ordered.slice(index).map((value) => [value.sendSequence, value]))
                for (const [sendSequence, value] of current) next.set(sendSequence, value)
                return next
              })
            )
          )
          const update = afterSend.get(outbound.sendSequence)
          if (update !== undefined) yield* update
        }
        return ordered.length
      })

    const flush = flushLock.withPermit(Effect.gen(function*() {
      if (!(yield* Ref.get(active))) return
      yield* drainOutbox(new Map())
      if (!(yield* Ref.get(active))) return
      const current = yield* Ref.get(dirty)
      for (const entry of options.documents) {
        if (!(yield* Ref.get(active))) return
        // The whole point of the refusal record. Without this skip a refused document that is
        // marked dirty again -- by a later local commit, or by a full refresh -- would be handed
        // back to `generate` on every flush and refused again, once per flush, forever.
        if ((yield* Ref.get(refused)).has(entry.documentId)) continue
        const revision = current.get(entry.documentId)
        if (revision === undefined) continue
        const generated = yield* Effect.gen(function*() {
          while (true) {
            const attempt = yield* Effect.raceFirst(
              Deferred.await(teardown).pipe(
                Effect.as({ _tag: "TornDown" } as const)
              ),
              withSyncLock(
                entry.documentId,
                Effect.gen(function*() {
                  const observationRevision = (yield* Ref.get(observed)).get(entry.documentId)?.revision ?? 0
                  const result = yield* sync.generate(entry.document, entry.documentId, session, {
                    lineageAware: connection.capabilities.lineageAware === true
                  })
                  yield* Ref.update(observed, (values) => {
                    const current = values.get(entry.documentId)
                    if ((current?.revision ?? 0) !== observationRevision) return values
                    return new Map(values).set(entry.documentId, {
                      value: result.observedByPeer,
                      revision: observationRevision
                    })
                  })
                  return result
                }).pipe(
                  Effect.map((result) => ({ _tag: "Generated", result } as const)),
                  // The send direction refusal. The peer never sees it, so it must not travel any
                  // further than this document's own slot in the flush loop.
                  Effect.catchReason(
                    "ReplicaError",
                    "DocumentLineageChanged",
                    (reason) => refuse(entry.documentId, reason).pipe(Effect.as({ _tag: "Refused" } as const))
                  ),
                  Effect.catchIf(
                    (error) =>
                      error.reason._tag === "QuotaExceeded" &&
                      (error.reason.resource === "peer sync outbox messages" ||
                        error.reason.resource === "peer sync outbox bytes"),
                    (error) => Effect.succeed({ _tag: "OutboxQuota", error } as const)
                  )
                )
              )
            )
            if (attempt._tag !== "OutboxQuota") return attempt
            if ((yield* drainOutbox(new Map())) === 0) return yield* attempt.error
          }
        })
        if (generated._tag === "TornDown") return
        if (generated._tag === "Refused") continue
        const update = Ref.update(dirty, (values) => {
          if (values.get(entry.documentId) !== revision) return values
          const next = new Map(values)
          if (generated.result.dirty) next.set(entry.documentId, revision + 1)
          else next.delete(entry.documentId)
          return next
        })
        if (generated.result.outbound === null) yield* update
        else {
          yield* schedule(generated.result.outbound)
          yield* drainOutbox(new Map([[generated.result.outbound.sendSequence, update]]))
        }
      }
      if ((yield* Ref.get(scheduled)).size > 0) yield* drainOutbox(new Map())
    }))
    const guardTerminal = (effect: Effect.Effect<void, ReplicaError.ReplicaError>) =>
      transitionGate.withPermit(Ref.get(admissionOpen)).pipe(
        Effect.flatMap((admitted) =>
          admitted
            ? Effect.raceFirst(Deferred.await(terminalFailure), effect).pipe(
              Effect.andThen(Deferred.isDone(terminalFailure)),
              Effect.flatMap((failed) => failed ? Deferred.await(terminalFailure) : Effect.void)
            )
            : Deferred.isDone(terminalFailure).pipe(
              Effect.flatMap((failed) => failed ? Deferred.await(terminalFailure) : Effect.void)
            )
        )
      )
    const guardedFlush = guardTerminal(flush)

    const bindRemoteEpoch = (connectionEpoch: string, rotate: boolean) =>
      Effect.gen(function*() {
        const transition = yield* Ref.modify(
          remoteEpoch,
          (current): [{ readonly bound: string; readonly previous: string | null }, string] => {
            if (current === null) return [{ bound: connectionEpoch, previous: null }, connectionEpoch]
            if (current === connectionEpoch) return [{ bound: current, previous: null }, current]
            if (rotate) return [{ bound: connectionEpoch, previous: current }, connectionEpoch]
            return [{ bound: current, previous: null }, current]
          }
        )
        if (transition.bound !== connectionEpoch) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: transition.bound,
              observed: connectionEpoch
            })
          })
        }
        if (transition.previous !== null) {
          yield* sync.reset({
            peerId: connection.peerId,
            connectionEpoch: transition.previous,
            replicaIncarnation: session.replicaIncarnation
          })
        }
        return transition.bound
      })

    const processReceive = (
      bytes: Uint8Array,
      delivery: PeerTransport.AcknowledgedDelivery
    ) =>
      Effect.gen(function*() {
        const protocolInvalid = (expected: string, observed: string) => Effect.fail(new RelayProtocolInvalid())
        const envelope = yield* decodeRelay(bytes)
        const boundEpoch = yield* bindRemoteEpoch(envelope.connectionEpoch, true)
        const selectedDocument = selected.has(key(envelope.documentType, envelope.documentId))
        // Dropped after envelope validation but before entity dispatch. Nothing can restore the
        // refused lineage inside this session, so dispatching every further message the peer sends
        // for it would be a peer driven retry loop over allocation and storage.
        if (selectedDocument && (yield* Ref.get(refused)).has(envelope.documentId)) {
          return "ApplicationRejected" as const
        }
        if (!selectedDocument) {
          return yield* protocolInvalid(
            "selected whole document",
            `${envelope.documentType}:${envelope.documentId}`
          )
        }
        const result = yield* withSyncLock(
          envelope.documentId,
          Effect.gen(function*() {
            const incarnation = yield* Effect.scoped(Effect.gen(function*() {
              const permit = yield* gate.shared
              if (permit.incarnation !== session.replicaIncarnation) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause: new Error(
                      `Replica incarnation changed from ${session.replicaIncarnation} to ${permit.incarnation}`
                    )
                  })
                })
              }
              return permit.incarnation
            }))
            const observationRevision = (yield* Ref.get(observed)).get(envelope.documentId)?.revision ?? 0
            const client = yield* entity(envelope.documentId)
            const relay = yield* Effect.gen(function*() {
              if (relayReceiptLimits._tag === "None") {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause: new Error("Relay receipt limits are unavailable")
                  })
                })
              }
              if (
                !Number.isSafeInteger(delivery.receiptRetentionMillis) ||
                delivery.receiptRetentionMillis <= 0 ||
                delivery.receiptRetentionMillis > relayReceiptLimits.value.receiptRetentionMillis
              ) {
                return yield* new RelayProtocolInvalid()
              }
              if (delivery.identity.relayPeerId !== connection.relayPeerId) {
                return yield* new RelayProtocolInvalid()
              }
              if (delivery.identity.senderPeerId !== connection.peerId) {
                return yield* new RelayProtocolInvalid()
              }
              if (delivery.identity.messageHash !== envelope.messageHash) {
                return yield* new RelayProtocolInvalid()
              }
              const nowMillis = yield* Clock.currentTimeMillis
              return {
                ...delivery.identity,
                receiptExpiresAt: new Date(nowMillis + delivery.receiptRetentionMillis).toISOString(),
                encodedSize: bytes.byteLength
              } satisfies PeerSync.RelayReceipt
            })
            const applySync = client.ApplySync({
              replicaIncarnation: incarnation,
              peerId: connection.peerId,
              connectionEpoch: boundEpoch,
              localConnectionEpoch: session.connectionEpoch,
              receiveSequence: envelope.sequence,
              documentType: envelope.documentType,
              messageHash: envelope.messageHash,
              message: envelope.message,
              // The single point an envelope's lineage enters the system, so the single point the
              // absent key of a pre lineage peer becomes the genesis lineage.
              lineage: envelope.lineage,
              writerProvenance: envelope.writerProvenance,
              relay
            }).pipe(
              Effect.catchTag(
                ["MailboxFull", "AlreadyProcessingMessage", "PersistenceError"],
                (cause) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.StorageUnavailable({
                        cause
                      })
                    })
                  )
              )
            )
            const result = yield* applySync.pipe(
              Effect.catchReason(
                "ReplicaError",
                "ProtocolMismatch",
                () =>
                  Effect.gen(function*() {
                    const current = yield* gate.current
                    if (current.incarnation === session.replicaIncarnation) {
                      return yield* new RelayProtocolInvalid()
                    }
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.StorageUnavailable({
                        cause: new Error(
                          `Replica incarnation changed from ${session.replicaIncarnation} to ${current.incarnation}`
                        )
                      })
                    })
                  })
              )
            )
            yield* Ref.update(observed, (values) => {
              const current = values.get(envelope.documentId)
              if ((current?.revision ?? 0) !== observationRevision) return values
              return new Map(values).set(envelope.documentId, {
                value: result.observedByPeer,
                revision: observationRevision
              })
            })
            return result
          })
        ).pipe(
          // The receive direction refusal. `PeerSync.receive` rejects the message before it reaches
          // storage, so there is nothing to publish and nothing to reply with, and the session goes
          // on serving every other selected document.
          Effect.catchReason(
            "ReplicaError",
            "DocumentLineageChanged",
            (reason) => refuse(envelope.documentId, reason).pipe(Effect.as(null))
          ),
          // Quota trips are transient, so rejecting would discard a message the replica can hold
          // later, and failing the session turned one over-quota message into a reconnect churn
          // loop. Settling nothing keeps custody at the relay until the next session retries.
          Effect.catchReason(
            "ReplicaError",
            "QuotaExceeded",
            (reason) =>
              Effect.logWarning(
                "Peer session parked an inbound message after a receive quota was exceeded; the message stays in relay custody"
              ).pipe(
                Effect.annotateLogs({
                  documentId: envelope.documentId,
                  peerId: connection.peerId,
                  resource: reason.resource,
                  limit: reason.limit
                }),
                Effect.as("Parked" as const)
              )
          )
        )
        if (result === "Parked") return "Parked" as const
        if (result === null) return "ApplicationRejected" as const
        yield* publisher.publishPending
        if (result.reply !== null) {
          // A reply that cannot be enqueued deterministically — over the per-message change
          // budget, unresolvable provenance, an unencodable envelope — will fail identically on
          // every retry, and failing the session only reconnects into the same reply while the
          // relay redelivers the message that asked for it. Withholding the reply keeps the
          // session serving and lets the inbound settle; the peer stays behind on this document,
          // which the warning makes visible. Storage unavailability stays session-fatal: it is
          // transient and a reconnect genuinely retries it.
          yield* sync.enqueue(session, result.reply).pipe(
            Effect.flatMap(schedule),
            Effect.catchIf(
              (error) => error.reason._tag !== "StorageUnavailable",
              (error) =>
                Effect.logWarning(
                  "Peer session withheld an unsendable sync reply; the peer stays behind on this document"
                ).pipe(
                  Effect.annotateLogs({
                    documentId: envelope.documentId,
                    peerId: connection.peerId,
                    reason: error.reason._tag,
                    limit: error.reason._tag === "QuotaExceeded" ? error.reason.limit : undefined
                  })
                )
            )
          )
          yield* Queue.offer(flushRequests, undefined)
        }
        return "Applied" as const
      })

    const relayCall = (
      operation: string,
      effect: Effect.Effect<void, ReplicaError.ReplicaError>
    ) =>
      effect.pipe(
        Effect.timeout(limits.maxPeerSendMillis),
        Effect.catchTag("TimeoutError", () =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.OperationTimeout({
                operation,
                timeoutMillis: limits.maxPeerSendMillis
              })
            })
          ))
      )

    // A failed settlement costs the message, never the session. Transient failures are retried on
    // this session — the relay keeps the delivery addressable for exactly that. Once retries are
    // exhausted, custody stays with the relay: the attempt is abandoned server side at its settle
    // deadline and the head is redelivered, where the durable receipt written by the apply turns
    // the re-apply into a plain re-acknowledgement. Failing the session here turned one
    // unsettleable message into a reconnect loop that burned every pending message's delivery
    // budget toward dead letter.
    const settleDelivery = (
      operation: string,
      effect: Effect.Effect<void, ReplicaError.ReplicaError>
    ) =>
      relayCall(operation, effect).pipe(
        Effect.retry({
          schedule: Schedule.spaced(Duration.seconds(1)),
          times: 2,
          while: (error) => error.reason._tag === "StorageUnavailable"
        }),
        Effect.catchTag("ReplicaError", (error) =>
          Effect.logWarning("relay settlement failed; the message stays in relay custody").pipe(
            Effect.annotateLogs({ operation, reason: error.reason._tag })
          ))
      )

    const receiveAcknowledged = (delivery: PeerTransport.AcknowledgedDelivery) =>
      processReceive(delivery.message, delivery).pipe(
        Effect.flatMap((outcome) =>
          outcome === "ApplicationRejected"
            ? settleDelivery("relay reject", delivery.reject("ApplicationRejected"))
            : outcome === "Parked"
            ? Effect.void
            : settleDelivery("relay acknowledge", delivery.acknowledge)
        ),
        Effect.catchTag(
          "RelayProtocolInvalid",
          () => settleDelivery("relay reject", delivery.reject("ProtocolInvalid"))
        )
      )
    yield* Effect.addFinalizer(() =>
      disconnect.pipe(Effect.andThen(Effect.gen(function*() {
        const boundEpoch = yield* Ref.get(remoteEpoch)
        yield* sync.reset(session).pipe(
          Effect.ensuring(
            boundEpoch === null
              ? Effect.void
              : sync.reset({
                peerId: connection.peerId,
                connectionEpoch: boundEpoch,
                replicaIncarnation: session.replicaIncarnation
              }).pipe(Effect.orDie)
          )
        )
      }))).pipe(
        Effect.ensuring(connection.close),
        Effect.orDie
      )
    )
    yield* supervise(
      failTerminal,
      Stream.runForEach(connection.receive, receiveAcknowledged).pipe(
        Effect.andThen(
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause: new Error("Peer connection receive stream ended")
              })
            })
          )
        )
      )
    ).pipe(
      Effect.ensuring(
        disconnect.pipe(
          Effect.andThen(Ref.set(active, false)),
          Effect.andThen(sendLock.withPermit(connection.close))
        )
      ),
      Effect.forkScoped
    )
    yield* Effect.addFinalizer(() =>
      disconnect.pipe(
        Effect.andThen(Ref.set(active, false)),
        Effect.andThen(Deferred.succeed(teardown, undefined)),
        Effect.andThen(flushLock.withPermit(Effect.void))
      )
    )
    yield* Deferred.await(terminalFailure).pipe(
      Effect.exit,
      Effect.andThen(
        disconnect.pipe(
          Effect.andThen(Ref.set(active, false)),
          Effect.andThen(Deferred.succeed(teardown, undefined)),
          Effect.andThen(sendLock.withPermit(connection.close))
        )
      ),
      Effect.forkScoped({ startImmediately: true })
    )
    yield* reportConnected
    yield* Ref.set(dirty, new Map(options.documents.map((entry) => [entry.documentId, 0])))
    yield* guardedFlush
    yield* supervise(
      failTerminal,
      Stream.fromQueue(flushRequests).pipe(
        Stream.runForEach(() => guardedFlush)
      )
    ).pipe(
      Effect.forkScoped({ startImmediately: true })
    )
    const sessionValue: SupervisedPeerSession = {
      peerId: connection.peerId,
      connectionEpoch: session.connectionEpoch,
      markDirty: (documentId) =>
        guardTerminal(
          selectedById(documentId).pipe(
            Effect.andThen(Ref.update(
              dirty,
              (current) => new Map(current).set(documentId, (current.get(documentId) ?? 0) + 1)
            )),
            Effect.andThen(Ref.update(observed, (current) => {
              const value = current.get(documentId)
              return new Map(current).set(documentId, {
                value: false,
                revision: (value?.revision ?? 0) + 1
              })
            })),
            Effect.tapError(failTerminal)
          )
        ),
      flush: guardedFlush,
      observedByPeer: (documentId) =>
        Ref.get(observed).pipe(Effect.map((values) => values.get(documentId)?.value ?? false)),
      durableConfirmation: (documentId) => deliveries.documentConfirmed(documentId, connection.relayEndpoint),
      awaitDisconnect: Deferred.await(terminalFailure)
    }
    return { session: sessionValue, disconnect, failTerminal }
  }).pipe(
    Effect.onError(() => cleanupOnError),
    Effect.withSpan("PeerSession.connect")
  )
}

export const makeTestClient = (
  options: {
    readonly peerId: Identity.PeerId
    readonly documents: ReadonlyArray<SelectedDocument>
  },
  entity: (
    documentId: Identity.DocumentId
  ) => Effect.Effect<ReturnType<Effect.Success<typeof DocumentEntity.DocumentEntity.client>>>
): Effect.Effect<
  PeerSession,
  ReplicaError.ReplicaError,
  | Scope.Scope
  | CommandDeliveryStore.CommandDeliveryStore
  | CommitPublisher.CommitPublisher
  | Crypto.Crypto
  | PeerConnectionStatus.Reporter
  | PeerTransport.PeerTransport
  | PeerSync.PeerSync
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
> =>
  Deferred.make<never, ReplicaError.ReplicaError>().pipe(
    Effect.flatMap((terminalFailure) => makeWithTerminal(options, entity, terminalFailure)),
    Effect.map((opened) => opened.session)
  )

export const makeSupervised = (options: {
  readonly peerId: Identity.PeerId
  readonly documents: ReadonlyArray<SelectedDocument>
}): Effect.Effect<
  SupervisedPeerSession,
  ReplicaError.ReplicaError,
  | Scope.Scope
  | CommandDeliveryStore.CommandDeliveryStore
  | CommitPublisher.CommitPublisher
  | Crypto.Crypto
  | PeerConnectionStatus.Reporter
  | PeerTransport.PeerTransport
  | PeerSync.PeerSync
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
  | Sharding.Sharding
> =>
  Effect.gen(function*() {
    const entity = yield* DocumentEntity.DocumentEntity.client
    const terminalFailure = yield* Deferred.make<never, ReplicaError.ReplicaError>()
    const opened = yield* makeWithTerminal(
      options,
      (documentId) => Effect.succeed(entity(documentId)),
      terminalFailure
    )
    return opened.session
  })

export const make = (options: {
  readonly peerId: Identity.PeerId
  readonly documents: ReadonlyArray<SelectedDocument>
}): Effect.Effect<
  PeerSession,
  ReplicaError.ReplicaError,
  | Scope.Scope
  | CommandDeliveryStore.CommandDeliveryStore
  | CommitPublisher.CommitPublisher
  | Crypto.Crypto
  | PeerConnectionStatus.Reporter
  | PeerTransport.PeerTransport
  | PeerSync.PeerSync
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
  | Sharding.Sharding
> => makeSupervised(options)

export const makeLive = (options: {
  readonly peerId: Identity.PeerId
  readonly documents: ReadonlyArray<SelectedDocument>
}): Effect.Effect<
  SupervisedPeerSession,
  ReplicaError.ReplicaError,
  | Scope.Scope
  | CommandDeliveryStore.CommandDeliveryStore
  | CommitPublisher.CommitPublisher
  | Crypto.Crypto
  | PeerConnectionStatus.Reporter
  | PeerTransport.PeerTransport
  | PeerSync.PeerSync
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
  | Sharding.Sharding
> =>
  Effect.gen(function*() {
    const publisher = yield* CommitPublisher.CommitPublisher
    const subscription = yield* publisher.subscribe
    const entity = yield* DocumentEntity.DocumentEntity.client
    const terminalFailure = yield* Deferred.make<never, ReplicaError.ReplicaError>()
    const opened = yield* makeWithTerminal(
      options,
      (documentId) => Effect.succeed(entity(documentId)),
      terminalFailure
    )
    const session = opened.session
    const selected = new Set(options.documents.map((entry) => entry.documentId))
    const subscriptionEnded = new ReplicaError.ReplicaError({
      reason: new ReplicaError.StorageUnavailable({
        cause: new Error("Commit subscription stream ended")
      })
    })
    const consume = Stream.runForEach(subscription.events, (event) => {
      if (event._tag === "Commit") {
        return selected.has(event.documentId)
          ? session.markDirty(event.documentId).pipe(Effect.andThen(session.flush))
          : Effect.void
      }
      return Effect.forEach(options.documents, (entry) => session.markDirty(entry.documentId), {
        discard: true
      }).pipe(Effect.andThen(session.flush))
    }).pipe(
      Effect.andThen(Effect.fail(subscriptionEnded))
    )
    yield* supervise(opened.failTerminal, Effect.raceFirst(Deferred.await(terminalFailure), consume)).pipe(
      Effect.forkScoped({ startImmediately: true })
    )
    return session
  })
