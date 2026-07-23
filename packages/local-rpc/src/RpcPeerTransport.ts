import * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Sink from "effect/Sink"
import * as Stream from "effect/Stream"
import { RpcClientError } from "effect/unstable/rpc/RpcClientError"
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

const mapError = (error: PeerRpcError.PeerRpcError | RpcClientError) => {
  if (error instanceof RpcClientError) return unavailable()
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
    case "InvalidRequest":
    case "RequestLimitExceeded":
      return protocolFailure(error._tag)
  }
}

export const isRetryable = (error: ReplicaError.ReplicaError): boolean => error.reason._tag === "StorageUnavailable"

const adapterResult = (exit: Exit.Exit<unknown, ReplicaError.ReplicaError>) => {
  if (Exit.isSuccess(exit)) return "Success" as const
  const error = PeerRpcObservability.failure(exit)
  return error !== undefined && (error.reason._tag === "ProtocolMismatch" || error.reason._tag === "QuotaExceeded")
    ? "ProtocolRejected" as const
    : "Failure" as const
}

export const layer = (
  client: PeerRpc.RpcClient,
  options: { readonly documents: ReadonlyArray<PeerSession.SelectedDocument> }
) =>
  Layer.succeed(PeerTransport.PeerTransport, {
    capabilities: { storeAndForward: false },
    connect: (connectOptions) =>
      PeerRpcObservability.observe({
        effect: Effect.gen(function*() {
          const parentScope = yield* Scope.Scope
          return yield* Effect.uninterruptibleMask((restore) =>
            Effect.gen(function*() {
              const lifetimeScope = yield* Scope.fork(parentScope, "sequential")
              const connectionScope = yield* Scope.make("parallel")
              const stateLock = yield* Semaphore.make(1)
              const closeCompleted = yield* Deferred.make<void>()
              let closing = false
              const closeConnection = (exit: Exit.Exit<unknown, unknown>) =>
                stateLock.withPermit(Effect.sync(() => {
                  if (closing) return false
                  closing = true
                  return true
                })).pipe(
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
              yield* Scope.addFinalizerExit(lifetimeScope, closeConnection)
              return yield* restore(Effect.gen(function*() {
                const openCompleted = yield* Deferred.make<
                  Exit.Exit<
                    readonly [
                      ReadonlyArray<PeerRpc.OpenEvent>,
                      Stream.Stream<PeerRpc.OpenEvent, ReplicaError.ReplicaError>
                    ],
                    ReplicaError.ReplicaError
                  >
                >()
                const openRequest = client.Open({
                  protocolVersion: PeerRpc.protocolVersion,
                  expectedPeerId: connectOptions.peerId,
                  documents: options.documents.map((entry) => ({
                    documentType: entry.document.name,
                    documentId: entry.documentId
                  }))
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
                const openExit = yield* Deferred.await(openCompleted).pipe(
                  Effect.onInterrupt(() => Fiber.interrupt(openFiber))
                )
                const [first, remainder] = yield* Exit.isSuccess(openExit)
                  ? Effect.succeed(openExit.value)
                  : Effect.failCause(openExit.cause)
                const handshake = first[0]
                if (handshake === undefined || handshake._tag !== "Opened") {
                  return yield* protocolFailure(handshake?._tag ?? "Open stream ended before handshake")
                }
                if (handshake.peerId !== connectOptions.peerId) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: connectOptions.peerId,
                      observed: handshake.peerId
                    })
                  })
                }
                yield* stateLock.withPermit(
                  Effect.suspend(() => closing ? Effect.fail(unavailable()) : Effect.void)
                )
                const sendLock = yield* Semaphore.make(1)
                const send = (message: Uint8Array) =>
                  PeerRpcObservability.observe({
                    effect: Effect.uninterruptibleMask((restoreSend) =>
                      stateLock.withPermit(Effect.gen(function*() {
                        if (closing) return yield* unavailable()
                        const completed = yield* Deferred.make<Exit.Exit<void, ReplicaError.ReplicaError>>()
                        const fiber = yield* client.Push({ sessionId: handshake.sessionId, payload: message }).pipe(
                          Effect.mapError(mapError),
                          sendLock.withPermit,
                          Effect.onExit((exit) => Deferred.succeed(completed, exit).pipe(Effect.asVoid)),
                          Effect.forkIn(connectionScope, { startImmediately: true })
                        )
                        return [fiber, completed] as const
                      })).pipe(
                        Effect.flatMap(([fiber, completed]) =>
                          restoreSend(Deferred.await(completed)).pipe(
                            Effect.flatMap((exit) => Exit.isSuccess(exit) ? Effect.void : Effect.failCause(exit.cause)),
                            Effect.onInterrupt(() => Fiber.interrupt(fiber))
                          )
                        )
                      )
                    ),
                    operation: "AdapterPush",
                    spanName: "effect_local_rpc.adapter.push",
                    attributes: { "rpc.payload_bytes": message.byteLength },
                    result: adapterResult
                  })
                return {
                  peerId: handshake.peerId,
                  capabilities: handshake.capabilities,
                  receive: remainder.pipe(
                    Stream.mapEffect((event) =>
                      event._tag === "Message"
                        ? Effect.succeed(event.payload)
                        : Effect.fail(protocolFailure(event._tag))
                    )
                  ),
                  send,
                  close: closeWithExit(Exit.void)
                }
              })).pipe(
                Effect.onExit((exit) => Exit.isFailure(exit) ? closeWithExit(exit) : Effect.void)
              )
            })
          )
        }),
        operation: "AdapterOpen",
        spanName: "effect_local_rpc.adapter.open",
        attributes: { "rpc.selected_documents": options.documents.length },
        result: adapterResult
      })
  })

export const makeSession = (
  client: PeerRpc.RpcClient,
  options: {
    readonly peerId: Identity.PeerId
    readonly documents: ReadonlyArray<PeerSession.SelectedDocument>
  }
) =>
  PeerSession.makeLive(options).pipe(
    Effect.provide(layer(client, { documents: options.documents }))
  )
