import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Transient from "@lucas-barake/effect-local/Transient"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import type * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import { literal } from "./internal/literal.js"
import * as NativeError from "./internal/nativeError.js"
import * as OutboxMaintenance from "./internal/peerRelayOutboxMaintenance.js"
import * as ReceiptMaintenance from "./internal/peerRelayReceiptMaintenance.js"
import * as PeerRelayOutbox from "./PeerRelayOutbox.js"
import * as PeerRelayOutboxLimits from "./PeerRelayOutboxLimits.js"
import * as PeerRelayReceiptLimits from "./PeerRelayReceiptLimits.js"
import type * as PeerSession from "./PeerSession.js"
import * as PeerSync from "./PeerSync.js"
import * as ReplicaGate from "./ReplicaGate.js"

export interface ConnectionConfiguration {
  readonly replicaIncarnation: Identity.ReplicaIncarnation
  readonly retryHorizonMillis: number
  readonly replayBatchSize: number
}

export interface TransientMessage extends PeerTransport.TransientDelivery {}

export interface Registration {
  readonly unregister: Effect.Effect<void>
}

type RuntimeState =
  | { readonly _tag: "Running" }
  | { readonly _tag: "Closing" }
  | {
    readonly _tag: "Failed"
    readonly cause: Cause.Cause<ReplicaError.ReplicaError>
  }

export class PeerRelayClientRuntime extends Context.Service<PeerRelayClientRuntime, {
  readonly admit: PeerRelayOutbox.PeerRelayOutbox["Service"]["admit"]
  readonly dueForEndpoint: PeerRelayOutbox.PeerRelayOutbox["Service"]["dueForEndpoint"]
  readonly maximumPendingHorizon: PeerRelayOutbox.PeerRelayOutbox["Service"]["maximumPendingHorizon"]
  readonly markCustody: PeerRelayOutbox.PeerRelayOutbox["Service"]["markCustody"]
  readonly validateReplicaIncarnation: PeerRelayOutbox.PeerRelayOutbox["Service"]["validateReplicaIncarnation"]
  readonly validateConnectionConfiguration: (
    input: ConnectionConfiguration
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly signalReceiptPrune: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly health: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly awaitFatal: Effect.Effect<never, ReplicaError.ReplicaError>
  readonly register: (
    session: PeerSession.PeerSession,
    documents: ReadonlyArray<PeerSession.SelectedDocument>
  ) => Effect.Effect<Registration, ReplicaError.ReplicaError, Scope.Scope>
  readonly send: (
    peerId: Identity.PeerId,
    documentId: Identity.DocumentId,
    payload: Uint8Array
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly transients: Stream.Stream<TransientMessage>
}>()("@lucas-barake/effect-local-sql/PeerRelayClientRuntime") {}

const unexpectedExit = (
  name: string,
  exit: Exit.Exit<never, ReplicaError.ReplicaError>
): Cause.Cause<ReplicaError.ReplicaError> =>
  (() => {
    if (Exit.isFailure(exit)) return (exit.cause)
    return (Cause.die(NativeError.nativeError(`${name} stopped unexpectedly`)))
  })()

export const makeScoped = Effect.gen(function*() {
  const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
  const sync = yield* PeerSync.PeerSync
  const gate = yield* ReplicaGate.ReplicaGate
  const receiptLimits = yield* PeerRelayReceiptLimits.PeerRelayReceiptLimits
  const outboxLimits = yield* PeerRelayOutboxLimits.PeerRelayOutboxLimits
  const pruneRelayReceipts = sync.pruneRelayReceipts
  if (pruneRelayReceipts === undefined) {
    return yield* Effect.fail(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ProtocolMismatch({
          expected: "relay enabled PeerSync",
          observed: "direct PeerSync"
        })
      })
    )
  }
  const state = yield* Ref.make<RuntimeState>({ _tag: "Running" })
  const fatal = yield* Deferred.make<Cause.Cause<ReplicaError.ReplicaError>>()
  const receiptWakeup = yield* Effect.acquireRelease(
    Queue.dropping<void>(1),
    Queue.shutdown
  )
  const transientPubSub = yield* Effect.acquireRelease(
    PubSub.sliding<TransientMessage>(64),
    PubSub.shutdown
  )
  interface Route {
    readonly token: symbol
    readonly session: PeerSession.PeerSession
  }
  const routes = yield* Ref.make(
    new Map<Identity.PeerId, ReadonlyMap<Identity.DocumentId, Route>>()
  )

  const awaitFatal: Effect.Effect<never, ReplicaError.ReplicaError> = Deferred.await(fatal).pipe(
    Effect.flatMap(Effect.failCause)
  )

  const health = Ref.get(state).pipe(
    Effect.flatMap((current) => {
      switch (current._tag) {
        case "Running":
          return Effect.void
        case "Failed":
          return Effect.failCause(current.cause)
        case "Closing":
          return Effect.interrupt
      }
      return Effect.void
    })
  )

  const guarded = <A, E extends ReplicaError.ReplicaError, R,>(
    effect: Effect.Effect<A, E, R>
  ): Effect.Effect<A, E | ReplicaError.ReplicaError, R> =>
    health.pipe(
      Effect.andThen(effect),
      Effect.raceFirst(awaitFatal)
    )

  const outboxFiber = yield* OutboxMaintenance.run({
    prune: outbox.pruneExpired,
    intervalMillis: outboxLimits.maintenanceIntervalMillis,
    pruneRowsPerSecond: outboxLimits.pruneRowsPerSecond
  }).pipe(Effect.forkScoped({ startImmediately: true }))

  const receiptFiber = yield* ReceiptMaintenance.run({
    prune: pruneRelayReceipts,
    intervalMillis: receiptLimits.maintenanceIntervalMillis,
    pruneRowsPerSecond: receiptLimits.pruneRowsPerSecond,
    wakeup: receiptWakeup
  }).pipe(Effect.forkScoped({ startImmediately: true }))

  const failRuntime = (
    name: string,
    exit: Exit.Exit<never, ReplicaError.ReplicaError>,
    sibling: Fiber.Fiber<never, ReplicaError.ReplicaError>
  ) =>
    Effect.gen(function*() {
      const cause = unexpectedExit(name, exit)
      const first = yield* Ref.modify(state, (current) =>
        (() => {
          if (current._tag === "Running") return [true, literal({ _tag: "Failed", cause })]
          return [false, current]
        })())
      if (!first) return
      yield* Deferred.succeed(fatal, cause)
      yield* Fiber.interrupt(sibling)
    })

  yield* Effect.raceFirst(
    Fiber.await(outboxFiber).pipe(
      Effect.flatMap((exit) => failRuntime("relay outbox maintenance", exit, receiptFiber))
    ),
    Fiber.await(receiptFiber).pipe(
      Effect.flatMap((exit) => failRuntime("relay receipt maintenance", exit, outboxFiber))
    )
  ).pipe(Effect.forkScoped({ startImmediately: true }))

  yield* Effect.addFinalizer(() =>
    Ref.update(state, (current) =>
      (() => {
        if (current._tag === "Running") return (literal({ _tag: "Closing" }))
        return current
      })()).pipe(
        Effect.andThen(Deferred.interrupt(fatal)),
        Effect.asVoid
      )
  )

  const validateConnectionConfiguration = (input: ConnectionConfiguration) =>
    guarded(Effect.gen(function*() {
      yield* outbox.validateReplicaIncarnation(input.replicaIncarnation)
      if (
        !Number.isSafeInteger(input.retryHorizonMillis) ||
        input.retryHorizonMillis <= 0 ||
        input.retryHorizonMillis > outboxLimits.maxRetryHorizonMillis
      ) {
        return yield* Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: `retry horizon from 1 through ${outboxLimits.maxRetryHorizonMillis}`,
              observed: "invalid retry horizon"
            })
          })
        )
      }
      if (
        !Number.isSafeInteger(input.replayBatchSize) ||
        input.replayBatchSize <= 0 ||
        input.replayBatchSize > outboxLimits.maxMessagesPerRemote
      ) {
        return yield* Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: `replay batch from 1 through ${outboxLimits.maxMessagesPerRemote}`,
              observed: "invalid replay batch"
            })
          })
        )
      }
      const permit = yield* gate.current
      if (permit.incarnation !== input.replicaIncarnation) {
        return yield* Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: "current replica incarnation",
              observed: "stale replica incarnation"
            })
          })
        )
      }
      return undefined
    }))

  const unregister = (
    peerId: Identity.PeerId,
    documentIds: ReadonlyArray<Identity.DocumentId>,
    token: symbol
  ) =>
    Ref.update(routes, (current) => {
      const peerRoutes = current.get(peerId)
      if (peerRoutes === undefined) return current
      const nextPeerRoutes = new Map(peerRoutes)
      for (const documentId of documentIds) {
        if (nextPeerRoutes.get(documentId)?.token === token) nextPeerRoutes.delete(documentId)
      }
      const next = new Map(current)
      if (nextPeerRoutes.size === 0) next.delete(peerId)
      else next.set(peerId, nextPeerRoutes)
      return next
    })

  const register = (
    session: PeerSession.PeerSession,
    documents: ReadonlyArray<PeerSession.SelectedDocument>
  ): Effect.Effect<Registration, ReplicaError.ReplicaError, Scope.Scope> =>
    guarded(Effect.gen(function*() {
      const documentIds = documents.map((entry) => entry.documentId)
      if (new Set(documentIds).size !== documentIds.length) {
        return yield* new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "unique selected transient routes",
            observed: String(documents.length)
          })
        })
      }
      const token = Symbol("peer relay transient registration")
      const registered = yield* Ref.modify(routes, (current) => {
        const peerRoutes = current.get(session.peerId) ?? new Map<Identity.DocumentId, Route>()
        const duplicate = documentIds.find((documentId) => peerRoutes.has(documentId))
        if (duplicate !== undefined) return literal([duplicate, current])
        const nextPeerRoutes = new Map(peerRoutes)
        for (const documentId of documentIds) nextPeerRoutes.set(documentId, { token, session })
        return literal([undefined, new Map(current).set(session.peerId, nextPeerRoutes)])
      })
      if (registered !== undefined) {
        return yield* new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: "one live transient route per peer and document",
            observed: `${session.peerId}:${registered}`
          })
        })
      }
      const unregisterRoute = unregister(session.peerId, documentIds, token)
      yield* Effect.addFinalizer(() => unregisterRoute)
      const fiber = yield* session.transients.pipe(
        Stream.runForEachArray((messages) => PubSub.publishAll(transientPubSub, messages)),
        Effect.forkScoped({ startImmediately: true })
      )
      return {
        unregister: unregisterRoute.pipe(
          Effect.andThen(Fiber.interrupt(fiber)),
          Effect.asVoid
        )
      }
    }))

  const send = (
    peerId: Identity.PeerId,
    documentId: Identity.DocumentId,
    payload: Uint8Array
  ) =>
    guarded(
      Ref.get(routes).pipe(
        Effect.flatMap((current) => {
          const route = current.get(peerId)?.get(documentId)
          return (() => {
            if (route === undefined) {
              return (Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause: NativeError.nativeError("Peer transient route is unavailable")
                  })
                })
              ))
            }
            return (route.session.transient(documentId, payload))
          })()
        })
      )
    )

  return {
    admit: (input: PeerRelayOutbox.AdmitInput) => guarded(outbox.admit(input)),
    dueForEndpoint: (input: PeerRelayOutbox.ReplayInput) => guarded(outbox.dueForEndpoint(input)),
    maximumPendingHorizon: (input: PeerRelayOutbox.Endpoint) => guarded(outbox.maximumPendingHorizon(input)),
    markCustody: (input: PeerRelayOutbox.CustodyInput) => guarded(outbox.markCustody(input)),
    validateReplicaIncarnation: (expected: Identity.ReplicaIncarnation) =>
      guarded(outbox.validateReplicaIncarnation(expected)),
    validateConnectionConfiguration,
    signalReceiptPrune: health.pipe(
      Effect.andThen(Queue.offer(receiptWakeup, undefined)),
      Effect.asVoid,
      Effect.raceFirst(awaitFatal)
    ),
    health,
    awaitFatal,
    register,
    send,
    transients: Stream.fromPubSub(transientPubSub)
  }
})

export const layer = Layer.effectContext(
  makeScoped.pipe(
    Effect.map((runtime) =>
      Context.make(PeerRelayClientRuntime, runtime).pipe(
        Context.add(Transient.Transport, {
          send: runtime.send,
          messages: runtime.transients
        })
      )
    )
  )
)

/**
 * `layer` leaves `PeerRelayOutbox` to the consumer, so every relay client had to rediscover that
 * it is satisfied by `layerSql` and that both want the same gate, limits and client.
 */
export const layerSql: Layer.Layer<
  PeerRelayClientRuntime | Transient.Transport,
  ReplicaError.ReplicaError,
  | SqlClient.SqlClient
  | Crypto.Crypto
  | ReplicaLimits.ReplicaLimits
  | ReplicaGate.ReplicaGate
  | PeerSync.PeerSync
  | PeerRelayOutboxLimits.PeerRelayOutboxLimits
  | PeerRelayReceiptLimits.PeerRelayReceiptLimits
> = layer.pipe(Layer.provide(PeerRelayOutbox.layerSql))
