import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import type * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as OutboxMaintenance from "./internal/peerRelayOutboxMaintenance.js"
import * as ReceiptMaintenance from "./internal/peerRelayReceiptMaintenance.js"
import * as PeerRelayOutbox from "./PeerRelayOutbox.js"
import * as PeerRelayOutboxLimits from "./PeerRelayOutboxLimits.js"
import * as PeerRelayReceiptLimits from "./PeerRelayReceiptLimits.js"
import * as PeerSync from "./PeerSync.js"
import * as ReplicaGate from "./ReplicaGate.js"

export interface ConnectionConfiguration {
  readonly replicaIncarnation: Identity.ReplicaIncarnation
  readonly retryHorizonMillis: number
  readonly replayBatchSize: number
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
}>()("@lucas-barake/effect-local-sql/PeerRelayClientRuntime") {}

const protocolMismatch = (expected: string, observed: string) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.ProtocolMismatch({ expected, observed })
  })

const unexpectedExit = (
  name: string,
  exit: Exit.Exit<never, ReplicaError.ReplicaError>
): Cause.Cause<ReplicaError.ReplicaError> =>
  Exit.isFailure(exit)
    ? exit.cause
    : Cause.die(new Error(`${name} stopped unexpectedly`))

export const makeScoped = Effect.gen(function*() {
  const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
  const sync = yield* PeerSync.PeerSync
  const gate = yield* ReplicaGate.ReplicaGate
  const receiptLimits = yield* PeerRelayReceiptLimits.PeerRelayReceiptLimits
  const outboxLimits = yield* PeerRelayOutboxLimits.PeerRelayOutboxLimits
  const pruneRelayReceipts = sync.pruneRelayReceipts
  if (pruneRelayReceipts === undefined) {
    return yield* Effect.fail(
      protocolMismatch("relay enabled PeerSync", "direct PeerSync")
    )
  }
  const state = yield* Ref.make<RuntimeState>({ _tag: "Running" })
  const fatal = yield* Deferred.make<Cause.Cause<ReplicaError.ReplicaError>>()
  const receiptWakeup = yield* Effect.acquireRelease(
    Queue.dropping<void>(1),
    Queue.shutdown
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
    })
  )

  const guarded = <A, E extends ReplicaError.ReplicaError,>(
    effect: Effect.Effect<A, E>
  ): Effect.Effect<A, E | ReplicaError.ReplicaError> =>
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
        current._tag === "Running"
          ? [true, { _tag: "Failed", cause } as const]
          : [false, current])
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
    Ref.update(state, (current) => current._tag === "Running" ? { _tag: "Closing" } as const : current).pipe(
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
          protocolMismatch(
            `retry horizon from 1 through ${outboxLimits.maxRetryHorizonMillis}`,
            "invalid retry horizon"
          )
        )
      }
      if (
        !Number.isSafeInteger(input.replayBatchSize) ||
        input.replayBatchSize <= 0 ||
        input.replayBatchSize > outboxLimits.maxMessagesPerRemote
      ) {
        return yield* Effect.fail(
          protocolMismatch(
            `replay batch from 1 through ${outboxLimits.maxMessagesPerRemote}`,
            "invalid replay batch"
          )
        )
      }
      const permit = yield* gate.current
      if (permit.incarnation !== input.replicaIncarnation) {
        return yield* Effect.fail(
          protocolMismatch("current replica incarnation", "stale replica incarnation")
        )
      }
    }))

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
    awaitFatal
  }
})

export const layer = Layer.effect(PeerRelayClientRuntime, makeScoped)

/**
 * `layer` leaves `PeerRelayOutbox` to the consumer, so every relay client had to rediscover that
 * it is satisfied by `layerSql` and that both want the same gate, limits and client.
 */
export const layerSql: Layer.Layer<
  PeerRelayClientRuntime,
  ReplicaError.ReplicaError,
  | SqlClient.SqlClient
  | Crypto.Crypto
  | ReplicaLimits.ReplicaLimits
  | ReplicaGate.ReplicaGate
  | PeerSync.PeerSync
  | PeerRelayOutboxLimits.PeerRelayOutboxLimits
  | PeerRelayReceiptLimits.PeerRelayReceiptLimits
> = layer.pipe(Layer.provide(PeerRelayOutbox.layerSql))
