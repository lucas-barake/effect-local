import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as FiberMap from "effect/FiberMap"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import type * as SyncEngine from "../SyncEngine.js"
import * as Configuration from "./configuration.js"
import * as Errors from "./errors.js"

export const isTransportFailure = (error: ReplicaError.ReplicaError) =>
  error._tag === "ServerUnavailable" || error._tag === "OperationTimeout"

export const backoff = (
  remote: SyncEngine.Service,
  delay: number,
  error: ReplicaError.ReplicaError,
  transportGeneration: number
) => {
  if (!isTransportFailure(error)) return Effect.sleep(delay)
  return Effect.raceFirst(Effect.sleep(delay), remote.waitForTransportChange(transportGeneration))
}

export const credentialChange = (
  remote: SyncEngine.Service,
  rejectedGeneration: number,
  retryDelayMillis: number
): Effect.Effect<void> => {
  let reported = false
  const wait: Effect.Effect<void> = Effect.suspend(() => remote.waitForCredentialChange(rejectedGeneration)).pipe(
    Effect.catchCause((cause) => {
      let logged = Effect.void
      if (!reported && Errors.causeKind(cause) === "Defect") {
        reported = true
        logged = Effect.logError("Credential wait died", cause)
      }
      return logged.pipe(Effect.andThen(Effect.sleep(retryDelayMillis)), Effect.andThen(wait))
    })
  )
  return wait
}

export const answeredUnderCredential = <A,>(
  remote: SyncEngine.Service,
  generation: number,
  call: Effect.Effect<A, ReplicaError.ReplicaError>
): Effect.Effect<Option.Option<A>, ReplicaError.ReplicaError> =>
  Effect.result(call).pipe(
    Effect.zip(remote.credentialGeneration),
    Effect.flatMap(([answer, current]): Effect.Effect<Option.Option<A>, ReplicaError.ReplicaError> => {
      if (current !== generation) return Effect.succeed(Option.none())
      if (Result.isFailure(answer)) return Effect.fail(answer.failure)
      return Effect.succeed(Option.some(answer.success))
    })
  )

export const makeRetryPosition = Effect.fnUntraced(function*(options: {
  readonly timing: Configuration.RetryTiming
  readonly pending: Effect.Effect<number>
  readonly retry: Effect.Effect<void>
}) {
  const stalls = yield* FiberMap.make<"stalled", void, never>()
  let attempt = 0
  let stalledPending = 0
  let stallTimed = false
  const nextDelay = () => {
    attempt += 1
    return Configuration.retryMillis(options.timing, attempt)
  }
  const cancelStalledRetry = Effect.suspend(() => {
    stallTimed = false
    return FiberMap.remove(stalls, "stalled")
  })
  const retryUnfinished = Effect.gen(function*() {
    const left = yield* options.pending
    if (left === 0 || left < stalledPending) attempt = 0
    stalledPending = left
    if (left === 0) {
      yield* cancelStalledRetry
      return
    }
    if (stallTimed) return
    stallTimed = true
    const elapsed = Effect.sync(() => {
      stallTimed = false
    })
    const retried = Effect.sleep(nextDelay()).pipe(Effect.andThen(elapsed), Effect.andThen(options.retry))
    yield* FiberMap.run(stalls, "stalled", retried)
  })
  return {
    nextDelay,
    reset: () => {
      attempt = 0
    },
    cancelStalledRetry,
    retryUnfinished
  }
})

export const superviseWatch = <R,>(options: {
  readonly spaceId: Identity.SpaceId
  readonly watch: Effect.Effect<void, never, R>
  readonly closedDelay: Effect.Effect<number>
  readonly watchFailed: (error: ReplicaError.ReplicaError) => Effect.Effect<void>
  readonly resync: Effect.Effect<void>
}): Effect.Effect<void, never, R> => {
  let watchEnded: Cause.Cause<never> | undefined
  const supervised: Effect.Effect<void, never, R> = Effect.suspend(() => {
    const ended = watchEnded
    watchEnded = undefined
    if (ended === undefined) return options.watch
    const resubscribed = options.closedDelay.pipe(Effect.flatMap(Effect.sleep))
    if (Errors.causeKind(ended) !== "Defect") return Effect.andThen(resubscribed, options.watch)
    const died = Errors.iterationFailure("Sync watch died", ended)
    return Effect.logError("Sync watch died", ended).pipe(
      Effect.andThen(options.watchFailed(died)),
      Effect.andThen(resubscribed),
      Effect.andThen(options.resync),
      Effect.annotateLogs({ "space.id": options.spaceId }),
      Effect.andThen(options.watch)
    )
  }).pipe(
    Effect.catchCause((cause) => {
      watchEnded = cause
      return supervised
    })
  )
  return supervised
}
