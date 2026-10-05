import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import type * as SyncEngine from "../SyncEngine.js"
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

export const superviseWatch = <R,>(options: {
  readonly scope: Scope.Scope
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
    const died = Errors.unexpectedFailure("Sync watch died", ended)
    const reported = options.watchFailed(died).pipe(
      Effect.catchCause((cause) => Errors.logDefect("Sync watch failure report died", cause))
    )
    const requested = options.resync.pipe(
      Effect.catchCause((cause) => Errors.logDefect("Sync request after a watch failure died", cause))
    )
    return Effect.logError("Sync watch died", ended).pipe(
      Effect.andThen(reported),
      Effect.andThen(resubscribed),
      Effect.andThen(requested),
      Effect.annotateLogs({ "space.id": options.spaceId }),
      Effect.andThen(options.watch)
    )
  }).pipe(
    Effect.catchCause((cause) => {
      if (Errors.endsLoop(options.scope, cause)) return Effect.failCause(cause)
      watchEnded = cause
      return supervised
    })
  )
  return supervised
}
