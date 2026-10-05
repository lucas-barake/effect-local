import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"

export const causeKind = <E,>(cause: Cause.Cause<E>): "Failure" | "Defect" | "Interruption" => {
  if (Cause.hasFails(cause)) return "Failure"
  if (Cause.hasDies(cause)) return "Defect"
  return "Interruption"
}

export const iterationFailure = (
  message: string,
  cause: Cause.Cause<unknown>
): ReplicaError.UnexpectedFailure | ReplicaError.ServerUnavailable => {
  if (causeKind(cause) === "Defect") {
    return new ReplicaError.UnexpectedFailure({ message, cause: Cause.squash(cause) })
  }
  return new ReplicaError.ServerUnavailable()
}

export const logDefect = (message: string, cause: Cause.Cause<unknown>): Effect.Effect<void> => {
  if (causeKind(cause) !== "Defect") return Effect.void
  return Effect.logError(message, cause)
}

export const failDiedIteration = (
  message: string,
  spaceId: Identity.SpaceId,
  beforeFailing: Effect.Effect<void> = Effect.void,
  owner?: Scope.Scope
) =>
<A, E extends { readonly _tag: string }, R,>(
  iteration: Effect.Effect<A, E, R>
): Effect.Effect<A, E | ReplicaError.UnexpectedFailure | ReplicaError.ServerUnavailable, R> =>
  Effect.catchCause(
    iteration,
    (cause): Effect.Effect<never, E | ReplicaError.UnexpectedFailure | ReplicaError.ServerUnavailable> => {
      if (causeKind(cause) === "Failure") return Effect.failCause(cause)
      if (owner?.state._tag === "Closed") return Effect.failCause(cause)
      const failure = iterationFailure(message, cause)
      return logDefect(message, cause).pipe(
        Effect.annotateLogs({ "space.id": spaceId }),
        Effect.andThen(beforeFailing),
        Effect.andThen(Effect.fail(failure))
      )
    }
  )
