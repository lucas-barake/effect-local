import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"

export const causeKind = <E,>(cause: Cause.Cause<E>): "Failure" | "Defect" | "Interruption" => {
  if (Cause.hasFails(cause)) return "Failure"
  if (Cause.hasDies(cause)) return "Defect"
  return "Interruption"
}

export const unexpectedFailure = (message: string, cause: Cause.Cause<unknown>) =>
  new ReplicaError.UnexpectedFailure({ message, cause: Cause.squash(cause) })

export const iterationFailure = (
  message: string,
  cause: Cause.Cause<unknown>
): ReplicaError.UnexpectedFailure | ReplicaError.ServerUnavailable => {
  if (causeKind(cause) === "Defect") return unexpectedFailure(message, cause)
  return new ReplicaError.ServerUnavailable()
}

export const logDefect = (message: string, cause: Cause.Cause<unknown>): Effect.Effect<void> => {
  if (causeKind(cause) !== "Defect") return Effect.void
  return Effect.logError(message, cause)
}

const isStopping = (scope: Scope.Scope): boolean => {
  let current: Scope.Scope | undefined = scope
  while (current !== undefined) {
    if (current.state._tag === "Closed") return true
    current = current.parent
  }
  return false
}

export const endsLoop = (scope: Scope.Scope, cause: Cause.Cause<unknown>): boolean =>
  causeKind(cause) === "Interruption" && isStopping(scope)
