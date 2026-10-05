import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"

export const unexpectedFailure = (message: string, cause: Cause.Cause<unknown>) =>
  new ReplicaError.UnexpectedFailure({ message, cause: Cause.squash(cause) })
