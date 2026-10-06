import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"

export const invalidConfiguration = (option: string, message: string) =>
  new ReplicaError.InvalidConfiguration({ option, message })

export const causeKind = <E,>(cause: Cause.Cause<E>): "Failure" | "Defect" | "Interruption" => {
  if (Cause.hasFails(cause)) return "Failure"
  if (Cause.hasDies(cause)) return "Defect"
  return "Interruption"
}
