import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"

export const reconciliationDied = (message: string, cause: Cause.Cause<never>) =>
  new ReplicaError.ProtocolInvalid({ message, cause: Cause.squash(cause) })
