import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"

export const invalidConfiguration = (option: string, message: string) =>
  new ReplicaError.InvalidConfiguration({ option, message })
