import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
export const invalidConfiguration = (option: string, message: string) =>
  new ReplicaError.InvalidConfiguration({ option, message })

export const capacityExceeded = (resource: string, limit: number) =>
  new ReplicaError.CapacityExceeded({ resource, limit })
