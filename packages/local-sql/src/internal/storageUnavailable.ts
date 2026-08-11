import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"

export const make = (cause: unknown): ReplicaError.StorageUnavailable => new ReplicaError.StorageUnavailable({ cause })
