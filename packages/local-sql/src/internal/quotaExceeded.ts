import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"

export const quotaExceeded = (resource: string, limit: number): ReplicaError.ReplicaError =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.QuotaExceeded({ resource, limit })
  })
