import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"

export const isCurrent = (requested: Identity.SchemaIdentity, current: Identity.SchemaIdentity) =>
  requested.version === current.version && requested.hash === current.hash

export const validate = (
  scope: Protocol.ReplicationScope,
  requested: Identity.SchemaIdentity,
  current: Identity.SchemaIdentity
) => {
  if (
    scope.windows === undefined ||
    isCurrent(requested, current)
  ) return Effect.void
  return Effect.fail(
    new ReplicaError.ProtocolInvalid({
      message: "Windowed replication scope requires the current server schema"
    })
  )
}
