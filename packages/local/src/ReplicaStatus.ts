import * as Schema from "effect/Schema"
import * as Identity from "./Identity.js"

export const Offline = Schema.TaggedStruct("Offline", { pending: Schema.Int })
export const Connecting = Schema.TaggedStruct("Connecting", { pending: Schema.Int })
export const Online = Schema.TaggedStruct("Online", {
  pending: Schema.Int,
  cursor: Identity.ServerSequence
})
export const SchemaUpdateAvailable = Schema.TaggedStruct("SchemaUpdateAvailable", {
  pending: Schema.Int,
  cursor: Identity.ServerSequence,
  serverSchema: Identity.SchemaIdentity
})
export const NeedsAuthentication = Schema.TaggedStruct("NeedsAuthentication", { pending: Schema.Int })
export const Failed = Schema.TaggedStruct("Failed", { pending: Schema.Int, message: Schema.String })
export const ReplicaStatus = Schema.Union([
  Offline,
  Connecting,
  Online,
  SchemaUpdateAvailable,
  NeedsAuthentication,
  Failed
])
export type ReplicaStatus = typeof ReplicaStatus.Type

export const SpaceStatus = Schema.Union([
  Schema.Struct({ spaceId: Identity.SpaceId, ...Offline.fields }),
  Schema.Struct({ spaceId: Identity.SpaceId, ...Connecting.fields }),
  Schema.Struct({ spaceId: Identity.SpaceId, ...Online.fields }),
  Schema.Struct({ spaceId: Identity.SpaceId, ...SchemaUpdateAvailable.fields }),
  Schema.Struct({ spaceId: Identity.SpaceId, ...NeedsAuthentication.fields }),
  Schema.Struct({ spaceId: Identity.SpaceId, ...Failed.fields })
])
export type SpaceStatus = typeof SpaceStatus.Type

export const AggregateState = Schema.Literals([
  "Idle",
  "Failed",
  "NeedsAuthentication",
  "Online",
  "Offline",
  "Connecting",
  "Degraded"
])
export type AggregateState = typeof AggregateState.Type

export const Aggregate = Schema.Struct({
  state: AggregateState,
  spaces: Schema.Array(SpaceStatus),
  totalPending: Schema.Int,
  counts: Schema.Struct({
    offline: Schema.Int,
    connecting: Schema.Int,
    online: Schema.Int,
    needsAuthentication: Schema.Int,
    failed: Schema.Int
  })
})
export type Aggregate = typeof Aggregate.Type
