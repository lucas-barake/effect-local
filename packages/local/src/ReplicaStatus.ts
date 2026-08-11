import * as Schema from "effect/Schema"
import * as Identity from "./Identity.js"

export const Offline = Schema.TaggedStruct("Offline", { pending: Schema.Int })
export const Connecting = Schema.TaggedStruct("Connecting", { pending: Schema.Int })
export const Online = Schema.TaggedStruct("Online", {
  pending: Schema.Int,
  cursor: Identity.ServerSequence
})
export const NeedsAuthentication = Schema.TaggedStruct("NeedsAuthentication", { pending: Schema.Int })
export const Failed = Schema.TaggedStruct("Failed", { pending: Schema.Int, message: Schema.String })
export const ReplicaStatus = Schema.Union([Offline, Connecting, Online, NeedsAuthentication, Failed])
export type ReplicaStatus = typeof ReplicaStatus.Type
