import * as Schema from "effect/Schema"
import * as Identity from "./Identity.js"
import * as Protocol from "./Protocol.js"

export const QuarantinedMutation = Schema.Struct({
  envelope: Protocol.MutationEnvelope,
  rejection: Schema.Json,
  targetSchema: Identity.SchemaIdentity
})
export type QuarantinedMutation = typeof QuarantinedMutation.Type

export const Resubmitted = Schema.TaggedStruct("Resubmitted", { pending: Protocol.PendingMutation })
export const AlreadyResolved = Schema.TaggedStruct("AlreadyResolved", { receipt: Protocol.Receipt })
export const ResubmitResult = Schema.Union([Resubmitted, AlreadyResolved])
export type ResubmitResult = typeof ResubmitResult.Type
