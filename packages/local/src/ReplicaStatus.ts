import * as Schema from "effect/Schema"

export const Starting = Schema.TaggedStruct("Starting", { phase: Schema.String })
export type Starting = typeof Starting.Type

export const Ready = Schema.TaggedStruct("Ready", {
  pendingCommands: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
})
export type Ready = typeof Ready.Type

export const ReadOnly = Schema.TaggedStruct("ReadOnly", { reason: Schema.String })
export type ReadOnly = typeof ReadOnly.Type

export const Degraded = Schema.TaggedStruct("Degraded", { reason: Schema.String })
export type Degraded = typeof Degraded.Type

export const ProjectionBlocked = Schema.TaggedStruct("ProjectionBlocked", {
  projection: Schema.String,
  reason: Schema.String
})
export type ProjectionBlocked = typeof ProjectionBlocked.Type

export const Restoring = Schema.TaggedStruct("Restoring", {
  processedBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
})
export type Restoring = typeof Restoring.Type

export const Failed = Schema.TaggedStruct("Failed", { message: Schema.String })
export type Failed = typeof Failed.Type

export const ReplicaStatus = Schema.Union([
  Starting,
  Ready,
  ReadOnly,
  Degraded,
  ProjectionBlocked,
  Restoring,
  Failed
])
export type ReplicaStatus = typeof ReplicaStatus.Type
