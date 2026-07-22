import * as Context from "effect/Context"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

const Limit = Schema.Int.check(Schema.isGreaterThan(0))

export const Values = Schema.Struct({
  maxBackupBytes: Limit,
  maxChunkBytes: Limit,
  maxArchiveRecords: Limit,
  maxJsonDepth: Limit,
  maxSyncMessageBytes: Limit,
  maxPeerSendMillis: Limit,
  maxSyncChangesPerMessage: Limit,
  maxSyncDependencyEdgesPerMessage: Limit,
  maxSyncOperationsPerMessage: Limit,
  maxPendingBytesPerDocument: Limit,
  maxPendingBytesPerPeer: Limit,
  maxPendingBytesPerReplica: Limit,
  maxPendingAgeMillis: Limit,
  maxPendingChangesPerDocument: Limit,
  maxPendingChangesPerPeer: Limit,
  maxPendingChangesPerReplica: Limit,
  maxPendingDependencyEdgesPerDocument: Limit,
  maxPendingDependencyEdgesPerPeer: Limit,
  maxPendingDependencyEdgesPerReplica: Limit,
  maxSessions: Limit,
  maxStreamsPerSession: Limit,
  maxInFlightPerSession: Limit,
  maxQueuedRpc: Limit
})
export type Values = typeof Values.Type

export class ReplicaLimits extends Context.Service<ReplicaLimits, Values>()(
  "@lucas-barake/effect-local/ReplicaLimits"
) {}

export const make = (values: Values) => Values.makeEffect(values)

export const layer = (values: Values) => Layer.effect(ReplicaLimits, make(values))
