import * as Context from "effect/Context"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

const Limit = Schema.Int.check(Schema.isGreaterThan(0))
const boundedLimit = (maximum: number) => Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(maximum))
const utf8Bytes = (value: string): number => new TextEncoder().encode(value).byteLength

export const maxConflictDepthHardLimit = 128
export const maxConflictNodesHardLimit = 100_000
export const maxConflictAlternativesHardLimit = 10_000
export const maxConflictPathSegmentsHardLimit = 128
export const maxConflictValueBytesHardLimit = 16 * 1024 * 1024
export const maxConflictSourceChangesHardLimit = 100_000
export const maxConflictSourceOperationsHardLimit = 100_000
export const maxConflictSourceBytesHardLimit = 64 * 1024 * 1024

export const minimumRestoreErrorBytes = [
  "_tag",
  "UnsupportedDocumentVersion",
  "documentId",
  "doc_00000000-0000-4000-8000-000000000000",
  "observedVersion",
  "supportedVersion"
].reduce(
  (total, value) => total + utf8Bytes(value),
  0
)

const RestoreErrorLimit = Schema.Int.check(Schema.isGreaterThanOrEqualTo(minimumRestoreErrorBytes))

export const Values = Schema.Struct({
  maxBackupBytes: Limit,
  maxChunkBytes: Limit,
  maxArchiveRecords: Limit,
  maxJsonDepth: Limit,
  maxConflictDepth: boundedLimit(maxConflictDepthHardLimit),
  maxConflictNodes: boundedLimit(maxConflictNodesHardLimit),
  maxConflictAlternatives: boundedLimit(maxConflictAlternativesHardLimit),
  maxConflictPathSegments: boundedLimit(maxConflictPathSegmentsHardLimit),
  maxConflictValueBytes: boundedLimit(maxConflictValueBytesHardLimit),
  maxConflictSourceChanges: boundedLimit(maxConflictSourceChangesHardLimit),
  maxConflictSourceOperations: boundedLimit(maxConflictSourceOperationsHardLimit),
  maxConflictSourceBytes: boundedLimit(maxConflictSourceBytesHardLimit),
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
  maxQueuedRpc: Limit,
  maxQueuedPermits: Limit,
  maxActiveRestores: Limit,
  maxRestoresPerSession: Limit,
  maxRestoreMillis: Limit,
  maxRestorePullMillis: Limit,
  maxRestoreCoalesceMillis: Limit,
  maxRestoreErrorBytes: RestoreErrorLimit
}).check(
  Schema.makeFilter(
    (values) => values.maxRestoreCoalesceMillis < values.maxRestorePullMillis,
    { expected: "maxRestoreCoalesceMillis being less than maxRestorePullMillis" }
  )
)
export type Values = typeof Values.Type

export class ReplicaLimits extends Context.Service<ReplicaLimits, Values>()(
  "@lucas-barake/effect-local/ReplicaLimits"
) {}

export const make = (values: Values) => Values.makeEffect(values)

export const layer = (values: Values) => Layer.effect(ReplicaLimits, make(values))
