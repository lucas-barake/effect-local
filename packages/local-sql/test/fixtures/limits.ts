import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Layer from "effect/Layer"
import type * as Schema from "effect/Schema"

export const gateLimits: ReplicaLimits.Values = {
  maxBackupBytes: 16 * 1024 * 1024,
  maxChunkBytes: 64 * 1024,
  maxArchiveRecords: 10_000,
  maxJsonDepth: 64,
  maxConflictDepth: 64,
  maxConflictNodes: 100_000,
  maxConflictAlternatives: 10_000,
  maxConflictPathSegments: 128,
  maxConflictValueBytes: 16 * 1024 * 1024,
  maxConflictSourceChanges: 100_000,
  maxConflictSourceOperations: 100_000,
  maxConflictSourceBytes: 64 * 1024 * 1024,
  maxSyncMessageBytes: 1024 * 1024,
  maxPeerSendMillis: 10_000,
  maxSyncChangesPerMessage: 1000,
  maxSyncDependencyEdgesPerMessage: 10_000,
  maxSyncOperationsPerMessage: 100_000,
  maxPendingBytesPerDocument: 16 * 1024 * 1024,
  maxPendingBytesPerPeer: 32 * 1024 * 1024,
  maxPendingBytesPerReplica: 64 * 1024 * 1024,
  maxPendingAgeMillis: 60_000,
  maxPendingChangesPerDocument: 10_000,
  maxPendingChangesPerPeer: 20_000,
  maxPendingChangesPerReplica: 50_000,
  maxPendingDependencyEdgesPerDocument: 100_000,
  maxPendingDependencyEdgesPerPeer: 200_000,
  maxPendingDependencyEdgesPerReplica: 500_000,
  maxSessions: 32,
  maxStreamsPerSession: 32,
  maxInFlightPerSession: 128,
  maxQueuedRpc: 1024,
  maxQueuedPermits: 1024,
  maxActiveRestores: 1024,
  maxRestoresPerSession: 128,
  maxRestoreMillis: 30_000,
  maxRestorePullMillis: 10_000,
  maxRestoreCoalesceMillis: 25,
  maxRestoreErrorBytes: 4_096
}

const gateLimitsLayer = ReplicaLimits.layer(gateLimits)

export const withGateLimits = <ROut, E, RIn,>(
  self: Layer.Layer<ROut, E, RIn>
): Layer.Layer<ROut | ReplicaLimits.ReplicaLimits, E | Schema.SchemaError, Exclude<RIn, ReplicaLimits.ReplicaLimits>> =>
  self.pipe(Layer.provideMerge(gateLimitsLayer))
