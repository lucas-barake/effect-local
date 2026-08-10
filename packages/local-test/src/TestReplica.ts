import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as PeerSync from "@lucas-barake/effect-local-sql/PeerSync"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as ReplicaHealth from "@lucas-barake/effect-local-sql/ReplicaHealth"
import type * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Layer from "effect/Layer"

export const defaultLimits: ReplicaLimits.Values = {
  maxBackupBytes: 16 * 1024 * 1024,
  maxChunkBytes: 64 * 1024,
  maxArchiveRecords: 10_000,
  maxJsonDepth: 64,
  maxConflictDepth: 128,
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

export const layerWithLimits = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: {
    readonly health?: ReplicaHealth.Options
    readonly projections: Bindings
    readonly limits: ReplicaLimits.Values
  }
) =>
  SqlReplica.layerWithBindings(definition, {
    health: options.health ?? ReplicaHealth.defaultOptions,
    projections: options.projections
  }).pipe(
    Layer.provide([
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer,
      ReplicaLimits.layer(options.limits)
    ])
  )

export const layer = <D extends ReplicaDefinition.Any, const Bindings extends ReadonlyArray<SqlProjection.Any>,>(
  definition: D,
  options: { readonly health?: ReplicaHealth.Options; readonly projections: Bindings }
) =>
  layerWithLimits(definition, {
    health: options.health ?? ReplicaHealth.defaultOptions,
    projections: options.projections,
    limits: defaultLimits
  })

export const layerWithSyncAndLimits = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: {
    readonly database?: SqliteClient.SqliteClientConfig
    readonly health?: ReplicaHealth.Options
    readonly projections: Bindings
    readonly limits: ReplicaLimits.Values
  }
) => {
  const infrastructure = Layer.mergeAll(
    SqliteClient.layer(options.database ?? { filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    ReplicaLimits.layer(options.limits)
  )
  const services = SqlReplica.servicesLayerWithBindings(definition, {
    health: options.health ?? ReplicaHealth.defaultOptions,
    projections: options.projections
  })
  const sync = PeerSync.layer.pipe(Layer.provideMerge(services))
  return Layer.mergeAll(
    SqlReplica.layerFromServices(definition).pipe(
      Layer.provideMerge(sync),
      Layer.provideMerge(infrastructure)
    ),
    PeerConnectionStatus.layer,
    // Direct mode, so there is no relay to report on. `layerFromServices` hand assembles the stack
    // and never reaches the constructors that answer this, so it is answered here.
    RelayConnectionStatus.layerNotConfigured
  )
}

export const layerWithSync = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: {
    readonly database?: SqliteClient.SqliteClientConfig
    readonly health?: ReplicaHealth.Options
    readonly projections: Bindings
  }
) => {
  const common = {
    health: options.health ?? ReplicaHealth.defaultOptions,
    projections: options.projections,
    limits: defaultLimits
  }
  if (options.database === undefined) return layerWithSyncAndLimits(definition, common)
  return layerWithSyncAndLimits(definition, { ...common, database: options.database })
}
