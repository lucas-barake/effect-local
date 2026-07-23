import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as BackupStore from "@lucas-barake/effect-local-sql/BackupStore"
import * as CommandExecutor from "@lucas-barake/effect-local-sql/CommandExecutor"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as DocumentStore from "@lucas-barake/effect-local-sql/DocumentStore"
import * as PeerSync from "@lucas-barake/effect-local-sql/PeerSync"
import * as ProjectionStore from "@lucas-barake/effect-local-sql/ProjectionStore"
import * as QueryExecutor from "@lucas-barake/effect-local-sql/QueryExecutor"
import * as Recovery from "@lucas-barake/effect-local-sql/Recovery"
import * as ReplicaBootstrap from "@lucas-barake/effect-local-sql/ReplicaBootstrap"
import * as ReplicaGate from "@lucas-barake/effect-local-sql/ReplicaGate"
import type * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Layer from "effect/Layer"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"

export const defaultLimits: ReplicaLimits.Values = {
  maxBackupBytes: 16 * 1024 * 1024,
  maxChunkBytes: 64 * 1024,
  maxArchiveRecords: 10_000,
  maxJsonDepth: 64,
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
  maxQueuedRpc: 1024
}

export const layerWithLimits = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: { readonly projections: Bindings; readonly limits: ReplicaLimits.Values }
) =>
  SqlReplica.layerWithBindings(definition, { projections: options.projections }).pipe(
    Layer.provide([
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer,
      ReplicaLimits.layer(options.limits)
    ])
  )

export const layer = <D extends ReplicaDefinition.Any, const Bindings extends ReadonlyArray<SqlProjection.Any>,>(
  definition: D,
  options: { readonly projections: Bindings }
) => layerWithLimits(definition, { projections: options.projections, limits: defaultLimits })

export const layerWithSyncAndLimits = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: { readonly projections: Bindings; readonly limits: ReplicaLimits.Values }
) => {
  const bootstrap = ReplicaBootstrap.layer(definition).pipe(
    Layer.provideMerge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))
  )
  const gate = ReplicaGate.layer.pipe(
    Layer.provideMerge([bootstrap, ReplicaLimits.layer(options.limits), NodeCrypto.layer])
  )
  const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
  const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
  const projections = ProjectionStore.layer(options.projections).pipe(Layer.provideMerge(store))
  const commands = CommandExecutor.layer(definition).pipe(Layer.provideMerge(projections))
  const queries = QueryExecutor.layer(definition).pipe(
    Layer.provideMerge([commands, Reactivity.layer])
  )
  const publisher = CommitPublisher.layer.pipe(Layer.provideMerge(queries))
  const backups = BackupStore.layer(definition).pipe(Layer.provideMerge(publisher))
  const sync = PeerSync.layer.pipe(Layer.provideMerge(backups))
  return SqlReplica.layerFromServices(definition).pipe(
    Layer.provideMerge(sync),
    Layer.provide([Layer.empty, ...options.projections.map((binding) => binding.layer)])
  )
}

export const layerWithSync = <
  D extends ReplicaDefinition.Any,
  const Bindings extends ReadonlyArray<SqlProjection.Any>,
>(
  definition: D,
  options: { readonly projections: Bindings }
) => layerWithSyncAndLimits(definition, { projections: options.projections, limits: defaultLimits })
