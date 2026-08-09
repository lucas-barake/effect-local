import * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"

export const Task = Document.make("Task", {
  schema: Schema.Struct({
    title: Schema.String,
    labels: Schema.Array(Schema.String)
  }),
  version: 1
})

export const AddLabel = Mutation.make("Task.AddLabel", {
  document: Task,
  payload: Schema.String
})

export const AddLabelLive = AddLabel.toLayer(({ draft, payload }) => {
  draft.labels.push(payload)
  return undefined
})

const Labels = Projection.make("Labels", {
  document: Task,
  version: 1,
  Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, label: Schema.String }),
  key: (row) => `${row.sourceDocumentId}:${row.label}`,
  project: (snapshot) => snapshot.value.labels.map((label) => ({ sourceDocumentId: snapshot.documentId, label }))
})

export const LabelsSql = SqlProjection.make(Labels, {
  table: "task_labels_v1",
  migrations: [{
    id: 1,
    name: "task_labels_v1",
    run: (sql, table) =>
      sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
        sourceDocumentId TEXT NOT NULL,
        label TEXT NOT NULL
      )`.pipe(Effect.asVoid)
  }],
  deleteByDocument: (sql, table, documentId) =>
    sql`DELETE FROM ${sql(table)} WHERE sourceDocumentId = ${documentId}`.pipe(Effect.asVoid),
  insert: (sql, table, row) =>
    sql`INSERT INTO ${sql(table)} (sourceDocumentId, label)
      VALUES (${row.sourceDocumentId}, ${row.label})`.pipe(Effect.asVoid)
})

export const ListLabels = Query.make("ListLabels", {
  payload: { prefix: Schema.String },
  success: Schema.Array(Labels.Row),
  dependsOn: [Labels]
})

export const definition = ReplicaDefinition.make({
  name: "react-native-fixture",
  documents: DocumentSet.make(Task),
  mutations: [AddLabel],
  projections: [Labels],
  queries: [ListLabels]
})

const listLabels = SqlSchema.findAll({
  Request: ListLabels.payloadSchema,
  Result: Labels.Row,
  execute: ({ prefix }) =>
    SqlClient.SqlClient.use((sql) =>
      sql`SELECT sourceDocumentId, label FROM task_labels_v1
      WHERE label LIKE ${`${prefix}%`} ORDER BY label`
    )
})

export const ListLabelsLive = ListLabels.toLayer((request) => listLabels(request).pipe(Effect.orDie))

export const limits: ReplicaLimits.Values = {
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
  maxPeerSendMillis: 30_000,
  maxSyncChangesPerMessage: 1_000,
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
  maxQueuedRpc: 1_024,
  maxQueuedPermits: 1_024,
  maxActiveRestores: 1_024,
  maxRestoresPerSession: 128,
  maxRestoreMillis: 30_000,
  maxRestorePullMillis: 10_000,
  maxRestoreCoalesceMillis: 25,
  maxRestoreErrorBytes: 4_096
}
