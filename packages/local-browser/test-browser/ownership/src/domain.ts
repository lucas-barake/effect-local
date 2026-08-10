import * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"

const Title = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160))

const completedToBit = (completed: boolean) => {
  if (completed) return 1
  return 0
}

export const TaskDocument = Document.make("Task", {
  schema: Schema.Struct({
    title: Title,
    completed: Schema.Boolean,
    createdAt: Schema.Number,
    updatedAt: Schema.Number
  }),
  version: 1
})

export const RenameTask = Mutation.make("RenameTask", {
  document: TaskDocument,
  payload: { title: Title }
})

export const SetTaskCompleted = Mutation.make("SetTaskCompleted", {
  document: TaskDocument,
  payload: { completed: Schema.Boolean }
})

const TaskRow = Schema.Struct({
  sourceDocumentId: Identity.DocumentId,
  title: Title,
  completed: Schema.Boolean,
  createdAt: Schema.Number,
  updatedAt: Schema.Number
})

export type TaskRow = typeof TaskRow.Type

export const TaskList = Projection.make("TaskList", {
  document: TaskDocument,
  version: 1,
  Row: TaskRow,
  key: (row) => row.sourceDocumentId,
  project: (snapshot) => [{ sourceDocumentId: snapshot.documentId, ...snapshot.value }]
})

export const ListTasks = Query.make("ListTasks", {
  payload: {
    filter: Schema.Literals(["all", "active", "completed"]),
    search: Schema.String
  },
  success: Schema.Array(TaskRow),
  dependsOn: [TaskList]
})

export const definition = ReplicaDefinition.make({
  name: "local-tasks",
  documents: DocumentSet.make(TaskDocument),
  mutations: [RenameTask, SetTaskCompleted],
  projections: [TaskList],
  queries: [ListTasks]
})

export const TaskListSql = SqlProjection.make(TaskList, {
  table: "task_list_v1",
  migrations: [{
    id: 1,
    name: "task_list_v1",
    run: (sql, table) =>
      sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
        source_document_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        completed INTEGER NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
      )`.pipe(Effect.asVoid)
  }],
  deleteByDocument: (sql, table, documentId) =>
    sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
  insert: (sql, table, row) =>
    sql`INSERT INTO ${sql(table)} (
      source_document_id, title, completed, created_at, updated_at
    ) VALUES (
        ${row.sourceDocumentId}, ${row.title}, ${completedToBit(row.completed)}, ${row.createdAt}, ${row.updatedAt}
    )`.pipe(Effect.asVoid)
})

const ListTasksSql = SqlSchema.findAll({
  Request: ListTasks.payloadSchema,
  Result: Schema.Struct({
    ...TaskRow.fields,
    completed: Schema.BooleanFromBit
  }),
  execute: (payload) => {
    const search = `%${payload.search.trim().toLocaleLowerCase()}%`
    return SqlClient.SqlClient.use((sql) =>
      sql`SELECT
        source_document_id AS sourceDocumentId,
        title,
        completed,
        created_at AS createdAt,
        updated_at AS updatedAt
      FROM task_list_v1
      WHERE
        (${payload.filter} = 'all' OR (${payload.filter} = 'active' AND completed = 0) OR
          (${payload.filter} = 'completed' AND completed = 1))
        AND (${search} = '%%' OR LOWER(title) LIKE ${search})
      ORDER BY completed ASC, created_at DESC`
    )
  }
})

export const DomainLive = Layer.mergeAll(
  RenameTask.toLayer(Effect.gen(function*() {
    const clock = yield* Clock.Clock
    return ({ draft, payload }) => {
      draft.title = payload.title
      draft.updatedAt = clock.currentTimeMillisUnsafe()
      return undefined
    }
  })),
  SetTaskCompleted.toLayer(Effect.gen(function*() {
    const clock = yield* Clock.Clock
    return ({ draft, payload }) => {
      draft.completed = payload.completed
      draft.updatedAt = clock.currentTimeMillisUnsafe()
      return undefined
    }
  })),
  ListTasks.toLayer((payload) => ListTasksSql(payload).pipe(Effect.orDie))
)

export const limits: ReplicaLimits.Values = {
  maxBackupBytes: 32 * 1024 * 1024,
  maxChunkBytes: 256 * 1024,
  maxArchiveRecords: 100_000,
  maxJsonDepth: 32,
  maxConflictDepth: 32,
  maxConflictNodes: 100_000,
  maxConflictAlternatives: 10_000,
  maxConflictPathSegments: 128,
  maxConflictValueBytes: 16 * 1024 * 1024,
  maxConflictSourceChanges: 100_000,
  maxConflictSourceOperations: 100_000,
  maxConflictSourceBytes: 64 * 1024 * 1024,
  maxSyncMessageBytes: 256 * 1024,
  maxPeerSendMillis: 15_000,
  maxSyncChangesPerMessage: 500,
  maxSyncDependencyEdgesPerMessage: 5_000,
  maxSyncOperationsPerMessage: 50_000,
  maxPendingBytesPerDocument: 4 * 1024 * 1024,
  maxPendingBytesPerPeer: 16 * 1024 * 1024,
  maxPendingBytesPerReplica: 32 * 1024 * 1024,
  maxPendingAgeMillis: 7 * 24 * 60 * 60 * 1000,
  maxPendingChangesPerDocument: 5_000,
  maxPendingChangesPerPeer: 20_000,
  maxPendingChangesPerReplica: 50_000,
  maxPendingDependencyEdgesPerDocument: 25_000,
  maxPendingDependencyEdgesPerPeer: 100_000,
  maxPendingDependencyEdgesPerReplica: 250_000,
  maxSessions: 16,
  maxStreamsPerSession: 16,
  maxInFlightPerSession: 64,
  maxQueuedRpc: 256,
  maxQueuedPermits: 256,
  maxActiveRestores: 256,
  maxRestoresPerSession: 64,
  maxRestoreMillis: 30_000,
  maxRestorePullMillis: 10_000,
  maxRestoreCoalesceMillis: 25,
  maxRestoreErrorBytes: 4_096
}
