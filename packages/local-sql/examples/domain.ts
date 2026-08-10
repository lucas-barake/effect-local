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
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"

const Title = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160))

export const TaskDocument = Document.make("Task", {
  schema: Schema.Struct({
    title: Title,
    completed: Schema.Boolean,
    createdAt: Schema.Number,
    updatedAt: Schema.Number
  }),
  version: 1
})

export class TitleEmpty extends Schema.TaggedErrorClass<TitleEmpty>()("TitleEmpty", {}) {}

export const RenameTask = Mutation.make("RenameTask", {
  document: TaskDocument,
  payload: { title: Schema.String },
  error: TitleEmpty
})

export const SetTaskCompleted = Mutation.make("SetTaskCompleted", {
  document: TaskDocument,
  payload: { completed: Schema.Boolean }
})

export const TaskRow = Schema.Struct({
  sourceDocumentId: Identity.DocumentId,
  title: Schema.String,
  completed: Schema.Boolean,
  updatedAt: Schema.Number
})

export class ListTasksError extends Schema.TaggedErrorClass<ListTasksError>(
  "@lucas-barake/effect-local/examples/ListTasksError"
)("ListTasksError", {
  reason: Schema.Literals(["StorageUnavailable", "StorageCorrupt"])
}) {}

export const TaskList = Projection.make("TaskList", {
  document: TaskDocument,
  version: 1,
  Row: TaskRow,
  key: (row) => row.sourceDocumentId,
  project: (snapshot) => [{
    sourceDocumentId: snapshot.documentId,
    title: snapshot.value.title,
    completed: snapshot.value.completed,
    updatedAt: snapshot.value.updatedAt
  }]
})

export const ListTasks = Query.make("ListTasks", {
  payload: { search: Schema.String },
  success: Schema.Array(TaskRow),
  error: ListTasksError,
  dependsOn: [TaskList]
})

export const Documents = DocumentSet.make(TaskDocument)

export const definition = ReplicaDefinition.make({
  name: "tasks",
  documents: Documents,
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
        updated_at REAL NOT NULL
      )`.pipe(Effect.asVoid)
  }],
  deleteByDocument: (sql, table, documentId) =>
    sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
  insert: (sql, table, row) =>
    sql`INSERT INTO ${sql(table)} (
      source_document_id, title, completed, updated_at
    ) VALUES (
      ${row.sourceDocumentId}, ${row.title}, ${
      (() => {
        if (row.completed) return (1)
        return (0)
      })()
    }, ${row.updatedAt}
    )`.pipe(Effect.asVoid)
})

const ListTasksSql = SqlSchema.findAll({
  Request: ListTasks.payloadSchema,
  Result: Schema.Struct({
    ...TaskRow.fields,
    completed: Schema.BooleanFromBit
  }),
  execute: ({ search }) => {
    const pattern = `%${search.trim().toLocaleLowerCase()}%`
    return SqlClient.SqlClient.use((sql) =>
      sql`SELECT source_document_id AS sourceDocumentId, title, completed, updated_at AS updatedAt
          FROM task_list_v1
          WHERE ${pattern} = '%%' OR LOWER(title) LIKE ${pattern}
          ORDER BY updated_at DESC`
    )
  }
})

export const DomainLive = Layer.mergeAll(
  RenameTask.toLayer(({ draft, payload }) => {
    const title = payload.title.trim()
    if (title.length === 0) return Result.fail(new TitleEmpty())
    draft.title = title
    draft.updatedAt = Effect.runSync(Clock.currentTimeMillis)
    return Result.void
  }),
  SetTaskCompleted.toLayer(({ draft, payload }) => {
    draft.completed = payload.completed
    draft.updatedAt = Effect.runSync(Clock.currentTimeMillis)
    return undefined
  }),
  ListTasks.toLayer((payload) =>
    ListTasksSql(payload).pipe(
      Effect.catchTags({
        SqlError: () => Effect.fail(new ListTasksError({ reason: "StorageUnavailable" })),
        SchemaError: () => Effect.fail(new ListTasksError({ reason: "StorageCorrupt" }))
      })
    )
  )
)

export const limits: ReplicaLimits.Values = {
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
