import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as TestReplica from "@lucas-barake/effect-local-test/TestReplica"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"

export const Task = Document.make("ApiTourTask", {
  schema: Schema.Struct({
    title: Schema.String,
    completed: Schema.Boolean,
    labels: Schema.Array(Schema.String)
  }),
  version: 1
})

export class TitleEmpty extends Schema.TaggedErrorClass<TitleEmpty>("@effect-local/api-tour/TitleEmpty")(
  "TitleEmpty",
  {}
) {}

export class UnboundedTaskQuery extends Schema.TaggedErrorClass<UnboundedTaskQuery>(
  "@effect-local/api-tour/UnboundedTaskQuery"
)("UnboundedTaskQuery", {}) {}

export const RenameTask = Mutation.make("ApiTourTask.Rename", {
  document: Task,
  payload: Schema.String,
  success: Schema.String,
  error: TitleEmpty
})

export const SetCompleted = Mutation.make("ApiTourTask.SetCompleted", {
  document: Task,
  payload: Schema.Boolean
})

export const AddLabel = Mutation.make("ApiTourTask.AddLabel", {
  document: Task,
  payload: Schema.String
})

export const TaskList = Projection.make("ApiTourTaskList", {
  document: Task,
  version: 1,
  Row: Schema.Struct({
    sourceDocumentId: Identity.DocumentId,
    title: Schema.String,
    state: Schema.Literals(["open", "done"]),
    labelCount: Schema.Int
  }),
  key: (row) => row.sourceDocumentId,
  project: (snapshot) =>
    snapshot.tombstone
      ? []
      : [{
        sourceDocumentId: snapshot.documentId,
        title: snapshot.value.title,
        state: snapshot.value.completed ? "done" as const : "open" as const,
        labelCount: snapshot.value.labels.length
      }]
})

export const ListTasks = Query.make("ApiTourListTasks", {
  payload: Schema.Struct({ state: Schema.NullOr(Schema.Literals(["open", "done"])) }),
  success: Schema.Array(TaskList.Row),
  error: UnboundedTaskQuery,
  dependsOn: [TaskList]
})

export const definition = ReplicaDefinition.make({
  name: "api-tour",
  documents: DocumentSet.make(Task),
  mutations: [RenameTask, SetCompleted, AddLabel],
  projections: [TaskList],
  queries: [ListTasks]
})

export const TaskListSql = SqlProjection.make(TaskList, {
  table: "api_tour_task_list_v1",
  migrations: [{
    id: 1,
    name: "api_tour_task_list_v1",
    run: (sql, table) =>
      sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
        source_document_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        state TEXT NOT NULL,
        label_count INTEGER NOT NULL
      )`.pipe(Effect.asVoid)
  }],
  deleteByDocument: (sql, table, documentId) =>
    sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
  insert: (sql, table, row) =>
    sql`INSERT INTO ${sql(table)} (source_document_id, title, state, label_count)
      VALUES (${row.sourceDocumentId}, ${row.title}, ${row.state}, ${row.labelCount})`.pipe(Effect.asVoid)
})

const ListTasksSql = SqlSchema.findAll({
  Request: ListTasks.payloadSchema,
  Result: TaskList.Row,
  execute: ({ state }) =>
    SqlClient.SqlClient.use((sql) =>
      sql`SELECT
        source_document_id AS sourceDocumentId,
        title,
        state,
        label_count AS labelCount
      FROM api_tour_task_list_v1
      WHERE ${state} IS NULL OR state = ${state}
      ORDER BY title, source_document_id`
    )
})

export const MutationLive = Layer.mergeAll(
  RenameTask.toLayer(({ draft, payload }) => {
    const title = payload.trim()
    if (title.length === 0) return Result.fail(new TitleEmpty())
    draft.title = title
    return Result.succeed(title)
  }),
  SetCompleted.toLayer(({ draft, payload }) => {
    draft.completed = payload
    return undefined
  }),
  AddLabel.toLayer(({ draft, payload }) => {
    draft.labels.push(payload)
    return undefined
  })
)

const DatabaseLive = SqliteClient.layer({ filename: ":memory:", disableWAL: true })

const QueryLive = ListTasks.toLayer((request) =>
  request.state === null
    ? Effect.fail(new UnboundedTaskQuery())
    : ListTasksSql(request).pipe(Effect.orDie)
).pipe(
  Layer.provide(DatabaseLive)
)

export const EngineLive = SqlReplica.layerWithBindings(definition, { projections: [TaskListSql] }).pipe(
  Layer.provide(Layer.mergeAll(
    DatabaseLive,
    NodeCrypto.layer,
    MutationLive,
    QueryLive
  )),
  Layer.provideMerge(ReplicaLimits.layer(TestReplica.defaultLimits))
)

const TestQueryLive = ListTasks.toLayer(() => Effect.succeed([]))

export const InMemoryTestLive = TestReplica.layer(definition, { projections: [TaskListSql] }).pipe(
  Layer.provide(Layer.mergeAll(MutationLive, TestQueryLive, TaskListSql.layer))
)

export const SyncTestLive = TestReplica.layerWithSync(definition, { projections: [TaskListSql] }).pipe(
  Layer.provide(Layer.mergeAll(MutationLive, TestQueryLive, TaskListSql.layer))
)
