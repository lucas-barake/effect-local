import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as SqlProjection from "../src/SqlProjection.js"
import * as SqlReplica from "../src/SqlReplica.js"

const limits: ReplicaLimits.Values = {
  maxBackupBytes: 1024 * 1024,
  maxChunkBytes: 64 * 1024,
  maxArchiveRecords: 1000,
  maxJsonDepth: 32,
  maxSyncMessageBytes: 64 * 1024,
  maxPeerSendMillis: 1_000,
  maxSyncChangesPerMessage: 100,
  maxSyncDependencyEdgesPerMessage: 1000,
  maxSyncOperationsPerMessage: 10_000,
  maxPendingBytesPerDocument: 1024 * 1024,
  maxPendingBytesPerPeer: 1024 * 1024,
  maxPendingBytesPerReplica: 1024 * 1024,
  maxPendingAgeMillis: 60_000,
  maxPendingChangesPerDocument: 1000,
  maxPendingChangesPerPeer: 1000,
  maxPendingChangesPerReplica: 1000,
  maxPendingDependencyEdgesPerDocument: 10_000,
  maxPendingDependencyEdgesPerPeer: 10_000,
  maxPendingDependencyEdgesPerReplica: 10_000,
  maxSessions: 4,
  maxStreamsPerSession: 4,
  maxInFlightPerSession: 16,
  maxQueuedRpc: 64
}

const environment = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  ReplicaLimits.layer(limits)
)

const TaskV1 = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
const TaskV2 = Document.make("Task", {
  schema: Schema.Struct({ title: Schema.String, done: Schema.Boolean }),
  version: 2,
  migrations: [
    Document.migration({
      from: 1,
      schema: Schema.Struct({ title: Schema.String }),
      migrate: (value) => ({ ...value, done: false })
    })
  ]
})

describe("ReplicaEvolution", () => {
  describe("bootstrap gate", () => {
    it.effect("opens an existing replica after a query is added to the definition", () =>
      Effect.gen(function*() {
        const initial = ReplicaDefinition.make({
          name: "tasks",
          documents: DocumentSet.make(TaskV1)
        })
        const first = yield* ReplicaBootstrap.make(initial)
        const evolved = ReplicaDefinition.make({
          name: "tasks",
          documents: DocumentSet.make(TaskV1),
          queries: [Query.make("countTasks", { success: Schema.Int, dependsOn: [] })]
        })
        assert.notStrictEqual(evolved.hash, initial.hash)
        const second = yield* ReplicaBootstrap.make(evolved)
        assert.strictEqual(second.replicaId, first.replicaId)
        assert.strictEqual(second.definitionHash, evolved.hash)
        const sql = yield* SqlClient.SqlClient
        const metadata = yield* sql<{ readonly definition_hash: string }>`
          SELECT definition_hash FROM effect_local_metadata WHERE singleton = 1
        `
        assert.strictEqual(metadata[0]?.definition_hash, evolved.hash)
      }).pipe(Effect.provide(environment)))

    it.effect("opens a replica with stored documents when the version bump has a migration chain", () =>
      Effect.gen(function*() {
        const initial = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
        yield* ReplicaBootstrap.make(initial)
        const sql = yield* SqlClient.SqlClient
        yield* sql`INSERT INTO effect_local_documents (
          document_id, document_type, schema_version, observed_versions, materialized_heads,
          accepted_heads, tombstone, projection_status, checkpoint_hash
        ) VALUES ('doc_1', 'Task', 1, '[1]', '["first"]', '["first"]', 0, 'Ready', NULL)`
        const evolved = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV2) })
        const state = yield* ReplicaBootstrap.make(evolved)
        assert.strictEqual(state.definitionHash, evolved.hash)
      }).pipe(Effect.provide(environment)))

    it.effect("rejects a definition that removed a document type with stored rows", () =>
      Effect.gen(function*() {
        const initial = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
        yield* ReplicaBootstrap.make(initial)
        const sql = yield* SqlClient.SqlClient
        yield* sql`INSERT INTO effect_local_documents (
          document_id, document_type, schema_version, observed_versions, materialized_heads,
          accepted_heads, tombstone, projection_status, checkpoint_hash
        ) VALUES ('doc_1', 'Task', 1, '[1]', '["first"]', '["first"]', 0, 'Ready', NULL)`
        const Note = Document.make("Note", { schema: Schema.String, version: 1 })
        const evolved = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(Note) })
        const result = yield* Effect.result(ReplicaBootstrap.make(evolved))
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result) && result.failure._tag === "ReplicaError") {
          assert.strictEqual(result.failure.reason._tag, "ProtocolMismatch")
        }
        const metadata = yield* sql<{ readonly definition_hash: string }>`
          SELECT definition_hash FROM effect_local_metadata WHERE singleton = 1
        `
        assert.strictEqual(metadata[0]?.definition_hash, initial.hash)
      }).pipe(Effect.provide(environment)))

    it.effect("reopens under the previous definition after a rolled back deployment", () =>
      Effect.gen(function*() {
        const initial = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
        yield* ReplicaBootstrap.make(initial)
        const evolved = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV2) })
        yield* ReplicaBootstrap.make(evolved)
        const state = yield* ReplicaBootstrap.make(initial)
        assert.strictEqual(state.definitionHash, initial.hash)
      }).pipe(Effect.provide(environment)))
  })

  describe("startup migration", () => {
    it.effect("migrates stored documents when reopening under an evolved definition", () =>
      Effect.gen(function*() {
        const definitionV1 = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
        const definitionV2 = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV2) })
        const commandId = yield* Identity.makeCommandId
        const documentId = Identity.documentIdFromCommandId(commandId)
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(SqlReplica.layerWithBindings(definitionV1, { projections: [] }))
          const replica = Context.get(context, Replica.Replica)
          yield* replica.create(TaskV1, { commandId, value: { title: "write tests" } })
        }))
        const sql = yield* SqlClient.SqlClient
        const before = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_changes`
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(SqlReplica.layerWithBindings(definitionV2, { projections: [] }))
          const replica = Context.get(context, Replica.Replica)
          const snapshot = yield* replica.get(TaskV2, documentId)
          assert.deepStrictEqual(snapshot.value, { title: "write tests", done: false })
          assert.strictEqual(snapshot.version, 2)
        }))
        const rows = yield* sql<{ readonly schema_version: number }>`
          SELECT schema_version FROM effect_local_documents WHERE document_id = ${documentId}
        `
        assert.strictEqual(rows[0]?.schema_version, 2)
        const after = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_changes`
        assert.strictEqual(after[0]!.count, before[0]!.count + 1)
        yield* Effect.scoped(Effect.gen(function*() {
          yield* Layer.build(SqlReplica.layerWithBindings(definitionV2, { projections: [] }))
        }))
        const settled = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_changes`
        assert.strictEqual(settled[0]!.count, after[0]!.count)
      }).pipe(Effect.provide(environment)))
  })

  describe("projection rebuild", () => {
    const projectionRow = Schema.Struct({ sourceDocumentId: Identity.DocumentId, title: Schema.String })
    const binding = (projection: Projection.Projection<string, typeof TaskV1, typeof projectionRow>, table: string) =>
      SqlProjection.make(projection, {
        table,
        migrations: [{
          id: 1,
          name: table,
          run: (sql, target) =>
            sql`CREATE TABLE IF NOT EXISTS ${sql(target)} (
              source_document_id TEXT PRIMARY KEY,
              title TEXT NOT NULL
            )`.pipe(Effect.asVoid)
        }],
        deleteByDocument: (sql, target, documentId) =>
          sql`DELETE FROM ${sql(target)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
        insert: (sql, target, row) =>
          sql`INSERT INTO ${sql(target)} (source_document_id, title)
            VALUES (${row.sourceDocumentId}, ${row.title})`.pipe(Effect.asVoid)
      })

    it.effect("populates a projection added over existing documents", () =>
      Effect.gen(function*() {
        const definitionV1 = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
        const commandId = yield* Identity.makeCommandId
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(SqlReplica.layerWithBindings(definitionV1, { projections: [] }))
          const replica = Context.get(context, Replica.Replica)
          yield* replica.create(TaskV1, { commandId, value: { title: "project me" } })
        }))
        const TaskTitle = Projection.make("TaskTitle", {
          document: TaskV1,
          version: 1,
          Row: projectionRow,
          key: (row) => row.sourceDocumentId,
          project: (snapshot) =>
            snapshot.tombstone ? [] : [{ sourceDocumentId: snapshot.documentId, title: snapshot.value.title }]
        })
        const evolved = ReplicaDefinition.make({
          name: "tasks",
          documents: DocumentSet.make(TaskV1),
          projections: [TaskTitle]
        })
        yield* Effect.scoped(Effect.gen(function*() {
          yield* Layer.build(
            SqlReplica.layerWithBindings(evolved, { projections: [binding(TaskTitle, "task_title_v1")] })
          )
        }))
        const sql = yield* SqlClient.SqlClient
        const rows = yield* sql<{ readonly title: string }>`SELECT title FROM task_title_v1`
        assert.deepStrictEqual(rows, [{ title: "project me" }])
        const registry = yield* sql<{ readonly status: string }>`
          SELECT status FROM effect_local_projection_registry WHERE projection_name = 'TaskTitle'
        `
        assert.strictEqual(registry[0]?.status, "Ready")
      }).pipe(Effect.provide(environment)))

    it.effect("rebuilds a projection whose version changed", () =>
      Effect.gen(function*() {
        const TaskTitle = Projection.make("TaskTitle", {
          document: TaskV1,
          version: 1,
          Row: projectionRow,
          key: (row) => row.sourceDocumentId,
          project: (snapshot) =>
            snapshot.tombstone ? [] : [{ sourceDocumentId: snapshot.documentId, title: snapshot.value.title }]
        })
        const initial = ReplicaDefinition.make({
          name: "tasks",
          documents: DocumentSet.make(TaskV1),
          projections: [TaskTitle]
        })
        const commandId = yield* Identity.makeCommandId
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(
            SqlReplica.layerWithBindings(initial, { projections: [binding(TaskTitle, "task_title_v1")] })
          )
          const replica = Context.get(context, Replica.Replica)
          yield* replica.create(TaskV1, { commandId, value: { title: "loud" } })
        }))
        const TaskTitleV2 = Projection.make("TaskTitle", {
          document: TaskV1,
          version: 2,
          Row: projectionRow,
          key: (row) => row.sourceDocumentId,
          project: (snapshot) =>
            snapshot.tombstone
              ? []
              : [{ sourceDocumentId: snapshot.documentId, title: snapshot.value.title.toUpperCase() }]
        })
        const evolved = ReplicaDefinition.make({
          name: "tasks",
          documents: DocumentSet.make(TaskV1),
          projections: [TaskTitleV2]
        })
        yield* Effect.scoped(Effect.gen(function*() {
          yield* Layer.build(
            SqlReplica.layerWithBindings(evolved, { projections: [binding(TaskTitleV2, "task_title_v2")] })
          )
        }))
        const sql = yield* SqlClient.SqlClient
        const rows = yield* sql<{ readonly title: string }>`SELECT title FROM task_title_v2`
        assert.deepStrictEqual(rows, [{ title: "LOUD" }])
        const registry = yield* sql<{ readonly status: string; readonly table_name: string }>`
          SELECT status, table_name FROM effect_local_projection_registry WHERE projection_name = 'TaskTitle'
        `
        assert.deepStrictEqual(registry, [{ status: "Ready", table_name: "task_title_v2" }])
      }).pipe(Effect.provide(environment)))
  })
})
