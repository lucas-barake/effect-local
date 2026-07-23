import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
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
import * as DocumentStore from "../src/DocumentStore.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaEvolution from "../src/ReplicaEvolution.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
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
        const sql = yield* SqlClient.SqlClient
        yield* sql`INSERT INTO effect_local_documents (
          document_id, document_type, schema_version, observed_versions, materialized_heads,
          accepted_heads, tombstone, projection_status, checkpoint_hash
        ) VALUES ('doc_1', 'Task', 1, '[1]', '["first"]', '["first"]', 0, 'Ready', NULL)`
        const evolved = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV2) })
        yield* ReplicaBootstrap.make(evolved)
        const state = yield* ReplicaBootstrap.make(initial)
        assert.strictEqual(state.definitionHash, initial.hash)
      }).pipe(Effect.provide(environment)))

    it.effect("accepts stored rows at multiple migratable versions of one type", () =>
      Effect.gen(function*() {
        const TaskV3 = Document.make("Task", {
          schema: Schema.Struct({ title: Schema.String, done: Schema.Boolean, priority: Schema.Int }),
          version: 3,
          migrations: [
            Document.migration({
              from: 1,
              schema: Schema.Struct({ title: Schema.String }),
              migrate: (value) => ({ ...value, done: false })
            }),
            Document.migration({
              from: 2,
              schema: Schema.Struct({ title: Schema.String, done: Schema.Boolean }),
              migrate: (value) => ({ ...value, priority: 0 })
            })
          ]
        })
        const initial = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
        yield* ReplicaBootstrap.make(initial)
        const sql = yield* SqlClient.SqlClient
        yield* sql`INSERT INTO effect_local_documents (
          document_id, document_type, schema_version, observed_versions, materialized_heads,
          accepted_heads, tombstone, projection_status, checkpoint_hash
        ) VALUES ('doc_1', 'Task', 1, '[1]', '["first"]', '["first"]', 0, 'Ready', NULL)`
        yield* sql`INSERT INTO effect_local_documents (
          document_id, document_type, schema_version, observed_versions, materialized_heads,
          accepted_heads, tombstone, projection_status, checkpoint_hash
        ) VALUES ('doc_2', 'Task', 2, '[2]', '["second"]', '["second"]', 0, 'Ready', NULL)`
        const evolved = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV3) })
        const state = yield* ReplicaBootstrap.make(evolved)
        assert.strictEqual(state.definitionHash, evolved.hash)
      }).pipe(Effect.provide(environment)))

    it.effect("rejects stored rows whose oldest version predates the migration chain", () =>
      Effect.gen(function*() {
        const TaskV3Partial = Document.make("Task", {
          schema: Schema.Struct({ title: Schema.String, done: Schema.Boolean, priority: Schema.Int }),
          version: 3,
          migrations: [
            Document.migration({
              from: 2,
              schema: Schema.Struct({ title: Schema.String, done: Schema.Boolean }),
              migrate: (value) => ({ ...value, priority: 0 })
            })
          ]
        })
        const initial = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
        yield* ReplicaBootstrap.make(initial)
        const sql = yield* SqlClient.SqlClient
        yield* sql`INSERT INTO effect_local_documents (
          document_id, document_type, schema_version, observed_versions, materialized_heads,
          accepted_heads, tombstone, projection_status, checkpoint_hash
        ) VALUES ('doc_1', 'Task', 1, '[1]', '["first"]', '["first"]', 0, 'Ready', NULL)`
        yield* sql`INSERT INTO effect_local_documents (
          document_id, document_type, schema_version, observed_versions, materialized_heads,
          accepted_heads, tombstone, projection_status, checkpoint_hash
        ) VALUES ('doc_2', 'Task', 2, '[2]', '["second"]', '["second"]', 0, 'Ready', NULL)`
        const evolved = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV3Partial) })
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

    it.effect("skips an unrecoverable document durably while migrating the healthy ones", () =>
      Effect.gen(function*() {
        const definitionV1 = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
        const definitionV2 = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV2) })
        const commandId = yield* Identity.makeCommandId
        const healthyId = Identity.documentIdFromCommandId(commandId)
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(SqlReplica.layerWithBindings(definitionV1, { projections: [] }))
          const replica = Context.get(context, Replica.Replica)
          yield* replica.create(TaskV1, { commandId, value: { title: "survives" } })
        }))
        const sql = yield* SqlClient.SqlClient
        const corruptId = "doc_00000000-0000-4000-8000-00000000c0de"
        yield* sql`INSERT INTO effect_local_documents (
          document_id, document_type, schema_version, observed_versions, materialized_heads,
          accepted_heads, tombstone, projection_status, checkpoint_hash
        ) VALUES (${corruptId}, 'Task', 1, '[1]', '["ghost"]', '["ghost"]', 0, 'Ready', NULL)`
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(SqlReplica.layerWithBindings(definitionV2, { projections: [] }))
          const replica = Context.get(context, Replica.Replica)
          const snapshot = yield* replica.get(TaskV2, healthyId)
          assert.deepStrictEqual(snapshot.value, { title: "survives", done: false })
        }))
        const rows = yield* sql<{
          readonly document_id: string
          readonly projection_status: string
          readonly schema_version: number
        }>`
          SELECT document_id, projection_status, schema_version FROM effect_local_documents
        `
        const corrupt = rows.find((row) => row.document_id === corruptId)
        const healthy = rows.find((row) => row.document_id === healthyId)
        assert.strictEqual(corrupt?.projection_status, "Blocked")
        assert.strictEqual(corrupt?.schema_version, 1)
        assert.strictEqual(healthy?.projection_status, "Ready")
        assert.strictEqual(healthy?.schema_version, 2)
      }).pipe(Effect.provide(environment)))

    it.effect("reports per-type migration counts across multiple documents and types", () =>
      Effect.gen(function*() {
        const NoteV1 = Document.make("Note", { schema: Schema.Struct({ body: Schema.String }), version: 1 })
        const NoteV2 = Document.make("Note", {
          schema: Schema.Struct({ body: Schema.String, pinned: Schema.Boolean }),
          version: 2,
          migrations: [
            Document.migration({
              from: 1,
              schema: Schema.Struct({ body: Schema.String }),
              migrate: (value) => ({ ...value, pinned: false })
            })
          ]
        })
        const definitionV1 = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1, NoteV1) })
        const definitionV2 = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV2, NoteV2) })
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(SqlReplica.layerWithBindings(definitionV1, { projections: [] }))
          const replica = Context.get(context, Replica.Replica)
          const first = yield* Identity.makeCommandId
          const second = yield* Identity.makeCommandId
          const third = yield* Identity.makeCommandId
          yield* replica.create(TaskV1, { commandId: first, value: { title: "one" } })
          yield* replica.create(TaskV1, { commandId: second, value: { title: "two" } })
          yield* replica.create(NoteV1, { commandId: third, value: { body: "keep" } })
        }))
        const bootstrap = ReplicaBootstrap.layer(definitionV2)
        const gate = ReplicaGate.layer.pipe(Layer.provideMerge(bootstrap))
        const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
        const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
        const projections = ProjectionStore.layer([]).pipe(Layer.provideMerge(store))
        const state = yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(projections)
          return yield* ReplicaEvolution.make(definitionV2).pipe(Effect.provide(context))
        }))
        const counts = state.migratedDocuments.toSorted((a, b) => a.documentType.localeCompare(b.documentType))
        assert.deepStrictEqual(counts, [
          { documentType: "Note", count: 1 },
          { documentType: "Task", count: 2 }
        ])
        assert.deepStrictEqual(state.rebuiltProjections, [])
        const sql = yield* SqlClient.SqlClient
        const stale = yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_local_documents WHERE schema_version = 1
        `
        assert.strictEqual(stale[0]?.count, 0)
      }).pipe(Effect.provide(environment)))

    it.effect("mutating a migrated document produces a consistent commit", () =>
      Effect.gen(function*() {
        const SetDone = Mutation.make("SetDone", { document: TaskV2 })
        const definitionV1 = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
        const definitionV2 = ReplicaDefinition.make({
          name: "tasks",
          documents: DocumentSet.make(TaskV2),
          mutations: [SetDone]
        })
        const createId = yield* Identity.makeCommandId
        const documentId = Identity.documentIdFromCommandId(createId)
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(SqlReplica.layerWithBindings(definitionV1, { projections: [] }))
          const replica = Context.get(context, Replica.Replica)
          yield* replica.create(TaskV1, { commandId: createId, value: { title: "mutate me" } })
        }))
        const sql = yield* SqlClient.SqlClient
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(
            SqlReplica.layerWithBindings(definitionV2, { projections: [] }).pipe(
              Layer.provide(SetDone.toLayer(({ draft }) => {
                draft.done = true
                return undefined
              }))
            )
          )
          const replica = Context.get(context, Replica.Replica)
          const before = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_changes`
          const mutateId = yield* Identity.makeCommandId
          yield* replica.mutate(SetDone, { commandId: mutateId, documentId })
          const snapshot = yield* replica.get(TaskV2, documentId)
          assert.deepStrictEqual(snapshot.value, { title: "mutate me", done: true })
          const after = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_changes`
          assert.strictEqual(after[0]!.count, before[0]!.count + 1)
        }))
      }).pipe(Effect.provide(environment)))
  })

  describe("fencing", () => {
    it.effect("does not commit registry writes after the replica was superseded", () =>
      Effect.gen(function*() {
        const projectionRow = Schema.Struct({ sourceDocumentId: Identity.DocumentId, title: Schema.String })
        const TaskTitle = Projection.make("TaskTitle", {
          document: TaskV1,
          version: 1,
          Row: projectionRow,
          key: (row) => row.sourceDocumentId,
          project: (snapshot) =>
            snapshot.tombstone ? [] : [{ sourceDocumentId: snapshot.documentId, title: snapshot.value.title }]
        })
        const TaskTitleSql = SqlProjection.make(TaskTitle, {
          table: "task_title_fencing",
          migrations: [{
            id: 1,
            name: "task_title_fencing",
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
        const definition = ReplicaDefinition.make({
          name: "tasks",
          documents: DocumentSet.make(TaskV1),
          projections: [TaskTitle]
        })
        const sql = yield* SqlClient.SqlClient
        const bootstrap = ReplicaBootstrap.layer(definition)
        const gate = ReplicaGate.layer.pipe(Layer.provideMerge(bootstrap))
        const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
        const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
        const projections = ProjectionStore.layer([TaskTitleSql]).pipe(
          Layer.provideMerge(store),
          Layer.provide(TaskTitleSql.layer)
        )
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(projections)
          yield* sql`UPDATE effect_local_projection_registry SET status = 'Rebuilding'
            WHERE projection_name = 'TaskTitle'`
          yield* sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1
            WHERE singleton = 1`
          const result = yield* Effect.result(ReplicaEvolution.make(definition).pipe(Effect.provide(context)))
          assert.isTrue(Result.isFailure(result))
          if (Result.isFailure(result) && result.failure._tag === "ReplicaError") {
            assert.strictEqual(result.failure.reason._tag, "ReplicaFenced")
          }
          const registry = yield* sql<{ readonly status: string }>`
            SELECT status FROM effect_local_projection_registry WHERE projection_name = 'TaskTitle'
          `
          assert.strictEqual(registry[0]?.status, "Rebuilding")
        }))
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

    it.effect("removes registry rows for a projection dropped from the definition", () =>
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
            SqlReplica.layerWithBindings(initial, { projections: [binding(TaskTitle, "task_title_dropped")] })
          )
          const replica = Context.get(context, Replica.Replica)
          yield* replica.create(TaskV1, { commandId, value: { title: "orphaned" } })
        }))
        const sql = yield* SqlClient.SqlClient
        const seeded = yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_local_projection_registry WHERE projection_name = 'TaskTitle'
        `
        assert.strictEqual(seeded[0]?.count, 1)
        const evolved = ReplicaDefinition.make({ name: "tasks", documents: DocumentSet.make(TaskV1) })
        yield* Effect.scoped(Effect.gen(function*() {
          yield* Layer.build(SqlReplica.layerWithBindings(evolved, { projections: [] }))
        }))
        const registry = yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_local_projection_registry WHERE projection_name = 'TaskTitle'
        `
        const documentProjections = yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_local_document_projections WHERE projection_name = 'TaskTitle'
        `
        assert.strictEqual(registry[0]?.count, 0)
        assert.strictEqual(documentProjections[0]?.count, 0)
      }).pipe(Effect.provide(environment)))

    it.effect("does not clobber an unrelated commit's outbox row when re-projecting", () =>
      Effect.gen(function*() {
        const Note = Document.make("Note", { schema: Schema.Struct({ body: Schema.String }), version: 1 })
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
          documents: DocumentSet.make(TaskV1, Note),
          projections: [TaskTitle]
        })
        const taskId = yield* Identity.makeCommandId
        const noteId = yield* Identity.makeCommandId
        yield* Effect.scoped(Effect.gen(function*() {
          const context = yield* Layer.build(
            SqlReplica.layerWithBindings(initial, { projections: [binding(TaskTitle, "task_title_v1")] })
          )
          const replica = Context.get(context, Replica.Replica)
          yield* replica.create(TaskV1, { commandId: taskId, value: { title: "rebuild me" } })
          yield* replica.create(Note, { commandId: noteId, value: { body: "unrelated" } })
        }))
        const sql = yield* SqlClient.SqlClient
        const noteDocumentId = Identity.documentIdFromCommandId(noteId)
        yield* sql`UPDATE effect_local_commit_outbox SET published = 0 WHERE document_id = ${noteDocumentId}`
        const before = yield* sql<{ readonly invalidation_keys: string }>`
          SELECT invalidation_keys FROM effect_local_commit_outbox WHERE document_id = ${noteDocumentId}
        `
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
          documents: DocumentSet.make(TaskV1, Note),
          projections: [TaskTitleV2]
        })
        yield* Effect.scoped(Effect.gen(function*() {
          yield* Layer.build(
            SqlReplica.layerWithBindings(evolved, { projections: [binding(TaskTitleV2, "task_title_v2")] })
          )
        }))
        const after = yield* sql<{ readonly invalidation_keys: string }>`
          SELECT invalidation_keys FROM effect_local_commit_outbox WHERE document_id = ${noteDocumentId}
        `
        assert.deepStrictEqual(after, before)
      }).pipe(Effect.provide(environment)))
  })
})
