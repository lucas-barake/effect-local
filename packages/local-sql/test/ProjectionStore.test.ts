import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as SqlProjection from "../src/SqlProjection.js"

describe("ProjectionStore", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ labels: Schema.Array(Schema.String) }),
    version: 1
  })
  const Labels = Projection.make("Labels", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, label: Schema.String }),
    key: (row) => row.sourceDocumentId,
    project: (snapshot) =>
      snapshot.value.labels.map((label) => ({
        sourceDocumentId: snapshot.documentId,
        label
      }))
  })
  const LabelsSql = SqlProjection.make(Labels, {
    table: "task_labels_v1",
    migrations: [{
      id: 1,
      name: "task_labels_v1",
      run: (sql, table) =>
        sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
        source_document_id TEXT NOT NULL,
        label TEXT NOT NULL CHECK (label != 'invalid')
      )`.pipe(Effect.asVoid)
    }],
    deleteByDocument: (sql, table, documentId) =>
      sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
    insert: (sql, table, row) =>
      sql`INSERT INTO ${sql(table)} (source_document_id, label)
      VALUES (${row.sourceDocumentId}, ${row.label})`.pipe(Effect.asVoid)
  })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [Labels],
    queries: []
  })
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Base = Layer.merge(Database, Bootstrap)
  const Store = ProjectionStore.layer([LabelsSql]).pipe(Layer.provide(Layer.merge(Base, LabelsSql.layer)))
  const Live = Layer.merge(Base, Store)

  it("rejects projection migrations that cannot be scheduled", () => {
    const migration = (id: number, name: string): SqlProjection.Migration => ({
      id,
      name,
      run: () => Effect.void
    })
    const makeBinding = (migrations: ReadonlyArray<SqlProjection.Migration>) =>
      SqlProjection.make(Labels, {
        table: "invalid_projection_migrations",
        migrations,
        deleteByDocument: () => Effect.void,
        insert: () => Effect.void
      })

    assert.throws(() => makeBinding([]), TypeError)

    for (
      const id of [
        0,
        -1,
        1.5,
        Number.NaN,
        Number.POSITIVE_INFINITY,
        Number.MAX_SAFE_INTEGER + 1
      ]
    ) {
      assert.throws(() => makeBinding([migration(id, "invalid")]), TypeError)
    }

    assert.throws(() => makeBinding([migration(1, "")]), TypeError)
  })

  it.effect("translates projection migration execution failures", () => {
    const BrokenLabelsSql = SqlProjection.make(Labels, {
      table: "broken_task_labels_v1",
      migrations: [{
        id: 1,
        name: "broken",
        run: (sql) => sql`SELECT * FROM definitely_missing_projection_table`.pipe(Effect.asVoid)
      }],
      deleteByDocument: () => Effect.void,
      insert: () => Effect.void
    })
    const BrokenStore = ProjectionStore.layer([BrokenLabelsSql]).pipe(
      Layer.provide(Layer.merge(Base, BrokenLabelsSql.layer))
    )

    return Effect.gen(function*() {
      const error = yield* ProjectionStore.ProjectionStore.pipe(
        Effect.provide(BrokenStore),
        Effect.flip
      )
      assert.strictEqual(error._tag, "ReplicaError")
      if (error._tag === "ReplicaError") {
        assert.strictEqual(error.reason._tag, "ProjectionBlocked")
        if (error.reason._tag === "ProjectionBlocked") {
          assert.strictEqual(error.reason.projection, Labels.name)
        }
      }
    })
  })

  it.effect("rolls back earlier binding setup when a later binding is rejected", () => {
    const Titles = Projection.make("Titles", {
      document: Task,
      version: 1,
      Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId }),
      key: (row) => row.sourceDocumentId,
      project: (snapshot) => [{ sourceDocumentId: snapshot.documentId }]
    })
    const BrokenTitlesSql = SqlProjection.make(Titles, {
      table: "broken_task_titles_v1",
      migrations: [{
        id: 1,
        name: "broken",
        run: (sql) => sql`SELECT * FROM definitely_missing_projection_table`.pipe(Effect.asVoid)
      }],
      deleteByDocument: () => Effect.void,
      insert: () => Effect.void
    })
    const PartialStore = ProjectionStore.layer([LabelsSql, BrokenTitlesSql]).pipe(
      Layer.provide(Layer.merge(LabelsSql.layer, BrokenTitlesSql.layer))
    )

    return Effect.gen(function*() {
      const error = yield* ProjectionStore.ProjectionStore.pipe(
        Effect.provide(PartialStore),
        Effect.flip
      )
      assert.strictEqual(error._tag, "ReplicaError")
      if (error._tag === "ReplicaError") {
        assert.strictEqual(error.reason._tag, "ProjectionBlocked")
        if (error.reason._tag === "ProjectionBlocked") {
          assert.strictEqual(error.reason.projection, Titles.name)
        }
      }

      const sql = yield* SqlClient.SqlClient
      const tables = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (
          'task_labels_v1', 'task_labels_v1_effect_sql_migrations'
        )
      `
      assert.strictEqual(tables.length, 0)
      const registry = yield* sql<{ readonly projection_name: string }>`
        SELECT projection_name FROM effect_local_projection_registry
      `
      assert.strictEqual(registry.length, 0)
    }).pipe(Effect.provide(Base))
  })

  it.effect("validates all rows before replacing a source document", () =>
    Effect.gen(function*() {
      const store = yield* ProjectionStore.ProjectionStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions,
        materialized_heads, accepted_heads, tombstone, projection_status, checkpoint_hash
      ) VALUES (${documentId}, 'Task', 1, '[1]', '["first"]', '["first"]', 0, 'Ready', NULL)`
      yield* store.replace(LabelsSql, {
        documentId,
        value: { labels: ["one"] },
        version: 1,
        heads: ["first"],
        tombstone: false,
        projection: "Ready"
      }, LabelsSql.table)
      const failed = yield* Effect.exit(store.replace(LabelsSql, {
        documentId,
        value: { labels: ["two", "duplicate"] },
        version: 1,
        heads: ["second"],
        tombstone: false,
        projection: "Ready"
      }, LabelsSql.table))
      assert.strictEqual(failed._tag, "Failure")
      const rows = yield* sql<{ readonly label: string }>`SELECT label FROM task_labels_v1`
      assert.deepStrictEqual(rows, [{ label: "one" }])
    }).pipe(Effect.provide(Live)))

  it.effect("rolls back a projection replacement when an insert fails", () =>
    Effect.gen(function*() {
      const store = yield* ProjectionStore.ProjectionStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions,
        materialized_heads, accepted_heads, tombstone, projection_status, checkpoint_hash
      ) VALUES (${documentId}, 'Task', 1, '[1]', '["first"]', '["first"]', 0, 'Ready', NULL)`
      yield* store.replace(LabelsSql, {
        documentId,
        value: { labels: ["preserved"] },
        version: 1,
        heads: ["first"],
        tombstone: false,
        projection: "Ready"
      }, LabelsSql.table)

      const failed = yield* Effect.exit(store.replace(LabelsSql, {
        documentId,
        value: { labels: ["invalid"] },
        version: 1,
        heads: ["second"],
        tombstone: false,
        projection: "Ready"
      }, LabelsSql.table))
      assert.strictEqual(failed._tag, "Failure")
      const rows = yield* sql<{ readonly label: string }>`SELECT label FROM task_labels_v1`
      assert.deepStrictEqual(rows, [{ label: "preserved" }])
    }).pipe(Effect.provide(Live)))
})
