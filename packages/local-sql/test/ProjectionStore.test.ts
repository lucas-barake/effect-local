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
