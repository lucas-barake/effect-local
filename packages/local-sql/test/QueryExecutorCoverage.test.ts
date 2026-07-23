import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as QueryExecutor from "../src/QueryExecutor.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as SqlProjection from "../src/SqlProjection.js"

describe("QueryExecutor coverage probes", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ labels: Schema.Array(Schema.String) }),
    version: 1
  })
  const Labels = Projection.make("Labels", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, label: Schema.String }),
    key: (row) => `${row.sourceDocumentId}:${row.label}`,
    project: (snapshot) => snapshot.value.labels.map((label) => ({ sourceDocumentId: snapshot.documentId, label }))
  })
  const LabelsSql = SqlProjection.make(Labels, {
    table: "query_cov_labels_v1",
    migrations: [{
      id: 1,
      name: "query_cov_labels_v1",
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
  const ListLabels = Query.make("ListLabels", {
    payload: { prefix: Schema.String },
    success: Schema.Array(Labels.Row),
    dependsOn: [Labels]
  })
  const definition = ReplicaDefinition.make({
    name: "query-tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [Labels],
    queries: [ListLabels]
  })
  const listLabels = SqlSchema.findAll({
    Request: ListLabels.payloadSchema,
    Result: Labels.Row,
    execute: ({ prefix }) =>
      SqlClient.SqlClient.use((sql) =>
        sql`SELECT sourceDocumentId, label FROM query_cov_labels_v1
        WHERE label LIKE ${`${prefix}%`} ORDER BY label`
      )
  })
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Reactive = Reactivity.layer
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Base = Layer.merge(Database, Bootstrap)
  const Projections = ProjectionStore.layer([LabelsSql]).pipe(Layer.provide(Layer.merge(Base, LabelsSql.layer)))
  const Handler = ListLabels.toLayer((request) => listLabels(request).pipe(Effect.orDie)).pipe(Layer.provide(Database))
  const Executor = QueryExecutor.layer(definition).pipe(Layer.provide(Layer.mergeAll(Database, Handler, Reactive)))
  const Live = Layer.mergeAll(Base, Projections, Handler, Reactive, Executor)

  it.effect("blocks a query when a document projection row is not ready", () =>
    Effect.gen(function*() {
      const executor = yield* QueryExecutor.QueryExecutor
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions,
        materialized_heads, accepted_heads, tombstone, projection_status, checkpoint_hash
      ) VALUES (${documentId}, ${Task.name}, 1, '[1]', '["h"]', '["h"]', 0, 'Ready', NULL)`
      yield* sql`INSERT INTO effect_local_document_projections (
        document_id, projection_name, projected_heads, status
      ) VALUES (${documentId}, ${Labels.name}, '["h"]', 'Rebuilding')`
      const error = yield* Effect.flip(executor.execute(ListLabels, { prefix: "" }))
      assert.strictEqual(error.reason._tag, "ProjectionBlocked")
    }).pipe(Effect.provide(Live)))
})
