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

describe("ProjectionStore coverage probes", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ labels: Schema.Array(Schema.String) }),
    version: 1
  })
  const Labels = Projection.make("Labels", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, label: Schema.String }),
    key: (row) => row.sourceDocumentId,
    project: (snapshot) => snapshot.value.labels.map((label) => ({ sourceDocumentId: snapshot.documentId, label }))
  })
  const migration = {
    id: 1,
    name: "labels_v1",
    run: (sql: SqlClient.SqlClient, table: string) =>
      sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
        source_document_id TEXT NOT NULL,
        label TEXT NOT NULL
      )`.pipe(Effect.asVoid)
  }
  const deleteByDocument = (sql: SqlClient.SqlClient, table: string, documentId: Identity.DocumentId) =>
    sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid)
  const insert = (sql: SqlClient.SqlClient, table: string, row: { sourceDocumentId: string; label: string }) =>
    sql`INSERT INTO ${sql(table)} (source_document_id, label)
      VALUES (${row.sourceDocumentId}, ${row.label})`.pipe(Effect.asVoid)
  const LabelsSql = SqlProjection.make(Labels, {
    table: "cov_labels_v1",
    migrations: [migration],
    deleteByDocument,
    insert
  })
  const OtherLabels = Projection.make("OtherLabels", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, label: Schema.String }),
    key: (row) => row.sourceDocumentId,
    project: (snapshot) => snapshot.value.labels.map((label) => ({ sourceDocumentId: snapshot.documentId, label }))
  })
  const OtherLabelsSql = SqlProjection.make(OtherLabels, {
    table: "cov_labels_v1",
    migrations: [migration],
    deleteByDocument,
    insert
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

  it.effect("reconciles a projection whose registered schema checksum no longer matches for rebuild", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* sql`INSERT INTO effect_local_projection_registry (
        projection_name, table_name, projection_version, schema_checksum, status
      ) VALUES ('Labels', 'cov_labels_v1', 1, 'stale-checksum', 'Ready')`
      yield* ProjectionStore.ProjectionStore.pipe(
        Effect.provide(ProjectionStore.layer([LabelsSql]).pipe(Layer.provide(LabelsSql.layer)))
      )
      const registry = yield* sql<{ readonly schema_checksum: string; readonly status: string }>`
        SELECT schema_checksum, status FROM effect_local_projection_registry WHERE projection_name = 'Labels'
      `
      assert.strictEqual(registry[0]?.status, "Rebuilding")
      assert.notStrictEqual(registry[0]?.schema_checksum, "stale-checksum")
    }).pipe(Effect.provide(Base)))

  it.effect("blocks construction when two projections target the same table", () =>
    Effect.gen(function*() {
      const error = yield* Effect.flip(
        ProjectionStore.ProjectionStore.pipe(
          Effect.provide(
            ProjectionStore.layer([LabelsSql, OtherLabelsSql]).pipe(
              Layer.provide(Layer.merge(LabelsSql.layer, OtherLabelsSql.layer))
            )
          )
        )
      )
      assert.strictEqual(error.reason._tag, "ProjectionBlocked")
    }).pipe(Effect.provide(Base)))
})
