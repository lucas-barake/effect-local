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

describe("ProjectionStore checksum", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ labels: Schema.Array(Schema.String) }),
    version: 1
  })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task)
  })
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Base = Layer.merge(Database, Bootstrap)

  const makeBinding = (label: Document.WireSchema) => {
    const projection = Projection.make("Labels", {
      document: Task,
      version: 1,
      Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, label }),
      key: (row) => String(row.sourceDocumentId),
      project: (snapshot) =>
        snapshot.value.labels.map((value) => ({
          sourceDocumentId: snapshot.documentId,
          label: value
        }))
    })
    return SqlProjection.make(projection, {
      table: "labels_checksum",
      migrations: [{
        id: 1,
        name: "labels_checksum",
        run: (sql, table) =>
          sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
            source_document_id TEXT NOT NULL,
            label TEXT NOT NULL
          )`.pipe(Effect.asVoid)
      }],
      deleteByDocument: (sql, table, documentId) =>
        sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
      insert: (sql, table, row) =>
        sql`INSERT INTO ${sql(table)} (source_document_id, label)
        VALUES (${row.sourceDocumentId}, ${row.label})`.pipe(Effect.asVoid)
    })
  }

  const bootstrapStore = (binding: SqlProjection.SqlProjection<Projection.Any>) =>
    ProjectionStore.ProjectionStore.pipe(
      Effect.provide(ProjectionStore.layer([binding]).pipe(Layer.provide(binding.layer))),
      Effect.asVoid
    )

  it.effect("keeps projection data when only documentation annotations change", () => {
    const plain = makeBinding(Schema.String)
    const documented = makeBinding(Schema.String.pipe(Schema.annotate({ description: "The label" })))

    return Effect.gen(function*() {
      yield* bootstrapStore(plain)
      const sql = yield* SqlClient.SqlClient
      yield* sql`INSERT INTO labels_checksum (source_document_id, label) VALUES ('doc-1', 'keep')`
      yield* bootstrapStore(documented)
      const rows = yield* sql<{ readonly label: string }>`SELECT label FROM labels_checksum`
      assert.strictEqual(rows.length, 1)
    }).pipe(Effect.provide(Base))
  })

  it.effect("invalidates projection data when the row codec changes", () => {
    const plain = makeBinding(Schema.String)
    const codec = makeBinding(Schema.NumberFromString)

    return Effect.gen(function*() {
      yield* bootstrapStore(plain)
      const sql = yield* SqlClient.SqlClient
      yield* sql`INSERT INTO labels_checksum (source_document_id, label) VALUES ('doc-1', 'stale')`
      yield* bootstrapStore(codec)
      const rows = yield* sql<{ readonly label: string }>`SELECT label FROM labels_checksum`
      assert.strictEqual(rows.length, 0)
    }).pipe(Effect.provide(Base))
  })
})
