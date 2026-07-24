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

  const seedProjectionState = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* sql`INSERT INTO effect_local_documents (
      document_id, document_type, schema_version, observed_versions,
      materialized_heads, accepted_heads, tombstone, projection_status, checkpoint_hash
    ) VALUES ('doc-1', 'Task', 1, '[1]', '["head"]', '["head"]', 0, 'Ready', NULL)`
    yield* sql`INSERT INTO labels_checksum (source_document_id, label) VALUES ('doc-1', 'seed')`
    yield* sql`INSERT INTO effect_local_document_projections (
      document_id, projection_name, projected_heads, status
    ) VALUES ('doc-1', 'Labels', '["head"]', 'Ready')`
  })

  const readProjectionState = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const rows = yield* sql<{ readonly label: string }>`SELECT label FROM labels_checksum ORDER BY label`
    const documents = yield* sql<{ readonly count: number }>`
      SELECT COUNT(*) AS count FROM effect_local_document_projections
      WHERE projection_name = 'Labels'
    `
    const registry = yield* sql<{
      readonly schema_checksum: string
      readonly status: string
      readonly table_name: string
      readonly projection_version: number
    }>`SELECT schema_checksum, status, table_name, projection_version
      FROM effect_local_projection_registry WHERE projection_name = 'Labels'`
    return {
      rows,
      documentProjectionCount: documents[0]?.count,
      registry: registry[0]
    }
  })

  it.effect("keeps all projection state when only documentation annotations change", () => {
    const plain = makeBinding(Schema.String)
    const documented = makeBinding(Schema.String.pipe(Schema.annotate({ description: "The label" })))

    return Effect.gen(function*() {
      yield* bootstrapStore(plain)
      yield* seedProjectionState
      const before = yield* readProjectionState
      yield* bootstrapStore(documented)
      const after = yield* readProjectionState
      assert.deepStrictEqual(after, before)
      assert.deepStrictEqual(after.rows, [{ label: "seed" }])
      assert.strictEqual(after.documentProjectionCount, 1)
      assert.strictEqual(after.registry?.status, "Ready")
    }).pipe(Effect.provide(Base))
  })

  it.effect("invalidates all projection state when the row type changes", () => {
    const plain = makeBinding(Schema.String)
    const codec = makeBinding(Schema.NumberFromString)

    return Effect.gen(function*() {
      yield* bootstrapStore(plain)
      yield* seedProjectionState
      const before = yield* readProjectionState
      yield* bootstrapStore(codec)
      const after = yield* readProjectionState
      assert.deepStrictEqual(after.rows, [])
      assert.strictEqual(after.documentProjectionCount, 0)
      assert.notStrictEqual(after.registry?.schema_checksum, before.registry?.schema_checksum)
      assert.strictEqual(after.registry?.status, "Rebuilding")
      assert.strictEqual(after.registry?.table_name, "labels_checksum")
      assert.strictEqual(after.registry?.projection_version, 1)
    }).pipe(Effect.provide(Base))
  })

  it.effect("keeps projection state when only the row encoding changes", () => {
    const base64 = makeBinding(Schema.StringFromBase64)
    const hex = makeBinding(Schema.StringFromHex)

    return Effect.gen(function*() {
      yield* bootstrapStore(base64)
      yield* seedProjectionState
      const before = yield* readProjectionState
      yield* bootstrapStore(hex)
      assert.deepStrictEqual(yield* readProjectionState, before)
    }).pipe(Effect.provide(Base))
  })

  it.effect("keeps projection state when only constructor behavior changes", () => {
    const plain = makeBinding(Schema.Literal("seed"))
    const defaulted = makeBinding(Schema.tagDefaultOmit("seed"))

    return Effect.gen(function*() {
      yield* bootstrapStore(plain)
      yield* seedProjectionState
      const before = yield* readProjectionState
      yield* bootstrapStore(defaulted)
      assert.deepStrictEqual(yield* readProjectionState, before)
    }).pipe(Effect.provide(Base))
  })

  it.effect("rolls back checksum invalidation when registry reconciliation fails", () => {
    const plain = makeBinding(Schema.String)
    const codec = makeBinding(Schema.NumberFromString)

    return Effect.gen(function*() {
      yield* bootstrapStore(plain)
      yield* seedProjectionState
      const before = yield* readProjectionState
      const sql = yield* SqlClient.SqlClient
      yield* sql`CREATE TRIGGER reject_checksum_update
        BEFORE UPDATE OF schema_checksum ON effect_local_projection_registry
        BEGIN SELECT RAISE(ABORT, 'blocked'); END`
      const exit = yield* Effect.exit(bootstrapStore(codec))
      assert.strictEqual(exit._tag, "Failure")
      assert.deepStrictEqual(yield* readProjectionState, before)
    }).pipe(Effect.provide(Base))
  })
})
