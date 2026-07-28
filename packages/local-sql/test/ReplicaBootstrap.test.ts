import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as Migrations from "../src/Migrations.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"

describe("ReplicaBootstrap", () => {
  const Task = Document.make("Task", { schema: Schema.String, version: 1 })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })

  it.effect("runs migrations before atomically claiming writer generations", () =>
    Effect.gen(function*() {
      const first = yield* ReplicaBootstrap.make(definition)
      const second = yield* ReplicaBootstrap.make(definition)
      assert.strictEqual(first.replicaId, second.replicaId)
      assert.strictEqual(first.writerGeneration, 1)
      assert.strictEqual(second.writerGeneration, 2)
      assert.strictEqual(second.incarnation, 0)
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("reports a newer storage format version without migrating", () =>
    Effect.gen(function*() {
      const first = yield* ReplicaBootstrap.make(definition)
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_metadata SET storage_format_version = 2 WHERE singleton = 1`
      // a build that refuses to open the replica must not have migrated it on the way to refusing:
      // a stale catalog turns any migration attempt into a MigrationError instead of this failure
      yield* sql`DELETE FROM effect_local_migration_catalog WHERE migration_id = 6`
      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (!Result.isFailure(result)) return
      assert.strictEqual(result.failure._tag, "ReplicaError")
      if (result.failure._tag !== "ReplicaError") return
      assert.strictEqual(result.failure.reason._tag, "UnsupportedStorageFormatVersion")
      if (result.failure.reason._tag !== "UnsupportedStorageFormatVersion") return
      assert.strictEqual(result.failure.reason.observedVersion, 2)
      assert.strictEqual(result.failure.reason.supportedVersion, 1)
      // the rejected open must leave the replica exactly as the previous build left it
      const metadata = yield* sql<{ readonly definition_hash: string; readonly writer_generation: number }>`
        SELECT definition_hash, writer_generation FROM effect_local_metadata WHERE singleton = 1
      `
      assert.deepStrictEqual(metadata, [{
        definition_hash: definition.hash,
        writer_generation: first.writerGeneration
      }])
      const generations = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_writer_generations
      `
      assert.strictEqual(generations[0]?.count, 1)
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("reports an older storage format version without migrating", () =>
    Effect.gen(function*() {
      yield* ReplicaBootstrap.make(definition)
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_metadata SET storage_format_version = 0 WHERE singleton = 1`
      // the guard must reject both directions, so a one-sided "only refuse newer" check has to fail here
      yield* sql`DELETE FROM effect_local_migration_catalog WHERE migration_id = 6`
      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (!Result.isFailure(result)) return
      assert.strictEqual(result.failure._tag, "ReplicaError")
      if (result.failure._tag !== "ReplicaError") return
      assert.strictEqual(result.failure.reason._tag, "UnsupportedStorageFormatVersion")
      if (result.failure.reason._tag !== "UnsupportedStorageFormatVersion") return
      assert.strictEqual(result.failure.reason.observedVersion, 0)
      assert.strictEqual(result.failure.reason.supportedVersion, 1)
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("initializes a migrated replica that has no metadata row and no durable state", () =>
    Effect.gen(function*() {
      // a crash between the migration commit and the bootstrap transaction leaves schema but no metadata row.
      // the guard must treat that as a fresh replica, not as corruption, or first boot after a crash bricks it.
      yield* Migrations.run
      const sql = yield* SqlClient.SqlClient
      const before = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_metadata
      `
      assert.strictEqual(before[0]?.count, 0)
      const state = yield* ReplicaBootstrap.make(definition)
      assert.strictEqual(state.writerGeneration, 1)
      assert.strictEqual(state.definitionHash, definition.hash)
      const metadata = yield* sql<{ readonly storage_format_version: number }>`
        SELECT storage_format_version FROM effect_local_metadata WHERE singleton = 1
      `
      assert.deepStrictEqual(metadata, [{ storage_format_version: 1 }])
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("reports a corrupt storage format version as corrupt storage without migrating", () =>
    Effect.gen(function*() {
      yield* ReplicaBootstrap.make(definition)
      const sql = yield* SqlClient.SqlClient
      // SQLite INTEGER affinity stores a non-numeric value verbatim as TEXT
      yield* sql`UPDATE effect_local_metadata SET storage_format_version = 'nope' WHERE singleton = 1`
      yield* sql`DELETE FROM effect_local_migration_catalog WHERE migration_id = 6`
      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (!Result.isFailure(result)) return
      assert.strictEqual(result.failure._tag, "ReplicaError")
      if (result.failure._tag !== "ReplicaError") return
      assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("reports corrupt persisted identity through the typed error channel", () =>
    Effect.gen(function*() {
      yield* ReplicaBootstrap.make(definition)
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_metadata SET replica_id = 'invalid' WHERE singleton = 1`
      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result) && result.failure._tag === "ReplicaError") {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
      }
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("rejects missing metadata in a populated migrated database", () =>
    Effect.gen(function*() {
      yield* ReplicaBootstrap.make(definition)
      const sql = yield* SqlClient.SqlClient
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`
      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (!Result.isFailure(result)) return
      assert.strictEqual(result.failure._tag, "ReplicaError")
      if (result.failure._tag !== "ReplicaError") return
      assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
      const generations = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_writer_generations
      `
      assert.strictEqual(generations[0]?.count, 1)
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("does not migrate a populated replica whose metadata row is missing", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      // a replica an older build left at migration 2, holding real durability state but no metadata row
      yield* Migrator.make({})({
        loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, 2)),
        table: "effect_local_migrations"
      })
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions, materialized_heads,
        accepted_heads, tombstone, projection_status, checkpoint_hash
      ) VALUES ('doc_1', 'Task', 1, '[1]', '["first"]', '["first"]', 0, 'Ready', NULL)`
      yield* sql`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
        message_hash, reply, reply_hash, pending_message, heads, accepted_heads,
        commit_sequence, accepted_at
      ) VALUES (
        0, 'peer_1', 'connection_1', 1, 'doc_1', 'message_1',
        NULL, NULL, NULL, '[]', '[]', 1, '2026-01-01T00:00:00.000Z'
      )`

      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (!Result.isFailure(result)) return
      assert.strictEqual(result.failure._tag, "ReplicaError")
      if (result.failure._tag !== "ReplicaError") return
      assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")

      // a build that refuses to open the replica must not have migrated it on the way to refusing
      const applied = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_local_migrations ORDER BY migration_id
      `
      assert.deepStrictEqual(applied.map((row) => row.migration_id), [1, 2])
      const receipts = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_peer_receipts
      `
      assert.strictEqual(receipts[0]?.count, 1)
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("rejects an incompatible replica definition without modifying metadata", () =>
    Effect.gen(function*() {
      const first = yield* ReplicaBootstrap.make(definition)
      const sql = yield* SqlClient.SqlClient
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions, materialized_heads,
        accepted_heads, tombstone, projection_status, checkpoint_hash
      ) VALUES ('doc_1', 'Task', 1, '[1]', '["first"]', '["first"]', 0, 'Ready', NULL)`
      const TaskV2 = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 2 })
      const incompatible = ReplicaDefinition.make({
        name: "tasks",
        documents: DocumentSet.make(TaskV2),
        mutations: [],
        projections: [],
        queries: []
      })

      const result = yield* Effect.result(ReplicaBootstrap.make(incompatible))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result) && result.failure._tag === "ReplicaError") {
        assert.strictEqual(result.failure.reason._tag, "ProtocolMismatch")
      }
      const metadata = yield* sql<{ readonly definition_hash: string; readonly writer_generation: number }>`
        SELECT definition_hash, writer_generation FROM effect_local_metadata WHERE singleton = 1
      `
      assert.deepStrictEqual(metadata, [{
        definition_hash: definition.hash,
        writer_generation: first.writerGeneration
      }])
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))
})
