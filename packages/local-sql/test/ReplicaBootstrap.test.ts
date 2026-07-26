import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
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
      assert.strictEqual(result.failure.reason._tag as string, "ReplicaMetadataMissing")
      const generations = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_writer_generations
      `
      assert.strictEqual(generations[0]?.count, 1)
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("rejects a surviving history rewrite marker when metadata is absent", () =>
    Effect.gen(function*() {
      const first = yield* ReplicaBootstrap.make(definition)
      const sql = yield* SqlClient.SqlClient
      yield* sql`INSERT INTO effect_local_history_rewrites (
        replica_incarnation, operation_id, document_id, lineage, rewritten_at
      ) VALUES (
        ${first.incarnation}, 'rewrite-operation', 'document-1', 'lineage-1', '2026-01-01T00:00:00.000Z'
      )`
      const marker = yield* sql`SELECT * FROM effect_local_history_rewrites`
      yield* sql`DELETE FROM effect_local_writer_generations`
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`

      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (!Result.isFailure(result) || result.failure._tag !== "ReplicaError") return
      assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
      assert.deepStrictEqual(yield* sql`SELECT * FROM effect_local_history_rewrites`, marker)
      assert.deepStrictEqual(yield* sql`SELECT * FROM effect_local_metadata`, [])
      assert.deepStrictEqual(yield* sql`SELECT * FROM effect_local_writer_generations`, [])
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
      assert.strictEqual(result.failure.reason._tag as string, "ReplicaMetadataMissing")

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

  it.effect("opens empty migration 1 storage without peer tables", () =>
    Effect.gen(function*() {
      yield* Migrator.make({})({
        loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, 1)),
        table: "effect_local_migrations"
      })
      const state = yield* ReplicaBootstrap.make(definition)
      assert.strictEqual(state.writerGeneration, 1)
    }).pipe(
      Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
    ))

  it.effect("rejects peer only migration 2 storage before migrating", () =>
    Effect.forEach(["receipt", "outbox"] as const, (fixture) =>
      Effect.gen(function*() {
        const sql = yield* SqlClient.SqlClient
        yield* sql`PRAGMA foreign_keys = OFF`
        assert.strictEqual(
          (yield* sql<{ readonly foreign_keys: number }>`PRAGMA foreign_keys`)[0]?.foreign_keys,
          0
        )
        yield* Migrator.make({})({
          loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, 2)),
          table: "effect_local_migrations"
        })
        const canonical = (yield* sql<{ readonly count: number }>`SELECT
          (SELECT COUNT(*) FROM effect_local_writer_generations) +
          (SELECT COUNT(*) FROM effect_local_documents) +
          (SELECT COUNT(*) FROM effect_local_changes) +
          (SELECT COUNT(*) FROM effect_local_checkpoints) +
          (SELECT COUNT(*) FROM effect_local_command_receipts) +
          (SELECT COUNT(*) FROM effect_local_projection_registry) +
          (SELECT COUNT(*) FROM effect_local_document_projections) +
          (SELECT COUNT(*) FROM effect_local_commit_outbox) +
          (SELECT COUNT(*) FROM effect_local_quarantine) +
          (SELECT COUNT(*) FROM effect_local_backup_installations) AS count`)[0]!.count
        assert.strictEqual(canonical, 0)
        if (fixture === "receipt") {
          yield* sql`INSERT INTO effect_local_peer_receipts (
            replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
            message_hash, reply, reply_hash, pending_message, heads, accepted_heads,
            commit_sequence, accepted_at
          ) VALUES (
            0, 'peer_1', 'connection_1', 1, 'doc_missing', 'message_1',
            NULL, NULL, NULL, '[]', '[]', 1, '2026-01-01T00:00:00.000Z'
          )`
        } else {
          yield* sql`INSERT INTO effect_local_peer_outbox (
            replica_incarnation, peer_id, connection_epoch, document_id, send_sequence,
            message, message_hash, heads, status
          ) VALUES (
            0, 'peer_1', 'connection_1', 'doc_missing', 1,
            x'01', 'message_1', '[]', 'Pending'
          )`
        }
        const table = fixture === "receipt" ? "effect_local_peer_receipts" : "effect_local_peer_outbox"
        const before = fixture === "receipt"
          ? yield* sql<Readonly<Record<string, unknown>>>`SELECT * FROM effect_local_peer_receipts`
          : yield* sql<Readonly<Record<string, unknown>>>`SELECT * FROM effect_local_peer_outbox`

        const result = yield* Effect.result(ReplicaBootstrap.make(definition))
        assert.isTrue(Result.isFailure(result))
        if (!Result.isFailure(result) || result.failure._tag !== "ReplicaError") return
        assert.strictEqual(result.failure.reason._tag as string, "ReplicaMetadataMissing")
        const applied = yield* sql<{ readonly migration_id: number }>`
          SELECT migration_id FROM effect_local_migrations ORDER BY migration_id
        `
        assert.deepStrictEqual(applied.map((row) => row.migration_id), [1, 2])
        const after = table === "effect_local_peer_receipts"
          ? yield* sql<Readonly<Record<string, unknown>>>`SELECT * FROM effect_local_peer_receipts`
          : yield* sql<Readonly<Record<string, unknown>>>`SELECT * FROM effect_local_peer_outbox`
        assert.deepStrictEqual(after, before)
        const outboxColumns = yield* sql<{ readonly name: string }>`PRAGMA table_info(effect_local_peer_outbox)`
        assert.notInclude(outboxColumns.map((column) => column.name), "created_at")
        assert.deepStrictEqual(
          (yield* sql<{ readonly migration_id: number }>`
            SELECT migration_id FROM effect_local_migration_catalog ORDER BY migration_id
          `).map((row) => row.migration_id),
          [1, 2]
        )
        assert.strictEqual(
          (yield* sql<{ readonly count: number }>`SELECT
            (SELECT COUNT(*) FROM effect_local_metadata) +
            (SELECT COUNT(*) FROM effect_local_writer_generations) AS count`)[0]!.count,
          0
        )
      }).pipe(
        Effect.provide(Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer))
      ), { discard: true }))

  it.effect("serializes peer validation with migrations", () =>
    Effect.scoped(Effect.gen(function*() {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() => mkdtempSync(join(tmpdir(), "effect-local-bootstrap-"))),
        (path) => Effect.sync(() => rmSync(path, { force: true, recursive: true }))
      )
      const filename = join(directory, "replica.sqlite")
      const atMigration = yield* Deferred.make<void>()
      const releaseMigration = yield* Latch.make()
      let armed = false
      const pausingClient = Layer.effect(
        SqlClient.SqlClient,
        Effect.map(SqlClient.SqlClient, (sql) =>
          new Proxy(sql, {
            apply(target, thisArg, args: Array<unknown>) {
              const statement = Reflect.apply(target as never, thisArg, args) as Effect.Effect<
                unknown,
                unknown,
                never
              >
              const strings = args[0]
              if (!armed || !Array.isArray(strings)) return statement
              const text = (strings as ReadonlyArray<string>).join("?").replace(/\s+/g, " ").trim()
              if (
                !text.includes("CREATE TABLE IF NOT EXISTS ?") ||
                !text.includes("migration_id integer PRIMARY KEY") ||
                !text.includes("created_at datetime")
              ) {
                return statement
              }
              armed = false
              return Deferred.succeed(atMigration, undefined).pipe(
                Effect.andThen(releaseMigration.await),
                Effect.andThen(statement)
              )
            }
          }) as typeof sql)
      )
      const databaseA = Layer.merge(
        pausingClient.pipe(Layer.provide(SqliteClient.layer({ filename }))),
        NodeCrypto.layer
      )
      const databaseB = Layer.merge(SqliteClient.layer({ filename }), NodeCrypto.layer)
      const contextA = yield* Layer.build(databaseA)
      const contextB = yield* Layer.build(databaseB)
      const sqlA = Context.get(contextA, SqlClient.SqlClient)
      const sqlB = Context.get(contextB, SqlClient.SqlClient)
      yield* Migrator.make({})({
        loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, 2)),
        table: "effect_local_migrations"
      }).pipe(Effect.provide(contextA))
      yield* sqlB`PRAGMA foreign_keys = OFF`
      armed = true
      const opening = yield* ReplicaBootstrap.make(definition).pipe(
        Effect.provide(contextA),
        Effect.result,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(atMigration)
      yield* sqlB`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
        message_hash, reply, reply_hash, pending_message, heads, accepted_heads,
        commit_sequence, accepted_at
      ) VALUES (
        0, 'peer_1', 'connection_1', 1, 'doc_missing', 'message_1',
        NULL, NULL, NULL, '[]', '[]', 1, '2026-01-01T00:00:00.000Z'
      )`
      yield* releaseMigration.open
      const raced = yield* Fiber.join(opening).pipe(Effect.ensuring(Fiber.interrupt(opening)))
      assert.isTrue(Result.isFailure(raced))
      assert.deepStrictEqual(
        (yield* sqlB<{ readonly migration_id: number }>`
          SELECT migration_id FROM effect_local_migrations ORDER BY migration_id
        `).map((row) => row.migration_id),
        [1, 2]
      )
      assert.strictEqual(
        (yield* sqlB<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_local_peer_receipts
        `)[0]!.count,
        1
      )
      const retried = yield* Effect.result(ReplicaBootstrap.make(definition).pipe(Effect.provide(contextB)))
      assert.isTrue(Result.isFailure(retried))
      if (Result.isFailure(retried) && retried.failure._tag === "ReplicaError") {
        assert.strictEqual(retried.failure.reason._tag as string, "ReplicaMetadataMissing")
      }
      assert.strictEqual(
        (yield* sqlA<{ readonly count: number }>`SELECT COUNT(*) AS count FROM effect_local_metadata`)[0]!.count,
        0
      )
    })))

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
