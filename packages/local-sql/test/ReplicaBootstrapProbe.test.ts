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
import { populatedTables, tables } from "../src/internal/schema.js"
import * as Migrations from "../src/Migrations.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"

/**
 * The populated probe decides whether a database with no metadata singleton is a fresh replica or a
 * corrupt one. Both mistakes are expensive and they pull in opposite directions: answering "fresh"
 * destroys durable state and mints a new identity over it, and answering "corrupt" is unrecoverable,
 * because nothing in this package can put the singleton back.
 *
 * These tests hold both sides at once.
 */
describe("ReplicaBootstrap populated probe", () => {
  const Task = Document.make("Task", { schema: Schema.String, version: 1 })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)

  const runPartialMigrations = (count: number) =>
    Migrator.make({})({
      loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, count)),
      table: "effect_local_migrations"
    })

  // The one shape an interrupted first launch can leave behind. The identity insert and the writer
  // generation insert share a transaction, so there is no half-written identity - only "migrations
  // 1..k committed, no singleton". Every rung has to recover, and the ladder also walks the probe
  // across 10, 12, 13 and 18 existing tables as later migrations add them.
  for (let count = 1; count <= 11; count++) {
    it.effect(`still mints an identity after only ${count} migration(s) committed`, () =>
      Effect.gen(function*() {
        yield* runPartialMigrations(count)
        const state = yield* ReplicaBootstrap.make(definition)
        assert.strictEqual(state.incarnation, 0)
        assert.strictEqual(state.writerGeneration, 1)
        assert.strictEqual(state.definitionHash, definition.hash)
      }).pipe(Effect.provide(Database)))
  }

  // The probe selects table names by the declared list rather than by a name pattern, so a table this
  // package does not own cannot make a database look populated. Without that, the cluster message
  // store alone would brick every replica that had ever accepted a command.
  it.effect("cannot be driven into rejecting by a table outside the declared list", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.run
      yield* sql`CREATE TABLE effect_local_cluster_messages (id INTEGER PRIMARY KEY)`
      yield* sql`INSERT INTO effect_local_cluster_messages (id) VALUES (1)`
      yield* sql`CREATE TABLE effect_local_rogue (id INTEGER PRIMARY KEY)`
      yield* sql`INSERT INTO effect_local_rogue (id) VALUES (1)`

      const state = yield* ReplicaBootstrap.make(definition)
      assert.strictEqual(state.incarnation, 0)
      assert.strictEqual(state.writerGeneration, 1)
    }).pipe(Effect.provide(Database)))

  it.effect("reopens a replica whose documents were all deleted", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const first = yield* ReplicaBootstrap.make(definition)
      yield* sql`DELETE FROM effect_local_documents`
      const second = yield* ReplicaBootstrap.make(definition)
      assert.strictEqual(second.replicaId, first.replicaId)
      assert.strictEqual(second.writerGeneration, 2)
    }).pipe(Effect.provide(Database)))

  // A single probed table takes `sql.join`'s one-clause branch, which emits no separator and no
  // parentheses. Built by hand so the intersection really is one table.
  it.effect("produces valid SQL when exactly one durable table exists", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* sql`CREATE TABLE effect_local_metadata (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        storage_format_version INTEGER NOT NULL
      )`
      yield* sql`CREATE TABLE effect_local_documents (document_id TEXT PRIMARY KEY)`
      yield* sql`INSERT INTO effect_local_documents (document_id) VALUES ('doc_1')`

      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (!Result.isFailure(result)) return
      assert.strictEqual(result.failure._tag, "ReplicaError")
      if (result.failure._tag !== "ReplicaError") return
      assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
      if (result.failure.reason._tag !== "ReplicaMetadataMissing") return
      assert.strictEqual(result.failure.reason.operation, "ReplicaBootstrap.probe")
    }).pipe(Effect.provide(Database)))

  // Seeded in the LAST probed table on purpose: a UNION ALL chain that lost its tail would still pass
  // if the row were in the first one.
  it.effect("rejects a fully migrated replica holding only a relay outbox replica usage row", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.run
      yield* sql`PRAGMA foreign_keys = OFF`
      yield* sql`INSERT INTO effect_local_peer_relay_outbox_replica_usage (
        replica_incarnation, message_count, encoded_bytes
      ) VALUES (0, 1, 1)`
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`

      const result = yield* Effect.result(ReplicaBootstrap.make(definition))
      assert.isTrue(Result.isFailure(result))
      if (!Result.isFailure(result)) return
      assert.strictEqual(result.failure._tag, "ReplicaError")
      if (result.failure._tag !== "ReplicaError") return
      assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
    }).pipe(Effect.provide(Database)))

  // Structural rather than an enumeration, so a migration that adds a table fails here instead of
  // silently escaping the probe. This is the test that stops the two lists drifting again.
  it.effect("probes every durable table that exists after the last migration", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.run
      const existing = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'effect\\_local\\_%' ESCAPE '\\'
        ORDER BY name`
      // The migrator owns this one, so this package's schema does not declare it.
      const owned = existing.map((row) => row.name).filter((name) => name !== "effect_local_migrations")

      const declared = new Set<string>(tables)
      const probed = new Set(populatedTables)
      const excluded = new Set([
        "effect_local_metadata",
        "effect_local_migration_catalog",
        "effect_local_command_delivery_control"
      ])

      assert.deepStrictEqual(owned.filter((name) => !declared.has(name)), [])
      assert.deepStrictEqual(owned.filter((name) => !excluded.has(name) && !probed.has(name)), [])
    }).pipe(Effect.provide(Database)))
})
