import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Migrations from "../src/Migrations.js"
import * as Domain from "./Domain.js"

const database = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const LedgerRow = Schema.Struct({ id: Schema.Number, name: Schema.String, checksum: Schema.String })
const NameRow = Schema.Struct({ name: Schema.String })
const CountRow = Schema.Struct({ count: Schema.Number })
const LegacyClientRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: Schema.NullOr(Schema.Number),
  entity_value: Schema.String,
  model_version: Schema.NullOr(Schema.Number)
})
const LegacyServerRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: Schema.NullOr(Schema.Number),
  entity_value: Schema.String,
  model_version: Schema.NullOr(Schema.Number)
})

const clientLedger = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: LedgerRow,
    execute: () => sql`SELECT id, name, checksum FROM effect_local_client_migrations ORDER BY id`
  })(undefined)

const tableNames = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: NameRow,
    execute: () => sql`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`
  })(undefined)

const probeCount = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Schema.Void,
    Result: CountRow,
    execute: () => sql`SELECT COUNT(*) AS count FROM migration_probe`
  })(undefined)

describe("storage migration catalogs", () => {
  it.effect("applies the complete client and server catalogs once", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.client({
        definition: Domain.definition,
        spaceId,
        clientId
      })
      yield* Migrations.server
      yield* Migrations.client({
        definition: Domain.definition,
        spaceId,
        clientId
      })

      assert.deepStrictEqual((yield* clientLedger(sql)).map((row) => row.id), [1, 2, 3, 4, 5])
      const names = (yield* tableNames(sql)).map((row) => row.name)
      assert.includeMembers(names, [
        "effect_local_client_evolution",
        "effect_local_client_key_lineage",
        "effect_local_client_key_lineage_groups",
        "effect_local_client_key_lineage_targets",
        "effect_local_client_shadow_entities",
        "effect_local_client_shadow_pending",
        "effect_local_client_shadow_receipts_v2",
        "effect_local_client_shadow_visible_entities",
        "effect_local_server_evolution",
        "effect_local_server_key_lineage",
        "effect_local_server_key_lineage_groups",
        "effect_local_server_key_lineage_targets",
        "effect_local_server_shadow_entities"
      ])
    }).pipe(Effect.provide(database)))

  it.effect("rejects changed, deleted, inserted, duplicate, and gapped migration history", () =>
    Effect.gen(function*() {
      const first = Migrations.makeMigration({
        id: 1,
        name: "first",
        statements: ["CREATE TABLE catalog_probe (value INTEGER NOT NULL)"]
      })
      yield* Migrations.runCatalog("Client", [first])

      const changed = Migrations.makeMigration({
        id: 1,
        name: "first",
        statements: ["CREATE TABLE catalog_probe (value TEXT NOT NULL)"]
      })
      assert.strictEqual(
        (yield* Migrations.runCatalog("Client", [changed]).pipe(Effect.flip))._tag,
        "StorageMigrationMismatch"
      )
      assert.strictEqual(
        (yield* Migrations.runCatalog("Client", []).pipe(Effect.flip))._tag,
        "StorageMigrationMismatch"
      )

      const second = Migrations.makeMigration({
        id: 2,
        name: "second",
        statements: ["CREATE TABLE second_probe (value INTEGER NOT NULL)"]
      })
      const duplicate = Migrations.makeMigration({
        id: 2,
        name: "first",
        statements: ["CREATE TABLE duplicate_probe (value INTEGER NOT NULL)"]
      })
      assert.strictEqual(
        (yield* Migrations.runCatalog("Server", [second]).pipe(Effect.flip))._tag,
        "StorageMigrationMismatch"
      )
      assert.strictEqual(
        (yield* Migrations.runCatalog("Server", [first, duplicate]).pipe(Effect.flip))._tag,
        "StorageMigrationMismatch"
      )

      const duplicateId = Migrations.makeMigration({
        id: 1,
        name: "duplicate-id",
        statements: ["CREATE TABLE duplicate_id_probe (value INTEGER NOT NULL)"]
      })
      assert.strictEqual(
        (yield* Migrations.runCatalog("Server", [first, duplicateId]).pipe(Effect.flip))._tag,
        "StorageMigrationMismatch"
      )
    }).pipe(Effect.provide(database)))

  it.effect("upgrades the existing mutation log tables without stamping untrusted schema provenance", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Effect.forEach(Migrations.clientCatalog[0]!.statements, (statement) => sql.unsafe(statement), {
        discard: true
      })
      yield* Effect.forEach(Migrations.serverCatalog[0]!.statements, (statement) => sql.unsafe(statement), {
        discard: true
      })
      yield* sql`INSERT INTO effect_local_client_meta
      (singleton, space_id, client_id, definition_hash, next_local_sequence, server_cursor, visible_revision,
        requested_generation, completed_generation)
      VALUES (1, ${spaceId}, ${clientId}, 'legacy-client-hash', 1, 0, 0, 0, 0)`
      yield* sql`INSERT INTO effect_local_canonical_entities (model, entity_key, value_json)
      VALUES ('Todo', '"1"', '{"id":"1","title":"legacy"}')`
      yield* sql`INSERT INTO effect_local_server_spaces (space_id, definition_hash, next_server_sequence)
      VALUES (${spaceId}, 'legacy-server-hash', 1)`
      yield* sql`INSERT INTO effect_local_server_entities (space_id, model, entity_key, value_json)
      VALUES (${spaceId}, 'Todo', '"1"', '{"id":"1","title":"legacy"}')`

      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId })
      yield* Migrations.server

      const clientRow = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: LegacyClientRow,
        execute: () =>
          sql`SELECT m.definition_hash, m.schema_version, e.value_json AS entity_value,
          e.model_version
        FROM effect_local_client_meta AS m
        INNER JOIN effect_local_canonical_entities AS e ON e.model = 'Todo' AND e.entity_key = '"1"'
        WHERE m.singleton = 1`
      })(undefined)
      assert.deepStrictEqual(clientRow, {
        definition_hash: "legacy-client-hash",
        schema_version: null,
        entity_value: "{\"id\":\"1\",\"title\":\"legacy\"}",
        model_version: null
      })

      const serverRow = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: LegacyServerRow,
        execute: () =>
          sql`SELECT s.definition_hash, s.schema_version, e.value_json AS entity_value,
          e.model_version
        FROM effect_local_server_spaces AS s
        INNER JOIN effect_local_server_entities AS e
          ON e.space_id = s.space_id AND e.model = 'Todo' AND e.entity_key = '"1"'
        WHERE s.space_id = ${spaceId}`
      })(undefined)
      assert.deepStrictEqual(serverRow, {
        definition_hash: "legacy-server-hash",
        schema_version: null,
        entity_value: "{\"id\":\"1\",\"title\":\"legacy\"}",
        model_version: null
      })
    }).pipe(Effect.provide(database)))

  it.effect("rolls back migration statements and the ledger together, then reuses the same client", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const broken = Migrations.makeMigration({
        id: 1,
        name: "broken",
        statements: [
          "CREATE TABLE migration_probe (value INTEGER NOT NULL)",
          "INSERT INTO migration_probe (value) VALUES (1)",
          "INSERT INTO table_that_does_not_exist (value) VALUES (1)"
        ]
      })
      assert.strictEqual(
        (yield* Migrations.runCatalog("Client", [broken]).pipe(Effect.flip))._tag,
        "StorageUnavailable"
      )
      assert.notInclude((yield* tableNames(sql)).map((row) => row.name), "migration_probe")
      assert.deepStrictEqual(yield* clientLedger(sql), [])

      const corrected = Migrations.makeMigration({
        id: 1,
        name: "corrected",
        statements: [
          "CREATE TABLE migration_probe (value INTEGER NOT NULL)",
          "INSERT INTO migration_probe (value) VALUES (1)"
        ]
      })
      yield* Migrations.runCatalog("Client", [corrected])
      assert.strictEqual((yield* probeCount(sql)).count, 1)
      assert.strictEqual((yield* clientLedger(sql)).length, 1)
    }).pipe(Effect.provide(database)))

  it.effect("rolls back an interrupted migration and remains reusable", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const started = yield* Deferred.make<void>()
      const interrupted = Migrations.makeMigration({
        id: 1,
        name: "interrupted",
        statements: [
          "CREATE TABLE migration_probe (value INTEGER NOT NULL)",
          "INSERT INTO migration_probe (value) VALUES (1)"
        ],
        effect: {
          id: "wait-forever",
          run: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
        }
      })
      const fiber = yield* Migrations.runCatalog("Client", [interrupted]).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(started)
      yield* Fiber.interrupt(fiber)
      assert.notInclude((yield* tableNames(sql)).map((row) => row.name), "migration_probe")
      assert.deepStrictEqual(yield* clientLedger(sql), [])

      const corrected = Migrations.makeMigration({
        id: 1,
        name: "corrected",
        statements: [
          "CREATE TABLE migration_probe (value INTEGER NOT NULL)",
          "INSERT INTO migration_probe (value) VALUES (1)"
        ]
      })
      yield* Migrations.runCatalog("Client", [corrected])
      assert.strictEqual((yield* probeCount(sql)).count, 1)
    }).pipe(Effect.provide(database), Effect.scoped))
})
