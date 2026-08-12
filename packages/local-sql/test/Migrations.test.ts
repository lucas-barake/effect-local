import { NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Migrations from "../src/Migrations.js"
import * as Domain from "./Domain.js"

const database = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const LedgerRow = Schema.Struct({ id: Schema.Number, name: Schema.String, checksum: Schema.String })
const NameRow = Schema.Struct({ name: Schema.String })
const CountRow = Schema.Struct({ count: Schema.Number })
const ClientReplicationMetaRow = Schema.Struct({
  replication_view_id: Schema.NullOr(Schema.String),
  replication_view_revision: Schema.Number,
  desired_scope_json: Schema.String,
  desired_scope_digest: Schema.String,
  scope_generation: Schema.Number
})
const clientLedger = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: LedgerRow,
    execute: () => sql`SELECT id, name, checksum FROM effect_local_client_migrations ORDER BY id`
  })(undefined)

const serverMigrationLedger = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: LedgerRow,
    execute: () => sql`SELECT id, name, checksum FROM effect_local_server_migrations ORDER BY id`
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
  it.effect("creates covering lifecycle indexes and fences pre upgrade server writers", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.server()

      const plan = yield* sql<{ readonly detail: string }>`EXPLAIN QUERY PLAN
        SELECT model, model_version, entity_key, value_json, entity_bytes
        FROM effect_local_server_entities WHERE space_id = ${spaceId}
        ORDER BY entity_bytes DESC, model, entity_key LIMIT 1`
      assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")))

      const error = yield* sql`INSERT INTO effect_local_server_spaces
        (space_id, definition_hash, next_server_sequence) VALUES (${spaceId}, 'definition', 1)`.pipe(Effect.flip)
      assert.isTrue(SqlError.isSqlError(error))
    }).pipe(Effect.provide(database)))

  it.effect("retries lock contention while initializing an existing server catalog", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/migration-contention.sqlite`
        const lockClient = yield* SqliteClient.make({ filename })
        const migratorClient = yield* SqliteClient.make({ filename })
        yield* migratorClient`PRAGMA busy_timeout = 1`
        yield* Migrations.runCatalog("Server", Migrations.serverCatalog.slice(0, -1)).pipe(
          Effect.provideService(SqlClient.SqlClient, migratorClient)
        )
        const firstFailure = yield* Deferred.make<void>()
        const observedClient = new Proxy(migratorClient, {
          get: (target, property, receiver) => {
            if (property === "unsafe") {
              return <A extends object,>(statement: string, parameters?: ReadonlyArray<unknown>) =>
                target.unsafe<A>(statement, parameters).pipe(
                  Effect.tapError(() => Deferred.succeed(firstFailure, undefined))
                )
            }
            if (property !== "withTransaction") return Reflect.get(target, property, receiver)
            return <R, E, A,>(effect: Effect.Effect<A, E, R>) =>
              target.withTransaction(effect).pipe(
                Effect.tapError(() => Deferred.succeed(firstFailure, undefined))
              )
          }
        })

        yield* lockClient`BEGIN IMMEDIATE`
        const migrationFiber = yield* Migrations.server({
          retryDelay: "1 second",
          maximumAttempts: 2
        }).pipe(
          Effect.provideService(SqlClient.SqlClient, observedClient),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(firstFailure)
        yield* lockClient`ROLLBACK`
        yield* TestClock.adjust("1 second")
        yield* Fiber.join(migrationFiber)
      }).pipe(Effect.provide([NodeFileSystem.layer, Reactivity.layer]))
    ))

  it.effect("applies the complete client and server catalogs once", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.client({
        definition: Domain.definition,
        spaceId,
        clientId
      })
      yield* Migrations.server()
      yield* Migrations.client({
        definition: Domain.definition,
        spaceId,
        clientId
      })
      yield* Migrations.server()

      assert.deepStrictEqual((yield* clientLedger(sql)).map((row) => row.id), [1, 2, 3, 4, 5, 6, 7, 8])
      assert.deepStrictEqual((yield* serverMigrationLedger(sql)).map((row) => row.id), [1, 2, 3, 4, 5, 6, 7, 8])
      const names = (yield* tableNames(sql)).map((row) => row.name)
      assert.includeMembers(names, [
        "effect_local_client_evolution",
        "effect_local_client_key_lineage",
        "effect_local_client_key_lineage_groups",
        "effect_local_client_key_lineage_targets",
        "effect_local_client_spaces",
        "effect_local_client_retractions",
        "effect_local_client_scoped_bootstrap",
        "effect_local_client_scoped_bootstrap_entries",
        "effect_local_client_canonical_entities_data",
        "effect_local_client_pending_data",
        "effect_local_client_receipts_data",
        "effect_local_client_visible_entities_data",
        "effect_local_client_retractions",
        "effect_local_client_scoped_bootstrap",
        "effect_local_client_scoped_bootstrap_entries",
        "effect_local_server_evolution",
        "effect_local_server_key_lineage",
        "effect_local_server_key_lineage_groups",
        "effect_local_server_key_lineage_targets",
        "effect_local_server_shadow_entities",
        "effect_local_server_entities_data",
        "effect_local_server_replication_views",
        "effect_local_server_replication_view_entities",
        "effect_local_server_replication_pages",
        "effect_local_server_scoped_snapshots",
        "effect_local_server_scoped_snapshot_entries"
      ])
      assert.notInclude(names, "effect_local_bootstrap")
      assert.notInclude(names, "effect_local_bootstrap_entities")
    }).pipe(Effect.provide(database)))

  it.effect("initializes scoped storage without fabricating a client view", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId })
      yield* Migrations.server()

      const meta = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: ClientReplicationMetaRow,
        execute: () =>
          sql`SELECT replication_view_id, replication_view_revision, desired_scope_json,
            desired_scope_digest, scope_generation
          FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
      })(undefined)
      assert.deepStrictEqual(meta, {
        replication_view_id: null,
        replication_view_revision: 0,
        desired_scope_json: "{\"models\":[]}",
        desired_scope_digest: "0".repeat(64),
        scope_generation: 0
      })
      const clientRetractions = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: CountRow,
        execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_client_retractions`
      })(undefined)
      const serverViews = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: CountRow,
        execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_server_replication_views`
      })(undefined)
      assert.strictEqual(clientRetractions.count, 0)
      assert.strictEqual(serverViews.count, 0)
    }).pipe(Effect.provide(database)))

  it.effect("claims pending migrations before executing their effects", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const migration = Migrations.makeMigration({
        id: 1,
        name: "claimed-first",
        statements: ["SELECT 1"],
        effect: {
          id: "assert-claim",
          run: (transaction) =>
            clientLedger(transaction).pipe(
              Effect.orDie,
              Effect.flatMap((rows) => {
                if (rows.length === 1 && rows[0]?.id === 1) return Effect.void
                return Effect.die("Migration effect ran before its durable claim")
              })
            )
        }
      })
      yield* Migrations.runCatalog("Client", [migration])
      assert.deepStrictEqual((yield* clientLedger(sql)).map((row) => row.id), [1])
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
