import { NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import { pipe } from "effect/Function"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Migrations from "../src/Migrations.js"
import * as Domain from "./Domain.js"

const layerDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
const provideDatabase = Effect.provide(layerDatabase)
const provideNodeFileSystemAndReactivity = Effect.provide([NodeFileSystem.layer, Reactivity.layer])
const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const expectedFailure = <A, E extends { readonly _tag: string },>(exit: Exit.Exit<A, E>) => {
  assert.isTrue(Exit.isFailure(exit))
  if (Exit.isFailure(exit)) return Cause.findErrorOption(exit.cause)
  return Option.none<E>()
}
const LedgerRow = Schema.Struct({ id: Schema.Number, name: Schema.String, checksum: Schema.String })
const NameRow = Schema.Struct({ name: Schema.String })
const CountRow = Schema.Struct({ count: Schema.Number })
const UpgradedScopedRow = Schema.Struct({
  delivered_sequence: Schema.Number,
  read_auth_epoch: Schema.Number,
  view_layout: Schema.String,
  snapshot_layout: Schema.String
})
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

const indexNames = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: NameRow,
    execute: () => sql`SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name`
  })(undefined)

const probeCount = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Schema.Void,
    Result: CountRow,
    execute: () => sql`SELECT COUNT(*) AS count FROM migration_probe`
  })(undefined)

describe("storage migration catalogs", () => {
  it.effect(
    "creates covering lifecycle indexes and fences pre upgrade server writers",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.server()

      const plan = yield* sql<{ readonly detail: string }>`EXPLAIN QUERY PLAN
        SELECT model, model_version, entity_key, value_json, entity_bytes
        FROM effect_local_server_entities WHERE space_id = ${spaceId}
        ORDER BY entity_bytes DESC, model, entity_key LIMIT 1`
      assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")))

      const result = yield* sql`INSERT INTO effect_local_server_spaces
        (space_id, definition_hash, next_server_sequence) VALUES (${spaceId}, 'definition', 1)`.pipe(Effect.exit)
      const error = expectedFailure(result).pipe(Option.getOrThrow)
      assert.isTrue(SqlError.isSqlError(error))
    }, provideDatabase)
  )

  it.effect(
    "retries lock contention while initializing an existing server catalog",
    Effect.fnUntraced(
      function*() {
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
            return <R, E extends { readonly _tag: string }, A,>(effect: Effect.Effect<A, E, R>) =>
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
      },
      provideNodeFileSystemAndReactivity,
      Effect.scoped
    )
  )

  it.effect(
    "applies the complete client and server catalogs once",
    Effect.fnUntraced(function*() {
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

      pipe(
        (yield* clientLedger(sql)).map((row) => row.id),
        (ids) => assert.deepStrictEqual(ids, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])
      )
      pipe(
        (yield* serverMigrationLedger(sql)).map((row) => row.id),
        (ids) => assert.deepStrictEqual(ids, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
      )
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
        "effect_local_client_quarantine",
        "effect_local_client_quarantine_cancellations",
        "effect_local_client_quarantine_resubmissions",
        "effect_local_client_pending_data",
        "effect_local_client_receipts_data",
        "effect_local_client_visible_entities_data",
        "effect_local_server_evolution",
        "effect_local_server_key_lineage",
        "effect_local_server_key_lineage_groups",
        "effect_local_server_key_lineage_targets",
        "effect_local_server_shadow_entities",
        "effect_local_server_entities_data",
        "effect_local_server_replication_views",
        "effect_local_server_replication_view_entities",
        "effect_local_server_replication_pages",
        "effect_local_server_offline_wake_acknowledgements",
        "effect_local_server_offline_wake_spaces",
        "effect_local_server_offline_wakes",
        "effect_local_server_watch_presence",
        "effect_local_server_watch_runtimes",
        "effect_local_server_scoped_snapshots",
        "effect_local_server_scoped_snapshot_entries",
        "effect_local_server_snapshot_projections",
        "effect_local_server_snapshot_projection_entities"
      ])
      assert.notInclude(names, "effect_local_bootstrap")
      assert.notInclude(names, "effect_local_bootstrap_entities")
      assert.include((yield* indexNames(sql)).map((row) => row.name), "effect_local_server_watch_presence_runtime")
      yield* sql`INSERT INTO effect_local_server_watch_runtimes (runtime_id, expires_at) VALUES ('runtime', 1)`
      yield* sql`INSERT INTO effect_local_server_watch_presence
        (space_id, client_id, watcher_id, runtime_id) VALUES (${spaceId}, ${clientId}, 'watcher', 'runtime')`
      const duplicatePresence = yield* sql`INSERT INTO effect_local_server_watch_presence
        (space_id, client_id, watcher_id, runtime_id)
        VALUES ('spc_00000000-0000-4000-8000-000000000002', ${clientId}, 'watcher', 'runtime')`.pipe(
        Effect.exit
      )
      assert.isTrue(SqlError.isSqlError(expectedFailure(duplicatePresence).pipe(Option.getOrThrow)))
    }, provideDatabase)
  )

  it.effect(
    "preserves released server migration checksums and upgrades scoped storage",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      assert.strictEqual(Migrations.serverCatalog[6].checksum, "165fbc25e03ae1c8")
      yield* Migrations.runCatalog("Server", Migrations.serverCatalog.slice(0, 9))
      yield* sql`INSERT INTO effect_local_server_replication_views
        (space_id, client_id, principal_digest, view_id, view_revision, scope_generation,
          scope_json, scope_digest, definition_hash, schema_version, schema_hash, server_sequence)
        VALUES (${spaceId}, ${clientId}, ${"0".repeat(64)}, ${"view-1"}, 0, 1,
          ${"{\"models\":[]}"}, ${"1".repeat(64)}, ${"definition"}, 1, ${"schema"}, 7)`
      yield* sql`INSERT INTO effect_local_server_scoped_snapshots
        (snapshot_id, space_id, client_id, principal_digest, definition_hash, schema_version,
          schema_hash, scope_json, scope_digest, scope_generation, view_id, view_revision,
          server_sequence, terminal_sequence, entry_count, content_bytes, digest)
        VALUES (${"snapshot-1"}, ${spaceId}, ${clientId}, ${"0".repeat(64)}, ${"definition"}, 1,
          ${"schema"}, ${"{\"models\":[]}"}, ${"1".repeat(64)}, 1, ${"view-1"}, 0, 7, 7, 0, 0,
          ${"2".repeat(64)})`

      yield* Migrations.server()

      const upgraded = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: UpgradedScopedRow,
        execute: () =>
          sql`SELECT v.delivered_sequence, v.read_auth_epoch,
            v.index_layout_hash AS view_layout,
            s.index_layout_hash AS snapshot_layout
          FROM effect_local_server_replication_views AS v
          INNER JOIN effect_local_server_scoped_snapshots AS s
            ON s.space_id = v.space_id AND s.client_id = v.client_id`
      })(undefined)
      assert.deepStrictEqual(upgraded, {
        delivered_sequence: 7,
        read_auth_epoch: 0,
        view_layout: "",
        snapshot_layout: ""
      })
      pipe(
        (yield* serverMigrationLedger(sql)).map((row) => row.id),
        (ids) => assert.deepStrictEqual(ids, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
      )
    }, provideDatabase)
  )

  it.effect(
    "initializes scoped storage without fabricating a client view",
    Effect.fnUntraced(function*() {
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
    }, provideDatabase)
  )

  it.effect(
    "claims pending migrations before executing their effects",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      const migration = Migrations.makeMigration({
        id: 1,
        name: "claimed-first",
        statements: ["SELECT 1"],
        effect: {
          id: "assert-claim",
          run: (transaction) =>
            clientLedger(transaction).pipe(
              Effect.catchTags({
                SchemaError: (error) => Effect.die(error),
                SqlError: (error) => Effect.die(error)
              }),
              Effect.flatMap((rows) => {
                if (rows.length === 1 && rows[0]?.id === 1) return Effect.void
                return Effect.die("Migration effect ran before its durable claim")
              })
            )
        }
      })
      yield* Migrations.runCatalog("Client", [migration])
      pipe((yield* clientLedger(sql)).map((row) => row.id), (ids) => assert.deepStrictEqual(ids, [1]))
    }, provideDatabase)
  )

  it.effect(
    "rejects changed, deleted, inserted, duplicate, and gapped migration history",
    Effect.fnUntraced(
      function*() {
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
        const changedResult = yield* Migrations.runCatalog("Client", [changed]).pipe(Effect.exit)
        const changedError = expectedFailure(changedResult).pipe(Option.getOrThrow)
        assert.strictEqual(changedError._tag, "StorageMigrationMismatch")
        const deletedResult = yield* Migrations.runCatalog("Client", []).pipe(Effect.exit)
        const deletedError = expectedFailure(deletedResult).pipe(Option.getOrThrow)
        assert.strictEqual(deletedError._tag, "StorageMigrationMismatch")

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
        const insertedResult = yield* Migrations.runCatalog("Server", [second]).pipe(Effect.exit)
        const insertedError = expectedFailure(insertedResult).pipe(Option.getOrThrow)
        assert.strictEqual(insertedError._tag, "StorageMigrationMismatch")
        const duplicateResult = yield* Migrations.runCatalog("Server", [first, duplicate]).pipe(Effect.exit)
        const duplicateError = expectedFailure(duplicateResult).pipe(Option.getOrThrow)
        assert.strictEqual(duplicateError._tag, "StorageMigrationMismatch")

        const duplicateId = Migrations.makeMigration({
          id: 1,
          name: "duplicate-id",
          statements: ["CREATE TABLE duplicate_id_probe (value INTEGER NOT NULL)"]
        })
        const duplicateIdResult = yield* Migrations.runCatalog("Server", [first, duplicateId]).pipe(Effect.exit)
        const duplicateIdError = expectedFailure(duplicateIdResult).pipe(Option.getOrThrow)
        assert.strictEqual(duplicateIdError._tag, "StorageMigrationMismatch")
      },
      provideDatabase
    )
  )

  it.effect(
    "rolls back migration statements and the ledger together, then reuses the same client",
    Effect.fnUntraced(
      function*() {
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
        const brokenResult = yield* Migrations.runCatalog("Client", [broken]).pipe(Effect.exit)
        const brokenError = expectedFailure(brokenResult).pipe(Option.getOrThrow)
        assert.strictEqual(brokenError._tag, "StorageUnavailable")
        pipe((yield* tableNames(sql)).map((row) => row.name), (names) => assert.notInclude(names, "migration_probe"))
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
      },
      provideDatabase
    )
  )

  it.effect(
    "rolls back an interrupted migration and remains reusable",
    Effect.fnUntraced(
      function*() {
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
        pipe((yield* tableNames(sql)).map((row) => row.name), (names) => assert.notInclude(names, "migration_probe"))
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
      },
      provideDatabase,
      Effect.scoped
    )
  )
})
