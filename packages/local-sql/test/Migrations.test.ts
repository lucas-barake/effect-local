import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlError from "effect/sql/SqlError"
import * as SqlSchema from "effect/sql/SqlSchema"
import * as Statement from "effect/sql/Statement"
import * as TestClock from "effect/testing/TestClock"
import * as ConnectionLane from "../src/ConnectionLane.js"
import * as Rows from "../src/internal/rows.js"
import * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"
import { postgresDatabaseUrl, postgresLayer, serverDatabases } from "./fixtures/ServerDatabase.js"
import { gateStatements } from "./fixtures/SqlGate.js"

const layerDatabase = ConnectionLane.makeLayer().pipe(
  Layer.provideMerge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))
)
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
    "creates covering lifecycle indexes",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.server()

      const plan = yield* sql<{ readonly detail: string }>`EXPLAIN QUERY PLAN
        SELECT model, model_version, entity_key, value_json, entity_bytes
        FROM effect_local_server_entities WHERE space_id = ${spaceId}
        ORDER BY entity_bytes DESC, model, entity_key LIMIT 1`
      assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")))
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
        yield* Migrations.server().pipe(Effect.provideService(SqlClient.SqlClient, migratorClient))
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
        const ledger = yield* serverMigrationLedger(migratorClient)
        assert.deepStrictEqual(
          ledger,
          Migrations.serverCatalog.map(({ checksum, id, name }) => ({ id, name, checksum }))
        )
      },
      provideNodeFileSystemAndReactivity,
      Effect.scoped
    )
  )

  it.effect(
    "mints a client identity for a fresh database and keeps it across reopenings",
    Effect.fnUntraced(function*() {
      const minted = yield* Migrations.client({ definition: Domain.definition })
      assert.isTrue(Schema.is(Identity.ClientId)(minted))
      assert.strictEqual(yield* Migrations.client({ definition: Domain.definition }), minted)
      assert.strictEqual(yield* Migrations.client({ definition: Domain.definition, clientId: minted }), minted)
    }, provideDatabase)
  )

  it.effect(
    "adopts the identity an explicit client stored and still rejects a different explicit identity",
    Effect.fnUntraced(function*() {
      assert.strictEqual(yield* Migrations.client({ definition: Domain.definition, clientId }), clientId)
      assert.strictEqual(yield* Migrations.client({ definition: Domain.definition }), clientId)
      const other = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
      const exit = yield* Effect.exit(Migrations.client({ definition: Domain.definition, clientId: other }))
      const failure = expectedFailure(exit)
      assert.isTrue(Option.isSome(failure))
      if (Option.isSome(failure)) {
        assert.deepStrictEqual(
          failure.value,
          new ReplicaError.ReplicaIdentityMismatch({ expectedClientId: other, actualClientId: clientId })
        )
      }
    }, provideDatabase)
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
        (ids) => assert.deepStrictEqual(ids, [1])
      )
      pipe(
        (yield* serverMigrationLedger(sql)).map((row) => row.id),
        (ids) => assert.deepStrictEqual(ids, [1])
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
        "effect_local_server_scoped_snapshot_entries"
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
        const gate = yield* gateStatements(sql, (statement) => {
          if (statement.startsWith("INSERT INTO effect_local_client_migrations")) return ["after"]
          return []
        })
        const interrupted = Migrations.makeMigration({
          id: 1,
          name: "interrupted",
          statements: [
            "CREATE TABLE migration_probe (value INTEGER NOT NULL)",
            "INSERT INTO migration_probe (value) VALUES (1)"
          ]
        })
        const fiber = yield* Migrations.runCatalog("Client", [interrupted]).pipe(
          Effect.provideService(SqlClient.SqlClient, gate.sql),
          Effect.forkChild({ startImmediately: true })
        )
        yield* Queue.take(gate.pauses)
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

const PostgresNameRow = Schema.Struct({ table_name: Schema.String })
const PostgresColumnRow = Schema.Struct({ table_name: Schema.String, column_name: Schema.String })
const LockWaiterRow = Schema.Struct({ waiters: Schema.Number })

const postgresTableNames = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: PostgresNameRow,
    execute: () =>
      sql`SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' ORDER BY table_name`
  })(undefined)

const postgresNonBytewiseTextColumns = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: PostgresColumnRow,
    execute: () =>
      sql`SELECT table_name, column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND data_type = 'text' AND collation_name IS DISTINCT FROM 'C'
        ORDER BY table_name, column_name`
  })(undefined)

const awaitLockWaiters = (sql: SqlClient.SqlClient, waiters: number) =>
  SqlSchema.findOne({
    Request: Schema.Void,
    Result: LockWaiterRow,
    execute: () =>
      sql`SELECT COUNT(*)::int AS waiters FROM pg_stat_activity
        WHERE datname = current_database() AND wait_event_type = 'Lock'`
  })(undefined).pipe(Effect.repeat({ until: (row) => row.waiters >= waiters }))

const unsupportedDialectClient = SqlClient.make({
  acquirer: Effect.die("An unsupported SQL dialect must be rejected before any statement runs"),
  compiler: Statement.makeCompiler({
    dialect: "mysql",
    placeholder: () => "?",
    onIdentifier: (value) => `\`${value}\``,
    onRecordUpdate: () => ["", []],
    onCustom: () => ["", []]
  }),
  spanAttributes: []
}).pipe(Effect.provide(Reactivity.layer))

const providePostgres = Effect.provide(postgresLayer())
const provideReactivity = Effect.provide(Reactivity.layer)

describe("postgres server catalog", () => {
  it.effect(
    "applies the postgres server catalog once with bytewise text columns",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.server()
      yield* Migrations.server()

      const ledger = yield* serverMigrationLedger(sql)
      assert.deepStrictEqual(ledger, [
        { id: 1, name: "postgres-baseline", checksum: Migrations.serverPostgresCatalog[0].checksum }
      ])
      const names = (yield* postgresTableNames(sql)).map((row) => row.table_name)
      assert.includeMembers(names, [
        "effect_local_authoritative_log",
        "effect_local_server_clients",
        "effect_local_server_entities",
        "effect_local_server_entities_data",
        "effect_local_server_evolution",
        "effect_local_server_index_catalog",
        "effect_local_server_index_partition_log",
        "effect_local_server_index_state",
        "effect_local_server_key_lineage",
        "effect_local_server_key_lineage_groups",
        "effect_local_server_key_lineage_targets",
        "effect_local_server_migrations",
        "effect_local_server_offline_wake_acknowledgements",
        "effect_local_server_offline_wake_spaces",
        "effect_local_server_offline_wakes",
        "effect_local_server_receipts",
        "effect_local_server_replication_pages",
        "effect_local_server_replication_view_entities",
        "effect_local_server_replication_views",
        "effect_local_server_scoped_snapshot_entries",
        "effect_local_server_scoped_snapshots",
        "effect_local_server_snapshot_entities",
        "effect_local_server_snapshots",
        "effect_local_server_space_counts",
        "effect_local_server_spaces",
        "effect_local_server_watch_presence",
        "effect_local_server_watch_runtimes"
      ])
      assert.deepStrictEqual(yield* postgresNonBytewiseTextColumns(sql), [])
    }, providePostgres)
  )

  it.effect(
    "rejects unsupported sql dialects before touching storage",
    Effect.fnUntraced(function*() {
      const unsupported = yield* unsupportedDialectClient
      const migration = yield* Migrations.server().pipe(
        Effect.provideService(SqlClient.SqlClient, unsupported),
        Effect.exit
      )
      assert.strictEqual(expectedFailure(migration).pipe(Option.getOrThrow)._tag, "InvalidConfiguration")
      const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
      const layerUnsupported = Layer.succeed(SqlClient.SqlClient, unsupported)
      const store = yield* ServerStore.layerTrusted({ definition: Domain.definition }).pipe(
        Layer.provide([layerRuntime, layerUnsupported, NodeCrypto.layer]),
        Layer.build,
        Effect.exit
      )
      assert.strictEqual(expectedFailure(store).pipe(Option.getOrThrow)._tag, "InvalidConfiguration")
    }, Effect.scoped)
  )

  it.effect(
    "concurrent runners migrate one postgres catalog",
    Effect.fnUntraced(
      function*() {
        const { url } = yield* postgresDatabaseUrl
        const blocker = yield* PgClient.makeClient({ url })
        const observer = yield* PgClient.makeClient({ url })
        const first = yield* PgClient.make({ url, maxConnections: 2 })
        const second = yield* PgClient.make({ url, maxConnections: 2 })
        yield* blocker`BEGIN`
        yield* blocker`LOCK TABLE pg_catalog.pg_class IN SHARE MODE`
        const firstRunner = yield* Migrations.server().pipe(
          Effect.provideService(SqlClient.SqlClient, first),
          Effect.forkChild({ startImmediately: true })
        )
        const secondRunner = yield* Migrations.server().pipe(
          Effect.provideService(SqlClient.SqlClient, second),
          Effect.forkChild({ startImmediately: true })
        )
        yield* awaitLockWaiters(observer, 2)
        yield* blocker`COMMIT`
        yield* Fiber.join(firstRunner)
        yield* Fiber.join(secondRunner)
        pipe(
          (yield* serverMigrationLedger(observer)).map((row) => row.id),
          (ids) => assert.deepStrictEqual(ids, [1])
        )
      },
      Effect.scoped,
      provideReactivity
    )
  )
})

describe.each(serverDatabases)("server catalog counters ($dialect)", (database) => {
  const provideServerDatabase = Effect.provide(database.layer())

  it.effect(
    "maintains retained and entity counters through row triggers",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.server()
      yield* sql`INSERT INTO effect_local_server_spaces
        (space_id, definition_hash, next_server_sequence, schema_version, schema_hash, schema_generation,
          next_terminal_sequence, history_floor, receipt_floor, retained_history_count,
          retained_receipt_count, entity_count, entity_bytes, snapshot_sequence,
          snapshot_terminal_sequence)
        VALUES (${spaceId}, 'definition', 1, 1, 'aaaaaaaaaaaaaaaa', 0, 1, 0, 0, 0, 0, 0, 0, 0, 0)`
      yield* sql`INSERT INTO effect_local_server_space_counts (space_id, history_count, receipt_count)
        VALUES (${spaceId}, 0, 0)`
      for (const sequence of [1, 2]) {
        yield* sql`INSERT INTO effect_local_authoritative_log
          (space_id, server_sequence, client_id, membership_incarnation, local_sequence, mutation_id, digest,
            entry_bytes, entry_json, source_schema_version, source_schema_hash, mutation_version)
          VALUES (${spaceId}, ${sequence}, ${clientId}, 'incarnation', ${sequence}, ${`mutation-${sequence}`},
            'digest', 2, '{}', 1, 'aaaaaaaaaaaaaaaa', 1)`
      }
      yield* sql`INSERT INTO effect_local_server_receipts
        (space_id, client_id, membership_incarnation, local_sequence, mutation_id, digest, receipt_json,
          digest_version, terminal_sequence, source_schema_version, source_schema_hash, mutation_version,
          mutation_name)
        VALUES (${spaceId}, ${clientId}, 'incarnation', 1, 'mutation-1', 'digest', '{}', 1, 1, 1,
          'aaaaaaaaaaaaaaaa', 1, 'Put')`
      yield* sql`DELETE FROM effect_local_authoritative_log WHERE space_id = ${spaceId} AND server_sequence = 1`
      for (const [generation, key, bytes] of [[0, "a", 10], [0, "b", 20], [1, "c", 40]] as const) {
        yield* sql`INSERT INTO effect_local_server_entities_data
          (space_id, generation, model, entity_key, value_json, model_version, entity_bytes)
          VALUES (${spaceId}, ${generation}, 'Todo', ${key}, '{}', 1, ${bytes})`
      }
      yield* sql`UPDATE effect_local_server_entities_data SET entity_bytes = 15
        WHERE space_id = ${spaceId} AND generation = 0 AND entity_key = 'a'`
      yield* sql`DELETE FROM effect_local_server_entities_data
        WHERE space_id = ${spaceId} AND generation = 0 AND entity_key = 'b'`

      const space = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: Rows.ServerMetaRow,
        execute: () =>
          sql`SELECT definition_hash, schema_version, schema_hash, schema_generation, active_schema_generation,
            target_schema_version, target_schema_hash, migration_hash, next_server_sequence, next_terminal_sequence,
            history_floor, receipt_floor, retained_history_count, retained_receipt_count, entity_count, entity_bytes,
            snapshot_id, snapshot_sequence, snapshot_terminal_sequence
          FROM effect_local_server_spaces WHERE space_id = ${spaceId}`
      })(undefined)
      const counts = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: Rows.ServerCountRow,
        execute: () =>
          sql`SELECT history_count, receipt_count FROM effect_local_server_space_counts WHERE space_id = ${spaceId}`
      })(undefined)
      assert.deepStrictEqual(
        {
          history: space.retained_history_count,
          receipts: space.retained_receipt_count,
          entities: space.entity_count,
          bytes: space.entity_bytes,
          counts
        },
        { history: 1, receipts: 1, entities: 1, bytes: 15, counts: { history_count: 1, receipt_count: 1 } }
      )
    }, provideServerDatabase)
  )
})

describe.each(serverDatabases)("migration constraint failures ($dialect)", (database) => {
  const provideServerDatabase = Effect.provide(database.layer())

  it.effect(
    "reports a migration that violates a constraint on its own as corrupt storage",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      const duplicate = Migrations.makeMigration({
        id: 1,
        name: "duplicate-probe",
        statements: [
          "CREATE TABLE constraint_probe (value INTEGER NOT NULL UNIQUE)",
          "INSERT INTO constraint_probe (value) VALUES (1)",
          "INSERT INTO constraint_probe (value) VALUES (1)"
        ]
      })
      const exit = yield* Migrations.runCatalog("Server", [duplicate]).pipe(Effect.exit)
      const failure = expectedFailure(exit).pipe(Option.getOrThrow)
      if (failure._tag !== "StorageCorrupt") assert.fail(`expected StorageCorrupt, got ${failure._tag}`)
      assert.strictEqual(failure.message, "Server migration failed a permanent constraint")
      assert.isTrue(SqlError.isSqlError(failure.cause))
      assert.deepStrictEqual(yield* serverMigrationLedger(sql), [])
    }, provideServerDatabase)
  )

  it.effect(
    "accepts a constraint failure once another runner has applied the same catalog",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      const migration = Migrations.makeMigration({
        id: 1,
        name: "raced-probe",
        statements: ["CREATE TABLE raced_probe (value INTEGER NOT NULL)"]
      })
      let transactions = 0
      const racedClient = new Proxy(sql, {
        get: (target, property, receiver) => {
          if (property !== "withTransaction") return Reflect.get(target, property, receiver)
          return <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) => {
            transactions += 1
            if (transactions !== 2) return target.withTransaction(effect)
            return Migrations.runCatalog("Server", [migration]).pipe(
              Effect.provideService(SqlClient.SqlClient, target),
              Effect.andThen(Effect.fail(
                new SqlError.SqlError({
                  reason: new SqlError.UniqueViolation({ cause: "concurrent ledger insert", constraint: "name" })
                })
              ))
            )
          }
        }
      })
      yield* Migrations.runCatalog("Server", [migration]).pipe(Effect.provideService(SqlClient.SqlClient, racedClient))
      assert.strictEqual(transactions, 2)
      assert.deepStrictEqual(
        yield* serverMigrationLedger(sql),
        [{ id: migration.id, name: migration.name, checksum: migration.checksum }]
      )
    }, provideServerDatabase)
  )
})

describe("client identity adoption race", () => {
  it.effect(
    "rejects an explicit identity when another opener stores a different identity before its insert",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      const winner = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
      yield* Migrations.runCatalog("Client", Migrations.clientCatalog)
      yield* sql.unsafe(`CREATE TRIGGER concurrent_opener BEFORE INSERT ON effect_local_client_meta
        WHEN NEW.client_id <> '${winner}'
        BEGIN INSERT INTO effect_local_client_meta (singleton, client_id) VALUES (1, '${winner}'); END`)
      const exit = yield* Effect.exit(Migrations.client({ definition: Domain.definition, clientId }))
      const failure = expectedFailure(exit)
      assert.isTrue(Option.isSome(failure))
      if (Option.isSome(failure)) {
        assert.deepStrictEqual(
          failure.value,
          new ReplicaError.ReplicaIdentityMismatch({ expectedClientId: clientId, actualClientId: winner })
        )
      }
    }, provideDatabase)
  )
})

describe("client identity race with a space", () => {
  it.effect(
    "does not register the space in a database whose identity another opener stored first",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      const winner = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
      yield* Migrations.runCatalog("Client", Migrations.clientCatalog)
      yield* sql.unsafe(`CREATE TRIGGER concurrent_opener BEFORE INSERT ON effect_local_client_meta
        WHEN NEW.client_id <> '${winner}'
        BEGIN INSERT INTO effect_local_client_meta (singleton, client_id) VALUES (1, '${winner}'); END`)
      const exit = yield* Effect.exit(Migrations.client({ definition: Domain.definition, spaceId, clientId }))
      assert.isTrue(Exit.isFailure(exit))
      const spaces = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: CountRow,
        execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
      })(undefined)
      assert.strictEqual(spaces.count, 0)
    }, provideDatabase)
  )
})
