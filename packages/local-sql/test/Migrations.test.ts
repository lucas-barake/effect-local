import { NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
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
const otherClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")

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
        "effect_local_bootstrap",
        "effect_local_bootstrap_entities",
        "effect_local_client_canonical_entities_data",
        "effect_local_client_pending_data",
        "effect_local_client_receipts_data",
        "effect_local_client_visible_entities_data",
        "effect_local_server_evolution",
        "effect_local_server_key_lineage",
        "effect_local_server_key_lineage_groups",
        "effect_local_server_key_lineage_targets",
        "effect_local_server_shadow_entities",
        "effect_local_server_entities_data"
      ])
    }).pipe(Effect.provide(database)))

  it.effect("promotes the singleton client catalog to partitioned space ownership without data loss", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.runCatalog("Client", Migrations.clientCatalog.slice(0, 7))
      yield* sql`INSERT INTO effect_local_client_meta
        (singleton, space_id, client_id, definition_hash, next_local_sequence, server_cursor, visible_revision,
          requested_generation, completed_generation, schema_version, schema_hash, schema_generation,
          active_schema_generation, installed_snapshot_sequence, installed_snapshot_terminal_sequence)
        VALUES (1, ${spaceId}, ${clientId}, ${Domain.definition.hash}, 2, 4, 3, 5, 4,
          ${Domain.definition.schemaIdentity.version}, ${Domain.definition.schemaIdentity.hash}, 2, 2, 0, 0)`
      yield* sql`INSERT INTO effect_local_client_canonical_entities_data
        (generation, model, entity_key, value_json, model_version)
        VALUES (2, 'Todo', '"legacy"', '{"id":"legacy","title":"kept"}', 1)`
      yield* sql`INSERT INTO effect_local_client_visible_entities_data
        (generation, model, entity_key, value_json, model_version)
        VALUES (2, 'Todo', '"legacy"', '{"id":"legacy","title":"kept"}', 1)`

      yield* Migrations.client({ definition: Domain.definition, spaceId, clientId })

      assert.deepStrictEqual((yield* clientLedger(sql)).map((row) => row.id), [1, 2, 3, 4, 5, 6, 7, 8])
      const identities = yield* sql<{
        readonly singleton: number
        readonly client_id: string
      }>`SELECT singleton, client_id FROM effect_local_client_meta`
      assert.deepStrictEqual(identities, [{ singleton: 1, client_id: clientId }])
      const memberships = yield* sql<{
        readonly space_id: string
        readonly membership_incarnation: string
        readonly active_schema_generation: number
        readonly active_projection_generation: number
      }>`SELECT space_id, membership_incarnation, active_schema_generation, active_projection_generation
        FROM effect_local_client_spaces`
      assert.deepStrictEqual(memberships, [{
        space_id: spaceId,
        membership_incarnation: Identity.legacyMembershipIncarnation,
        active_schema_generation: 2,
        active_projection_generation: 0
      }])
      const visible = yield* sql<{
        readonly space_id: string
        readonly schema_generation: number
        readonly projection_generation: number
        readonly value_json: string
      }>`SELECT space_id, schema_generation, projection_generation, value_json
        FROM effect_local_client_visible_entities_data`
      assert.deepStrictEqual(visible, [{
        space_id: spaceId,
        schema_generation: 2,
        projection_generation: 0,
        value_json: "{\"id\":\"legacy\",\"title\":\"kept\"}"
      }])
    }).pipe(Effect.provide(database)))

  it.effect("rejects a wrong client before mutating a version 7 catalog", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.runCatalog("Client", Migrations.clientCatalog.slice(0, 7))
      yield* sql`INSERT INTO effect_local_client_meta
        (singleton, space_id, client_id, definition_hash, next_local_sequence, server_cursor, visible_revision,
          requested_generation, completed_generation, schema_version, schema_hash, schema_generation,
          active_schema_generation, installed_snapshot_sequence, installed_snapshot_terminal_sequence)
        VALUES (1, ${spaceId}, ${clientId}, ${Domain.definition.hash}, 1, 0, 0, 0, 0,
          ${Domain.definition.schemaIdentity.version}, ${Domain.definition.schemaIdentity.hash}, 0, 0, 0, 0)`

      const error = yield* Migrations.client({
        definition: Domain.definition,
        spaceId,
        clientId: otherClientId
      }).pipe(Effect.flip)

      assert.strictEqual(error._tag, "ReplicaIdentityMismatch")
      if (error._tag === "ReplicaIdentityMismatch") {
        assert.strictEqual(error.expectedClientId, otherClientId)
        assert.strictEqual(error.actualClientId, clientId)
      }
      assert.deepStrictEqual((yield* clientLedger(sql)).map((row) => row.id), [1, 2, 3, 4, 5, 6, 7])
      const metadata = yield* sql<{
        readonly space_id: string
        readonly client_id: string
      }>`SELECT space_id, client_id FROM effect_local_client_meta`
      assert.deepStrictEqual(metadata, [{ space_id: spaceId, client_id: clientId }])
    }).pipe(Effect.provide(database)))

  it.effect("rolls back version 8 when legacy bootstrap ownership is corrupt", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.runCatalog("Client", Migrations.clientCatalog.slice(0, 7))
      yield* sql`INSERT INTO effect_local_client_meta
        (singleton, space_id, client_id, definition_hash, next_local_sequence, server_cursor, visible_revision,
          requested_generation, completed_generation, schema_version, schema_hash, schema_generation,
          active_schema_generation, installed_snapshot_sequence, installed_snapshot_terminal_sequence)
        VALUES (1, ${spaceId}, ${clientId}, ${Domain.definition.hash}, 1, 0, 0, 0, 0,
          ${Domain.definition.schemaIdentity.version}, ${Domain.definition.schemaIdentity.hash}, 0, 0, 0, 0)`
      yield* sql`INSERT INTO effect_local_bootstrap
        (space_id, snapshot_id, definition_hash, schema_version, schema_hash, server_sequence,
          terminal_sequence, entity_count, content_bytes, digest, next_ordinal, received_bytes, rolling_digest)
        VALUES ('spc_00000000-0000-4000-8000-000000000099', 'snp_legacy', ${Domain.definition.hash},
          ${Domain.definition.schemaIdentity.version}, ${Domain.definition.schemaIdentity.hash},
          0, 0, 0, 0, ${"0".repeat(64)}, 0, 0, ${"0".repeat(64)})`

      const error = yield* Migrations.client({ definition: Domain.definition, spaceId, clientId }).pipe(Effect.flip)

      assert.strictEqual(error._tag, "StorageCorrupt")
      assert.deepStrictEqual((yield* clientLedger(sql)).map((row) => row.id), [1, 2, 3, 4, 5, 6, 7])
      const metadata = yield* sql<{ readonly space_id: string }>`SELECT space_id FROM effect_local_client_meta`
      const bootstrap = yield* sql<{ readonly space_id: string }>`SELECT space_id FROM effect_local_bootstrap`
      assert.deepStrictEqual(metadata, [{ space_id: spaceId }])
      assert.deepStrictEqual(bootstrap, [{ space_id: "spc_00000000-0000-4000-8000-000000000099" }])
    }).pipe(Effect.provide(database)))

  it.effect("promotes server mutation lineage to membership incarnations with canonical durable JSON", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.runCatalog("Server", Migrations.serverCatalog.slice(0, 7))
      const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")
      const digest = Protocol.MutationDigest.make("1".repeat(64))
      const schemaHash = Identity.SchemaHash.make("2".repeat(16))
      const entry = {
        sequence: 1,
        spaceId,
        clientId,
        mutationId,
        localSequence: 1,
        sourceSchema: { version: 1, hash: schemaHash },
        digest,
        changes: []
      }
      const receipt = {
        _tag: "Accepted",
        spaceId,
        clientId,
        mutationId,
        localSequence: 1,
        name: "put",
        sourceSchema: { version: 1, hash: schemaHash },
        mutationVersion: 1,
        serverSequence: 1,
        terminalSequence: 1,
        result: null
      }
      yield* sql`INSERT INTO effect_local_server_spaces
        (space_id, definition_hash, next_server_sequence, schema_version, schema_hash,
          schema_generation, active_schema_generation, next_terminal_sequence, metadata_verified)
        VALUES (${spaceId}, 'definition', 2, 1, ${schemaHash}, 0, 0, 2, 1)`
      yield* sql`INSERT INTO effect_local_server_space_counts (space_id, history_count, receipt_count)
        VALUES (${spaceId}, 0, 0)`
      yield* sql`INSERT INTO effect_local_server_clients
        (space_id, client_id, last_local_sequence, expired_local_sequence)
        VALUES (${spaceId}, ${clientId}, 1, 0)`
      yield* sql`INSERT INTO effect_local_authoritative_log
        (space_id, server_sequence, mutation_id, entry_bytes, entry_json, source_schema_version,
          source_schema_hash, mutation_version, client_id, local_sequence, digest)
        VALUES (${spaceId}, 1, ${mutationId}, 1, ${Canonical.stringify(entry)}, 1,
          ${schemaHash}, 1, ${clientId}, 1, ${digest})`
      yield* sql`INSERT INTO effect_local_server_receipts
        (space_id, client_id, local_sequence, mutation_id, digest, receipt_json, digest_version,
          source_schema_version, source_schema_hash, mutation_version, mutation_name,
          terminal_sequence, server_sequence)
        VALUES (${spaceId}, ${clientId}, 1, ${mutationId}, ${digest}, ${Canonical.stringify(receipt)}, 2,
          1, ${schemaHash}, 1, 'put', 1, 1)`

      yield* Migrations.server()

      const clientRows = yield* sql<{ readonly membership_incarnation: string }>`
        SELECT membership_incarnation FROM effect_local_server_clients`
      const receiptRows = yield* sql<{
        readonly membership_incarnation: string
        readonly receipt_json: string
      }>`SELECT membership_incarnation, receipt_json FROM effect_local_server_receipts`
      const entryRows = yield* sql<{
        readonly membership_incarnation: string
        readonly entry_bytes: number
        readonly entry_json: string
      }>`SELECT membership_incarnation, entry_bytes, entry_json FROM effect_local_authoritative_log`
      assert.strictEqual(clientRows[0].membership_incarnation, Identity.legacyMembershipIncarnation)
      assert.strictEqual(receiptRows[0].membership_incarnation, Identity.legacyMembershipIncarnation)
      assert.strictEqual(entryRows[0].membership_incarnation, Identity.legacyMembershipIncarnation)
      const migratedReceipt = Schema.decodeUnknownSync(Schema.fromJsonString(Protocol.Receipt))(
        receiptRows[0].receipt_json
      )
      const migratedEntry = Schema.decodeUnknownSync(Schema.fromJsonString(Protocol.AcceptedMutation))(
        entryRows[0].entry_json
      )
      assert.strictEqual(migratedReceipt.membershipIncarnation, Identity.legacyMembershipIncarnation)
      assert.strictEqual(migratedEntry.membershipIncarnation, Identity.legacyMembershipIncarnation)
      assert.strictEqual(entryRows[0].entry_bytes, Protocol.encodedBytes(migratedEntry))
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

  it.effect("upgrades the existing mutation log tables without stamping untrusted schema provenance", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const clientInitial = Migrations.clientCatalog[0]
      const serverInitial = Migrations.serverCatalog[0]
      if (clientInitial === undefined || serverInitial === undefined) {
        assert.fail("Migration catalogs must contain their initial schema")
      }
      yield* Effect.forEach(clientInitial.statements, (statement) => sql.unsafe(statement), {
        discard: true
      })
      yield* Effect.forEach(serverInitial.statements, (statement) => sql.unsafe(statement), {
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
      yield* Migrations.server()

      const clientRow = yield* SqlSchema.findOne({
        Request: Schema.Void,
        Result: LegacyClientRow,
        execute: () =>
          sql`SELECT m.definition_hash, m.schema_version, e.value_json AS entity_value,
          e.model_version
        FROM effect_local_client_spaces AS m
        INNER JOIN effect_local_client_canonical_entities_data AS e
          ON e.space_id = m.space_id AND e.schema_generation = m.active_schema_generation
          AND e.model = 'Todo' AND e.entity_key = '"1"'
        WHERE m.space_id = ${spaceId}`
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

  it.effect("restarts pre-generation promotions from preserved source rows", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrations.runCatalog("Client", Migrations.clientCatalog.slice(0, 6))
      yield* Migrations.runCatalog("Server", Migrations.serverCatalog.slice(0, 6))
      yield* sql`INSERT INTO effect_local_client_meta
        (singleton, space_id, client_id, definition_hash, next_local_sequence, server_cursor, visible_revision,
          requested_generation, completed_generation, schema_version, schema_hash, schema_generation,
          target_schema_version, target_schema_hash, migration_hash)
        VALUES (1, ${spaceId}, ${clientId}, 'source-definition', 1, 0, 0, 0, 0,
          1, 'source-hash', 4, 2, 'target-hash', 'migration-hash')`
      yield* sql`INSERT INTO effect_local_canonical_entities (model, model_version, entity_key, value_json)
        VALUES ('Todo', 1, '"source"', '{"id":"source"}')`
      yield* sql`INSERT INTO effect_local_client_evolution
        (singleton, source_schema_version, source_schema_hash, target_schema_version, target_schema_hash,
          migration_hash, generation, phase, cursor_model, cursor_key, cursor_sequence)
        VALUES (1, 1, 'source-hash', 2, 'target-hash', 'migration-hash', 4,
          'Entities', 'Todo', '"source"', NULL)`
      yield* sql`INSERT INTO effect_local_client_shadow_entities
        (generation, model, model_version, entity_key, value_json)
        VALUES (4, 'Todo', 2, '"partial"', '{"id":"partial"}')`

      yield* sql`INSERT INTO effect_local_server_spaces
        (space_id, definition_hash, next_server_sequence, schema_version, schema_hash, schema_generation,
          target_schema_version, target_schema_hash, migration_hash, next_terminal_sequence, history_floor,
          receipt_floor, retained_history_count, retained_receipt_count, entity_count, entity_bytes,
          snapshot_sequence, snapshot_terminal_sequence, metadata_verified)
        VALUES (${spaceId}, 'source-definition', 1, 1, 'source-hash', 4, 2, 'target-hash',
          'migration-hash', 1, 0, 0, 0, 0, 1, 10, 0, 0, 1)`
      yield* sql`INSERT INTO effect_local_server_entities
        (space_id, model, model_version, entity_key, value_json, entity_bytes)
        VALUES (${spaceId}, 'Todo', 1, '"source"', '{"id":"source"}', 10)`
      yield* sql`INSERT INTO effect_local_server_evolution
        (space_id, source_schema_version, source_schema_hash, target_schema_version, target_schema_hash,
          migration_hash, generation, phase, cursor_model, cursor_key, cursor_sequence)
        VALUES (${spaceId}, 1, 'source-hash', 2, 'target-hash', 'migration-hash', 4,
          'Entities', 'Todo', '"source"', NULL)`
      yield* sql`INSERT INTO effect_local_server_shadow_entities
        (space_id, generation, model, model_version, entity_key, value_json)
        VALUES (${spaceId}, 4, 'Todo', 2, '"partial"', '{"id":"partial"}')`

      yield* Migrations.runCatalog("Client", Migrations.clientCatalog)
      yield* Migrations.runCatalog("Server", Migrations.serverCatalog)

      const client = (yield* sql<{
        readonly active_schema_generation: number
        readonly phase: string
        readonly source_generation: number
        readonly source_count: number
        readonly target_count: number
      }>`SELECT m.active_schema_generation, e.phase, e.source_generation,
        (SELECT COUNT(*) FROM effect_local_client_canonical_entities_data
          WHERE space_id = ${spaceId} AND schema_generation = 3) AS source_count,
        (SELECT COUNT(*) FROM effect_local_client_canonical_entities_data
          WHERE space_id = ${spaceId} AND schema_generation = 4) AS target_count
        FROM effect_local_client_spaces AS m INNER JOIN effect_local_client_evolution AS e
          ON e.space_id = m.space_id WHERE m.space_id = ${spaceId}`)[
        0
      ]
      assert.deepStrictEqual(client, {
        active_schema_generation: 3,
        phase: "Log",
        source_generation: 3,
        source_count: 1,
        target_count: 0
      })
      const server = (yield* sql<{
        readonly active_schema_generation: number
        readonly phase: string
        readonly source_generation: number
        readonly source_count: number
        readonly target_count: number
        readonly target_entity_count: number
        readonly target_entity_bytes: number
      }>`SELECT s.active_schema_generation, e.phase, e.source_generation,
        e.target_entity_count, e.target_entity_bytes,
        (SELECT COUNT(*) FROM effect_local_server_entities_data
          WHERE space_id = ${spaceId} AND generation = 3) AS source_count,
        (SELECT COUNT(*) FROM effect_local_server_entities_data
          WHERE space_id = ${spaceId} AND generation = 4) AS target_count
        FROM effect_local_server_spaces AS s INNER JOIN effect_local_server_evolution AS e
          ON e.space_id = s.space_id WHERE s.space_id = ${spaceId}`)[0]
      assert.deepStrictEqual(server, {
        active_schema_generation: 3,
        phase: "Log",
        source_generation: 3,
        source_count: 1,
        target_count: 0,
        target_entity_count: 0,
        target_entity_bytes: 0
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
