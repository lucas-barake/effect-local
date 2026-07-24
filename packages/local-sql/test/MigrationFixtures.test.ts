import { NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { FileSystem } from "effect/FileSystem"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Migrations from "../src/Migrations.js"
import type { FixtureSpec, HistoricalVersion } from "./fixtures/versions.js"
import { fixturePath, fixtures } from "./fixtures/versions.js"

const expectations = {
  1: {
    applied: [
      [2, "peer_sync"],
      [3, "durability_indexes"],
      [4, "projection_readiness"],
      [5, "pending_receipt_indexes"]
    ],
    outbox: "none"
  },
  2: {
    applied: [
      [3, "durability_indexes"],
      [4, "projection_readiness"],
      [5, "pending_receipt_indexes"]
    ],
    outbox: "backfilled"
  },
  3: {
    applied: [
      [4, "projection_readiness"],
      [5, "pending_receipt_indexes"]
    ],
    outbox: { frozen: "2020-01-01T00:00:00.000Z" }
  }
} as const satisfies Record<
  HistoricalVersion,
  {
    readonly applied: ReadonlyArray<readonly [number, string]>
    readonly outbox: "none" | "backfilled" | { readonly frozen: string }
  }
>

const SchemaObject = Schema.Struct({
  type: Schema.String,
  name: Schema.String,
  table_name: Schema.String,
  definition: Schema.NullOr(Schema.String)
})

const normalizeSchema = (definition: string | null) =>
  definition === null ? null : definition.replace(/\s+/g, " ").trim()

const readSchema = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const rows = yield* SqlSchema.findAll({
    Request: Schema.Void,
    Result: SchemaObject,
    execute: () =>
      sql`SELECT type, name, tbl_name AS table_name, sql AS definition
        FROM sqlite_master
        WHERE name LIKE 'effect_local_%' OR tbl_name LIKE 'effect_local_%'
        ORDER BY type, name`
  })(undefined)
  return rows.map((row) => ({
    type: row.type,
    name: row.name,
    table_name: row.table_name,
    definition: normalizeSchema(row.definition)
  }))
})

const readCurrentSchema = Effect.gen(function*() {
  yield* Migrations.run
  return yield* readSchema
}).pipe(
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
  Effect.scoped
)

const assertDatabaseIntegrity = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const integrity = yield* SqlSchema.findAll({
    Request: Schema.Void,
    Result: Schema.Struct({ integrity_check: Schema.String }),
    execute: () => sql`PRAGMA integrity_check`
  })(undefined)
  assert.deepStrictEqual(integrity, [{ integrity_check: "ok" }])

  const foreignKeyViolations = yield* SqlSchema.findAll({
    Request: Schema.Void,
    Result: Schema.Struct({
      table: Schema.String,
      rowid: Schema.NullOr(Schema.Int),
      parent: Schema.String,
      fkid: Schema.Int
    }),
    execute: () => sql`PRAGMA foreign_key_check`
  })(undefined)
  assert.deepStrictEqual(foreignKeyViolations, [])
})

const assertMigrationHistory = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const history = yield* SqlSchema.findAll({
    Request: Schema.Void,
    Result: Schema.Struct({ migration_id: Schema.Int, name: Schema.String }),
    execute: () => sql`SELECT migration_id, name FROM effect_local_migrations ORDER BY migration_id`
  })(undefined)
  assert.deepStrictEqual(history, [
    { migration_id: 1, name: "canonical_store" },
    { migration_id: 2, name: "peer_sync" },
    { migration_id: 3, name: "durability_indexes" },
    { migration_id: 4, name: "projection_readiness" },
    { migration_id: 5, name: "pending_receipt_indexes" }
  ])

  const catalog = yield* SqlSchema.findAll({
    Request: Schema.Void,
    Result: Schema.Struct({ migration_id: Schema.Int, name: Schema.String, checksum: Schema.String }),
    execute: () => sql`SELECT migration_id, name, checksum FROM effect_local_migration_catalog ORDER BY migration_id`
  })(undefined)
  assert.deepStrictEqual(catalog, [
    { migration_id: 1, name: "canonical_store", checksum: Migrations.canonicalStoreChecksum },
    { migration_id: 2, name: "peer_sync", checksum: Migrations.peerSyncChecksum },
    { migration_id: 3, name: "durability_indexes", checksum: Migrations.durabilityIndexesChecksum },
    { migration_id: 4, name: "projection_readiness", checksum: Migrations.projectionReadinessChecksum },
    { migration_id: 5, name: "pending_receipt_indexes", checksum: Migrations.pendingReceiptIndexesChecksum }
  ])
})

const assertSeededDurabilityState = (version: HistoricalVersion) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const documents = yield* SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({
        document_id: Schema.String,
        document_type: Schema.String,
        schema_version: Schema.Int,
        observed_versions: Schema.String,
        materialized_heads: Schema.String,
        accepted_heads: Schema.String,
        tombstone: Schema.Int,
        projection_status: Schema.String,
        checkpoint_hash: Schema.NullOr(Schema.String)
      }),
      execute: () => sql`SELECT * FROM effect_local_documents ORDER BY document_id`
    })(undefined)
    assert.deepStrictEqual(documents, [{
      document_id: "task-1",
      document_type: "Task",
      schema_version: 1,
      observed_versions: "[]",
      materialized_heads: "[]",
      accepted_heads: "[]",
      tombstone: 0,
      projection_status: "ready",
      checkpoint_hash: null
    }])

    const checkpoints = yield* SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({
        checkpoint_hash: Schema.String,
        document_id: Schema.String,
        heads: Schema.String,
        bytes: Schema.Uint8Array,
        checksum: Schema.String,
        commit_sequence: Schema.Int,
        verified: Schema.Int
      }),
      execute: () => sql`SELECT * FROM effect_local_checkpoints ORDER BY checkpoint_hash`
    })(undefined)
    assert.deepStrictEqual(checkpoints, [
      {
        checkpoint_hash: "checkpoint-1",
        document_id: "task-1",
        heads: "[]",
        bytes: Uint8Array.of(1),
        checksum: "checksum-1",
        commit_sequence: 1,
        verified: 1
      },
      {
        checkpoint_hash: "checkpoint-3",
        document_id: "task-1",
        heads: "[]",
        bytes: Uint8Array.of(3),
        checksum: "checksum-3",
        commit_sequence: 3,
        verified: 1
      }
    ])

    const outbox = yield* SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({
        replica_incarnation: Schema.Int,
        peer_id: Schema.String,
        connection_epoch: Schema.String,
        document_id: Schema.String,
        send_sequence: Schema.Int,
        message: Schema.Uint8Array,
        message_hash: Schema.String,
        heads: Schema.String,
        status: Schema.String,
        created_at: Schema.String
      }),
      execute: () => sql`SELECT * FROM effect_local_peer_outbox ORDER BY send_sequence`
    })(undefined)
    if (expectations[version].outbox === "none") {
      assert.deepStrictEqual(outbox, [])
      return
    }
    assert.deepStrictEqual(
      outbox.map(({ created_at: _, ...row }) => row),
      [{
        replica_incarnation: 0,
        peer_id: "peer-1",
        connection_epoch: "connection-1",
        document_id: "task-1",
        send_sequence: 1,
        message: Uint8Array.of(1),
        message_hash: "message-1",
        heads: "[]",
        status: "pending"
      }]
    )
    const createdAt = outbox[0]?.created_at ?? ""
    const expectedOutbox = expectations[version].outbox
    if (expectedOutbox === "backfilled") {
      const timestamp = Date.parse(createdAt)
      assert.isFalse(Number.isNaN(timestamp), `invalid backfilled timestamp ${createdAt}`)
      assert.strictEqual(new Date(timestamp).toISOString(), createdAt)
    } else {
      assert.strictEqual(createdAt, expectedOutbox.frozen)
    }
  })

const assertUpgradesToCurrentSchema = (spec: FixtureSpec) =>
  Effect.gen(function*() {
    const applied = yield* Migrations.run
    assert.deepStrictEqual(applied, expectations[spec.version].applied)

    yield* assertDatabaseIntegrity
    yield* assertMigrationHistory
    assert.deepStrictEqual(yield* readSchema, yield* readCurrentSchema)
    yield* assertSeededDurabilityState(spec.version)
  })

describe("Migrations against frozen historical fixtures", () => {
  for (const spec of fixtures) {
    it.effect(`upgrades the version ${spec.version} database to the current schema`, () =>
      Effect.gen(function*() {
        const fs = yield* FileSystem
        const dir = yield* fs.makeTempDirectoryScoped()
        const database = `${dir}/${spec.file}`
        yield* fs.copyFile(fixturePath(spec.file), database)
        yield* assertUpgradesToCurrentSchema(spec).pipe(
          Effect.provide(SqliteClient.layer({ filename: database, disableWAL: true }))
        )
      }).pipe(Effect.provide(NodeFileSystem.layer)))
  }
})
