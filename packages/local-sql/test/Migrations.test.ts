import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { tables } from "../src/internal/schema.js"
import * as Migrations from "../src/Migrations.js"

describe("Migrations", () => {
  it.effect("creates every canonical table", () =>
    Effect.gen(function*() {
      yield* Migrations.run
      const sql = yield* SqlClient.SqlClient
      const rows = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'effect_local_%'
      `
      const names = new Set(rows.map((row) => row.name))
      for (const table of tables) assert.isTrue(names.has(table))
      const indexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'effect_local_peer_%'
      `
      assert.deepStrictEqual(indexes.map((row) => row.name).toSorted(), [
        "effect_local_peer_outbox_connection_status",
        "effect_local_peer_outbox_incarnation_created",
        "effect_local_peer_receipts_document_sequence",
        "effect_local_peer_receipts_incarnation_accepted"
      ])
      const readinessIndexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'index' AND name IN (
          'effect_local_checkpoints_document_verified_sequence',
          'effect_local_commit_outbox_published_sequence',
          'effect_local_document_projections_not_ready',
          'effect_local_documents_type_projection_status',
          'effect_local_projection_registry_name_status'
        )
      `
      assert.deepStrictEqual(readinessIndexes.map((row) => row.name).toSorted(), [
        "effect_local_checkpoints_document_verified_sequence",
        "effect_local_commit_outbox_published_sequence",
        "effect_local_document_projections_not_ready",
        "effect_local_documents_type_projection_status",
        "effect_local_projection_registry_name_status"
      ])

      const readinessPlan = yield* sql<{ readonly detail: string }>`EXPLAIN QUERY PLAN
        SELECT COUNT(*) FROM effect_local_document_projections
        WHERE projection_name = 'tasks' AND status != 'Ready'
      `
      assert.isTrue(
        readinessPlan.some((row) => row.detail.includes("effect_local_document_projections_not_ready"))
      )
      yield* sql`UPDATE effect_local_migration_catalog SET checksum = 'changed' WHERE migration_id = 1`
      assert.strictEqual((yield* Effect.exit(Migrations.run))._tag, "Failure")
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))

  it.effect("rejects a corrupt catalog without committing pending migrations", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrator.make({})({
        loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, 3)),
        table: "effect_local_migrations"
      })
      yield* sql`UPDATE effect_local_migration_catalog SET checksum = 'corrupt' WHERE migration_id = 1`

      const error = yield* Effect.flip(Migrations.run)
      assert.strictEqual(error._tag, "MigrationError")
      assert.include(error.message, "Canonical store")

      const index = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master
        WHERE type = 'index' AND name = 'effect_local_document_projections_not_ready'
      `
      assert.strictEqual(index.length, 0)
      const recorded = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_local_migrations WHERE migration_id = 4
      `
      assert.strictEqual(recorded.length, 0)
      const catalog = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_local_migration_catalog WHERE migration_id = 4
      `
      assert.strictEqual(catalog.length, 0)
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))

  it.effect("upgrades populated version two storage without losing durability state", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrator.make({})({
        loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, 2)),
        table: "effect_local_migrations"
      })
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions, materialized_heads,
        accepted_heads, tombstone, projection_status, checkpoint_hash
      ) VALUES ('task-1', 'Task', 1, '[]', '[]', '[]', 0, 'ready', NULL)`
      yield* sql`INSERT INTO effect_local_peer_outbox (
        replica_incarnation, peer_id, connection_epoch, document_id, send_sequence,
        message, message_hash, heads, status
      ) VALUES (0, 'peer-1', 'connection-1', 'task-1', 1, ${new Uint8Array([1])}, 'message-1', '[]', 'pending')`
      yield* sql`INSERT INTO effect_local_checkpoints (
        checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified
      ) VALUES
        ('checkpoint-1', 'task-1', '[]', ${new Uint8Array([1])}, 'checksum-1', 1, 1),
        ('checkpoint-2', 'task-1', '[]', ${new Uint8Array([2])}, 'checksum-2', 2, 0),
        ('checkpoint-3', 'task-1', '[]', ${new Uint8Array([3])}, 'checksum-3', 3, 1),
        ('checkpoint-4', 'task-1', '[]', ${new Uint8Array([4])}, 'checksum-4', 4, 0)`

      const applied = yield* Migrations.run
      assert.deepStrictEqual(applied, [[3, "durability_indexes"], [4, "projection_readiness"]])

      const outbox = yield* sql<{ readonly created_at: string }>`
        SELECT created_at FROM effect_local_peer_outbox WHERE message_hash = 'message-1'
      `
      assert.match(outbox[0]?.created_at ?? "", /^\d{4}-\d{2}-\d{2}T/)

      const checkpoints = yield* sql<{ readonly checkpoint_hash: string }>`
        SELECT checkpoint_hash FROM effect_local_checkpoints
        WHERE document_id = 'task-1'
        ORDER BY checkpoint_hash
      `
      assert.deepStrictEqual(checkpoints.map((row) => row.checkpoint_hash), ["checkpoint-1", "checkpoint-3"])

      const catalog = yield* sql<{
        readonly checksum: string
        readonly migration_id: number
        readonly name: string
      }>`SELECT migration_id, name, checksum FROM effect_local_migration_catalog ORDER BY migration_id`
      assert.deepStrictEqual(catalog, [
        { migration_id: 1, name: "canonical_store", checksum: Migrations.canonicalStoreChecksum },
        { migration_id: 2, name: "peer_sync", checksum: Migrations.peerSyncChecksum },
        { migration_id: 3, name: "durability_indexes", checksum: Migrations.durabilityIndexesChecksum },
        { migration_id: 4, name: "projection_readiness", checksum: Migrations.projectionReadinessChecksum }
      ])

      const indexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'index' AND name IN (
          'effect_local_checkpoints_document_verified_sequence',
          'effect_local_commit_outbox_published_sequence',
          'effect_local_document_projections_not_ready',
          'effect_local_documents_type_projection_status',
          'effect_local_projection_registry_name_status',
          'effect_local_peer_outbox_incarnation_created',
          'effect_local_peer_receipts_incarnation_accepted'
        ) ORDER BY name
      `
      assert.deepStrictEqual(indexes.map((row) => row.name), [
        "effect_local_checkpoints_document_verified_sequence",
        "effect_local_commit_outbox_published_sequence",
        "effect_local_document_projections_not_ready",
        "effect_local_documents_type_projection_status",
        "effect_local_peer_outbox_incarnation_created",
        "effect_local_peer_receipts_incarnation_accepted",
        "effect_local_projection_registry_name_status"
      ])
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))
})
