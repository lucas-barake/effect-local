import * as Automerge from "@automerge/automerge"
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
        "effect_local_peer_receipts_incarnation_accepted",
        "effect_local_peer_receipts_pending_document",
        "effect_local_peer_receipts_pending_peer"
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
      const receiptQuotaPlan = yield* sql<{ readonly detail: string }>`EXPLAIN QUERY PLAN
        SELECT
          (SELECT COUNT(*) FROM effect_local_peer_receipts
            WHERE replica_incarnation = 1
              AND document_id = 'document-1'
              AND pending_message IS NOT NULL),
          (SELECT COUNT(*) FROM effect_local_peer_receipts
            WHERE replica_incarnation = 1
              AND peer_id = 'peer-1'
              AND pending_message IS NOT NULL),
          (SELECT COUNT(*) FROM effect_local_peer_receipts
            WHERE replica_incarnation = 1
              AND pending_message IS NOT NULL)
      `
      assert.isTrue(
        receiptQuotaPlan.some((row) => row.detail.includes("effect_local_peer_receipts_pending_document"))
      )
      assert.isTrue(
        receiptQuotaPlan.some((row) => row.detail.includes("effect_local_peer_receipts_pending_peer"))
      )
      assert.strictEqual(
        receiptQuotaPlan.filter((row) => row.detail.includes("effect_local_peer_receipts_pending_")).length,
        3
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

      const indexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master
        WHERE type = 'index' AND name IN (
          'effect_local_document_projections_not_ready',
          'effect_local_peer_receipts_pending_document',
          'effect_local_peer_receipts_pending_peer'
        )
      `
      assert.strictEqual(indexes.length, 0)
      const recorded = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_local_migrations WHERE migration_id IN (4, 5, 6)
      `
      assert.strictEqual(recorded.length, 0)
      const catalog = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_local_migration_catalog WHERE migration_id IN (4, 5, 6)
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
      yield* sql`INSERT INTO effect_local_metadata (
        singleton, storage_format_version, replica_id, replica_incarnation,
        writer_generation, definition_hash, commit_sequence
      ) VALUES (1, 1, 'replica-1', 0, 1, 'definition-1', 1)`
      yield* sql`INSERT INTO effect_local_changes (
        change_hash, document_id, document_type, writer_schema_version,
        writer_definition_hash, actor, sequence, dependencies, bytes, applied,
        peer_id, accepted_at, commit_sequence
      ) VALUES (
        ${"a".repeat(64)}, 'task-1', 'Task', 1, 'local', ${"b".repeat(32)}, 1,
        '[]', ${new Uint8Array([1])}, 1, NULL, '2026-01-01T00:00:00.000Z', 1
      )`
      yield* sql`INSERT INTO effect_local_peer_outbox (
        replica_incarnation, peer_id, connection_epoch, document_id, send_sequence,
        message, message_hash, heads, status
      ) VALUES (0, 'peer-1', 'connection-1', 'task-1', 1, ${new Uint8Array([1])}, 'message-1', '[]', 'pending')`
      let legacyCheckpoint = Automerge.from(
        { value: { title: "one" }, tombstone: false },
        { actor: "c".repeat(32) }
      )
      const legacyCheckpointBytes = Automerge.save(legacyCheckpoint)
      const legacyChangeHash = Automerge.decodeChange(Automerge.getAllChanges(legacyCheckpoint)[0]!).hash
      const emptyPeer = Automerge.init()
      const handshake = Automerge.generateSyncMessage(emptyPeer, Automerge.initSyncState())[1]!
      const receivedHandshake = Automerge.receiveSyncMessage(
        legacyCheckpoint,
        Automerge.initSyncState(),
        handshake
      )
      legacyCheckpoint = receivedHandshake[0]
      const legacySyncMessage = Automerge.generateSyncMessage(legacyCheckpoint, receivedHandshake[1])[1]!
      yield* sql`INSERT INTO effect_local_checkpoints (
        checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified
      ) VALUES
        ('checkpoint-1', 'task-1', '[]', ${new Uint8Array([1])}, 'checksum-1', 1, 1),
        ('checkpoint-2', 'task-1', '[]', ${new Uint8Array([2])}, 'checksum-2', 2, 0),
        ('checkpoint-3', 'task-1', '[]', ${legacyCheckpointBytes}, 'checksum-3', 3, 1),
        ('checkpoint-4', 'task-1', '[]', ${new Uint8Array([4])}, 'checksum-4', 4, 0)`
      yield* sql`INSERT INTO effect_local_peer_outbox (
        replica_incarnation, peer_id, connection_epoch, document_id, send_sequence,
        message, message_hash, heads, status
      ) VALUES (
        0, 'peer-1', 'connection-1', 'task-1', 2,
        ${legacySyncMessage}, 'valid-message', '[]', 'pending'
      )`
      yield* sql`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
        message_hash, reply, reply_hash, pending_message, heads, accepted_heads,
        commit_sequence, accepted_at
      ) VALUES
        (
          0, 'peer-1', 'remote-1', 0, 'task-1', 'valid-incoming',
          NULL, NULL, ${legacySyncMessage}, '[]', '[]', 1, '2026-01-01T00:00:00.000Z'
        ),
        (
          0, 'peer-1', 'remote-1', 1, 'task-1', 'resolved-incoming',
          NULL, NULL, NULL, '[]', '[]', 1, '2026-01-01T00:00:00.000Z'
        ),
        (
          0, 'peer-1', 'remote-1', 2, 'task-1', 'malformed-incoming',
          NULL, NULL, ${new Uint8Array([1])}, '[]', '[]', 1, '2026-01-01T00:00:00.000Z'
        )`

      const applied = yield* Migrations.run
      assert.deepStrictEqual(applied, [
        [3, "durability_indexes"],
        [4, "projection_readiness"],
        [5, "pending_receipt_indexes"],
        [6, "peer_writer_provenance"]
      ])

      const outbox = yield* sql<{
        readonly created_at: string
        readonly writer_provenance: string
      }>`
        SELECT created_at, writer_provenance
        FROM effect_local_peer_outbox
        WHERE message_hash = 'message-1'
      `
      assert.match(outbox[0]?.created_at ?? "", /^\d{4}-\d{2}-\d{2}T/)
      assert.strictEqual(outbox[0]?.writer_provenance, "[]")

      const changes = yield* sql<{ readonly writer_definition_hash: string }>`
        SELECT writer_definition_hash
        FROM effect_local_changes
        WHERE change_hash = ${"a".repeat(64)}
      `
      assert.strictEqual(changes[0]?.writer_definition_hash, "definition-1")

      const receiptColumns = yield* sql<{ readonly name: string }>`
        SELECT name FROM pragma_table_info('effect_local_peer_receipts')
      `
      assert.isTrue(receiptColumns.some((column) => column.name === "writer_provenance"))

      const checkpoints = yield* sql<{
        readonly checkpoint_hash: string
        readonly writer_provenance: string
      }>`
        SELECT checkpoint_hash, writer_provenance FROM effect_local_checkpoints
        WHERE document_id = 'task-1'
        ORDER BY checkpoint_hash
      `
      assert.deepStrictEqual(checkpoints.map((row) => row.checkpoint_hash), ["checkpoint-1", "checkpoint-3"])
      assert.strictEqual(checkpoints[0]?.writer_provenance, "[]")
      assert.deepStrictEqual(JSON.parse(checkpoints[1]!.writer_provenance), [{
        changeHash: legacyChangeHash,
        writerDefinitionHash: "definition-1",
        writerSchemaVersion: 1
      }])
      const migratedPeerRows = yield* sql<{
        readonly kind: string
        readonly writer_provenance: string
      }>`SELECT 'outbox' AS kind, writer_provenance FROM effect_local_peer_outbox
        WHERE message_hash = 'valid-message'
        UNION ALL
        SELECT 'receipt' AS kind, writer_provenance FROM effect_local_peer_receipts
        WHERE message_hash = 'valid-incoming'
        ORDER BY kind`
      const expectedLegacyProvenance = [{
        changeHash: legacyChangeHash,
        writerDefinitionHash: "definition-1",
        writerSchemaVersion: 1
      }]
      assert.deepStrictEqual(
        migratedPeerRows.map((row) => ({
          kind: row.kind,
          writerProvenance: JSON.parse(row.writer_provenance)
        })),
        [
          { kind: "outbox", writerProvenance: expectedLegacyProvenance },
          { kind: "receipt", writerProvenance: expectedLegacyProvenance }
        ]
      )
      assert.deepStrictEqual(
        yield* sql`SELECT message_hash FROM effect_local_peer_receipts
          WHERE message_hash = 'resolved-incoming'`,
        []
      )
      assert.deepStrictEqual(
        yield* sql`SELECT writer_provenance FROM effect_local_peer_receipts
          WHERE message_hash = 'malformed-incoming'`,
        [{ writer_provenance: "[]" }]
      )
      Automerge.free(legacyCheckpoint)
      Automerge.free(emptyPeer)

      const catalog = yield* sql<{
        readonly checksum: string
        readonly migration_id: number
        readonly name: string
      }>`SELECT migration_id, name, checksum FROM effect_local_migration_catalog ORDER BY migration_id`
      assert.deepStrictEqual(catalog, [
        { migration_id: 1, name: "canonical_store", checksum: Migrations.canonicalStoreChecksum },
        { migration_id: 2, name: "peer_sync", checksum: Migrations.peerSyncChecksum },
        { migration_id: 3, name: "durability_indexes", checksum: Migrations.durabilityIndexesChecksum },
        { migration_id: 4, name: "projection_readiness", checksum: Migrations.projectionReadinessChecksum },
        { migration_id: 5, name: "pending_receipt_indexes", checksum: Migrations.pendingReceiptIndexesChecksum },
        { migration_id: 6, name: "peer_writer_provenance", checksum: Migrations.peerWriterProvenanceChecksum }
      ])

      const indexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'index' AND name IN (
          'effect_local_checkpoints_document_verified_sequence',
          'effect_local_commit_outbox_published_sequence',
          'effect_local_document_projections_not_ready',
          'effect_local_documents_type_projection_status',
          'effect_local_projection_registry_name_status',
          'effect_local_peer_outbox_incarnation_created',
          'effect_local_peer_receipts_incarnation_accepted',
          'effect_local_peer_receipts_pending_document',
          'effect_local_peer_receipts_pending_peer'
        ) ORDER BY name
      `
      assert.deepStrictEqual(indexes.map((row) => row.name), [
        "effect_local_checkpoints_document_verified_sequence",
        "effect_local_commit_outbox_published_sequence",
        "effect_local_document_projections_not_ready",
        "effect_local_documents_type_projection_status",
        "effect_local_peer_outbox_incarnation_created",
        "effect_local_peer_receipts_incarnation_accepted",
        "effect_local_peer_receipts_pending_document",
        "effect_local_peer_receipts_pending_peer",
        "effect_local_projection_registry_name_status"
      ])
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))
})
