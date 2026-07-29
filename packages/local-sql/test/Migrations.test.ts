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
        "effect_local_peer_receipts_direct_identity",
        "effect_local_peer_receipts_document_sequence",
        "effect_local_peer_receipts_incarnation_accepted",
        "effect_local_peer_receipts_pending_document",
        "effect_local_peer_receipts_pending_peer",
        "effect_local_peer_receipts_relay_expiry",
        "effect_local_peer_receipts_relay_identity",
        "effect_local_peer_relay_delivery_changes_hash",
        "effect_local_peer_relay_delivery_messages_document",
        "effect_local_peer_relay_outbox_due_endpoint",
        "effect_local_peer_relay_outbox_retry_deadline"
      ])
      const readinessIndexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'index' AND name IN (
          'effect_local_checkpoints_document_verified_sequence',
          'effect_local_commit_outbox_published_sequence',
          'effect_local_document_projections_not_ready',
          'effect_local_documents_not_ready_type',
          'effect_local_documents_type_projection_status',
          'effect_local_projection_registry_name_status'
        )
      `
      assert.deepStrictEqual(readinessIndexes.map((row) => row.name).toSorted(), [
        "effect_local_checkpoints_document_verified_sequence",
        "effect_local_commit_outbox_published_sequence",
        "effect_local_document_projections_not_ready",
        "effect_local_documents_not_ready_type",
        "effect_local_documents_type_projection_status",
        "effect_local_projection_registry_name_status"
      ])

      yield* sql`WITH RECURSIVE documents(id) AS (
          VALUES(1)
          UNION ALL
          SELECT id + 1 FROM documents WHERE id < 10000
        )
        INSERT INTO effect_local_documents (
          document_id, document_type, schema_version, observed_versions, materialized_heads,
          accepted_heads, tombstone, projection_status
        )
        SELECT printf('document-%05d', id), 'Task', 1, '[]', '[]', '[]', 0, 'Ready'
        FROM documents`
      yield* sql`UPDATE effect_local_documents SET projection_status = 'Blocked'
        WHERE document_id = 'document-10000'`
      yield* sql`ANALYZE`
      const blockedDocumentPlan = yield* sql<{ readonly detail: string }>`EXPLAIN QUERY PLAN
        SELECT DISTINCT document_type FROM effect_local_documents
        WHERE projection_status != 'Ready' ORDER BY document_type
      `
      assert.isTrue(
        blockedDocumentPlan.some((row) => row.detail.includes("effect_local_documents_not_ready_type")),
        JSON.stringify(blockedDocumentPlan)
      )
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
          'effect_local_documents_not_ready_type',
          'effect_local_peer_receipts_pending_document',
          'effect_local_peer_receipts_pending_peer'
        )
      `
      assert.strictEqual(indexes.length, 0)
      const recorded = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_local_migrations WHERE migration_id IN (4, 5, 6, 7, 8, 9, 10)
      `
      assert.strictEqual(recorded.length, 0)
      const catalog = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_local_migration_catalog WHERE migration_id IN (4, 5, 6, 7, 8, 9, 10)
      `
      assert.strictEqual(catalog.length, 0)
      const receiptPrimaryKey = yield* sql<{ readonly name: string; readonly pk: number }>`
        SELECT name, pk FROM pragma_table_info('effect_local_peer_receipts')
        WHERE pk != 0 ORDER BY pk
      `
      assert.deepStrictEqual(receiptPrimaryKey, [
        { name: "replica_incarnation", pk: 1 },
        { name: "peer_id", pk: 2 },
        { name: "connection_epoch", pk: 3 },
        { name: "receive_sequence", pk: 4 }
      ])
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master
          WHERE type = 'table' AND name = 'effect_local_peer_relay_outbox'`,
        []
      )
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
        [6, "peer_writer_provenance"],
        [7, "replica_health_indexes"],
        [8, "document_lineage"],
        [9, "history_rewrite_markers"],
        [10, "peer_relay_state"],
        [11, "command_delivery"]
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
      for (
        const name of [
          "relay_sender_tenant_id",
          "relay_sender_subject_id",
          "relay_sender_peer_id",
          "relay_message_id",
          "relay_outer_envelope_digest",
          "relay_receipt_expires_at",
          "relay_encoded_size"
        ]
      ) {
        assert.isTrue(receiptColumns.some((column) => column.name === name))
      }
      const receiptPrimaryKey = yield* sql<{ readonly name: string; readonly pk: number }>`
        SELECT name, pk FROM pragma_table_info('effect_local_peer_receipts')
        WHERE pk != 0
      `
      assert.deepStrictEqual(receiptPrimaryKey, [{ name: "row_id", pk: 1 }])

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
      assert.deepStrictEqual(
        yield* sql`SELECT
          relay_sender_tenant_id,
          relay_sender_subject_id,
          relay_sender_peer_id,
          relay_message_id,
          relay_outer_envelope_digest,
          relay_receipt_expires_at,
          relay_encoded_size
        FROM effect_local_peer_receipts
        WHERE message_hash = 'malformed-incoming'`,
        [{
          relay_sender_tenant_id: null,
          relay_sender_subject_id: null,
          relay_sender_peer_id: null,
          relay_message_id: null,
          relay_outer_envelope_digest: null,
          relay_receipt_expires_at: null,
          relay_encoded_size: null
        }]
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
        { migration_id: 6, name: "peer_writer_provenance", checksum: Migrations.peerWriterProvenanceChecksum },
        { migration_id: 7, name: "replica_health_indexes", checksum: Migrations.replicaHealthIndexesChecksum },
        { migration_id: 8, name: "document_lineage", checksum: Migrations.documentLineageChecksum },
        {
          migration_id: 9,
          name: "history_rewrite_markers",
          checksum: Migrations.historyRewriteMarkersChecksum
        },
        { migration_id: 10, name: "peer_relay_state", checksum: Migrations.peerRelayStateChecksum },
        { migration_id: 11, name: "command_delivery", checksum: Migrations.commandDeliveryChecksum }
      ])

      const indexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'index' AND name IN (
          'effect_local_checkpoints_document_verified_sequence',
          'effect_local_commit_outbox_published_sequence',
          'effect_local_document_projections_not_ready',
          'effect_local_documents_not_ready_type',
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
        "effect_local_documents_not_ready_type",
        "effect_local_documents_type_projection_status",
        "effect_local_peer_outbox_incarnation_created",
        "effect_local_peer_receipts_incarnation_accepted",
        "effect_local_peer_receipts_pending_document",
        "effect_local_peer_receipts_pending_peer",
        "effect_local_projection_registry_name_status"
      ])
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))

  it.effect("upgrades populated relay custody state without changing storage format", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrator.make({})({
        loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, 10)),
        table: "effect_local_migrations"
      })
      yield* sql`INSERT INTO effect_local_metadata (
        singleton, storage_format_version, replica_id, replica_incarnation,
        writer_generation, definition_hash, commit_sequence
      ) VALUES (
        1, 1, 'rep_00000000-0000-4000-8000-000000000001', 3,
        7, 'definition-1', 11
      )`
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions, materialized_heads,
        accepted_heads, tombstone, projection_status
      ) VALUES (
        'doc_00000000-0000-4000-8000-000000000001',
        'Task',
        1,
        '[]',
        '[]',
        '[]',
        0,
        'Ready'
      )`

      const pendingPayload = Uint8Array.of(1, 2, 3)
      const inFlightPayload = Uint8Array.of(4, 5, 6, 7)
      yield* sql`INSERT INTO effect_local_peer_relay_outbox (
        row_id, replica_id, replica_incarnation, writer_generation,
        expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
        remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
        relay_message_id, outer_envelope_digest, protocol_version, payload_version,
        sender_connection_epoch, sender_sequence, document_id, document_type,
        writer_provenance, message_hash, payload, encoded_size, created_at,
        retry_deadline, next_attempt_at, custody_state
      ) VALUES
        (
          41, 'rep_00000000-0000-4000-8000-000000000001', 3, 7,
          'tenant-1', 'local-subject', 'local-peer',
          'tenant-1', 'remote-subject-1', 'remote-peer-1', 'relay-peer-1',
          'relay-message-1', ${"a".repeat(64)}, 1, 1,
          'sender-epoch-1', 101,
          'doc_00000000-0000-4000-8000-000000000001', 'Task',
          '[{"changeHash":"change-1"}]', ${"b".repeat(64)},
          ${pendingPayload}, ${pendingPayload.byteLength},
          '2026-01-01T00:00:00.000Z', '2026-01-08T00:00:00.000Z',
          '2026-01-02T00:00:00.000Z', 'Pending'
        ),
        (
          42, 'rep_00000000-0000-4000-8000-000000000001', 3, 8,
          'tenant-1', 'local-subject', 'local-peer',
          'tenant-1', 'remote-subject-2', 'remote-peer-2', 'relay-peer-2',
          'relay-message-2', ${"c".repeat(64)}, 1, 1,
          'sender-epoch-2', 102,
          'doc_00000000-0000-4000-8000-000000000001', 'Task',
          '[{"changeHash":"change-2"}]', ${"d".repeat(64)},
          ${inFlightPayload}, ${inFlightPayload.byteLength},
          '2026-02-01T00:00:00.000Z', '2026-02-08T00:00:00.000Z',
          '2026-02-02T00:00:00.000Z', 'InFlight'
        )`
      yield* sql`INSERT INTO effect_local_peer_relay_outbox_remote_usage (
        replica_incarnation, remote_tenant_id, remote_subject_id, remote_peer_id,
        message_count, encoded_bytes
      ) VALUES
        (3, 'tenant-1', 'remote-subject-1', 'remote-peer-1', 1, ${pendingPayload.byteLength}),
        (3, 'tenant-1', 'remote-subject-2', 'remote-peer-2', 1, ${inFlightPayload.byteLength})`
      yield* sql`INSERT INTO effect_local_peer_relay_outbox_replica_usage (
        replica_incarnation, message_count, encoded_bytes
      ) VALUES (3, 2, ${pendingPayload.byteLength + inFlightPayload.byteLength})`

      const selectDurableOutbox = sql<{
        readonly created_at: string
        readonly document_id: string
        readonly document_type: string
        readonly encoded_size: number
        readonly expected_local_peer_id: string
        readonly expected_local_subject_id: string
        readonly expected_local_tenant_id: string
        readonly message_hash: string
        readonly next_attempt_at: string
        readonly outer_envelope_digest: string
        readonly payload_hex: string
        readonly payload_version: number
        readonly protocol_version: number
        readonly relay_message_id: string
        readonly relay_peer_id: string
        readonly remote_peer_id: string
        readonly remote_subject_id: string
        readonly remote_tenant_id: string
        readonly replica_id: string
        readonly replica_incarnation: number
        readonly retry_deadline: string
        readonly row_id: number
        readonly sender_connection_epoch: string
        readonly sender_sequence: number
        readonly writer_generation: number
        readonly writer_provenance: string
      }>`SELECT
        row_id, replica_id, replica_incarnation, writer_generation,
        expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
        remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
        relay_message_id, outer_envelope_digest, protocol_version, payload_version,
        sender_connection_epoch, sender_sequence, document_id, document_type,
        writer_provenance, message_hash, hex(payload) AS payload_hex, encoded_size,
        created_at, retry_deadline, next_attempt_at
      FROM effect_local_peer_relay_outbox
      ORDER BY row_id`
      const outboxBefore = yield* selectDurableOutbox
      const remoteUsageBefore = yield* sql`SELECT *
        FROM effect_local_peer_relay_outbox_remote_usage
        ORDER BY remote_subject_id`
      const replicaUsageBefore = yield* sql`SELECT *
        FROM effect_local_peer_relay_outbox_replica_usage
        ORDER BY replica_incarnation`

      assert.deepStrictEqual(yield* Migrations.run, [[11, "command_delivery"]])
      assert.deepStrictEqual(yield* selectDurableOutbox, outboxBefore)
      assert.deepStrictEqual(
        yield* sql`SELECT *
          FROM effect_local_peer_relay_outbox_remote_usage
          ORDER BY remote_subject_id`,
        remoteUsageBefore
      )
      assert.deepStrictEqual(
        yield* sql`SELECT *
          FROM effect_local_peer_relay_outbox_replica_usage
          ORDER BY replica_incarnation`,
        replicaUsageBefore
      )

      const outboxColumns = yield* sql<{ readonly name: string }>`
        SELECT name FROM pragma_table_info('effect_local_peer_relay_outbox')
      `
      assert.isFalse(outboxColumns.some((column) => column.name === "custody_state"))
      assert.deepStrictEqual(
        yield* sql`SELECT storage_format_version FROM effect_local_metadata WHERE singleton = 1`,
        [{ storage_format_version: 1 }]
      )
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id, name
          FROM effect_local_migrations
          ORDER BY migration_id`,
        [
          { migration_id: 1, name: "canonical_store" },
          { migration_id: 2, name: "peer_sync" },
          { migration_id: 3, name: "durability_indexes" },
          { migration_id: 4, name: "projection_readiness" },
          { migration_id: 5, name: "pending_receipt_indexes" },
          { migration_id: 6, name: "peer_writer_provenance" },
          { migration_id: 7, name: "replica_health_indexes" },
          { migration_id: 8, name: "document_lineage" },
          { migration_id: 9, name: "history_rewrite_markers" },
          { migration_id: 10, name: "peer_relay_state" },
          { migration_id: 11, name: "command_delivery" }
        ]
      )
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id, name, checksum
          FROM effect_local_migration_catalog
          WHERE migration_id = 11`,
        [{
          migration_id: 11,
          name: "command_delivery",
          checksum: Migrations.commandDeliveryChecksum
        }]
      )
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master
          WHERE type = 'index'
            AND name IN (
              'effect_local_peer_relay_outbox_due_endpoint',
              'effect_local_peer_relay_outbox_retry_deadline'
            )
          ORDER BY name`,
        [
          { name: "effect_local_peer_relay_outbox_due_endpoint" },
          { name: "effect_local_peer_relay_outbox_retry_deadline" }
        ]
      )
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))

  it.effect("rolls back migration eleven when relay outbox copy fails", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Migrator.make({})({
        loader: Effect.map(Migrations.loader, (migrations) => migrations.slice(0, 10)),
        table: "effect_local_migrations"
      })
      yield* sql`INSERT INTO effect_local_metadata (
        singleton, storage_format_version, replica_id, replica_incarnation,
        writer_generation, definition_hash, commit_sequence
      ) VALUES (
        1, 1, 'rep_00000000-0000-4000-8000-000000000001', 0,
        1, 'definition-1', 0
      )`
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions, materialized_heads,
        accepted_heads, tombstone, projection_status
      ) VALUES (
        'doc_00000000-0000-4000-8000-000000000001',
        'Task',
        1,
        '[]',
        '[]',
        '[]',
        0,
        'Ready'
      )`

      const payload = Uint8Array.of(1, 2, 3)
      yield* sql`PRAGMA ignore_check_constraints = ON`
      yield* sql`INSERT INTO effect_local_peer_relay_outbox (
        replica_id, replica_incarnation, writer_generation,
        expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
        remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
        relay_message_id, outer_envelope_digest, protocol_version, payload_version,
        sender_connection_epoch, sender_sequence, document_id, document_type,
        writer_provenance, message_hash, payload, encoded_size, created_at,
        retry_deadline, next_attempt_at, custody_state
      ) VALUES (
        'rep_00000000-0000-4000-8000-000000000001', 0, 1,
        'tenant-local', 'local-subject', 'local-peer',
        'tenant-remote', 'remote-subject', 'remote-peer', 'relay-peer',
        'relay-message-1', ${"a".repeat(64)}, 1, 1,
        'sender-epoch-1', 1,
        'doc_00000000-0000-4000-8000-000000000001', 'Task',
        '[]', ${"b".repeat(64)}, ${payload}, ${payload.byteLength},
        '2026-01-01T00:00:00.000Z', '2026-01-08T00:00:00.000Z',
        '2026-01-02T00:00:00.000Z', 'InFlight'
      )`
      yield* sql`PRAGMA ignore_check_constraints = OFF`

      const outboxBefore = yield* sql`SELECT
        row_id, replica_id, replica_incarnation, writer_generation,
        expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
        remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
        relay_message_id, outer_envelope_digest, protocol_version, payload_version,
        sender_connection_epoch, sender_sequence, document_id, document_type,
        writer_provenance, message_hash, hex(payload) AS payload_hex, encoded_size,
        created_at, retry_deadline, next_attempt_at, custody_state
      FROM effect_local_peer_relay_outbox`
      const indexesBefore = yield* sql`SELECT name, sql
        FROM sqlite_master
        WHERE type = 'index'
          AND tbl_name = 'effect_local_peer_relay_outbox'
        ORDER BY name`
      const catalogBefore = yield* sql`SELECT migration_id, name, checksum
        FROM effect_local_migration_catalog
        ORDER BY migration_id`
      const historyBefore = yield* sql`SELECT migration_id, name
        FROM effect_local_migrations
        ORDER BY migration_id`

      assert.strictEqual((yield* Effect.exit(Migrations.run))._tag, "Failure")

      const outboxColumns = yield* sql<{ readonly name: string }>`
        SELECT name FROM pragma_table_info('effect_local_peer_relay_outbox')
      `
      assert.isTrue(outboxColumns.some((column) => column.name === "custody_state"))
      assert.deepStrictEqual(
        yield* sql`SELECT
          row_id, replica_id, replica_incarnation, writer_generation,
          expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
          remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
          relay_message_id, outer_envelope_digest, protocol_version, payload_version,
          sender_connection_epoch, sender_sequence, document_id, document_type,
          writer_provenance, message_hash, hex(payload) AS payload_hex, encoded_size,
          created_at, retry_deadline, next_attempt_at, custody_state
        FROM effect_local_peer_relay_outbox`,
        outboxBefore
      )
      assert.deepStrictEqual(
        yield* sql`SELECT name, sql
          FROM sqlite_master
          WHERE type = 'index'
            AND tbl_name = 'effect_local_peer_relay_outbox'
          ORDER BY name`,
        indexesBefore
      )
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id, name, checksum
          FROM effect_local_migration_catalog
          ORDER BY migration_id`,
        catalogBefore
      )
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id, name
          FROM effect_local_migrations
          ORDER BY migration_id`,
        historyBefore
      )
      assert.deepStrictEqual(
        yield* sql`SELECT storage_format_version FROM effect_local_metadata WHERE singleton = 1`,
        [{ storage_format_version: 1 }]
      )
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master
          WHERE type = 'table'
            AND (
              name = 'effect_local_peer_relay_outbox_v11'
              OR name LIKE 'effect_local_command_delivery_%'
              OR name LIKE 'effect_local_peer_relay_delivery_%'
            )
          ORDER BY name`,
        []
      )
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))

  it.effect("enforces relay identity, payload, and usage invariants", () =>
    Effect.gen(function*() {
      yield* Migrations.run
      const sql = yield* SqlClient.SqlClient
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions, materialized_heads,
        accepted_heads, tombstone, projection_status
      ) VALUES (
        'doc_00000000-0000-4000-8000-000000000001',
        'Task',
        1,
        '[]',
        '[]',
        '[]',
        0,
        'Ready'
      )`

      const partialReceipt = yield* Effect.exit(sql`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
        message_hash, heads, accepted_heads, commit_sequence, accepted_at, writer_provenance,
        relay_message_id
      ) VALUES (
        0, 'peer-direct', 'epoch-partial', 0,
        'doc_00000000-0000-4000-8000-000000000001',
        ${"a".repeat(64)}, '[]', '[]', 0, '2026-01-01T00:00:00.000Z', '[]', 'relay-partial'
      )`)
      assert.strictEqual(partialReceipt._tag, "Failure")

      const insertRelayReceipt = (
        peerId: string,
        epoch: string,
        senderSubjectId: string
      ) =>
        sql`INSERT INTO effect_local_peer_receipts (
          replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
          message_hash, heads, accepted_heads, commit_sequence, accepted_at, writer_provenance,
          relay_sender_tenant_id, relay_sender_subject_id, relay_sender_peer_id,
          relay_message_id, relay_outer_envelope_digest, relay_receipt_expires_at, relay_encoded_size
        ) VALUES (
          0, ${peerId}, ${epoch}, 0,
          'doc_00000000-0000-4000-8000-000000000001',
          ${"b".repeat(64)}, '[]', '[]', 0, '2026-01-01T00:00:00.000Z', '[]',
          'tenant-1', ${senderSubjectId}, 'peer-sender', 'relay-id-1',
          ${"c".repeat(64)}, '2026-01-09T00:00:00.000Z', 32
        )`
      yield* insertRelayReceipt("peer-direct", "epoch-1", "subject-1")
      assert.strictEqual(
        (yield* Effect.exit(insertRelayReceipt("peer-direct", "epoch-2", "subject-1")))._tag,
        "Failure"
      )
      yield* insertRelayReceipt("peer-direct", "epoch-1", "subject-2")

      const insertDirectReceipt = (messageHash: string) =>
        sql`INSERT INTO effect_local_peer_receipts (
          replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
          message_hash, heads, accepted_heads, commit_sequence, accepted_at, writer_provenance
        ) VALUES (
          0, 'peer-direct', 'epoch-1', 0,
          'doc_00000000-0000-4000-8000-000000000001',
          ${messageHash}, '[]', '[]', 0, '2026-01-01T00:00:00.000Z', '[]'
        )`
      yield* insertDirectReceipt("1".repeat(64))
      assert.strictEqual(
        (yield* Effect.exit(insertDirectReceipt("2".repeat(64))))._tag,
        "Failure"
      )

      const payload = Uint8Array.of(1, 2, 3)
      const insertRelayOutbox = (
        relayMessageId: string,
        senderSequence: number,
        writerGeneration = 1
      ) =>
        sql`INSERT INTO effect_local_peer_relay_outbox (
          replica_id, replica_incarnation, writer_generation,
          expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
          remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
          relay_message_id, outer_envelope_digest, protocol_version, payload_version,
          sender_connection_epoch, sender_sequence, document_id, document_type,
          writer_provenance, message_hash, payload, encoded_size,
          created_at, retry_deadline, next_attempt_at
        ) VALUES (
          'rep_00000000-0000-4000-8000-000000000011', 0, ${writerGeneration},
          'tenant-1', 'subject-local', 'peer-local',
          'tenant-1', 'subject-remote', 'peer-remote', 'peer-relay',
          ${relayMessageId}, ${"d".repeat(64)}, 1, 1,
          'epoch-outbound', ${senderSequence},
          'doc_00000000-0000-4000-8000-000000000001', 'Task',
          '[]', ${"e".repeat(64)}, ${payload}, ${payload.byteLength},
          '2026-01-01T00:00:00.000Z', '2026-01-08T00:00:00.000Z',
          '2026-01-01T00:00:00.000Z'
        )`
      yield* insertRelayOutbox("relay-outbox-1", 1)
      assert.strictEqual(
        (yield* Effect.exit(insertRelayOutbox("relay-outbox-2", 1, 2)))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(insertRelayOutbox("relay-outbox-1", 2)))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(sql`UPDATE effect_local_peer_relay_outbox
          SET encoded_size = encoded_size + 1 WHERE relay_message_id = 'relay-outbox-1'`))._tag,
        "Failure"
      )

      assert.strictEqual(
        (yield* Effect.exit(sql`INSERT INTO effect_local_peer_relay_receipt_usage (
          replica_incarnation, sender_tenant_id, sender_subject_id, sender_peer_id,
          receipt_count, encoded_bytes
        ) VALUES (0, 'tenant-1', 'subject-1', 'peer-sender', 1, 0)`))._tag,
        "Failure"
      )
      yield* sql`INSERT INTO effect_local_peer_relay_outbox_remote_usage (
        replica_incarnation, remote_tenant_id, remote_subject_id, remote_peer_id,
        message_count, encoded_bytes
      ) VALUES (0, 'tenant-1', 'subject-remote', 'peer-remote', 1, 3)`
      yield* sql`INSERT INTO effect_local_peer_relay_outbox_replica_usage (
        replica_incarnation, message_count, encoded_bytes
      ) VALUES (0, 1, 3)`

      assert.strictEqual(
        (yield* Effect.exit(sql`DELETE FROM effect_local_documents
          WHERE document_id = 'doc_00000000-0000-4000-8000-000000000001'`))._tag,
        "Failure"
      )
      const [outboxCount] = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count
        FROM effect_local_peer_relay_outbox
        WHERE relay_message_id = 'relay-outbox-1'
      `
      const [remoteUsage] = yield* sql<{
        readonly message_count: number
        readonly encoded_bytes: number
      }>`SELECT message_count, encoded_bytes
        FROM effect_local_peer_relay_outbox_remote_usage
        WHERE replica_incarnation = 0
          AND remote_tenant_id = 'tenant-1'
          AND remote_subject_id = 'subject-remote'
          AND remote_peer_id = 'peer-remote'`
      const [replicaUsage] = yield* sql<{
        readonly message_count: number
        readonly encoded_bytes: number
      }>`SELECT message_count, encoded_bytes
        FROM effect_local_peer_relay_outbox_replica_usage
        WHERE replica_incarnation = 0`
      assert.strictEqual(outboxCount?.count, 1)
      assert.deepStrictEqual(remoteUsage, { message_count: 1, encoded_bytes: 3 })
      assert.deepStrictEqual(replicaUsage, { message_count: 1, encoded_bytes: 3 })
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))

  it.effect("prevents legacy receipt cleanup from deleting relay receipts", () =>
    Effect.gen(function*() {
      yield* Migrations.run
      const sql = yield* SqlClient.SqlClient
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions, materialized_heads,
        accepted_heads, tombstone, projection_status
      ) VALUES (
        'doc_00000000-0000-4000-8000-000000000001',
        'Task',
        1,
        '[]',
        '[]',
        '[]',
        0,
        'Ready'
      )`
      yield* sql`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
        message_hash, heads, accepted_heads, commit_sequence, accepted_at, writer_provenance,
        relay_sender_tenant_id, relay_sender_subject_id, relay_sender_peer_id,
        relay_message_id, relay_outer_envelope_digest, relay_receipt_expires_at, relay_encoded_size
      ) VALUES (
        0, 'peer-relay', 'epoch-relay', 0,
        'doc_00000000-0000-4000-8000-000000000001',
        ${"a".repeat(64)}, '[]', '[]', 0, '2026-01-01T00:00:00.000Z', '[]',
        'tenant-1', 'subject-1', 'peer-sender', 'relay-id-1',
        ${"b".repeat(64)}, '2026-01-09T00:00:00.000Z', 32
      )`
      yield* sql`INSERT INTO effect_local_peer_relay_receipt_usage (
        replica_incarnation, sender_tenant_id, sender_subject_id, sender_peer_id,
        receipt_count, encoded_bytes
      ) VALUES (0, 'tenant-1', 'subject-1', 'peer-sender', 1, 32)`
      yield* sql`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
        message_hash, heads, accepted_heads, commit_sequence, accepted_at, writer_provenance
      ) VALUES (
        0, 'peer-direct', 'epoch-direct', 0,
        'doc_00000000-0000-4000-8000-000000000001',
        ${"c".repeat(64)}, '[]', '[]', 0, '2026-01-01T00:00:00.000Z', '[]'
      )`

      assert.strictEqual(
        (yield* Effect.exit(sql`DELETE FROM effect_local_peer_receipts
          WHERE pending_message IS NULL`))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(sql`DELETE FROM effect_local_documents
          WHERE document_id = 'doc_00000000-0000-4000-8000-000000000001'`))._tag,
        "Failure"
      )
      yield* sql`DELETE FROM effect_local_peer_receipts
        WHERE relay_message_id IS NULL`

      assert.deepStrictEqual(
        yield* sql`SELECT relay_message_id FROM effect_local_peer_receipts`,
        [{ relay_message_id: "relay-id-1" }]
      )
      assert.deepStrictEqual(
        yield* sql`SELECT receipt_count, encoded_bytes
          FROM effect_local_peer_relay_receipt_usage`,
        [{ receipt_count: 1, encoded_bytes: 32 }]
      )
      assert.deepStrictEqual(
        yield* sql`SELECT receipt_row_id
          FROM effect_local_peer_relay_receipt_delete_tokens`,
        []
      )
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))

  it.effect("rejects a changed relay migration checksum", () =>
    Effect.gen(function*() {
      yield* Migrations.run
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_migration_catalog
        SET checksum = 'changed'
        WHERE migration_id = 10`
      const error = yield* Effect.flip(Migrations.run)
      assert.strictEqual(error._tag, "MigrationError")
      assert.include(error.message, "Peer relay state")
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))
})
