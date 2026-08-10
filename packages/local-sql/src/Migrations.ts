import * as Automerge from "@automerge/automerge"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { literal } from "./internal/literal.js"
import * as NativeError from "./internal/nativeError.js"
import * as WriterProvenance from "./internal/writerProvenance.js"

export const canonicalStoreChecksum = "sha256:effect-local-canonical-store-v1"
export const peerSyncChecksum = "sha256:effect-local-peer-sync-v3"
export const durabilityIndexesChecksum = "sha256:effect-local-durability-indexes-v1"
export const projectionReadinessChecksum = "sha256:effect-local-projection-readiness-v1"
export const pendingReceiptIndexesChecksum = "sha256:effect-local-pending-receipt-indexes-v1"
export const peerWriterProvenanceChecksum = "sha256:effect-local-peer-writer-provenance-v1"
export const replicaHealthIndexesChecksum = "sha256:effect-local-replica-health-indexes-v1"
export const documentLineageChecksum = "sha256:effect-local-document-lineage-v1"
export const historyRewriteMarkersChecksum = "sha256:effect-local-history-rewrite-markers-v1"
export const peerRelayStateChecksum = "sha256:effect-local-peer-relay-state-v3"
export const commandDeliveryChecksum = "sha256:effect-local-command-delivery-v1"
export const documentHistoryCountersChecksum = "sha256:effect-local-document-history-counters-v1"
export const backupDocumentInstallationsChecksum = "sha256:effect-local-backup-document-installations-v1"
export const checkpointShippingChecksum = "sha256:effect-local-checkpoint-shipping-v1"
export const batchedSyncRepliesChecksum = "sha256:effect-local-batched-sync-replies-v3"

const migration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE effect_local_migration_catalog (
    migration_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    storage_format_version INTEGER NOT NULL,
    replica_id TEXT NOT NULL,
    replica_incarnation INTEGER NOT NULL,
    writer_generation INTEGER NOT NULL,
    definition_hash TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_writer_generations (
    generation INTEGER PRIMARY KEY,
    claimed_at TEXT NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_documents (
    document_id TEXT PRIMARY KEY,
    document_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    observed_versions TEXT NOT NULL,
    materialized_heads TEXT NOT NULL,
    accepted_heads TEXT NOT NULL,
    tombstone INTEGER NOT NULL,
    projection_status TEXT NOT NULL,
    checkpoint_hash TEXT
  )`
  yield* sql`CREATE TABLE effect_local_changes (
    change_hash TEXT PRIMARY KEY,
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    document_type TEXT NOT NULL,
    writer_schema_version INTEGER NOT NULL,
    writer_definition_hash TEXT NOT NULL,
    actor TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    dependencies TEXT NOT NULL,
    bytes BLOB NOT NULL,
    applied INTEGER NOT NULL,
    peer_id TEXT,
    accepted_at TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL,
    UNIQUE(document_id, actor, sequence)
  )`
  yield* sql`CREATE INDEX effect_local_changes_document_sequence
    ON effect_local_changes(document_id, commit_sequence)`
  yield* sql`CREATE TABLE effect_local_checkpoints (
    checkpoint_hash TEXT PRIMARY KEY,
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    heads TEXT NOT NULL,
    bytes BLOB NOT NULL,
    checksum TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL,
    verified INTEGER NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_command_receipts (
    replica_incarnation INTEGER NOT NULL,
    command_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    mutation_name TEXT NOT NULL,
    result BLOB NOT NULL,
    document_id TEXT NOT NULL,
    heads TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL,
    PRIMARY KEY(replica_incarnation, command_id)
  )`
  yield* sql`CREATE TABLE effect_local_projection_registry (
    projection_name TEXT PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE,
    projection_version INTEGER NOT NULL,
    schema_checksum TEXT NOT NULL,
    status TEXT NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_document_projections (
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    projection_name TEXT NOT NULL REFERENCES effect_local_projection_registry(projection_name),
    projected_heads TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(document_id, projection_name)
  )`
  yield* sql`CREATE TABLE effect_local_commit_outbox (
    commit_sequence INTEGER PRIMARY KEY,
    document_id TEXT NOT NULL,
    invalidation_keys TEXT NOT NULL,
    published INTEGER NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT,
    peer_id TEXT,
    reason TEXT NOT NULL,
    bytes BLOB,
    created_at TEXT NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_backup_installations (
    installation_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    manifest_checksum TEXT NOT NULL,
    installed_at TEXT NOT NULL,
    replica_incarnation INTEGER NOT NULL
  )`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (1, 'canonical_store', ${canonicalStoreChecksum})`
})

const peerSyncMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE effect_local_peer_receipts (
    replica_incarnation INTEGER NOT NULL,
    peer_id TEXT NOT NULL,
    connection_epoch TEXT NOT NULL,
    receive_sequence INTEGER NOT NULL,
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    message_hash TEXT NOT NULL,
    reply BLOB,
    reply_hash TEXT,
    pending_message BLOB,
    heads TEXT NOT NULL,
    accepted_heads TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL,
    accepted_at TEXT NOT NULL,
    PRIMARY KEY(replica_incarnation, peer_id, connection_epoch, receive_sequence)
  )`
  yield* sql`CREATE INDEX effect_local_peer_receipts_document_sequence
    ON effect_local_peer_receipts(document_id, commit_sequence)`
  yield* sql`CREATE TABLE effect_local_peer_outbox (
    replica_incarnation INTEGER NOT NULL,
    peer_id TEXT NOT NULL,
    connection_epoch TEXT NOT NULL,
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    send_sequence INTEGER NOT NULL,
    message BLOB NOT NULL,
    message_hash TEXT NOT NULL,
    heads TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(replica_incarnation, peer_id, connection_epoch, send_sequence)
  )`
  yield* sql`CREATE INDEX effect_local_peer_outbox_connection_status
    ON effect_local_peer_outbox(replica_incarnation, peer_id, connection_epoch, status, send_sequence)`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (2, 'peer_sync', ${peerSyncChecksum})`
})

const durabilityIndexesMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`ALTER TABLE effect_local_peer_outbox ADD COLUMN created_at TEXT NOT NULL DEFAULT ''`
  yield* sql`UPDATE effect_local_peer_outbox
    SET created_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE created_at = ''`
  yield* sql`DELETE FROM effect_local_checkpoints WHERE rowid IN (
    SELECT rowid FROM (
      SELECT rowid, ROW_NUMBER() OVER (
        PARTITION BY document_id
        ORDER BY verified DESC, commit_sequence DESC, checkpoint_hash DESC
      ) AS checkpoint_rank
      FROM effect_local_checkpoints
    ) WHERE checkpoint_rank > 2
  )`
  yield* sql`CREATE INDEX effect_local_checkpoints_document_verified_sequence
    ON effect_local_checkpoints(document_id, verified, commit_sequence DESC, checkpoint_hash DESC)`
  yield* sql`CREATE INDEX effect_local_commit_outbox_published_sequence
    ON effect_local_commit_outbox(published, commit_sequence)`
  yield* sql`CREATE INDEX effect_local_documents_type_projection_status
    ON effect_local_documents(document_type, projection_status)`
  yield* sql`CREATE INDEX effect_local_projection_registry_name_status
    ON effect_local_projection_registry(projection_name, status)`
  yield* sql`CREATE INDEX effect_local_peer_outbox_incarnation_created
    ON effect_local_peer_outbox(replica_incarnation, created_at)`
  yield* sql`CREATE INDEX effect_local_peer_receipts_incarnation_accepted
    ON effect_local_peer_receipts(replica_incarnation, accepted_at)`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (3, 'durability_indexes', ${durabilityIndexesChecksum})`
})

const projectionReadinessMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE INDEX effect_local_document_projections_not_ready
    ON effect_local_document_projections(projection_name)
    WHERE status != 'Ready'`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (4, 'projection_readiness', ${projectionReadinessChecksum})`
})

const pendingReceiptIndexesMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE INDEX effect_local_peer_receipts_pending_document
    ON effect_local_peer_receipts(replica_incarnation, document_id)
    WHERE pending_message IS NOT NULL`
  yield* sql`CREATE INDEX effect_local_peer_receipts_pending_peer
    ON effect_local_peer_receipts(replica_incarnation, peer_id)
    WHERE pending_message IS NOT NULL`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (5, 'pending_receipt_indexes', ${pendingReceiptIndexesChecksum})`
})

const peerWriterProvenanceMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`ALTER TABLE effect_local_peer_receipts
    ADD COLUMN writer_provenance TEXT NOT NULL DEFAULT '[]'`
  yield* sql`ALTER TABLE effect_local_peer_outbox
    ADD COLUMN writer_provenance TEXT NOT NULL DEFAULT '[]'`
  yield* sql`ALTER TABLE effect_local_checkpoints
    ADD COLUMN writer_provenance TEXT NOT NULL DEFAULT '[]'`
  yield* sql`UPDATE effect_local_changes
    SET writer_definition_hash = (
      SELECT definition_hash FROM effect_local_metadata WHERE singleton = 1
    )
    WHERE writer_definition_hash = 'local'`
  const defaults = (yield* sql<{
    readonly definition_hash: string
  }>`SELECT definition_hash FROM effect_local_metadata WHERE singleton = 1`)[0]
  const documents = yield* sql<{
    readonly document_id: string
    readonly schema_version: number
  }>`SELECT document_id, schema_version FROM effect_local_documents`
  const schemaVersionByDocument = new Map(
    documents.map((document) => [document.document_id, document.schema_version])
  )
  const checkpoints = yield* sql<{
    readonly bytes: Uint8Array
    readonly checkpoint_hash: string
    readonly document_id: string
    readonly schema_version: number
  }>`SELECT checkpoint.bytes, checkpoint.checkpoint_hash, checkpoint.document_id, document.schema_version
    FROM effect_local_checkpoints AS checkpoint
    INNER JOIN effect_local_documents AS document ON document.document_id = checkpoint.document_id`
  const changes = yield* sql<{
    readonly change_hash: string
    readonly document_id: string
    readonly writer_definition_hash: string
    readonly writer_schema_version: number
  }>`SELECT change_hash, document_id, writer_definition_hash, writer_schema_version
    FROM effect_local_changes`
  const changesByDocument = new Map<string, Array<(typeof changes)[number]>>()
  for (const change of changes) {
    const existing = changesByDocument.get(change.document_id)
    if (existing === undefined) changesByDocument.set(change.document_id, [change])
    else existing.push(change)
  }
  const checkpointProvenanceByDocument = new Map<string, Array<WriterProvenance.ChangeProvenance>>()
  for (const checkpoint of checkpoints) {
    yield* Effect.acquireUseRelease(
      Effect.option(Effect.try({
        try: () => Automerge.load(checkpoint.bytes),
        catch: (cause) => cause
      })),
      (option) =>
        Option.match(option, {
          onNone: () => Effect.void,
          onSome: (document) =>
            Effect.gen(function*() {
              const encoded = yield* Effect.option(Effect.try({
                try: () => {
                  // Legacy pruning discarded provenance for some checkpointed changes. Preserve the
                  // old receiver attribution by using the stored document version and local definition.
                  const writerProvenance = WriterProvenance.backfill(
                    WriterProvenance.changeHashes(document),
                    (changesByDocument.get(checkpoint.document_id) ?? []).map((change) => ({
                      changeHash: change.change_hash,
                      writerSchemaVersion: change.writer_schema_version,
                      writerDefinitionHash: change.writer_definition_hash
                    })),
                    {
                      writerSchemaVersion: checkpoint.schema_version,
                      writerDefinitionHash: defaults.definition_hash
                    }
                  )
                  return {
                    stored: Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(writerProvenance),
                    writerProvenance
                  }
                },
                catch: (cause) => cause
              }))
              if (Option.isNone(encoded)) return
              yield* sql`UPDATE effect_local_checkpoints
                SET writer_provenance = ${encoded.value.stored}
                WHERE checkpoint_hash = ${checkpoint.checkpoint_hash}`
              const existing = checkpointProvenanceByDocument.get(checkpoint.document_id)
              if (existing === undefined) {
                checkpointProvenanceByDocument.set(checkpoint.document_id, [...encoded.value.writerProvenance])
              } else {
                existing.push(...encoded.value.writerProvenance)
              }
            })
        }),
      (option) =>
        Option.match(option, {
          onNone: () => Effect.void,
          onSome: (document) => Effect.sync(() => Automerge.free(document))
        })
    )
  }
  const storedEntries = (documentId: string): ReadonlyArray<WriterProvenance.ChangeProvenance> => [
    ...(changesByDocument.get(documentId) ?? []).map((change) => ({
      changeHash: change.change_hash,
      writerSchemaVersion: change.writer_schema_version,
      writerDefinitionHash: change.writer_definition_hash
    })),
    ...(checkpointProvenanceByDocument.get(documentId) ?? [])
  ]
  const backfillMessage = (documentId: string, message: Uint8Array) =>
    WriterProvenance.backfill(
      WriterProvenance.syncMessageChangeHashes(message),
      storedEntries(documentId),
      {
        writerSchemaVersion: schemaVersionByDocument.get(documentId)!,
        writerDefinitionHash: defaults.definition_hash
      }
    )
  const outbox = yield* sql<{
    readonly document_id: string
    readonly message: Uint8Array
    readonly row_id: number
  }>`SELECT rowid AS row_id, document_id, message FROM effect_local_peer_outbox`
  for (const row of outbox) {
    const writerProvenance = yield* Effect.option(Effect.try({
      try: () => backfillMessage(row.document_id, row.message),
      catch: (cause) => cause
    }))
    if (Option.isSome(writerProvenance)) {
      yield* sql`UPDATE effect_local_peer_outbox
        SET writer_provenance = ${Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(writerProvenance.value)}
        WHERE rowid = ${row.row_id}`
    }
  }
  const pendingReceipts = yield* sql<{
    readonly document_id: string
    readonly pending_message: Uint8Array
    readonly row_id: number
  }>`SELECT rowid AS row_id, document_id, pending_message
    FROM effect_local_peer_receipts WHERE pending_message IS NOT NULL`
  for (const row of pendingReceipts) {
    const writerProvenance = yield* Effect.option(Effect.try({
      try: () => backfillMessage(row.document_id, row.pending_message),
      catch: (cause) => cause
    }))
    if (Option.isSome(writerProvenance)) {
      yield* sql`UPDATE effect_local_peer_receipts
        SET writer_provenance = ${Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(writerProvenance.value)}
        WHERE rowid = ${row.row_id}`
    }
  }
  yield* sql`DELETE FROM effect_local_peer_receipts WHERE pending_message IS NULL`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (6, 'peer_writer_provenance', ${peerWriterProvenanceChecksum})`
})

const replicaHealthIndexesMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE INDEX effect_local_documents_not_ready_type
    ON effect_local_documents(document_type)
    WHERE projection_status != 'Ready'`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (7, 'replica_health_indexes', ${replicaHealthIndexesChecksum})`
})

const documentLineageMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`ALTER TABLE effect_local_documents
    ADD COLUMN lineage TEXT NOT NULL DEFAULT ''`
  yield* sql`ALTER TABLE effect_local_checkpoints
    ADD COLUMN lineage TEXT NOT NULL DEFAULT ''`
  yield* sql`ALTER TABLE effect_local_peer_outbox
    ADD COLUMN lineage TEXT NOT NULL DEFAULT ''`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (8, 'document_lineage', ${documentLineageChecksum})`
})

/**
 * The durable `(replica_incarnation, operation_id) -> lineage` record of an already performed history
 * rewrite.
 *
 * `Compaction.rewriteHistory` is destructive and mints a lineage that permanently invalidates every
 * peer's view. Workflow idempotency only dedupes operator REQUESTS: a crash between the rewrite's SQL
 * commit and the journaling of its activity result makes the activity run again, and without this
 * table the replay would mint a second lineage and force every peer that already resynced onto the
 * first one to resync again. The row is written inside the rewrite's own transaction, so it cannot be
 * observed apart from the rewrite it guards.
 *
 * Keyed by incarnation as well as operation, exactly like `effect_local_command_receipts`. A restore
 * claims the gate and raises the incarnation, so rows written before it can never satisfy a lookup
 * again and cannot short circuit a rewrite of the restored document. No foreign key to
 * `effect_local_documents`, for the same reason command receipts carry none: the row records that an
 * operator request was served, not that the document still exists.
 */
const historyRewriteMarkersMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE effect_local_history_rewrites (
    replica_incarnation INTEGER NOT NULL,
    operation_id TEXT NOT NULL,
    document_id TEXT NOT NULL,
    lineage TEXT NOT NULL,
    rewritten_at TEXT NOT NULL,
    PRIMARY KEY(replica_incarnation, operation_id)
  )`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (9, 'history_rewrite_markers', ${historyRewriteMarkersChecksum})`
})

const peerRelayStateMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE effect_local_peer_receipts_relay (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    replica_incarnation INTEGER NOT NULL,
    peer_id TEXT NOT NULL,
    connection_epoch TEXT NOT NULL,
    receive_sequence INTEGER NOT NULL,
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    message_hash TEXT NOT NULL,
    reply BLOB,
    reply_hash TEXT,
    pending_message BLOB,
    heads TEXT NOT NULL,
    accepted_heads TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL,
    accepted_at TEXT NOT NULL,
    writer_provenance TEXT NOT NULL DEFAULT '[]',
    relay_sender_tenant_id TEXT,
    relay_sender_subject_id TEXT,
    relay_sender_peer_id TEXT,
    relay_message_id TEXT,
    relay_outer_envelope_digest TEXT,
    relay_receipt_expires_at TEXT,
    relay_encoded_size INTEGER,
    CHECK (
      (
        relay_sender_tenant_id IS NULL
        AND relay_sender_subject_id IS NULL
        AND relay_sender_peer_id IS NULL
        AND relay_message_id IS NULL
        AND relay_outer_envelope_digest IS NULL
        AND relay_receipt_expires_at IS NULL
        AND relay_encoded_size IS NULL
      )
      OR (
        relay_sender_tenant_id IS NOT NULL
        AND length(relay_sender_tenant_id) > 0
        AND relay_sender_subject_id IS NOT NULL
        AND length(relay_sender_subject_id) > 0
        AND relay_sender_peer_id IS NOT NULL
        AND length(relay_sender_peer_id) > 0
        AND relay_message_id IS NOT NULL
        AND length(relay_message_id) > 0
        AND relay_outer_envelope_digest IS NOT NULL
        AND length(relay_outer_envelope_digest) = 64
        AND relay_receipt_expires_at IS NOT NULL
        AND length(relay_receipt_expires_at) > 0
        AND relay_encoded_size > 0
      )
    )
  )`
  yield* sql`INSERT INTO effect_local_peer_receipts_relay (
    replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
    message_hash, reply, reply_hash, pending_message, heads, accepted_heads,
    commit_sequence, accepted_at, writer_provenance
  )
  SELECT
    replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
    message_hash, reply, reply_hash, pending_message, heads, accepted_heads,
    commit_sequence, accepted_at, writer_provenance
  FROM effect_local_peer_receipts`
  yield* sql`DROP TABLE effect_local_peer_receipts`
  yield* sql`ALTER TABLE effect_local_peer_receipts_relay
    RENAME TO effect_local_peer_receipts`
  yield* sql`CREATE UNIQUE INDEX effect_local_peer_receipts_direct_identity
    ON effect_local_peer_receipts(
      replica_incarnation,
      peer_id,
      connection_epoch,
      receive_sequence
    )
    WHERE relay_message_id IS NULL`
  yield* sql`CREATE UNIQUE INDEX effect_local_peer_receipts_relay_identity
    ON effect_local_peer_receipts(
      replica_incarnation,
      relay_sender_tenant_id,
      relay_sender_subject_id,
      relay_sender_peer_id,
      relay_message_id
    )
    WHERE relay_message_id IS NOT NULL`
  yield* sql`CREATE INDEX effect_local_peer_receipts_document_sequence
    ON effect_local_peer_receipts(document_id, commit_sequence)`
  yield* sql`CREATE INDEX effect_local_peer_receipts_incarnation_accepted
    ON effect_local_peer_receipts(replica_incarnation, accepted_at)`
  yield* sql`CREATE INDEX effect_local_peer_receipts_pending_document
    ON effect_local_peer_receipts(replica_incarnation, document_id)
    WHERE pending_message IS NOT NULL`
  yield* sql`CREATE INDEX effect_local_peer_receipts_pending_peer
    ON effect_local_peer_receipts(replica_incarnation, peer_id)
    WHERE pending_message IS NOT NULL`
  yield* sql`CREATE INDEX effect_local_peer_receipts_relay_expiry
    ON effect_local_peer_receipts(
      replica_incarnation,
      relay_receipt_expires_at,
      relay_sender_tenant_id,
      relay_sender_subject_id,
      relay_sender_peer_id,
      relay_message_id,
      row_id
    )
    WHERE relay_message_id IS NOT NULL`
  yield* sql`CREATE TABLE effect_local_peer_relay_receipt_delete_tokens (
    receipt_row_id INTEGER PRIMARY KEY
  )`
  yield* sql`CREATE TRIGGER effect_local_peer_relay_receipt_delete_guard
    BEFORE DELETE ON effect_local_peer_receipts
    WHEN OLD.relay_message_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM effect_local_peer_relay_receipt_delete_tokens
        WHERE receipt_row_id = OLD.row_id
      )
    BEGIN
      SELECT RAISE(ABORT, 'Relay receipt deletion requires an exact token');
    END`
  yield* sql`CREATE TRIGGER effect_local_peer_relay_receipt_delete_consume
    AFTER DELETE ON effect_local_peer_receipts
    WHEN OLD.relay_message_id IS NOT NULL
    BEGIN
      DELETE FROM effect_local_peer_relay_receipt_delete_tokens
      WHERE receipt_row_id = OLD.row_id;
    END`
  yield* sql`CREATE TABLE effect_local_peer_relay_receipt_usage (
    replica_incarnation INTEGER NOT NULL CHECK (replica_incarnation >= 0),
    sender_tenant_id TEXT NOT NULL CHECK (length(sender_tenant_id) > 0),
    sender_subject_id TEXT NOT NULL CHECK (length(sender_subject_id) > 0),
    sender_peer_id TEXT NOT NULL CHECK (length(sender_peer_id) > 0),
    receipt_count INTEGER NOT NULL CHECK (receipt_count >= 0),
    encoded_bytes INTEGER NOT NULL CHECK (encoded_bytes >= 0),
    PRIMARY KEY(replica_incarnation, sender_tenant_id, sender_subject_id, sender_peer_id),
    CHECK (
      (receipt_count = 0 AND encoded_bytes = 0)
      OR (receipt_count > 0 AND encoded_bytes > 0)
    )
  )`
  yield* sql`CREATE TABLE effect_local_peer_relay_outbox (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    replica_id TEXT NOT NULL CHECK (length(replica_id) > 0),
    replica_incarnation INTEGER NOT NULL CHECK (replica_incarnation >= 0),
    writer_generation INTEGER NOT NULL CHECK (writer_generation >= 0),
    expected_local_tenant_id TEXT NOT NULL CHECK (length(expected_local_tenant_id) > 0),
    expected_local_subject_id TEXT NOT NULL CHECK (length(expected_local_subject_id) > 0),
    expected_local_peer_id TEXT NOT NULL CHECK (length(expected_local_peer_id) > 0),
    remote_tenant_id TEXT NOT NULL CHECK (length(remote_tenant_id) > 0),
    remote_subject_id TEXT NOT NULL CHECK (length(remote_subject_id) > 0),
    remote_peer_id TEXT NOT NULL CHECK (length(remote_peer_id) > 0),
    relay_peer_id TEXT NOT NULL CHECK (length(relay_peer_id) > 0),
    relay_message_id TEXT NOT NULL CHECK (length(relay_message_id) > 0),
    outer_envelope_digest TEXT NOT NULL CHECK (length(outer_envelope_digest) = 64),
    protocol_version INTEGER NOT NULL CHECK (protocol_version = 1),
    payload_version INTEGER NOT NULL CHECK (payload_version = 1),
    sender_connection_epoch TEXT NOT NULL CHECK (length(sender_connection_epoch) > 0),
    sender_sequence INTEGER NOT NULL CHECK (sender_sequence >= 0),
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE RESTRICT,
    document_type TEXT NOT NULL CHECK (length(document_type) > 0),
    writer_provenance TEXT NOT NULL,
    message_hash TEXT NOT NULL CHECK (length(message_hash) = 64),
    payload BLOB NOT NULL,
    encoded_size INTEGER NOT NULL CHECK (encoded_size > 0 AND encoded_size = length(payload)),
    created_at TEXT NOT NULL CHECK (length(created_at) > 0),
    retry_deadline TEXT NOT NULL CHECK (length(retry_deadline) > 0),
    next_attempt_at TEXT NOT NULL CHECK (length(next_attempt_at) > 0),
    custody_state TEXT NOT NULL CHECK (custody_state IN ('Pending', 'InFlight')),
    CHECK (expected_local_tenant_id = remote_tenant_id),
    UNIQUE(replica_id, replica_incarnation, relay_message_id),
    UNIQUE(
      replica_id,
      replica_incarnation,
      relay_peer_id,
      remote_tenant_id,
      remote_subject_id,
      remote_peer_id,
      sender_connection_epoch,
      sender_sequence
    )
  )`
  yield* sql`CREATE INDEX effect_local_peer_relay_outbox_due_endpoint
    ON effect_local_peer_relay_outbox(
      replica_id,
      replica_incarnation,
      relay_peer_id,
      remote_tenant_id,
      remote_subject_id,
      remote_peer_id,
      custody_state,
      next_attempt_at,
      row_id
    )`
  yield* sql`CREATE INDEX effect_local_peer_relay_outbox_retry_deadline
    ON effect_local_peer_relay_outbox(replica_id, replica_incarnation, retry_deadline, row_id)`
  yield* sql`CREATE TABLE effect_local_peer_relay_outbox_remote_usage (
    replica_incarnation INTEGER NOT NULL CHECK (replica_incarnation >= 0),
    remote_tenant_id TEXT NOT NULL CHECK (length(remote_tenant_id) > 0),
    remote_subject_id TEXT NOT NULL CHECK (length(remote_subject_id) > 0),
    remote_peer_id TEXT NOT NULL CHECK (length(remote_peer_id) > 0),
    message_count INTEGER NOT NULL CHECK (message_count >= 0),
    encoded_bytes INTEGER NOT NULL CHECK (encoded_bytes >= 0),
    PRIMARY KEY(replica_incarnation, remote_tenant_id, remote_subject_id, remote_peer_id),
    CHECK (
      (message_count = 0 AND encoded_bytes = 0)
      OR (message_count > 0 AND encoded_bytes > 0)
    )
  )`
  yield* sql`CREATE TABLE effect_local_peer_relay_outbox_replica_usage (
    replica_incarnation INTEGER PRIMARY KEY CHECK (replica_incarnation >= 0),
    message_count INTEGER NOT NULL CHECK (message_count >= 0),
    encoded_bytes INTEGER NOT NULL CHECK (encoded_bytes >= 0),
    CHECK (
      (message_count = 0 AND encoded_bytes = 0)
      OR (message_count > 0 AND encoded_bytes > 0)
    )
  )`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (10, 'peer_relay_state', ${peerRelayStateChecksum})`
})

const documentHistoryCountersMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
  const DocumentPageRow = Schema.Struct({
    document_id: Schema.String,
    document_type: Schema.String,
    materialized_heads: Schema.String,
    page_rowid: Schema.Int
  })
  const RawDocumentPageRow = Schema.Struct({
    document_id: Schema.Unknown,
    document_type: Schema.Unknown,
    materialized_heads: Schema.Unknown,
    page_rowid: Schema.Int
  })
  const HistoryAggregateRow = Schema.Struct({
    change_bytes: NonNegativeInt,
    change_count: NonNegativeInt,
    document_id: Schema.String
  })
  const RetainedChangeRow = Schema.Struct({
    applied: Schema.Int,
    actor: Schema.String,
    bytes: Schema.Uint8Array,
    change_hash: WriterProvenance.ChangeHash,
    dependencies: Schema.String,
    document_id: Schema.String,
    document_type: Schema.String,
    sequence: Schema.Int
  })
  const RawRetainedChangeRow = Schema.Struct({
    applied: Schema.Unknown,
    actor: Schema.Unknown,
    bytes: Schema.Unknown,
    change_hash: Schema.Unknown,
    dependencies: Schema.Unknown,
    document_id: Schema.String,
    document_type: Schema.Unknown,
    sequence: Schema.Unknown
  })
  const findDocumentPage = SqlSchema.findAll({
    Request: Schema.Struct({
      after: Schema.NullOr(Schema.Int),
      limit: Schema.Int
    }),
    Result: RawDocumentPageRow,
    execute: ({ after, limit }) =>
      (() => {
        if (after === null) {
          return (sql`SELECT rowid AS page_rowid, document_id, document_type, materialized_heads
          FROM effect_local_documents
          ORDER BY rowid
          LIMIT ${limit}`)
        }
        return (sql`SELECT rowid AS page_rowid, document_id, document_type, materialized_heads
          FROM effect_local_documents
          WHERE rowid > ${after}
          ORDER BY rowid
          LIMIT ${limit}`)
      })()
  })
  const findHistoryAggregates = SqlSchema.findAll({
    Request: Schema.Array(Schema.String),
    Result: HistoryAggregateRow,
    execute: (documentIds) =>
      sql`SELECT
          document_id,
          COUNT(change_hash) AS change_count,
          COALESCE(SUM(length(bytes)), 0) AS change_bytes
        FROM effect_local_changes
        WHERE ${sql.in("document_id", documentIds)}
        GROUP BY document_id`
  })
  const findRetainedChanges = SqlSchema.findAll({
    Request: Schema.Array(Schema.String),
    Result: RawRetainedChangeRow,
    execute: (documentIds) =>
      sql`SELECT applied, actor, bytes, change_hash, dependencies, document_id, document_type, sequence
        FROM effect_local_changes
        WHERE ${sql.in("document_id", documentIds)}
        ORDER BY document_id, commit_sequence, sequence, change_hash`
  })
  const decodeDocumentPageRow = Schema.decodeUnknownEffect(DocumentPageRow)
  const decodeRetainedChanges = Schema.decodeUnknownEffect(Schema.mutable(Schema.Array(RetainedChangeRow)))
  const transitionStorageFormat = SqlSchema.findOneOption({
    Request: Schema.Void,
    Result: Schema.Struct({ writer_generation: Identity.WriterGeneration }),
    execute: () =>
      sql`UPDATE effect_local_metadata SET
          storage_format_version = 2,
          writer_generation = writer_generation + 1
        WHERE singleton = 1 AND storage_format_version = 1
        RETURNING writer_generation`
  })
  yield* sql`ALTER TABLE effect_local_documents
    ADD COLUMN history_changes INTEGER CHECK (history_changes IS NULL OR history_changes >= 0)`
  yield* sql`ALTER TABLE effect_local_documents
    ADD COLUMN history_operations INTEGER CHECK (history_operations IS NULL OR history_operations >= 0)`
  yield* sql`ALTER TABLE effect_local_documents
    ADD COLUMN history_bytes INTEGER CHECK (history_bytes IS NULL OR history_bytes >= 0)`
  yield* sql`CREATE TEMP TABLE effect_local_history_counter_backfill (
    document_id TEXT PRIMARY KEY,
    history_changes INTEGER NOT NULL,
    history_operations INTEGER NOT NULL,
    history_bytes INTEGER NOT NULL
  )`

  const pageSize = 64
  const decodeHeads = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Array(Schema.String)))
  type DocumentPageEntry = typeof DocumentPageRow.Type & {
    readonly change_bytes: number
    readonly change_count: number
  }
  let after: number | null = null
  while (true) {
    const page: Array<typeof RawDocumentPageRow.Type> = yield* findDocumentPage({ after, limit: pageSize })
    if (page.length === 0) break
    after = page.at(-1)!.page_rowid
    const decodedPage: Array<typeof DocumentPageRow.Type> = []
    for (const document of page) {
      const decoded = yield* Effect.option(decodeDocumentPageRow(document))
      if (Option.isSome(decoded)) decodedPage.push(decoded.value)
    }
    const aggregates = yield* Effect.gen(function*() {
      if (decodedPage.length === 0) return []
      return (yield* findHistoryAggregates(decodedPage.map((document) => document.document_id)))
    })
    const aggregateByDocument = new Map(aggregates.map((aggregate) => [aggregate.document_id, aggregate]))
    const documents: Array<DocumentPageEntry> = decodedPage.map((document) => {
      const aggregate = aggregateByDocument.get(document.document_id)
      return {
        change_bytes: aggregate?.change_bytes ?? 0,
        change_count: aggregate?.change_count ?? 0,
        document_id: document.document_id,
        document_type: document.document_type,
        materialized_heads: document.materialized_heads,
        page_rowid: document.page_rowid
      }
    })

    const eligible: Array<DocumentPageEntry> = documents.filter((document) =>
      document.change_count <= ReplicaLimits.maxConflictSourceChangesHardLimit &&
      document.change_bytes <= ReplicaLimits.maxConflictSourceBytesHardLimit
    )
    const batches: Array<Array<DocumentPageEntry>> = []
    let batch: Array<DocumentPageEntry> = []
    let batchChanges = 0
    let batchBytes = 0
    for (const document of eligible) {
      if (
        batch.length > 0 &&
        (
          batchChanges + document.change_count > ReplicaLimits.maxConflictSourceChangesHardLimit ||
          batchBytes + document.change_bytes > ReplicaLimits.maxConflictSourceBytesHardLimit
        )
      ) {
        batches.push(batch)
        batch = []
        batchChanges = 0
        batchBytes = 0
      }
      batch.push(document)
      batchChanges += document.change_count
      batchBytes += document.change_bytes
    }
    if (batch.length > 0) batches.push(batch)

    for (const documentBatch of batches) {
      const changes = yield* findRetainedChanges(documentBatch.map((document) => document.document_id))
      const changesByDocument = new Map<string, Array<typeof RawRetainedChangeRow.Type>>()
      for (const change of changes) {
        const current = changesByDocument.get(change.document_id)
        if (current === undefined) changesByDocument.set(change.document_id, [change])
        else current.push(change)
      }
      const backfilled: Array<{
        readonly document_id: string
        readonly history_bytes: number
        readonly history_changes: number
        readonly history_operations: number
      }> = []
      for (const document of documentBatch) {
        const rawRetained = changesByDocument.get(document.document_id) ?? []
        const counters = yield* Effect.option(Effect.gen(function*() {
          const retained = yield* decodeRetainedChanges(rawRetained)
          return yield* Effect.try({
            try: () => {
              let operations = 0
              for (const change of retained) {
                const decoded = Automerge.decodeChange(change.bytes)
                operations += decoded.ops.length
                if (
                  operations > ReplicaLimits.maxConflictSourceOperationsHardLimit ||
                  (change.applied !== 0 && change.applied !== 1) ||
                  change.document_type !== document.document_type ||
                  decoded.hash !== change.change_hash ||
                  decoded.actor !== change.actor ||
                  decoded.seq !== change.sequence ||
                  Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(decoded.deps) !== change.dependencies
                ) return NativeError.throwTypeError(`Invalid retained change ${change.change_hash}`)
              }

              return Effect.runSync(Effect.acquireUseRelease(
                Effect.sync(() => Automerge.init()),
                (recovered) =>
                  Effect.sync(() => {
                    const applied = retained.filter((change) => change.applied === 1)
                    if (applied.length > 0) {
                      recovered = Automerge.applyChanges(recovered, applied.map((change) => change.bytes))[0]
                    }
                    const expectedHeads = decodeHeads(document.materialized_heads)
                    const observedHeads = Automerge.getHeads(recovered)
                    if (
                      observedHeads.length !== expectedHeads.length ||
                      !Automerge.hasHeads(recovered, [...expectedHeads])
                    ) {
                      return NativeError.throwTypeError(`Incomplete retained history for ${document.document_id}`)
                    }
                    return {
                      document_id: document.document_id,
                      history_bytes: document.change_bytes,
                      history_changes: retained.length,
                      history_operations: operations
                    }
                  }),
                (recovered) => Effect.sync(() => Automerge.free(recovered))
              ))
            },
            catch: (cause) => cause
          })
        }))
        if (Option.isSome(counters)) backfilled.push(counters.value)
      }
      if (backfilled.length > 0) {
        yield* sql`INSERT INTO effect_local_history_counter_backfill ${sql.insert(backfilled)}`
      }
      yield* Effect.yieldNow
    }
    yield* sql`UPDATE effect_local_documents SET
      history_changes = (
        SELECT history_changes FROM effect_local_history_counter_backfill AS backfill
        WHERE backfill.document_id = effect_local_documents.document_id
      ),
      history_operations = (
        SELECT history_operations FROM effect_local_history_counter_backfill AS backfill
        WHERE backfill.document_id = effect_local_documents.document_id
      ),
      history_bytes = (
        SELECT history_bytes FROM effect_local_history_counter_backfill AS backfill
        WHERE backfill.document_id = effect_local_documents.document_id
      )
      WHERE document_id IN (SELECT document_id FROM effect_local_history_counter_backfill)`
    yield* sql`DELETE FROM effect_local_history_counter_backfill`
    if (page.length < pageSize) break
  }

  yield* sql`DROP TABLE effect_local_history_counter_backfill`
  const transition = yield* transitionStorageFormat(undefined)
  if (Option.isSome(transition)) {
    yield* sql`INSERT INTO effect_local_writer_generations (generation, claimed_at)
      VALUES (
        ${transition.value.writer_generation},
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      )`
  }
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (12, 'document_history_counters', ${documentHistoryCountersChecksum})`
})

const commandDeliveryMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE effect_local_command_delivery_sources (
    replica_incarnation INTEGER NOT NULL CHECK (replica_incarnation >= 0),
    command_id TEXT NOT NULL,
    document_id TEXT NOT NULL,
    PRIMARY KEY(replica_incarnation, command_id),
    FOREIGN KEY(replica_incarnation, command_id)
      REFERENCES effect_local_command_receipts(replica_incarnation, command_id)
      ON DELETE CASCADE
  )`
  yield* sql`CREATE INDEX effect_local_command_delivery_sources_document
    ON effect_local_command_delivery_sources(replica_incarnation, document_id, command_id)`
  yield* sql`CREATE TABLE effect_local_command_delivery_changes (
    replica_incarnation INTEGER NOT NULL CHECK (replica_incarnation >= 0),
    command_id TEXT NOT NULL,
    change_hash TEXT NOT NULL CHECK (length(change_hash) = 64),
    PRIMARY KEY(replica_incarnation, command_id, change_hash),
    FOREIGN KEY(replica_incarnation, command_id)
      REFERENCES effect_local_command_delivery_sources(replica_incarnation, command_id)
      ON DELETE CASCADE
  )`
  yield* sql`CREATE INDEX effect_local_command_delivery_changes_hash
    ON effect_local_command_delivery_changes(replica_incarnation, change_hash, command_id)`
  yield* sql`CREATE TABLE effect_local_peer_relay_delivery_messages (
    replica_id TEXT NOT NULL CHECK (length(replica_id) > 0),
    replica_incarnation INTEGER NOT NULL CHECK (replica_incarnation >= 0),
    expected_local_tenant_id TEXT NOT NULL CHECK (length(expected_local_tenant_id) > 0),
    expected_local_subject_id TEXT NOT NULL CHECK (length(expected_local_subject_id) > 0),
    expected_local_peer_id TEXT NOT NULL CHECK (length(expected_local_peer_id) > 0),
    remote_tenant_id TEXT NOT NULL CHECK (length(remote_tenant_id) > 0),
    remote_subject_id TEXT NOT NULL CHECK (length(remote_subject_id) > 0),
    remote_peer_id TEXT NOT NULL CHECK (length(remote_peer_id) > 0),
    relay_peer_id TEXT NOT NULL CHECK (length(relay_peer_id) > 0),
    relay_message_id TEXT NOT NULL CHECK (length(relay_message_id) > 0),
    outer_envelope_digest TEXT NOT NULL CHECK (length(outer_envelope_digest) = 64),
    sender_connection_epoch TEXT NOT NULL CHECK (length(sender_connection_epoch) > 0),
    sender_sequence INTEGER NOT NULL CHECK (sender_sequence >= 0),
    document_id TEXT NOT NULL,
    created_at TEXT NOT NULL CHECK (length(created_at) > 0),
    retry_deadline TEXT NOT NULL CHECK (length(retry_deadline) > 0),
    relay_custody_accepted_at TEXT,
    sender_custody_unconfirmed_at TEXT,
    PRIMARY KEY(replica_incarnation, relay_message_id),
    UNIQUE(
      replica_id,
      replica_incarnation,
      relay_peer_id,
      remote_tenant_id,
      remote_subject_id,
      remote_peer_id,
      sender_connection_epoch,
      sender_sequence
    ),
    CHECK (expected_local_tenant_id = remote_tenant_id)
  )`
  yield* sql`CREATE INDEX effect_local_peer_relay_delivery_messages_document
    ON effect_local_peer_relay_delivery_messages(
      replica_incarnation, document_id, relay_peer_id, remote_peer_id, relay_message_id
    )`
  yield* sql`CREATE TABLE effect_local_peer_relay_delivery_changes (
    replica_incarnation INTEGER NOT NULL CHECK (replica_incarnation >= 0),
    relay_message_id TEXT NOT NULL,
    change_hash TEXT NOT NULL CHECK (length(change_hash) = 64),
    PRIMARY KEY(replica_incarnation, relay_message_id, change_hash),
    FOREIGN KEY(replica_incarnation, relay_message_id)
      REFERENCES effect_local_peer_relay_delivery_messages(replica_incarnation, relay_message_id)
      ON DELETE CASCADE
  )`
  yield* sql`CREATE INDEX effect_local_peer_relay_delivery_changes_hash
    ON effect_local_peer_relay_delivery_changes(replica_incarnation, change_hash, relay_message_id)`
  yield* sql`CREATE TABLE effect_local_command_delivery_events (
    event_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    replica_incarnation INTEGER NOT NULL CHECK (replica_incarnation >= 0),
    command_id TEXT,
    document_id TEXT NOT NULL,
    published INTEGER NOT NULL CHECK (published IN (0, 1))
  )`
  yield* sql`CREATE INDEX effect_local_command_delivery_events_unpublished
    ON effect_local_command_delivery_events(published, event_sequence)`
  yield* sql`CREATE TABLE effect_local_command_delivery_control (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    refresh_epoch INTEGER NOT NULL CHECK (refresh_epoch >= 0)
  )`
  yield* sql`INSERT INTO effect_local_command_delivery_control(singleton, refresh_epoch) VALUES (1, 0)`
  yield* sql`INSERT INTO effect_local_peer_relay_delivery_messages (
    replica_id, replica_incarnation,
    expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
    remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
    relay_message_id, outer_envelope_digest, sender_connection_epoch,
    sender_sequence, document_id, created_at, retry_deadline,
    relay_custody_accepted_at, sender_custody_unconfirmed_at
  )
  SELECT
    replica_id, replica_incarnation,
    expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
    remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
    relay_message_id, outer_envelope_digest, sender_connection_epoch,
    sender_sequence, document_id, created_at, retry_deadline,
    NULL, NULL
  FROM effect_local_peer_relay_outbox`
  yield* sql`INSERT INTO effect_local_peer_relay_delivery_changes (
    replica_incarnation, relay_message_id, change_hash
  )
  SELECT
    outbox.replica_incarnation,
    outbox.relay_message_id,
    json_extract(provenance.value, '$.changeHash') AS change_hash
  FROM effect_local_peer_relay_outbox AS outbox
  INNER JOIN json_each(outbox.writer_provenance) AS provenance
  ORDER BY outbox.replica_incarnation, outbox.relay_message_id, change_hash`

  yield* sql`CREATE TABLE effect_local_peer_relay_outbox_v11 (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    replica_id TEXT NOT NULL CHECK (length(replica_id) > 0),
    replica_incarnation INTEGER NOT NULL CHECK (replica_incarnation >= 0),
    writer_generation INTEGER NOT NULL CHECK (writer_generation >= 0),
    expected_local_tenant_id TEXT NOT NULL CHECK (length(expected_local_tenant_id) > 0),
    expected_local_subject_id TEXT NOT NULL CHECK (length(expected_local_subject_id) > 0),
    expected_local_peer_id TEXT NOT NULL CHECK (length(expected_local_peer_id) > 0),
    remote_tenant_id TEXT NOT NULL CHECK (length(remote_tenant_id) > 0),
    remote_subject_id TEXT NOT NULL CHECK (length(remote_subject_id) > 0),
    remote_peer_id TEXT NOT NULL CHECK (length(remote_peer_id) > 0),
    relay_peer_id TEXT NOT NULL CHECK (length(relay_peer_id) > 0),
    relay_message_id TEXT NOT NULL CHECK (length(relay_message_id) > 0),
    outer_envelope_digest TEXT NOT NULL CHECK (length(outer_envelope_digest) = 64),
    protocol_version INTEGER NOT NULL CHECK (protocol_version = 1),
    payload_version INTEGER NOT NULL CHECK (payload_version = 1),
    sender_connection_epoch TEXT NOT NULL CHECK (length(sender_connection_epoch) > 0),
    sender_sequence INTEGER NOT NULL CHECK (sender_sequence >= 0),
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE RESTRICT,
    document_type TEXT NOT NULL CHECK (length(document_type) > 0),
    writer_provenance TEXT NOT NULL,
    message_hash TEXT NOT NULL CHECK (length(message_hash) = 64),
    payload BLOB NOT NULL,
    encoded_size INTEGER NOT NULL CHECK (encoded_size > 0 AND encoded_size = length(payload)),
    created_at TEXT NOT NULL CHECK (length(created_at) > 0),
    retry_deadline TEXT NOT NULL CHECK (length(retry_deadline) > 0),
    next_attempt_at TEXT NOT NULL CHECK (length(next_attempt_at) > 0),
    CHECK (expected_local_tenant_id = remote_tenant_id),
    UNIQUE(replica_id, replica_incarnation, relay_message_id),
    UNIQUE(
      replica_id,
      replica_incarnation,
      relay_peer_id,
      remote_tenant_id,
      remote_subject_id,
      remote_peer_id,
      sender_connection_epoch,
      sender_sequence
    )
  )`
  yield* sql`INSERT INTO effect_local_peer_relay_outbox_v11 (
    row_id, replica_id, replica_incarnation, writer_generation,
    expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
    remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
    relay_message_id, outer_envelope_digest, protocol_version, payload_version,
    sender_connection_epoch, sender_sequence, document_id, document_type,
    writer_provenance, message_hash, payload, encoded_size, created_at,
    retry_deadline, next_attempt_at
  )
  SELECT
    row_id, replica_id, replica_incarnation, writer_generation,
    expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
    remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
    relay_message_id, outer_envelope_digest, protocol_version, payload_version,
    sender_connection_epoch, sender_sequence, document_id, document_type,
    writer_provenance, message_hash, payload, encoded_size, created_at,
    retry_deadline, next_attempt_at
  FROM effect_local_peer_relay_outbox`
  yield* sql`DROP TABLE effect_local_peer_relay_outbox`
  yield* sql`ALTER TABLE effect_local_peer_relay_outbox_v11 RENAME TO effect_local_peer_relay_outbox`
  yield* sql`CREATE INDEX effect_local_peer_relay_outbox_due_endpoint
    ON effect_local_peer_relay_outbox(
      replica_id, replica_incarnation, relay_peer_id, remote_tenant_id,
      remote_subject_id, remote_peer_id, next_attempt_at, row_id
    )`
  yield* sql`CREATE INDEX effect_local_peer_relay_outbox_retry_deadline
    ON effect_local_peer_relay_outbox(replica_id, replica_incarnation, retry_deadline, row_id)`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (11, 'command_delivery', ${commandDeliveryChecksum})`
})

const backupDocumentInstallationsMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`ALTER TABLE effect_local_backup_installations ADD COLUMN document_id TEXT`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (13, 'backup_document_installations', ${backupDocumentInstallationsChecksum})`
})

const checkpointShippingMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE effect_local_lineage_transitions (
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    prior_lineage TEXT NOT NULL,
    prior_checkpoint_hash TEXT NOT NULL,
    prior_heads TEXT NOT NULL,
    prior_snapshot BLOB NOT NULL,
    lineage TEXT NOT NULL,
    checkpoint_hash TEXT NOT NULL,
    heads TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    writer_definition_hash TEXT NOT NULL,
    authorization BLOB CHECK (
      authorization IS NULL OR length(authorization) BETWEEN 1 AND 16384
    ),
    created_at TEXT NOT NULL,
    PRIMARY KEY(document_id, lineage),
    UNIQUE(document_id, prior_lineage)
  )`
  yield* sql`ALTER TABLE effect_local_peer_outbox
    ADD COLUMN checkpoint_transfer BLOB`
  yield* sql`ALTER TABLE effect_local_peer_receipts
    ADD COLUMN checkpoint_transfer BLOB`
  yield* sql`CREATE INDEX effect_local_peer_outbox_document_status
    ON effect_local_peer_outbox(document_id, status)`
  yield* sql`CREATE INDEX effect_local_peer_relay_outbox_document
    ON effect_local_peer_relay_outbox(document_id)`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (14, 'checkpoint_shipping', ${checkpointShippingChecksum})`
})

const batchedSyncRepliesMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE effect_local_peer_receipt_replies (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_row_id INTEGER REFERENCES effect_local_peer_receipts(row_id) ON DELETE SET NULL,
    reply_index INTEGER NOT NULL CHECK(reply_index >= 0),
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    message BLOB NOT NULL,
    message_hash TEXT NOT NULL,
    heads TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('Pending', 'Sent')),
    UNIQUE(receipt_row_id, reply_index)
  )`
  yield* sql`INSERT INTO effect_local_peer_receipt_replies (
    receipt_row_id, reply_index, document_id, message, message_hash, heads, status
  )
  SELECT row_id, 0, document_id, reply, reply_hash, heads, 'Pending'
  FROM effect_local_peer_receipts
  WHERE reply IS NOT NULL AND reply_hash IS NOT NULL`
  yield* sql`CREATE INDEX effect_local_peer_receipt_replies_receipt_status
    ON effect_local_peer_receipt_replies(receipt_row_id, status, reply_index)`
  yield* sql`ALTER TABLE effect_local_peer_outbox ADD COLUMN receipt_reply_id INTEGER`
  yield* sql`CREATE INDEX effect_local_migration_15_receipt_match
    ON effect_local_peer_receipts(
      replica_incarnation, peer_id, document_id, reply_hash, row_id
    ) WHERE reply_hash IS NOT NULL`
  yield* sql`CREATE INDEX effect_local_migration_15_outbox_match
    ON effect_local_peer_outbox(
      replica_incarnation, peer_id, document_id, message_hash,
      connection_epoch, send_sequence, status
    )`
  yield* sql`UPDATE effect_local_peer_outbox AS outbox
    SET receipt_reply_id = (
      SELECT MIN(reply.row_id)
      FROM effect_local_peer_receipts AS receipt
      JOIN effect_local_peer_receipt_replies AS reply ON reply.receipt_row_id = receipt.row_id
      WHERE receipt.replica_incarnation = outbox.replica_incarnation
        AND receipt.peer_id = outbox.peer_id
        AND receipt.document_id = outbox.document_id
        AND receipt.reply_hash = outbox.message_hash
    )`
  yield* sql`CREATE UNIQUE INDEX effect_local_peer_outbox_receipt_reply
    ON effect_local_peer_outbox(
      replica_incarnation, peer_id, connection_epoch, receipt_reply_id
    )
    WHERE receipt_reply_id IS NOT NULL`
  yield* sql`CREATE INDEX effect_local_peer_outbox_pending_receipt_reply
    ON effect_local_peer_outbox(receipt_reply_id)
    WHERE status = 'Pending'`
  yield* sql`WITH coverage AS (
      SELECT reply.row_id AS reply_row_id, MAX(outbox.status = 'Sent') AS has_sent
      FROM effect_local_peer_receipt_replies AS reply
      JOIN effect_local_peer_receipts AS receipt ON receipt.row_id = reply.receipt_row_id
      JOIN effect_local_peer_outbox AS outbox
        ON outbox.replica_incarnation = receipt.replica_incarnation
        AND outbox.peer_id = receipt.peer_id
        AND outbox.document_id = receipt.document_id
        AND outbox.message_hash = reply.message_hash
      GROUP BY reply.row_id
    )
    UPDATE effect_local_peer_receipt_replies AS reply
    SET status = 'Sent'
    WHERE EXISTS (
      SELECT 1 FROM coverage
      WHERE coverage.reply_row_id = reply.row_id
        AND (
          coverage.has_sent = 1
          OR NOT EXISTS (
            SELECT 1 FROM effect_local_peer_outbox AS outbox
            WHERE outbox.receipt_reply_id = reply.row_id AND outbox.status = 'Pending'
          )
        )
    )`
  yield* sql`DROP INDEX effect_local_migration_15_outbox_match`
  yield* sql`DROP INDEX effect_local_migration_15_receipt_match`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (15, 'batched_sync_replies', ${batchedSyncRepliesChecksum})`
})

export const loader = Migrator.fromRecord({
  "1_canonical_store": migration,
  "2_peer_sync": peerSyncMigration,
  "3_durability_indexes": durabilityIndexesMigration,
  "4_projection_readiness": projectionReadinessMigration,
  "5_pending_receipt_indexes": pendingReceiptIndexesMigration,
  "6_peer_writer_provenance": peerWriterProvenanceMigration,
  "7_replica_health_indexes": replicaHealthIndexesMigration,
  "8_document_lineage": documentLineageMigration,
  "9_history_rewrite_markers": historyRewriteMarkersMigration,
  "10_peer_relay_state": peerRelayStateMigration,
  "11_command_delivery": commandDeliveryMigration,
  "12_document_history_counters": documentHistoryCountersMigration,
  "13_backup_document_installations": backupDocumentInstallationsMigration,
  "14_checkpoint_shipping": checkpointShippingMigration,
  "15_batched_sync_replies": batchedSyncRepliesMigration
})

const migrate = Migrator.make({})({ loader, table: "effect_local_migrations" })

export const run = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const findCatalog = SqlSchema.findAll({
    Request: Schema.Int,
    Result: Schema.Struct({ checksum: Schema.String, name: Schema.String }),
    execute: (migrationId) =>
      sql`SELECT name, checksum FROM effect_local_migration_catalog WHERE migration_id = ${migrationId}`
  })
  const expectedCatalog = literal([
    { id: 1, name: "canonical_store", checksum: canonicalStoreChecksum, label: "Canonical store" },
    { id: 2, name: "peer_sync", checksum: peerSyncChecksum, label: "Peer sync" },
    { id: 3, name: "durability_indexes", checksum: durabilityIndexesChecksum, label: "Durability indexes" },
    { id: 4, name: "projection_readiness", checksum: projectionReadinessChecksum, label: "Projection readiness" },
    {
      id: 5,
      name: "pending_receipt_indexes",
      checksum: pendingReceiptIndexesChecksum,
      label: "Pending receipt indexes"
    },
    {
      id: 6,
      name: "peer_writer_provenance",
      checksum: peerWriterProvenanceChecksum,
      label: "Peer writer provenance"
    },
    {
      id: 7,
      name: "replica_health_indexes",
      checksum: replicaHealthIndexesChecksum,
      label: "Replica health indexes"
    },
    { id: 8, name: "document_lineage", checksum: documentLineageChecksum, label: "Document lineage" },
    {
      id: 9,
      name: "history_rewrite_markers",
      checksum: historyRewriteMarkersChecksum,
      label: "History rewrite markers"
    },
    {
      id: 10,
      name: "peer_relay_state",
      checksum: peerRelayStateChecksum,
      label: "Peer relay state"
    },
    {
      id: 11,
      name: "command_delivery",
      checksum: commandDeliveryChecksum,
      label: "Command delivery"
    },
    {
      id: 12,
      name: "document_history_counters",
      checksum: documentHistoryCountersChecksum,
      label: "Document history counters"
    },
    {
      id: 13,
      name: "backup_document_installations",
      checksum: backupDocumentInstallationsChecksum,
      label: "Backup document installations"
    },
    {
      id: 14,
      name: "checkpoint_shipping",
      checksum: checkpointShippingChecksum,
      label: "Checkpoint shipping"
    },
    {
      id: 15,
      name: "batched_sync_replies",
      checksum: batchedSyncRepliesChecksum,
      label: "Batched sync replies"
    }
  ])
  // One transaction over migrate + validation so a rejected catalog rolls back
  // the freshly applied migrations instead of leaving a partial schema.
  return yield* sql.withTransaction(Effect.gen(function*() {
    const applied = yield* migrate
    for (const expected of expectedCatalog) {
      const row = (yield* findCatalog(expected.id))[0]
      if (row?.name !== expected.name || row?.checksum !== expected.checksum) {
        return yield* new Migrator.MigrationError({
          kind: "BadState",
          message: `${expected.label} migration checksum mismatch`
        })
      }
    }
    return applied
  }))
}).pipe(
  Effect.catchTag("SchemaError", (cause) =>
    Effect.fail(
      new Migrator.MigrationError({
        kind: "BadState",
        message: `Invalid migration catalog: ${String(cause)}`
      })
    ))
)

export const layer: Layer.Layer<never, Migrator.MigrationError | SqlError.SqlError, SqlClient.SqlClient> = Layer
  .effectDiscard(run)
