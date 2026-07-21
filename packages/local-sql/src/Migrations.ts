import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"

export const canonicalStoreChecksum = "sha256:effect-local-canonical-store-v1"
export const peerSyncChecksum = "sha256:effect-local-peer-sync-v3"
export const durabilityIndexesChecksum = "sha256:effect-local-durability-indexes-v1"

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

export const loader = Effect.succeed([
  [1, "canonical_store", Effect.succeed(migration)] as const,
  [2, "peer_sync", Effect.succeed(peerSyncMigration)] as const,
  [3, "durability_indexes", Effect.succeed(durabilityIndexesMigration)] as const
])

const migrate = Migrator.make({})({ loader, table: "effect_local_migrations" })

export const run = Effect.gen(function*() {
  const applied = yield* migrate
  const sql = yield* SqlClient.SqlClient
  const rows = yield* sql<{ readonly checksum: string; readonly name: string }>`
    SELECT name, checksum FROM effect_local_migration_catalog WHERE migration_id = 1
  `
  if (rows[0]?.name !== "canonical_store" || rows[0]?.checksum !== canonicalStoreChecksum) {
    return yield* new Migrator.MigrationError({
      kind: "BadState",
      message: "Canonical store migration checksum mismatch"
    })
  }
  const peerRows = yield* sql<{ readonly checksum: string; readonly name: string }>`
    SELECT name, checksum FROM effect_local_migration_catalog WHERE migration_id = 2
  `
  if (peerRows[0]?.name !== "peer_sync" || peerRows[0]?.checksum !== peerSyncChecksum) {
    return yield* new Migrator.MigrationError({
      kind: "BadState",
      message: "Peer sync migration checksum mismatch"
    })
  }
  const durabilityRows = yield* sql<{ readonly checksum: string; readonly name: string }>`
    SELECT name, checksum FROM effect_local_migration_catalog WHERE migration_id = 3
  `
  if (
    durabilityRows[0]?.name !== "durability_indexes" ||
    durabilityRows[0]?.checksum !== durabilityIndexesChecksum
  ) {
    return yield* new Migrator.MigrationError({
      kind: "BadState",
      message: "Durability indexes migration checksum mismatch"
    })
  }
  return applied
})

export const layer: Layer.Layer<never, Migrator.MigrationError | SqlError.SqlError, SqlClient.SqlClient> = Layer
  .effectDiscard(run)
