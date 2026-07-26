import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"

export const relayCustodyChecksum = "sha256:effect-local-relay-custody-v1"
export const relayMaintenanceIndexesChecksum = "sha256:effect-local-relay-maintenance-indexes-v1"

const custody = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE effect_local_relay_migration_catalog (
    migration_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_relay_channels (
    channel_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    sender_subject_id TEXT NOT NULL,
    sender_peer_id TEXT NOT NULL,
    sender_replica_incarnation INTEGER NOT NULL,
    recipient_subject_id TEXT NOT NULL,
    recipient_peer_id TEXT NOT NULL,
    next_sequence INTEGER NOT NULL DEFAULT 0 CHECK (next_sequence >= 0),
    claimed_message_id INTEGER,
    claim_session_generation INTEGER,
    claim_token TEXT,
    claim_deadline INTEGER,
    UNIQUE (
      tenant_id,
      sender_subject_id,
      sender_peer_id,
      sender_replica_incarnation,
      recipient_subject_id,
      recipient_peer_id
    )
  )`
  yield* sql`CREATE TABLE effect_local_relay_messages (
    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER NOT NULL REFERENCES effect_local_relay_channels(channel_id) ON DELETE CASCADE,
    channel_sequence INTEGER NOT NULL CHECK (channel_sequence >= 0),
    tenant_id TEXT NOT NULL,
    sender_subject_id TEXT NOT NULL,
    sender_peer_id TEXT NOT NULL,
    relay_message_id TEXT NOT NULL,
    relay_peer_id TEXT NOT NULL,
    sender_connection_epoch TEXT NOT NULL,
    sender_sequence INTEGER NOT NULL CHECK (sender_sequence >= 0),
    document_ids TEXT NOT NULL,
    payload_version INTEGER NOT NULL,
    message_hash TEXT NOT NULL,
    outer_envelope_digest TEXT NOT NULL,
    payload BLOB,
    payload_length INTEGER NOT NULL CHECK (payload_length >= 0),
    state TEXT NOT NULL CHECK (
      state IN ('Pending', 'Claimed', 'Acknowledged', 'DeadLettered', 'Expired')
    ),
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    deduplicate_until INTEGER NOT NULL,
    next_eligible_at INTEGER NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    claim_token TEXT,
    claim_session_generation INTEGER,
    claim_deadline INTEGER,
    terminal_at INTEGER,
    terminal_claim_token TEXT,
    terminal_session_generation INTEGER,
    terminal_reason TEXT,
    UNIQUE(channel_id, channel_sequence),
    UNIQUE(tenant_id, sender_subject_id, sender_peer_id, relay_message_id)
  )`
  yield* sql`CREATE TABLE effect_local_relay_usage (
    scope_kind TEXT NOT NULL CHECK (
      scope_kind IN ('SenderPeer', 'RecipientPeer', 'RecipientSubject', 'Tenant', 'Shard')
    ),
    scope_key TEXT NOT NULL,
    active_count INTEGER NOT NULL CHECK (active_count >= 0),
    active_bytes INTEGER NOT NULL CHECK (active_bytes >= 0),
    retained_count INTEGER NOT NULL CHECK (retained_count >= 0),
    retained_bytes INTEGER NOT NULL CHECK (retained_bytes >= 0),
    PRIMARY KEY(scope_kind, scope_key)
  )`
  yield* sql`CREATE TABLE effect_local_relay_reservations (
    message_id INTEGER PRIMARY KEY REFERENCES effect_local_relay_messages(message_id) ON DELETE CASCADE,
    sender_peer_usage_key TEXT NOT NULL,
    recipient_peer_usage_key TEXT NOT NULL,
    recipient_subject_usage_key TEXT NOT NULL,
    tenant_usage_key TEXT NOT NULL,
    shard_usage_key TEXT NOT NULL,
    active_count_delta INTEGER NOT NULL CHECK (active_count_delta = 1),
    active_bytes_delta INTEGER NOT NULL CHECK (active_bytes_delta >= 0),
    retained_count_delta INTEGER NOT NULL CHECK (retained_count_delta = 1),
    retained_bytes_delta INTEGER NOT NULL CHECK (retained_bytes_delta >= 0),
    active_consumed INTEGER NOT NULL DEFAULT 0 CHECK (active_consumed IN (0, 1)),
    retained_consumed INTEGER NOT NULL DEFAULT 0 CHECK (retained_consumed IN (0, 1))
  )`
  yield* sql`INSERT INTO effect_local_relay_migration_catalog (migration_id, name, checksum)
    VALUES (1, 'relay_custody', ${relayCustodyChecksum})`
})

const maintenanceIndexes = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE INDEX effect_local_relay_messages_channel_head
    ON effect_local_relay_messages(channel_id, channel_sequence, state, next_eligible_at)`
  yield* sql`CREATE INDEX effect_local_relay_channels_discovery
    ON effect_local_relay_channels(
      tenant_id,
      recipient_subject_id,
      recipient_peer_id,
      sender_subject_id,
      sender_peer_id,
      channel_id
    )`
  yield* sql`CREATE INDEX effect_local_relay_messages_admission_order
    ON effect_local_relay_messages(created_at, message_id, channel_id, channel_sequence)`
  yield* sql`CREATE INDEX effect_local_relay_messages_recovery
    ON effect_local_relay_messages(state, claim_deadline, message_id)`
  yield* sql`CREATE INDEX effect_local_relay_messages_expiry
    ON effect_local_relay_messages(expires_at, message_id)
    WHERE state IN ('Pending', 'Claimed')`
  yield* sql`CREATE INDEX effect_local_relay_messages_collection
    ON effect_local_relay_messages(deduplicate_until, message_id)
    WHERE state IN ('Acknowledged', 'DeadLettered', 'Expired')`
  yield* sql`CREATE INDEX effect_local_relay_channels_claim
    ON effect_local_relay_channels(claimed_message_id, channel_id)`
  yield* sql`INSERT INTO effect_local_relay_migration_catalog (migration_id, name, checksum)
    VALUES (2, 'relay_maintenance_indexes', ${relayMaintenanceIndexesChecksum})`
})

export const loader = Migrator.fromRecord({
  "1_relay_custody": custody,
  "2_relay_maintenance_indexes": maintenanceIndexes
})

const migrate = Migrator.make({})({
  loader,
  table: "effect_local_relay_migrations"
})

const expectedCatalog = [
  {
    id: 1,
    name: "relay_custody",
    checksum: relayCustodyChecksum,
    label: "Relay custody"
  },
  {
    id: 2,
    name: "relay_maintenance_indexes",
    checksum: relayMaintenanceIndexesChecksum,
    label: "Relay maintenance indexes"
  }
] as const

export const run = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const findCatalog = SqlSchema.findAll({
    Request: Schema.Int,
    Result: Schema.Struct({
      name: Schema.String,
      checksum: Schema.String
    }),
    execute: (migrationId) =>
      sql`SELECT name, checksum
        FROM effect_local_relay_migration_catalog
        WHERE migration_id = ${migrationId}`
  })
  return yield* sql.withTransaction(Effect.gen(function*() {
    const applied = yield* migrate
    for (const expected of expectedCatalog) {
      const rows = yield* findCatalog(expected.id)
      const row = rows[0]
      if (
        rows.length !== 1 ||
        row?.name !== expected.name ||
        row.checksum !== expected.checksum
      ) {
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
        message: `Invalid relay migration catalog: ${cause}`
      })
    ))
)

export const layer: Layer.Layer<
  never,
  Migrator.MigrationError | SqlError.SqlError,
  SqlClient.SqlClient
> = Layer.effectDiscard(run)
