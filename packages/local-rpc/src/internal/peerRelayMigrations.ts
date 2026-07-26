import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"

const executeStatements = (
  sql: SqlClient.SqlClient,
  source: string
) =>
  Effect.forEach(
    source.split(";").map((statement) => statement.trim()).filter((statement) => statement.length > 0),
    (statement) => sql.unsafe(statement),
    { discard: true }
  )

const mysqlIndexes = [
  {
    table: "effect_local_relay_messages",
    name: "effect_local_relay_messages_channel_head",
    columns: ["channel_id", "channel_sequence", "state", "next_eligible_at"],
    statement: `CREATE INDEX effect_local_relay_messages_channel_head
      ON effect_local_relay_messages(channel_id, channel_sequence, state, next_eligible_at)`
  },
  {
    table: "effect_local_relay_channels",
    name: "effect_local_relay_channels_discovery",
    columns: ["recipient_peer_id", "sender_peer_id", "channel_id"],
    statement: `CREATE INDEX effect_local_relay_channels_discovery
      ON effect_local_relay_channels(recipient_peer_id, sender_peer_id, channel_id)`
  },
  {
    table: "effect_local_relay_messages",
    name: "effect_local_relay_messages_admission_order",
    columns: ["created_at", "message_id", "channel_id", "channel_sequence"],
    statement: `CREATE INDEX effect_local_relay_messages_admission_order
      ON effect_local_relay_messages(created_at, message_id, channel_id, channel_sequence)`
  },
  {
    table: "effect_local_relay_messages",
    name: "effect_local_relay_messages_recovery",
    columns: ["state", "claim_deadline", "message_id"],
    statement: `CREATE INDEX effect_local_relay_messages_recovery
      ON effect_local_relay_messages(state, claim_deadline, message_id)`
  },
  {
    table: "effect_local_relay_messages",
    name: "effect_local_relay_messages_expiry",
    columns: ["state", "expires_at", "message_id"],
    statement: `CREATE INDEX effect_local_relay_messages_expiry
      ON effect_local_relay_messages(state, expires_at, message_id)`
  },
  {
    table: "effect_local_relay_messages",
    name: "effect_local_relay_messages_collection",
    columns: ["state", "deduplicate_until", "message_id"],
    statement: `CREATE INDEX effect_local_relay_messages_collection
      ON effect_local_relay_messages(state, deduplicate_until, message_id)`
  },
  {
    table: "effect_local_relay_channels",
    name: "effect_local_relay_channels_claim",
    columns: ["claimed_message_id", "channel_id"],
    statement: `CREATE INDEX effect_local_relay_channels_claim
      ON effect_local_relay_channels(claimed_message_id, channel_id)`
  },
  {
    table: "effect_local_relay_messages",
    name: "effect_local_relay_messages_claim_admission",
    columns: ["state", "tenant_id", "sender_peer_id", "recipient_peer_id", "created_at", "message_id"],
    statement: `CREATE INDEX effect_local_relay_messages_claim_admission
      ON effect_local_relay_messages(
        state,
        tenant_id,
        sender_peer_id,
        recipient_peer_id,
        created_at,
        message_id
      )`
  }
] as const

const migrationFailure = (message: string, cause?: unknown) =>
  new Migrator.MigrationError({
    kind: "Failed",
    message,
    ...(cause === undefined ? {} : { cause })
  })

const DatabaseInt = Schema.Union([Schema.Int, Schema.NumberFromString]).check(Schema.isInt())

const decodeMysqlRows = <S extends Schema.Constraint,>(
  schema: S,
  rows: unknown,
  message: string
) =>
  Schema.decodeUnknownEffect(schema)(rows).pipe(
    Effect.mapError((cause) => migrationFailure(message, cause))
  )

const createMysqlIndexes = (sql: SqlClient.SqlClient) =>
  Effect.gen(function*() {
    const rows = yield* sql.unsafe(`
      SELECT table_name AS tableName, index_name AS indexName
      FROM information_schema.statistics
      WHERE table_schema = DATABASE()
    `)
    const existing = yield* decodeMysqlRows(
      Schema.Array(Schema.Struct({
        tableName: Schema.String,
        indexName: Schema.String
      })),
      rows,
      "Could not inspect MySQL relay indexes"
    )
    const names = new Set(existing.map((row) => `${row.tableName}.${row.indexName}`))
    for (const index of mysqlIndexes) {
      if (!names.has(`${index.table}.${index.name}`)) {
        yield* sql.unsafe(index.statement)
      }
    }
  })

const createRelayTables = Effect.gen(function*() {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms()

  yield* sql.onDialectOrElse({
    pg: () =>
      executeStatements(
        sql,
        `
          CREATE TABLE effect_local_relay_write_lock (
            lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1)
          );
          INSERT INTO effect_local_relay_write_lock (lock_id) VALUES (1);

          CREATE TABLE effect_local_relay_channels (
            channel_id BIGSERIAL PRIMARY KEY,
            tenant_id VARCHAR(256) NOT NULL,
            sender_subject_id VARCHAR(256) NOT NULL,
            sender_peer_id VARCHAR(64) NOT NULL,
            sender_replica_incarnation BIGINT NOT NULL,
            recipient_subject_id VARCHAR(256) NOT NULL,
            recipient_peer_id VARCHAR(64) NOT NULL,
            next_sequence BIGINT NOT NULL DEFAULT 0 CHECK (next_sequence >= 0),
            claimed_message_id BIGINT,
            claim_session_generation BIGINT,
            claim_token VARCHAR(256),
            claim_deadline BIGINT,
            UNIQUE (
              tenant_id,
              sender_subject_id,
              sender_peer_id,
              sender_replica_incarnation,
              recipient_subject_id,
              recipient_peer_id
            )
          );

          CREATE TABLE effect_local_relay_messages (
            message_id BIGSERIAL PRIMARY KEY,
            channel_id BIGINT NOT NULL REFERENCES effect_local_relay_channels(channel_id) ON DELETE CASCADE,
            channel_sequence BIGINT NOT NULL CHECK (channel_sequence >= 0),
            tenant_id VARCHAR(256) NOT NULL,
            sender_subject_id VARCHAR(256) NOT NULL,
            sender_peer_id VARCHAR(64) NOT NULL,
            recipient_subject_id VARCHAR(256) NOT NULL,
            recipient_peer_id VARCHAR(64) NOT NULL,
            relay_message_id VARCHAR(64) NOT NULL,
            relay_peer_id VARCHAR(64) NOT NULL,
            sender_connection_epoch VARCHAR(256) NOT NULL,
            sender_sequence BIGINT NOT NULL CHECK (sender_sequence >= 0),
            document_ids TEXT NOT NULL,
            payload_version INTEGER NOT NULL,
            message_hash VARCHAR(256) NOT NULL,
            outer_envelope_digest VARCHAR(256) NOT NULL,
            payload BYTEA,
            payload_length BIGINT NOT NULL CHECK (payload_length >= 0),
            state VARCHAR(32) NOT NULL CHECK (
              state IN ('Pending', 'Claimed', 'Acknowledged', 'DeadLettered', 'Expired')
            ),
            created_at BIGINT NOT NULL,
            expires_at BIGINT NOT NULL,
            deduplicate_until BIGINT NOT NULL,
            next_eligible_at BIGINT NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
            claim_token VARCHAR(256),
            claim_session_generation BIGINT,
            claim_deadline BIGINT,
            terminal_at BIGINT,
            terminal_claim_token VARCHAR(256),
            terminal_session_generation BIGINT,
            terminal_reason VARCHAR(256),
            UNIQUE(channel_id, channel_sequence),
            UNIQUE(tenant_id, sender_subject_id, sender_peer_id, relay_message_id)
          );

          CREATE TABLE effect_local_relay_usage (
            scope_kind VARCHAR(32) NOT NULL,
            scope_key VARCHAR(1024) NOT NULL,
            active_count BIGINT NOT NULL CHECK (active_count >= 0),
            active_bytes BIGINT NOT NULL CHECK (active_bytes >= 0),
            retained_count BIGINT NOT NULL CHECK (retained_count >= 0),
            retained_bytes BIGINT NOT NULL CHECK (retained_bytes >= 0),
            PRIMARY KEY(scope_kind, scope_key)
          );

          CREATE TABLE effect_local_relay_reservations (
            message_id BIGINT PRIMARY KEY REFERENCES effect_local_relay_messages(message_id) ON DELETE CASCADE,
            sender_peer_usage_key VARCHAR(1024) NOT NULL,
            recipient_peer_usage_key VARCHAR(1024) NOT NULL,
            recipient_subject_usage_key VARCHAR(1024) NOT NULL,
            tenant_usage_key VARCHAR(1024) NOT NULL,
            shard_usage_key VARCHAR(1024) NOT NULL,
            active_count_delta INTEGER NOT NULL CHECK (active_count_delta = 1),
            active_bytes_delta BIGINT NOT NULL CHECK (active_bytes_delta >= 0),
            retained_count_delta INTEGER NOT NULL CHECK (retained_count_delta = 1),
            retained_bytes_delta BIGINT NOT NULL CHECK (retained_bytes_delta >= 0),
            active_consumed INTEGER NOT NULL DEFAULT 0 CHECK (active_consumed IN (0, 1)),
            retained_consumed INTEGER NOT NULL DEFAULT 0 CHECK (retained_consumed IN (0, 1))
          )
        `
      ),
    mysql: () =>
      executeStatements(
        sql,
        `
          CREATE TABLE IF NOT EXISTS effect_local_relay_write_lock (
            lock_id INT PRIMARY KEY,
            CHECK (lock_id = 1)
          );
          INSERT IGNORE INTO effect_local_relay_write_lock (lock_id) VALUES (1);

          CREATE TABLE IF NOT EXISTS effect_local_relay_channels (
            channel_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            tenant_id VARCHAR(256) COLLATE utf8mb4_bin NOT NULL,
            sender_subject_id VARCHAR(256) COLLATE utf8mb4_bin NOT NULL,
            sender_peer_id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
            sender_replica_incarnation BIGINT NOT NULL,
            recipient_subject_id VARCHAR(256) COLLATE utf8mb4_bin NOT NULL,
            recipient_peer_id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
            channel_identity CHAR(64) COLLATE utf8mb4_bin GENERATED ALWAYS AS (
              SHA2(JSON_ARRAY(
                tenant_id,
                sender_subject_id,
                sender_peer_id,
                sender_replica_incarnation,
                recipient_subject_id,
                recipient_peer_id
              ), 256)
            ) STORED,
            next_sequence BIGINT NOT NULL DEFAULT 0,
            claimed_message_id BIGINT,
            claim_session_generation BIGINT,
            claim_token VARCHAR(256) COLLATE utf8mb4_bin,
            claim_deadline BIGINT,
            UNIQUE KEY effect_local_relay_channels_identity (channel_identity),
            CHECK (next_sequence >= 0)
          );

          CREATE TABLE IF NOT EXISTS effect_local_relay_messages (
            message_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            channel_id BIGINT NOT NULL,
            channel_sequence BIGINT NOT NULL,
            tenant_id VARCHAR(256) COLLATE utf8mb4_bin NOT NULL,
            sender_subject_id VARCHAR(256) COLLATE utf8mb4_bin NOT NULL,
            sender_peer_id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
            recipient_subject_id VARCHAR(256) COLLATE utf8mb4_bin NOT NULL,
            recipient_peer_id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
            relay_message_id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
            relay_peer_id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
            sender_connection_epoch VARCHAR(256) COLLATE utf8mb4_bin NOT NULL,
            sender_sequence BIGINT NOT NULL,
            sender_message_identity CHAR(64) COLLATE utf8mb4_bin GENERATED ALWAYS AS (
              SHA2(JSON_ARRAY(
                tenant_id,
                sender_subject_id,
                sender_peer_id,
                relay_message_id
              ), 256)
            ) STORED,
            document_ids LONGTEXT NOT NULL,
            payload_version INT NOT NULL,
            message_hash VARCHAR(256) COLLATE utf8mb4_bin NOT NULL,
            outer_envelope_digest VARCHAR(256) COLLATE utf8mb4_bin NOT NULL,
            payload LONGBLOB,
            payload_length BIGINT NOT NULL,
            state VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
            created_at BIGINT NOT NULL,
            expires_at BIGINT NOT NULL,
            deduplicate_until BIGINT NOT NULL,
            next_eligible_at BIGINT NOT NULL,
            retry_count INT NOT NULL DEFAULT 0,
            claim_token VARCHAR(256) COLLATE utf8mb4_bin,
            claim_session_generation BIGINT,
            claim_deadline BIGINT,
            terminal_at BIGINT,
            terminal_claim_token VARCHAR(256) COLLATE utf8mb4_bin,
            terminal_session_generation BIGINT,
            terminal_reason VARCHAR(256) COLLATE utf8mb4_bin,
            UNIQUE KEY effect_local_relay_messages_channel_sequence(channel_id, channel_sequence),
            UNIQUE KEY effect_local_relay_messages_sender_identity(sender_message_identity),
            CONSTRAINT effect_local_relay_messages_channel_fk
              FOREIGN KEY(channel_id) REFERENCES effect_local_relay_channels(channel_id) ON DELETE CASCADE,
            CHECK (channel_sequence >= 0),
            CHECK (sender_sequence >= 0),
            CHECK (payload_length >= 0),
            CHECK (retry_count >= 0),
            CHECK (state IN ('Pending', 'Claimed', 'Acknowledged', 'DeadLettered', 'Expired'))
          );

          CREATE TABLE IF NOT EXISTS effect_local_relay_usage (
            scope_kind VARCHAR(32) COLLATE utf8mb4_bin NOT NULL,
            scope_key VARCHAR(700) COLLATE utf8mb4_bin NOT NULL,
            active_count BIGINT NOT NULL,
            active_bytes BIGINT NOT NULL,
            retained_count BIGINT NOT NULL,
            retained_bytes BIGINT NOT NULL,
            PRIMARY KEY(scope_kind, scope_key),
            CHECK (active_count >= 0),
            CHECK (active_bytes >= 0),
            CHECK (retained_count >= 0),
            CHECK (retained_bytes >= 0)
          );

          CREATE TABLE IF NOT EXISTS effect_local_relay_reservations (
            message_id BIGINT PRIMARY KEY,
            sender_peer_usage_key TEXT NOT NULL,
            recipient_peer_usage_key TEXT NOT NULL,
            recipient_subject_usage_key TEXT NOT NULL,
            tenant_usage_key TEXT NOT NULL,
            shard_usage_key TEXT NOT NULL,
            active_count_delta INT NOT NULL,
            active_bytes_delta BIGINT NOT NULL,
            retained_count_delta INT NOT NULL,
            retained_bytes_delta BIGINT NOT NULL,
            active_consumed INT NOT NULL DEFAULT 0,
            retained_consumed INT NOT NULL DEFAULT 0,
            CONSTRAINT effect_local_relay_reservations_message_fk
              FOREIGN KEY(message_id) REFERENCES effect_local_relay_messages(message_id) ON DELETE CASCADE,
            CHECK (active_count_delta = 1),
            CHECK (active_bytes_delta >= 0),
            CHECK (retained_count_delta = 1),
            CHECK (retained_bytes_delta >= 0),
            CHECK (active_consumed IN (0, 1)),
            CHECK (retained_consumed IN (0, 1))
          )
        `
      ),
    orElse: () =>
      executeStatements(
        sql,
        `
          CREATE TABLE effect_local_relay_write_lock (
            lock_id INTEGER PRIMARY KEY CHECK (lock_id = 1)
          );
          INSERT INTO effect_local_relay_write_lock (lock_id) VALUES (1);

          CREATE TABLE effect_local_relay_channels (
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
          );

          CREATE TABLE effect_local_relay_messages (
            message_id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL REFERENCES effect_local_relay_channels(channel_id) ON DELETE CASCADE,
            channel_sequence INTEGER NOT NULL CHECK (channel_sequence >= 0),
            tenant_id TEXT NOT NULL,
            sender_subject_id TEXT NOT NULL,
            sender_peer_id TEXT NOT NULL,
            recipient_subject_id TEXT NOT NULL,
            recipient_peer_id TEXT NOT NULL,
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
          );

          CREATE TABLE effect_local_relay_usage (
            scope_kind TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            active_count INTEGER NOT NULL CHECK (active_count >= 0),
            active_bytes INTEGER NOT NULL CHECK (active_bytes >= 0),
            retained_count INTEGER NOT NULL CHECK (retained_count >= 0),
            retained_bytes INTEGER NOT NULL CHECK (retained_bytes >= 0),
            PRIMARY KEY(scope_kind, scope_key)
          );

          CREATE TABLE effect_local_relay_reservations (
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
          )
        `
      )
  })

  yield* sql.onDialectOrElse({
    mysql: () => createMysqlIndexes(sql),
    orElse: () =>
      executeStatements(
        sql,
        `
          CREATE INDEX effect_local_relay_messages_channel_head
            ON effect_local_relay_messages(channel_id, channel_sequence, state, next_eligible_at);
          CREATE INDEX effect_local_relay_channels_discovery
            ON effect_local_relay_channels(
              tenant_id,
              recipient_subject_id,
              recipient_peer_id,
              sender_subject_id,
              sender_peer_id,
              channel_id
            );
          CREATE INDEX effect_local_relay_messages_admission_order
            ON effect_local_relay_messages(created_at, message_id, channel_id, channel_sequence);
          CREATE INDEX effect_local_relay_messages_recovery
            ON effect_local_relay_messages(state, claim_deadline, message_id);
          CREATE INDEX effect_local_relay_messages_expiry
            ON effect_local_relay_messages(expires_at, message_id);
          CREATE INDEX effect_local_relay_messages_collection
            ON effect_local_relay_messages(deduplicate_until, message_id);
          CREATE INDEX effect_local_relay_channels_claim
            ON effect_local_relay_channels(claimed_message_id, channel_id);
          CREATE INDEX effect_local_relay_messages_claim_admission
            ON effect_local_relay_messages(
              tenant_id,
              sender_subject_id,
              sender_peer_id,
              recipient_subject_id,
              recipient_peer_id,
              created_at,
              message_id
            )
        `
      )
  })
})

const migrations = Migrator.fromRecord({
  "0001_create_relay_tables": createRelayTables
})

const mysqlExpectedColumns = {
  effect_local_relay_write_lock: [
    "lock_id:int:NO"
  ],
  effect_local_relay_channels: [
    "channel_id:bigint:NO",
    "tenant_id:varchar:NO",
    "sender_subject_id:varchar:NO",
    "sender_peer_id:varchar:NO",
    "sender_replica_incarnation:bigint:NO",
    "recipient_subject_id:varchar:NO",
    "recipient_peer_id:varchar:NO",
    "channel_identity:char:YES",
    "next_sequence:bigint:NO",
    "claimed_message_id:bigint:YES",
    "claim_session_generation:bigint:YES",
    "claim_token:varchar:YES",
    "claim_deadline:bigint:YES"
  ],
  effect_local_relay_messages: [
    "message_id:bigint:NO",
    "channel_id:bigint:NO",
    "channel_sequence:bigint:NO",
    "tenant_id:varchar:NO",
    "sender_subject_id:varchar:NO",
    "sender_peer_id:varchar:NO",
    "recipient_subject_id:varchar:NO",
    "recipient_peer_id:varchar:NO",
    "relay_message_id:varchar:NO",
    "relay_peer_id:varchar:NO",
    "sender_connection_epoch:varchar:NO",
    "sender_sequence:bigint:NO",
    "sender_message_identity:char:YES",
    "document_ids:longtext:NO",
    "payload_version:int:NO",
    "message_hash:varchar:NO",
    "outer_envelope_digest:varchar:NO",
    "payload:longblob:YES",
    "payload_length:bigint:NO",
    "state:varchar:NO",
    "created_at:bigint:NO",
    "expires_at:bigint:NO",
    "deduplicate_until:bigint:NO",
    "next_eligible_at:bigint:NO",
    "retry_count:int:NO",
    "claim_token:varchar:YES",
    "claim_session_generation:bigint:YES",
    "claim_deadline:bigint:YES",
    "terminal_at:bigint:YES",
    "terminal_claim_token:varchar:YES",
    "terminal_session_generation:bigint:YES",
    "terminal_reason:varchar:YES"
  ],
  effect_local_relay_usage: [
    "scope_kind:varchar:NO",
    "scope_key:varchar:NO",
    "active_count:bigint:NO",
    "active_bytes:bigint:NO",
    "retained_count:bigint:NO",
    "retained_bytes:bigint:NO"
  ],
  effect_local_relay_reservations: [
    "message_id:bigint:NO",
    "sender_peer_usage_key:text:NO",
    "recipient_peer_usage_key:text:NO",
    "recipient_subject_usage_key:text:NO",
    "tenant_usage_key:text:NO",
    "shard_usage_key:text:NO",
    "active_count_delta:int:NO",
    "active_bytes_delta:bigint:NO",
    "retained_count_delta:int:NO",
    "retained_bytes_delta:bigint:NO",
    "active_consumed:int:NO",
    "retained_consumed:int:NO"
  ]
} as const

const mysqlBinaryColumns = new Set([
  "effect_local_relay_channels.tenant_id",
  "effect_local_relay_channels.sender_subject_id",
  "effect_local_relay_channels.sender_peer_id",
  "effect_local_relay_channels.recipient_subject_id",
  "effect_local_relay_channels.recipient_peer_id",
  "effect_local_relay_channels.channel_identity",
  "effect_local_relay_channels.claim_token",
  "effect_local_relay_messages.tenant_id",
  "effect_local_relay_messages.sender_subject_id",
  "effect_local_relay_messages.sender_peer_id",
  "effect_local_relay_messages.recipient_subject_id",
  "effect_local_relay_messages.recipient_peer_id",
  "effect_local_relay_messages.relay_message_id",
  "effect_local_relay_messages.relay_peer_id",
  "effect_local_relay_messages.sender_connection_epoch",
  "effect_local_relay_messages.sender_message_identity",
  "effect_local_relay_messages.message_hash",
  "effect_local_relay_messages.outer_envelope_digest",
  "effect_local_relay_messages.state",
  "effect_local_relay_messages.claim_token",
  "effect_local_relay_messages.terminal_claim_token",
  "effect_local_relay_messages.terminal_reason",
  "effect_local_relay_usage.scope_kind",
  "effect_local_relay_usage.scope_key"
])

const mysqlRequiredIndexes = [
  ...mysqlIndexes,
  {
    table: "effect_local_relay_write_lock",
    name: "PRIMARY",
    columns: ["lock_id"]
  },
  {
    table: "effect_local_relay_channels",
    name: "PRIMARY",
    columns: ["channel_id"]
  },
  {
    table: "effect_local_relay_channels",
    name: "effect_local_relay_channels_identity",
    columns: ["channel_identity"]
  },
  {
    table: "effect_local_relay_messages",
    name: "PRIMARY",
    columns: ["message_id"]
  },
  {
    table: "effect_local_relay_messages",
    name: "effect_local_relay_messages_channel_sequence",
    columns: ["channel_id", "channel_sequence"]
  },
  {
    table: "effect_local_relay_messages",
    name: "effect_local_relay_messages_sender_identity",
    columns: ["sender_message_identity"]
  },
  {
    table: "effect_local_relay_usage",
    name: "PRIMARY",
    columns: ["scope_kind", "scope_key"]
  },
  {
    table: "effect_local_relay_reservations",
    name: "PRIMARY",
    columns: ["message_id"]
  }
] as const

const verifyMysqlSchema = (sql: SqlClient.SqlClient) =>
  Effect.gen(function*() {
    const columnRows = yield* sql.unsafe(`
      SELECT
        table_name AS tableName,
        column_name AS columnName,
        data_type AS dataType,
        is_nullable AS isNullable,
        collation_name AS collationName
      FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name IN (
          'effect_local_relay_write_lock',
          'effect_local_relay_channels',
          'effect_local_relay_messages',
          'effect_local_relay_usage',
          'effect_local_relay_reservations'
        )
      ORDER BY table_name, ordinal_position
    `)
    const columns = yield* decodeMysqlRows(
      Schema.Array(Schema.Struct({
        tableName: Schema.String,
        columnName: Schema.String,
        dataType: Schema.String,
        isNullable: Schema.String,
        collationName: Schema.NullOr(Schema.String)
      })),
      columnRows,
      "Could not inspect MySQL relay columns"
    )
    for (const [table, expected] of Object.entries(mysqlExpectedColumns)) {
      const actual = columns
        .filter((column) => column.tableName === table)
        .map((column) => `${column.columnName}:${column.dataType}:${column.isNullable}`)
      if (actual.length !== expected.length || actual.some((value, index) => value !== expected[index])) {
        return yield* migrationFailure(`MySQL relay table "${table}" does not match migration 1`)
      }
    }
    for (const column of columns) {
      if (
        mysqlBinaryColumns.has(`${column.tableName}.${column.columnName}`) &&
        column.collationName !== "utf8mb4_bin"
      ) {
        return yield* migrationFailure(
          `MySQL relay column "${column.tableName}.${column.columnName}" must use utf8mb4_bin`
        )
      }
    }

    const indexRows = yield* sql.unsafe(`
      SELECT
        table_name AS tableName,
        index_name AS indexName,
        column_name AS columnName,
        seq_in_index AS sequence
      FROM information_schema.statistics
      WHERE table_schema = DATABASE()
      ORDER BY table_name, index_name, seq_in_index
    `)
    const indexes = yield* decodeMysqlRows(
      Schema.Array(Schema.Struct({
        tableName: Schema.String,
        indexName: Schema.String,
        columnName: Schema.String,
        sequence: DatabaseInt
      })),
      indexRows,
      "Could not inspect MySQL relay indexes"
    )
    for (const expected of mysqlRequiredIndexes) {
      const actual = indexes
        .filter((index) => index.tableName === expected.table && index.indexName === expected.name)
        .toSorted((left, right) => left.sequence - right.sequence)
        .map((index) => index.columnName)
      if (
        actual.length !== expected.columns.length ||
        actual.some((column, index) => column !== expected.columns[index])
      ) {
        return yield* migrationFailure(`MySQL relay index "${expected.name}" is missing or invalid`)
      }
    }

    const foreignKeyRows = yield* sql.unsafe(`
      SELECT
        usage_table.table_name AS tableName,
        usage_table.column_name AS columnName,
        usage_table.referenced_table_name AS referencedTableName,
        usage_table.referenced_column_name AS referencedColumnName,
        rules.delete_rule AS deleteRule
      FROM information_schema.key_column_usage usage_table
      JOIN information_schema.referential_constraints rules
        ON rules.constraint_schema = usage_table.constraint_schema
        AND rules.constraint_name = usage_table.constraint_name
      WHERE usage_table.constraint_schema = DATABASE()
        AND usage_table.referenced_table_name IS NOT NULL
        AND usage_table.table_name IN (
          'effect_local_relay_messages',
          'effect_local_relay_reservations'
        )
      ORDER BY usage_table.table_name
    `)
    const foreignKeys = yield* decodeMysqlRows(
      Schema.Array(Schema.Struct({
        tableName: Schema.String,
        columnName: Schema.String,
        referencedTableName: Schema.String,
        referencedColumnName: Schema.String,
        deleteRule: Schema.String
      })),
      foreignKeyRows,
      "Could not inspect MySQL relay foreign keys"
    )
    const actualForeignKeys = foreignKeys.map((foreignKey) =>
      [
        foreignKey.tableName,
        foreignKey.columnName,
        foreignKey.referencedTableName,
        foreignKey.referencedColumnName,
        foreignKey.deleteRule
      ].join(":")
    )
    const expectedForeignKeys = [
      "effect_local_relay_messages:channel_id:effect_local_relay_channels:channel_id:CASCADE",
      "effect_local_relay_reservations:message_id:effect_local_relay_messages:message_id:CASCADE"
    ]
    if (
      actualForeignKeys.length !== expectedForeignKeys.length ||
      actualForeignKeys.some((foreignKey, index) => foreignKey !== expectedForeignKeys[index])
    ) {
      return yield* migrationFailure("MySQL relay foreign keys do not match migration 1")
    }

    const checkRows = yield* sql.unsafe(`
      SELECT constraint_name AS constraintName
      FROM information_schema.table_constraints
      WHERE constraint_schema = DATABASE()
        AND constraint_type = 'CHECK'
        AND table_name IN (
          'effect_local_relay_write_lock',
          'effect_local_relay_channels',
          'effect_local_relay_messages',
          'effect_local_relay_usage',
          'effect_local_relay_reservations'
        )
    `)
    const checks = yield* decodeMysqlRows(
      Schema.Array(Schema.Struct({ constraintName: Schema.String })),
      checkRows,
      "Could not inspect MySQL relay check constraints"
    )
    if (checks.length !== 17) {
      return yield* migrationFailure("MySQL relay check constraints do not match migration 1")
    }
  })

const genericRun = Migrator.make({})({
  loader: migrations,
  table: "effect_local_relay_migrations"
})

const runMysql = (sql: SqlClient.SqlClient) =>
  sql.withTransaction(
    Effect.acquireUseRelease(
      Effect.gen(function*() {
        const lockRows = yield* sql`SELECT GET_LOCK(
          'effect_local_relay_migrations',
          30
        ) AS acquired`
        const lock = yield* decodeMysqlRows(
          Schema.Array(Schema.Struct({ acquired: Schema.NullOr(DatabaseInt) })),
          lockRows,
          "Could not decode the MySQL relay migration lock result"
        )
        if (lock.length !== 1 || lock[0]!.acquired !== 1) {
          return yield* new Migrator.MigrationError({
            kind: "Locked",
            message: "Could not acquire the MySQL relay migration lock"
          })
        }
      }),
      () =>
        Effect.gen(function*() {
          yield* sql`
            CREATE TABLE IF NOT EXISTS effect_local_relay_migrations (
              migration_id INTEGER UNSIGNED NOT NULL,
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              name VARCHAR(255) NOT NULL,
              PRIMARY KEY (migration_id)
            )
          `
          const markerRows = yield* sql`
            SELECT migration_id AS migrationId
            FROM effect_local_relay_migrations
            WHERE migration_id = 1
          `
          const markers = yield* decodeMysqlRows(
            Schema.Array(Schema.Struct({ migrationId: DatabaseInt })),
            markerRows,
            "Could not decode MySQL relay migration state"
          )
          if (markers.length > 1) {
            return yield* migrationFailure("MySQL relay migration state contains duplicate markers")
          }
          if (markers.length === 0) {
            yield* createRelayTables
          }
          yield* verifyMysqlSchema(sql)
          if (markers.length === 0) {
            yield* sql`
              INSERT INTO effect_local_relay_migrations (migration_id, name)
              VALUES (1, 'create_relay_tables')
            `
          }
          return markers.length === 0
            ? [[1, "create_relay_tables"] as const]
            : []
        }),
      () =>
        sql`SELECT RELEASE_LOCK('effect_local_relay_migrations')`.pipe(
          Effect.asVoid
        )
    )
  )

export const run = Effect.gen(function*() {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms()
  return yield* sql.onDialectOrElse({
    mysql: () => runMysql(sql),
    orElse: () => genericRun
  })
})

export const layer: Layer.Layer<
  never,
  Migrator.MigrationError | SqlError.SqlError,
  SqlClient.SqlClient
> = Layer.effectDiscard(run)
