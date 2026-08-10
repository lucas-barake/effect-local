import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"

/**
 * Schema for the relay inbox.
 *
 * The lineage is pinned to its own table. The client replica lineage owns `effect_local_migrations`
 * and the cluster's own storage owns `effect_sql_migrations`, and a deployment can put all three in
 * one database, so sharing a table would collide migration ids across unrelated lineages.
 */
export const migratorTable = "effect_local_relay_inbox_migrations"

export const tableName = "effect_local_relay_inbox"

/**
 * Identity columns are compared byte for byte.
 *
 * MySQL's default collation is case insensitive, which would let two principals that differ only by
 * case address the same inbox — a route crossover between tenants rather than a cosmetic issue.
 */
const mysqlIdentity = (length: number) => `VARCHAR(${length}) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`

const createTable = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient

  /**
   * MySQL has no `CREATE INDEX IF NOT EXISTS`, and swallowing the error instead would also hide a
   * lock wait timeout, a dropped connection, or a key limit breach. Because MySQL commits DDL
   * implicitly and the migrator writes its completion marker before running the migration, a
   * silently skipped index would never be retried: the lineage would look applied while the unique
   * constraint that makes head selection deterministic was simply missing.
   */
  const countIndex = SqlSchema.findOne({
    Request: Schema.String,
    Result: Schema.Struct({
      count: Schema.Union([Schema.Int, Schema.NumberFromString]).check(Schema.isInt())
    }),
    execute: (name) =>
      sql`
        SELECT COUNT(*) AS count FROM information_schema.statistics
        WHERE table_schema = DATABASE() AND table_name = ${tableName} AND index_name = ${name}
      `
  })

  const createIndexMysql = (name: string, columns: string, unique: boolean) =>
    Effect.gen(function*() {
      const existing = yield* countIndex(name)
      if (existing.count > 0) return
      let uniqueness = ""
      if (unique) uniqueness = "UNIQUE "
      yield* sql.unsafe(`CREATE ${uniqueness}INDEX ${name} ON ${tableName} (${columns})`)
    })

  yield* sql.onDialectOrElse({
    orElse: () =>
      sql.unsafe(`
        CREATE TABLE IF NOT EXISTS ${tableName} (
          inbox_key TEXT NOT NULL,
          relay_message_id TEXT NOT NULL,
          channel_key TEXT NOT NULL,
          tenant_id TEXT NOT NULL,
          sender_subject_id TEXT NOT NULL,
          sender_peer_id TEXT NOT NULL,
          sender_replica_incarnation INTEGER NOT NULL,
          sender_connection_epoch TEXT NOT NULL,
          sender_sequence INTEGER NOT NULL,
          state TEXT NOT NULL,
          deliveries INTEGER NOT NULL DEFAULT 0,
          charged_at INTEGER NOT NULL DEFAULT 0,
          envelope TEXT NOT NULL,
          message_hash TEXT NOT NULL,
          outer_envelope_digest TEXT NOT NULL,
          byte_size INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          expires_at INTEGER NOT NULL,
          deduplicate_until INTEGER NOT NULL,
          terminal_at INTEGER,
          PRIMARY KEY (inbox_key, relay_message_id)
        )
      `),
    pg: () =>
      sql.unsafe(`
        CREATE TABLE IF NOT EXISTS ${tableName} (
          inbox_key TEXT NOT NULL,
          relay_message_id TEXT NOT NULL,
          channel_key TEXT NOT NULL,
          tenant_id TEXT NOT NULL,
          sender_subject_id TEXT NOT NULL,
          sender_peer_id TEXT NOT NULL,
          sender_replica_incarnation BIGINT NOT NULL,
          sender_connection_epoch TEXT NOT NULL,
          sender_sequence BIGINT NOT NULL,
          state TEXT NOT NULL,
          deliveries INTEGER NOT NULL DEFAULT 0,
          charged_at BIGINT NOT NULL DEFAULT 0,
          envelope TEXT NOT NULL,
          message_hash TEXT NOT NULL,
          outer_envelope_digest TEXT NOT NULL,
          byte_size BIGINT NOT NULL,
          created_at BIGINT NOT NULL,
          expires_at BIGINT NOT NULL,
          deduplicate_until BIGINT NOT NULL,
          terminal_at BIGINT,
          PRIMARY KEY (inbox_key, relay_message_id)
        )
      `),
    mysql: () =>
      sql.unsafe(`
        CREATE TABLE IF NOT EXISTS ${tableName} (
          inbox_key ${mysqlIdentity(64)} NOT NULL,
          relay_message_id ${mysqlIdentity(64)} NOT NULL,
          channel_key ${mysqlIdentity(64)} NOT NULL,
          tenant_id ${mysqlIdentity(128)} NOT NULL,
          sender_subject_id ${mysqlIdentity(256)} NOT NULL,
          sender_peer_id ${mysqlIdentity(64)} NOT NULL,
          sender_replica_incarnation BIGINT NOT NULL,
          sender_connection_epoch ${mysqlIdentity(128)} NOT NULL,
          sender_sequence BIGINT NOT NULL,
          state ${mysqlIdentity(16)} NOT NULL,
          deliveries INT NOT NULL DEFAULT 0,
          charged_at BIGINT NOT NULL DEFAULT 0,
          envelope LONGTEXT NOT NULL,
          message_hash ${mysqlIdentity(64)} NOT NULL,
          outer_envelope_digest ${mysqlIdentity(64)} NOT NULL,
          byte_size BIGINT NOT NULL,
          created_at BIGINT NOT NULL,
          expires_at BIGINT NOT NULL,
          deduplicate_until BIGINT NOT NULL,
          terminal_at BIGINT,
          PRIMARY KEY (inbox_key, relay_message_id)
        )
      `)
  })

  // Channel ordering. Indexed on the channel digest rather than the six raw channel columns: a
  // composite index over them would exceed MySQL's key length limit once `sender_subject_id` is
  // sized for real input, and the digest is fixed width.
  //
  // Unique because head selection picks the lowest `sender_sequence` in a channel. Two rows sharing
  // a sequence would make that choice arbitrary and could starve one of them indefinitely.
  yield* sql.onDialectOrElse({
    orElse: () =>
      sql.unsafe(
        `CREATE UNIQUE INDEX IF NOT EXISTS ${tableName}_channel_sequence
           ON ${tableName} (inbox_key, channel_key, sender_sequence)`
      ),
    mysql: () =>
      createIndexMysql(
        `${tableName}_channel_sequence`,
        "inbox_key, channel_key, sender_sequence",
        true
      )
  })

  yield* sql.onDialectOrElse({
    orElse: () =>
      sql.unsafe(
        `CREATE INDEX IF NOT EXISTS ${tableName}_pending_head
           ON ${tableName} (inbox_key, state, channel_key, sender_sequence)`
      ),
    mysql: () =>
      createIndexMysql(
        `${tableName}_pending_head`,
        "inbox_key, state, channel_key, sender_sequence",
        false
      )
  })

  yield* sql.onDialectOrElse({
    orElse: () =>
      sql.unsafe(
        `CREATE INDEX IF NOT EXISTS ${tableName}_expiry ON ${tableName} (state, expires_at)`
      ),
    mysql: () => createIndexMysql(`${tableName}_expiry`, "state, expires_at", false)
  })

  yield* sql.onDialectOrElse({
    orElse: () =>
      sql.unsafe(
        `CREATE INDEX IF NOT EXISTS ${tableName}_collect ON ${tableName} (state, deduplicate_until)`
      ),
    mysql: () => createIndexMysql(`${tableName}_collect`, "state, deduplicate_until", false)
  })
})

/**
 * `charged_at` records when a `Pending` row was last charged a delivery, so the budget can count
 * consecutive charges without settled inbox progress rather than lifetime transmissions. Databases
 * created by migration 1 after this column landed already have it, so the add is guarded by an
 * existence check on every dialect — none of the three supports `ADD COLUMN IF NOT EXISTS` in a
 * form usable here, and swallowing a duplicate-column error would also swallow real failures.
 */
const addChargedAt = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient

  const CountResult = Schema.Struct({
    count: Schema.Union([Schema.Int, Schema.NumberFromString]).check(Schema.isInt())
  })
  const columnCount = SqlSchema.findOne({
    Request: Schema.Void,
    Result: CountResult,
    execute: () =>
      sql.onDialectOrElse({
        orElse: () =>
          sql`
            SELECT COUNT(*) AS count FROM pragma_table_info(${tableName})
            WHERE name = 'charged_at'
          `,
        pg: () =>
          sql`
            SELECT COUNT(*) AS count FROM information_schema.columns
            WHERE table_name = ${tableName} AND column_name = 'charged_at'
          `,
        mysql: () =>
          sql`
            SELECT COUNT(*) AS count FROM information_schema.columns
            WHERE table_schema = DATABASE() AND table_name = ${tableName} AND column_name = 'charged_at'
          `
      })
  })

  const existing = yield* columnCount(undefined)
  if (existing.count > 0) return
  yield* sql.onDialectOrElse({
    orElse: () => sql.unsafe(`ALTER TABLE ${tableName} ADD COLUMN charged_at INTEGER NOT NULL DEFAULT 0`),
    pg: () => sql.unsafe(`ALTER TABLE ${tableName} ADD COLUMN charged_at BIGINT NOT NULL DEFAULT 0`),
    mysql: () => sql.unsafe(`ALTER TABLE ${tableName} ADD COLUMN charged_at BIGINT NOT NULL DEFAULT 0`)
  })
})

const loader = Migrator.fromRecord({
  "1_create_relay_inbox": createTable,
  "2_add_charged_at": addChargedAt
})

/**
 * MySQL commits DDL implicitly, and the migrator inserts its completion marker *before* running the
 * migration body, so an interrupted run leaves a partially built schema that later runs treat as
 * complete. Every statement above is therefore individually idempotent and individually verified
 * rather than relying on transactional rollback, which this dialect does not provide for DDL.
 */
export const run = Migrator.make({})({ loader, table: migratorTable })
