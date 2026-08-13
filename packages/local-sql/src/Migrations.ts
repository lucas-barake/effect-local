import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Configuration from "./internal/configuration.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"

export type Catalog = "Client" | "Server"

export interface Migration {
  readonly id: number
  readonly name: string
  readonly checksum: Identity.SchemaHash
  readonly statements: ReadonlyArray<string>
  readonly effect?: ((sql: SqlClient.SqlClient) => Effect.Effect<void, ReplicaError.ReplicaError>) | undefined
}

export interface Options {
  readonly retryDelay: Duration.Input
  readonly maximumAttempts: number
}

const defaultOptions: Options = { retryDelay: "5 millis", maximumAttempts: 8 }

const stableName = /^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$/

/* oxlint-disable effect/noThrowStatement, effect/noNewError -- Migration descriptors are synchronous schema values and must reject invalid catalogs before any Effect is constructed. */
export const makeMigration = (options: {
  readonly id: number
  readonly name: string
  readonly statements: ReadonlyArray<string>
  readonly effect?: {
    readonly id: string
    readonly run: (sql: SqlClient.SqlClient) => Effect.Effect<void, ReplicaError.ReplicaError>
  } | undefined
}): Migration => {
  if (!Number.isSafeInteger(options.id) || options.id <= 0) {
    throw new TypeError(`Storage migration id must be a positive safe integer: ${options.id}`)
  }
  if (!stableName.test(options.name)) throw new TypeError(`Storage migration name is not stable: ${options.name}`)
  if (options.statements.length === 0) throw new TypeError(`Storage migration ${options.name} has no statements`)
  if (options.effect !== undefined && !stableName.test(options.effect.id)) {
    throw new TypeError(`Storage migration effect id is not stable: ${options.effect.id}`)
  }
  const statements = Object.freeze([...options.statements])
  const migration = {
    id: options.id,
    name: options.name,
    checksum: Identity.SchemaHash.make(Canonical.hash({
      format: 1,
      id: options.id,
      name: options.name,
      statements,
      effect: options.effect?.id ?? null
    })),
    statements
  }
  if (options.effect === undefined) return Object.freeze(migration)
  return Object.freeze({ ...migration, effect: options.effect.run })
}
/* oxlint-enable effect/noThrowStatement, effect/noNewError */

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const MigrationRow = Schema.Struct({
  id: PositiveInt,
  name: Schema.String,
  checksum: Identity.SchemaHash
})
const CountRow = Schema.Struct({ count: NonNegativeInt })
const ForeignKeyCheckRow = Schema.Struct({
  table: Schema.String,
  rowid: Schema.NullOr(Schema.Int),
  parent: Schema.String,
  fkid: Schema.Int
})

const ClientIdentityRow = Schema.Struct({
  client_id: Identity.ClientId
})
const PragmaEnabledRow = Schema.Struct({ foreign_keys: Schema.Literals([0, 1]) })

const validateCatalog = (
  catalog: Catalog,
  migrations: ReadonlyArray<Migration>
): ReplicaError.StorageMigrationMismatch | undefined => {
  const names = new Set<string>()
  for (let index = 0; index < migrations.length; index++) {
    const migration = migrations[index]
    if (migration.id !== index + 1) {
      return new ReplicaError.StorageMigrationMismatch({
        catalog,
        message: `${catalog} migration ids must be contiguous from 1. Expected ${index + 1}, got ${migration.id}`
      })
    }
    if (names.has(migration.name)) {
      return new ReplicaError.StorageMigrationMismatch({
        catalog,
        message: `Duplicate migration name: ${migration.name}`
      })
    }
    names.add(migration.name)
  }
  return undefined
}

const clientLedger = `CREATE TABLE IF NOT EXISTS effect_local_client_migrations (
  id INTEGER PRIMARY KEY CHECK (id > 0),
  name TEXT NOT NULL UNIQUE,
  checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

const serverLedger = `CREATE TABLE IF NOT EXISTS effect_local_server_migrations (
  id INTEGER PRIMARY KEY CHECK (id > 0),
  name TEXT NOT NULL UNIQUE,
  checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

export const runCatalog = Effect.fn("Migrations.runCatalog")(function*(
  catalog: Catalog,
  migrations: ReadonlyArray<Migration>,
  options: Options = defaultOptions
) {
  yield* Effect.annotateCurrentSpan({
    "migration.catalog": catalog,
    "migration.count": migrations.length
  })
  const invalid = validateCatalog(catalog, migrations)
  if (invalid !== undefined) return yield* invalid
  if (!Number.isSafeInteger(options.maximumAttempts) || options.maximumAttempts <= 0) {
    return yield* new ReplicaError.InvalidConfiguration({
      option: "migration.maximumAttempts",
      message: "migration.maximumAttempts must be a positive safe integer"
    })
  }
  const retryDelayMillis = yield* Configuration.positiveFiniteDurationMillis(
    "migration.retryDelay",
    options.retryDelay
  )
  const sql = yield* SqlClient.SqlClient
  const readClient = SqlSchema.findAll({
    Request: Schema.Void,
    Result: MigrationRow,
    execute: () => sql`SELECT id, name, checksum FROM effect_local_client_migrations ORDER BY id`
  })
  const readServer = SqlSchema.findAll({
    Request: Schema.Void,
    Result: MigrationRow,
    execute: () => sql`SELECT id, name, checksum FROM effect_local_server_migrations ORDER BY id`
  })
  let appliedAtAttempt = 0
  const migrate = Effect.gen(function*() {
    let ledger = serverLedger
    if (catalog === "Client") ledger = clientLedger
    yield* sql.unsafe(ledger)
    yield* sql.withTransaction(Effect.gen(function*() {
      let read = readServer
      if (catalog === "Client") read = readClient
      const applied = yield* read(undefined).pipe(
        Effect.mapError((cause) => {
          if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
          return new ReplicaError.StorageCorrupt({ message: `${catalog} migration ledger is corrupt`, cause })
        })
      )
      appliedAtAttempt = applied.length
      if (applied.length > migrations.length) {
        return yield* new ReplicaError.StorageMigrationMismatch({
          catalog,
          message: `${catalog} catalog deleted ${applied.length - migrations.length} applied migration(s)`
        })
      }
      for (let index = 0; index < applied.length; index++) {
        const stored = applied[index]
        const expected = migrations[index]
        if (stored.id !== expected.id || stored.name !== expected.name || stored.checksum !== expected.checksum) {
          return yield* new ReplicaError.StorageMigrationMismatch({
            catalog,
            message:
              `Applied migration ${stored.id}:${stored.name}:${stored.checksum} does not match ${expected.id}:${expected.name}:${expected.checksum}`
          })
        }
      }
      for (let index = applied.length; index < migrations.length; index++) {
        const migration = migrations[index]
        if (catalog === "Client") {
          yield* sql`INSERT INTO effect_local_client_migrations (id, name, checksum)
          VALUES (${migration.id}, ${migration.name}, ${migration.checksum})`
        } else {
          yield* sql`INSERT INTO effect_local_server_migrations (id, name, checksum)
            VALUES (${migration.id}, ${migration.name}, ${migration.checksum})`
        }
      }
      for (let index = applied.length; index < migrations.length; index++) {
        const migration = migrations[index]
        yield* Effect.forEach(migration.statements, (statement) => sql.unsafe(statement), { discard: true })
        if (migration.effect !== undefined) yield* migration.effect(sql)
      }
      return yield* Effect.void
    }))
  }).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))

  let attempt = 1
  while (true) {
    const result = yield* migrate.pipe(Effect.result)
    if (Result.isSuccess(result)) return yield* Effect.void
    const failure = result.failure
    if (
      failure._tag === "StorageUnavailable" &&
      SqlError.isSqlError(failure.cause) &&
      (failure.cause.reason._tag === "ConstraintError" || failure.cause.reason._tag === "UniqueViolation")
    ) {
      let read = readServer
      if (catalog === "Client") read = readClient
      const applied = yield* read(undefined).pipe(
        Effect.mapError((cause) => {
          if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
          return new ReplicaError.StorageCorrupt({ message: `${catalog} migration ledger is corrupt`, cause })
        })
      )
      let valid = applied.length <= migrations.length
      for (let index = 0; valid && index < applied.length; index++) {
        const stored = applied[index]
        const expected = migrations[index]
        valid = stored.id === expected.id && stored.name === expected.name && stored.checksum === expected.checksum
      }
      if (valid && applied.length > appliedAtAttempt) {
        if (applied.length === migrations.length) return yield* Effect.void
        continue
      }
      return yield* new ReplicaError.StorageCorrupt({
        message: `${catalog} migration failed a permanent constraint`,
        cause: failure.cause
      })
    }
    if (
      failure._tag !== "StorageUnavailable" ||
      !SqlError.isSqlError(failure.cause) ||
      failure.cause.reason._tag !== "LockTimeoutError" ||
      attempt >= options.maximumAttempts
    ) return yield* failure
    attempt += 1
    yield* Effect.sleep(retryDelayMillis)
  }
})

const clientV1 = makeMigration({
  id: 1,
  name: "mutation-log",
  statements: [
    `CREATE TABLE IF NOT EXISTS effect_local_client_meta (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      definition_hash TEXT NOT NULL,
      next_local_sequence INTEGER NOT NULL,
      server_cursor INTEGER NOT NULL,
      visible_revision INTEGER NOT NULL,
      requested_generation INTEGER NOT NULL DEFAULT 0 CHECK (requested_generation >= 0),
      completed_generation INTEGER NOT NULL DEFAULT 0 CHECK (
        completed_generation >= 0 AND completed_generation <= requested_generation
      )
    )`,
    `CREATE TABLE IF NOT EXISTS effect_local_pending (
      mutation_id TEXT PRIMARY KEY,
      local_sequence INTEGER NOT NULL UNIQUE,
      basis INTEGER NOT NULL,
      name TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      digest TEXT NOT NULL,
      optimistic_result_json TEXT NOT NULL,
      changes_json TEXT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS effect_local_receipts (
      mutation_id TEXT PRIMARY KEY,
      local_sequence INTEGER NOT NULL UNIQUE,
      receipt_json TEXT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS effect_local_server_log (
      server_sequence INTEGER PRIMARY KEY,
      mutation_id TEXT NOT NULL UNIQUE,
      entry_json TEXT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS effect_local_canonical_entities (
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      PRIMARY KEY (model, entity_key)
    )`,
    `CREATE TABLE IF NOT EXISTS effect_local_visible_entities (
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      PRIMARY KEY (model, entity_key)
    )`
  ]
})

const clientV2 = makeMigration({
  id: 2,
  name: "schema-evolution",
  statements: [
    "ALTER TABLE effect_local_client_meta ADD COLUMN schema_version INTEGER",
    "ALTER TABLE effect_local_client_meta ADD COLUMN schema_hash TEXT",
    "ALTER TABLE effect_local_client_meta ADD COLUMN schema_generation INTEGER NOT NULL DEFAULT 0 CHECK (schema_generation >= 0)",
    "ALTER TABLE effect_local_client_meta ADD COLUMN target_schema_version INTEGER",
    "ALTER TABLE effect_local_client_meta ADD COLUMN target_schema_hash TEXT",
    "ALTER TABLE effect_local_client_meta ADD COLUMN migration_hash TEXT",
    "ALTER TABLE effect_local_pending ADD COLUMN digest_version INTEGER NOT NULL DEFAULT 1 CHECK (digest_version IN (1, 2))",
    "ALTER TABLE effect_local_pending ADD COLUMN source_schema_version INTEGER",
    "ALTER TABLE effect_local_pending ADD COLUMN source_schema_hash TEXT",
    "ALTER TABLE effect_local_pending ADD COLUMN mutation_version INTEGER",
    "ALTER TABLE effect_local_receipts ADD COLUMN source_schema_version INTEGER",
    "ALTER TABLE effect_local_receipts ADD COLUMN source_schema_hash TEXT",
    "ALTER TABLE effect_local_receipts ADD COLUMN mutation_version INTEGER",
    "ALTER TABLE effect_local_receipts ADD COLUMN rejection_origin TEXT",
    "ALTER TABLE effect_local_server_log ADD COLUMN source_schema_version INTEGER",
    "ALTER TABLE effect_local_server_log ADD COLUMN source_schema_hash TEXT",
    "ALTER TABLE effect_local_server_log ADD COLUMN mutation_version INTEGER",
    "ALTER TABLE effect_local_canonical_entities ADD COLUMN model_version INTEGER",
    "ALTER TABLE effect_local_visible_entities ADD COLUMN model_version INTEGER",
    `CREATE TABLE effect_local_client_evolution (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      target_schema_version INTEGER NOT NULL,
      target_schema_hash TEXT NOT NULL,
      migration_hash TEXT NOT NULL,
      generation INTEGER NOT NULL CHECK (generation > 0),
      phase TEXT NOT NULL,
      cursor_model TEXT,
      cursor_key TEXT
    )`,
    `CREATE TABLE effect_local_client_shadow_entities (
      generation INTEGER NOT NULL,
      model TEXT NOT NULL,
      model_version INTEGER NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      PRIMARY KEY (generation, model, entity_key)
    )`,
    `CREATE TABLE effect_local_client_shadow_receipts (
      generation INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      receipt_json TEXT NOT NULL,
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      mutation_version INTEGER NOT NULL,
      rejection_origin TEXT,
      PRIMARY KEY (generation, mutation_id),
      UNIQUE (generation, local_sequence)
    )`,
    `CREATE TABLE effect_local_client_key_lineage (
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      source_model TEXT NOT NULL,
      source_model_version INTEGER NOT NULL,
      source_key TEXT NOT NULL,
      target_model TEXT NOT NULL,
      target_model_version INTEGER NOT NULL,
      target_key TEXT NOT NULL,
      PRIMARY KEY (source_schema_version, source_schema_hash, source_model, source_model_version, source_key)
    )`,
    `CREATE INDEX effect_local_client_key_lineage_target
      ON effect_local_client_key_lineage (target_model, target_model_version, target_key)`
  ]
})

const clientV3 = makeMigration({
  id: 3,
  name: "schema-key-lineage-groups",
  statements: [
    `CREATE TABLE effect_local_client_key_lineage_groups (
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      source_model TEXT NOT NULL,
      source_model_version INTEGER NOT NULL,
      source_key TEXT NOT NULL,
      lineage_id TEXT NOT NULL,
      PRIMARY KEY (source_schema_version, source_schema_hash, source_model, source_model_version, source_key)
    )`,
    `CREATE TABLE effect_local_client_key_lineage_targets (
      target_model TEXT NOT NULL,
      target_model_version INTEGER NOT NULL,
      target_key TEXT NOT NULL,
      lineage_id TEXT NOT NULL,
      PRIMARY KEY (target_model, target_model_version, target_key)
    )`
  ]
})

const clientV4 = makeMigration({
  id: 4,
  name: "schema-evolution-staging",
  statements: [
    "ALTER TABLE effect_local_client_evolution ADD COLUMN cursor_sequence INTEGER",
    "ALTER TABLE effect_local_receipts ADD COLUMN mutation_name TEXT",
    "ALTER TABLE effect_local_client_shadow_receipts ADD COLUMN mutation_name TEXT",
    `CREATE TABLE effect_local_client_shadow_visible_entities (
      generation INTEGER NOT NULL,
      model TEXT NOT NULL,
      model_version INTEGER NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      PRIMARY KEY (generation, model, entity_key)
    )`,
    `CREATE TABLE effect_local_client_shadow_pending (
      generation INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      basis INTEGER NOT NULL,
      name TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      digest TEXT NOT NULL,
      digest_version INTEGER NOT NULL CHECK (digest_version IN (1, 2)),
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      mutation_version INTEGER NOT NULL,
      optimistic_result_json TEXT NOT NULL,
      changes_json TEXT NOT NULL,
      PRIMARY KEY (generation, mutation_id),
      UNIQUE (generation, local_sequence)
    )`
  ]
})

const clientV5 = makeMigration({
  id: 5,
  name: "opaque-legacy-receipts",
  statements: [
    `CREATE TABLE effect_local_client_shadow_receipts_v2 (
      generation INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      receipt_json TEXT NOT NULL,
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      mutation_version INTEGER,
      mutation_name TEXT,
      rejection_origin TEXT,
      PRIMARY KEY (generation, mutation_id),
      UNIQUE (generation, local_sequence)
    )`
  ]
})

const clientV6 = makeMigration({
  id: 6,
  name: "bounded-history-and-snapshots",
  statements: [
    "ALTER TABLE effect_local_client_meta ADD COLUMN installed_snapshot_id TEXT",
    "ALTER TABLE effect_local_client_meta ADD COLUMN installed_snapshot_sequence INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_client_meta ADD COLUMN installed_snapshot_terminal_sequence INTEGER NOT NULL DEFAULT 0"
  ]
})

const serverV1 = makeMigration({
  id: 1,
  name: "mutation-log",
  statements: [
    `CREATE TABLE IF NOT EXISTS effect_local_server_spaces (
      space_id TEXT PRIMARY KEY,
      definition_hash TEXT NOT NULL,
      next_server_sequence INTEGER NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS effect_local_server_clients (
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      last_local_sequence INTEGER NOT NULL,
      PRIMARY KEY (space_id, client_id)
    )`,
    `CREATE TABLE IF NOT EXISTS effect_local_server_receipts (
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      digest TEXT NOT NULL,
      receipt_json TEXT NOT NULL,
      PRIMARY KEY (space_id, client_id, local_sequence),
      UNIQUE (space_id, mutation_id)
    )`,
    `CREATE TABLE IF NOT EXISTS effect_local_authoritative_log (
      space_id TEXT NOT NULL,
      server_sequence INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      entry_bytes INTEGER NOT NULL CHECK (entry_bytes > 0),
      entry_json TEXT NOT NULL,
      PRIMARY KEY (space_id, server_sequence),
      UNIQUE (space_id, mutation_id)
    )`,
    `CREATE TABLE IF NOT EXISTS effect_local_server_entities (
      space_id TEXT NOT NULL,
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      PRIMARY KEY (space_id, model, entity_key)
    )`
  ]
})

const serverV2 = makeMigration({
  id: 2,
  name: "schema-evolution",
  statements: [
    "ALTER TABLE effect_local_server_spaces ADD COLUMN schema_version INTEGER",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN schema_hash TEXT",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN schema_generation INTEGER NOT NULL DEFAULT 0 CHECK (schema_generation >= 0)",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN target_schema_version INTEGER",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN target_schema_hash TEXT",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN migration_hash TEXT",
    "ALTER TABLE effect_local_server_receipts ADD COLUMN digest_version INTEGER NOT NULL DEFAULT 1 CHECK (digest_version IN (1, 2))",
    "ALTER TABLE effect_local_server_receipts ADD COLUMN source_schema_version INTEGER",
    "ALTER TABLE effect_local_server_receipts ADD COLUMN source_schema_hash TEXT",
    "ALTER TABLE effect_local_server_receipts ADD COLUMN mutation_version INTEGER",
    "ALTER TABLE effect_local_server_receipts ADD COLUMN rejection_origin TEXT",
    "ALTER TABLE effect_local_authoritative_log ADD COLUMN source_schema_version INTEGER",
    "ALTER TABLE effect_local_authoritative_log ADD COLUMN source_schema_hash TEXT",
    "ALTER TABLE effect_local_authoritative_log ADD COLUMN mutation_version INTEGER",
    "ALTER TABLE effect_local_server_entities ADD COLUMN model_version INTEGER",
    `CREATE TABLE effect_local_server_evolution (
      space_id TEXT PRIMARY KEY,
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      target_schema_version INTEGER NOT NULL,
      target_schema_hash TEXT NOT NULL,
      migration_hash TEXT NOT NULL,
      generation INTEGER NOT NULL CHECK (generation > 0),
      phase TEXT NOT NULL,
      cursor_model TEXT,
      cursor_key TEXT
    )`,
    `CREATE TABLE effect_local_server_shadow_entities (
      space_id TEXT NOT NULL,
      generation INTEGER NOT NULL,
      model TEXT NOT NULL,
      model_version INTEGER NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      PRIMARY KEY (space_id, generation, model, entity_key)
    )`,
    `CREATE TABLE effect_local_server_key_lineage (
      space_id TEXT NOT NULL,
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      source_model TEXT NOT NULL,
      source_model_version INTEGER NOT NULL,
      source_key TEXT NOT NULL,
      target_model TEXT NOT NULL,
      target_model_version INTEGER NOT NULL,
      target_key TEXT NOT NULL,
      PRIMARY KEY (
        space_id, source_schema_version, source_schema_hash, source_model, source_model_version, source_key
      )
    )`,
    `CREATE INDEX effect_local_server_key_lineage_target
      ON effect_local_server_key_lineage (space_id, target_model, target_model_version, target_key)`
  ]
})

const serverV3 = makeMigration({
  id: 3,
  name: "schema-key-lineage-groups",
  statements: [
    `CREATE TABLE effect_local_server_key_lineage_groups (
      space_id TEXT NOT NULL,
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      source_model TEXT NOT NULL,
      source_model_version INTEGER NOT NULL,
      source_key TEXT NOT NULL,
      lineage_id TEXT NOT NULL,
      PRIMARY KEY (
        space_id, source_schema_version, source_schema_hash, source_model, source_model_version, source_key
      )
    )`,
    `CREATE TABLE effect_local_server_key_lineage_targets (
      space_id TEXT NOT NULL,
      target_model TEXT NOT NULL,
      target_model_version INTEGER NOT NULL,
      target_key TEXT NOT NULL,
      lineage_id TEXT NOT NULL,
      PRIMARY KEY (space_id, target_model, target_model_version, target_key)
    )`
  ]
})

const serverV4 = makeMigration({
  id: 4,
  name: "schema-evolution-staging",
  statements: [
    "ALTER TABLE effect_local_server_evolution ADD COLUMN cursor_sequence INTEGER",
    "ALTER TABLE effect_local_server_receipts ADD COLUMN mutation_name TEXT"
  ]
})

const serverV5 = makeMigration({
  id: 5,
  name: "bounded-history-and-snapshots",
  statements: [
    "ALTER TABLE effect_local_server_spaces ADD COLUMN next_terminal_sequence INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN history_floor INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN receipt_floor INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN retained_history_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN retained_receipt_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN entity_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN entity_bytes INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN snapshot_id TEXT",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN snapshot_sequence INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN snapshot_terminal_sequence INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN metadata_verified INTEGER NOT NULL DEFAULT 0 CHECK (metadata_verified IN (0, 1))",
    "ALTER TABLE effect_local_server_clients ADD COLUMN expired_local_sequence INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_server_receipts ADD COLUMN terminal_sequence INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_server_receipts ADD COLUMN server_sequence INTEGER",
    "ALTER TABLE effect_local_authoritative_log ADD COLUMN client_id TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE effect_local_authoritative_log ADD COLUMN local_sequence INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE effect_local_authoritative_log ADD COLUMN digest TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE effect_local_server_entities ADD COLUMN entity_bytes INTEGER NOT NULL DEFAULT 0",
    `UPDATE effect_local_authoritative_log SET
      client_id = COALESCE(json_extract(entry_json, '$.clientId'), ''),
      local_sequence = COALESCE(json_extract(entry_json, '$.localSequence'), 0),
      digest = COALESCE(json_extract(entry_json, '$.digest'), '')`,
    `WITH ranked AS (
      SELECT rowid AS receipt_rowid,
        ROW_NUMBER() OVER (PARTITION BY space_id ORDER BY rowid) AS terminal_sequence
      FROM effect_local_server_receipts
    )
    UPDATE effect_local_server_receipts SET
      terminal_sequence = (
        SELECT ranked.terminal_sequence FROM ranked
        WHERE ranked.receipt_rowid = effect_local_server_receipts.rowid
      ),
      server_sequence = CASE
        WHEN json_extract(receipt_json, '$._tag') = 'Accepted'
        THEN json_extract(receipt_json, '$.serverSequence')
        ELSE NULL
      END`,
    `UPDATE effect_local_server_receipts SET receipt_json =
      json_set(receipt_json, '$.terminalSequence', terminal_sequence)
      WHERE json_extract(receipt_json, '$._tag') IN ('Accepted', 'Rejected')`,
    `UPDATE effect_local_server_spaces SET
      next_terminal_sequence = COALESCE((
        SELECT MAX(r.terminal_sequence) + 1 FROM effect_local_server_receipts AS r
        WHERE r.space_id = effect_local_server_spaces.space_id
      ), 1),
      retained_history_count = (
        SELECT COUNT(*) FROM effect_local_authoritative_log AS l
        WHERE l.space_id = effect_local_server_spaces.space_id
      ),
      retained_receipt_count = (
        SELECT COUNT(*) FROM effect_local_server_receipts AS r
        WHERE r.space_id = effect_local_server_spaces.space_id
      )`,
    `CREATE TABLE effect_local_server_space_counts (
      space_id TEXT PRIMARY KEY,
      history_count INTEGER NOT NULL CHECK (history_count >= 0),
      receipt_count INTEGER NOT NULL CHECK (receipt_count >= 0)
    )`,
    `CREATE INDEX effect_local_server_space_counts_history
      ON effect_local_server_space_counts (history_count DESC)`,
    `CREATE INDEX effect_local_server_space_counts_receipts
      ON effect_local_server_space_counts (receipt_count DESC)`,
    `INSERT INTO effect_local_server_space_counts (space_id, history_count, receipt_count)
      SELECT space_id, retained_history_count, retained_receipt_count
      FROM effect_local_server_spaces`,
    `CREATE TABLE effect_local_server_snapshots (
      space_id TEXT NOT NULL,
      snapshot_id TEXT NOT NULL,
      definition_hash TEXT NOT NULL,
      schema_version INTEGER NOT NULL,
      schema_hash TEXT NOT NULL,
      server_sequence INTEGER NOT NULL,
      terminal_sequence INTEGER NOT NULL,
      entity_count INTEGER NOT NULL,
      content_bytes INTEGER NOT NULL,
      digest TEXT NOT NULL,
      PRIMARY KEY (space_id, snapshot_id),
      UNIQUE (space_id, server_sequence, terminal_sequence)
    )`,
    `CREATE TABLE effect_local_server_snapshot_entities (
      space_id TEXT NOT NULL,
      snapshot_id TEXT NOT NULL,
      ordinal INTEGER NOT NULL,
      model TEXT NOT NULL,
      model_version INTEGER NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      entity_bytes INTEGER NOT NULL,
      wire_json TEXT NOT NULL,
      wire_bytes INTEGER NOT NULL CHECK (wire_bytes > 0),
      PRIMARY KEY (space_id, snapshot_id, ordinal),
      UNIQUE (space_id, snapshot_id, model, entity_key)
    )`,
    `CREATE INDEX effect_local_server_history_terminal
      ON effect_local_authoritative_log (space_id, server_sequence, mutation_id)`,
    `CREATE INDEX effect_local_server_receipts_terminal
      ON effect_local_server_receipts (space_id, terminal_sequence, client_id, local_sequence)`,
    `CREATE INDEX effect_local_server_snapshots_latest
      ON effect_local_server_snapshots (space_id, server_sequence DESC, terminal_sequence DESC)`,
    `CREATE INDEX effect_local_server_entities_largest
      ON effect_local_server_entities (space_id, entity_bytes DESC, model, entity_key)`,
    `CREATE TRIGGER effect_local_count_history_insert AFTER INSERT ON effect_local_authoritative_log
      BEGIN
        UPDATE effect_local_server_spaces SET retained_history_count = retained_history_count + 1
          WHERE space_id = NEW.space_id;
        UPDATE effect_local_server_space_counts SET history_count = history_count + 1
          WHERE space_id = NEW.space_id;
      END`,
    `CREATE TRIGGER effect_local_count_history_delete AFTER DELETE ON effect_local_authoritative_log
      BEGIN
        UPDATE effect_local_server_spaces SET retained_history_count = retained_history_count - 1
          WHERE space_id = OLD.space_id;
        UPDATE effect_local_server_space_counts SET history_count = history_count - 1
          WHERE space_id = OLD.space_id;
      END`,
    `CREATE TRIGGER effect_local_count_receipt_insert AFTER INSERT ON effect_local_server_receipts
      BEGIN
        UPDATE effect_local_server_spaces SET retained_receipt_count = retained_receipt_count + 1
          WHERE space_id = NEW.space_id;
        UPDATE effect_local_server_space_counts SET receipt_count = receipt_count + 1
          WHERE space_id = NEW.space_id;
      END`,
    `CREATE TRIGGER effect_local_count_receipt_delete AFTER DELETE ON effect_local_server_receipts
      BEGIN
        UPDATE effect_local_server_spaces SET retained_receipt_count = retained_receipt_count - 1
          WHERE space_id = OLD.space_id;
        UPDATE effect_local_server_space_counts SET receipt_count = receipt_count - 1
          WHERE space_id = OLD.space_id;
      END`,
    `UPDATE effect_local_server_spaces SET metadata_verified = 1 WHERE NOT EXISTS (
      SELECT 1 FROM effect_local_server_entities AS e
      WHERE e.space_id = effect_local_server_spaces.space_id
    )`,
    `CREATE TRIGGER effect_local_require_current_space_writer
      BEFORE INSERT ON effect_local_server_spaces
      WHEN NEW.metadata_verified = 0
      BEGIN SELECT RAISE(ABORT, 'effect-local server writer upgrade required'); END`,
    `CREATE TRIGGER effect_local_require_current_receipt_writer
      BEFORE INSERT ON effect_local_server_receipts
      WHEN NEW.terminal_sequence = 0
      BEGIN SELECT RAISE(ABORT, 'effect-local server writer upgrade required'); END`,
    `CREATE TRIGGER effect_local_require_current_history_writer
      BEFORE INSERT ON effect_local_authoritative_log
      WHEN NEW.client_id = '' OR NEW.local_sequence = 0 OR NEW.digest = ''
      BEGIN SELECT RAISE(ABORT, 'effect-local server writer upgrade required'); END`
  ],
  effect: {
    id: "validate-bounded-history-backfill",
    run: Effect.fnUntraced(
      function*(sql) {
        const row = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: CountRow,
          execute: () =>
            sql`SELECT
            (SELECT COUNT(*) FROM effect_local_authoritative_log
              WHERE json_valid(entry_json) = 0 OR client_id = '' OR local_sequence = 0 OR digest = '') +
            (SELECT COUNT(*) FROM effect_local_server_receipts AS r
              WHERE json_valid(r.receipt_json) = 0 OR r.terminal_sequence <= 0 OR
                (json_extract(r.receipt_json, '$._tag') = 'Accepted' AND (
                  r.server_sequence IS NULL OR NOT EXISTS (
                    SELECT 1 FROM effect_local_authoritative_log AS l
                    WHERE l.space_id = r.space_id AND l.mutation_id = r.mutation_id AND
                      l.server_sequence = r.server_sequence
                  )
                )) OR
                (json_extract(r.receipt_json, '$._tag') <> 'Accepted' AND r.server_sequence IS NOT NULL)
            ) AS count`
        })(undefined)
        if (row.count !== 0) {
          return yield* new ReplicaError.StorageCorrupt({
            message: "Legacy server history contains invalid mutation identity"
          })
        }
        return yield* Effect.void
      },
      Effect.mapError((cause) => {
        if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
        return new ReplicaError.StorageCorrupt({ message: "Server history backfill validation failed", cause })
      })
    )
  }
})

const clientV7 = makeMigration({
  id: 7,
  name: "generation-owned-storage",
  statements: [
    "ALTER TABLE effect_local_client_meta ADD COLUMN active_schema_generation INTEGER NOT NULL DEFAULT 0 CHECK (active_schema_generation >= 0)",
    "ALTER TABLE effect_local_client_evolution ADD COLUMN source_generation INTEGER NOT NULL DEFAULT 0 CHECK (source_generation >= 0)",
    `UPDATE effect_local_client_meta SET active_schema_generation = CASE
      WHEN EXISTS (SELECT 1 FROM effect_local_client_evolution WHERE singleton = 1)
      THEN MAX(schema_generation - 1, 0) ELSE schema_generation END`,
    `UPDATE effect_local_client_evolution SET source_generation =
      (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1),
      phase = 'Log', cursor_model = NULL, cursor_key = NULL, cursor_sequence = 0`,
    `CREATE TABLE effect_local_client_pending_data (
      generation INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      basis INTEGER NOT NULL,
      name TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      digest TEXT NOT NULL,
      digest_version INTEGER NOT NULL CHECK (digest_version IN (1, 2)),
      source_schema_version INTEGER,
      source_schema_hash TEXT,
      mutation_version INTEGER,
      optimistic_result_json TEXT NOT NULL,
      changes_json TEXT NOT NULL,
      PRIMARY KEY (generation, mutation_id),
      UNIQUE (generation, local_sequence)
    )`,
    `INSERT INTO effect_local_client_pending_data
      SELECT (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1),
        mutation_id, local_sequence, basis, name, payload_json, digest, digest_version,
        source_schema_version, source_schema_hash, mutation_version, optimistic_result_json, changes_json
      FROM effect_local_pending`,
    `CREATE TABLE effect_local_client_receipts_data (
      generation INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      receipt_json TEXT NOT NULL,
      source_schema_version INTEGER,
      source_schema_hash TEXT,
      mutation_version INTEGER,
      rejection_origin TEXT,
      mutation_name TEXT,
      PRIMARY KEY (generation, mutation_id),
      UNIQUE (generation, local_sequence)
    )`,
    `INSERT INTO effect_local_client_receipts_data
      SELECT (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1),
        mutation_id, local_sequence, receipt_json, source_schema_version, source_schema_hash,
        mutation_version, rejection_origin, mutation_name FROM effect_local_receipts`,
    `CREATE TABLE effect_local_client_canonical_entities_data (
      generation INTEGER NOT NULL,
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      model_version INTEGER,
      PRIMARY KEY (generation, model, entity_key)
    )`,
    `INSERT INTO effect_local_client_canonical_entities_data
      SELECT (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1),
        model, entity_key, value_json, model_version FROM effect_local_canonical_entities`,
    `CREATE TABLE effect_local_client_visible_entities_data (
      generation INTEGER NOT NULL,
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      model_version INTEGER,
      PRIMARY KEY (generation, model, entity_key)
    )`,
    `INSERT INTO effect_local_client_visible_entities_data
      SELECT (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1),
        model, entity_key, value_json, model_version FROM effect_local_visible_entities`,
    "DROP TABLE effect_local_pending",
    "DROP TABLE effect_local_receipts",
    "DROP TABLE effect_local_canonical_entities",
    "DROP TABLE effect_local_visible_entities",
    "DELETE FROM effect_local_client_shadow_entities",
    "DELETE FROM effect_local_client_shadow_visible_entities",
    "DELETE FROM effect_local_client_shadow_receipts_v2",
    "DELETE FROM effect_local_client_shadow_pending",
    `CREATE VIEW effect_local_pending AS SELECT mutation_id, local_sequence, basis, name, payload_json,
      digest, digest_version, source_schema_version, source_schema_hash, mutation_version,
      optimistic_result_json, changes_json FROM effect_local_client_pending_data
      WHERE generation = (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1)`,
    `CREATE VIEW effect_local_receipts AS SELECT mutation_id, local_sequence, receipt_json,
      source_schema_version, source_schema_hash, mutation_version, rejection_origin, mutation_name
      FROM effect_local_client_receipts_data
      WHERE generation = (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1)`,
    `CREATE VIEW effect_local_canonical_entities AS SELECT model, entity_key, value_json, model_version
      FROM effect_local_client_canonical_entities_data
      WHERE generation = (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1)`,
    `CREATE VIEW effect_local_visible_entities AS SELECT model, entity_key, value_json, model_version
      FROM effect_local_client_visible_entities_data
      WHERE generation = (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1)`,
    `CREATE TRIGGER effect_local_pending_insert INSTEAD OF INSERT ON effect_local_pending BEGIN
      INSERT INTO effect_local_client_pending_data
        (generation, mutation_id, local_sequence, basis, name, payload_json, digest, digest_version,
          source_schema_version, source_schema_hash, mutation_version, optimistic_result_json, changes_json)
      VALUES ((SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1),
        NEW.mutation_id, NEW.local_sequence, NEW.basis, NEW.name, NEW.payload_json, NEW.digest,
        NEW.digest_version, NEW.source_schema_version, NEW.source_schema_hash, NEW.mutation_version,
        NEW.optimistic_result_json, NEW.changes_json); END`,
    `CREATE TRIGGER effect_local_pending_update INSTEAD OF UPDATE ON effect_local_pending BEGIN
      UPDATE effect_local_client_pending_data SET mutation_id = NEW.mutation_id, local_sequence = NEW.local_sequence,
        basis = NEW.basis, name = NEW.name, payload_json = NEW.payload_json, digest = NEW.digest,
        digest_version = NEW.digest_version, source_schema_version = NEW.source_schema_version,
        source_schema_hash = NEW.source_schema_hash, mutation_version = NEW.mutation_version,
        optimistic_result_json = NEW.optimistic_result_json, changes_json = NEW.changes_json
      WHERE generation = (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1)
        AND mutation_id = OLD.mutation_id; END`,
    `CREATE TRIGGER effect_local_pending_delete INSTEAD OF DELETE ON effect_local_pending BEGIN
      DELETE FROM effect_local_client_pending_data
      WHERE generation = (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1)
        AND mutation_id = OLD.mutation_id; END`,
    `CREATE TRIGGER effect_local_receipts_insert INSTEAD OF INSERT ON effect_local_receipts BEGIN
      INSERT INTO effect_local_client_receipts_data
        (generation, mutation_id, local_sequence, receipt_json, source_schema_version, source_schema_hash,
          mutation_version, rejection_origin, mutation_name)
      VALUES ((SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1),
        NEW.mutation_id, NEW.local_sequence, NEW.receipt_json, NEW.source_schema_version, NEW.source_schema_hash,
        NEW.mutation_version, NEW.rejection_origin, NEW.mutation_name); END`,
    `CREATE TRIGGER effect_local_receipts_update INSTEAD OF UPDATE ON effect_local_receipts BEGIN
      UPDATE effect_local_client_receipts_data SET mutation_id = NEW.mutation_id, local_sequence = NEW.local_sequence,
        receipt_json = NEW.receipt_json, source_schema_version = NEW.source_schema_version,
        source_schema_hash = NEW.source_schema_hash, mutation_version = NEW.mutation_version,
        rejection_origin = NEW.rejection_origin, mutation_name = NEW.mutation_name
      WHERE generation = (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1)
        AND mutation_id = OLD.mutation_id; END`,
    `CREATE TRIGGER effect_local_receipts_delete INSTEAD OF DELETE ON effect_local_receipts BEGIN
      DELETE FROM effect_local_client_receipts_data
      WHERE generation = (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1)
        AND mutation_id = OLD.mutation_id; END`,
    ...["canonical", "visible"].flatMap((kind) => [
      `CREATE TRIGGER effect_local_${kind}_entities_insert INSTEAD OF INSERT ON effect_local_${kind}_entities BEGIN
        INSERT INTO effect_local_client_${kind}_entities_data
          (generation, model, entity_key, value_json, model_version)
        VALUES ((SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1),
          NEW.model, NEW.entity_key, NEW.value_json, NEW.model_version); END`,
      `CREATE TRIGGER effect_local_${kind}_entities_update INSTEAD OF UPDATE ON effect_local_${kind}_entities BEGIN
        UPDATE effect_local_client_${kind}_entities_data SET model = NEW.model, entity_key = NEW.entity_key,
          value_json = NEW.value_json, model_version = NEW.model_version
        WHERE generation = (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1)
          AND model = OLD.model AND entity_key = OLD.entity_key; END`,
      `CREATE TRIGGER effect_local_${kind}_entities_delete INSTEAD OF DELETE ON effect_local_${kind}_entities BEGIN
        DELETE FROM effect_local_client_${kind}_entities_data
        WHERE generation = (SELECT active_schema_generation FROM effect_local_client_meta WHERE singleton = 1)
          AND model = OLD.model AND entity_key = OLD.entity_key; END`
    ])
  ]
})

const clientV8 = makeMigration({
  id: 8,
  name: "multi-space-client-storage",
  statements: [
    "DROP TRIGGER effect_local_pending_insert",
    "DROP TRIGGER effect_local_pending_update",
    "DROP TRIGGER effect_local_pending_delete",
    "DROP TRIGGER effect_local_receipts_insert",
    "DROP TRIGGER effect_local_receipts_update",
    "DROP TRIGGER effect_local_receipts_delete",
    "DROP TRIGGER effect_local_canonical_entities_insert",
    "DROP TRIGGER effect_local_canonical_entities_update",
    "DROP TRIGGER effect_local_canonical_entities_delete",
    "DROP TRIGGER effect_local_visible_entities_insert",
    "DROP TRIGGER effect_local_visible_entities_update",
    "DROP TRIGGER effect_local_visible_entities_delete",
    "DROP VIEW effect_local_pending",
    "DROP VIEW effect_local_receipts",
    "DROP VIEW effect_local_canonical_entities",
    "DROP VIEW effect_local_visible_entities",
    "ALTER TABLE effect_local_client_meta RENAME TO effect_local_client_meta_v7",
    `CREATE TABLE effect_local_client_meta (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      client_id TEXT NOT NULL UNIQUE
    )`,
    `CREATE TABLE effect_local_client_spaces (
      space_id TEXT PRIMARY KEY,
      membership_incarnation TEXT NOT NULL,
      definition_hash TEXT NOT NULL,
      schema_version INTEGER,
      schema_hash TEXT,
      schema_generation INTEGER NOT NULL CHECK (schema_generation >= 0),
      active_schema_generation INTEGER NOT NULL CHECK (active_schema_generation >= 0),
      active_projection_generation INTEGER NOT NULL DEFAULT 0 CHECK (active_projection_generation >= 0),
      projection_schema_generation INTEGER NOT NULL CHECK (projection_schema_generation >= 0),
      target_schema_version INTEGER,
      target_schema_hash TEXT,
      migration_hash TEXT,
      next_local_sequence INTEGER NOT NULL CHECK (next_local_sequence > 0),
      server_cursor INTEGER NOT NULL CHECK (server_cursor >= 0),
      visible_revision INTEGER NOT NULL CHECK (visible_revision >= 0),
      requested_generation INTEGER NOT NULL CHECK (requested_generation >= 0),
      completed_generation INTEGER NOT NULL CHECK (
        completed_generation >= 0 AND completed_generation <= requested_generation
      ),
      installed_snapshot_id TEXT,
      installed_snapshot_sequence INTEGER NOT NULL CHECK (installed_snapshot_sequence >= 0),
      installed_snapshot_terminal_sequence INTEGER NOT NULL CHECK (installed_snapshot_terminal_sequence >= 0),
      replication_view_id TEXT,
      replication_view_revision INTEGER NOT NULL DEFAULT 0 CHECK (replication_view_revision >= 0),
      desired_scope_json TEXT NOT NULL DEFAULT '{"models":[]}' CHECK (json_valid(desired_scope_json)),
      desired_scope_digest TEXT NOT NULL
        DEFAULT '0000000000000000000000000000000000000000000000000000000000000000'
        CHECK (length(desired_scope_digest) = 64),
      scope_generation INTEGER NOT NULL DEFAULT 0 CHECK (scope_generation >= 0),
      projection_replay_generation INTEGER,
      projection_replay_cursor TEXT
    )`,
    "ALTER TABLE effect_local_server_log RENAME TO effect_local_server_log_v7",
    `CREATE TABLE effect_local_server_log (
      space_id TEXT NOT NULL,
      membership_incarnation TEXT NOT NULL,
      server_sequence INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      entry_json TEXT NOT NULL,
      source_schema_version INTEGER,
      source_schema_hash TEXT,
      mutation_version INTEGER,
      PRIMARY KEY (space_id, server_sequence),
      UNIQUE (space_id, mutation_id),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    "ALTER TABLE effect_local_client_pending_data RENAME TO effect_local_client_pending_data_v7",
    `CREATE TABLE effect_local_client_pending_data (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL,
      membership_incarnation TEXT NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      basis INTEGER NOT NULL,
      name TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      digest TEXT NOT NULL,
      digest_version INTEGER NOT NULL CHECK (digest_version = 3),
      source_schema_version INTEGER,
      source_schema_hash TEXT,
      mutation_version INTEGER,
      optimistic_result_json TEXT NOT NULL,
      changes_json TEXT NOT NULL,
      submission_state TEXT NOT NULL DEFAULT 'Queued' CHECK (
        submission_state IN ('Queued', 'Submitting', 'Retrying', 'Submitted', 'AwaitingReceipt')
      ),
      attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
      PRIMARY KEY (space_id, schema_generation, mutation_id),
      UNIQUE (space_id, schema_generation, local_sequence),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    "ALTER TABLE effect_local_client_receipts_data RENAME TO effect_local_client_receipts_data_v7",
    `CREATE TABLE effect_local_client_receipts_data (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL,
      membership_incarnation TEXT NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      receipt_json TEXT NOT NULL,
      source_schema_version INTEGER,
      source_schema_hash TEXT,
      mutation_version INTEGER,
      rejection_origin TEXT,
      mutation_name TEXT,
      PRIMARY KEY (space_id, schema_generation, mutation_id),
      UNIQUE (space_id, schema_generation, local_sequence),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    "ALTER TABLE effect_local_client_canonical_entities_data RENAME TO effect_local_client_canonical_entities_data_v7",
    `CREATE TABLE effect_local_client_canonical_entities_data (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL,
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      model_version INTEGER,
      PRIMARY KEY (space_id, schema_generation, model, entity_key),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    "ALTER TABLE effect_local_client_visible_entities_data RENAME TO effect_local_client_visible_entities_data_v7",
    `CREATE TABLE effect_local_client_visible_entities_data (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL,
      projection_generation INTEGER NOT NULL,
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      model_version INTEGER,
      PRIMARY KEY (space_id, schema_generation, projection_generation, model, entity_key),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    "ALTER TABLE effect_local_client_evolution RENAME TO effect_local_client_evolution_v7",
    `CREATE TABLE effect_local_client_evolution (
      space_id TEXT PRIMARY KEY,
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      target_schema_version INTEGER NOT NULL,
      target_schema_hash TEXT NOT NULL,
      migration_hash TEXT NOT NULL,
      generation INTEGER NOT NULL CHECK (generation > 0),
      source_generation INTEGER NOT NULL CHECK (source_generation >= 0),
      source_projection_generation INTEGER NOT NULL DEFAULT 0 CHECK (source_projection_generation >= 0),
      target_projection_generation INTEGER NOT NULL DEFAULT 0 CHECK (target_projection_generation >= 0),
      phase TEXT NOT NULL,
      cursor_model TEXT,
      cursor_key TEXT,
      cursor_sequence INTEGER,
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    "ALTER TABLE effect_local_client_key_lineage RENAME TO effect_local_client_key_lineage_v7",
    `CREATE TABLE effect_local_client_key_lineage (
      space_id TEXT NOT NULL,
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      source_model TEXT NOT NULL,
      source_model_version INTEGER NOT NULL,
      source_key TEXT NOT NULL,
      target_model TEXT NOT NULL,
      target_model_version INTEGER NOT NULL,
      target_key TEXT NOT NULL,
      PRIMARY KEY (space_id, source_schema_version, source_schema_hash, source_model, source_model_version, source_key),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    "ALTER TABLE effect_local_client_key_lineage_groups RENAME TO effect_local_client_key_lineage_groups_v7",
    `CREATE TABLE effect_local_client_key_lineage_groups (
      space_id TEXT NOT NULL,
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      source_model TEXT NOT NULL,
      source_model_version INTEGER NOT NULL,
      source_key TEXT NOT NULL,
      lineage_id TEXT NOT NULL,
      PRIMARY KEY (space_id, source_schema_version, source_schema_hash, source_model, source_model_version, source_key),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    "ALTER TABLE effect_local_client_key_lineage_targets RENAME TO effect_local_client_key_lineage_targets_v7",
    `CREATE TABLE effect_local_client_key_lineage_targets (
      space_id TEXT NOT NULL,
      target_model TEXT NOT NULL,
      target_model_version INTEGER NOT NULL,
      target_key TEXT NOT NULL,
      lineage_id TEXT NOT NULL,
      PRIMARY KEY (space_id, target_model, target_model_version, target_key),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    `CREATE TABLE effect_local_client_retractions (
      space_id TEXT NOT NULL,
      generation INTEGER NOT NULL CHECK (generation >= 0),
      model TEXT NOT NULL,
      model_version INTEGER NOT NULL CHECK (model_version > 0),
      entity_key TEXT NOT NULL,
      PRIMARY KEY (space_id, generation, model, entity_key),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    `CREATE TABLE effect_local_client_scoped_bootstrap (
      snapshot_id TEXT NOT NULL,
      space_id TEXT PRIMARY KEY,
      client_id TEXT NOT NULL,
      definition_hash TEXT NOT NULL,
      schema_version INTEGER NOT NULL,
      schema_hash TEXT NOT NULL,
      scope_digest TEXT NOT NULL CHECK (length(scope_digest) = 64),
      scope_generation INTEGER NOT NULL CHECK (scope_generation >= 0),
      view_id TEXT NOT NULL,
      view_revision INTEGER NOT NULL CHECK (view_revision >= 0),
      server_sequence INTEGER NOT NULL,
      terminal_sequence INTEGER NOT NULL,
      entry_count INTEGER NOT NULL,
      content_bytes INTEGER NOT NULL,
      digest TEXT NOT NULL,
      next_ordinal INTEGER NOT NULL,
      received_bytes INTEGER NOT NULL,
      rolling_digest TEXT NOT NULL,
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    `CREATE TABLE effect_local_client_scoped_bootstrap_entries (
      space_id TEXT NOT NULL,
      snapshot_id TEXT NOT NULL,
      ordinal INTEGER NOT NULL,
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      change_json TEXT NOT NULL CHECK (json_valid(change_json)),
      entry_bytes INTEGER NOT NULL CHECK (entry_bytes > 0),
      PRIMARY KEY (space_id, ordinal),
      UNIQUE (space_id, model, entity_key),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_scoped_bootstrap(space_id) ON DELETE CASCADE
    )`,
    "DROP TABLE effect_local_client_shadow_entities",
    "DROP TABLE effect_local_client_shadow_receipts",
    "DROP TABLE effect_local_client_shadow_visible_entities",
    "DROP TABLE effect_local_client_shadow_pending",
    "DROP TABLE effect_local_client_shadow_receipts_v2",
    "DROP TABLE effect_local_client_key_lineage_targets_v7",
    "DROP TABLE effect_local_client_key_lineage_groups_v7",
    "DROP TABLE effect_local_client_key_lineage_v7",
    "DROP TABLE effect_local_client_evolution_v7",
    "DROP TABLE effect_local_client_visible_entities_data_v7",
    "DROP TABLE effect_local_client_canonical_entities_data_v7",
    "DROP TABLE effect_local_client_receipts_data_v7",
    "DROP TABLE effect_local_client_pending_data_v7",
    "DROP TABLE effect_local_server_log_v7",
    "DROP TABLE effect_local_client_meta_v7"
  ],
  effect: {
    id: "validate-multi-space-client-storage",
    run: (sql) =>
      SqlSchema.findAll({
        Request: Schema.Void,
        Result: ForeignKeyCheckRow,
        execute: () => sql`PRAGMA foreign_key_check`
      })(undefined).pipe(
        Effect.flatMap((rows) => {
          if (rows.length === 0) return Effect.void
          return new ReplicaError.StorageCorrupt({
            message: `Client migration left ${rows.length} foreign key violation(s)`
          })
        }),
        Effect.mapError((cause) => {
          if (cause._tag === "StorageCorrupt") return cause
          return StorageUnavailable.make(cause)
        })
      )
  }
})

const clientV9 = makeMigration({
  id: 9,
  name: "pending-mutation-quarantine",
  statements: [
    `CREATE TABLE effect_local_client_quarantine (
      space_id TEXT NOT NULL,
      membership_incarnation TEXT NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      basis INTEGER NOT NULL,
      name TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      digest TEXT NOT NULL,
      digest_version INTEGER NOT NULL CHECK (digest_version = 3),
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      mutation_version INTEGER NOT NULL,
      rejection_json TEXT NOT NULL,
      target_schema_version INTEGER NOT NULL,
      target_schema_hash TEXT NOT NULL,
      PRIMARY KEY (space_id, mutation_id),
      UNIQUE (space_id, membership_incarnation, local_sequence),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`
  ]
})

const clientV10 = makeMigration({
  id: 10,
  name: "quarantine-resubmission-intents",
  statements: [
    `CREATE TABLE effect_local_client_quarantine_resubmissions (
      space_id TEXT NOT NULL,
      original_mutation_id TEXT NOT NULL,
      replacement_mutation_id TEXT NOT NULL,
      PRIMARY KEY (space_id, original_mutation_id),
      UNIQUE (space_id, replacement_mutation_id),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`
  ]
})

const clientV11 = makeMigration({
  id: 11,
  name: "quarantine-cancellation-continuations",
  statements: [
    `CREATE TABLE effect_local_client_quarantine_cancellations (
      space_id TEXT NOT NULL,
      root_mutation_id TEXT NOT NULL,
      current_mutation_id TEXT NOT NULL,
      PRIMARY KEY (space_id, root_mutation_id),
      UNIQUE (space_id, current_mutation_id),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`
  ]
})

const clientV12 = makeMigration({
  id: 12,
  name: "secondary-index-catalog",
  statements: [
    `CREATE TABLE effect_local_client_index_catalog (
      model TEXT NOT NULL,
      index_name TEXT NOT NULL,
      descriptor_hash TEXT NOT NULL,
      layout_hash TEXT NOT NULL,
      table_name TEXT NOT NULL UNIQUE,
      table_checksum TEXT NOT NULL,
      scan_index_name TEXT NOT NULL UNIQUE,
      scan_index_checksum TEXT NOT NULL,
      PRIMARY KEY (model, index_name, descriptor_hash)
    )`,
    `CREATE TABLE effect_local_client_index_state (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL CHECK (schema_generation >= 0),
      projection_generation INTEGER NOT NULL CHECK (projection_generation >= 0),
      model TEXT NOT NULL,
      index_name TEXT NOT NULL,
      descriptor_hash TEXT NOT NULL,
      backfill_after_key TEXT,
      backfill_visible_revision INTEGER CHECK (
        backfill_visible_revision IS NULL OR backfill_visible_revision >= 0
      ),
      ready INTEGER NOT NULL DEFAULT 0 CHECK (ready IN (0, 1)),
      PRIMARY KEY (
        space_id, schema_generation, projection_generation, model, index_name, descriptor_hash
      ),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE,
      FOREIGN KEY (model, index_name, descriptor_hash)
        REFERENCES effect_local_client_index_catalog(model, index_name, descriptor_hash) ON DELETE CASCADE
    )`
  ]
})

const clientV13 = makeMigration({
  id: 13,
  name: "projection-dirty-entities",
  statements: [
    `CREATE TABLE effect_local_client_projection_dirty (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL CHECK (schema_generation >= 0),
      model TEXT NOT NULL,
      model_version INTEGER NOT NULL CHECK (model_version > 0),
      entity_key TEXT NOT NULL,
      PRIMARY KEY (space_id, schema_generation, model, entity_key)
    )`
  ]
})

export const clientCatalog = Object.freeze([
  clientV1,
  clientV2,
  clientV3,
  clientV4,
  clientV5,
  clientV6,
  clientV7,
  clientV8,
  clientV9,
  clientV10,
  clientV11,
  clientV12,
  clientV13
])

const serverV6 = makeMigration({
  id: 6,
  name: "legacy-schema-baseline",
  statements: [
    "ALTER TABLE effect_local_server_spaces ADD COLUMN legacy_schema_version INTEGER",
    "ALTER TABLE effect_local_server_spaces ADD COLUMN legacy_schema_hash TEXT"
  ]
})

const serverV7 = makeMigration({
  id: 7,
  name: "generation-owned-storage",
  statements: [
    "ALTER TABLE effect_local_server_spaces ADD COLUMN active_schema_generation INTEGER NOT NULL DEFAULT 0 CHECK (active_schema_generation >= 0)",
    "ALTER TABLE effect_local_server_evolution ADD COLUMN source_generation INTEGER NOT NULL DEFAULT 0 CHECK (source_generation >= 0)",
    "ALTER TABLE effect_local_server_evolution ADD COLUMN target_entity_count INTEGER NOT NULL DEFAULT 0 CHECK (target_entity_count >= 0)",
    "ALTER TABLE effect_local_server_evolution ADD COLUMN target_entity_bytes INTEGER NOT NULL DEFAULT 0 CHECK (target_entity_bytes >= 0)",
    `UPDATE effect_local_server_spaces SET active_schema_generation = CASE
      WHEN EXISTS (SELECT 1 FROM effect_local_server_evolution AS e
        WHERE e.space_id = effect_local_server_spaces.space_id)
      THEN MAX(schema_generation - 1, 0) ELSE schema_generation END`,
    `UPDATE effect_local_server_evolution SET source_generation =
      (SELECT active_schema_generation FROM effect_local_server_spaces AS s
        WHERE s.space_id = effect_local_server_evolution.space_id),
      target_entity_count = 0, target_entity_bytes = 0,
      phase = 'Log', cursor_model = NULL, cursor_key = NULL, cursor_sequence = 0`,
    `CREATE TABLE effect_local_server_entities_data (
      space_id TEXT NOT NULL,
      generation INTEGER NOT NULL,
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      model_version INTEGER,
      entity_bytes INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (space_id, generation, model, entity_key)
    )`,
    `INSERT INTO effect_local_server_entities_data
      SELECT e.space_id, s.active_schema_generation, e.model, e.entity_key, e.value_json,
        e.model_version, e.entity_bytes FROM effect_local_server_entities AS e
      INNER JOIN effect_local_server_spaces AS s ON s.space_id = e.space_id`,
    "DROP TABLE effect_local_server_entities",
    "DELETE FROM effect_local_server_shadow_entities",
    `CREATE INDEX effect_local_server_entities_largest
      ON effect_local_server_entities_data (space_id, generation, entity_bytes DESC, model, entity_key)`,
    `CREATE VIEW effect_local_server_entities AS
      SELECT d.space_id, d.model, d.entity_key, d.value_json, d.model_version, d.entity_bytes
      FROM effect_local_server_entities_data AS d
      INNER JOIN effect_local_server_spaces AS s ON s.space_id = d.space_id
        AND s.active_schema_generation = d.generation`,
    `CREATE TRIGGER effect_local_server_entities_insert INSTEAD OF INSERT ON effect_local_server_entities BEGIN
      INSERT INTO effect_local_server_entities_data
        (space_id, generation, model, entity_key, value_json, model_version, entity_bytes)
      VALUES (NEW.space_id, (SELECT active_schema_generation FROM effect_local_server_spaces
        WHERE space_id = NEW.space_id), NEW.model, NEW.entity_key, NEW.value_json, NEW.model_version,
        NEW.entity_bytes); END`,
    `CREATE TRIGGER effect_local_server_entities_update INSTEAD OF UPDATE ON effect_local_server_entities BEGIN
      UPDATE effect_local_server_entities_data SET space_id = NEW.space_id, model = NEW.model,
        entity_key = NEW.entity_key, value_json = NEW.value_json, model_version = NEW.model_version,
        entity_bytes = NEW.entity_bytes
      WHERE space_id = OLD.space_id AND generation = (SELECT active_schema_generation
        FROM effect_local_server_spaces WHERE space_id = OLD.space_id)
        AND model = OLD.model AND entity_key = OLD.entity_key; END`,
    `CREATE TRIGGER effect_local_server_entities_delete INSTEAD OF DELETE ON effect_local_server_entities BEGIN
      DELETE FROM effect_local_server_entities_data WHERE space_id = OLD.space_id
        AND generation = (SELECT active_schema_generation FROM effect_local_server_spaces
          WHERE space_id = OLD.space_id) AND model = OLD.model AND entity_key = OLD.entity_key; END`,
    `CREATE TRIGGER effect_local_server_entity_count_insert AFTER INSERT ON effect_local_server_entities_data
      WHEN NEW.generation = (SELECT active_schema_generation FROM effect_local_server_spaces
        WHERE space_id = NEW.space_id) BEGIN
      UPDATE effect_local_server_spaces SET entity_count = entity_count + 1,
        entity_bytes = entity_bytes + NEW.entity_bytes WHERE space_id = NEW.space_id; END`,
    `CREATE TRIGGER effect_local_server_entity_count_delete AFTER DELETE ON effect_local_server_entities_data
      WHEN OLD.generation = (SELECT active_schema_generation FROM effect_local_server_spaces
        WHERE space_id = OLD.space_id) BEGIN
      UPDATE effect_local_server_spaces SET entity_count = entity_count - 1,
        entity_bytes = entity_bytes - OLD.entity_bytes WHERE space_id = OLD.space_id; END`,
    `CREATE TRIGGER effect_local_server_entity_count_update AFTER UPDATE OF entity_bytes
      ON effect_local_server_entities_data
      WHEN NEW.generation = OLD.generation AND NEW.space_id = OLD.space_id AND
        NEW.generation = (SELECT active_schema_generation FROM effect_local_server_spaces
          WHERE space_id = NEW.space_id) BEGIN
      UPDATE effect_local_server_spaces SET entity_bytes = entity_bytes + NEW.entity_bytes - OLD.entity_bytes
        WHERE space_id = NEW.space_id; END`,
    `CREATE TABLE effect_local_server_replication_views (
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      principal_digest TEXT NOT NULL CHECK (length(principal_digest) = 64),
      view_id TEXT NOT NULL,
      view_revision INTEGER NOT NULL CHECK (view_revision >= 0),
      scope_generation INTEGER NOT NULL CHECK (scope_generation >= 0),
      scope_json TEXT NOT NULL CHECK (json_valid(scope_json)),
      scope_digest TEXT NOT NULL CHECK (length(scope_digest) = 64),
      definition_hash TEXT NOT NULL,
      schema_version INTEGER NOT NULL,
      schema_hash TEXT NOT NULL,
      server_sequence INTEGER NOT NULL CHECK (server_sequence >= 0),
      PRIMARY KEY (space_id, client_id)
    )`,
    `CREATE TABLE effect_local_server_replication_view_entities (
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      principal_digest TEXT NOT NULL CHECK (length(principal_digest) = 64),
      view_id TEXT NOT NULL,
      model TEXT NOT NULL,
      model_version INTEGER NOT NULL CHECK (model_version > 0),
      entity_key TEXT NOT NULL,
      disposition TEXT NOT NULL CHECK (disposition IN ('Upsert', 'Delete', 'Retract')),
      value_json TEXT CHECK (value_json IS NULL OR json_valid(value_json)),
      PRIMARY KEY (space_id, client_id, view_id, model, entity_key)
    )`,
    `CREATE TABLE effect_local_server_replication_pages (
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      principal_digest TEXT NOT NULL CHECK (length(principal_digest) = 64),
      view_id TEXT NOT NULL,
      base_revision INTEGER NOT NULL CHECK (base_revision >= 0),
      target_revision INTEGER NOT NULL CHECK (target_revision = base_revision + 1),
      scope_generation INTEGER NOT NULL CHECK (scope_generation >= 0),
      scope_json TEXT NOT NULL CHECK (json_valid(scope_json)),
      scope_digest TEXT NOT NULL CHECK (length(scope_digest) = 64),
      server_sequence INTEGER NOT NULL CHECK (server_sequence >= 0),
      changes_json TEXT NOT NULL CHECK (json_valid(changes_json)),
      content_bytes INTEGER NOT NULL CHECK (content_bytes >= 0),
      digest TEXT NOT NULL CHECK (length(digest) = 64),
      has_more INTEGER NOT NULL CHECK (has_more IN (0, 1)),
      PRIMARY KEY (space_id, client_id)
    )`,
    `CREATE TABLE effect_local_server_scoped_snapshots (
      snapshot_id TEXT PRIMARY KEY,
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      principal_digest TEXT NOT NULL CHECK (length(principal_digest) = 64),
      definition_hash TEXT NOT NULL,
      schema_version INTEGER NOT NULL,
      schema_hash TEXT NOT NULL,
      scope_json TEXT NOT NULL CHECK (json_valid(scope_json)),
      scope_digest TEXT NOT NULL CHECK (length(scope_digest) = 64),
      scope_generation INTEGER NOT NULL CHECK (scope_generation >= 0),
      view_id TEXT NOT NULL,
      view_revision INTEGER NOT NULL CHECK (view_revision >= 0),
      server_sequence INTEGER NOT NULL CHECK (server_sequence >= 0),
      terminal_sequence INTEGER NOT NULL CHECK (terminal_sequence >= 0),
      entry_count INTEGER NOT NULL CHECK (entry_count >= 0),
      content_bytes INTEGER NOT NULL CHECK (content_bytes >= 0),
      digest TEXT NOT NULL CHECK (length(digest) = 64),
      UNIQUE (space_id, client_id)
    )`,
    `CREATE TABLE effect_local_server_scoped_snapshot_entries (
      snapshot_id TEXT NOT NULL,
      ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
      change_json TEXT NOT NULL CHECK (json_valid(change_json)),
      entry_bytes INTEGER NOT NULL CHECK (entry_bytes > 0),
      source_model TEXT NOT NULL,
      source_model_version INTEGER NOT NULL CHECK (source_model_version > 0),
      source_entity_key TEXT NOT NULL,
      source_value_json TEXT NOT NULL CHECK (json_valid(source_value_json)),
      PRIMARY KEY (snapshot_id, ordinal)
    )`,
    `CREATE INDEX effect_local_server_replication_view_entities_identity
      ON effect_local_server_replication_view_entities (space_id, client_id, model, entity_key)`,
    `CREATE INDEX effect_local_server_scoped_snapshot_entries_page
      ON effect_local_server_scoped_snapshot_entries (snapshot_id, ordinal)`
  ]
})

const serverV8 = makeMigration({
  id: 8,
  name: "membership-incarnation-lineage",
  statements: [
    "DROP TRIGGER effect_local_count_history_insert",
    "DROP TRIGGER effect_local_count_history_delete",
    "DROP TRIGGER effect_local_count_receipt_insert",
    "DROP TRIGGER effect_local_count_receipt_delete",
    "DROP TRIGGER effect_local_require_current_receipt_writer",
    "DROP TRIGGER effect_local_require_current_history_writer",
    "ALTER TABLE effect_local_server_clients RENAME TO effect_local_server_clients_v7",
    "ALTER TABLE effect_local_server_receipts RENAME TO effect_local_server_receipts_v7",
    "ALTER TABLE effect_local_authoritative_log RENAME TO effect_local_authoritative_log_v7",
    `CREATE TABLE effect_local_server_clients (
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      membership_incarnation TEXT NOT NULL,
      last_local_sequence INTEGER NOT NULL,
      expired_local_sequence INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (space_id, client_id, membership_incarnation)
    )`,
    `CREATE TABLE effect_local_server_receipts (
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      membership_incarnation TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      digest TEXT NOT NULL,
      receipt_json TEXT NOT NULL,
      digest_version INTEGER NOT NULL CHECK (digest_version = 3),
      source_schema_version INTEGER,
      source_schema_hash TEXT,
      mutation_version INTEGER,
      rejection_origin TEXT,
      terminal_sequence INTEGER NOT NULL DEFAULT 0,
      server_sequence INTEGER,
      mutation_name TEXT,
      PRIMARY KEY (space_id, client_id, membership_incarnation, local_sequence),
      UNIQUE (space_id, mutation_id)
    )`,
    `CREATE TABLE effect_local_authoritative_log (
      space_id TEXT NOT NULL,
      server_sequence INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      entry_bytes INTEGER NOT NULL CHECK (entry_bytes > 0),
      entry_json TEXT NOT NULL,
      source_schema_version INTEGER,
      source_schema_hash TEXT,
      mutation_version INTEGER,
      client_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      digest TEXT NOT NULL,
      membership_incarnation TEXT NOT NULL,
      PRIMARY KEY (space_id, server_sequence),
      UNIQUE (space_id, mutation_id)
    )`,
    "DROP TABLE effect_local_server_clients_v7",
    "DROP TABLE effect_local_server_receipts_v7",
    "DROP TABLE effect_local_authoritative_log_v7",
    `CREATE INDEX effect_local_server_history_terminal
      ON effect_local_authoritative_log (space_id, server_sequence, mutation_id)`,
    `CREATE INDEX effect_local_server_receipts_terminal
      ON effect_local_server_receipts
        (space_id, terminal_sequence, client_id, membership_incarnation, local_sequence)`,
    `CREATE TRIGGER effect_local_count_history_insert AFTER INSERT ON effect_local_authoritative_log
      BEGIN
        UPDATE effect_local_server_spaces SET retained_history_count = retained_history_count + 1
          WHERE space_id = NEW.space_id;
        UPDATE effect_local_server_space_counts SET history_count = history_count + 1
          WHERE space_id = NEW.space_id;
      END`,
    `CREATE TRIGGER effect_local_count_history_delete AFTER DELETE ON effect_local_authoritative_log
      BEGIN
        UPDATE effect_local_server_spaces SET retained_history_count = retained_history_count - 1
          WHERE space_id = OLD.space_id;
        UPDATE effect_local_server_space_counts SET history_count = history_count - 1
          WHERE space_id = OLD.space_id;
      END`,
    `CREATE TRIGGER effect_local_count_receipt_insert AFTER INSERT ON effect_local_server_receipts
      BEGIN
        UPDATE effect_local_server_spaces SET retained_receipt_count = retained_receipt_count + 1
          WHERE space_id = NEW.space_id;
        UPDATE effect_local_server_space_counts SET receipt_count = receipt_count + 1
          WHERE space_id = NEW.space_id;
      END`,
    `CREATE TRIGGER effect_local_count_receipt_delete AFTER DELETE ON effect_local_server_receipts
      BEGIN
        UPDATE effect_local_server_spaces SET retained_receipt_count = retained_receipt_count - 1
          WHERE space_id = OLD.space_id;
        UPDATE effect_local_server_space_counts SET receipt_count = receipt_count - 1
          WHERE space_id = OLD.space_id;
      END`,
    `CREATE TRIGGER effect_local_require_current_receipt_writer
      BEFORE INSERT ON effect_local_server_receipts
      WHEN NEW.terminal_sequence = 0
      BEGIN SELECT RAISE(ABORT, 'effect-local server writer upgrade required'); END`,
    `CREATE TRIGGER effect_local_require_current_history_writer
      BEFORE INSERT ON effect_local_authoritative_log
      WHEN NEW.client_id = '' OR NEW.local_sequence = 0 OR NEW.digest = ''
      BEGIN SELECT RAISE(ABORT, 'effect-local server writer upgrade required'); END`
  ]
})

const serverV9 = makeMigration({
  id: 9,
  name: "snapshot-schema-projections",
  statements: [
    `CREATE TABLE effect_local_server_snapshot_projections (
      space_id TEXT NOT NULL,
      snapshot_id TEXT NOT NULL,
      target_schema_version INTEGER NOT NULL,
      target_schema_hash TEXT NOT NULL,
      definition_hash TEXT NOT NULL,
      entity_count INTEGER NOT NULL CHECK (entity_count >= 0),
      content_bytes INTEGER NOT NULL CHECK (content_bytes >= 0),
      digest TEXT NOT NULL,
      PRIMARY KEY (space_id, snapshot_id, target_schema_version, target_schema_hash)
    )`,
    `CREATE TABLE effect_local_server_snapshot_projection_entities (
      space_id TEXT NOT NULL,
      snapshot_id TEXT NOT NULL,
      target_schema_version INTEGER NOT NULL,
      target_schema_hash TEXT NOT NULL,
      ordinal INTEGER NOT NULL,
      model TEXT NOT NULL,
      model_version INTEGER NOT NULL,
      entity_key TEXT NOT NULL,
      wire_json TEXT NOT NULL,
      wire_bytes INTEGER NOT NULL CHECK (wire_bytes > 0),
      PRIMARY KEY (space_id, snapshot_id, target_schema_version, target_schema_hash, ordinal),
      UNIQUE (space_id, snapshot_id, target_schema_version, target_schema_hash, model, entity_key)
    )`,
    `CREATE TRIGGER effect_local_delete_snapshot_projections AFTER DELETE ON effect_local_server_snapshots BEGIN
      DELETE FROM effect_local_server_snapshot_projection_entities
        WHERE space_id = OLD.space_id AND snapshot_id = OLD.snapshot_id;
      DELETE FROM effect_local_server_snapshot_projections
        WHERE space_id = OLD.space_id AND snapshot_id = OLD.snapshot_id;
    END`
  ]
})

const serverV10 = makeMigration({
  id: 10,
  name: "server-secondary-indexes",
  statements: [
    `CREATE TABLE effect_local_server_index_catalog (
      model TEXT NOT NULL,
      index_name TEXT NOT NULL,
      descriptor_hash TEXT NOT NULL,
      table_name TEXT NOT NULL UNIQUE,
      scan_index_name TEXT NOT NULL UNIQUE,
      PRIMARY KEY (model, index_name, descriptor_hash)
    )`,
    `CREATE TABLE effect_local_server_index_state (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL CHECK (schema_generation >= 0),
      descriptor_hash TEXT NOT NULL,
      built INTEGER NOT NULL DEFAULT 0 CHECK (built IN (0, 1)),
      PRIMARY KEY (space_id, schema_generation, descriptor_hash)
    )`,
    `CREATE TABLE effect_local_server_index_partition_log (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL CHECK (schema_generation >= 0),
      server_sequence INTEGER NOT NULL CHECK (server_sequence >= 0),
      descriptor_hash TEXT NOT NULL,
      partition_json TEXT NOT NULL CHECK (json_valid(partition_json)),
      PRIMARY KEY (space_id, server_sequence, descriptor_hash, partition_json)
    )`
  ]
})

const serverV11 = makeMigration({
  id: 11,
  name: "scoped-replication-fences",
  statements: [
    "ALTER TABLE effect_local_server_spaces ADD COLUMN read_auth_epoch INTEGER NOT NULL DEFAULT 0 CHECK (read_auth_epoch >= 0)",
    "ALTER TABLE effect_local_server_replication_views ADD COLUMN delivered_sequence INTEGER NOT NULL DEFAULT 0 CHECK (delivered_sequence >= 0)",
    "UPDATE effect_local_server_replication_views SET delivered_sequence = server_sequence",
    "ALTER TABLE effect_local_server_replication_views ADD COLUMN read_auth_epoch INTEGER NOT NULL DEFAULT 0 CHECK (read_auth_epoch >= 0)",
    "ALTER TABLE effect_local_server_replication_pages ADD COLUMN read_auth_epoch INTEGER NOT NULL DEFAULT 0 CHECK (read_auth_epoch >= 0)",
    "ALTER TABLE effect_local_server_replication_views ADD COLUMN index_layout_hash TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE effect_local_server_scoped_snapshots ADD COLUMN index_layout_hash TEXT NOT NULL DEFAULT ''"
  ]
})

const serverV12 = makeMigration({
  id: 12,
  name: "durable-offline-wakes",
  statements: [
    `CREATE TABLE effect_local_server_offline_wake_acknowledgements (
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      acknowledged_sequence INTEGER NOT NULL CHECK (acknowledged_sequence >= 0),
      PRIMARY KEY (space_id, client_id)
    )`,
    `CREATE TABLE effect_local_server_offline_wake_spaces (
      space_id TEXT PRIMARY KEY,
      high_water_sequence INTEGER NOT NULL CHECK (high_water_sequence > 0),
      expanded_sequence INTEGER NOT NULL DEFAULT 0 CHECK (expanded_sequence >= 0 AND expanded_sequence <= high_water_sequence),
      membership_generation INTEGER NOT NULL DEFAULT 0 CHECK (membership_generation >= 0),
      attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
      next_attempt_at INTEGER NOT NULL CHECK (next_attempt_at >= 0),
      claim_token TEXT,
      claimed_until INTEGER,
      CHECK ((claim_token IS NULL AND claimed_until IS NULL) OR
        (claim_token IS NOT NULL AND claimed_until IS NOT NULL AND claimed_until >= 0))
    )`,
    `CREATE INDEX effect_local_server_offline_wake_spaces_due
      ON effect_local_server_offline_wake_spaces (next_attempt_at, space_id)
      WHERE high_water_sequence > expanded_sequence`,
    `CREATE TABLE effect_local_server_offline_wakes (
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      wake_id TEXT NOT NULL,
      high_water_sequence INTEGER NOT NULL CHECK (high_water_sequence > 0),
      notified_sequence INTEGER NOT NULL DEFAULT 0 CHECK (notified_sequence >= 0 AND notified_sequence <= high_water_sequence),
      membership_generation INTEGER NOT NULL CHECK (membership_generation > 0),
      attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
      next_attempt_at INTEGER NOT NULL CHECK (next_attempt_at >= 0),
      claim_token TEXT,
      claimed_until INTEGER,
      PRIMARY KEY (space_id, client_id),
      UNIQUE (wake_id),
      CHECK ((claim_token IS NULL AND claimed_until IS NULL) OR
        (claim_token IS NOT NULL AND claimed_until IS NOT NULL AND claimed_until >= 0))
    )`,
    `CREATE INDEX effect_local_server_offline_wakes_due
      ON effect_local_server_offline_wakes (next_attempt_at, space_id, client_id)
      WHERE high_water_sequence > notified_sequence`,
    `CREATE TABLE effect_local_server_watch_runtimes (
      runtime_id TEXT PRIMARY KEY,
      expires_at INTEGER NOT NULL CHECK (expires_at >= 0)
    )`,
    `CREATE INDEX effect_local_server_watch_runtimes_expiry
      ON effect_local_server_watch_runtimes (expires_at, runtime_id)`,
    `CREATE TABLE effect_local_server_watch_presence (
      space_id TEXT NOT NULL,
      client_id TEXT NOT NULL,
      watcher_id TEXT NOT NULL,
      runtime_id TEXT NOT NULL,
      PRIMARY KEY (space_id, client_id, watcher_id),
      UNIQUE (runtime_id, watcher_id)
    )`,
    `CREATE INDEX effect_local_server_watch_presence_active
      ON effect_local_server_watch_presence (space_id, client_id, runtime_id)`,
    `CREATE INDEX effect_local_server_watch_presence_runtime
      ON effect_local_server_watch_presence (runtime_id)`
  ]
})

export const serverCatalog = Object.freeze([
  serverV1,
  serverV2,
  serverV3,
  serverV4,
  serverV5,
  serverV6,
  serverV7,
  serverV8,
  serverV9,
  serverV10,
  serverV11,
  serverV12
])

export const client = Effect.fnUntraced(function*(options: {
  readonly definition: Definition.Any
  readonly spaceId?: Identity.SpaceId
  readonly clientId: Identity.ClientId
  readonly migration?: Options
}) {
  const sql = yield* SqlClient.SqlClient
  yield* sql.unsafe("PRAGMA foreign_keys = ON")
  const pragma = yield* SqlSchema.findOne({
    Request: Schema.Void,
    Result: PragmaEnabledRow,
    execute: () => sql`PRAGMA foreign_keys`
  })(undefined).pipe(Effect.mapError((cause) => {
    if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
    return new ReplicaError.StorageCorrupt({ message: "SQLite foreign key state is unreadable", cause })
  }))
  if (pragma.foreign_keys !== 1) {
    return yield* new ReplicaError.StorageCorrupt({ message: "SQLite foreign keys could not be enabled" })
  }
  const metaExists = yield* SqlSchema.findOne({
    Request: Schema.Void,
    Result: CountRow,
    execute: () =>
      sql`SELECT COUNT(*) AS count FROM sqlite_master
        WHERE type = 'table' AND name = 'effect_local_client_meta'`
  })(undefined).pipe(Effect.mapError((cause) => {
    if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
    return new ReplicaError.StorageCorrupt({ message: "Client metadata catalog is unreadable", cause })
  }))
  if (metaExists.count !== 0) {
    const beforeMigration = yield* SqlSchema.findOneOption({
      Request: Schema.Void,
      Result: ClientIdentityRow,
      execute: () => sql`SELECT client_id FROM effect_local_client_meta WHERE singleton = 1`
    })(undefined).pipe(Effect.mapError((cause) => {
      if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
      return new ReplicaError.StorageCorrupt({ message: "Client replica identity is corrupt", cause })
    }))
    if (Option.isSome(beforeMigration) && beforeMigration.value.client_id !== options.clientId) {
      return yield* new ReplicaError.ReplicaIdentityMismatch({
        expectedClientId: options.clientId,
        actualClientId: beforeMigration.value.client_id
      })
    }
  }
  yield* runCatalog("Client", clientCatalog, options.migration)
  const existing = yield* SqlSchema.findOneOption({
    Request: Schema.Void,
    Result: ClientIdentityRow,
    execute: () => sql`SELECT client_id FROM effect_local_client_meta WHERE singleton = 1`
  })(undefined).pipe(
    Effect.mapError((cause) => {
      if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
      return new ReplicaError.StorageCorrupt({ message: "Client replica identity is corrupt", cause })
    })
  )
  if (Option.isSome(existing) && existing.value.client_id !== options.clientId) {
    return yield* new ReplicaError.ReplicaIdentityMismatch({
      expectedClientId: options.clientId,
      actualClientId: existing.value.client_id
    })
  }
  yield* sql`INSERT INTO effect_local_client_meta
    (singleton, client_id) VALUES (1, ${options.clientId})
    ON CONFLICT (singleton) DO NOTHING`
  if (options.spaceId !== undefined) {
    yield* sql`INSERT INTO effect_local_client_spaces
        (space_id, membership_incarnation, definition_hash, schema_version, schema_hash, schema_generation,
          active_schema_generation, active_projection_generation, projection_schema_generation,
          next_local_sequence, server_cursor, visible_revision, requested_generation, completed_generation,
          installed_snapshot_sequence, installed_snapshot_terminal_sequence)
        VALUES (${options.spaceId},
          ('inc_' || lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
            substr(lower(hex(randomblob(2))), 2) || '-' ||
            substr('89ab', abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))), 2) || '-' ||
            lower(hex(randomblob(6)))), ${options.definition.hash},
          ${options.definition.schemaIdentity.version}, ${options.definition.schemaIdentity.hash}, 0, 0, 0, 0,
          1, 0, 0, 0, 0, 0, 0)
        ON CONFLICT (space_id) DO NOTHING`
  }
  return undefined
}, Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))

export const server = (options: Options = defaultOptions) => runCatalog("Server", serverCatalog, options)
