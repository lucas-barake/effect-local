import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import { identity } from "effect/Function"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlError from "effect/sql/SqlError"
import * as SqlSchema from "effect/sql/SqlSchema"
import * as ConnectionLane from "./ConnectionLane.js"
import * as Configuration from "./internal/configuration.js"
import * as Dialect from "./internal/dialect.js"
import * as ServerIndex from "./internal/serverIndex.js"
import * as SqliteIdentifier from "./internal/sqliteIdentifier.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"

export type Catalog = "Client" | "Server"

export interface Migration {
  readonly id: number
  readonly name: string
  readonly checksum: Identity.SchemaHash
  readonly statements: ReadonlyArray<string>
}

export interface Options {
  readonly retryDelay?: Duration.Input | undefined
  readonly maximumAttempts?: number | undefined
}

export interface ServerOptions extends Options {
  readonly mode?: "apply" | "verify" | undefined
}

export interface RenderServerOptions {
  readonly definition: Definition.Any
}

const defaultOptions = { retryDelay: "5 millis", maximumAttempts: 8 } as const satisfies Options

const stableName = /^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$/

/* oxlint-disable effect/noThrowStatement, effect/noNewError -- Migration descriptors are synchronous schema values and must reject invalid catalogs before any Effect is constructed. */
export const makeMigration = (options: {
  readonly id: number
  readonly name: string
  readonly statements: ReadonlyArray<string>
}): Migration => {
  if (!Number.isSafeInteger(options.id) || options.id <= 0) {
    throw new TypeError(`Storage migration id must be a positive safe integer: ${options.id}`)
  }
  if (!stableName.test(options.name)) throw new TypeError(`Storage migration name is not stable: ${options.name}`)
  if (options.statements.length === 0) throw new TypeError(`Storage migration ${options.name} has no statements`)
  const statements = Object.freeze([...options.statements])
  return Object.freeze({
    id: options.id,
    name: options.name,
    checksum: Identity.SchemaHash.make(Canonical.hash({
      format: 1,
      id: options.id,
      name: options.name,
      statements
    })),
    statements
  })
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

const compareLedger = (
  catalog: Catalog,
  migrations: ReadonlyArray<Migration>,
  applied: ReadonlyArray<typeof MigrationRow.Type>
): ReplicaError.StorageMigrationMismatch | undefined => {
  if (applied.length > migrations.length) {
    return new ReplicaError.StorageMigrationMismatch({
      catalog,
      message: `${catalog} catalog deleted ${applied.length - migrations.length} applied migration(s)`
    })
  }
  for (let index = 0; index < applied.length; index++) {
    const stored = applied[index]
    const expected = migrations[index]
    if (stored.id !== expected.id || stored.name !== expected.name || stored.checksum !== expected.checksum) {
      return new ReplicaError.StorageMigrationMismatch({
        catalog,
        message:
          `Applied migration ${stored.id}:${stored.name}:${stored.checksum} does not match ${expected.id}:${expected.name}:${expected.checksum}`
      })
    }
  }
  return undefined
}

const readServerLedger = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: MigrationRow,
    execute: () => sql`SELECT id, name, checksum FROM effect_local_server_migrations ORDER BY id`
  })

const retryPolicy = Effect.fnUntraced(function*(options: Options) {
  const maximumAttempts = yield* Configuration.positiveSafeInteger(
    "migration.maximumAttempts",
    options.maximumAttempts ?? defaultOptions.maximumAttempts
  )
  const retryDelayMillis = yield* Configuration.positiveFiniteDurationMillis(
    "migration.retryDelay",
    options.retryDelay ?? defaultOptions.retryDelay
  )
  return { maximumAttempts, retryDelayMillis }
})

const ledger = (table: string, text: string) =>
  `CREATE TABLE IF NOT EXISTS ${table} (
  id INTEGER PRIMARY KEY CHECK (id > 0),
  name ${text} NOT NULL UNIQUE,
  checksum ${text} NOT NULL,
  applied_at ${text} NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

interface Access {
  readonly withTransaction: <A, E extends { readonly _tag: string }, R,>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E | SqlError.SqlError, R>
  readonly withStatement: <A, E extends { readonly _tag: string }, R,>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E, R>
}

const runCatalogWith = Effect.fn("Migrations.runCatalog")(function*(
  access: Access,
  catalog: Catalog,
  migrations: ReadonlyArray<Migration>,
  options: Options
) {
  yield* Effect.annotateCurrentSpan({
    "migration.catalog": catalog,
    "migration.count": migrations.length
  })
  const invalid = validateCatalog(catalog, migrations)
  if (invalid !== undefined) return yield* invalid
  const { maximumAttempts, retryDelayMillis } = yield* retryPolicy(options)
  const sql = yield* SqlClient.SqlClient
  const readClient = SqlSchema.findAll({
    Request: Schema.Void,
    Result: MigrationRow,
    execute: () => sql`SELECT id, name, checksum FROM effect_local_client_migrations ORDER BY id`
  })
  const readServer = readServerLedger(sql)
  const dialect = yield* Dialect.make(sql)
  let ledgerTable = "effect_local_server_migrations"
  if (catalog === "Client") ledgerTable = "effect_local_client_migrations"
  let appliedAtAttempt = 0
  const migrate = Effect.gen(function*() {
    const ledgerStatement = ledger(ledgerTable, dialect.text)
    yield* access.withTransaction(dialect.lockSchema.pipe(Effect.andThen(sql.unsafe(ledgerStatement))))
    yield* access.withTransaction(Effect.gen(function*() {
      yield* dialect.lockSchema
      let read = readServer
      if (catalog === "Client") read = readClient
      const applied = yield* read(undefined).pipe(
        Effect.mapError((cause) => {
          if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
          return new ReplicaError.StorageCorrupt({ message: `${catalog} migration ledger is corrupt`, cause })
        })
      )
      appliedAtAttempt = applied.length
      const mismatch = compareLedger(catalog, migrations, applied)
      if (mismatch !== undefined) return yield* mismatch
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
      const applied = yield* access.withStatement(read(undefined)).pipe(
        Effect.mapError((cause) => {
          if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
          return new ReplicaError.StorageCorrupt({ message: `${catalog} migration ledger is corrupt`, cause })
        })
      )
      if (compareLedger(catalog, migrations, applied) === undefined && applied.length > appliedAtAttempt) {
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
      attempt >= maximumAttempts
    ) return yield* failure
    attempt += 1
    yield* Effect.sleep(retryDelayMillis)
  }
})

export const runCatalog = (catalog: Catalog, migrations: ReadonlyArray<Migration>, options: Options = {}) =>
  Effect.flatMap(
    SqlClient.SqlClient,
    (sql) =>
      runCatalogWith({ withTransaction: sql.withTransaction, withStatement: identity }, catalog, migrations, options)
  )

const clientBaseline = makeMigration({
  id: 1,
  name: "client-baseline",
  statements: [
    `CREATE TABLE effect_local_client_meta (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      client_id TEXT NOT NULL UNIQUE
    )`,
    `CREATE TABLE effect_local_client_spaces (
      space_id TEXT PRIMARY KEY,
      membership_incarnation TEXT NOT NULL,
      definition_hash TEXT NOT NULL,
      schema_version INTEGER NOT NULL,
      schema_hash TEXT NOT NULL,
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
      projection_replay_cursor TEXT,
      next_settled_sequence INTEGER NOT NULL DEFAULT 1,
      settlement_floor INTEGER NOT NULL DEFAULT 0,
      settlement_prune_sequence INTEGER NOT NULL DEFAULT 0
    )`,
    `CREATE TABLE effect_local_server_log (
      space_id TEXT NOT NULL,
      membership_incarnation TEXT NOT NULL,
      server_sequence INTEGER NOT NULL,
      mutation_id TEXT NOT NULL,
      entry_json TEXT NOT NULL,
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      PRIMARY KEY (space_id, server_sequence),
      UNIQUE (space_id, mutation_id),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
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
      digest_version INTEGER NOT NULL CHECK (digest_version = 1),
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      mutation_version INTEGER NOT NULL,
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
    `CREATE TABLE effect_local_client_receipts_data (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL,
      membership_incarnation TEXT NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      receipt_json TEXT NOT NULL,
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      mutation_version INTEGER NOT NULL,
      mutation_name TEXT NOT NULL,
      rejection_origin TEXT,
      settled_pending_json TEXT,
      settled_sequence INTEGER,
      pending_name TEXT,
      PRIMARY KEY (space_id, schema_generation, mutation_id),
      UNIQUE (space_id, schema_generation, local_sequence),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    `CREATE UNIQUE INDEX effect_local_client_receipts_settled
      ON effect_local_client_receipts_data (space_id, schema_generation, settled_sequence)
      WHERE settled_sequence IS NOT NULL`,
    `CREATE INDEX effect_local_client_receipts_unsettled
      ON effect_local_client_receipts_data (space_id, schema_generation, local_sequence)
      WHERE settled_sequence IS NULL`,
    `CREATE TABLE effect_local_client_canonical_entities_data (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL,
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      model_version INTEGER NOT NULL,
      PRIMARY KEY (space_id, schema_generation, model, entity_key),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    `CREATE TABLE effect_local_client_visible_entities_data (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL,
      projection_generation INTEGER NOT NULL,
      model TEXT NOT NULL,
      entity_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      model_version INTEGER NOT NULL,
      PRIMARY KEY (space_id, schema_generation, projection_generation, model, entity_key),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
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
    `CREATE TABLE effect_local_client_quarantine (
      space_id TEXT NOT NULL,
      membership_incarnation TEXT NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      basis INTEGER NOT NULL,
      name TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      digest TEXT NOT NULL,
      digest_version INTEGER NOT NULL CHECK (digest_version = 1),
      source_schema_version INTEGER NOT NULL,
      source_schema_hash TEXT NOT NULL,
      mutation_version INTEGER NOT NULL,
      rejection_json TEXT NOT NULL,
      target_schema_version INTEGER NOT NULL,
      target_schema_hash TEXT NOT NULL,
      PRIMARY KEY (space_id, mutation_id),
      UNIQUE (space_id, membership_incarnation, local_sequence),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    `CREATE TABLE effect_local_client_quarantine_resubmissions (
      space_id TEXT NOT NULL,
      original_mutation_id TEXT NOT NULL,
      replacement_mutation_id TEXT NOT NULL,
      PRIMARY KEY (space_id, original_mutation_id),
      UNIQUE (space_id, replacement_mutation_id),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    `CREATE TABLE effect_local_client_quarantine_cancellations (
      space_id TEXT NOT NULL,
      root_mutation_id TEXT NOT NULL,
      current_mutation_id TEXT NOT NULL,
      PRIMARY KEY (space_id, root_mutation_id),
      UNIQUE (space_id, current_mutation_id),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    `CREATE TABLE effect_local_client_projection_dirty (
      space_id TEXT NOT NULL,
      schema_generation INTEGER NOT NULL CHECK (schema_generation >= 0),
      model TEXT NOT NULL,
      model_version INTEGER NOT NULL CHECK (model_version > 0),
      entity_key TEXT NOT NULL,
      PRIMARY KEY (space_id, schema_generation, model, entity_key)
    )`,
    `CREATE TABLE effect_local_client_settlement_prune (
      space_id TEXT NOT NULL,
      pending_name TEXT NOT NULL,
      pruned_sequence INTEGER NOT NULL CHECK (pruned_sequence > 0),
      PRIMARY KEY (space_id, pending_name),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    `CREATE TABLE effect_local_client_retired_mutations (
      space_id TEXT NOT NULL,
      mutation_id TEXT NOT NULL,
      local_sequence INTEGER NOT NULL,
      PRIMARY KEY (space_id, mutation_id),
      FOREIGN KEY (space_id) REFERENCES effect_local_client_spaces(space_id) ON DELETE CASCADE
    )`,
    `CREATE INDEX effect_local_client_retired_mutations_sequence
      ON effect_local_client_retired_mutations (space_id, local_sequence)`
  ]
})

export const clientCatalog = Object.freeze([clientBaseline])

interface ServerTypes {
  readonly text: string
  readonly integer: string
  readonly flag: string
  readonly json: (column: string) => string
}

const serverTables = (types: ServerTypes): ReadonlyArray<string> => {
  const { flag, integer, json, text } = types
  return [
    `CREATE TABLE effect_local_server_spaces (
      space_id ${text} PRIMARY KEY,
      definition_hash ${text} NOT NULL,
      next_server_sequence ${integer} NOT NULL,
      schema_version ${integer} NOT NULL,
      schema_hash ${text} NOT NULL,
      schema_generation ${integer} NOT NULL DEFAULT 0 CHECK (schema_generation >= 0),
      active_schema_generation ${integer} NOT NULL DEFAULT 0 CHECK (active_schema_generation >= 0),
      target_schema_version ${integer},
      target_schema_hash ${text},
      migration_hash ${text},
      next_terminal_sequence ${integer} NOT NULL DEFAULT 1,
      history_floor ${integer} NOT NULL DEFAULT 0,
      receipt_floor ${integer} NOT NULL DEFAULT 0,
      retained_history_count ${integer} NOT NULL DEFAULT 0,
      retained_receipt_count ${integer} NOT NULL DEFAULT 0,
      entity_count ${integer} NOT NULL DEFAULT 0,
      entity_bytes ${integer} NOT NULL DEFAULT 0,
      snapshot_id ${text},
      snapshot_sequence ${integer} NOT NULL DEFAULT 0,
      snapshot_terminal_sequence ${integer} NOT NULL DEFAULT 0,
      read_auth_epoch ${integer} NOT NULL DEFAULT 0 CHECK (read_auth_epoch >= 0)
    )`,
    `CREATE TABLE effect_local_server_clients (
      space_id ${text} NOT NULL,
      client_id ${text} NOT NULL,
      membership_incarnation ${text} NOT NULL,
      last_local_sequence ${integer} NOT NULL,
      expired_local_sequence ${integer} NOT NULL DEFAULT 0,
      PRIMARY KEY (space_id, client_id, membership_incarnation)
    )`,
    `CREATE TABLE effect_local_server_receipts (
      space_id ${text} NOT NULL,
      client_id ${text} NOT NULL,
      membership_incarnation ${text} NOT NULL,
      local_sequence ${integer} NOT NULL,
      mutation_id ${text} NOT NULL,
      digest ${text} NOT NULL,
      digest_version INTEGER NOT NULL CHECK (digest_version = 1),
      receipt_json ${text} NOT NULL,
      source_schema_version ${integer} NOT NULL,
      source_schema_hash ${text} NOT NULL,
      mutation_version ${integer} NOT NULL,
      mutation_name ${text} NOT NULL,
      rejection_origin ${text},
      terminal_sequence ${integer} NOT NULL,
      server_sequence ${integer},
      PRIMARY KEY (space_id, client_id, membership_incarnation, local_sequence),
      UNIQUE (space_id, mutation_id)
    )`,
    `CREATE INDEX effect_local_server_receipts_terminal
      ON effect_local_server_receipts
        (space_id, terminal_sequence, client_id, membership_incarnation, local_sequence)`,
    `CREATE TABLE effect_local_authoritative_log (
      space_id ${text} NOT NULL,
      server_sequence ${integer} NOT NULL,
      mutation_id ${text} NOT NULL,
      client_id ${text} NOT NULL,
      membership_incarnation ${text} NOT NULL,
      local_sequence ${integer} NOT NULL,
      digest ${text} NOT NULL,
      entry_bytes ${integer} NOT NULL CHECK (entry_bytes > 0),
      entry_json ${text} NOT NULL,
      source_schema_version ${integer} NOT NULL,
      source_schema_hash ${text} NOT NULL,
      mutation_version ${integer} NOT NULL,
      PRIMARY KEY (space_id, server_sequence),
      UNIQUE (space_id, mutation_id)
    )`,
    `CREATE INDEX effect_local_server_history_terminal
      ON effect_local_authoritative_log (space_id, server_sequence, mutation_id)`,
    `CREATE TABLE effect_local_server_entities_data (
      space_id ${text} NOT NULL,
      generation ${integer} NOT NULL,
      model ${text} NOT NULL,
      entity_key ${text} NOT NULL,
      value_json ${text} NOT NULL,
      model_version ${integer} NOT NULL,
      entity_bytes ${integer} NOT NULL,
      PRIMARY KEY (space_id, generation, model, entity_key)
    )`,
    `CREATE INDEX effect_local_server_entities_largest
      ON effect_local_server_entities_data (space_id, generation, entity_bytes DESC, model, entity_key)`,
    `CREATE VIEW effect_local_server_entities AS
      SELECT d.space_id, d.model, d.entity_key, d.value_json, d.model_version, d.entity_bytes
      FROM effect_local_server_entities_data AS d
      INNER JOIN effect_local_server_spaces AS s ON s.space_id = d.space_id
        AND s.active_schema_generation = d.generation`,
    `CREATE TABLE effect_local_server_evolution (
      space_id ${text} PRIMARY KEY,
      source_schema_version ${integer} NOT NULL,
      source_schema_hash ${text} NOT NULL,
      target_schema_version ${integer} NOT NULL,
      target_schema_hash ${text} NOT NULL,
      migration_hash ${text} NOT NULL,
      generation ${integer} NOT NULL CHECK (generation > 0),
      source_generation ${integer} NOT NULL CHECK (source_generation >= 0),
      target_entity_count ${integer} NOT NULL CHECK (target_entity_count >= 0),
      target_entity_bytes ${integer} NOT NULL CHECK (target_entity_bytes >= 0),
      phase ${text} NOT NULL,
      cursor_model ${text},
      cursor_key ${text},
      cursor_sequence ${integer}
    )`,
    `CREATE TABLE effect_local_server_key_lineage (
      space_id ${text} NOT NULL,
      source_schema_version ${integer} NOT NULL,
      source_schema_hash ${text} NOT NULL,
      source_model ${text} NOT NULL,
      source_model_version ${integer} NOT NULL,
      source_key ${text} NOT NULL,
      target_model ${text} NOT NULL,
      target_model_version ${integer} NOT NULL,
      target_key ${text} NOT NULL,
      PRIMARY KEY (
        space_id, source_schema_version, source_schema_hash, source_model, source_model_version, source_key
      )
    )`,
    `CREATE INDEX effect_local_server_key_lineage_target
      ON effect_local_server_key_lineage (space_id, target_model, target_model_version, target_key)`,
    `CREATE TABLE effect_local_server_key_lineage_groups (
      space_id ${text} NOT NULL,
      source_schema_version ${integer} NOT NULL,
      source_schema_hash ${text} NOT NULL,
      source_model ${text} NOT NULL,
      source_model_version ${integer} NOT NULL,
      source_key ${text} NOT NULL,
      lineage_id ${text} NOT NULL,
      PRIMARY KEY (
        space_id, source_schema_version, source_schema_hash, source_model, source_model_version, source_key
      )
    )`,
    `CREATE TABLE effect_local_server_key_lineage_targets (
      space_id ${text} NOT NULL,
      target_model ${text} NOT NULL,
      target_model_version ${integer} NOT NULL,
      target_key ${text} NOT NULL,
      lineage_id ${text} NOT NULL,
      PRIMARY KEY (space_id, target_model, target_model_version, target_key)
    )`,
    `CREATE TABLE effect_local_server_space_counts (
      space_id ${text} PRIMARY KEY,
      history_count ${integer} NOT NULL CHECK (history_count >= 0),
      receipt_count ${integer} NOT NULL CHECK (receipt_count >= 0)
    )`,
    `CREATE INDEX effect_local_server_space_counts_history
      ON effect_local_server_space_counts (history_count DESC)`,
    `CREATE INDEX effect_local_server_space_counts_receipts
      ON effect_local_server_space_counts (receipt_count DESC)`,
    `CREATE TABLE effect_local_server_snapshots (
      space_id ${text} NOT NULL,
      snapshot_id ${text} NOT NULL,
      definition_hash ${text} NOT NULL,
      schema_version ${integer} NOT NULL,
      schema_hash ${text} NOT NULL,
      server_sequence ${integer} NOT NULL,
      terminal_sequence ${integer} NOT NULL,
      entity_count ${integer} NOT NULL,
      content_bytes ${integer} NOT NULL,
      digest ${text} NOT NULL,
      PRIMARY KEY (space_id, snapshot_id),
      UNIQUE (space_id, server_sequence, terminal_sequence)
    )`,
    `CREATE INDEX effect_local_server_snapshots_latest
      ON effect_local_server_snapshots (space_id, server_sequence DESC, terminal_sequence DESC)`,
    `CREATE TABLE effect_local_server_snapshot_entities (
      space_id ${text} NOT NULL,
      snapshot_id ${text} NOT NULL,
      ordinal ${integer} NOT NULL,
      model ${text} NOT NULL,
      model_version ${integer} NOT NULL,
      entity_key ${text} NOT NULL,
      value_json ${text} NOT NULL,
      entity_bytes ${integer} NOT NULL,
      wire_json ${text} NOT NULL,
      wire_bytes ${integer} NOT NULL CHECK (wire_bytes > 0),
      PRIMARY KEY (space_id, snapshot_id, ordinal),
      UNIQUE (space_id, snapshot_id, model, entity_key)
    )`,
    `CREATE TABLE effect_local_server_replication_views (
      space_id ${text} NOT NULL,
      client_id ${text} NOT NULL,
      principal_digest ${text} NOT NULL CHECK (length(principal_digest) = 64),
      view_id ${text} NOT NULL,
      view_revision ${integer} NOT NULL CHECK (view_revision >= 0),
      membership_incarnation ${text} NOT NULL,
      scope_generation ${integer} NOT NULL CHECK (scope_generation >= 0),
      scope_json ${text} NOT NULL CHECK (${json("scope_json")}),
      scope_digest ${text} NOT NULL CHECK (length(scope_digest) = 64),
      definition_hash ${text} NOT NULL,
      index_layout_hash ${text} NOT NULL,
      schema_version ${integer} NOT NULL,
      schema_hash ${text} NOT NULL,
      server_sequence ${integer} NOT NULL CHECK (server_sequence >= 0),
      delivered_sequence ${integer} NOT NULL CHECK (delivered_sequence >= 0),
      read_auth_epoch ${integer} NOT NULL CHECK (read_auth_epoch >= 0),
      PRIMARY KEY (space_id, client_id)
    )`,
    `CREATE TABLE effect_local_server_replication_view_entities (
      space_id ${text} NOT NULL,
      client_id ${text} NOT NULL,
      principal_digest ${text} NOT NULL CHECK (length(principal_digest) = 64),
      view_id ${text} NOT NULL,
      model ${text} NOT NULL,
      model_version ${integer} NOT NULL CHECK (model_version > 0),
      entity_key ${text} NOT NULL,
      disposition ${text} NOT NULL CHECK (disposition IN ('Upsert', 'Delete', 'Retract')),
      value_json ${text} CHECK (value_json IS NULL OR ${json("value_json")}),
      PRIMARY KEY (space_id, client_id, view_id, model, entity_key)
    )`,
    `CREATE INDEX effect_local_server_replication_view_entities_identity
      ON effect_local_server_replication_view_entities (space_id, client_id, model, entity_key)`,
    `CREATE TABLE effect_local_server_replication_pages (
      space_id ${text} NOT NULL,
      client_id ${text} NOT NULL,
      principal_digest ${text} NOT NULL CHECK (length(principal_digest) = 64),
      view_id ${text} NOT NULL,
      base_revision ${integer} NOT NULL CHECK (base_revision >= 0),
      target_revision ${integer} NOT NULL CHECK (target_revision = base_revision + 1),
      scope_generation ${integer} NOT NULL CHECK (scope_generation >= 0),
      scope_json ${text} NOT NULL CHECK (${json("scope_json")}),
      scope_digest ${text} NOT NULL CHECK (length(scope_digest) = 64),
      server_sequence ${integer} NOT NULL CHECK (server_sequence >= 0),
      changes_json ${text} NOT NULL CHECK (${json("changes_json")}),
      content_bytes ${integer} NOT NULL CHECK (content_bytes >= 0),
      digest ${text} NOT NULL CHECK (length(digest) = 64),
      has_more ${flag} NOT NULL CHECK (has_more IN (0, 1)),
      read_auth_epoch ${integer} NOT NULL CHECK (read_auth_epoch >= 0),
      PRIMARY KEY (space_id, client_id)
    )`,
    `CREATE TABLE effect_local_server_scoped_snapshots (
      snapshot_id ${text} PRIMARY KEY,
      space_id ${text} NOT NULL,
      client_id ${text} NOT NULL,
      membership_incarnation ${text} NOT NULL,
      principal_digest ${text} NOT NULL CHECK (length(principal_digest) = 64),
      definition_hash ${text} NOT NULL,
      index_layout_hash ${text} NOT NULL,
      schema_version ${integer} NOT NULL,
      schema_hash ${text} NOT NULL,
      scope_json ${text} NOT NULL CHECK (${json("scope_json")}),
      scope_digest ${text} NOT NULL CHECK (length(scope_digest) = 64),
      scope_generation ${integer} NOT NULL CHECK (scope_generation >= 0),
      view_id ${text} NOT NULL,
      view_revision ${integer} NOT NULL CHECK (view_revision >= 0),
      server_sequence ${integer} NOT NULL CHECK (server_sequence >= 0),
      terminal_sequence ${integer} NOT NULL CHECK (terminal_sequence >= 0),
      entry_count ${integer} NOT NULL CHECK (entry_count >= 0),
      content_bytes ${integer} NOT NULL CHECK (content_bytes >= 0),
      digest ${text} NOT NULL CHECK (length(digest) = 64),
      UNIQUE (space_id, client_id)
    )`,
    `CREATE TABLE effect_local_server_scoped_snapshot_entries (
      snapshot_id ${text} NOT NULL,
      ordinal ${integer} NOT NULL CHECK (ordinal >= 0),
      change_json ${text} NOT NULL CHECK (${json("change_json")}),
      entry_bytes ${integer} NOT NULL CHECK (entry_bytes > 0),
      source_model ${text} NOT NULL,
      source_model_version ${integer} NOT NULL CHECK (source_model_version > 0),
      source_entity_key ${text} NOT NULL,
      source_value_json ${text} NOT NULL CHECK (${json("source_value_json")}),
      PRIMARY KEY (snapshot_id, ordinal)
    )`,
    `CREATE INDEX effect_local_server_scoped_snapshot_entries_page
      ON effect_local_server_scoped_snapshot_entries (snapshot_id, ordinal)`,
    `CREATE TABLE effect_local_server_index_catalog (
      model ${text} NOT NULL,
      index_name ${text} NOT NULL,
      descriptor_hash ${text} NOT NULL,
      table_name ${text} NOT NULL UNIQUE,
      scan_index_name ${text} NOT NULL UNIQUE,
      PRIMARY KEY (model, index_name, descriptor_hash)
    )`,
    `CREATE TABLE effect_local_server_index_state (
      space_id ${text} NOT NULL,
      schema_generation ${integer} NOT NULL CHECK (schema_generation >= 0),
      descriptor_hash ${text} NOT NULL,
      built ${flag} NOT NULL DEFAULT 0 CHECK (built IN (0, 1)),
      PRIMARY KEY (space_id, schema_generation, descriptor_hash)
    )`,
    `CREATE TABLE effect_local_server_index_partition_log (
      space_id ${text} NOT NULL,
      schema_generation ${integer} NOT NULL CHECK (schema_generation >= 0),
      server_sequence ${integer} NOT NULL CHECK (server_sequence >= 0),
      descriptor_hash ${text} NOT NULL,
      partition_json ${text} NOT NULL CHECK (${json("partition_json")}),
      PRIMARY KEY (space_id, server_sequence, descriptor_hash, partition_json)
    )`,
    `CREATE TABLE effect_local_server_index_generations (
      generation ${integer} PRIMARY KEY CHECK (generation > 0)
    )`,
    `CREATE TABLE effect_local_server_offline_wake_acknowledgements (
      space_id ${text} NOT NULL,
      client_id ${text} NOT NULL,
      acknowledged_sequence ${integer} NOT NULL CHECK (acknowledged_sequence >= 0),
      PRIMARY KEY (space_id, client_id)
    )`,
    `CREATE TABLE effect_local_server_offline_wake_spaces (
      space_id ${text} PRIMARY KEY,
      high_water_sequence ${integer} NOT NULL CHECK (high_water_sequence > 0),
      expanded_sequence ${integer} NOT NULL DEFAULT 0
        CHECK (expanded_sequence >= 0 AND expanded_sequence <= high_water_sequence),
      membership_generation ${integer} NOT NULL DEFAULT 0 CHECK (membership_generation >= 0),
      attempt_count ${integer} NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
      next_attempt_at ${integer} NOT NULL CHECK (next_attempt_at >= 0),
      claim_token ${text},
      claimed_until ${integer},
      CHECK ((claim_token IS NULL AND claimed_until IS NULL) OR
        (claim_token IS NOT NULL AND claimed_until IS NOT NULL AND claimed_until >= 0))
    )`,
    `CREATE INDEX effect_local_server_offline_wake_spaces_due
      ON effect_local_server_offline_wake_spaces (next_attempt_at, space_id)
      WHERE high_water_sequence > expanded_sequence`,
    `CREATE TABLE effect_local_server_offline_wakes (
      space_id ${text} NOT NULL,
      client_id ${text} NOT NULL,
      wake_id ${text} NOT NULL,
      high_water_sequence ${integer} NOT NULL CHECK (high_water_sequence > 0),
      notified_sequence ${integer} NOT NULL DEFAULT 0
        CHECK (notified_sequence >= 0 AND notified_sequence <= high_water_sequence),
      membership_generation ${integer} NOT NULL CHECK (membership_generation > 0),
      attempt_count ${integer} NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
      next_attempt_at ${integer} NOT NULL CHECK (next_attempt_at >= 0),
      claim_token ${text},
      claimed_until ${integer},
      PRIMARY KEY (space_id, client_id),
      UNIQUE (wake_id),
      CHECK ((claim_token IS NULL AND claimed_until IS NULL) OR
        (claim_token IS NOT NULL AND claimed_until IS NOT NULL AND claimed_until >= 0))
    )`,
    `CREATE INDEX effect_local_server_offline_wakes_due
      ON effect_local_server_offline_wakes (next_attempt_at, space_id, client_id)
      WHERE high_water_sequence > notified_sequence`,
    `CREATE TABLE effect_local_server_watch_runtimes (
      runtime_id ${text} PRIMARY KEY,
      expires_at ${integer} NOT NULL CHECK (expires_at >= 0)
    )`,
    `CREATE INDEX effect_local_server_watch_runtimes_expiry
      ON effect_local_server_watch_runtimes (expires_at, runtime_id)`,
    `CREATE TABLE effect_local_server_watch_presence (
      space_id ${text} NOT NULL,
      client_id ${text} NOT NULL,
      watcher_id ${text} NOT NULL,
      runtime_id ${text} NOT NULL,
      PRIMARY KEY (space_id, client_id, watcher_id),
      UNIQUE (runtime_id, watcher_id)
    )`,
    `CREATE INDEX effect_local_server_watch_presence_active
      ON effect_local_server_watch_presence (space_id, client_id, runtime_id)`,
    `CREATE INDEX effect_local_server_watch_presence_runtime
      ON effect_local_server_watch_presence (runtime_id)`
  ]
}

const sqliteServerBaseline = makeMigration({
  id: 1,
  name: "server-baseline",
  statements: [
    ...serverTables({
      text: "TEXT",
      integer: "INTEGER",
      flag: "INTEGER",
      json: (column) => `json_valid(${column})`
    }),
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
        WHERE space_id = NEW.space_id; END`
  ]
})

export const serverCatalog = Object.freeze([sqliteServerBaseline])

const postgresBaseline = makeMigration({
  id: 1,
  name: "postgres-baseline",
  statements: [
    ...serverTables({
      text: "TEXT COLLATE \"C\"",
      integer: "BIGINT",
      flag: "SMALLINT",
      json: (column) => `${column} IS JSON`
    }),
    `CREATE FUNCTION effect_local_count_history() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'INSERT' THEN
          UPDATE effect_local_server_spaces SET retained_history_count = retained_history_count + 1
            WHERE space_id = NEW.space_id;
          UPDATE effect_local_server_space_counts SET history_count = history_count + 1
            WHERE space_id = NEW.space_id;
          RETURN NEW;
        END IF;
        UPDATE effect_local_server_spaces SET retained_history_count = retained_history_count - 1
          WHERE space_id = OLD.space_id;
        UPDATE effect_local_server_space_counts SET history_count = history_count - 1
          WHERE space_id = OLD.space_id;
        RETURN OLD;
      END
    $$`,
    `CREATE TRIGGER effect_local_count_history AFTER INSERT OR DELETE ON effect_local_authoritative_log
      FOR EACH ROW EXECUTE FUNCTION effect_local_count_history()`,
    `CREATE FUNCTION effect_local_count_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'INSERT' THEN
          UPDATE effect_local_server_spaces SET retained_receipt_count = retained_receipt_count + 1
            WHERE space_id = NEW.space_id;
          UPDATE effect_local_server_space_counts SET receipt_count = receipt_count + 1
            WHERE space_id = NEW.space_id;
          RETURN NEW;
        END IF;
        UPDATE effect_local_server_spaces SET retained_receipt_count = retained_receipt_count - 1
          WHERE space_id = OLD.space_id;
        UPDATE effect_local_server_space_counts SET receipt_count = receipt_count - 1
          WHERE space_id = OLD.space_id;
        RETURN OLD;
      END
    $$`,
    `CREATE TRIGGER effect_local_count_receipt AFTER INSERT OR DELETE ON effect_local_server_receipts
      FOR EACH ROW EXECUTE FUNCTION effect_local_count_receipt()`,
    `CREATE FUNCTION effect_local_server_entity_count() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'INSERT' THEN
          UPDATE effect_local_server_spaces SET entity_count = entity_count + 1,
            entity_bytes = entity_bytes + NEW.entity_bytes
            WHERE space_id = NEW.space_id AND active_schema_generation = NEW.generation;
          RETURN NEW;
        END IF;
        IF TG_OP = 'DELETE' THEN
          UPDATE effect_local_server_spaces SET entity_count = entity_count - 1,
            entity_bytes = entity_bytes - OLD.entity_bytes
            WHERE space_id = OLD.space_id AND active_schema_generation = OLD.generation;
          RETURN OLD;
        END IF;
        IF NEW.generation = OLD.generation AND NEW.space_id = OLD.space_id THEN
          UPDATE effect_local_server_spaces SET entity_bytes = entity_bytes + NEW.entity_bytes - OLD.entity_bytes
            WHERE space_id = NEW.space_id AND active_schema_generation = NEW.generation;
        END IF;
        RETURN NEW;
      END
    $$`,
    `CREATE TRIGGER effect_local_server_entity_count
      AFTER INSERT OR DELETE OR UPDATE OF entity_bytes ON effect_local_server_entities_data
      FOR EACH ROW EXECUTE FUNCTION effect_local_server_entity_count()`
  ]
})

export const serverPostgresCatalog = Object.freeze([postgresBaseline])

export const client = Effect.fnUntraced(function*(options: {
  readonly definition: Definition.Any
  readonly spaceId?: Identity.SpaceId
  readonly clientId?: Identity.ClientId | undefined
  readonly migration?: Options
}) {
  const sql = yield* SqlClient.SqlClient
  const lane = yield* ConnectionLane.ConnectionLane
  yield* lane.withStatement(sql.unsafe("PRAGMA foreign_keys = ON"))
  const pragma = yield* lane.withStatement(
    SqlSchema.findOne({
      Request: Schema.Void,
      Result: PragmaEnabledRow,
      execute: () => sql`PRAGMA foreign_keys`
    })(undefined)
  ).pipe(Effect.mapError((cause) => {
    if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
    return new ReplicaError.StorageCorrupt({ message: "SQLite foreign key state is unreadable", cause })
  }))
  if (pragma.foreign_keys !== 1) {
    return yield* new ReplicaError.StorageCorrupt({ message: "SQLite foreign keys could not be enabled" })
  }
  const metaExists = yield* lane.withStatement(
    SqlSchema.findOne({
      Request: Schema.Void,
      Result: CountRow,
      execute: () =>
        sql`SELECT COUNT(*) AS count FROM sqlite_master
        WHERE type = 'table' AND name = 'effect_local_client_meta'`
    })(undefined)
  ).pipe(Effect.mapError((cause) => {
    if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
    return new ReplicaError.StorageCorrupt({ message: "Client metadata catalog is unreadable", cause })
  }))
  if (metaExists.count !== 0) {
    const beforeMigration = yield* lane.withStatement(
      SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: ClientIdentityRow,
        execute: () => sql`SELECT client_id FROM effect_local_client_meta WHERE singleton = 1`
      })(undefined)
    ).pipe(Effect.mapError((cause) => {
      if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
      return new ReplicaError.StorageCorrupt({ message: "Client replica identity is corrupt", cause })
    }))
    if (
      options.clientId !== undefined && Option.isSome(beforeMigration) &&
      beforeMigration.value.client_id !== options.clientId
    ) {
      return yield* new ReplicaError.ReplicaIdentityMismatch({
        expectedClientId: options.clientId,
        actualClientId: beforeMigration.value.client_id
      })
    }
  }
  yield* runCatalogWith(lane, "Client", clientCatalog, options.migration ?? {})
  yield* lane.withStatement(sql`INSERT INTO effect_local_client_meta
    (singleton, client_id) VALUES (1, ${options.clientId ?? SqliteIdentifier.random(sql, "cli")})
    ON CONFLICT (singleton) DO NOTHING`)
  const stored = yield* lane.withStatement(
    SqlSchema.findOne({
      Request: Schema.Void,
      Result: ClientIdentityRow,
      execute: () => sql`SELECT client_id FROM effect_local_client_meta WHERE singleton = 1`
    })(undefined)
  ).pipe(
    Effect.catchTags({
      SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
      SchemaError: (cause) =>
        Effect.fail(new ReplicaError.StorageCorrupt({ message: "Client replica identity is corrupt", cause })),
      NoSuchElementError: (cause) =>
        Effect.fail(new ReplicaError.StorageCorrupt({ message: "Client replica identity is corrupt", cause }))
    })
  )
  if (options.clientId !== undefined && stored.client_id !== options.clientId) {
    return yield* new ReplicaError.ReplicaIdentityMismatch({
      expectedClientId: options.clientId,
      actualClientId: stored.client_id
    })
  }
  if (options.spaceId !== undefined) {
    yield* lane.withStatement(sql`INSERT INTO effect_local_client_spaces
        (space_id, membership_incarnation, definition_hash, schema_version, schema_hash, schema_generation,
          active_schema_generation, active_projection_generation, projection_schema_generation,
          next_local_sequence, server_cursor, visible_revision, requested_generation, completed_generation,
          installed_snapshot_sequence, installed_snapshot_terminal_sequence)
        VALUES (${options.spaceId},
          ${SqliteIdentifier.random(sql, "inc")}, ${options.definition.hash},
          ${options.definition.schemaIdentity.version}, ${options.definition.schemaIdentity.hash}, 0, 0, 0, 0,
          1, 0, 0, 0, 0, 0, 0)
        ON CONFLICT (space_id) DO NOTHING`)
  }
  return stored.client_id
}, Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))

const serverCatalogFor = (dialect: Dialect.Dialect) => {
  if (dialect.name === "pg") return serverPostgresCatalog
  return serverCatalog
}

const planServer = Effect.fn("Migrations.planServer")(
  function*(sql: SqlClient.SqlClient, dialect: Dialect.Dialect) {
    const migrations = serverCatalogFor(dialect)
    const ledgerExists = yield* dialect.tableExists("effect_local_server_migrations")
    if (!ledgerExists) return migrations
    const applied = yield* readServerLedger(sql)(undefined)
    const mismatch = compareLedger("Server", migrations, applied)
    if (mismatch !== undefined) return yield* mismatch
    return migrations.slice(applied.length)
  },
  Effect.catchTags({
    SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
    SchemaError: (cause) =>
      Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server migration ledger is corrupt", cause }))
  })
)

export const server = Effect.fnUntraced(function*(options: ServerOptions = {}) {
  const sql = yield* SqlClient.SqlClient
  const dialect = yield* Dialect.make(sql)
  if (options.mode !== "verify") return yield* runCatalog("Server", serverCatalogFor(dialect), options)
  yield* retryPolicy(options)
  const pending = yield* planServer(sql, dialect)
  if (pending.length === 0) return yield* Effect.void
  return yield* new ReplicaError.StorageMigrationPending({
    catalog: "Server",
    message: `Server database is behind: pending ${
      pending.map((migration) => `${migration.id}:${migration.name}`).join(", ")
    }. Apply the script from Migrations.renderServer`
  })
})

export const renderServer = Effect.fn("Migrations.renderServer")(function*(options: RenderServerOptions) {
  const sql = yield* SqlClient.SqlClient
  const dialect = yield* Dialect.make(sql)
  const pending = yield* planServer(sql, dialect)
  const index = yield* ServerIndex.plan(sql, dialect, options.definition)
  if (pending.length === 0 && index.missing.length === 0 && index.orphans.length === 0) {
    return Option.none<string>()
  }
  for (const descriptor of index.missing) {
    for (const value of [descriptor.model.name, descriptor.indexName]) {
      if (value.includes("\u0000") || Dialect.hasUnpairedSurrogate(value)) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "definition",
          message: `Index ${descriptor.model.name}.${descriptor.indexName} cannot be written as SQL text`
        })
      }
    }
  }
  const statements = [...dialect.scriptPrologue]
  if (pending.length > 0) statements.push(ledger("effect_local_server_migrations", dialect.text))
  for (const migration of pending) {
    statements.push(
      `INSERT INTO effect_local_server_migrations (id, name, checksum) VALUES (${migration.id}, ${
        dialect.literal(migration.name)
      }, ${dialect.literal(migration.checksum)})`
    )
  }
  for (const migration of pending) statements.push(...migration.statements)
  if (index.missing.length > 0 || index.orphans.length > 0) {
    statements.push(`INSERT INTO effect_local_server_index_generations (generation) VALUES (${index.generation + 1})`)
  }
  for (const descriptor of index.missing) {
    statements.push(
      descriptor.tableDdl,
      descriptor.scanIndexDdl,
      `INSERT INTO effect_local_server_index_catalog (model, index_name, descriptor_hash, table_name, scan_index_name)
VALUES (${dialect.literal(descriptor.model.name)}, ${dialect.literal(descriptor.indexName)}, ${
        dialect.literal(descriptor.hash)
      }, ${dialect.literal(descriptor.tableName)}, ${dialect.literal(descriptor.scanIndexName)})
ON CONFLICT (model, index_name, descriptor_hash) DO NOTHING`
    )
  }
  for (const row of index.orphans) {
    statements.push(
      `DROP INDEX IF EXISTS ${row.scan_index_name}`,
      `DROP TABLE IF EXISTS ${row.table_name}`,
      `DELETE FROM effect_local_server_index_state WHERE descriptor_hash = ${dialect.literal(row.descriptor_hash)}`,
      `DELETE FROM effect_local_server_index_catalog WHERE descriptor_hash = ${dialect.literal(row.descriptor_hash)}`
    )
  }
  statements.push("COMMIT")
  return Option.some(statements.map((statement) => `${statement};\n`).join("\n"))
})
