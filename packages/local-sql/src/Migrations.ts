import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as StorageUnavailable from "./internal/storageUnavailable.js"

export type Catalog = "Client" | "Server"

export interface Migration {
  readonly id: number
  readonly name: string
  readonly checksum: Identity.SchemaHash
  readonly statements: ReadonlyArray<string>
  readonly effect?: ((sql: SqlClient.SqlClient) => Effect.Effect<void, never, never>) | undefined
}

const stableName = /^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$/

export const makeMigration = (options: {
  readonly id: number
  readonly name: string
  readonly statements: ReadonlyArray<string>
  readonly effect?: {
    readonly id: string
    readonly run: (sql: SqlClient.SqlClient) => Effect.Effect<void, never, never>
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
  return Object.freeze({
    id: options.id,
    name: options.name,
    checksum: Identity.SchemaHash.make(Canonical.hash({
      format: 1,
      id: options.id,
      name: options.name,
      statements,
      effect: options.effect?.id ?? null
    })),
    statements,
    ...(options.effect === undefined ? {} : { effect: options.effect.run })
  })
}

const MigrationRow = Schema.Struct({
  id: Schema.Int.check(Schema.isGreaterThan(0)),
  name: Schema.String,
  checksum: Identity.SchemaHash
})

const mismatch = (catalog: Catalog, message: string) =>
  new ReplicaError.StorageMigrationMismatch({ catalog, message })

const validateCatalog = (
  catalog: Catalog,
  migrations: ReadonlyArray<Migration>
): ReplicaError.StorageMigrationMismatch | undefined => {
  const names = new Set<string>()
  for (let index = 0; index < migrations.length; index++) {
    const migration = migrations[index]!
    if (migration.id !== index + 1) {
      return mismatch(catalog, `${catalog} migration ids must be contiguous from 1. Expected ${index + 1}, got ${migration.id}`)
    }
    if (names.has(migration.name)) return mismatch(catalog, `Duplicate migration name: ${migration.name}`)
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

export const runCatalog = (
  catalog: Catalog,
  migrations: ReadonlyArray<Migration>
): Effect.Effect<
  void,
  ReplicaError.StorageMigrationMismatch | ReplicaError.StorageUnavailable | ReplicaError.StorageCorrupt,
  SqlClient.SqlClient
> =>
  Effect.gen(function*() {
    const invalid = validateCatalog(catalog, migrations)
    if (invalid !== undefined) return yield* invalid
    const sql = yield* SqlClient.SqlClient
    yield* sql.unsafe(catalog === "Client" ? clientLedger : serverLedger)
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
    yield* sql.withTransaction(Effect.gen(function*() {
      const applied = yield* (catalog === "Client" ? readClient(undefined) : readServer(undefined)).pipe(
        Effect.mapError((cause) => SqlError.isSqlError(cause)
          ? StorageUnavailable.make(cause)
          : new ReplicaError.StorageCorrupt({ message: `${catalog} migration ledger is corrupt`, cause }))
      )
      if (applied.length > migrations.length) {
        return yield* mismatch(catalog, `${catalog} catalog deleted ${applied.length - migrations.length} applied migration(s)`)
      }
      for (let index = 0; index < applied.length; index++) {
        const stored = applied[index]!
        const expected = migrations[index]!
        if (stored.id !== expected.id || stored.name !== expected.name || stored.checksum !== expected.checksum) {
          return yield* mismatch(
            catalog,
            `Applied migration ${stored.id}:${stored.name}:${stored.checksum} does not match ${expected.id}:${expected.name}:${expected.checksum}`
          )
        }
      }
      for (let index = applied.length; index < migrations.length; index++) {
        const migration = migrations[index]!
        yield* Effect.forEach(migration.statements, (statement) => sql.unsafe(statement), { discard: true })
        if (migration.effect !== undefined) yield* migration.effect(sql)
        if (catalog === "Client") {
          yield* sql`INSERT INTO effect_local_client_migrations (id, name, checksum)
          VALUES (${migration.id}, ${migration.name}, ${migration.checksum})`
        } else {
          yield* sql`INSERT INTO effect_local_server_migrations (id, name, checksum)
          VALUES (${migration.id}, ${migration.name}, ${migration.checksum})`
        }
      }
    }))
  }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

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

export const clientCatalog = Object.freeze([clientV1, clientV2, clientV3, clientV4])
export const serverCatalog = Object.freeze([serverV1, serverV2, serverV3, serverV4])

export const client = (options: {
  readonly definition: Definition.Any
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
}) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* runCatalog("Client", clientCatalog)
    yield* sql`INSERT INTO effect_local_client_meta
    (singleton, space_id, client_id, definition_hash, next_local_sequence, server_cursor, visible_revision,
      requested_generation, completed_generation)
    VALUES (1, ${options.spaceId}, ${options.clientId}, ${options.definition.hash}, 1, 0, 0, 0, 0)
    ON CONFLICT (singleton) DO NOTHING`
  }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

export const server = runCatalog("Server", serverCatalog)
