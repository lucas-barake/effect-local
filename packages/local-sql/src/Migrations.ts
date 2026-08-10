import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"

import * as StorageUnavailable from "./internal/storageUnavailable.js"

export const client = (options: {
  readonly definition: Definition.Any
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
}) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* sql`CREATE TABLE IF NOT EXISTS effect_local_client_meta (
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
  )`
    yield* sql`CREATE TABLE IF NOT EXISTS effect_local_pending (
    mutation_id TEXT PRIMARY KEY,
    local_sequence INTEGER NOT NULL UNIQUE,
    basis INTEGER NOT NULL,
    name TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    optimistic_result_json TEXT NOT NULL,
    changes_json TEXT NOT NULL
  )`
    yield* sql`CREATE TABLE IF NOT EXISTS effect_local_receipts (
    mutation_id TEXT PRIMARY KEY,
    local_sequence INTEGER NOT NULL UNIQUE,
    receipt_json TEXT NOT NULL
  )`
    yield* sql`CREATE TABLE IF NOT EXISTS effect_local_server_log (
    server_sequence INTEGER PRIMARY KEY,
    mutation_id TEXT NOT NULL UNIQUE,
    entry_json TEXT NOT NULL
  )`
    yield* sql`CREATE TABLE IF NOT EXISTS effect_local_canonical_entities (
    model TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY (model, entity_key)
  )`
    yield* sql`CREATE TABLE IF NOT EXISTS effect_local_visible_entities (
    model TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY (model, entity_key)
  )`
    yield* sql`INSERT INTO effect_local_client_meta
    (singleton, space_id, client_id, definition_hash, next_local_sequence, server_cursor, visible_revision,
      requested_generation, completed_generation)
    VALUES (1, ${options.spaceId}, ${options.clientId}, ${options.definition.hash}, 1, 0, 0, 0, 0)
    ON CONFLICT (singleton) DO NOTHING`
  }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

export const server = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE IF NOT EXISTS effect_local_server_spaces (
    space_id TEXT PRIMARY KEY,
    definition_hash TEXT NOT NULL,
    next_server_sequence INTEGER NOT NULL
  )`
  yield* sql`CREATE TABLE IF NOT EXISTS effect_local_server_clients (
    space_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    last_local_sequence INTEGER NOT NULL,
    PRIMARY KEY (space_id, client_id)
  )`
  yield* sql`CREATE TABLE IF NOT EXISTS effect_local_server_receipts (
    space_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    local_sequence INTEGER NOT NULL,
    mutation_id TEXT NOT NULL,
    digest TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    PRIMARY KEY (space_id, client_id, local_sequence),
    UNIQUE (space_id, mutation_id)
  )`
  yield* sql`CREATE TABLE IF NOT EXISTS effect_local_authoritative_log (
    space_id TEXT NOT NULL,
    server_sequence INTEGER NOT NULL,
    mutation_id TEXT NOT NULL,
    entry_bytes INTEGER NOT NULL CHECK (entry_bytes > 0),
    entry_json TEXT NOT NULL,
    PRIMARY KEY (space_id, server_sequence),
    UNIQUE (space_id, mutation_id)
  )`
  yield* sql`CREATE TABLE IF NOT EXISTS effect_local_server_entities (
    space_id TEXT NOT NULL,
    model TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY (space_id, model, entity_key)
  )`
}).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))
