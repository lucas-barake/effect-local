import { NodeCrypto } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import type * as PlatformError from "effect/PlatformError"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Redacted from "effect/Redacted"
import type * as Scope from "effect/Scope"
import * as SqlClient from "effect/sql/SqlClient"
import type * as SqlError from "effect/sql/SqlError"
import { inject } from "vitest"

export type Dialect = "sqlite" | "pg"

export interface SharedDatabase {
  readonly layer: () => Layer.Layer<SqlClient.SqlClient, SqlError.SqlError>
  readonly client: Effect.Effect<SqlClient.SqlClient, SqlError.SqlError, Scope.Scope>
}

export interface ServerDatabase {
  readonly dialect: Dialect
  readonly layer: () => Layer.Layer<SqlClient.SqlClient, SqlError.SqlError>
  readonly client: Effect.Effect<SqlClient.SqlClient, SqlError.SqlError, Scope.Scope>
  readonly shared: Effect.Effect<
    SharedDatabase,
    SqlError.SqlError | PlatformError.PlatformError,
    Scope.Scope | FileSystem.FileSystem
  >
}

const adminStatement = (statement: string) =>
  PgClient.makeClient({ url: Redacted.make(inject("postgresUrl")) }).pipe(
    Effect.flatMap((admin) => admin.unsafe(statement)),
    Effect.scoped,
    Effect.provide(Reactivity.layer)
  )

export const postgresDatabaseUrl = Effect.acquireRelease(
  Effect.gen(function*() {
    const identifier = yield* Crypto.Crypto.pipe(
      Effect.flatMap((crypto) => crypto.randomUUIDv4),
      Effect.provide(NodeCrypto.layer),
      Effect.catchTag("PlatformError", (error) => Effect.die(error))
    )
    const name = `effect_local_${identifier.replaceAll("-", "")}`
    yield* adminStatement(
      `CREATE DATABASE ${name} LOCALE_PROVIDER icu ICU_LOCALE 'en-US' LOCALE 'en_US.utf8' TEMPLATE template0`
    )
    const url = new URL(inject("postgresUrl"))
    url.pathname = `/${name}`
    return { name, url: Redacted.make(url.toString()) }
  }),
  ({ name }) =>
    adminStatement(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`).pipe(
      Effect.catchTag("SqlError", (error) => Effect.die(error))
    )
)

export const postgresLayer = (): Layer.Layer<SqlClient.SqlClient, SqlError.SqlError> =>
  Layer.unwrap(
    Effect.map(postgresDatabaseUrl, ({ url }) => PgClient.layer({ url, maxConnections: 8 }))
  )

export const sqliteLayer = (): Layer.Layer<SqlClient.SqlClient, SqlError.SqlError> =>
  SqliteClient.layer({ filename: ":memory:", disableWAL: true })

const sharedSqlite = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  const client = yield* SqliteClient.make({ filename: `${directory}/server.sqlite`, disableWAL: true }).pipe(
    Effect.provide(Reactivity.layer)
  )
  return {
    layer: () => Layer.succeed(SqlClient.SqlClient, client),
    client: Effect.succeed(client)
  } satisfies SharedDatabase
})

const sharedPostgres = Effect.map(postgresDatabaseUrl, ({ url }) => ({
  layer: () => PgClient.layer({ url, maxConnections: 8 }),
  client: PgClient.make({ url, maxConnections: 2 }).pipe(Effect.provide(Reactivity.layer))
} satisfies SharedDatabase))

const sqliteClient = SqliteClient.make({ filename: ":memory:", disableWAL: true }).pipe(
  Effect.provide(Reactivity.layer)
)

const postgresClient = Effect.flatMap(postgresDatabaseUrl, ({ url }) => PgClient.make({ url, maxConnections: 8 })).pipe(
  Effect.provide(Reactivity.layer)
)

export const serverDatabases: ReadonlyArray<ServerDatabase> = [
  { dialect: "sqlite", layer: sqliteLayer, client: sqliteClient, shared: sharedSqlite },
  { dialect: "pg", layer: postgresLayer, client: postgresClient, shared: sharedPostgres }
]

export const installSpaceUpdateProbe = Effect.fnUntraced(function*(sql: SqlClient.SqlClient, dialect: Dialect) {
  yield* sql`CREATE TABLE space_update_probe (count INTEGER NOT NULL)`
  if (dialect === "pg") {
    yield* sql.unsafe(`CREATE FUNCTION count_space_updates() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN INSERT INTO space_update_probe (count) VALUES (1); RETURN NEW; END $$`)
    yield* sql`CREATE TRIGGER count_space_updates AFTER UPDATE ON effect_local_server_spaces
      FOR EACH ROW EXECUTE FUNCTION count_space_updates()`
    return
  }
  yield* sql`CREATE TRIGGER count_space_updates AFTER UPDATE ON effect_local_server_spaces
    BEGIN INSERT INTO space_update_probe (count) VALUES (1); END`
})
