import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as SecondaryIndex from "@lucas-barake/effect-local/SecondaryIndex"
import * as Effect from "effect/Effect"
import type * as SqlClient from "effect/sql/SqlClient"
import type * as SqlError from "effect/sql/SqlError"
import type * as Statement from "effect/sql/Statement"

interface JsonField {
  readonly name: string
  readonly affinity: SecondaryIndex.ComponentInput["affinity"]
}

interface PresenceKey {
  readonly spaceId: Identity.SpaceId
  readonly clientId: Identity.ClientId
}

const unpairedSurrogate = /[\ud800-\udbff](?![\udc00-\udfff])|(?<![\ud800-\udbff])[\udc00-\udfff]/g
export interface Dialect {
  readonly name: "sqlite" | "pg"
  readonly text: string
  readonly integer: string
  readonly tableOptions: string
  readonly indexColumn: (affinity: SecondaryIndex.ComponentInput["affinity"]) => string
  readonly encodeText: (value: string) => string
  readonly decodeText: (value: string) => string
  readonly lockSchema: Effect.Effect<void, SqlError.SqlError>
  readonly lockPresences: (keys: ReadonlyArray<PresenceKey>) => Effect.Effect<void, SqlError.SqlError>
  readonly beginSnapshotRead: Effect.Effect<void, SqlError.SqlError>
  readonly forNoKeyUpdate: Statement.Fragment
  readonly skipLocked: Statement.Fragment
  readonly greatest: (left: Statement.Fragment, right: Statement.Fragment) => Statement.Fragment
  readonly offset: (count: number) => Statement.Fragment
  readonly byteLength: (column: string) => Statement.Fragment
  readonly jsonArrayText: (columns: ReadonlyArray<string>) => Statement.Fragment
  readonly jsonRecords: (json: string, alias: string, fields: ReadonlyArray<JsonField>) => Statement.Fragment
}

const schemaLockClass = 0x656c6f63
const presenceLockClass = 0x656c7072

const presenceLockKey = (key: PresenceKey) => {
  const text = Canonical.stringify([key.spaceId, key.clientId])
  let hash = 0x811c9dc5
  for (let index = 0; index < text.length; index++) {
    hash = Math.imul(hash ^ text.charCodeAt(index), 0x01000193)
  }
  return hash | 0
}

const decodeEscapedText = (value: string) => {
  let decoded = ""
  for (let index = 0; index < value.length; index++) {
    const character = value[index]
    if (character !== "\u0001") {
      decoded += character
      continue
    }
    index += 1
    if (value[index] === "\u0001") decoded += "\u0000"
    else decoded += "\u0001"
  }
  return decoded
}

const sqlite = (sql: SqlClient.SqlClient): Dialect => ({
  name: "sqlite",
  text: "TEXT",
  integer: "INTEGER",
  tableOptions: " WITHOUT ROWID",
  indexColumn: (affinity) => {
    if (affinity === "text") return "TEXT"
    if (affinity === "real") return "REAL"
    return "INTEGER"
  },
  encodeText: (value) => value,
  decodeText: (value) => value,
  lockSchema: Effect.void,
  lockPresences: () => Effect.void,
  beginSnapshotRead: Effect.void,
  forNoKeyUpdate: sql.literal(""),
  skipLocked: sql.literal(""),
  greatest: (left, right) => sql`MAX(${left}, ${right})`,
  offset: (count) => sql`LIMIT -1 OFFSET ${count}`,
  byteLength: (column) => sql.literal(`length(CAST(${column} AS BLOB))`),
  jsonArrayText: (columns) => sql.literal(`json_array(${columns.join(", ")})`),
  jsonRecords: (json, alias, fields) =>
    sql`(SELECT ${
      sql.literal(fields.map((field) => `json_extract(value, '$.${field.name}') AS ${field.name}`).join(", "))
    } FROM json_each(${json})) AS ${sql.literal(alias)}`
})

const postgresType = (affinity: SecondaryIndex.ComponentInput["affinity"]) => {
  if (affinity === "text") return "TEXT COLLATE \"C\""
  if (affinity === "real") return "DOUBLE PRECISION"
  return "BIGINT"
}

const postgres = (sql: SqlClient.SqlClient): Dialect => ({
  name: "pg",
  text: "TEXT COLLATE \"C\"",
  integer: "BIGINT",
  tableOptions: "",
  indexColumn: postgresType,
  encodeText: (value) =>
    value.replaceAll(unpairedSurrogate, "\ufffd").replaceAll("\u0001", "\u0001\u0002").replaceAll(
      "\u0000",
      "\u0001\u0001"
    ),
  decodeText: decodeEscapedText,
  lockSchema: sql`SELECT pg_advisory_xact_lock(${schemaLockClass}, 0)`.pipe(Effect.asVoid),
  lockPresences: (keys) =>
    Effect.forEach(
      [...new Set(keys.map(presenceLockKey))].toSorted((left, right) => left - right),
      (key) => sql`SELECT pg_advisory_xact_lock(${presenceLockClass}, ${key})`,
      { discard: true }
    ),
  beginSnapshotRead: sql`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`.pipe(Effect.asVoid),
  forNoKeyUpdate: sql.literal("FOR NO KEY UPDATE"),
  skipLocked: sql.literal("FOR UPDATE SKIP LOCKED"),
  greatest: (left, right) => sql`GREATEST(${left}, ${right})`,
  offset: (count) => sql`OFFSET ${count}`,
  byteLength: (column) => sql.literal(`octet_length(${column})`),
  jsonArrayText: (columns) => sql.literal(`json_build_array(${columns.join(", ")})::text`),
  jsonRecords: (json, alias, fields) =>
    sql`json_to_recordset(${json}::json) AS ${
      sql.literal(`${alias}(${fields.map((field) => `${field.name} ${postgresType(field.affinity)}`).join(", ")})`)
    }`
})

export const make = (sql: SqlClient.SqlClient): Effect.Effect<Dialect, ReplicaError.InvalidConfiguration> =>
  sql.onDialectOrElse({
    sqlite: () => Effect.succeed(sqlite(sql)),
    pg: () => Effect.succeed(postgres(sql)),
    orElse: () =>
      Effect.fail(
        new ReplicaError.InvalidConfiguration({
          option: "sql",
          message: "Effect Local server storage supports only the sqlite and pg SQL dialects"
        })
      )
  })
