import { NodeServices } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import type * as Layer from "effect/Layer"
import * as ChildProcess from "effect/process/ChildProcess"
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import type * as SqlClient from "effect/sql/SqlClient"
import type * as SqlError from "effect/sql/SqlError"
import * as Stream from "effect/Stream"
import { DatabaseSync } from "node:sqlite"
import { inject } from "vitest"
import { type Dialect, postgresDatabaseUrl } from "./ServerDatabase.js"

class ScriptRejected extends Schema.TaggedError<ScriptRejected>(
  "@lucas-barake/effect-local-sql/test/ScriptRejected"
)("ScriptRejected", { message: Schema.String }) {}

export interface ManualDatabase {
  readonly dialect: Dialect
  readonly layer: () => Layer.Layer<SqlClient.SqlClient, SqlError.SqlError>
  readonly apply: (script: string) => Effect.Effect<void, ScriptRejected>
}

const sqliteDatabase = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  const filename = `${directory}/server.sqlite`
  return {
    dialect: "sqlite",
    layer: () => SqliteClient.layer({ filename, disableWAL: true }),
    apply: (script) =>
      Effect.acquireUseRelease(
        Effect.sync(() => new DatabaseSync(filename)),
        (database) =>
          Effect.try({
            try: () => database.exec(script),
            catch: (cause) => new ScriptRejected({ message: String(cause) })
          }).pipe(Effect.tapError(() =>
            Effect.sync(() => {
              if (database.isTransaction) database.exec("ROLLBACK")
            })
          )),
        (database) => Effect.sync(() => database.close())
      )
  } satisfies ManualDatabase
})

const postgresDatabase = Effect.gen(function*() {
  const { name, url } = yield* postgresDatabaseUrl
  const internal = new URL(Redacted.value(url))
  internal.host = "localhost:5432"
  internal.pathname = `/${name}`
  return {
    dialect: "pg",
    layer: () => PgClient.layer({ url, maxConnections: 4 }),
    apply: (script) =>
      ChildProcessSpawner.ChildProcessSpawner.use((spawner) =>
        spawner.exitCode(
          ChildProcess.make(
            "docker",
            [
              "exec",
              "-i",
              inject("postgresContainerId"),
              "psql",
              internal.toString(),
              "--quiet",
              "--no-psqlrc",
              "--set",
              "ON_ERROR_STOP=1",
              "--file",
              "-"
            ],
            { stdin: Stream.make(new TextEncoder().encode(script)), stdout: "ignore", stderr: "ignore" }
          )
        )
      ).pipe(
        Effect.provide(NodeServices.layer),
        Effect.mapError((cause) => new ScriptRejected({ message: String(cause) })),
        Effect.flatMap((code) => {
          if (code === 0) return Effect.void
          return Effect.fail(new ScriptRejected({ message: `psql exited with ${code}` }))
        })
      )
  } satisfies ManualDatabase
})

export const manualDatabases = [
  { dialect: "sqlite", make: sqliteDatabase },
  { dialect: "pg", make: postgresDatabase }
] as const
