import { PgClient } from "@effect/sql-pg"
import * as Effect from "effect/Effect"
import * as Redacted from "effect/Redacted"
import * as SqlClient from "effect/unstable/sql/SqlClient"

/**
 * Run by the Playwright relay command before the server boots, so every suite run starts from an
 * empty relay state. The seed archive and the inbox must always be dropped together: replicas
 * provisioned from a regenerated archive would receive retained old-genesis traffic as a
 * shadowing register conflict.
 */

const databaseUrl = process.env.DATABASE_URL
if (databaseUrl === undefined) {
  throw new Error("reset-test-db requires DATABASE_URL")
}

const program = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`DROP TABLE IF EXISTS chat_seed`
  yield* sql`DROP TABLE IF EXISTS effect_local_relay_inbox`
  yield* sql`DROP TABLE IF EXISTS effect_local_relay_inbox_migrations`
}).pipe(
  Effect.provide(PgClient.layer({ url: Redacted.make(databaseUrl) })),
  Effect.scoped
)

Effect.runPromise(program).then(
  () => process.exit(0),
  (error) => {
    // eslint-disable-next-line no-console
    console.error(error)
    process.exit(1)
  }
)
