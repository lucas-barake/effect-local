import { NodeCrypto } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Redacted from "effect/Redacted"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { inject } from "vitest"

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
