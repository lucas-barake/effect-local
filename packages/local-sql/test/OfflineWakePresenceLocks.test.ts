import { NodeCrypto } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import { PostgreSqlContainer } from "@testcontainers/postgresql"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Logger from "effect/Logger"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlSchema from "effect/sql/SqlSchema"
import * as TestClock from "effect/testing/TestClock"
import * as OfflineWakeRuntime from "../src/internal/offlineWake.js"
import * as Rows from "../src/internal/rows.js"
import * as Migrations from "../src/Migrations.js"
import type * as OfflineWake from "../src/OfflineWake.js"
import { gateStatements } from "./fixtures/SqlGate.js"

const watcherCount = 2_000

const options: OfflineWake.Options = {
  recipients: () => Effect.succeed([]),
  deliver: () => Effect.succeed("Delivered"),
  coalescingWindow: "1 millis",
  pollInterval: "1 hour",
  retryDelay: "1 second",
  maximumRetryDelay: "1 minute",
  claimLeaseDuration: "30 seconds",
  hookTimeout: "10 seconds",
  presenceLeaseDuration: "30 seconds",
  presenceHeartbeatInterval: "20 seconds",
  claimBatchSize: 8,
  maximumConcurrentRecipientResolutions: 1,
  maximumConcurrentDeliveries: 1,
  maximumRecipientsPerSpace: 8
}

const suffix = (index: number) => String(index).padStart(12, "0")

const CountRow = Schema.Struct({ count: Rows.integer(Schema.Int) })

const provideServices = Effect.provide([Reactivity.layer, NodeCrypto.layer])

describe("offline wake presence heartbeat on postgres", () => {
  it.effect(
    "restores durable presence for every local watcher after the runtime lease lapsed",
    Effect.fnUntraced(
      function*() {
        const container = yield* Effect.acquireRelease(
          Effect.promise(() =>
            new PostgreSqlContainer("postgres:16-alpine").withCommand([
              "postgres",
              "-c",
              "max_connections=20",
              "-c",
              "max_locks_per_transaction=10",
              "-c",
              "fsync=off"
            ]).start()
          ),
          (started) => Effect.promise(() => started.stop())
        )
        const pool = yield* PgClient.make({ url: Redacted.make(container.getConnectionUri()), maxConnections: 4 })
        yield* Migrations.server({ retryDelay: "1 millis", maximumAttempts: 8 }).pipe(
          Effect.provideService(SqlClient.SqlClient, pool)
        )
        const gate = yield* gateStatements(pool, (statement) => {
          if (statement.includes("local_presence AS MATERIALIZED")) return ["after"]
          return []
        })
        const heartbeatFailed = yield* Deferred.make<string>()
        const logger = Logger.make<unknown, void>((event) => {
          const line = Logger.formatJson.log(event)
          if (line.includes("Offline wake presence heartbeat failed")) {
            Deferred.doneUnsafe(heartbeatFailed, Effect.succeed(line))
          }
        })
        const owner = yield* Scope.make()
        const crypto = yield* Crypto.Crypto
        const runtime = yield* OfflineWakeRuntime.make(options, Context.empty()).pipe(
          Effect.provideService(SqlClient.SqlClient, gate.sql),
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.provide(Logger.layer([logger])),
          Scope.provide(owner)
        )
        const watchers = yield* Scope.make()
        yield* Effect.forEach(
          Array.from({ length: watcherCount }, (_, index) => index),
          (index) =>
            runtime.registerWatch(
              Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${suffix(index)}`),
              Identity.ClientId.make(`cli_00000000-0000-4000-8000-${suffix(index)}`)
            ).pipe(Scope.provide(watchers)),
          { discard: true }
        )
        yield* pool`DELETE FROM effect_local_server_watch_presence`
        yield* pool`DELETE FROM effect_local_server_watch_runtimes`

        yield* TestClock.adjust("20 seconds")
        const outcome = yield* Effect.raceFirst(
          Deferred.await(heartbeatFailed),
          Queue.take(gate.pauses).pipe(
            Effect.tap((pause) => Deferred.succeed(pause.release, undefined)),
            Effect.as("reconciled")
          )
        )
        assert.strictEqual(outcome, "reconciled")
        const restored = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: CountRow,
          execute: () => pool`SELECT COUNT(*) AS count FROM effect_local_server_watch_presence`
        })(undefined)
        assert.strictEqual(restored.count, watcherCount)
      },
      Effect.scoped,
      provideServices
    ),
    180_000
  )
})
