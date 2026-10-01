import { NodeCrypto } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Result from "effect/Result"
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlError from "effect/sql/SqlError"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"
import { postgresDatabaseUrl } from "./fixtures/ServerDatabase.js"
import { gateStatements, lockWaiters } from "./fixtures/SqlGate.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000a01")
const readerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000a02")

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const buildStore = (sql: SqlClient.SqlClient) =>
  ServerStore.layer({
    definition: Domain.definition,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 },
    authorizeAccess: () => Effect.void,
    authorizeMutation: () => Effect.void,
    authorizeRead: () => Effect.void
  }).pipe(
    Layer.provide(layerRuntime),
    Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
    Layer.provide(NodeCrypto.layer),
    Layer.build,
    Effect.map(Context.get(ServerStore.ServerStore))
  )

const windowedPull = Protocol.PullRequest.make({
  spaceId,
  clientId: readerId,
  schema: Domain.definition.schemaIdentity,
  scope: Protocol.ReplicationScope.make({
    models: [],
    windows: [
      Protocol.ReplicationWindow.make({ model: Domain.Todo.name, index: "byCount", count: 1 }),
      Protocol.ReplicationWindow.make({ model: Domain.Message.name, index: "byChat", count: 1 })
    ]
  }),
  scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
  cursor: null,
  limit: 100
})

const outcomeTag = <A, E extends { readonly _tag: string },>(outcome: Result.Result<A, E>) => {
  if (Result.isSuccess(outcome)) return "Succeeded"
  const failure: { readonly _tag: string; readonly cause?: unknown } = outcome.failure
  if (SqlError.isSqlError(failure.cause)) return `${failure._tag}(${failure.cause.reason._tag})`
  return failure._tag
}

const provideReactivity = Effect.provide(Reactivity.layer)

describe("postgres server index boot", () => {
  it.effect(
    "a runner booting while another runner backfills window indexes both complete",
    Effect.fnUntraced(
      function*() {
        const { url } = yield* postgresDatabaseUrl
        const observer = yield* PgClient.makeClient({ url })
        const serving = yield* PgClient.make({ url, maxConnections: 4 })
        const gate = yield* gateStatements(serving, (statement) => {
          if (statement.startsWith("DELETE FROM ? WHERE space_id = ? AND schema_generation <> ?")) return ["before"]
          return []
        })
        const store = yield* buildStore(gate.sql)
        const pull = yield* store.pull(windowedPull).pipe(Effect.result, Effect.forkChild({ startImmediately: true }))
        const first = yield* Queue.take(gate.pauses)
        yield* Deferred.succeed(first.release, undefined)
        const second = yield* Queue.take(gate.pauses)

        const booting = yield* PgClient.make({ url, maxConnections: 4 })
        const boot = yield* buildStore(booting).pipe(Effect.result, Effect.forkChild({ startImmediately: true }))
        yield* lockWaiters(observer).pipe(
          Effect.repeat({ until: (waiters) => waiters > 0 || boot.pollUnsafe() !== undefined })
        )
        yield* Deferred.succeed(second.release, undefined)

        const pulled = yield* Fiber.join(pull)
        const booted = yield* Fiber.join(boot)
        assert.deepStrictEqual([outcomeTag(pulled), outcomeTag(booted)], ["Succeeded", "Succeeded"])
      },
      Effect.scoped,
      provideReactivity
    ),
    60_000
  )
})
