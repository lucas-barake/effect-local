import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as SqlClient from "effect/sql/SqlClient"
import * as ConnectionLane from "../src/ConnectionLane.js"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000701")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000701")

const unexpectedSuccess = { _tag: "UnexpectedSuccess" } as const

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const localStore = Effect.gen(function*() {
  const database = yield* Layer.build(
    Layer.mergeAll(
      ConnectionLane.makeLayer().pipe(
        Layer.provideMerge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))
      ),
      NodeCrypto.layer,
      Reactivity.layer
    )
  )
  const context = yield* LocalStore.layer({
    definition: Domain.definition,
    spaceId,
    clientId,
    scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 }
  }).pipe(
    Layer.provide(layerRuntime),
    Layer.provide(QueryReactivity.layer),
    Layer.provide(Layer.succeedContext(database)),
    Layer.build
  )
  return { local: Context.get(context, LocalStore.Store), sql: Context.get(database, SqlClient.SqlClient) }
})

describe("client storage failures", () => {
  it.effect(
    "reports a pending mutation row that no longer decodes as corrupt storage",
    Effect.fnUntraced(function*() {
      const { local, sql } = yield* localStore
      yield* local.mutate(Domain.PutTodo, Domain.todo("undecodable"))
      yield* sql`UPDATE effect_local_client_pending_data SET digest = 'not-a-digest'`

      const error = yield* local.pending.pipe(Effect.as(unexpectedSuccess), Effect.flip)

      assert.strictEqual(error._tag, "StorageCorrupt")
      if (error._tag === "StorageCorrupt") {
        assert.strictEqual(error.message, "Client pending mutation row is corrupt")
      }
    })
  )

  it.effect(
    "reports an entity row that no longer decodes as corrupt storage",
    Effect.fnUntraced(function*() {
      const { local, sql } = yield* localStore
      yield* local.mutate(Domain.PutTodo, Domain.todo("undecodable"))
      yield* sql`UPDATE effect_local_client_visible_entities_data SET value_json = x'00'`

      const error = yield* local.get(Domain.Todo, "undecodable").pipe(Effect.as(unexpectedSuccess), Effect.flip)

      assert.strictEqual(error._tag, "StorageCorrupt")
      if (error._tag === "StorageCorrupt") assert.strictEqual(error.message, "Client entity row is corrupt")
    })
  )

  it.effect(
    "reports a removed membership row as an unavailable space",
    Effect.fnUntraced(function*() {
      const { local, sql } = yield* localStore
      yield* sql`DELETE FROM effect_local_client_spaces WHERE space_id = ${spaceId}`

      const error = yield* local.pendingCount.pipe(Effect.as(unexpectedSuccess), Effect.flip)

      assert.strictEqual(error._tag, "SpaceUnavailable")
      if (error._tag === "SpaceUnavailable") assert.strictEqual(error.spaceId, spaceId)
    })
  )

  it.effect(
    "rejects a fractional settlement replay position as an invalid request",
    Effect.fnUntraced(function*() {
      const { local } = yield* localStore

      const error = yield* local.readSettlements({ after: 0.5 }).pipe(Effect.as(unexpectedSuccess), Effect.flip)

      assert.strictEqual(error._tag, "ProtocolInvalid")
    })
  )
})
