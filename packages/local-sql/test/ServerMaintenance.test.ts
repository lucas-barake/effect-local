import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000501")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000501")
const membershipIncarnation = Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000501")

const envelope = Effect.fnUntraced(function*(localSequence: number) {
  const identity = {
    spaceId,
    clientId,
    mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(localSequence).padStart(12, "0")}`),
    localSequence: Identity.LocalSequence.make(localSequence),
    basis: Identity.ServerSequence.make(0),
    name: Domain.PutTodo.name,
    payload: Domain.todo(`todo-${localSequence}`),
    digestVersion: 3 as const,
    membershipIncarnation,
    sourceSchema: Domain.definition.schemaIdentity,
    mutationVersion: Domain.PutTodo.version
  }
  return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
})

const layerDatabase = Layer.mergeAll(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const layerStore = (options: Omit<ServerStore.TrustedOptions, "definition">) =>
  ServerStore.layerTrusted({
    ...options,
    definition: Domain.definition,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 }
  }).pipe(
    Layer.provide(layerRuntime)
  )

const HistoryCount = Schema.Struct({ count: Schema.Int })

const historyCount = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const row = yield* SqlSchema.findOne({
    Request: Schema.Void,
    Result: HistoryCount,
    execute: () =>
      sql`SELECT retained_history_count AS count FROM effect_local_server_spaces WHERE space_id = ${spaceId}`
  })(undefined)
  return row.count
})

const awaitHistoryAtMost = Effect.fnUntraced(function*(limit: number) {
  for (let attempt = 0; attempt < 10_000; attempt++) {
    if ((yield* historyCount) <= limit) return yield* Effect.void
    yield* Effect.yieldNow
  }
  return yield* Effect.die(`space history stayed above ${limit}`)
})

const submitAll = Effect.fnUntraced(function*(from: number, to: number) {
  const store = yield* ServerStore.ServerStore
  const outcomes: Array<string> = []
  for (let sequence = from; sequence <= to; sequence++) {
    outcomes.push((yield* store.submit(yield* envelope(sequence)))._tag)
  }
  return outcomes
})

describe("ServerStore maintenance", () => {
  it.effect(
    "compacts a space without an external sweep once its writes cross the high watermark",
    Effect.fnUntraced(function*() {
      const context = yield* layerStore({
        retainedHistoryEntries: 1,
        maximumHistoryEntries: 4,
        retainedReceipts: 1,
        maximumReceipts: 4
      }).pipe(Layer.provideMerge(layerDatabase), Layer.build)
      const outcomes: Array<string> = []
      for (let sequence = 1; sequence <= 12; sequence++) {
        const submitted = yield* submitAll(sequence, sequence).pipe(Effect.provide(context))
        outcomes.push(...submitted)
        yield* awaitHistoryAtMost(3).pipe(Effect.provide(context))
      }
      assert.deepStrictEqual(outcomes, Array.from({ length: 12 }, () => "Accepted"))
    }, Effect.scoped)
  )

  it.effect(
    "sweeps every space from one cluster singleton on the configured interval",
    Effect.fnUntraced(function*() {
      const layerSharding = SingleRunner.layer({
        runnerStorage: "memory",
        shardingConfig: { entityTerminationTimeout: 0 }
      })
      const context = yield* Layer.build(
        ServerStore.layerMaintenance({ interval: "10 minutes" }).pipe(
          Layer.provideMerge(layerStore({ retainedHistoryEntries: 1, maximumHistoryEntries: 10_000 })),
          Layer.provideMerge(layerSharding),
          Layer.provideMerge(layerDatabase)
        )
      )
      const provide = Effect.provide(context)
      assert.deepStrictEqual(yield* submitAll(1, 3).pipe(provide), ["Accepted", "Accepted", "Accepted"])
      yield* TestClock.adjust("1 minute")
      assert.strictEqual(yield* historyCount.pipe(provide), 1)
      yield* submitAll(4, 6).pipe(provide)
      yield* TestClock.adjust("1 minute")
      assert.strictEqual(yield* historyCount.pipe(provide), 4)
      yield* TestClock.adjust("10 minutes")
      assert.strictEqual(yield* historyCount.pipe(provide), 1)
    }, Effect.scoped)
  )
})
