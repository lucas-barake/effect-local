import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Rows from "../src/internal/rows.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"
import { type ServerDatabase, serverDatabases, sqliteLayer } from "./fixtures/ServerDatabase.js"

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

const layerDatabase = (database: ServerDatabase) => Layer.mergeAll(database.layer(), NodeCrypto.layer)

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
const layerSqliteDatabase = Layer.mergeAll(sqliteLayer(), NodeCrypto.layer)
const provideNodeCrypto = Effect.provide(NodeCrypto.layer)

const layerStore = (options: Omit<ServerStore.TrustedOptions, "definition">) =>
  ServerStore.layerTrusted({
    ...options,
    definition: Domain.definition,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 }
  }).pipe(
    Layer.provide(layerRuntime)
  )

const historyCount = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const row = yield* SqlSchema.findOne({
    Request: Schema.Void,
    Result: Rows.CountRow,
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

describe.each(serverDatabases)("ServerStore maintenance ($dialect)", (database) => {
  it.effect(
    "compacts a space without an external sweep once its writes cross the high watermark",
    Effect.fnUntraced(function*() {
      const context = yield* layerStore({
        retainedHistoryEntries: 1,
        maximumHistoryEntries: 4,
        retainedReceipts: 1,
        maximumReceipts: 4
      }).pipe(Layer.provideMerge(layerDatabase(database)), Layer.build)
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
    "returns a compacted space's history to the retained bound in one compaction",
    Effect.fnUntraced(function*() {
      const context = yield* layerStore({
        retainedHistoryEntries: 1,
        maximumHistoryEntries: 100,
        retainedReceipts: 1,
        maximumReceipts: 100,
        pruneBatchSize: 10
      }).pipe(Layer.provideMerge(layerDatabase(database)), Layer.build)
      const outcomes = yield* submitAll(1, 51).pipe(Effect.provide(context))
      assert.deepStrictEqual(outcomes, Array.from({ length: 51 }, () => "Accepted"))
      yield* awaitHistoryAtMost(1).pipe(Effect.provide(context))
    }, Effect.scoped)
  )

  it.effect(
    "admits a server sequence at the largest safe integer and rejects the next",
    Effect.fnUntraced(
      function*() {
        const context = yield* layerStore({}).pipe(Layer.provideMerge(layerDatabase(database)), Layer.build)
        const provide = Effect.provide(context)
        const sql = Context.get(context, SqlClient.SqlClient)
        assert.deepStrictEqual(yield* submitAll(1, 1).pipe(provide), ["Accepted"])
        yield* sql`UPDATE effect_local_server_spaces SET next_server_sequence = ${Number.MAX_SAFE_INTEGER - 1}
        WHERE space_id = ${spaceId}`
        const store = Context.get(context, ServerStore.ServerStore)
        const accepted = yield* store.submit(yield* envelope(2))
        assert.strictEqual(accepted._tag, "Accepted")
        if (accepted._tag === "Accepted") assert.strictEqual(accepted.serverSequence, Number.MAX_SAFE_INTEGER - 1)
        const stored = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: Rows.SequenceRow,
          execute: () =>
            sql`SELECT MAX(server_sequence) AS server_sequence FROM effect_local_authoritative_log
            WHERE space_id = ${spaceId}`
        })(undefined)
        assert.strictEqual(stored.server_sequence, Number.MAX_SAFE_INTEGER - 1)
        const rejected = yield* store.submit(yield* envelope(3)).pipe(Effect.flip)
        assert.strictEqual(rejected._tag, "CapacityExceeded")
      },
      Effect.scoped,
      provideNodeCrypto
    )
  )

  it.effect(
    "compacts every space in one maintenance pass",
    Effect.fnUntraced(function*() {
      const context = yield* layerStore({ retainedHistoryEntries: 1, maximumHistoryEntries: 10_000 }).pipe(
        Layer.provideMerge(layerDatabase(database)),
        Layer.build
      )
      const provide = Effect.provide(context)
      assert.deepStrictEqual(yield* submitAll(1, 3).pipe(provide), ["Accepted", "Accepted", "Accepted"])
      assert.strictEqual(yield* historyCount.pipe(provide), 3)
      yield* ServerStore.ServerStore.pipe(Effect.flatMap((store) => store.maintainAll), provide)
      assert.strictEqual(yield* historyCount.pipe(provide), 1)
    }, Effect.scoped)
  )
})

describe("ServerStore maintenance singleton", () => {
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
          Layer.provideMerge(layerSqliteDatabase)
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
