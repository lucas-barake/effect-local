import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Rows from "../src/internal/rows.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"
import { type ServerDatabase, serverDatabases } from "./fixtures/ServerDatabase.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000601")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000601")
const membershipIncarnation = Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000601")

const mutationIdAt = (localSequence: number) =>
  Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(localSequence).padStart(12, "0")}`)

const envelope = Effect.fnUntraced(function*(
  localSequence: number,
  mutation: { readonly name: string; readonly version: Identity.SchemaVersion } = Domain.PutTodo,
  title = "first"
) {
  const identity = {
    spaceId,
    clientId,
    mutationId: mutationIdAt(localSequence),
    localSequence: Identity.LocalSequence.make(localSequence),
    basis: Identity.ServerSequence.make(0),
    name: mutation.name,
    payload: Domain.todo(`todo-${localSequence}`, title),
    digestVersion: 1 as const,
    membershipIncarnation,
    sourceSchema: Domain.definition.schemaIdentity,
    mutationVersion: mutation.version
  }
  return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
})

const envelopes = (from: number, to: number) =>
  Effect.forEach(Array.from({ length: to - from + 1 }, (_, index) => from + index), (sequence) => envelope(sequence))

const batch = (batchEnvelopes: ReadonlyArray<Protocol.MutationEnvelope>) => ({
  envelopes: batchEnvelopes,
  schema: Domain.definition.schemaIdentity
})

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const layerStore = (database: ServerDatabase, options: Omit<ServerStore.TrustedOptions, "definition"> = {}) => {
  const layerDatabase = Layer.mergeAll(database.layer(), NodeCrypto.layer)
  return ServerStore.layerTrusted({
    ...options,
    definition: Domain.definition,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 }
  }).pipe(
    Layer.provide(layerRuntime),
    Layer.provideMerge(layerDatabase)
  )
}

const submitBatchBudget = "1 second"

const layerGatedStore = (
  database: ServerDatabase,
  gate: { readonly entered: Deferred.Deferred<void>; readonly release: Deferred.Deferred<void> }
) => {
  const layerDatabase = Layer.mergeAll(database.layer(), NodeCrypto.layer)
  return ServerStore.layer({
    definition: Domain.definition,
    migration: { retryDelay: "1 millis", maximumAttempts: 8 },
    maximumSubmitBatchDuration: submitBatchBudget,
    authorizeAccess: () => Effect.void,
    authorizeRead: () => Effect.void,
    authorizeMutation: ({ mutation }) => {
      if (!Schema.is(Domain.Todo.schema)(mutation.payload) || mutation.payload.title !== "slow") return Effect.void
      return Deferred.succeed(gate.entered, undefined).pipe(Effect.andThen(Deferred.await(gate.release)))
    }
  }).pipe(
    Layer.provide(layerRuntime),
    Layer.provideMerge(layerDatabase)
  )
}

const failureOf = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.result,
    Effect.map((result) => {
      if (Result.isFailure(result)) return result.failure
      return assert.fail("expected Effect failure")
    })
  )

const LogRow = Schema.Struct({
  server_sequence: Rows.integer(Identity.ServerSequence),
  mutation_id: Schema.String
})

const EntityKeyRow = Schema.Struct({ entity_key: Schema.String })

const storedTodoKeys = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const rows = yield* SqlSchema.findAll({
    Request: Schema.Void,
    Result: EntityKeyRow,
    execute: () =>
      sql`SELECT entity_key FROM effect_local_server_entities_data
        WHERE space_id = ${spaceId} AND model = ${Domain.Todo.name} ORDER BY entity_key`
  })(undefined)
  return rows.map((row) => row.entity_key)
})

const authoritativeLog = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  return yield* SqlSchema.findAll({
    Request: Schema.Void,
    Result: LogRow,
    execute: () =>
      sql`SELECT server_sequence, mutation_id FROM effect_local_authoritative_log
        WHERE space_id = ${spaceId} ORDER BY server_sequence`
  })(undefined)
})

const receiptCount = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const row = yield* SqlSchema.findOne({
    Request: Schema.Void,
    Result: Rows.CountRow,
    execute: () => sql`SELECT COUNT(*) AS count FROM effect_local_server_receipts WHERE space_id = ${spaceId}`
  })(undefined)
  return row.count
})

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

describe.each(serverDatabases)("ServerStore batch admission ($dialect)", (database) => {
  it.effect(
    "admits every envelope in order and continues past a terminal rejection in the middle",
    Effect.fnUntraced(function*() {
      const store = yield* ServerStore.ServerStore
      const submitted = [
        yield* envelope(1),
        yield* envelope(2),
        yield* envelope(3, Domain.RejectAfterWrite),
        yield* envelope(4),
        yield* envelope(5)
      ]
      const result = yield* store.admitBatch(batch(submitted), null)

      assert.deepStrictEqual(
        result.receipts.map((receipt) => receipt._tag),
        ["Accepted", "Accepted", "Rejected", "Accepted", "Accepted"]
      )
      assert.deepStrictEqual(
        result.receipts.map((receipt) => [receipt.mutationId, receipt.localSequence]),
        submitted.map((sent) => [sent.mutationId, sent.localSequence])
      )
      assert.deepStrictEqual(
        result.receipts.map((receipt): number | undefined => {
          if (receipt._tag === "Accepted" || receipt._tag === "Rejected") return receipt.terminalSequence
          return assert.fail(`unexpected ${receipt._tag} receipt`)
        }),
        [1, 2, 3, 4, 5]
      )
      assert.deepStrictEqual(
        yield* authoritativeLog,
        [1, 2, 4, 5].map((sequence, index) => ({
          server_sequence: Identity.ServerSequence.make(index + 1),
          mutation_id: mutationIdAt(sequence)
        }))
      )
      assert.strictEqual(yield* receiptCount, 5)
      assert.deepStrictEqual(
        yield* storedTodoKeys,
        ["\"todo-1\"", "\"todo-2\"", "\"todo-4\"", "\"todo-5\""]
      )
    }, (effect) => effect.pipe(Effect.provide(layerStore(database)), Effect.scoped))
  )

  it.effect(
    "returns the stored receipts when a batch is resubmitted without applying anything twice",
    Effect.fnUntraced(function*() {
      const store = yield* ServerStore.ServerStore
      const submitted = yield* envelopes(1, 4)
      const first = yield* store.admitBatch(batch(submitted), null)
      const logAfterFirst = yield* authoritativeLog
      const retried = yield* store.admitBatch(batch(submitted), null)
      const overlapping = yield* store.admitBatch(batch([...submitted.slice(2), yield* envelope(5)]), null)

      assert.deepStrictEqual(retried.receipts, first.receipts)
      assert.deepStrictEqual(overlapping.receipts.slice(0, 2), first.receipts.slice(2))
      assert.strictEqual(overlapping.receipts[2]?._tag, "Accepted")
      assert.deepStrictEqual(
        (yield* authoritativeLog).slice(0, logAfterFirst.length),
        logAfterFirst
      )
      assert.deepStrictEqual(
        (yield* authoritativeLog).map((row) => row.server_sequence),
        [1, 2, 3, 4, 5]
      )
      assert.strictEqual(yield* receiptCount, 5)
    }, (effect) => effect.pipe(Effect.provide(layerStore(database)), Effect.scoped))
  )

  it.effect(
    "returns the admitted prefix when an envelope after the first fails and commits nothing past it",
    Effect.fnUntraced(function*() {
      const store = yield* ServerStore.ServerStore
      const result = yield* store.admitBatch(
        batch([yield* envelope(1), yield* envelope(2), yield* envelope(4), yield* envelope(5)]),
        null
      )
      assert.deepStrictEqual(result.receipts.map((receipt) => receipt.localSequence), [1, 2])
      assert.strictEqual((yield* authoritativeLog).length, 2)

      const outOfOrder = batch([yield* envelope(4), yield* envelope(5)])
      const failure = yield* failureOf(store.admitBatch(outOfOrder, null))
      assert.strictEqual(failure._tag, "OutOfOrderMutation")
      if (failure._tag === "OutOfOrderMutation") {
        assert.strictEqual(failure.expected, 3)
        assert.strictEqual(failure.actual, 4)
      }
      assert.strictEqual((yield* authoritativeLog).length, 2)
    }, (effect) => effect.pipe(Effect.provide(layerStore(database)), Effect.scoped))
  )

  it.effect(
    "bounds the receipts of one response by the protocol batch bytes and returns the rest on resubmission",
    Effect.fnUntraced(function*() {
      const store = yield* ServerStore.ServerStore
      const title = "x".repeat(200 * 1024)
      const submitted = yield* Effect.forEach(
        Array.from({ length: 25 }, (_, index) => index + 1),
        (sequence) => envelope(sequence, Domain.PutTodo, title)
      )
      const receipts: Array<Protocol.Receipt> = []
      const responseSizes: Array<number> = []
      while (receipts.length < submitted.length) {
        const remaining = batch(submitted.slice(receipts.length))
        const result = yield* store.admitBatch(remaining, null)
        responseSizes.push(result.receipts.length)
        assert.isAtMost(yield* Protocol.encodedBytesEffect(result.receipts), Protocol.maximumBatchBytes)
        receipts.push(...result.receipts)
      }

      assert.isAbove(responseSizes.length, 1)
      assert.deepStrictEqual(
        receipts.map((receipt) => [receipt._tag, receipt.mutationId]),
        submitted.map((sent) => ["Accepted", sent.mutationId])
      )
      assert.deepStrictEqual(
        (yield* authoritativeLog).map((row) => row.mutation_id),
        submitted.map((sent) => sent.mutationId)
      )
    }, (effect) => effect.pipe(Effect.provide(layerStore(database)), Effect.scoped))
  )

  it.effect(
    "returns the admitted prefix once a batch runs past its time budget",
    Effect.fnUntraced(function*() {
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const context = yield* Layer.build(layerGatedStore(database, { entered, release }))
      const store = Context.get(context, ServerStore.ServerStore)
      const submitted = [
        yield* envelope(1),
        yield* envelope(2, Domain.PutTodo, "slow"),
        yield* envelope(3),
        yield* envelope(4)
      ]

      const admitting = yield* store.admitBatch(batch(submitted), null).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(entered)
      yield* TestClock.adjust(submitBatchBudget)
      yield* Deferred.succeed(release, undefined)
      const first = yield* Fiber.join(admitting)
      assert.deepStrictEqual(first.receipts.map((receipt) => receipt.localSequence), [1, 2])
      const remaining = batch(submitted.slice(first.receipts.length))
      const rest = yield* store.admitBatch(remaining, null)
      assert.deepStrictEqual(rest.receipts.map((receipt) => receipt.localSequence), [3, 4])
      assert.deepStrictEqual(
        (yield* authoritativeLog.pipe(Effect.provide(context))).map((row) => row.mutation_id),
        submitted.map((sent) => sent.mutationId)
      )
    }, (effect) => effect.pipe(Effect.provide(NodeCrypto.layer), Effect.scoped))
  )

  it.effect(
    "compacts a space once a batch crosses the high watermark",
    Effect.fnUntraced(function*() {
      const store = yield* ServerStore.ServerStore
      const result = yield* store.admitBatch(batch(yield* envelopes(1, 51)), null)
      assert.deepStrictEqual(
        result.receipts.map((receipt) => receipt._tag),
        Array.from({ length: 51 }, () => "Accepted")
      )
      yield* awaitHistoryAtMost(1)
    }, (effect) =>
      effect.pipe(
        Effect.provide(layerStore(database, {
          retainedHistoryEntries: 1,
          maximumHistoryEntries: 100,
          retainedReceipts: 1,
          maximumReceipts: 100,
          pruneBatchSize: 10
        })),
        Effect.scoped
      ))
  )
})
