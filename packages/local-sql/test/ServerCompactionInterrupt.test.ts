import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Scheduler from "effect/Scheduler"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"

const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000601")
const membershipIncarnation = Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000601")

const pad = (value: number) => String(value).padStart(12, "0")

const spaceAt = (index: number) => Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${pad(index)}`)

const envelope = Effect.fnUntraced(function*(spaceId: Identity.SpaceId, index: number, localSequence: number) {
  const identity = {
    spaceId,
    clientId,
    mutationId: Identity.MutationId.make(
      `mut_00000000-0000-4000-8${String(index).padStart(3, "0")}-${pad(localSequence)}`
    ),
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

const layerStore = ServerStore.layerTrusted({
  definition: Domain.definition,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 },
  retainedHistoryEntries: 1,
  maximumHistoryEntries: 4,
  retainedReceipts: 1,
  maximumReceipts: 4
}).pipe(Layer.provide(MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))))

const historyCount = Effect.fnUntraced(function*(spaceId: Identity.SpaceId) {
  const sql = yield* SqlClient.SqlClient
  const row = yield* SqlSchema.findOne({
    Request: Schema.Void,
    Result: Schema.Struct({ count: Schema.Int }),
    execute: () =>
      sql`SELECT retained_history_count AS count FROM effect_local_server_spaces WHERE space_id = ${spaceId}`
  })(undefined)
  return row.count
})

const settle = Effect.fnUntraced(function*(spaceId: Identity.SpaceId) {
  for (let attempt = 0; attempt < 200; attempt++) {
    if ((yield* historyCount(spaceId)) <= 3) return
    yield* Effect.yieldNow
  }
})

const macrotask = Effect.yieldNow

const manualScheduler = () => {
  const tasks: Array<() => void> = []
  const scheduler = new Scheduler.MixedScheduler("async", (task) => {
    let cancelled = false
    tasks.push(() => {
      if (!cancelled) task()
    })
    return () => {
      cancelled = true
    }
  })
  const step = () => {
    const task = tasks.shift()
    if (task === undefined) return false
    task()
    return true
  }
  return { scheduler, step }
}

const submitOutcome = Effect.fnUntraced(function*(spaceId: Identity.SpaceId, index: number, localSequence: number) {
  const store = yield* ServerStore.ServerStore
  return yield* store.submit(yield* envelope(spaceId, index, localSequence)).pipe(
    Effect.map((receipt): string => receipt._tag),
    Effect.catch((error) => Effect.succeed(error._tag))
  )
})

const interruptedAt = Effect.fnUntraced(function*(index: number, steps: number) {
  const store = yield* ServerStore.ServerStore
  const spaceId = spaceAt(index)
  for (let sequence = 1; sequence <= 2; sequence++) {
    yield* store.submit(yield* envelope(spaceId, index, sequence))
    yield* settle(spaceId)
  }
  const crossing = yield* envelope(spaceId, index, 3)
  const manual = manualScheduler()
  const fiber = yield* store.submit(crossing).pipe(
    Effect.provideService(Scheduler.MaxOpsBeforeYield, 3),
    Effect.provideService(Scheduler.Scheduler, manual.scheduler),
    Effect.forkChild({ startImmediately: true })
  )
  let taken = 0
  let stalled = 0
  while (taken < steps && fiber.pollUnsafe() === undefined) {
    if (manual.step()) {
      taken++
      stalled = 0
    } else {
      stalled++
      if (stalled > 1000) return yield* Effect.die(`stalled after ${taken} steps`)
      yield* macrotask
    }
  }
  const completedFirst = fiber.pollUnsafe() !== undefined
  fiber.interruptUnsafe()
  for (let idle = 0; idle < 3;) {
    if (manual.step()) idle = 0
    else {
      idle++
      yield* macrotask
    }
  }
  const committed = (yield* historyCount(spaceId)) !== 2
  const outcomes: Array<string> = []
  for (let sequence = 3; sequence <= 8; sequence++) {
    const outcome = yield* submitOutcome(spaceId, index, sequence)
    outcomes.push(outcome)
    if (outcome !== "Accepted") break
    yield* settle(spaceId)
  }
  return { steps: taken, completedFirst, committed, outcomes }
})

describe("ServerStore write-triggered compaction", () => {
  it.effect(
    "keeps compacting a space after a submit is interrupted at any point once it committed",
    Effect.fnUntraced(function*() {
      const context = yield* layerStore.pipe(Layer.provideMerge(layerDatabase), Layer.build)
      const full = yield* interruptedAt(0, Number.MAX_SAFE_INTEGER).pipe(Effect.provide(context))
      assert.strictEqual(full.completedFirst, true)
      const stuck: Array<{ readonly steps: number; readonly outcomes: ReadonlyArray<string> }> = []
      let index = 1
      let scanned = 0
      for (let steps = full.steps + 2; steps >= 0; steps--) {
        const result = yield* interruptedAt(index++, steps).pipe(Effect.provide(context))
        if (!result.committed) break
        scanned++
        if (result.outcomes.some((outcome) => outcome !== "Accepted")) {
          stuck.push({ steps, outcomes: result.outcomes })
        }
      }
      assert.isAbove(scanned, 0)
      assert.deepStrictEqual(stuck, [])
    }, Effect.scoped),
    120_000
  )
})
