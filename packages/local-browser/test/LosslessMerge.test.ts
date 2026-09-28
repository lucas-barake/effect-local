import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as PubSub from "effect/PubSub"
import * as Scheduler from "effect/Scheduler"
import * as Stream from "effect/Stream"
import * as LosslessQueue from "../src/internal/losslessQueue.js"

const deliverOne = Effect.fnUntraced(
  function*(_maxOpsBeforeYield: number, spacing: number) {
    const published = yield* PubSub.unbounded<number>()
    const subscription = yield* PubSub.subscribe(published)
    const received = yield* LosslessQueue.merge(Stream.fromSubscription(subscription), Stream.never).pipe(
      Stream.take(6),
      Stream.runCollect,
      Effect.forkChild
    )
    for (let value = 0; value < 6; value++) {
      for (let turn = 0; turn < spacing; turn++) yield* Effect.yieldNow
      yield* PubSub.publish(published, value)
    }
    assert.deepStrictEqual(yield* Fiber.join(received), [0, 1, 2, 3, 4, 5])
  },
  (effect, maxOpsBeforeYield) => Effect.provideService(effect, Scheduler.MaxOpsBeforeYield, maxOpsBeforeYield),
  Effect.scoped
)

const burst = 17

const deliverMany = Effect.fnUntraced(
  function*(_maxOpsBeforeYield: number, spacing: number) {
    const left = yield* PubSub.unbounded<number>()
    const right = yield* PubSub.unbounded<number>()
    const leftSubscription = yield* PubSub.subscribe(left)
    const rightSubscription = yield* PubSub.subscribe(right)
    const received = yield* LosslessQueue.mergeAll([
      Stream.fromSubscription(leftSubscription),
      Stream.fromSubscription(rightSubscription).pipe(Stream.rechunk(1))
    ]).pipe(
      Stream.take(6 * (burst + 1)),
      Stream.runCollect,
      Effect.forkChild
    )
    for (let round = 0; round < 6; round++) {
      for (let turn = 0; turn < spacing; turn++) yield* Effect.yieldNow
      yield* PubSub.publish(left, round)
      yield* PubSub.publishAll(right, Array.from({ length: burst }, (_, index) => 100 * (round + 1) + index))
    }
    const values = yield* Fiber.join(received)
    assert.deepStrictEqual(values.filter((value) => value < 100), [0, 1, 2, 3, 4, 5])
    assert.strictEqual(values.length, 6 * (burst + 1))
  },
  (effect, maxOpsBeforeYield) => Effect.provideService(effect, Scheduler.MaxOpsBeforeYield, maxOpsBeforeYield),
  Effect.scoped
)

describe("lossless merge", () => {
  it.effect(
    "merges two streams without losing a wakeup when its fibers are preempted",
    Effect.fnUntraced(function*() {
      for (let maxOpsBeforeYield = 5; maxOpsBeforeYield <= 7; maxOpsBeforeYield++) {
        for (let spacing = 0; spacing < 6; spacing++) yield* deliverOne(maxOpsBeforeYield, spacing)
      }
    })
  )

  it.effect(
    "merges many streams through a full buffer without losing a wakeup when its fibers are preempted",
    Effect.fnUntraced(function*() {
      for (let maxOpsBeforeYield = 12; maxOpsBeforeYield <= 13; maxOpsBeforeYield++) {
        for (let spacing = 0; spacing < 5; spacing++) yield* deliverMany(maxOpsBeforeYield, spacing)
      }
    })
  )
})
