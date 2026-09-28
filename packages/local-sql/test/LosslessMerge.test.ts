import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as PubSub from "effect/PubSub"
import * as Scheduler from "effect/Scheduler"
import * as Stream from "effect/Stream"
import * as LosslessQueue from "../src/internal/losslessQueue.js"

const deliver = Effect.fnUntraced(
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

describe("lossless merge", () => {
  it.effect(
    "delivers every element when its fibers are preempted between a queue check and the wait",
    Effect.fnUntraced(function*() {
      for (let maxOpsBeforeYield = 5; maxOpsBeforeYield <= 7; maxOpsBeforeYield++) {
        for (let spacing = 0; spacing < 6; spacing++) yield* deliver(maxOpsBeforeYield, spacing)
      }
    })
  )
})
