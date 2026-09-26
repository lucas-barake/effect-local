import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as InvalidationHub from "../src/internal/invalidationHub.js"

describe("InvalidationHub", () => {
  it.effect(
    "coalesces keys published before the subscriber pulls into one deduplicated batch",
    Effect.fnUntraced(function*() {
      const hub = InvalidationHub.make(16)
      const batches = yield* hub.subscribe
      yield* hub.publish(["a", "b"])
      yield* hub.publish(["b", "c"])
      assert.deepStrictEqual(
        yield* Stream.runHead(batches),
        Option.some<InvalidationHub.InvalidationBatch>({ _tag: "Keys", keys: ["a", "b", "c"] })
      )
    }, Effect.scoped)
  )

  it.effect(
    "replaces a backlog past capacity with one overflow and delivers later keys after it",
    Effect.fnUntraced(function*() {
      const hub = InvalidationHub.make(2)
      const batches = yield* hub.subscribe
      yield* hub.publish(["a", "b", "c"])
      yield* hub.publish(["d"])
      const overflow = yield* Stream.runHead(batches)
      yield* hub.publish(["e"])
      const next = yield* Stream.runHead(batches)
      assert.deepStrictEqual(overflow, Option.some<InvalidationHub.InvalidationBatch>({ _tag: "Overflow" }))
      assert.deepStrictEqual(next, Option.some<InvalidationHub.InvalidationBatch>({ _tag: "Keys", keys: ["e"] }))
    }, Effect.scoped)
  )

  it.effect(
    "ends every subscription when shut down",
    Effect.fnUntraced(function*() {
      const hub = InvalidationHub.make(16)
      const batches = yield* hub.subscribe
      yield* hub.publish(["a"])
      yield* hub.shutdown
      const collected = yield* Stream.runCollect(batches)
      assert.deepStrictEqual(collected, [{ _tag: "Keys", keys: ["a"] }])
    }, Effect.scoped)
  )
})
