import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"
import * as SequencedPubSub from "../src/internal/sequencedPubSub.js"

describe("SequencedPubSub", () => {
  it.effect(
    "fails a subscriber whose buffer overflowed before its first take",
    Effect.fnUntraced(function*() {
      const events = yield* SequencedPubSub.sliding<number>("ephemeral events", 4)
      const subscribed = yield* SequencedPubSub.subscribe(events)
      for (let value = 1; value <= 10; value = value + 1) yield* SequencedPubSub.publish(events, value)
      const result = yield* subscribed.pipe(Stream.take(4), Stream.runCollect, Effect.result)
      assert.isTrue(Result.isFailure(result), "the subscriber silently skipped the overflowed events")
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure._tag, "CapacityExceeded")
        assert.strictEqual(result.failure.limit, 4)
      }
    }, Effect.scoped)
  )

  it.effect(
    "delivers events published after subscription and none from before it",
    Effect.fnUntraced(function*() {
      const events = yield* SequencedPubSub.sliding<number>("ephemeral events", 4)
      yield* SequencedPubSub.publish(events, 0)
      const subscribed = yield* SequencedPubSub.subscribe(events)
      for (let value = 1; value <= 4; value = value + 1) yield* SequencedPubSub.publish(events, value)
      const received = yield* subscribed.pipe(Stream.take(4), Stream.runCollect)
      assert.deepStrictEqual(received, [1, 2, 3, 4])
    }, Effect.scoped)
  )
})
