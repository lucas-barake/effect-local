import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as PubSub from "effect/PubSub"
import * as Result from "effect/Result"
import * as Scheduler from "effect/Scheduler"
import * as Stream from "effect/Stream"
import * as SequencedPubSub from "../src/internal/sequencedPubSub.js"

describe("SequencedPubSub", () => {
  it.effect(
    "holds no more than its capacity for a subscriber that never reads",
    Effect.fnUntraced(function*() {
      const events = yield* SequencedPubSub.sliding<number>("ephemeral events", 4)
      yield* SequencedPubSub.subscribe(events)
      for (let value = 1; value <= 40; value++) {
        yield* SequencedPubSub.publish(events, value)
        assert.isAtMost(yield* PubSub.size(events.pubsub), 4)
      }
      assert.strictEqual(yield* PubSub.size(events.pubsub), 4)
    }, Effect.scoped)
  )

  it.effect(
    "fails a subscriber whose buffer overflowed before its first take",
    Effect.fnUntraced(function*() {
      const events = yield* SequencedPubSub.sliding<number>("ephemeral events", 4)
      const subscribed = yield* SequencedPubSub.subscribe(events)
      for (let value = 1; value <= 10; value++) yield* SequencedPubSub.publish(events, value)
      const result = yield* subscribed.pipe(Stream.take(4), Stream.runCollect, Effect.result)
      assert.isTrue(Result.isFailure(result), "the subscriber silently skipped the overflowed events")
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure._tag, "CapacityExceeded")
        assert.strictEqual(result.failure.resource, "ephemeral events")
        assert.strictEqual(result.failure.limit, 4)
      }
    }, Effect.scoped)
  )

  it.effect(
    "delivers events published after subscription and none from before it",
    Effect.fnUntraced(function*() {
      const events = yield* SequencedPubSub.sliding<number>("ephemeral events", 4)
      yield* SequencedPubSub.publish(events, 0)
      assert.strictEqual(yield* PubSub.size(events.pubsub), 0)
      const subscribed = yield* SequencedPubSub.subscribe(events)
      for (let value = 1; value <= 4; value++) yield* SequencedPubSub.publish(events, value)
      const received = yield* subscribed.pipe(Stream.take(4), Stream.runCollect)
      assert.deepStrictEqual(received, [1, 2, 3, 4])
    }, Effect.scoped)
  )

  it.effect(
    "does not deliver an event to a subscription that registers while that event is being published",
    Effect.fnUntraced(function*() {
      const events = yield* SequencedPubSub.sliding<string>("ephemeral events", 4)
      const publishing = yield* SequencedPubSub.publish(events, "before").pipe(
        Effect.provideService(Scheduler.MaxOpsBeforeYield, 3),
        Effect.forkChild
      )
      while (events.published === 0) yield* Effect.yieldNow
      const subscribed = yield* SequencedPubSub.subscribe(events)
      yield* Fiber.join(publishing)
      yield* SequencedPubSub.publish(events, "after")
      const received = yield* subscribed.pipe(Stream.take(1), Stream.runCollect)
      assert.deepStrictEqual(received, ["after"])
    }, Effect.scoped)
  )

  it.effect(
    "does not deliver an event to a subscription that registers while that event slides into a full buffer",
    Effect.fnUntraced(function*() {
      const events = yield* SequencedPubSub.sliding<string>("ephemeral events", 2)
      yield* SequencedPubSub.subscribe(events)
      yield* SequencedPubSub.publish(events, "first")
      yield* SequencedPubSub.publish(events, "second")
      const publishing = yield* SequencedPubSub.publish(events, "before").pipe(
        Effect.provideService(Scheduler.MaxOpsBeforeYield, 3),
        Effect.forkChild
      )
      while (events.published === 2) yield* Effect.yieldNow
      const subscribed = yield* SequencedPubSub.subscribe(events)
      yield* Fiber.join(publishing)
      yield* SequencedPubSub.publish(events, "after")
      const received = yield* subscribed.pipe(Stream.take(1), Stream.runCollect)
      assert.deepStrictEqual(received, ["after"])
    }, Effect.scoped)
  )

  it.effect(
    "fails a subscription that registered during a publish once its own first event was evicted",
    Effect.fnUntraced(function*() {
      const events = yield* SequencedPubSub.sliding<string>("ephemeral events", 1)
      const publishing = yield* SequencedPubSub.publish(events, "before").pipe(
        Effect.provideService(Scheduler.MaxOpsBeforeYield, 3),
        Effect.forkChild
      )
      while (events.published === 0) yield* Effect.yieldNow
      const subscribed = yield* SequencedPubSub.subscribe(events)
      yield* Fiber.join(publishing)
      yield* SequencedPubSub.publish(events, "first")
      yield* SequencedPubSub.publish(events, "second")
      const result = yield* subscribed.pipe(Stream.take(1), Stream.runCollect, Effect.result)
      assert.isTrue(Result.isFailure(result), "the subscriber silently skipped its evicted first event")
      if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "CapacityExceeded")
    }, Effect.scoped)
  )

  it.effect(
    "delivers a gapless run of events to a subscription that registers while events are being published",
    Effect.fnUntraced(function*() {
      const events = yield* SequencedPubSub.sliding<number>("ephemeral events", 64)
      const subscribing = yield* SequencedPubSub.subscribe(events).pipe(
        Effect.provideService(Scheduler.MaxOpsBeforeYield, 3),
        Effect.forkChild
      )
      let published = 0
      let firstRetained = 0
      while (subscribing.pollUnsafe() === undefined) {
        published += 1
        yield* SequencedPubSub.publish(events, published)
        if (firstRetained === 0 && (yield* PubSub.size(events.pubsub)) > 0) firstRetained = published
        yield* Effect.yieldNow
      }
      const subscribed = yield* Fiber.join(subscribing)
      if (firstRetained === 0) firstRetained = published + 1
      for (let extra = 0; extra < 3; extra++) {
        published += 1
        yield* SequencedPubSub.publish(events, published)
      }
      const expected = Array.from({ length: published - firstRetained + 1 }, (_, index) => firstRetained + index)
      const received = yield* subscribed.pipe(Stream.take(expected.length), Stream.runCollect)
      assert.deepStrictEqual(received, expected)
    }, Effect.scoped)
  )
})
