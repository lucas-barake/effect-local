import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as FaultInjection from "../src/FaultInjection.js"
import * as TestPeer from "../src/TestPeer.js"

const leftId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const rightId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")

const options: TestPeer.Options = {
  queueCapacity: 2,
  maxCopies: 3,
  maxDelay: "10 seconds"
}

const bytes = (value: number) => Uint8Array.of(value)

describe("TestPeer", () => {
  it.effect("uses the test clock for deterministic delays", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const right = yield* network.connect(rightId, leftId)
      const send = yield* left.send(bytes(1)).pipe(Effect.forkChild)
      assert.strictEqual(yield* right.queued, 0)
      yield* TestClock.adjust("1 second")
      yield* Fiber.join(send)
      assert.deepStrictEqual(Array.from(yield* Stream.runCollect(Stream.take(right.receive, 1))), [bytes(1)])
    })).pipe(Effect.provide(FaultInjection.layerSequence([{
      drop: false,
      copies: 1,
      delay: "1 second",
      reorder: false
    }]))))

  it.effect("duplicates and reorders within configured bounds", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const right = yield* network.connect(rightId, leftId)
      yield* left.send(bytes(1))
      yield* left.send(bytes(2))
      const received = Array.from(yield* Stream.runCollect(Stream.take(right.receive, 3)))
      assert.deepStrictEqual(received, [bytes(2), bytes(2), bytes(1)])
    })).pipe(Effect.provide(FaultInjection.layerSequence([
      { drop: false, copies: 1, delay: 0, reorder: true },
      { drop: false, copies: 2, delay: 0, reorder: false }
    ]))))

  it.effect("models drops and partitions without enqueueing", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const right = yield* network.connect(rightId, leftId)
      yield* left.send(bytes(1))
      assert.strictEqual(yield* right.queued, 0)
      yield* network.partition(leftId, rightId)
      yield* left.send(bytes(2))
      assert.strictEqual(yield* right.queued, 0)
      yield* network.heal(leftId, rightId)
      yield* left.send(bytes(3))
      assert.strictEqual(yield* right.queued, 1)
    })).pipe(Effect.provide(FaultInjection.layerSequence([
      { drop: true, copies: 1, delay: 0, reorder: false },
      { drop: false, copies: 1, delay: 0, reorder: false }
    ]))))

  it.effect("fails rather than hanging when a route queue is full", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make({ ...options, queueCapacity: 1 })
      const left = yield* network.connect(leftId, rightId)
      yield* network.connect(rightId, leftId)
      yield* left.send(bytes(1))
      const error = yield* Effect.flip(left.send(bytes(2)))
      assert.strictEqual(error._tag, "TestPeerQueueFull")
      if (error._tag === "TestPeerQueueFull") assert.strictEqual(error.capacity, 1)
    })).pipe(Effect.provide(FaultInjection.none)))

  it.effect("rejects fault plans outside configured bounds", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const error = yield* Effect.flip(left.send(bytes(1)))
      assert.strictEqual(error._tag, "TestPeerInvalidFault")
    })).pipe(Effect.provide(FaultInjection.layerSequence([{
      drop: false,
      copies: 4,
      delay: 0,
      reorder: false
    }]))))
})
