import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as FaultInjection from "../src/FaultInjection.js"
import * as TestPeer from "../src/TestPeer.js"

const leftId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const rightId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const replicaId = Identity.ReplicaId.make("rep_00000000-0000-4000-8000-000000000009")

const options: TestPeer.Options = {
  queueCapacity: 2,
  maxCopies: 3,
  maxDelay: "10 seconds"
}

const bytes = (value: number) => Uint8Array.of(value)

describe("TestPeer coverage", () => {
  it.effect("pairs both transport directions and maps send failures to ReplicaError", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const [a, b] = yield* Effect.all([
        network.transport(leftId).connect({ replicaId, peerId: rightId }),
        network.transport(rightId).connect({ replicaId, peerId: leftId })
      ], { concurrency: "unbounded" })
      assert.strictEqual(a.peerId, rightId)
      assert.strictEqual(b.peerId, leftId)
      yield* a.send(bytes(1))
      assert.deepStrictEqual(Option.getOrThrow(yield* Stream.runHead(b.receive)), bytes(1))
      yield* b.send(bytes(2))
      assert.deepStrictEqual(Option.getOrThrow(yield* Stream.runHead(a.receive)), bytes(2))
      yield* a.close
      const error = yield* Effect.flip(a.send(bytes(3)))
      assert.strictEqual(error._tag, "ReplicaError")
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
    })).pipe(Effect.provide(FaultInjection.none)))

  it.effect("flush delivers a packet held for reordering", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const right = yield* network.connect(rightId, leftId)
      yield* left.send(bytes(1))
      assert.strictEqual(yield* right.queued, 0)
      yield* network.flush
      assert.deepStrictEqual(Option.getOrThrow(yield* Stream.runHead(right.receive)), bytes(1))
    })).pipe(Effect.provide(FaultInjection.layerSequence([
      { drop: false, copies: 1, delay: 0, reorder: true }
    ]))))

  it.effect("drops an in-flight delayed packet when a partition appears mid-delivery", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const right = yield* network.connect(rightId, leftId)
      const send = yield* left.send(bytes(1)).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      yield* network.partition(leftId, rightId)
      yield* TestClock.adjust("1 second")
      yield* Fiber.join(send)
      assert.strictEqual(yield* right.queued, 0)
    })).pipe(Effect.provide(FaultInjection.layerSequence([
      { drop: false, copies: 1, delay: "1 second", reorder: false }
    ]))))

  it.effect("rejects a fault plan whose delay exceeds the configured maximum", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      yield* network.connect(rightId, leftId)
      const error = yield* Effect.flip(left.send(bytes(1)))
      assert.strictEqual(error._tag, "InvalidFault")
      if (error._tag === "InvalidFault") assert.match(error.reason, /delay must be between/)
    })).pipe(Effect.provide(FaultInjection.layerSequence([
      { drop: false, copies: 1, delay: "20 seconds", reorder: false }
    ]))))

  it.effect("delivers sequential unfaulted sends in order without loss", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make({ ...options, queueCapacity: 8 })
      const left = yield* network.connect(leftId, rightId)
      const right = yield* network.connect(rightId, leftId)
      yield* Effect.forEach([1, 2, 3, 4, 5], (n) => left.send(bytes(n)), { discard: true })
      const received = Array.from(yield* Stream.runCollect(Stream.take(right.receive, 5)))
      assert.deepStrictEqual(received, [bytes(1), bytes(2), bytes(3), bytes(4), bytes(5)])
    })).pipe(Effect.provide(FaultInjection.none)))

  it.effect("interrupting an in-flight delayed send propagates interruption and delivers nothing", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const right = yield* network.connect(rightId, leftId)
      const send = yield* left.send(bytes(1)).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      yield* Fiber.interrupt(send)
      const exit = yield* Fiber.await(send)
      assert.isTrue(Exit.hasInterrupts(exit))
      assert.strictEqual(yield* right.queued, 0)
    })).pipe(Effect.provide(FaultInjection.layerSequence([
      { drop: false, copies: 1, delay: "1 second", reorder: false }
    ]))))
})
