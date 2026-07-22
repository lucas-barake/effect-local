import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Scheduler from "effect/Scheduler"
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
  it.effect("fails invalid options in the typed error channel", () =>
    Effect.gen(function*() {
      const invalidOptions: ReadonlyArray<TestPeer.Options> = [
        { ...options, queueCapacity: 0 },
        { ...options, maxCopies: 0 },
        { ...options, maxDelay: Number.POSITIVE_INFINITY }
      ]
      const errors = yield* Effect.forEach(
        invalidOptions,
        (options) => Effect.flip(TestPeer.make(options))
      )
      assert.deepStrictEqual(errors.map((error) => error._tag), [
        "InvalidOptions",
        "InvalidOptions",
        "InvalidOptions"
      ])
      assert.deepStrictEqual(errors.map((error) => error.reason), [
        "queueCapacity must be a positive integer",
        "maxCopies must be a positive integer",
        "maxDelay must be finite and nonnegative"
      ])
    }).pipe(Effect.provide(FaultInjection.none)))

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
      assert.strictEqual(error._tag, "QueueFull")
      if (error._tag === "QueueFull") assert.strictEqual(error.capacity, 1)
    })).pipe(Effect.provide(FaultInjection.none)))

  it.effect("keeps a full queue classification stable while a consumer races", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make({ ...options, queueCapacity: 1 })
      const left = yield* network.connect(leftId, rightId)
      const right = yield* network.connect(rightId, leftId)
      yield* left.send(bytes(1))
      const send = yield* Effect.flip(left.send(bytes(2))).pipe(Effect.forkChild)
      const receive = yield* Stream.runHead(right.receive).pipe(Effect.forkChild)
      const error = yield* Fiber.join(send)
      yield* Fiber.join(receive)
      assert.strictEqual(error._tag, "QueueFull")
    })).pipe(
      Effect.provide(FaultInjection.none),
      Effect.provideService(Scheduler.MaxOpsBeforeYield, 3)
    ))

  it.effect("rejects fault plans outside configured bounds", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const error = yield* Effect.flip(left.send(bytes(1)))
      assert.strictEqual(error._tag, "InvalidFault")
    })).pipe(Effect.provide(FaultInjection.layerSequence([{
      drop: false,
      copies: 4,
      delay: 0,
      reorder: false
    }]))))

  it.effect("rejects sends after the connection closes", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      yield* network.connect(rightId, leftId)
      yield* left.close
      const error = yield* Effect.flip(left.send(bytes(7)))
      assert.strictEqual(error._tag, "ConnectionClosed")
    })).pipe(Effect.provide(FaultInjection.none)))

  it.effect("does not retain sends for peers that have not connected", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const error = yield* Effect.flip(left.send(bytes(9)))
      assert.strictEqual(error._tag, "ConnectionClosed")
      const right = yield* network.connect(rightId, leftId)
      assert.strictEqual(yield* right.queued, 0)
    })).pipe(Effect.provide(FaultInjection.none)))

  it.effect("replaces duplicate inbound connections without sharing their lifecycle", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const first = yield* network.connect(rightId, leftId)
      const second = yield* network.connect(rightId, leftId)
      assert.strictEqual((yield* Effect.flip(first.send(bytes(2))))._tag, "ConnectionClosed")
      yield* first.close
      yield* left.send(bytes(3))
      assert.deepStrictEqual(
        Option.getOrThrow(yield* Stream.runHead(second.receive)),
        bytes(3)
      )
    })).pipe(Effect.provide(FaultInjection.none)))

  it.effect("does not deliver delayed sends across connection generations", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      yield* network.connect(rightId, leftId)
      const send = yield* Effect.flip(left.send(bytes(4))).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      const replacement = yield* network.connect(rightId, leftId)
      yield* TestClock.adjust("1 second")
      assert.strictEqual((yield* Fiber.join(send))._tag, "ConnectionClosed")
      assert.strictEqual(yield* replacement.queued, 0)
    })).pipe(Effect.provide(FaultInjection.layerSequence([{
      drop: false,
      copies: 1,
      delay: "1 second",
      reorder: false
    }]))))

  it.effect("does not deliver delayed sends from a replaced connection", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      const firstRight = yield* network.connect(rightId, leftId)
      const send = yield* Effect.flip(firstRight.send(bytes(5))).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      yield* network.connect(rightId, leftId)
      yield* TestClock.adjust("1 second")
      assert.strictEqual((yield* Fiber.join(send))._tag, "ConnectionClosed")
      assert.strictEqual(yield* left.queued, 0)
    })).pipe(Effect.provide(FaultInjection.layerSequence([{
      drop: false,
      copies: 1,
      delay: "1 second",
      reorder: false
    }]))))

  it.effect("retires held packets when a connection is replaced", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const left = yield* network.connect(leftId, rightId)
      yield* network.connect(rightId, leftId)
      yield* left.send(bytes(1))
      const replacement = yield* network.connect(rightId, leftId)
      yield* left.send(bytes(2))
      assert.deepStrictEqual(Option.getOrThrow(yield* Stream.runHead(replacement.receive)), bytes(2))
    })).pipe(Effect.provide(FaultInjection.layerSequence([
      { drop: false, copies: 1, delay: 0, reorder: true },
      { drop: false, copies: 1, delay: 0, reorder: false }
    ]))))

  it.effect("retires held packets when their source connection is replaced", () =>
    Effect.scoped(Effect.gen(function*() {
      const network = yield* TestPeer.make(options)
      const firstLeft = yield* network.connect(leftId, rightId)
      const right = yield* network.connect(rightId, leftId)
      yield* firstLeft.send(bytes(1))
      const replacement = yield* network.connect(leftId, rightId)
      yield* replacement.send(bytes(2))
      assert.deepStrictEqual(Option.getOrThrow(yield* Stream.runHead(right.receive)), bytes(2))
    })).pipe(Effect.provide(FaultInjection.layerSequence([
      { drop: false, copies: 1, delay: 0, reorder: true },
      { drop: false, copies: 1, delay: 0, reorder: false }
    ]))))
})
