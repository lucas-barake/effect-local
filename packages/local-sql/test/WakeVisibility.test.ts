import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as TestClock from "effect/testing/TestClock"
import * as WakeVisibility from "../src/internal/wakeVisibility.js"

type TestErrorReason = "timeout" | "pending-capacity" | "denied"

class TestError {
  readonly _tag = "TestError"

  constructor(readonly reason: TestErrorReason) {}
}

const timeoutError = new TestError("timeout")
const pendingCapacityError = new TestError("pending-capacity")
const deniedError = new TestError("denied")

const makeLimiter = (options?: Partial<WakeVisibility.Options<TestError>>) =>
  WakeVisibility.makeLimiter<TestError>({
    lookupTimeoutNanos: 1_000_000_000n,
    onLookupTimeout: () => timeoutError,
    maximumConcurrentLookups: 2,
    maximumPendingLookups: 16,
    onPendingCapacityExceeded: () => pendingCapacityError,
    ...options
  })

describe("wake visibility coordinator", () => {
  it.effect(
    "runs one visibility lookup for concurrent callers with the same canonical key",
    Effect.fnUntraced(function*() {
      const limiter = yield* makeLimiter()
      const coordinator = WakeVisibility.make<string, boolean, TestError>(limiter)
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let lookups = 0
      const lookup = Effect.fnUntraced(function*() {
        lookups++
        yield* Deferred.succeed(entered, undefined)
        yield* Deferred.await(release)
        return true
      })

      const leader = yield* coordinator.evaluate("canonical-reader", lookup).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(entered)
      const followers = yield* pipe(
        Array.from({ length: 15 }),
        Effect.forEach(() =>
          coordinator.evaluate("canonical-reader", lookup).pipe(Effect.forkChild({ startImmediately: true }))
        )
      )

      assert.strictEqual(lookups, 1)
      yield* Deferred.succeed(release, undefined)
      const results = yield* Effect.forEach([leader, ...followers], Fiber.join)
      assert.deepStrictEqual(results, Array.from({ length: 16 }, () => true))
    }, Effect.scoped)
  )

  it.effect(
    "isolates visibility by the complete canonical key",
    Effect.fnUntraced(function*() {
      const limiter = yield* makeLimiter()
      const coordinator = WakeVisibility.make<string, string, TestError>(limiter)
      let lookups = 0
      const evaluate = (key: string) =>
        coordinator.evaluate(key, () =>
          Effect.sync(() => {
            lookups++
            return key
          }))

      assert.deepStrictEqual(
        yield* Effect.all([
          evaluate("client-a|scope-a|principal-a"),
          evaluate("client-a|scope-a|principal-a"),
          evaluate("client-b|scope-a|principal-a"),
          evaluate("client-b|scope-a|principal-a"),
          evaluate("client-a|scope-b|principal-a"),
          evaluate("client-a|scope-b|principal-a"),
          evaluate("client-a|scope-a|principal-b"),
          evaluate("client-a|scope-a|principal-b")
        ], { concurrency: "unbounded" }),
        [
          "client-a|scope-a|principal-a",
          "client-a|scope-a|principal-a",
          "client-b|scope-a|principal-a",
          "client-b|scope-a|principal-a",
          "client-a|scope-b|principal-a",
          "client-a|scope-b|principal-a",
          "client-a|scope-a|principal-b",
          "client-a|scope-a|principal-b"
        ]
      )
      assert.strictEqual(lookups, 4)
    }, Effect.scoped)
  )

  it.effect(
    "cancels owner work when its last evaluator detaches and allows a restart",
    Effect.fnUntraced(function*() {
      const limiter = yield* makeLimiter()
      const coordinator = WakeVisibility.make<string, boolean, TestError>(limiter)
      const entered = yield* Deferred.make<void>()
      const lookupInterrupted = yield* Deferred.make<void>()
      let lookups = 0
      const lookup = Effect.fnUntraced(function*() {
        lookups++
        yield* Deferred.succeed(entered, undefined)
        return yield* Effect.never
      }, Effect.onInterrupt(() => Deferred.succeed(lookupInterrupted, undefined)))

      const evaluator = yield* coordinator.evaluate("canonical-reader", lookup).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(entered)
      yield* Fiber.interrupt(evaluator)
      assert.isTrue(yield* Deferred.isDone(lookupInterrupted))

      assert.isTrue(
        yield* coordinator.evaluate("canonical-reader", () => {
          lookups++
          return Effect.succeed(true)
        })
      )
      assert.strictEqual(lookups, 2)
    }, Effect.scoped)
  )

  it.effect(
    "shares typed failures and defects",
    Effect.fnUntraced(function*() {
      const limiter = yield* makeLimiter()
      const coordinator = WakeVisibility.make<string, boolean, TestError>(limiter)
      let failedLookups = 0
      const failed = () => {
        failedLookups++
        return Effect.fail(deniedError)
      }
      const firstFailure = yield* coordinator.evaluate("failure", failed).pipe(Effect.exit)
      const secondFailure = yield* coordinator.evaluate("failure", failed).pipe(Effect.exit)
      assert.deepStrictEqual(firstFailure, secondFailure)
      assert.strictEqual(failedLookups, 1)

      let defectLookups = 0
      const defect = "visibility defect"
      const firstDefect = yield* coordinator.evaluate("defect", () => {
        defectLookups++
        return Effect.die(defect)
      }).pipe(Effect.exit)
      const secondDefect = yield* coordinator.evaluate("defect", () => Effect.die(defect)).pipe(Effect.exit)
      assert.strictEqual(defectLookups, 1)
      assert.isTrue(Exit.isFailure(firstDefect))
      assert.isTrue(Exit.isFailure(secondDefect))
      if (Exit.isFailure(firstDefect)) {
        pipe(
          firstDefect.cause.reasons.filter(Cause.isDieReason).map((reason) => reason.defect),
          (defects) => assert.deepStrictEqual(defects, [defect])
        )
      }
      if (Exit.isFailure(secondDefect)) {
        pipe(
          secondDefect.cause.reasons.filter(Cause.isDieReason).map((reason) => reason.defect),
          (defects) => assert.deepStrictEqual(defects, [defect])
        )
      }
    }, Effect.scoped)
  )

  it.effect(
    "shares lookup interruption without stranding followers",
    Effect.fnUntraced(function*() {
      const limiter = yield* makeLimiter()
      const coordinator = WakeVisibility.make<string, boolean, TestError>(limiter)
      let lookups = 0
      const lookup = () => {
        lookups++
        return Effect.interrupt
      }
      const leaderExit = yield* coordinator.evaluate("interrupted", lookup).pipe(Effect.exit)
      const followerExit = yield* coordinator.evaluate("interrupted", lookup).pipe(Effect.exit)
      assert.strictEqual(lookups, 1)
      assert.isTrue(Exit.isFailure(leaderExit))
      assert.isTrue(Exit.isFailure(followerExit))
      if (Exit.isFailure(leaderExit)) {
        assert.isTrue(Cause.hasInterrupts(leaderExit.cause))
      }
      if (Exit.isFailure(followerExit)) {
        assert.isTrue(Cause.hasInterrupts(followerExit.cause))
      }
    }, Effect.scoped)
  )

  it.effect(
    "shares pending and execution bounds across wakes",
    Effect.fnUntraced(function*() {
      const limiter = yield* makeLimiter({
        maximumConcurrentLookups: 1,
        maximumPendingLookups: 2
      })
      const firstWake = WakeVisibility.make<string, boolean, TestError>(limiter)
      const secondWake = WakeVisibility.make<string, boolean, TestError>(limiter)
      const thirdWake = WakeVisibility.make<string, boolean, TestError>(limiter)
      const firstEntered = yield* Deferred.make<void>()
      const secondEntered = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const releaseSecond = yield* Deferred.make<void>()

      const first = yield* firstWake.evaluate("reader-a", () => {
        const enter = Deferred.succeed(firstEntered, undefined)
        return enter.pipe(Effect.andThen(Deferred.await(releaseFirst)), Effect.as(true))
      }).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(firstEntered)
      const second = yield* secondWake.evaluate("reader-b", () => {
        const enter = Deferred.succeed(secondEntered, undefined)
        return enter.pipe(Effect.andThen(Deferred.await(releaseSecond)), Effect.as(true))
      }).pipe(Effect.forkChild({ startImmediately: true }))
      const overflow = yield* thirdWake.evaluate("reader-c", () => Effect.succeed(true)).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow

      assert.deepStrictEqual(yield* limiter.snapshot, { pending: 2, running: 1 })
      assert.isFalse(yield* Deferred.isDone(secondEntered))
      const overflowExit = yield* Fiber.await(overflow)
      assert.deepStrictEqual(overflowExit, Exit.fail(pendingCapacityError))

      yield* Deferred.succeed(releaseFirst, undefined)
      assert.isTrue(yield* Fiber.join(first))
      yield* Deferred.await(secondEntered)
      assert.deepStrictEqual(yield* limiter.snapshot, { pending: 1, running: 1 })
      yield* Deferred.succeed(releaseSecond, undefined)
      assert.isTrue(yield* Fiber.join(second))
      assert.deepStrictEqual(yield* limiter.snapshot, { pending: 0, running: 0 })
    }, Effect.scoped)
  )

  it.effect(
    "times out executing and queued lookups from their admission time",
    Effect.fnUntraced(function*() {
      const limiter = yield* makeLimiter({
        maximumConcurrentLookups: 1,
        maximumPendingLookups: 2
      })
      const firstWake = WakeVisibility.make<string, boolean, TestError>(limiter)
      const secondWake = WakeVisibility.make<string, boolean, TestError>(limiter)
      const firstEntered = yield* Deferred.make<void>()
      let secondEntered = false
      const first = yield* firstWake.evaluate(
        "reader-a",
        () => Deferred.succeed(firstEntered, undefined).pipe(Effect.andThen(Effect.never))
      ).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(firstEntered)
      const second = yield* secondWake.evaluate("reader-b", () =>
        Effect.sync(() => {
          secondEntered = true
          return true
        })).pipe(Effect.forkChild({ startImmediately: true }))

      yield* TestClock.adjust("1 second")
      const firstExit = yield* Fiber.await(first)
      assert.deepStrictEqual(firstExit, Exit.fail(timeoutError))
      const secondExit = yield* Fiber.await(second)
      assert.deepStrictEqual(secondExit, Exit.fail(timeoutError))
      assert.isFalse(secondEntered)
      assert.deepStrictEqual(yield* limiter.snapshot, { pending: 0, running: 0 })
    }, Effect.scoped)
  )
})
