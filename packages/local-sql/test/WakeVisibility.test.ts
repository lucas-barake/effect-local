import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as TestClock from "effect/testing/TestClock"
import * as WakeVisibility from "../src/internal/wakeVisibility.js"

const makeLimiter = (options?: Partial<WakeVisibility.Options<string>>) =>
  WakeVisibility.makeLimiter<string>({
    lookupTimeoutNanos: 1_000_000_000n,
    onLookupTimeout: () => "timeout",
    maximumConcurrentLookups: 2,
    maximumPendingLookups: 16,
    onPendingCapacityExceeded: () => "pending-capacity",
    ...options
  })

describe("wake visibility coordinator", () => {
  it.effect("runs one visibility lookup for concurrent callers with the same canonical key", () =>
    Effect.gen(function*() {
      const limiter = yield* makeLimiter()
      const coordinator = WakeVisibility.make<string, boolean, string>(limiter)
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let lookups = 0
      const lookup = () =>
        Effect.gen(function*() {
          lookups++
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
          return true
        })

      const leader = yield* coordinator.evaluate("canonical-reader", lookup).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(entered)
      const followers = yield* Effect.forEach(
        Array.from({ length: 15 }),
        () => coordinator.evaluate("canonical-reader", lookup).pipe(Effect.forkChild({ startImmediately: true }))
      )

      assert.strictEqual(lookups, 1)
      yield* Deferred.succeed(release, undefined)
      assert.deepStrictEqual(
        yield* Effect.forEach([leader, ...followers], Fiber.join),
        Array.from({ length: 16 }, () => true)
      )
    }).pipe(Effect.scoped))

  it.effect("isolates visibility by the complete canonical key", () =>
    Effect.gen(function*() {
      const limiter = yield* makeLimiter()
      const coordinator = WakeVisibility.make<string, string, string>(limiter)
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
    }).pipe(Effect.scoped))

  it.effect("cancels owner work when its last evaluator detaches and allows a restart", () =>
    Effect.gen(function*() {
      const limiter = yield* makeLimiter()
      const coordinator = WakeVisibility.make<string, boolean, string>(limiter)
      const entered = yield* Deferred.make<void>()
      const lookupInterrupted = yield* Deferred.make<void>()
      let lookups = 0
      const lookup = () =>
        Effect.gen(function*() {
          lookups++
          yield* Deferred.succeed(entered, undefined)
          return yield* Effect.never
        }).pipe(Effect.onInterrupt(() => Deferred.succeed(lookupInterrupted, undefined)))

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
    }).pipe(Effect.scoped))

  it.effect("shares typed failures and defects", () =>
    Effect.gen(function*() {
      const limiter = yield* makeLimiter()
      const coordinator = WakeVisibility.make<string, boolean, string>(limiter)
      let failedLookups = 0
      const failed = () => {
        failedLookups++
        return Effect.fail("denied")
      }
      const firstFailure = yield* Effect.exit(coordinator.evaluate("failure", failed))
      const secondFailure = yield* Effect.exit(coordinator.evaluate("failure", failed))
      assert.deepStrictEqual(firstFailure, secondFailure)
      assert.strictEqual(failedLookups, 1)

      let defectLookups = 0
      const defect = "visibility defect"
      const firstDefect = yield* Effect.exit(
        coordinator.evaluate("defect", () => {
          defectLookups++
          return Effect.die(defect)
        })
      )
      const secondDefect = yield* Effect.exit(coordinator.evaluate("defect", () => Effect.die(defect)))
      assert.strictEqual(defectLookups, 1)
      assert.isTrue(Exit.isFailure(firstDefect))
      assert.isTrue(Exit.isFailure(secondDefect))
      if (Exit.isFailure(firstDefect)) {
        assert.deepStrictEqual(firstDefect.cause.reasons.filter(Cause.isDieReason).map((reason) => reason.defect), [
          defect
        ])
      }
      if (Exit.isFailure(secondDefect)) {
        assert.deepStrictEqual(secondDefect.cause.reasons.filter(Cause.isDieReason).map((reason) => reason.defect), [
          defect
        ])
      }
    }).pipe(Effect.scoped))

  it.effect("shares lookup interruption without stranding followers", () =>
    Effect.gen(function*() {
      const limiter = yield* makeLimiter()
      const coordinator = WakeVisibility.make<string, boolean, string>(limiter)
      let lookups = 0
      const lookup = () => {
        lookups++
        return Effect.interrupt
      }
      const leaderExit = yield* Effect.exit(coordinator.evaluate("interrupted", lookup))
      const followerExit = yield* Effect.exit(coordinator.evaluate("interrupted", lookup))
      assert.strictEqual(lookups, 1)
      assert.isTrue(Exit.isFailure(leaderExit))
      assert.isTrue(Exit.isFailure(followerExit))
      if (Exit.isFailure(leaderExit)) assert.isTrue(Cause.hasInterrupts(leaderExit.cause))
      if (Exit.isFailure(followerExit)) assert.isTrue(Cause.hasInterrupts(followerExit.cause))
    }).pipe(Effect.scoped))

  it.effect("shares pending and execution bounds across wakes", () =>
    Effect.gen(function*() {
      const limiter = yield* makeLimiter({
        maximumConcurrentLookups: 1,
        maximumPendingLookups: 2
      })
      const firstWake = WakeVisibility.make<string, boolean, string>(limiter)
      const secondWake = WakeVisibility.make<string, boolean, string>(limiter)
      const thirdWake = WakeVisibility.make<string, boolean, string>(limiter)
      const firstEntered = yield* Deferred.make<void>()
      const secondEntered = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const releaseSecond = yield* Deferred.make<void>()

      const first = yield* firstWake.evaluate("reader-a", () =>
        Deferred.succeed(firstEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releaseFirst)),
          Effect.as(true)
        )).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(firstEntered)
      const second = yield* secondWake.evaluate("reader-b", () =>
        Deferred.succeed(secondEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releaseSecond)),
          Effect.as(true)
        )).pipe(Effect.forkChild({ startImmediately: true }))
      const overflow = yield* thirdWake.evaluate("reader-c", () => Effect.succeed(true)).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow

      assert.deepStrictEqual(yield* limiter.snapshot, { pending: 2, running: 1 })
      assert.isFalse(yield* Deferred.isDone(secondEntered))
      assert.deepStrictEqual(yield* Fiber.await(overflow), Exit.fail("pending-capacity"))

      yield* Deferred.succeed(releaseFirst, undefined)
      assert.isTrue(yield* Fiber.join(first))
      yield* Deferred.await(secondEntered)
      assert.deepStrictEqual(yield* limiter.snapshot, { pending: 1, running: 1 })
      yield* Deferred.succeed(releaseSecond, undefined)
      assert.isTrue(yield* Fiber.join(second))
      assert.deepStrictEqual(yield* limiter.snapshot, { pending: 0, running: 0 })
    }).pipe(Effect.scoped))

  it.effect("times out executing and queued lookups from their admission time", () =>
    Effect.gen(function*() {
      const limiter = yield* makeLimiter({
        maximumConcurrentLookups: 1,
        maximumPendingLookups: 2
      })
      const firstWake = WakeVisibility.make<string, boolean, string>(limiter)
      const secondWake = WakeVisibility.make<string, boolean, string>(limiter)
      const firstEntered = yield* Deferred.make<void>()
      let secondEntered = false
      const first = yield* firstWake.evaluate("reader-a", () =>
        Deferred.succeed(firstEntered, undefined).pipe(Effect.andThen(Effect.never))).pipe(
          Effect.forkChild({ startImmediately: true })
        )
      yield* Deferred.await(firstEntered)
      const second = yield* secondWake.evaluate("reader-b", () =>
        Effect.sync(() => {
          secondEntered = true
          return true
        })).pipe(Effect.forkChild({ startImmediately: true }))

      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(yield* Fiber.await(first), Exit.fail("timeout"))
      assert.deepStrictEqual(yield* Fiber.await(second), Exit.fail("timeout"))
      assert.isFalse(secondEntered)
      assert.deepStrictEqual(yield* limiter.snapshot, { pending: 0, running: 0 })
    }).pipe(Effect.scoped))
})
