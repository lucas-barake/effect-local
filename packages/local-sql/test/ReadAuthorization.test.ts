import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as TestClock from "effect/testing/TestClock"
import * as ReadAuthorization from "../src/internal/readAuthorization.js"

interface Key {
  readonly tenant: string
  readonly path: ReadonlyArray<string>
}

type TestErrorReason = "timeout" | "pending-capacity" | "revoked" | "denied"

class TestError {
  readonly _tag = "TestError"

  constructor(readonly reason: TestErrorReason) {}
}

const timeoutError = new TestError("timeout")
const pendingCapacityError = new TestError("pending-capacity")
const revokedError = new TestError("revoked")
const deniedError = new TestError("denied")

const key = (tenant: string): Key => ({ tenant, path: ["spaces", "read"] })

const make = (options?: Partial<ReadAuthorization.Options<TestError>>) =>
  ReadAuthorization.make<Key, string, TestError>({
    refreshIntervalNanos: 1_000_000_000n,
    lookupTimeoutNanos: 1_000_000_000n,
    onLookupTimeout: () => timeoutError,
    maximumConcurrentLookups: 2,
    maximumPendingLookups: 16,
    onPendingCapacityExceeded: () => pendingCapacityError,
    completedCacheCapacity: 16,
    ...options
  })

describe("read authorization coordinator", () => {
  it.effect(
    "single flights equal structural keys and shared refreshes",
    Effect.fnUntraced(function*() {
      const coordinator = yield* make()
      const initialEntered = yield* Deferred.make<void>()
      const releaseInitial = yield* Deferred.make<void>()
      let initialLookups = 0
      const initial = Effect.fnUntraced(function*() {
        initialLookups++
        yield* Deferred.succeed(initialEntered, undefined)
        yield* Deferred.await(releaseInitial)
        return "initial"
      })

      const first = yield* coordinator.authorize(key("one"), initial).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(initialEntered)
      const second = yield* coordinator.authorize(key("one"), initial).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      assert.strictEqual(initialLookups, 1)
      yield* Deferred.succeed(releaseInitial, undefined)
      const [firstSuccess, secondSuccess] = yield* pipe([Fiber.join(first), Fiber.join(second)], Effect.all)
      assert.strictEqual(firstSuccess.generation, secondSuccess.generation)

      const refreshEntered = yield* Deferred.make<void>()
      const releaseRefresh = yield* Deferred.make<void>()
      let refreshLookups = 0
      const refresh = Effect.fnUntraced(function*() {
        refreshLookups++
        yield* Deferred.succeed(refreshEntered, undefined)
        yield* Deferred.await(releaseRefresh)
        return "refreshed"
      })
      const refreshing = yield* coordinator.refresh(key("one"), firstSuccess.generation, refresh).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(refreshEntered)
      const following = yield* coordinator.refresh(key("one"), firstSuccess.generation, refresh).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      assert.strictEqual(refreshLookups, 1)
      yield* Deferred.succeed(releaseRefresh, undefined)
      const [refreshed, followed] = yield* pipe([Fiber.join(refreshing), Fiber.join(following)], Effect.all)
      assert.strictEqual(refreshed.value, "refreshed")
      assert.strictEqual(refreshed.generation, followed.generation)
      assert.isTrue(refreshed.generation > firstSuccess.generation)
      const current = yield* pipe(key("one"), coordinator.current)
      assert.deepStrictEqual(current, Option.some(refreshed))
    }, Effect.scoped)
  )

  it.effect(
    "invalidates the matching cached authorization when its refresh is denied",
    Effect.fnUntraced(function*() {
      const coordinator = yield* make()
      const initial = yield* coordinator.authorize(key("revoked"), () => Effect.succeed("allowed"))

      const denied = yield* coordinator.refresh(key("revoked"), initial.generation, () => Effect.fail(revokedError))
        .pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(denied))
      const current = yield* pipe(key("revoked"), coordinator.current)
      assert.isTrue(Option.isNone(current))

      let replacementLookups = 0
      const replacement = yield* coordinator.authorize(key("revoked"), () => {
        replacementLookups++
        return Effect.succeed("allowed-again")
      })
      assert.strictEqual(replacementLookups, 1)
      assert.strictEqual(replacement.value, "allowed-again")
      assert.isTrue(replacement.generation > initial.generation)
    }, Effect.scoped)
  )

  it.effect(
    "bounds distinct lookup concurrency",
    Effect.fnUntraced(function*() {
      const coordinator = yield* make({ maximumConcurrentLookups: 2 })
      const twoEntered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let running = 0
      let maximumRunning = 0
      let entered = 0
      const lookup = Effect.fnUntraced(function*(input: Key) {
        running++
        entered++
        maximumRunning = Math.max(maximumRunning, running)
        if (entered === 2) yield* Deferred.succeed(twoEntered, undefined)
        yield* Deferred.await(release)
        running--
        return input.tenant
      })

      const fibers = yield* Effect.forEach(
        ["a", "b", "c", "d", "e"],
        (tenant) => coordinator.authorize(key(tenant), lookup).pipe(Effect.forkChild({ startImmediately: true }))
      )
      yield* Deferred.await(twoEntered)
      assert.strictEqual(maximumRunning, 2)
      assert.strictEqual(entered, 2)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 5, requesting: 5, completed: 0 })
      yield* Deferred.succeed(release, undefined)
      yield* Effect.forEach(fibers, Fiber.join)
      assert.strictEqual(maximumRunning, 2)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 5 })
    }, Effect.scoped)
  )

  it.effect(
    "bounds every live caller while equal-key callers still single flight",
    Effect.fnUntraced(function*() {
      const coordinator = yield* make({
        maximumConcurrentLookups: 2,
        maximumPendingLookups: 2,
        onPendingCapacityExceeded: () => pendingCapacityError
      })
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let lookups = 0
      const blocked = Effect.fnUntraced(function*() {
        lookups++
        yield* Deferred.succeed(entered, undefined)
        yield* Deferred.await(release)
        return "allowed"
      })

      const leader = yield* coordinator.authorize(key("shared"), blocked).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(entered)
      const follower = yield* coordinator.authorize(key("shared"), blocked).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      const overflow = yield* coordinator.authorize(key("shared"), blocked).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow

      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 1, requesting: 2, completed: 0 })
      const overflowExit = yield* Fiber.await(overflow)
      assert.deepStrictEqual(overflowExit, Exit.fail(pendingCapacityError))
      assert.strictEqual(lookups, 1)

      yield* Deferred.succeed(release, undefined)
      const [leaderSuccess, followerSuccess] = yield* pipe([Fiber.join(leader), Fiber.join(follower)], Effect.all)
      assert.strictEqual(leaderSuccess.generation, followerSuccess.generation)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 1 })
    }, Effect.scoped)
  )

  it.effect(
    "bounds detached distinct owner work until its lookup exits",
    Effect.fnUntraced(function*() {
      const coordinator = yield* make({
        maximumConcurrentLookups: 1,
        maximumPendingLookups: 2,
        onPendingCapacityExceeded: () => pendingCapacityError
      })
      const firstEntered = yield* Deferred.make<void>()
      const first = yield* coordinator.authorize(
        key("first"),
        () => Deferred.succeed(firstEntered, undefined).pipe(Effect.andThen(Effect.never))
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(firstEntered)
      const second = yield* coordinator.authorize(key("second"), () => Effect.never).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, requesting: 2, completed: 0 })

      yield* Fiber.interrupt(first)
      yield* Fiber.interrupt(second)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, requesting: 0, completed: 0 })

      const overflow = yield* coordinator.authorize(key("overflow"), () => Effect.succeed("overflow")).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow
      pipe(pendingCapacityError, Exit.fail, (expected) => assert.deepStrictEqual(overflow.pollUnsafe(), expected))
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, requesting: 0, completed: 0 })
    }, Effect.scoped)
  )

  it.effect(
    "expires queued owner work before it can run stale policy lookups",
    Effect.fnUntraced(function*() {
      const coordinator = yield* make({ maximumConcurrentLookups: 1 })
      const firstEntered = yield* Deferred.make<void>()
      let queuedEntered = false
      const first = yield* coordinator.authorize(
        key("first"),
        () => Deferred.succeed(firstEntered, undefined).pipe(Effect.andThen(Effect.never))
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(firstEntered)
      const queued = yield* coordinator.authorize(
        key("queued"),
        () =>
          Effect.sync(() => {
            queuedEntered = true
            return "queued"
          })
      ).pipe(Effect.forkChild({ startImmediately: true }))
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, requesting: 2, completed: 0 })
      yield* Effect.yieldNow

      yield* TestClock.adjust("2 seconds")
      yield* Effect.yieldNow
      assert.isTrue(Exit.isFailure(yield* Fiber.await(first)))
      assert.isTrue(Exit.isFailure(yield* Fiber.await(queued)))
      assert.isFalse(queuedEntered)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 0 })
      const replacement = yield* coordinator.authorize(key("replacement"), () => Effect.succeed("replacement"))
      assert.strictEqual(replacement.value, "replacement")
    }, Effect.scoped)
  )

  it.effect(
    "detaches interrupted leader and follower callers without canceling shared work",
    Effect.fnUntraced(function*() {
      const coordinator = yield* make()
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      let lookups = 0
      const lookup = Effect.fnUntraced(function*() {
        lookups++
        yield* Deferred.succeed(entered, undefined)
        yield* Deferred.await(release)
        return "allowed"
      }, Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)))

      const leader = yield* coordinator.authorize(key("shared"), lookup).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(entered)
      const follower = yield* coordinator.authorize(key("shared"), lookup).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      const observer = yield* coordinator.authorize(key("shared"), lookup).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Fiber.interrupt(leader)
      yield* Fiber.interrupt(follower)
      assert.isFalse(yield* Deferred.isDone(interrupted))
      assert.strictEqual(lookups, 1)
      yield* Deferred.succeed(release, undefined)
      const observed = yield* Fiber.join(observer)
      assert.strictEqual(observed.value, "allowed")
      assert.isFalse(yield* Deferred.isDone(interrupted))
    }, Effect.scoped)
  )

  it.effect(
    "releases pending work and permits after every lookup exit",
    Effect.fnUntraced(function*() {
      const coordinator = yield* make({ maximumConcurrentLookups: 1 })
      let typedFailures = 0
      const denied = () => {
        typedFailures++
        return Effect.fail(deniedError)
      }

      const firstDenied = yield* coordinator.authorize(key("denied"), denied).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(firstDenied))
      const secondDenied = yield* coordinator.authorize(key("denied"), denied).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(secondDenied))
      assert.strictEqual(typedFailures, 2)
      const afterDenial = yield* coordinator.authorize(key("after-denial"), () => Effect.succeed("ok"))
      assert.strictEqual(afterDenial.value, "ok")

      const defect = yield* coordinator.authorize(key("defect"), () => Effect.die("broken")).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(defect))
      const afterDefect = yield* coordinator.authorize(key("after-defect"), () => Effect.succeed("ok"))
      assert.strictEqual(afterDefect.value, "ok")

      const interruption = yield* coordinator.authorize(key("interrupt"), () => Effect.interrupt).pipe(Effect.exit)
      assert.isTrue(Exit.isFailure(interruption))
      const afterInterrupt = yield* coordinator.authorize(key("after-interrupt"), () => Effect.succeed("ok"))
      assert.strictEqual(afterInterrupt.value, "ok")
      const success = yield* coordinator.authorize(key("success"), () => Effect.succeed("ok"))
      assert.strictEqual(success.value, "ok")
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 4 })
    }, Effect.scoped)
  )

  it.effect(
    "bounds the successful cache under sequential churn",
    Effect.fnUntraced(function*() {
      const coordinator = yield* make({ completedCacheCapacity: 2 })
      let lookups = 0
      const lookup = (input: Key) => {
        lookups++
        return Effect.succeed(input.tenant)
      }
      yield* coordinator.authorize(key("a"), lookup)
      yield* coordinator.authorize(key("b"), lookup)
      yield* coordinator.authorize(key("c"), lookup)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 2 })
      assert.isTrue(Option.isNone(yield* pipe(key("a"), coordinator.current)))
      assert.isTrue(Option.isSome(yield* pipe(key("b"), coordinator.current)))
      assert.isTrue(Option.isSome(yield* pipe(key("c"), coordinator.current)))
      yield* coordinator.authorize(key("a"), lookup)
      assert.strictEqual(lookups, 4)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 2 })
    }, Effect.scoped)
  )

  it.effect(
    "eagerly removes successes at the exact monotonic expiry",
    Effect.fnUntraced(function*() {
      const coordinator = yield* make({ refreshIntervalNanos: 1_000_000_000n })
      const first = yield* coordinator.authorize(key("a"), () => Effect.succeed("a"))
      yield* coordinator.authorize(key("b"), () => Effect.succeed("b"))
      assert.strictEqual(first.expiresAtNanos, 1_000_000_000n)
      yield* TestClock.adjust("999 millis")
      assert.isTrue(Option.isSome(yield* pipe(key("a"), coordinator.current)))
      yield* TestClock.adjust("1 milli")
      assert.isTrue(Option.isNone(yield* pipe(key("a"), coordinator.current)))
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 0 })
    }, Effect.scoped)
  )

  it.effect(
    "interrupts and completes pending work when its owner scope closes",
    Effect.fnUntraced(function*() {
      const owner = yield* Scope.make()
      const coordinator = yield* make({ maximumConcurrentLookups: 1 }).pipe(Scope.provide(owner))
      const entered = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const caller = yield* coordinator.authorize(
        key("owner"),
        () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined))
          )
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(entered)
      const queued = yield* coordinator.authorize(key("queued"), () => Effect.never).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, requesting: 2, completed: 0 })
      yield* Scope.close(owner, Exit.void)
      yield* Deferred.await(interrupted)
      const [callerExit, queuedExit] = yield* pipe([Fiber.await(caller), Fiber.await(queued)], Effect.all)
      assert.isTrue(Exit.isFailure(callerExit))
      if (Exit.isFailure(callerExit)) assert.isAbove(Cause.interruptors(callerExit.cause).size, 0)
      assert.isTrue(Exit.isFailure(queuedExit))
      if (Exit.isFailure(queuedExit)) assert.isAbove(Cause.interruptors(queuedExit.cause).size, 0)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 0 })
    }, Effect.scoped)
  )
})
