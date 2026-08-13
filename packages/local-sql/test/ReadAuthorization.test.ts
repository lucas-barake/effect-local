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
  it.effect("single flights equal structural keys and shared refreshes", () =>
    Effect.gen(function*() {
      const coordinator = yield* make()
      const initialEntered = yield* Deferred.make<void>()
      const releaseInitial = yield* Deferred.make<void>()
      let initialLookups = 0
      const initial = () =>
        Effect.gen(function*() {
          initialLookups++
          yield* Deferred.succeed(initialEntered, undefined)
          yield* Deferred.await(releaseInitial)
          return "initial"
        })

      const first = yield* pipe(
        key("one"),
        (authorizationKey) => coordinator.authorize(authorizationKey, initial),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(initialEntered)
      const second = yield* pipe(
        key("one"),
        (authorizationKey) => coordinator.authorize(authorizationKey, initial),
        Effect.forkChild({ startImmediately: true })
      )
      assert.strictEqual(initialLookups, 1)
      yield* Deferred.succeed(releaseInitial, undefined)
      const [firstSuccess, secondSuccess] = yield* pipe([Fiber.join(first), Fiber.join(second)], Effect.all)
      assert.strictEqual(firstSuccess.generation, secondSuccess.generation)

      const refreshEntered = yield* Deferred.make<void>()
      const releaseRefresh = yield* Deferred.make<void>()
      let refreshLookups = 0
      const refresh = () =>
        Effect.gen(function*() {
          refreshLookups++
          yield* Deferred.succeed(refreshEntered, undefined)
          yield* Deferred.await(releaseRefresh)
          return "refreshed"
        })
      const refreshing = yield* pipe(
        key("one"),
        (authorizationKey) => coordinator.refresh(authorizationKey, firstSuccess.generation, refresh),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(refreshEntered)
      const following = yield* pipe(
        key("one"),
        (authorizationKey) => coordinator.refresh(authorizationKey, firstSuccess.generation, refresh),
        Effect.forkChild({ startImmediately: true })
      )
      assert.strictEqual(refreshLookups, 1)
      yield* Deferred.succeed(releaseRefresh, undefined)
      const [refreshed, followed] = yield* pipe([Fiber.join(refreshing), Fiber.join(following)], Effect.all)
      assert.strictEqual(refreshed.value, "refreshed")
      assert.strictEqual(refreshed.generation, followed.generation)
      assert.isTrue(refreshed.generation > firstSuccess.generation)
      const current = yield* pipe(key("one"), coordinator.current)
      pipe(refreshed, Option.some, (expected) => assert.deepStrictEqual(current, expected))
    }).pipe(Effect.scoped))

  it.effect("invalidates the matching cached authorization when its refresh is denied", () =>
    Effect.gen(function*() {
      const coordinator = yield* make()
      const initial = yield* pipe(
        key("revoked"),
        (authorizationKey) => coordinator.authorize(authorizationKey, () => Effect.succeed("allowed"))
      )

      const denied = yield* pipe(
        key("revoked"),
        (authorizationKey) =>
          coordinator.refresh(authorizationKey, initial.generation, () => Effect.fail(revokedError)),
        Effect.exit
      )
      pipe(denied, Exit.isFailure, (isFailure) => assert.isTrue(isFailure))
      const current = yield* pipe(key("revoked"), coordinator.current)
      pipe(current, Option.isNone, (isNone) => assert.isTrue(isNone))

      let replacementLookups = 0
      const replacement = yield* pipe(key("revoked"), (authorizationKey) =>
        coordinator.authorize(authorizationKey, () => {
          replacementLookups++
          return Effect.succeed("allowed-again")
        }))
      assert.strictEqual(replacementLookups, 1)
      assert.strictEqual(replacement.value, "allowed-again")
      assert.isTrue(replacement.generation > initial.generation)
    }).pipe(Effect.scoped))

  it.effect("bounds distinct lookup concurrency", () =>
    Effect.gen(function*() {
      const coordinator = yield* make({ maximumConcurrentLookups: 2 })
      const twoEntered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let running = 0
      let maximumRunning = 0
      let entered = 0
      const lookup = (input: Key) =>
        Effect.gen(function*() {
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
        (tenant) =>
          pipe(
            key(tenant),
            (authorizationKey) => coordinator.authorize(authorizationKey, lookup),
            Effect.forkChild({ startImmediately: true })
          )
      )
      yield* Deferred.await(twoEntered)
      assert.strictEqual(maximumRunning, 2)
      assert.strictEqual(entered, 2)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 5, requesting: 5, completed: 0 })
      yield* Deferred.succeed(release, undefined)
      yield* Effect.forEach(fibers, Fiber.join)
      assert.strictEqual(maximumRunning, 2)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 5 })
    }).pipe(Effect.scoped))

  it.effect("bounds every live caller while equal-key callers still single flight", () =>
    Effect.gen(function*() {
      const coordinator = yield* make({
        maximumConcurrentLookups: 2,
        maximumPendingLookups: 2,
        onPendingCapacityExceeded: () => pendingCapacityError
      })
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let lookups = 0
      const blocked = () =>
        Effect.gen(function*() {
          lookups++
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
          return "allowed"
        })

      const leader = yield* pipe(
        key("shared"),
        (authorizationKey) => coordinator.authorize(authorizationKey, blocked),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(entered)
      const follower = yield* pipe(
        key("shared"),
        (authorizationKey) => coordinator.authorize(authorizationKey, blocked),
        Effect.forkChild({ startImmediately: true })
      )
      const overflow = yield* pipe(
        key("shared"),
        (authorizationKey) => coordinator.authorize(authorizationKey, blocked),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow

      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 1, requesting: 2, completed: 0 })
      const overflowExit = yield* Fiber.await(overflow)
      pipe(pendingCapacityError, Exit.fail, (expected) => assert.deepStrictEqual(overflowExit, expected))
      assert.strictEqual(lookups, 1)

      yield* Deferred.succeed(release, undefined)
      const [leaderSuccess, followerSuccess] = yield* pipe([Fiber.join(leader), Fiber.join(follower)], Effect.all)
      assert.strictEqual(leaderSuccess.generation, followerSuccess.generation)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 1 })
    }).pipe(Effect.scoped))

  it.effect("bounds detached distinct owner work until its lookup exits", () =>
    Effect.gen(function*() {
      const coordinator = yield* make({
        maximumConcurrentLookups: 1,
        maximumPendingLookups: 2,
        onPendingCapacityExceeded: () => pendingCapacityError
      })
      const firstEntered = yield* Deferred.make<void>()
      const first = yield* pipe(
        key("first"),
        (authorizationKey) =>
          coordinator.authorize(
            authorizationKey,
            () => Deferred.succeed(firstEntered, undefined).pipe(Effect.andThen(Effect.never))
          ),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(firstEntered)
      const second = yield* pipe(
        key("second"),
        (authorizationKey) => coordinator.authorize(authorizationKey, () => Effect.never),
        Effect.forkChild({ startImmediately: true })
      )
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, requesting: 2, completed: 0 })

      yield* Fiber.interrupt(first)
      yield* Fiber.interrupt(second)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, requesting: 0, completed: 0 })

      const overflow = yield* pipe(
        key("overflow"),
        (authorizationKey) => coordinator.authorize(authorizationKey, () => Effect.succeed("overflow")),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow
      pipe(overflow.pollUnsafe(), (actual) =>
        pipe(pendingCapacityError, Exit.fail, (expected) => assert.deepStrictEqual(actual, expected)))
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, requesting: 0, completed: 0 })
    }).pipe(Effect.scoped))

  it.effect("expires queued owner work before it can run stale policy lookups", () =>
    Effect.gen(function*() {
      const coordinator = yield* make({ maximumConcurrentLookups: 1 })
      const firstEntered = yield* Deferred.make<void>()
      let queuedEntered = false
      const first = yield* pipe(
        key("first"),
        (authorizationKey) =>
          coordinator.authorize(
            authorizationKey,
            () => Deferred.succeed(firstEntered, undefined).pipe(Effect.andThen(Effect.never))
          ),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(firstEntered)
      const queued = yield* pipe(
        key("queued"),
        (authorizationKey) =>
          coordinator.authorize(
            authorizationKey,
            () =>
              Effect.sync(() => {
                queuedEntered = true
                return "queued"
              })
          ),
        Effect.forkChild({ startImmediately: true })
      )
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, requesting: 2, completed: 0 })
      yield* Effect.yieldNow

      yield* TestClock.adjust("2 seconds")
      yield* Effect.yieldNow
      pipe(yield* Fiber.await(first), Exit.isFailure, (isFailure) => assert.isTrue(isFailure))
      pipe(yield* Fiber.await(queued), Exit.isFailure, (isFailure) => assert.isTrue(isFailure))
      assert.isFalse(queuedEntered)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 0 })
      const replacement = yield* pipe(
        key("replacement"),
        (authorizationKey) => coordinator.authorize(authorizationKey, () => Effect.succeed("replacement"))
      )
      assert.strictEqual(replacement.value, "replacement")
    }).pipe(Effect.scoped))

  it.effect("detaches interrupted leader and follower callers without canceling shared work", () =>
    Effect.gen(function*() {
      const coordinator = yield* make()
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      let lookups = 0
      const lookup = () =>
        Effect.gen(function*() {
          lookups++
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
          return "allowed"
        }).pipe(Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)))

      const leader = yield* pipe(
        key("shared"),
        (authorizationKey) => coordinator.authorize(authorizationKey, lookup),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(entered)
      const follower = yield* pipe(
        key("shared"),
        (authorizationKey) => coordinator.authorize(authorizationKey, lookup),
        Effect.forkChild({ startImmediately: true })
      )
      const observer = yield* pipe(
        key("shared"),
        (authorizationKey) => coordinator.authorize(authorizationKey, lookup),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Fiber.interrupt(leader)
      yield* Fiber.interrupt(follower)
      pipe(yield* Deferred.isDone(interrupted), (isDone) => assert.isFalse(isDone))
      assert.strictEqual(lookups, 1)
      yield* Deferred.succeed(release, undefined)
      const observed = yield* Fiber.join(observer)
      assert.strictEqual(observed.value, "allowed")
      pipe(yield* Deferred.isDone(interrupted), (isDone) => assert.isFalse(isDone))
    }).pipe(Effect.scoped))

  it.effect("releases pending work and permits after every lookup exit", () =>
    Effect.gen(function*() {
      const coordinator = yield* make({ maximumConcurrentLookups: 1 })
      let typedFailures = 0
      const denied = () => {
        typedFailures++
        return Effect.fail(deniedError)
      }

      const firstDenied = yield* pipe(key("denied"), (authorizationKey) =>
        coordinator.authorize(authorizationKey, denied), Effect.exit)
      pipe(firstDenied, Exit.isFailure, (isFailure) =>
        assert.isTrue(isFailure))
      const secondDenied = yield* pipe(key("denied"), (authorizationKey) =>
        coordinator.authorize(authorizationKey, denied), Effect.exit)
      pipe(secondDenied, Exit.isFailure, (isFailure) =>
        assert.isTrue(isFailure))
      assert.strictEqual(typedFailures, 2)
      const afterDenial = yield* pipe(
        key("after-denial"),
        (authorizationKey) =>
          coordinator.authorize(authorizationKey, () =>
            Effect.succeed("ok"))
      )
      assert.strictEqual(afterDenial.value, "ok")

      const defect = yield* pipe(
        key("defect"),
        (authorizationKey) =>
          coordinator.authorize(authorizationKey, () => Effect.die("broken")),
        Effect.exit
      )
      pipe(defect, Exit.isFailure, (isFailure) => assert.isTrue(isFailure))
      const afterDefect = yield* pipe(
        key("after-defect"),
        (authorizationKey) => coordinator.authorize(authorizationKey, () => Effect.succeed("ok"))
      )
      assert.strictEqual(afterDefect.value, "ok")

      const interruption = yield* pipe(
        key("interrupt"),
        (authorizationKey) => coordinator.authorize(authorizationKey, () => Effect.interrupt),
        Effect.exit
      )
      pipe(interruption, Exit.isFailure, (isFailure) => assert.isTrue(isFailure))
      const afterInterrupt = yield* pipe(
        key("after-interrupt"),
        (authorizationKey) => coordinator.authorize(authorizationKey, () => Effect.succeed("ok"))
      )
      assert.strictEqual(afterInterrupt.value, "ok")
      const success = yield* pipe(
        key("success"),
        (authorizationKey) => coordinator.authorize(authorizationKey, () => Effect.succeed("ok"))
      )
      assert.strictEqual(success.value, "ok")
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 4 })
    }).pipe(Effect.scoped))

  it.effect("bounds the successful cache under sequential churn", () =>
    Effect.gen(function*() {
      const coordinator = yield* make({ completedCacheCapacity: 2 })
      let lookups = 0
      const lookup = (input: Key) => {
        lookups++
        return Effect.succeed(input.tenant)
      }
      yield* pipe(key("a"), (authorizationKey) => coordinator.authorize(authorizationKey, lookup))
      yield* pipe(key("b"), (authorizationKey) => coordinator.authorize(authorizationKey, lookup))
      yield* pipe(key("c"), (authorizationKey) => coordinator.authorize(authorizationKey, lookup))
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 2 })
      pipe(yield* pipe(key("a"), coordinator.current), Option.isNone, (isNone) => assert.isTrue(isNone))
      pipe(yield* pipe(key("b"), coordinator.current), Option.isSome, (isSome) => assert.isTrue(isSome))
      pipe(yield* pipe(key("c"), coordinator.current), Option.isSome, (isSome) => assert.isTrue(isSome))
      yield* pipe(key("a"), (authorizationKey) => coordinator.authorize(authorizationKey, lookup))
      assert.strictEqual(lookups, 4)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 2 })
    }).pipe(Effect.scoped))

  it.effect("eagerly removes successes at the exact monotonic expiry", () =>
    Effect.gen(function*() {
      const coordinator = yield* make({ refreshIntervalNanos: 1_000_000_000n })
      const first = yield* pipe(key("a"), (authorizationKey) =>
        coordinator.authorize(authorizationKey, () => Effect.succeed("a")))
      yield* pipe(key("b"), (authorizationKey) =>
        coordinator.authorize(authorizationKey, () => Effect.succeed("b")))
      assert.strictEqual(first.expiresAtNanos, 1_000_000_000n)
      yield* TestClock.adjust("999 millis")
      pipe(yield* pipe(key("a"), coordinator.current), Option.isSome, (isSome) => assert.isTrue(isSome))
      yield* TestClock.adjust("1 milli")
      pipe(yield* pipe(key("a"), coordinator.current), Option.isNone, (isNone) => assert.isTrue(isNone))
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 0 })
    }).pipe(Effect.scoped))

  it.effect("interrupts and completes pending work when its owner scope closes", () =>
    Effect.gen(function*() {
      const owner = yield* Scope.make()
      const coordinator = yield* make({ maximumConcurrentLookups: 1 }).pipe(Scope.provide(owner))
      const entered = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const caller = yield* pipe(
        key("owner"),
        (authorizationKey) =>
          coordinator.authorize(
            authorizationKey,
            () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined))
              )
          ),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(entered)
      const queued = yield* pipe(
        key("queued"),
        (authorizationKey) => coordinator.authorize(authorizationKey, () => Effect.never),
        Effect.forkChild({ startImmediately: true })
      )
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, requesting: 2, completed: 0 })
      yield* Scope.close(owner, Exit.void)
      yield* Deferred.await(interrupted)
      const [callerExit, queuedExit] = yield* pipe([Fiber.await(caller), Fiber.await(queued)], Effect.all)
      pipe(callerExit, Exit.isFailure, (isFailure) => assert.isTrue(isFailure))
      if (Exit.isFailure(callerExit)) pipe(callerExit.cause, Cause.interruptors, (ids) => assert.isAbove(ids.size, 0))
      pipe(queuedExit, Exit.isFailure, (isFailure) => assert.isTrue(isFailure))
      if (Exit.isFailure(queuedExit)) pipe(queuedExit.cause, Cause.interruptors, (ids) => assert.isAbove(ids.size, 0))
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, requesting: 0, completed: 0 })
    }).pipe(Effect.scoped))
})
