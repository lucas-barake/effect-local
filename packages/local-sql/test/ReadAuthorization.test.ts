import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as TestClock from "effect/testing/TestClock"
import * as ReadAuthorization from "../src/internal/readAuthorization.js"

interface Key {
  readonly tenant: string
  readonly path: ReadonlyArray<string>
}

const key = (tenant: string): Key => ({ tenant, path: ["spaces", "read"] })

const make = (options?: Partial<ReadAuthorization.Options<string>>) =>
  ReadAuthorization.make<Key, string, string>({
    refreshInterval: "1 second",
    lookupTimeout: "1 second",
    onLookupTimeout: () => "timeout",
    maximumConcurrentLookups: 2,
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

      const first = yield* coordinator.authorize(key("one"), initial).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(initialEntered)
      const second = yield* coordinator.authorize(key("one"), initial).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      assert.strictEqual(initialLookups, 1)
      yield* Deferred.succeed(releaseInitial, undefined)
      const [firstSuccess, secondSuccess] = yield* Effect.all([Fiber.join(first), Fiber.join(second)])
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
      const refreshing = yield* coordinator.refresh(key("one"), firstSuccess.generation, refresh).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(refreshEntered)
      const following = yield* coordinator.refresh(key("one"), firstSuccess.generation, refresh).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      assert.strictEqual(refreshLookups, 1)
      yield* Deferred.succeed(releaseRefresh, undefined)
      const [refreshed, followed] = yield* Effect.all([Fiber.join(refreshing), Fiber.join(following)])
      assert.strictEqual(refreshed.value, "refreshed")
      assert.strictEqual(refreshed.generation, followed.generation)
      assert.isTrue(refreshed.generation > firstSuccess.generation)
      assert.deepStrictEqual(yield* coordinator.current(key("one")), Option.some(refreshed))
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
        (tenant) => coordinator.authorize(key(tenant), lookup).pipe(Effect.forkChild({ startImmediately: true }))
      )
      yield* Deferred.await(twoEntered)
      assert.strictEqual(maximumRunning, 2)
      assert.strictEqual(entered, 2)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 5, completed: 0 })
      yield* Deferred.succeed(release, undefined)
      yield* Effect.forEach(fibers, Fiber.join)
      assert.strictEqual(maximumRunning, 2)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, completed: 5 })
    }).pipe(Effect.scoped))

  it.effect("expires queued owner work before it can run stale policy lookups", () =>
    Effect.gen(function*() {
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
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, completed: 0 })
      yield* Effect.yieldNow

      yield* TestClock.adjust("2 seconds")
      yield* Effect.yieldNow
      assert.isTrue(Exit.isFailure(yield* Fiber.await(first)))
      assert.isTrue(Exit.isFailure(yield* Fiber.await(queued)))
      assert.isFalse(queuedEntered)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, completed: 0 })
      assert.strictEqual(
        (yield* coordinator.authorize(key("replacement"), () => Effect.succeed("replacement"))).value,
        "replacement"
      )
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
      assert.strictEqual((yield* Fiber.join(observer)).value, "allowed")
      assert.isFalse(yield* Deferred.isDone(interrupted))
    }).pipe(Effect.scoped))

  it.effect("releases pending work and permits after every lookup exit", () =>
    Effect.gen(function*() {
      const coordinator = yield* make({ maximumConcurrentLookups: 1 })
      let typedFailures = 0
      const denied = () => {
        typedFailures++
        return Effect.fail("denied")
      }

      assert.isTrue(Exit.isFailure(yield* Effect.exit(coordinator.authorize(key("denied"), denied))))
      assert.isTrue(Exit.isFailure(yield* Effect.exit(coordinator.authorize(key("denied"), denied))))
      assert.strictEqual(typedFailures, 2)
      assert.strictEqual((yield* coordinator.authorize(key("after-denial"), () => Effect.succeed("ok"))).value, "ok")

      const defect = yield* Effect.exit(coordinator.authorize(key("defect"), () => Effect.die("broken")))
      assert.isTrue(Exit.isFailure(defect))
      assert.strictEqual((yield* coordinator.authorize(key("after-defect"), () => Effect.succeed("ok"))).value, "ok")

      const interruption = yield* Effect.exit(coordinator.authorize(key("interrupt"), () => Effect.interrupt))
      assert.isTrue(Exit.isFailure(interruption))
      assert.strictEqual((yield* coordinator.authorize(key("after-interrupt"), () => Effect.succeed("ok"))).value, "ok")
      assert.strictEqual((yield* coordinator.authorize(key("success"), () => Effect.succeed("ok"))).value, "ok")
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, completed: 4 })
    }).pipe(Effect.scoped))

  it.effect("bounds the successful cache under sequential churn", () =>
    Effect.gen(function*() {
      const coordinator = yield* make({ completedCacheCapacity: 2 })
      let lookups = 0
      const lookup = (input: Key) => {
        lookups++
        return Effect.succeed(input.tenant)
      }
      yield* coordinator.authorize(key("a"), lookup)
      yield* coordinator.authorize(key("b"), lookup)
      yield* coordinator.authorize(key("c"), lookup)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, completed: 2 })
      assert.isTrue(Option.isNone(yield* coordinator.current(key("a"))))
      assert.isTrue(Option.isSome(yield* coordinator.current(key("b"))))
      assert.isTrue(Option.isSome(yield* coordinator.current(key("c"))))
      yield* coordinator.authorize(key("a"), lookup)
      assert.strictEqual(lookups, 4)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, completed: 2 })
    }).pipe(Effect.scoped))

  it.effect("eagerly removes successes at the exact monotonic expiry", () =>
    Effect.gen(function*() {
      const coordinator = yield* make({ refreshInterval: "1 second" })
      const first = yield* coordinator.authorize(key("a"), () => Effect.succeed("a"))
      yield* coordinator.authorize(key("b"), () => Effect.succeed("b"))
      assert.strictEqual(first.expiresAtNanos, 1_000_000_000n)
      yield* TestClock.adjust("999 millis")
      assert.isTrue(Option.isSome(yield* coordinator.current(key("a"))))
      yield* TestClock.adjust("1 milli")
      assert.isTrue(Option.isNone(yield* coordinator.current(key("a"))))
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, completed: 0 })
    }).pipe(Effect.scoped))

  it.effect("interrupts and completes pending work when its owner scope closes", () =>
    Effect.gen(function*() {
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
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 2, completed: 0 })
      yield* Scope.close(owner, Exit.void)
      yield* Deferred.await(interrupted)
      const [callerExit, queuedExit] = yield* Effect.all([Fiber.await(caller), Fiber.await(queued)])
      assert.isTrue(Exit.isFailure(callerExit))
      if (Exit.isFailure(callerExit)) assert.isAbove(Cause.interruptors(callerExit.cause).size, 0)
      assert.isTrue(Exit.isFailure(queuedExit))
      if (Exit.isFailure(queuedExit)) assert.isAbove(Cause.interruptors(queuedExit.cause).size, 0)
      assert.deepStrictEqual(yield* coordinator.snapshot, { pending: 0, completed: 0 })
    }).pipe(Effect.scoped))
})
