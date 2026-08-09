import { assert, describe, it } from "@effect/vitest"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Result from "effect/Result"
import * as Scheduler from "effect/Scheduler"
import * as Scope from "effect/Scope"
import * as ReplicaOperationScheduler from "../src/ReplicaOperationScheduler.js"
import { gateLimits } from "./fixtures/limits.js"

describe("ReplicaOperationScheduler", () => {
  const live = (maxQueuedRpc: number) =>
    ReplicaOperationScheduler.layer.pipe(
      Layer.provide(ReplicaLimits.layer({ ...gateLimits, maxQueuedRpc }))
    )

  const lane = (
    scheduler: ReplicaOperationScheduler.ReplicaOperationScheduler["Service"],
    value: "interactive" | "background"
  ) => value === "interactive" ? scheduler.interactive : scheduler.background

  const start = (
    scheduler: ReplicaOperationScheduler.ReplicaOperationScheduler["Service"],
    value: "interactive" | "background",
    label: string,
    attempted: Queue.Queue<string>,
    acquired: Queue.Queue<string>,
    release: Deferred.Deferred<void>
  ) =>
    Effect.forkChild(
      Queue.offer(attempted, label).pipe(
        Effect.andThen(Effect.scoped(Effect.gen(function*() {
          yield* lane(scheduler, value)
          yield* Queue.offer(acquired, label)
          yield* Deferred.await(release)
        })))
      ),
      { startImmediately: true }
    )

  it.effect("prioritizes interactive admission and preserves FIFO within each lane", () =>
    Effect.gen(function*() {
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      const attempted = yield* Queue.unbounded<string>()
      const acquired = yield* Queue.unbounded<string>()
      const releases = yield* Effect.all(Array.from({ length: 4 }, () => Deferred.make<void>()))

      const holder = yield* start(scheduler, "background", "holder", attempted, acquired, releases[0]!)
      assert.strictEqual(yield* Queue.take(acquired), "holder")
      const backgroundOne = yield* start(
        scheduler,
        "background",
        "background-1",
        attempted,
        acquired,
        releases[1]!
      )
      const backgroundTwo = yield* start(
        scheduler,
        "background",
        "background-2",
        attempted,
        acquired,
        releases[2]!
      )
      const interactive = yield* start(
        scheduler,
        "interactive",
        "interactive",
        attempted,
        acquired,
        releases[3]!
      )
      assert.deepStrictEqual(yield* Queue.takeN(attempted, 4), [
        "holder",
        "background-1",
        "background-2",
        "interactive"
      ])

      yield* Deferred.succeed(releases[0]!, undefined)
      assert.strictEqual(yield* Queue.take(acquired), "interactive")
      yield* Deferred.succeed(releases[3]!, undefined)
      assert.strictEqual(yield* Queue.take(acquired), "background-1")
      yield* Deferred.succeed(releases[1]!, undefined)
      assert.strictEqual(yield* Queue.take(acquired), "background-2")
      yield* Deferred.succeed(releases[2]!, undefined)
      yield* Effect.forEach([holder, backgroundOne, backgroundTwo, interactive], Fiber.join, { discard: true })
    }).pipe(Effect.provide(live(4))))

  it.effect("bounds lanes independently and recovers capacity after cancellation", () =>
    Effect.gen(function*() {
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      const attempted = yield* Queue.unbounded<string>()
      const acquired = yield* Queue.unbounded<string>()
      const holderRelease = yield* Deferred.make<void>()
      const waiterRelease = yield* Deferred.make<void>()
      const holder = yield* start(scheduler, "background", "holder", attempted, acquired, holderRelease)
      assert.strictEqual(yield* Queue.take(acquired), "holder")
      const background = yield* start(
        scheduler,
        "background",
        "background",
        attempted,
        acquired,
        waiterRelease
      )
      assert.strictEqual(yield* Queue.take(attempted), "holder")
      assert.strictEqual(yield* Queue.take(attempted), "background")

      const rejected = yield* Effect.result(Effect.scoped(scheduler.background))
      assert.isTrue(Result.isFailure(rejected))
      if (Result.isFailure(rejected)) {
        assert.strictEqual(rejected.failure.reason._tag, "QuotaExceeded")
        if (rejected.failure.reason._tag === "QuotaExceeded") {
          assert.strictEqual(rejected.failure.reason.resource, "queued operation permits")
          assert.strictEqual(rejected.failure.reason.limit, 1)
        }
      }

      const interactive = yield* start(
        scheduler,
        "interactive",
        "interactive",
        attempted,
        acquired,
        waiterRelease
      )
      assert.strictEqual(yield* Queue.take(attempted), "interactive")
      yield* Fiber.interrupt(background)
      const replacement = yield* start(
        scheduler,
        "background",
        "replacement",
        attempted,
        acquired,
        waiterRelease
      )
      assert.strictEqual(yield* Queue.take(attempted), "replacement")

      yield* Deferred.succeed(holderRelease, undefined)
      assert.strictEqual(yield* Queue.take(acquired), "interactive")
      yield* Deferred.succeed(waiterRelease, undefined)
      assert.strictEqual(yield* Queue.take(acquired), "replacement")
      yield* Effect.forEach([holder, interactive, replacement], Fiber.join, { discard: true })
    }).pipe(Effect.provide(live(1))))

  it.effect("closes an interactive batch when background starts waiting", () =>
    Effect.gen(function*() {
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      const attempted = yield* Queue.unbounded<string>()
      const acquired = yield* Queue.unbounded<string>()
      const releases = yield* Effect.all(Array.from({ length: 3 }, () => Deferred.make<void>()))
      const first = yield* start(scheduler, "interactive", "interactive-1", attempted, acquired, releases[0]!)
      assert.strictEqual(yield* Queue.take(acquired), "interactive-1")
      const background = yield* start(scheduler, "background", "background", attempted, acquired, releases[1]!)
      const second = yield* start(scheduler, "interactive", "interactive-2", attempted, acquired, releases[2]!)

      yield* Deferred.succeed(releases[0]!, undefined)
      assert.strictEqual(yield* Queue.take(acquired), "background")
      yield* Deferred.succeed(releases[1]!, undefined)
      assert.strictEqual(yield* Queue.take(acquired), "interactive-2")
      yield* Deferred.succeed(releases[2]!, undefined)
      yield* Effect.forEach([first, background, second], Fiber.join, { discard: true })
    }).pipe(Effect.provide(live(3))))

  it.effect("reopens the interactive batch when the final background waiter is cancelled", () =>
    Effect.gen(function*() {
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      const attempted = yield* Queue.unbounded<string>()
      const acquired = yield* Queue.unbounded<string>()
      const releases = yield* Effect.all(Array.from({ length: 4 }, () => Deferred.make<void>()))
      const first = yield* start(scheduler, "interactive", "interactive-1", attempted, acquired, releases[0]!)
      assert.strictEqual(yield* Queue.take(acquired), "interactive-1")
      const second = yield* start(scheduler, "interactive", "interactive-2", attempted, acquired, releases[1]!)
      assert.strictEqual(yield* Queue.take(acquired), "interactive-2")

      const background = yield* start(scheduler, "background", "background", attempted, acquired, releases[2]!)
      const third = yield* start(scheduler, "interactive", "interactive-3", attempted, acquired, releases[3]!)
      assert.deepStrictEqual(yield* Queue.takeN(attempted, 4), [
        "interactive-1",
        "interactive-2",
        "background",
        "interactive-3"
      ])
      yield* Fiber.interrupt(background)
      yield* Deferred.succeed(releases[0]!, undefined)
      assert.strictEqual(yield* Queue.take(acquired), "interactive-3")

      yield* Effect.forEach(
        [releases[1]!, releases[3]!],
        (release) => Deferred.succeed(release, undefined),
        { discard: true }
      )
      yield* Effect.forEach([first, second, third], Fiber.join, { discard: true })
    }).pipe(Effect.provide(live(1))))

  it.effect("bounds the active interactive batch and its waiting queue", () =>
    Effect.gen(function*() {
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      const attempted = yield* Queue.unbounded<string>()
      const acquired = yield* Queue.unbounded<string>()
      const releases = yield* Effect.all(Array.from({ length: 3 }, () => Deferred.make<void>()))
      const first = yield* start(scheduler, "interactive", "interactive-1", attempted, acquired, releases[0]!)
      assert.strictEqual(yield* Queue.take(acquired), "interactive-1")
      const second = yield* start(scheduler, "interactive", "interactive-2", attempted, acquired, releases[1]!)
      assert.strictEqual(yield* Queue.take(acquired), "interactive-2")
      const queued = yield* start(scheduler, "interactive", "interactive-3", attempted, acquired, releases[2]!)
      assert.deepStrictEqual(yield* Queue.takeN(attempted, 3), ["interactive-1", "interactive-2", "interactive-3"])
      assert.isTrue(Option.isNone(yield* Queue.poll(acquired)))

      const rejected = yield* Effect.result(Effect.scoped(scheduler.interactive))
      assert.isTrue(Result.isFailure(rejected))
      yield* Deferred.succeed(releases[0]!, undefined)
      assert.strictEqual(yield* Queue.take(acquired), "interactive-3")
      yield* Effect.forEach([releases[1]!, releases[2]!], (release) => Deferred.succeed(release, undefined), {
        discard: true
      })
      yield* Effect.forEach([first, second, queued], Fiber.join, { discard: true })
    }).pipe(Effect.provide(live(1))))

  const assertUsable = (scheduler: ReplicaOperationScheduler.ReplicaOperationScheduler["Service"], label: string) =>
    Effect.gen(function*() {
      for (const admission of [scheduler.interactive, scheduler.background]) {
        const probe = yield* Effect.forkDetach(Effect.scoped(admission))
        const exit = yield* Fiber.await(probe).pipe(Effect.timeoutOption("2 seconds"))
        assert.isTrue(Option.isSome(exit), `${label}: scheduler remained occupied`)
      }
    })

  const grantBoundaryRound = (maxOps: number, offset: number) =>
    Effect.gen(function*() {
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      const held = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const holder = yield* Effect.forkChild(Effect.scoped(Effect.gen(function*() {
        yield* scheduler.background
        yield* Deferred.succeed(held, undefined)
        yield* Deferred.await(release)
      })))
      yield* Deferred.await(held)
      const waiter = yield* Effect.forkChild(
        Effect.result(Effect.scoped(scheduler.interactive)).pipe(
          Effect.provideService(Scheduler.MaxOpsBeforeYield, maxOps)
        )
      )
      yield* Effect.yieldNow
      yield* Deferred.succeed(release, undefined)
      for (let turn = 0; turn < offset; turn++) yield* Effect.yieldNow
      yield* Effect.sync(() => waiter.interruptUnsafe())
      assert.isTrue(Option.isSome(yield* Fiber.await(waiter).pipe(Effect.timeoutOption("2 seconds"))))
      assert.isTrue(Option.isSome(yield* Fiber.await(holder).pipe(Effect.timeoutOption("2 seconds"))))
      yield* assertUsable(scheduler, `maxOps ${maxOps} offset ${offset}`)
    })

  it.live("an interrupt landing at the grant boundary never strands admission", () =>
    Effect.gen(function*() {
      for (let maxOps = 2; maxOps <= 10; maxOps++) {
        for (let offset = 0; offset < 12; offset++) yield* grantBoundaryRound(maxOps, offset)
      }
    }).pipe(Effect.provide(live(4))), 120_000)

  it.effect("releases admission after failure and interruption", () =>
    Effect.gen(function*() {
      const scheduler = yield* ReplicaOperationScheduler.ReplicaOperationScheduler
      const failed = yield* Effect.result(Effect.scoped(
        scheduler.background.pipe(Effect.andThen(Effect.fail("expected")))
      ))
      assert.isTrue(Result.isFailure(failed))
      yield* Effect.scoped(scheduler.interactive)

      const entered = yield* Deferred.make<void>()
      const interrupted = yield* Effect.forkChild(Effect.scoped(Effect.gen(function*() {
        yield* scheduler.interactive
        yield* Deferred.succeed(entered, undefined)
        return yield* Effect.never
      })))
      yield* Deferred.await(entered)
      yield* Fiber.interrupt(interrupted)
      yield* Effect.scoped(scheduler.background)
    }).pipe(Effect.provide(live(2))))

  it.effect("shutdown interrupts waiters in both lanes", () =>
    Effect.gen(function*() {
      const layerScope = yield* Scope.make()
      const context = yield* Scope.provide(Layer.build(live(2)), layerScope)
      const scheduler = Context.get(context, ReplicaOperationScheduler.ReplicaOperationScheduler)
      const held = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const holder = yield* Effect.forkDetach(Effect.scoped(Effect.gen(function*() {
        yield* scheduler.background
        yield* Deferred.succeed(held, undefined)
        yield* Deferred.await(release)
      })))
      yield* Deferred.await(held)
      const interactive = yield* Effect.forkDetach(Effect.scoped(scheduler.interactive), { startImmediately: true })
      const background = yield* Effect.forkDetach(Effect.scoped(scheduler.background), { startImmediately: true })

      yield* Scope.close(layerScope, Exit.void)
      assert.isTrue(Option.isSome(yield* Fiber.await(interactive).pipe(Effect.timeoutOption("2 seconds"))))
      assert.isTrue(Option.isSome(yield* Fiber.await(background).pipe(Effect.timeoutOption("2 seconds"))))
      yield* Deferred.succeed(release, undefined)
      assert.isTrue(Option.isSome(yield* Fiber.await(holder).pipe(Effect.timeoutOption("2 seconds"))))
    }))
})
