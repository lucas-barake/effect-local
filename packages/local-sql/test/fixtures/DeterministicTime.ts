import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Scheduler from "effect/Scheduler"
import * as TestClock from "effect/testing/TestClock"

interface Driver {
  readonly idle: Effect.Effect<void>
  readonly advanceToNextSleep: Effect.Effect<boolean>
}

class DeterministicTime extends Context.Service<DeterministicTime, Driver>()(
  "@lucas-barake/effect-local-sql/test/DeterministicTime"
) {}

const makeDriver = Effect.gen(function*() {
  const testClock = yield* TestClock.testClockWith(Effect.succeed)
  const deadlines = new Set<{ readonly deadline: number }>()
  const waiters: Array<() => void> = []
  const base = new Scheduler.MixedScheduler("async")
  const checks = base.makeDispatcher()
  let scheduledTasks = 0
  let pendingDigests = 0
  const releaseWhenIdle = () => {
    if (scheduledTasks > 0 || pendingDigests > 0) return
    for (const release of waiters.splice(0)) release()
  }
  const scheduler: Scheduler.Scheduler = {
    executionMode: base.executionMode,
    shouldYield: (fiber) => base.shouldYield(fiber),
    makeDispatcher: () => {
      const dispatcher = base.makeDispatcher()
      return {
        scheduleTask: (task, priority) => {
          scheduledTasks += 1
          dispatcher.scheduleTask(() => {
            task()
            scheduledTasks -= 1
            releaseWhenIdle()
          }, priority)
        },
        flush: () => dispatcher.flush()
      }
    }
  }
  const subtle = globalThis.crypto.subtle
  const digest = subtle.digest.bind(subtle)
  const trackedDigest: typeof digest = (algorithm, data) => {
    pendingDigests += 1
    return digest(algorithm, data).finally(() => {
      pendingDigests -= 1
      checks.scheduleTask(releaseWhenIdle, 0)
    })
  }
  yield* Effect.acquireRelease(
    Effect.sync(() => {
      subtle.digest = trackedDigest
    }),
    () =>
      Effect.sync(() => {
        subtle.digest = digest
      })
  )
  const clock: Clock.Clock = {
    currentTimeMillisUnsafe: () => testClock.currentTimeMillisUnsafe(),
    currentTimeMillis: testClock.currentTimeMillis,
    currentTimeNanosUnsafe: () => testClock.currentTimeNanosUnsafe(),
    currentTimeNanos: testClock.currentTimeNanos,
    monotonicTimeNanosUnsafe: () => testClock.monotonicTimeNanosUnsafe(),
    monotonicTimeNanos: testClock.monotonicTimeNanos,
    sleep: (duration) =>
      Effect.suspend(() => {
        const entry = { deadline: testClock.currentTimeMillisUnsafe() + Duration.toMillis(duration) }
        deadlines.add(entry)
        const forget = Effect.sync(() => deadlines.delete(entry))
        return Effect.ensuring(testClock.sleep(duration), forget)
      })
  }
  const idle = Effect.callback<void>((resume) => {
    waiters.push(() => resume(Effect.void))
    checks.scheduleTask(releaseWhenIdle, 0)
  })
  const advanceToNextSleep = Effect.suspend(() => {
    let next: number | undefined
    for (const entry of deadlines) next = Math.min(next ?? entry.deadline, entry.deadline)
    if (next === undefined) return Effect.succeed(false)
    const target = Math.max(next, testClock.currentTimeMillisUnsafe())
    return Effect.as(testClock.setTime(target), true)
  })
  return { scheduler, clock, driver: DeterministicTime.of({ idle, advanceToNextSleep }) }
})

export const provide = <A, E extends { readonly _tag: string }, R,>(
  effect: Effect.Effect<A, E, R>
): Effect.Effect<A, E, Exclude<R, DeterministicTime>> =>
  makeDriver.pipe(
    Effect.flatMap(({ clock, driver, scheduler }) =>
      effect.pipe(
        Effect.provideService(DeterministicTime, driver),
        Effect.provideService(Clock.Clock, clock),
        Effect.provideService(Scheduler.Scheduler, scheduler)
      )
    ),
    Effect.scoped
  )

export const scoped = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  provide(Effect.scoped(effect))

export const advanceUntil = Effect.fnUntraced(function*<A, E extends { readonly _tag: string },>(
  awaited: Effect.Effect<A, E>
) {
  const driver = yield* DeterministicTime
  const fiber = yield* Effect.forkChild(awaited, { startImmediately: true })
  while (true) {
    yield* driver.idle
    if (fiber.pollUnsafe() !== undefined) return yield* Fiber.join(fiber)
    const advanced = yield* driver.advanceToNextSleep
    if (!advanced) return yield* Effect.die("Nothing is running and no sleep is pending, so the wait cannot finish")
  }
})
