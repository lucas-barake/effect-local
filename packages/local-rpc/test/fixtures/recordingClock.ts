import type * as Clock from "effect/Clock"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as TestClock from "effect/testing/TestClock"
import * as LosslessQueue from "../../src/internal/losslessQueue.js"

export interface RequestedSleep {
  readonly millis: number
  readonly deadline: number
}

export const make = Effect.gen(function*() {
  const testClock = yield* TestClock.testClockWith(Effect.succeed)
  const requests = yield* Queue.unbounded<RequestedSleep>()
  const clock: Clock.Clock = {
    currentTimeMillisUnsafe: () => testClock.currentTimeMillisUnsafe(),
    currentTimeMillis: testClock.currentTimeMillis,
    currentTimeNanosUnsafe: () => testClock.currentTimeNanosUnsafe(),
    currentTimeNanos: testClock.currentTimeNanos,
    monotonicTimeNanosUnsafe: () => testClock.monotonicTimeNanosUnsafe(),
    monotonicTimeNanos: testClock.monotonicTimeNanos,
    sleep: (duration) =>
      Effect.suspend(() => {
        const millis = Duration.toMillis(duration)
        const deadline = testClock.currentTimeMillisUnsafe() + millis
        Queue.offerUnsafe(requests, { millis, deadline })
        return Effect.suspend(() => {
          const remaining = Duration.millis(Math.max(0, deadline - testClock.currentTimeMillisUnsafe()))
          return testClock.sleep(remaining)
        })
      })
  }
  const nextSleep = (predicate: (request: RequestedSleep) => boolean): Effect.Effect<RequestedSleep> =>
    LosslessQueue.take(requests).pipe(
      Effect.flatMap((request) => {
        if (predicate(request)) return Effect.succeed(request)
        return nextSleep(predicate)
      })
    )
  const advanceTo = (timestamp: number) =>
    Effect.suspend(() => testClock.adjust(Duration.millis(timestamp - testClock.currentTimeMillisUnsafe())))
  return { clock, nextSleep, advanceTo } as const
})
