import * as Clock from "effect/Clock"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as TestClock from "effect/testing/TestClock"

const clockStep = 100

export const advanceUntil = <A, E extends { readonly _tag: string },>(
  awaited: Effect.Effect<A, E>,
  step: Duration.Input = clockStep
) => {
  const tick = TestClock.adjust(step).pipe(Effect.andThen(Effect.yieldNow))
  return Effect.raceFirst(awaited, Effect.forever(tick))
}

const guardedStep = Effect.fnUntraced(function*<E extends { readonly _tag: string },>(
  guard: (through: number) => Effect.Effect<unknown, E>,
  target: number
) {
  const now = yield* Clock.currentTimeMillis
  yield* guard(now + 2 * clockStep + 1)
  yield* TestClock.setTime(Math.min(target, now + clockStep))
  yield* Effect.yieldNow
})

export const advanceGuarded = Effect.fnUntraced(function*<E extends { readonly _tag: string },>(
  guard: (through: number) => Effect.Effect<unknown, E>,
  duration: Duration.Input
) {
  const target = (yield* Clock.currentTimeMillis) + Duration.toMillis(Duration.fromInputUnsafe(duration))
  while ((yield* Clock.currentTimeMillis) < target) yield* guardedStep(guard, target)
})

export const advanceGuardedUntil = <A, E extends { readonly _tag: string }, G extends { readonly _tag: string },>(
  guard: (through: number) => Effect.Effect<unknown, G>,
  awaited: Effect.Effect<A, E>
) => {
  const steps = Effect.forever(guardedStep(guard, Number.POSITIVE_INFINITY))
  return Effect.raceFirst(awaited, steps)
}
