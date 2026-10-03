import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as TestClock from "effect/testing/TestClock"

export const advanceClockUntil = Effect.fnUntraced(
  function*<A, E extends { readonly _tag: string },>(effect: Effect.Effect<A, E>) {
    const fiber = yield* Effect.forkChild(effect, { startImmediately: true })
    while (fiber.pollUnsafe() === undefined) yield* TestClock.adjust("1 millis")
    return yield* Fiber.join(fiber)
  }
)
