import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import type * as Reactivity from "effect/reactivity/Reactivity"

export const notify = (reactivity: Reactivity.Reactivity, keys: Iterable<string>): Effect.Effect<void> =>
  Effect.forEach(
    new Set(keys),
    (key) =>
      reactivity.invalidate([key]).pipe(
        Effect.catchCause((cause) => {
          if (!Cause.hasDies(cause)) return Effect.failCause(cause)
          return Effect.logError("Reactivity subscriber died", cause).pipe(
            Effect.annotateLogs({ "reactivity.key": key })
          )
        })
      ),
    { discard: true }
  )
