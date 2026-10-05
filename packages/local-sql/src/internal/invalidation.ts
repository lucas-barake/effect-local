import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import type * as Reactivity from "effect/reactivity/Reactivity"

const deliver = (keys: Iterable<string>, invalidate: (key: string) => Effect.Effect<void>): Effect.Effect<void> =>
  Effect.forEach(
    new Set(keys),
    (key) =>
      invalidate(key).pipe(
        Effect.catchCause((cause) => {
          if (!Cause.hasDies(cause)) return Effect.failCause(cause)
          return Effect.logError("Reactivity subscriber died", cause).pipe(
            Effect.annotateLogs({ "reactivity.key": key })
          )
        })
      ),
    { discard: true }
  ).pipe(Effect.uninterruptible)

export const notify = (reactivity: Reactivity.Reactivity, keys: Iterable<string>): Effect.Effect<void> =>
  deliver(keys, (key) => reactivity.invalidate([key]))

export const flush = (reactivity: Reactivity.Reactivity, keys: Iterable<string>): Effect.Effect<void> =>
  deliver(keys, (key) => reactivity.withBatch(reactivity.invalidate([key])))
