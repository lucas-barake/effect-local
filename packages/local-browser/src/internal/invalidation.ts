import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import type * as Reactivity from "effect/reactivity/Reactivity"
import { causeKind } from "./errors.js"

export const notify = (reactivity: Reactivity.Reactivity, keys: Iterable<string>): Effect.Effect<void> =>
  Effect.forEach(
    new Set(keys),
    (key) =>
      reactivity.invalidate([key]).pipe(
        Effect.catchCause((cause) => {
          if (causeKind(cause) !== "Defect") return Effect.failCause(cause)
          const logged = Effect.logError("Reactivity subscriber died", cause).pipe(
            Effect.annotateLogs({ "reactivity.key": key })
          )
          if (Cause.hasInterrupts(cause)) return Effect.andThen(logged, Effect.interrupt)
          return logged
        })
      ),
    { discard: true }
  )
