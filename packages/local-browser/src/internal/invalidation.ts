import * as Effect from "effect/Effect"
import type * as Reactivity from "effect/reactivity/Reactivity"
import { causeKind } from "./errors.js"

const messages = {
  Defect: "Reactivity subscriber died",
  Failure: "Reactivity notification failed",
  Interruption: "Reactivity notification was interrupted"
} as const

export const notify = (reactivity: Reactivity.Reactivity, keys: Iterable<string>): Effect.Effect<void> =>
  Effect.forEach(
    new Set(keys),
    (key) =>
      reactivity.invalidate([key]).pipe(
        Effect.catchCause((cause) =>
          Effect.logError(messages[causeKind(cause)], cause).pipe(
            Effect.annotateLogs({ "reactivity.key": key })
          )
        )
      ),
    { discard: true }
  )
