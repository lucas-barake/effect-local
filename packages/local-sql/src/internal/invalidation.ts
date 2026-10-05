import * as Effect from "effect/Effect"
import type * as Reactivity from "effect/reactivity/Reactivity"
import * as Errors from "./errors.js"

const messages = {
  Defect: "Reactivity subscriber died",
  Failure: "Reactivity notification failed",
  Interruption: "Reactivity notification was interrupted"
} as const

const deliver = (keys: Iterable<string>, invalidate: (key: string) => Effect.Effect<void>): Effect.Effect<void> =>
  Effect.forEach(
    new Set(keys),
    (key) =>
      invalidate(key).pipe(
        Effect.catchCause((cause) =>
          Effect.logError(messages[Errors.causeKind(cause)], cause).pipe(
            Effect.annotateLogs({ "reactivity.key": key })
          )
        )
      ),
    { discard: true }
  )

export const notify = (reactivity: Reactivity.Reactivity, keys: Iterable<string>): Effect.Effect<void> =>
  deliver(keys, (key) => reactivity.invalidate([key]))

export const flush = (reactivity: Reactivity.Reactivity, keys: Iterable<string>): Effect.Effect<void> =>
  deliver(keys, (key) => reactivity.withBatch(reactivity.invalidate([key])))
