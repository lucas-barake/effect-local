import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import type * as Reactivity from "effect/reactivity/Reactivity"
import * as Errors from "./errors.js"

const messages = {
  Defect: "Reactivity subscriber died",
  Failure: "Reactivity notification failed",
  Interruption: "Reactivity notification was interrupted"
} as const

const fiberInterrupted = Effect.interruptible(Effect.void).pipe(Effect.exit, Effect.map(Exit.isFailure))

const deliver = (keys: Iterable<string>, invalidate: (key: string) => Effect.Effect<void>): Effect.Effect<void> =>
  Effect.forEach(
    new Set(keys),
    (key) =>
      invalidate(key).pipe(
        Effect.catchCause((cause) => {
          const kind = Errors.causeKind(cause)
          const logged = Effect.logError(messages[kind], cause).pipe(Effect.annotateLogs({ "reactivity.key": key }))
          if (kind !== "Interruption") return logged
          return Effect.flatMap(fiberInterrupted, (interrupted) => {
            if (interrupted) return Effect.void
            return logged
          })
        })
      ),
    { discard: true }
  )

export const notify = (reactivity: Reactivity.Reactivity, keys: Iterable<string>): Effect.Effect<void> =>
  deliver(keys, (key) => reactivity.invalidate([key]))

export const flush = (reactivity: Reactivity.Reactivity, keys: Iterable<string>): Effect.Effect<void> =>
  deliver(keys, (key) => reactivity.withBatch(reactivity.invalidate([key])))
