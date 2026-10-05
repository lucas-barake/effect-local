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

const deliver = (
  keys: Iterable<string>,
  invalidate: (key: string) => Effect.Effect<void>,
  handOff: (keys: ReadonlyArray<string>) => Effect.Effect<void>
): Effect.Effect<void> =>
  Effect.suspend(() => {
    const owed = new Set(keys)
    const settled = (key: string) =>
      Effect.sync(() => {
        owed.delete(key)
      })
    return Effect.forEach(
      Array.from(owed),
      (key) =>
        invalidate(key).pipe(
          Effect.andThen(settled(key)),
          Effect.catchCause((cause) => {
            const kind = Errors.causeKind(cause)
            const logged = Effect.logError(messages[kind], cause).pipe(
              Effect.annotateLogs({ "reactivity.key": key }),
              Effect.andThen(settled(key))
            )
            if (kind !== "Interruption") return logged
            return Effect.flatMap(fiberInterrupted, (interrupted) => {
              if (interrupted) return Effect.void
              return logged
            })
          })
        ),
      { discard: true }
    ).pipe(
      Effect.onExit(() => {
        if (owed.size === 0) return Effect.void
        return handOff(Array.from(owed))
      })
    )
  })

export const notify = (
  reactivity: Reactivity.Reactivity,
  keys: Iterable<string>,
  handOff: (keys: ReadonlyArray<string>) => Effect.Effect<void>
): Effect.Effect<void> => deliver(keys, (key) => reactivity.invalidate([key]), handOff)

export const flush = (
  reactivity: Reactivity.Reactivity,
  keys: Iterable<string>,
  handOff: (keys: ReadonlyArray<string>) => Effect.Effect<void>
): Effect.Effect<void> => deliver(keys, (key) => reactivity.withBatch(reactivity.invalidate([key])), handOff)
