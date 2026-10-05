import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"

export const supervise = (completion: Effect.Effect<unknown>): Effect.Effect<void> =>
  completion.pipe(
    Effect.asVoid,
    Effect.catchCause((cause) => {
      if (!Cause.hasDies(cause)) return Effect.failCause(cause)
      return Effect.logError("Completion callback died", cause)
    })
  )
