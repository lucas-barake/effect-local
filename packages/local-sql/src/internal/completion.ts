import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import type * as Exit from "effect/Exit"
import * as Errors from "./errors.js"

export interface Completion<A, E extends { readonly _tag: string } = never,> {
  readonly waiters: Set<Deferred.Deferred<A, E>>
  exit: Exit.Exit<A, E> | undefined
}

export const make = <A, E extends { readonly _tag: string } = never,>(): Completion<A, E> => ({
  waiters: new Set(),
  exit: undefined
})

export const wait = <A, E extends { readonly _tag: string },>(self: Completion<A, E>): Effect.Effect<A, E> =>
  Effect.suspend(() => {
    if (self.exit !== undefined) return self.exit
    const own = Deferred.makeUnsafe<A, E>()
    self.waiters.add(own)
    return Effect.ensuring(Deferred.await(own), Effect.sync(() => self.waiters.delete(own)))
  })

export const settle = <A, E extends { readonly _tag: string },>(
  self: Completion<A, E>,
  exit: Exit.Exit<A, E>
): Effect.Effect<void> =>
  Effect.suspend(() => {
    if (self.exit !== undefined) return Effect.void
    self.exit = exit
    const waiters = Array.from(self.waiters)
    self.waiters.clear()
    return Effect.forEach(
      waiters,
      (waiter) =>
        Deferred.done(waiter, exit).pipe(
          Effect.asVoid,
          Effect.catchCause((cause) => {
            if (Errors.causeKind(cause) !== "Defect") return Effect.failCause(cause)
            return Effect.logError("Completion callback died", cause)
          })
        ),
      { discard: true }
    )
  })
