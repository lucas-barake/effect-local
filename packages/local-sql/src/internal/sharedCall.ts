import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import type * as Scope from "effect/Scope"

interface Detached<Request, A, E extends { readonly _tag: string },> {
  readonly request: Request
  readonly fiber: Fiber.Fiber<A, E>
}

export interface SharedCall<Request, A, E extends { readonly _tag: string },> {
  readonly run: (request: Request) => Effect.Effect<A, E>
  readonly cancel: Effect.Effect<void>
}

export const make = <Request, A, E extends { readonly _tag: string },>(
  call: (request: Request) => Effect.Effect<A, E>,
  scope: Scope.Scope
): SharedCall<Request, A, E> => {
  let detached: Detached<Request, A, E> | undefined

  const answers = (current: Detached<Request, A, E>, request: Request) => {
    const finished = current.fiber.pollUnsafe()
    return Equal.equals(current.request, request) && (finished === undefined || Exit.isSuccess(finished))
  }

  const predecessor = (request: Request) => {
    const current = detached
    if (current === undefined) return undefined
    if (answers(current, request)) return current
    detached = undefined
    current.fiber.interruptUnsafe()
    return undefined
  }

  const keep = (request: Request, fiber: Fiber.Fiber<A, E>) => {
    const current = detached
    if (current !== undefined && answers(current, request)) {
      fiber.interruptUnsafe()
      return
    }
    if (current !== undefined) current.fiber.interruptUnsafe()
    detached = { request, fiber }
  }

  const run = (request: Request): Effect.Effect<A, E> =>
    Effect.uninterruptibleMask(Effect.fnUntraced(function*(restore) {
      const prior = predecessor(request)
      if (prior !== undefined) {
        const finished = prior.fiber.pollUnsafe()
        if (finished !== undefined) {
          detached = undefined
          return yield* finished
        }
      }
      const own = yield* Effect.forkIn(call(request), scope, { startImmediately: true })
      let answered: Effect.Effect<Exit.Exit<A, E>> = Fiber.await(own)
      if (prior !== undefined) {
        const earlier = Fiber.await(prior.fiber).pipe(
          Effect.flatMap((exit) => {
            if (Exit.isSuccess(exit)) return Effect.succeed(exit)
            return Fiber.await(own)
          })
        )
        answered = Effect.raceFirst(earlier, answered)
      }
      const waited = yield* restore(answered).pipe(Effect.exit)
      if (Exit.isFailure(waited)) {
        keep(request, own)
        return yield* Effect.failCause(waited.cause)
      }
      if (prior !== undefined && detached === prior && Exit.isSuccess(waited.value)) {
        detached = undefined
        prior.fiber.interruptUnsafe()
      }
      own.interruptUnsafe()
      return yield* waited.value
    }))

  const cancel = Effect.suspend(() => {
    const current = detached
    detached = undefined
    if (current === undefined) return Effect.void
    return Effect.asVoid(Fiber.interrupt(current.fiber))
  })

  return { run, cancel }
}
