import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import type * as Scope from "effect/Scope"

interface Flight<Request, A, E extends { readonly _tag: string },> {
  readonly request: Request
  readonly fiber: Fiber.Fiber<A, E>
  readonly failure: { at: number | undefined }
  heard: boolean
}

export interface SharedCall<Request, A, E extends { readonly _tag: string },> {
  readonly run: (request: Request) => Effect.Effect<A, E>
  readonly cancel: Effect.Effect<void>
  readonly disownFailure: () => void
}

export const make = <Request, A, E extends { readonly _tag: string },>(
  call: (request: Request) => Effect.Effect<A, E>,
  scope: Scope.Scope,
  failureHeardWithinMillis: number
): SharedCall<Request, A, E> => {
  let detached: Flight<Request, A, E> | undefined

  const answers = (current: Flight<Request, A, E>, request: Request, now: number) => {
    if (!Equal.equals(current.request, request)) return false
    const finished = current.fiber.pollUnsafe()
    if (finished === undefined || Exit.isSuccess(finished)) return true
    const failedAt = current.failure.at
    return !current.heard && failedAt !== undefined && now - failedAt < failureHeardWithinMillis
  }

  const predecessor = (request: Request, now: number) => {
    const current = detached
    if (current === undefined) return undefined
    if (answers(current, request, now)) return current
    detached = undefined
    current.fiber.interruptUnsafe()
    return undefined
  }

  const keep = (flight: Flight<Request, A, E>, now: number) => {
    const current = detached
    if (current !== undefined && answers(current, flight.request, now)) {
      flight.fiber.interruptUnsafe()
      return
    }
    if (current !== undefined) current.fiber.interruptUnsafe()
    detached = flight
  }

  const start = Effect.fnUntraced(function*(request: Request) {
    const failure: { at: number | undefined } = { at: undefined }
    const timed = Effect.tapError(call(request), () =>
      Effect.map(Clock.currentTimeMillis, (now) => {
        failure.at = now
      }))
    const fiber = yield* Effect.forkIn(timed, scope, { startImmediately: true })
    const flight: Flight<Request, A, E> = { request, fiber, failure, heard: false }
    return flight
  })

  const run = (request: Request): Effect.Effect<A, E> =>
    Effect.uninterruptibleMask(Effect.fnUntraced(function*(restore) {
      const now = yield* Clock.currentTimeMillis
      const prior = predecessor(request, now)
      if (prior !== undefined) {
        const finished = prior.fiber.pollUnsafe()
        if (finished !== undefined) {
          detached = undefined
          prior.heard = true
          return yield* finished
        }
      }
      const own = yield* start(request)
      let answered: Effect.Effect<Exit.Exit<A, E>> = Fiber.await(own.fiber)
      if (prior !== undefined) {
        const earlier = Fiber.await(prior.fiber).pipe(
          Effect.flatMap((exit) => {
            if (Exit.isSuccess(exit)) return Effect.succeed(exit)
            return Fiber.await(own.fiber)
          })
        )
        answered = Effect.raceFirst(earlier, answered)
      }
      const waited = yield* restore(answered).pipe(Effect.exit)
      if (Exit.isFailure(waited)) {
        keep(own, now)
        return yield* Effect.failCause(waited.cause)
      }
      own.heard = true
      if (prior !== undefined && detached === prior && Exit.isSuccess(waited.value)) {
        detached = undefined
        prior.fiber.interruptUnsafe()
      }
      own.fiber.interruptUnsafe()
      return yield* waited.value
    }))

  const cancel = Effect.suspend(() => {
    const current = detached
    if (current === undefined) return Effect.void
    const finished = current.fiber.pollUnsafe()
    if (finished !== undefined && Exit.isFailure(finished) && !current.heard) return Effect.void
    detached = undefined
    return Effect.asVoid(Fiber.interrupt(current.fiber))
  })

  const disownFailure = () => {
    if (detached !== undefined) detached.heard = true
  }

  return { run, cancel, disownFailure }
}
