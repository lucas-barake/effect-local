import * as Clock from "effect/Clock"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import type * as Scope from "effect/Scope"

interface Flight<Request, A, E extends { readonly _tag: string },> {
  readonly request: Request
  readonly fiber: Fiber.Fiber<A, E>
  readonly answered: { at: number | undefined }
  readonly abandon: () => void
  heard: boolean
}

export interface Limiter {
  readonly limit: number
  running: number
  readonly detached: Set<() => void>
}

export const makeLimiter = (limit: number): Limiter => ({ limit, running: 0, detached: new Set() })

export interface Answer<A,> {
  readonly value: A
  readonly fresh: boolean
}

export interface SharedCall<Request, A, E extends { readonly _tag: string },> {
  readonly run: (request: Request) => Effect.Effect<Answer<A>, E>
  readonly cancel: Effect.Effect<void>
  readonly forget: () => void
  readonly disownFailure: () => void
}

export const make = <Request, A, E extends { readonly _tag: string },>(
  call: (request: Request) => Effect.Effect<A, E>,
  scope: Scope.Scope,
  keptForMillis: number,
  limiter: Limiter
): SharedCall<Request, A, E> => {
  let detached: Flight<Request, A, E> | undefined

  const detach = (next: Flight<Request, A, E> | undefined) => {
    if (detached !== undefined) limiter.detached.delete(detached.abandon)
    detached = next
    if (next !== undefined && next.fiber.pollUnsafe() === undefined) limiter.detached.add(next.abandon)
  }

  const answers = (current: Flight<Request, A, E>, request: Request, now: number) => {
    if (!Equal.equals(current.request, request)) return false
    const finished = current.fiber.pollUnsafe()
    if (finished === undefined) return true
    const answeredAt = current.answered.at
    if (answeredAt === undefined || now - answeredAt >= keptForMillis) return false
    return Exit.isSuccess(finished) || !current.heard
  }

  const predecessor = (request: Request, now: number) => {
    const current = detached
    if (current === undefined) return undefined
    if (answers(current, request, now)) return current
    detach(undefined)
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
    detach(flight)
  }

  const makeRoom = () => {
    while (limiter.running >= limiter.limit) {
      const [oldest] = limiter.detached
      if (oldest === undefined) return
      oldest()
    }
  }

  const start = Effect.fnUntraced(function*(request: Request) {
    const answered: { at: number | undefined } = { at: undefined }
    let counted = true
    const uncount = () => {
      if (!counted) return
      counted = false
      limiter.running -= 1
    }
    const abandon = () => {
      limiter.detached.delete(abandon)
      uncount()
      if (detached !== undefined && detached.abandon === abandon) detached = undefined
      fiber.interruptUnsafe()
    }
    const timed = Effect.exit(call(request)).pipe(
      Effect.tap(() =>
        Effect.map(Clock.currentTimeMillis, (now) => {
          answered.at = now
        })
      ),
      Effect.flatMap((exit) => exit),
      Effect.ensuring(Effect.sync(() => {
        limiter.detached.delete(abandon)
        uncount()
      }))
    )
    makeRoom()
    limiter.running += 1
    const fiber: Fiber.Fiber<A, E> = yield* Effect.forkIn(timed, scope, { startImmediately: true })
    const flight: Flight<Request, A, E> = { request, fiber, answered, abandon, heard: false }
    return flight
  })

  const run = (request: Request): Effect.Effect<Answer<A>, E> =>
    Effect.uninterruptibleMask(Effect.fnUntraced(function*(restore) {
      const now = yield* Clock.currentTimeMillis
      const prior = predecessor(request, now)
      if (prior !== undefined) {
        const finished = prior.fiber.pollUnsafe()
        if (finished !== undefined) {
          detach(undefined)
          prior.heard = true
          return { value: yield* finished, fresh: false }
        }
      }
      const own = yield* start(request)
      const requested = Effect.map(Fiber.await(own.fiber), (exit) => ({ exit, fresh: true }))
      let answered = requested
      if (prior !== undefined) {
        const earlier = Fiber.await(prior.fiber).pipe(
          Effect.flatMap((exit) => {
            if (Exit.isSuccess(exit)) return Effect.succeed({ exit, fresh: false })
            return requested
          })
        )
        answered = Effect.raceFirst(earlier, requested)
      }
      const waited = yield* restore(answered).pipe(Effect.exit)
      if (Exit.isFailure(waited)) {
        keep(own, now)
        return yield* Effect.failCause(waited.cause)
      }
      own.heard = true
      if (prior !== undefined && detached === prior && Exit.isSuccess(waited.value.exit)) {
        detach(undefined)
        prior.fiber.interruptUnsafe()
      }
      own.fiber.interruptUnsafe()
      return { value: yield* waited.value.exit, fresh: waited.value.fresh }
    }))

  const cancel = Effect.suspend(() => {
    const current = detached
    if (current === undefined) return Effect.void
    const finished = current.fiber.pollUnsafe()
    if (finished !== undefined && Exit.isFailure(finished) && !current.heard) return Effect.void
    detach(undefined)
    return Effect.asVoid(Fiber.interrupt(current.fiber))
  })

  const forget = () => {
    const current = detached
    detach(undefined)
    if (current !== undefined) current.fiber.interruptUnsafe()
  }

  const disownFailure = () => {
    if (detached !== undefined) detached.heard = true
  }

  return { run, cancel, forget, disownFailure }
}
