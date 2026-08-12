import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import type * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as MutableHashMap from "effect/MutableHashMap"
import * as Option from "effect/Option"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"

export interface Snapshot {
  readonly pending: number
  readonly running: number
}

export interface Options<E,> {
  readonly lookupTimeoutNanos: bigint
  readonly onLookupTimeout: () => E
  readonly maximumConcurrentLookups: number
  readonly maximumPendingLookups: number
  readonly onPendingCapacityExceeded: () => E
}

export interface Limiter<E,> {
  readonly ownerScope: Scope.Scope
  readonly tryReserveUnsafe: () => boolean
  readonly releaseUnsafe: () => void
  readonly execute: <A,>(lookup: () => Effect.Effect<A, E>) => Effect.Effect<A, E>
  readonly pendingCapacityExceeded: () => E
  readonly snapshot: Effect.Effect<Snapshot>
}

export interface Coordinator<K, A, E,> {
  readonly evaluate: (key: K, lookup: () => Effect.Effect<A, E>) => Effect.Effect<A, E>
}

interface Entry<A, E,> {
  readonly deferred: Deferred.Deferred<A, E>
  waiters: number
  state: "Pending" | "Canceling" | "Completed"
  owner: Fiber.Fiber<void> | undefined
}

type Registration<A, E,> =
  | { readonly _tag: "AtCapacity" }
  | { readonly _tag: "Leader"; readonly entry: Entry<A, E> }
  | { readonly _tag: "Follower"; readonly entry: Entry<A, E> }

export const makeLimiter = <E,>(options: Options<E>): Effect.Effect<Limiter<E>, never, Scope.Scope> =>
  Effect.gen(function*() {
    const ownerScope = yield* Effect.scope
    const clock = yield* Clock.Clock
    const semaphore = yield* Semaphore.make(options.maximumConcurrentLookups)
    let pending = 0
    let running = 0

    const execute = <A,>(lookup: () => Effect.Effect<A, E>): Effect.Effect<A, E> =>
      Effect.suspend(() => {
        const expiresAtNanos = clock.monotonicTimeNanosUnsafe() + options.lookupTimeoutNanos
        return Semaphore.withPermit(
          semaphore,
          Effect.suspend(() => {
            if (clock.monotonicTimeNanosUnsafe() >= expiresAtNanos) {
              return Effect.fail(options.onLookupTimeout())
            }
            return Effect.sync(() => {
              running++
            }).pipe(
              Effect.andThen(Effect.suspend(lookup)),
              Effect.ensuring(Effect.sync(() => {
                running--
              }))
            )
          })
        ).pipe(
          Effect.timeoutOption(options.lookupTimeoutNanos),
          Effect.flatMap(Option.match({
            onNone: () => Effect.fail(options.onLookupTimeout()),
            onSome: Effect.succeed
          }))
        )
      })

    return {
      ownerScope,
      tryReserveUnsafe: () => {
        if (pending >= options.maximumPendingLookups) return false
        pending++
        return true
      },
      releaseUnsafe: () => {
        pending--
      },
      execute,
      pendingCapacityExceeded: options.onPendingCapacityExceeded,
      snapshot: Effect.sync(() => ({ pending, running }))
    }
  })

export const make = <K, A, E,>(limiter: Limiter<E>): Coordinator<K, A, E> => {
  const entries = MutableHashMap.empty<K, Entry<A, E>>()

  const register = (key: K): Effect.Effect<Registration<A, E>> =>
    Effect.sync(() => {
      const existing = MutableHashMap.get(entries, key)
      if (Option.isSome(existing)) {
        existing.value.waiters++
        return { _tag: "Follower", entry: existing.value }
      }
      if (!limiter.tryReserveUnsafe()) return { _tag: "AtCapacity" }
      const entry: Entry<A, E> = {
        deferred: Deferred.makeUnsafe<A, E>(),
        waiters: 1,
        state: "Pending",
        owner: undefined
      }
      MutableHashMap.set(entries, key, entry)
      return { _tag: "Leader", entry }
    })

  const complete = (entry: Entry<A, E>, exit: Exit.Exit<A, E>): Effect.Effect<void> =>
    Effect.sync(() => {
      if (entry.state === "Completed") return
      entry.state = "Completed"
      limiter.releaseUnsafe()
      Deferred.doneUnsafe(entry.deferred, exit)
    })

  const run = (entry: Entry<A, E>, lookup: () => Effect.Effect<A, E>): Effect.Effect<void> =>
    Effect.uninterruptibleMask((restore) =>
      restore(limiter.execute(lookup)).pipe(
        Effect.exit,
        Effect.flatMap((exit) => complete(entry, exit))
      )
    )

  const detach = (key: K, entry: Entry<A, E>): Effect.Effect<void> =>
    Effect.suspend(() => {
      const owner = (() => {
        entry.waiters--
        if (entry.waiters !== 0 || entry.state !== "Pending") return undefined
        entry.state = "Canceling"
        const active = MutableHashMap.get(entries, key)
        if (Option.isSome(active) && active.value === entry) MutableHashMap.remove(entries, key)
        return entry.owner
      })()
      if (owner === undefined) return Effect.void
      return Fiber.interrupt(owner)
    })

  const evaluate = (key: K, lookup: () => Effect.Effect<A, E>): Effect.Effect<A, E> =>
    Effect.uninterruptibleMask((restore) =>
      register(key).pipe(
        Effect.flatMap((registration) => {
          if (registration._tag === "AtCapacity") return Effect.fail(limiter.pendingCapacityExceeded())
          const awaitResult = restore(Deferred.await(registration.entry.deferred)).pipe(
            Effect.ensuring(detach(key, registration.entry))
          )
          if (registration._tag === "Follower") return awaitResult
          return run(registration.entry, lookup).pipe(
            Effect.forkIn(limiter.ownerScope, { startImmediately: true }),
            Effect.tap((owner) =>
              Effect.sync(() => {
                registration.entry.owner = owner
              })
            ),
            Effect.andThen(awaitResult)
          )
        })
      )
    )

  return { evaluate }
}
