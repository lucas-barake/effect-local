import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as MutableHashMap from "effect/MutableHashMap"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"

export interface Success<out A,> {
  readonly value: A
  readonly generation: bigint
  readonly expiresAtNanos: bigint
}

export interface Snapshot {
  readonly pending: number
  readonly completed: number
}

export interface Coordinator<K, A, E,> {
  readonly authorize: (
    key: K,
    lookup: (key: K) => Effect.Effect<A, E>
  ) => Effect.Effect<Success<A>, E>
  readonly refresh: (
    key: K,
    generation: bigint,
    lookup: (key: K) => Effect.Effect<A, E>
  ) => Effect.Effect<Success<A>, E>
  readonly current: (key: K) => Effect.Effect<Option.Option<Success<A>>>
  readonly snapshot: Effect.Effect<Snapshot>
}

export interface Options {
  readonly refreshInterval: Duration.Input
  readonly maximumConcurrentLookups: number
  readonly completedCacheCapacity: number
}

interface Pending<A, E,> {
  readonly deferred: Deferred.Deferred<Success<A>, E>
}

type Registration<A, E,> =
  | { readonly _tag: "Cached"; readonly success: Success<A> }
  | { readonly _tag: "Follower"; readonly pending: Pending<A, E> }
  | { readonly _tag: "Leader"; readonly pending: Pending<A, E> }

const removeExpired = <K, A,>(
  completed: MutableHashMap.MutableHashMap<K, Success<A>>,
  nowNanos: bigint
): void => {
  for (const [key, success] of completed) {
    if (success.expiresAtNanos <= nowNanos) {
      const current = MutableHashMap.get(completed, key)
      if (Option.isSome(current) && current.value.generation === success.generation) {
        MutableHashMap.remove(completed, key)
      }
    }
  }
}

const evictCompleted = <K, A,>(
  completed: MutableHashMap.MutableHashMap<K, Success<A>>,
  capacity: number
): void => {
  while (MutableHashMap.size(completed) > capacity) {
    let oldest: readonly [K, Success<A>] | undefined
    for (const entry of completed) {
      if (oldest === undefined || entry[1].generation < oldest[1].generation) oldest = entry
    }
    if (oldest === undefined) return
    const current = MutableHashMap.get(completed, oldest[0])
    if (Option.isSome(current) && current.value.generation === oldest[1].generation) {
      MutableHashMap.remove(completed, oldest[0])
    }
  }
}

export const make = <K, A, E,>(options: Options): Effect.Effect<Coordinator<K, A, E>, never, Scope.Scope> =>
  Effect.gen(function*() {
    const ownerScope = yield* Effect.scope
    const clock = yield* Clock.Clock
    const semaphore = yield* Semaphore.make(options.maximumConcurrentLookups)
    const refreshIntervalNanos = Duration.toNanosUnsafe(options.refreshInterval)
    const pending = MutableHashMap.empty<K, Pending<A, E>>()
    const completed = MutableHashMap.empty<K, Success<A>>()
    let nextGeneration = 0n

    yield* Scope.addFinalizer(
      ownerScope,
      Effect.sync(() => MutableHashMap.clear(completed))
    )

    const register = (
      key: K,
      afterGeneration: bigint | undefined
    ): Effect.Effect<Registration<A, E>> =>
      Effect.sync(() => {
        const nowNanos = clock.monotonicTimeNanosUnsafe()
        removeExpired(completed, nowNanos)
        const cached = MutableHashMap.get(completed, key)
        if (
          Option.isSome(cached) &&
          (afterGeneration === undefined || cached.value.generation > afterGeneration)
        ) {
          return { _tag: "Cached", success: cached.value }
        }
        const active = MutableHashMap.get(pending, key)
        if (Option.isSome(active)) return { _tag: "Follower", pending: active.value }
        const created: Pending<A, E> = { deferred: Deferred.makeUnsafe<Success<A>, E>() }
        MutableHashMap.set(pending, key, created)
        return { _tag: "Leader", pending: created }
      })

    const complete = (
      key: K,
      registered: Pending<A, E>,
      exit: Exit.Exit<A, E>
    ): Effect.Effect<void> =>
      Effect.sync(() => {
        const active = MutableHashMap.get(pending, key)
        if (Option.isSome(active) && active.value.deferred === registered.deferred) {
          MutableHashMap.remove(pending, key)
        }
        let sharedExit: Exit.Exit<Success<A>, E>
        if (Exit.isSuccess(exit)) {
          const nowNanos = clock.monotonicTimeNanosUnsafe()
          removeExpired(completed, nowNanos)
          const success: Success<A> = {
            value: exit.value,
            generation: nextGeneration++,
            expiresAtNanos: nowNanos + refreshIntervalNanos
          }
          MutableHashMap.set(completed, key, success)
          evictCompleted(completed, options.completedCacheCapacity)
          sharedExit = Exit.succeed(success)
        } else {
          sharedExit = Exit.failCause(exit.cause)
        }
        Deferred.doneUnsafe(registered.deferred, sharedExit)
      })

    const run = (
      key: K,
      lookup: (key: K) => Effect.Effect<A, E>,
      registered: Pending<A, E>
    ): Effect.Effect<void> =>
      Effect.uninterruptibleMask((restore) =>
        restore(
          Semaphore.withPermit(
            semaphore,
            Effect.suspend(() => lookup(key))
          )
        ).pipe(
          Effect.exit,
          Effect.flatMap((exit) => complete(key, registered, exit))
        )
      )

    const request = (
      key: K,
      afterGeneration: bigint | undefined,
      lookup: (key: K) => Effect.Effect<A, E>
    ): Effect.Effect<Success<A>, E> =>
      Effect.uninterruptibleMask((restore) =>
        register(key, afterGeneration).pipe(
          Effect.flatMap((registration) => {
            if (registration._tag === "Cached") return Effect.succeed(registration.success)
            if (registration._tag === "Leader") {
              return run(key, lookup, registration.pending).pipe(
                Effect.forkIn(ownerScope, { startImmediately: true }),
                Effect.andThen(restore(Deferred.await(registration.pending.deferred)))
              )
            }
            return restore(Deferred.await(registration.pending.deferred))
          })
        )
      )

    return {
      authorize: (key, lookup) => request(key, undefined, lookup),
      refresh: (key, generation, lookup) => request(key, generation, lookup),
      current: (key) =>
        Effect.sync(() => {
          removeExpired(completed, clock.monotonicTimeNanosUnsafe())
          return MutableHashMap.get(completed, key)
        }),
      snapshot: Effect.sync(() => {
        removeExpired(completed, clock.monotonicTimeNanosUnsafe())
        return {
          pending: MutableHashMap.size(pending),
          completed: MutableHashMap.size(completed)
        }
      })
    }
  })
