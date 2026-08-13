import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
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
  readonly requesting: number
  readonly completed: number
}

interface TaggedError {
  readonly _tag: string
}

export interface Coordinator<K, A, E extends TaggedError,> {
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

export interface Options<E extends TaggedError,> {
  readonly refreshIntervalNanos: bigint
  readonly lookupTimeoutNanos: bigint
  readonly onLookupTimeout: () => E
  readonly maximumConcurrentLookups: number
  readonly maximumPendingLookups: number
  readonly onPendingCapacityExceeded: () => E
  readonly completedCacheCapacity: number
}

interface Pending<A, E extends TaggedError,> {
  readonly deferred: Deferred.Deferred<Success<A>, E>
}

type Registration<A, E extends TaggedError,> =
  | { readonly _tag: "Cached"; readonly success: Success<A> }
  | { readonly _tag: "Follower"; readonly pending: Pending<A, E> }
  | { readonly _tag: "Leader"; readonly pending: Pending<A, E> }
  | { readonly _tag: "AtCapacity" }

const pruneCompleted = <K, A,>(
  completed: MutableHashMap.MutableHashMap<K, Success<A>>,
  nowNanos: bigint,
  capacity: number
): void => {
  for (const [key, success] of completed) {
    if (success.expiresAtNanos > nowNanos) break
    MutableHashMap.remove(completed, key)
  }
  while (MutableHashMap.size(completed) > capacity) {
    const oldest = completed[Symbol.iterator]().next()
    if (oldest.done) return
    MutableHashMap.remove(completed, oldest.value[0])
  }
}

export const make = Effect.fnUntraced(function*<K, A, E extends TaggedError,>(
  options: Options<E>
): Effect.fn.Return<Coordinator<K, A, E>, never, Scope.Scope> {
  const ownerScope = yield* Effect.scope
  const clock = yield* Clock.Clock
  const semaphore = yield* Semaphore.make(options.maximumConcurrentLookups)
  const pending = MutableHashMap.empty<K, Pending<A, E>>()
  const completed = MutableHashMap.empty<K, Success<A>>()
  let nextGeneration = 0n
  let requesting = 0

  yield* Scope.addFinalizer(ownerScope, Effect.sync(() => MutableHashMap.clear(completed)))

  const register = (
    key: K,
    afterGeneration: bigint | undefined
  ): Effect.Effect<Registration<A, E>> =>
    Effect.sync(() => {
      pruneCompleted(completed, clock.monotonicTimeNanosUnsafe(), options.completedCacheCapacity)
      const cached = MutableHashMap.get(completed, key)
      if (
        Option.isSome(cached) &&
        (afterGeneration === undefined || cached.value.generation > afterGeneration)
      ) {
        return { _tag: "Cached", success: cached.value }
      }
      const active = MutableHashMap.get(pending, key)
      if (Option.isSome(active)) return { _tag: "Follower", pending: active.value }
      if (MutableHashMap.size(pending) >= options.maximumPendingLookups) return { _tag: "AtCapacity" }
      const created: Pending<A, E> = { deferred: Deferred.makeUnsafe<Success<A>, E>() }
      MutableHashMap.set(pending, key, created)
      return { _tag: "Leader", pending: created }
    })

  const complete = (
    key: K,
    registered: Pending<A, E>,
    afterGeneration: bigint | undefined,
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
        pruneCompleted(completed, nowNanos, options.completedCacheCapacity)
        const success: Success<A> = {
          value: exit.value,
          generation: nextGeneration++,
          expiresAtNanos: nowNanos + options.refreshIntervalNanos
        }
        MutableHashMap.remove(completed, key)
        MutableHashMap.set(completed, key, success)
        pruneCompleted(completed, nowNanos, options.completedCacheCapacity)
        sharedExit = Exit.succeed(success)
      } else {
        if (afterGeneration !== undefined) {
          const cached = MutableHashMap.get(completed, key)
          if (Option.isSome(cached) && cached.value.generation === afterGeneration) {
            MutableHashMap.remove(completed, key)
          }
        }
        sharedExit = Exit.failCause(exit.cause)
      }
      Deferred.doneUnsafe(registered.deferred, sharedExit)
    })

  const run = (
    key: K,
    afterGeneration: bigint | undefined,
    lookup: (key: K) => Effect.Effect<A, E>,
    registered: Pending<A, E>
  ): Effect.Effect<void> =>
    Effect.uninterruptibleMask((restore) =>
      restore(Effect.suspend(() => {
        const expiresAtNanos = clock.monotonicTimeNanosUnsafe() + options.lookupTimeoutNanos
        return Effect.suspend(() => {
          if (clock.monotonicTimeNanosUnsafe() >= expiresAtNanos) {
            return Effect.fail(options.onLookupTimeout())
          }
          return lookup(key)
        }).pipe(
          Semaphore.withPermit(semaphore),
          Effect.timeoutOption(options.lookupTimeoutNanos),
          Effect.flatMap(Option.match({
            onNone: () => Effect.fail(options.onLookupTimeout()),
            onSome: Effect.succeed
          }))
        )
      })).pipe(Effect.exit, Effect.flatMap((exit) => complete(key, registered, afterGeneration, exit)))
    )

  const request = (
    key: K,
    afterGeneration: bigint | undefined,
    lookup: (key: K) => Effect.Effect<A, E>
  ): Effect.Effect<Success<A>, E> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.sync(() => {
        if (requesting >= options.maximumPendingLookups) return false
        requesting++
        return true
      }).pipe(
        Effect.flatMap((admitted) => {
          if (!admitted) return Effect.fail(options.onPendingCapacityExceeded())
          return register(key, afterGeneration).pipe(
            Effect.flatMap((registration) => {
              if (registration._tag === "Cached") return Effect.succeed(registration.success)
              if (registration._tag === "AtCapacity") {
                return Effect.fail(options.onPendingCapacityExceeded())
              }
              if (registration._tag === "Leader") {
                const awaiting = Deferred.await(registration.pending.deferred)
                return run(key, afterGeneration, lookup, registration.pending).pipe(
                  Effect.forkIn(ownerScope, { startImmediately: true }),
                  Effect.andThen(restore(awaiting))
                )
              }
              return restore(Deferred.await(registration.pending.deferred))
            }),
            Effect.ensuring(Effect.sync(() => {
              requesting--
            }))
          )
        })
      )
    )

  return {
    authorize: (key, lookup) => request(key, undefined, lookup),
    refresh: (key, generation, lookup) => request(key, generation, lookup),
    current: (key) =>
      Effect.sync(() => {
        pruneCompleted(completed, clock.monotonicTimeNanosUnsafe(), options.completedCacheCapacity)
        return MutableHashMap.get(completed, key)
      }),
    snapshot: Effect.sync(() => {
      pruneCompleted(completed, clock.monotonicTimeNanosUnsafe(), options.completedCacheCapacity)
      return {
        pending: MutableHashMap.size(pending),
        requesting,
        completed: MutableHashMap.size(completed)
      }
    })
  }
})
