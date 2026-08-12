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

export interface Options<E,> {
  readonly refreshInterval: Duration.Input
  readonly lookupTimeout: Duration.Input
  readonly onLookupTimeout: () => E
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

export const make = <K, A, E,>(options: Options<E>): Effect.Effect<Coordinator<K, A, E>, never, Scope.Scope> =>
  Effect.gen(function*() {
    const ownerScope = yield* Effect.scope
    const clock = yield* Clock.Clock
    const semaphore = yield* Semaphore.make(options.maximumConcurrentLookups)
    const refreshIntervalNanos = Duration.toNanosUnsafe(options.refreshInterval)
    const lookupTimeoutNanos = Duration.toNanosUnsafe(options.lookupTimeout)
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
        pruneCompleted(completed, nowNanos, options.completedCacheCapacity)
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
          pruneCompleted(completed, nowNanos, options.completedCacheCapacity)
          const success: Success<A> = {
            value: exit.value,
            generation: nextGeneration++,
            expiresAtNanos: nowNanos + refreshIntervalNanos
          }
          MutableHashMap.remove(completed, key)
          MutableHashMap.set(completed, key, success)
          pruneCompleted(completed, nowNanos, options.completedCacheCapacity)
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
          Effect.suspend(() => {
            const expiresAtNanos = clock.monotonicTimeNanosUnsafe() + lookupTimeoutNanos
            return Semaphore.withPermit(
              semaphore,
              Effect.suspend(() => {
                if (clock.monotonicTimeNanosUnsafe() >= expiresAtNanos) {
                  return Effect.fail(options.onLookupTimeout())
                }
                return lookup(key)
              })
            ).pipe(
              Effect.timeoutOption(options.lookupTimeout),
              Effect.flatMap(Option.match({
                onNone: () => Effect.fail(options.onLookupTimeout()),
                onSome: Effect.succeed
              }))
            )
          })
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
          pruneCompleted(completed, clock.monotonicTimeNanosUnsafe(), options.completedCacheCapacity)
          return MutableHashMap.get(completed, key)
        }),
      snapshot: Effect.sync(() => {
        pruneCompleted(completed, clock.monotonicTimeNanosUnsafe(), options.completedCacheCapacity)
        return {
          pending: MutableHashMap.size(pending),
          completed: MutableHashMap.size(completed)
        }
      })
    }
  })
