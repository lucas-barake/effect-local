import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"

export type InvalidationBatch =
  | { readonly _tag: "Keys"; readonly keys: ReadonlyArray<string> }
  | { readonly _tag: "Overflow" }

export interface InvalidationHub {
  readonly publish: (keys: ReadonlyArray<string>) => Effect.Effect<void>
  readonly subscribe: Effect.Effect<Stream.Stream<InvalidationBatch>, never, Scope.Scope>
  readonly shutdown: Effect.Effect<void>
}

interface Subscriber {
  readonly pending: Set<string>
  overflowed: boolean
  wake: Deferred.Deferred<void> | undefined
}

export const make = (capacity: number): InvalidationHub => {
  const subscribers = new Set<Subscriber>()
  let closed = false

  const wake = (subscriber: Subscriber) => {
    const waiting = subscriber.wake
    if (waiting === undefined) return
    subscriber.wake = undefined
    Deferred.doneUnsafe(waiting, Effect.void)
  }

  const take = (subscriber: Subscriber): Effect.Effect<InvalidationBatch, Cause.Done> =>
    Effect.suspend(() => {
      if (subscriber.overflowed) {
        subscriber.overflowed = false
        return Effect.succeed<InvalidationBatch>({ _tag: "Overflow" })
      }
      if (subscriber.pending.size > 0) {
        const keys = Array.from(subscriber.pending)
        subscriber.pending.clear()
        return Effect.succeed<InvalidationBatch>({ _tag: "Keys", keys })
      }
      if (closed) return Cause.done()
      const waiting = Deferred.makeUnsafe<void>()
      subscriber.wake = waiting
      return Deferred.await(waiting).pipe(Effect.andThen(take(subscriber)))
    })

  const register = Effect.sync(() => {
    const subscriber: Subscriber = { pending: new Set(), overflowed: false, wake: undefined }
    subscribers.add(subscriber)
    return subscriber
  })

  const unregister = (subscriber: Subscriber) =>
    Effect.sync(() => {
      subscribers.delete(subscriber)
      wake(subscriber)
    })

  return {
    publish: (keys) =>
      Effect.sync(() => {
        if (closed || keys.length === 0) return
        for (const subscriber of subscribers) {
          if (!subscriber.overflowed) {
            for (const key of keys) subscriber.pending.add(key)
            if (subscriber.pending.size > capacity) {
              subscriber.pending.clear()
              subscriber.overflowed = true
            }
          }
          wake(subscriber)
        }
      }),
    subscribe: Effect.acquireRelease(register, unregister).pipe(
      Effect.map((subscriber) => Stream.fromEffectRepeat(take(subscriber)))
    ),
    shutdown: Effect.sync(() => {
      closed = true
      for (const subscriber of subscribers) wake(subscriber)
    })
  }
}
