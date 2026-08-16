import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as EffectLayer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import * as platform from "../src/internal/platform.js"

interface MemoryConnection {
  readonly subscribers: Set<Queue.Queue<unknown>>
}

interface MemoryHolder {
  readonly lost: Deferred.Deferred<void>
}

interface MemoryWaiter {
  readonly holder: MemoryHolder
  readonly grant: Deferred.Deferred<void>
  interrupted: boolean
}

interface MemoryLock {
  holder: MemoryHolder | undefined
  readonly queue: Array<MemoryWaiter>
}

export interface MemoryPlatform {
  readonly tabChannel: platform.TabChannelService
  readonly webLocks: platform.WebLocksService
  readonly epochStore: platform.EpochStoreService
  readonly clientIdentityStore: platform.ClientIdentityStoreService
  readonly layerAll: EffectLayer.Layer<
    platform.TabChannel | platform.WebLocks | platform.EpochStore | platform.ClientIdentityStore
  >
}

export const makeMemoryPlatform = Effect.sync((): MemoryPlatform => {
  const channels = new Map<string, Set<MemoryConnection>>()
  const locks = new Map<string, MemoryLock>()
  const epochs = new Map<string, number>()
  const identities = new Map<string, string>()

  const tabChannel: platform.TabChannelService = {
    open: Effect.fnUntraced(function*(name) {
      const scope = yield* Effect.scope
      let peers = channels.get(name)
      if (peers === undefined) {
        peers = new Set()
        channels.set(name, peers)
      }
      const registered = peers
      const connection: MemoryConnection = { subscribers: new Set() }
      registered.add(connection)
      yield* Scope.addFinalizer(scope, Effect.sync(() => registered.delete(connection)))
      return {
        post: (frame) =>
          Effect.sync(() => {
            for (const peer of registered) {
              if (peer === connection) continue
              for (const queue of peer.subscribers) {
                Queue.offerUnsafe(queue, frame)
              }
            }
          }),
        messages: Effect.gen(function*() {
          const subscriberScope = yield* Effect.scope
          const queue = yield* Queue.make<unknown>()
          connection.subscribers.add(queue)
          yield* Scope.addFinalizer(
            subscriberScope,
            Effect.sync(() => connection.subscribers.delete(queue)).pipe(
              Effect.andThen(Queue.shutdown(queue))
            )
          )
          return queue
        })
      }
    })
  }

  const lockState = (name: string): MemoryLock => {
    let state = locks.get(name)
    if (state === undefined) {
      state = { holder: undefined, queue: [] }
      locks.set(name, state)
    }
    return state
  }

  const promoteNext = (state: MemoryLock): void => {
    state.holder = undefined
    while (state.queue.length > 0) {
      const waiter = state.queue.shift()
      if (waiter === undefined || waiter.interrupted) continue
      state.holder = waiter.holder
      Deferred.doneUnsafe(waiter.grant, Effect.void)
      return
    }
  }

  const webLocks: platform.WebLocksService = {
    acquire: Effect.fnUntraced(function*(name, options) {
      const scope = yield* Effect.scope
      const state = lockState(name)
      const lost = yield* Deferred.make<void>()
      const holder: MemoryHolder = { lost }
      if (options?.steal === true) {
        const previous = state.holder
        if (previous !== undefined) Deferred.doneUnsafe(previous.lost, Effect.void)
        state.holder = holder
      } else if (state.holder === undefined && state.queue.length === 0) {
        state.holder = holder
      } else {
        const waiter: MemoryWaiter = { holder, grant: yield* Deferred.make<void>(), interrupted: false }
        state.queue.push(waiter)
        yield* Scope.addFinalizer(
          scope,
          Effect.sync(() => {
            waiter.interrupted = true
            if (state.holder === holder) promoteNext(state)
          })
        )
        yield* Deferred.await(waiter.grant)
      }
      yield* Scope.addFinalizer(
        scope,
        Effect.sync(() => {
          if (state.holder === holder) promoteNext(state)
        })
      )
      const hold: platform.WebLockHold = { lost: Deferred.await(lost) }
      return hold
    })
  }

  const epochStore: platform.EpochStoreService = {
    bump: (key) =>
      Effect.sync(() => {
        const next = (epochs.get(key) ?? 0) + 1
        epochs.set(key, next)
        return next
      })
  }

  const clientIdentityStore: platform.ClientIdentityStoreService = {
    load: (key) => Effect.sync(() => identities.get(key)),
    store: (key, value) =>
      Effect.sync(() => {
        identities.set(key, value)
      })
  }

  return {
    tabChannel,
    webLocks,
    epochStore,
    clientIdentityStore,
    layerAll: EffectLayer.mergeAll(
      EffectLayer.succeed(platform.TabChannel, tabChannel),
      EffectLayer.succeed(platform.WebLocks, webLocks),
      EffectLayer.succeed(platform.EpochStore, epochStore),
      EffectLayer.succeed(platform.ClientIdentityStore, clientIdentityStore)
    )
  }
})
