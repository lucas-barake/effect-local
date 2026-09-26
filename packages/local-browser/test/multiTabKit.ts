import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as EffectLayer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import * as LosslessQueue from "../src/internal/losslessQueue.js"
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
  readonly releases: Array<Deferred.Deferred<void>>
}

interface ChannelTraffic {
  posted: number
  delivered: number
}

export interface MemoryPlatform {
  readonly traffic: ChannelTraffic
  readonly tabChannel: platform.TabChannelService
  readonly webLocks: platform.WebLocksService
  readonly clientIdentityStore: platform.ClientIdentityStoreService
  readonly layerAll: EffectLayer.Layer<
    platform.TabChannel | platform.WebLocks | platform.ClientIdentityStore
  >
}

export interface MemoryVisibility {
  readonly service: platform.TabVisibilityService
  readonly set: (visible: boolean) => Effect.Effect<void>
}

export const makeMemoryVisibility = (initial: boolean) =>
  Effect.sync((): MemoryVisibility => {
    let visible = initial
    const listeners = new Set<() => void>()
    const service: platform.TabVisibilityService = {
      visible: Effect.sync(() => visible),
      changes: LosslessQueue.callback<void>((queue) =>
        Effect.acquireRelease(
          Effect.sync(() => {
            const listener = () => {
              Queue.offerUnsafe(queue, undefined)
            }
            listeners.add(listener)
            listener()
            return listener
          }),
          (listener) => Effect.sync(() => listeners.delete(listener))
        ), { bufferSize: 1, strategy: "sliding" })
    }
    return {
      service,
      set: (next) =>
        Effect.sync(() => {
          visible = next
          for (const listener of listeners) listener()
        })
    }
  })

export const makeMemoryPlatform = Effect.sync((): MemoryPlatform => {
  const channels = new Map<string, Set<MemoryConnection>>()
  const locks = new Map<string, MemoryLock>()
  const identities = new Map<string, string>()
  const traffic: ChannelTraffic = { posted: 0, delivered: 0 }

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
            traffic.posted += 1
            for (const peer of registered) {
              if (peer === connection) continue
              for (const queue of peer.subscribers) {
                traffic.delivered += 1
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
      state = { holder: undefined, queue: [], releases: [] }
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
    for (const release of state.releases.splice(0)) Deferred.doneUnsafe(release, Effect.void)
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
    }),
    tryAcquire: Effect.fnUntraced(function*(name) {
      const scope = yield* Effect.scope
      const state = lockState(name)
      if (state.holder !== undefined || state.queue.length > 0) return Option.none<platform.WebLockHold>()
      const lost = yield* Deferred.make<void>()
      const holder: MemoryHolder = { lost }
      state.holder = holder
      yield* Scope.addFinalizer(
        scope,
        Effect.sync(() => {
          if (state.holder === holder) promoteNext(state)
        })
      )
      return Option.some<platform.WebLockHold>({ lost: Deferred.await(lost) })
    }),
    held: Effect.sync(() => {
      const names: Array<string> = []
      for (const [name, state] of locks) {
        if (state.holder !== undefined) names.push(name)
      }
      return names
    }),
    released: (name) =>
      Effect.suspend(() => {
        const state = lockState(name)
        if (state.holder === undefined) return Effect.void
        const release = Deferred.makeUnsafe<void>()
        state.releases.push(release)
        return Deferred.await(release).pipe(
          Effect.onInterrupt(() =>
            Effect.sync(() => {
              const index = state.releases.indexOf(release)
              if (index >= 0) state.releases.splice(index, 1)
            })
          )
        )
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
    traffic,
    tabChannel,
    webLocks,
    clientIdentityStore,
    layerAll: EffectLayer.mergeAll(
      EffectLayer.succeed(platform.TabChannel, tabChannel),
      EffectLayer.succeed(platform.WebLocks, webLocks),
      EffectLayer.succeed(platform.ClientIdentityStore, clientIdentityStore)
    )
  }
})
