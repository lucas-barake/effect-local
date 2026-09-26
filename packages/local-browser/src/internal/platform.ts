import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as EffectLayer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import type * as Stream from "effect/Stream"
import { BrowserStorageError } from "../BrowserStorageError.js"
import * as LosslessQueue from "./losslessQueue.js"

export { BrowserStorageError }

export interface WebLockHold {
  readonly lost: Effect.Effect<void>
}

export interface WebLocksService {
  readonly acquire: (
    name: string,
    options?: { readonly steal?: boolean }
  ) => Effect.Effect<WebLockHold, never, Scope.Scope>
  readonly tryAcquire: (name: string) => Effect.Effect<Option.Option<WebLockHold>, never, Scope.Scope>
  readonly held: Effect.Effect<ReadonlyArray<string>>
  readonly released: (name: string) => Effect.Effect<void>
}

export class WebLocks extends Context.Service<WebLocks, WebLocksService>()(
  "@lucas-barake/effect-local-browser/WebLocks"
) {}

const requestNavigatorLock = Effect.fnUntraced(function*(
  name: string,
  mode: { readonly steal?: boolean; readonly ifAvailable?: boolean }
) {
  const scope = yield* Effect.scope
  const granted = yield* Deferred.make<boolean>()
  const lost = yield* Deferred.make<void>()
  let releaseLock: () => void = () => {}
  let closed = false
  const controller = new AbortController()
  let lockOptions: LockOptions
  if (mode.steal === true) {
    lockOptions = { mode: "exclusive", steal: true }
  } else if (mode.ifAvailable === true) {
    lockOptions = { mode: "exclusive", ifAvailable: true }
  } else {
    lockOptions = { mode: "exclusive", signal: controller.signal }
  }
  const request = navigator.locks.request(
    name,
    lockOptions,
    (lock) => {
      if (lock === null) {
        Deferred.doneUnsafe(granted, Effect.succeed(false))
        return undefined
      }
      if (closed) return undefined
      // oxlint-disable-next-line effect/noNewPromise -- navigator.locks holds the lock exactly as long as the callback's promise stays pending, so the release must be a raw resolver the scope finalizer calls.
      const held = new Promise<void>((resolve) => {
        releaseLock = resolve
      })
      Deferred.doneUnsafe(granted, Effect.succeed(true))
      return held
    }
  )
  // The request promise rejects when the grant is aborted or a later `steal`
  // preempts a held lock; either way this holder no longer owns the name.
  request.then(
    () => Deferred.doneUnsafe(lost, Effect.void),
    () => Deferred.doneUnsafe(lost, Effect.void)
  )
  yield* Scope.addFinalizer(
    scope,
    Effect.sync(() => {
      closed = true
      releaseLock()
      controller.abort()
    })
  )
  const acquired = yield* Deferred.await(granted)
  return { acquired, lost: Deferred.await(lost) }
})

const acquireNavigatorLock = (name: string, options?: { readonly steal?: boolean }) =>
  requestNavigatorLock(name, { steal: options?.steal === true }).pipe(
    Effect.map((request): WebLockHold => ({ lost: request.lost }))
  )

const tryAcquireNavigatorLock = (name: string) =>
  requestNavigatorLock(name, { ifAvailable: true }).pipe(
    Effect.map((request) => {
      if (!request.acquired) return Option.none<WebLockHold>()
      return Option.some<WebLockHold>({ lost: request.lost })
    })
  )

const heldNavigatorLocks: Effect.Effect<ReadonlyArray<string>> = Effect.promise(() => navigator.locks.query()).pipe(
  Effect.map((snapshot) => {
    const names: Array<string> = []
    for (const lock of snapshot.held ?? []) {
      if (lock.mode === "exclusive" && lock.name !== undefined) names.push(lock.name)
    }
    return names
  })
)

const releasedNavigatorLock = (name: string): Effect.Effect<void> =>
  Effect.promise((signal) => navigator.locks.request(name, { mode: "shared", signal }, () => undefined)).pipe(
    Effect.asVoid
  )

export const layerWebLocksNavigator: EffectLayer.Layer<WebLocks> = EffectLayer.succeed(
  WebLocks,
  {
    acquire: acquireNavigatorLock,
    tryAcquire: tryAcquireNavigatorLock,
    held: heldNavigatorLocks,
    released: releasedNavigatorLock
  }
)

export interface TabChannelConnection {
  readonly post: (frame: unknown) => Effect.Effect<void>
  readonly messages: Effect.Effect<Queue.Dequeue<unknown>, never, Scope.Scope>
}

export interface TabChannelService {
  readonly open: (name: string) => Effect.Effect<TabChannelConnection, never, Scope.Scope>
}

export class TabChannel extends Context.Service<TabChannel, TabChannelService>()(
  "@lucas-barake/effect-local-browser/TabChannel"
) {}

const openBroadcastChannel = Effect.fnUntraced(function*(name: string) {
  const scope = yield* Effect.scope
  const channel = new BroadcastChannel(name)
  let closed = false
  yield* Scope.addFinalizer(
    scope,
    Effect.sync(() => {
      closed = true
      channel.close()
    })
  )
  const connection: TabChannelConnection = {
    post: (frame) =>
      Effect.sync(() => {
        if (!closed) channel.postMessage(frame)
      }),
    messages: Effect.gen(function*() {
      const subscriberScope = yield* Effect.scope
      const queue = yield* Queue.make<unknown>()
      const listener = (event: MessageEvent) => {
        Queue.offerUnsafe(queue, event.data)
      }
      channel.addEventListener("message", listener)
      yield* Scope.addFinalizer(
        subscriberScope,
        Effect.sync(() => channel.removeEventListener("message", listener)).pipe(
          Effect.andThen(Queue.shutdown(queue))
        )
      )
      return queue
    })
  }
  return connection
})

export const layerTabChannelBroadcast: EffectLayer.Layer<TabChannel> = EffectLayer.succeed(
  TabChannel,
  { open: openBroadcastChannel }
)

export interface TabVisibilityService {
  readonly visible: Effect.Effect<boolean>
  readonly changes: Stream.Stream<void>
}

export class TabVisibility extends Context.Service<TabVisibility, TabVisibilityService>()(
  "@lucas-barake/effect-local-browser/TabVisibility"
) {}

export const layerTabVisibilityDocument: EffectLayer.Layer<TabVisibility> = EffectLayer.succeed(
  TabVisibility,
  {
    visible: Effect.sync(() => typeof document !== "object" || document.visibilityState !== "hidden"),
    changes: LosslessQueue.callback<void>((queue) => {
      if (typeof document !== "object") return Queue.offer(queue, undefined)
      const listener = () => {
        Queue.offerUnsafe(queue, undefined)
      }
      return Effect.acquireRelease(
        Effect.sync(() => {
          document.addEventListener("visibilitychange", listener)
          Queue.offerUnsafe(queue, undefined)
        }),
        () => Effect.sync(() => document.removeEventListener("visibilitychange", listener))
      )
    }, { bufferSize: 1, strategy: "sliding" })
  }
)

export interface ClientIdentityStoreService {
  readonly load: (key: string) => Effect.Effect<string | undefined, BrowserStorageError>
  readonly store: (key: string, value: string) => Effect.Effect<void, BrowserStorageError>
}

export class ClientIdentityStore extends Context.Service<ClientIdentityStore, ClientIdentityStoreService>()(
  "@lucas-barake/effect-local-browser/ClientIdentityStore"
) {}

export const layerClientIdentityStoreLocalStorage: EffectLayer.Layer<ClientIdentityStore> = EffectLayer.succeed(
  ClientIdentityStore,
  {
    load: (key) =>
      Effect.try({
        try: () => {
          // oxlint-disable-next-line effect/noGlobals -- This layer is the browser platform adapter for client identity storage.
          const raw = localStorage.getItem(key)
          if (raw === null) return undefined
          return raw
        },
        catch: (cause) => new BrowserStorageError({ operation: "read", key, cause })
      }),
    store: (key, value) =>
      Effect.try({
        try: () => {
          // oxlint-disable-next-line effect/noGlobals -- Same platform adapter boundary as the read above.
          localStorage.setItem(key, value)
        },
        catch: (cause) => new BrowserStorageError({ operation: "write", key, cause })
      })
  }
)
