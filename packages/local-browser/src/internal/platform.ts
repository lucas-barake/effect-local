import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as EffectLayer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import { BrowserStorageError } from "../BrowserStorageError.js"

export { BrowserStorageError }

const StoredEpoch = Schema.NumberFromString.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0)
)

export interface WebLockHold {
  readonly lost: Effect.Effect<void>
}

export interface WebLocksService {
  readonly acquire: (
    name: string,
    options?: { readonly steal?: boolean }
  ) => Effect.Effect<WebLockHold, never, Scope.Scope>
}

export class WebLocks extends Context.Service<WebLocks, WebLocksService>()(
  "@lucas-barake/effect-local-browser/WebLocks"
) {}

const acquireNavigatorLock = Effect.fnUntraced(function*(
  name: string,
  options?: { readonly steal?: boolean }
) {
  const scope = yield* Effect.scope
  const granted = yield* Deferred.make<void>()
  const lost = yield* Deferred.make<void>()
  let releaseLock: () => void = () => {}
  const controller = new AbortController()
  let lockOptions: LockOptions
  if (options?.steal === true) {
    lockOptions = { mode: "exclusive", steal: true }
  } else {
    lockOptions = { mode: "exclusive", signal: controller.signal }
  }
  const request = navigator.locks.request(
    name,
    lockOptions,
    () => {
      Deferred.doneUnsafe(granted, Effect.void)
      // oxlint-disable-next-line effect/noNewPromise -- navigator.locks holds the lock exactly as long as the callback's promise stays pending, so the release must be a raw resolver the scope finalizer calls.
      return new Promise<void>((resolve) => {
        releaseLock = resolve
      })
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
      releaseLock()
      controller.abort()
    })
  )
  yield* Deferred.await(granted)
  const hold: WebLockHold = { lost: Deferred.await(lost) }
  return hold
})

export const layerWebLocksNavigator: EffectLayer.Layer<WebLocks> = EffectLayer.succeed(
  WebLocks,
  { acquire: acquireNavigatorLock }
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
  yield* Scope.addFinalizer(scope, Effect.sync(() => channel.close()))
  const connection: TabChannelConnection = {
    post: (frame) => Effect.sync(() => channel.postMessage(frame)),
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

export interface EpochStoreService {
  readonly bump: (key: string) => Effect.Effect<number, BrowserStorageError>
}

export class EpochStore extends Context.Service<EpochStore, EpochStoreService>()(
  "@lucas-barake/effect-local-browser/EpochStore"
) {}

export const layerEpochStoreLocalStorage: EffectLayer.Layer<EpochStore> = EffectLayer.succeed(
  EpochStore,
  {
    bump: Effect.fnUntraced(function*(key) {
      const raw = yield* Effect.try({
        try: () => {
          // oxlint-disable-next-line effect/noGlobals -- This layer is the browser platform adapter for epoch storage; the epoch must be readable synchronously before any database is open.
          return localStorage.getItem(key)
        },
        catch: (cause) => new BrowserStorageError({ operation: "read", key, cause })
      })
      let previous = 0
      if (raw !== null) {
        previous = yield* Schema.decodeUnknownEffect(StoredEpoch)(raw).pipe(
          Effect.mapError((cause) => new BrowserStorageError({ operation: "decode", key, cause }))
        )
      }
      const next = previous + 1
      yield* Effect.try({
        try: () => {
          // oxlint-disable-next-line effect/noGlobals -- Same platform adapter boundary as the read above.
          localStorage.setItem(key, String(next))
        },
        catch: (cause) => new BrowserStorageError({ operation: "write", key, cause })
      })
      return next
    })
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
