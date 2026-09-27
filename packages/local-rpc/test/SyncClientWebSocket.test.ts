import { NodeSocket } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import type * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as FiberSet from "effect/FiberSet"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schedule from "effect/Schedule"
import * as Scheduler from "effect/Scheduler"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import * as Socket from "effect/unstable/socket/Socket"
import * as Authentication from "../src/Authentication.js"
import type * as EphemeralClient from "../src/EphemeralClient.js"
import * as SyncClient from "../src/SyncClient.js"

/**
 * The bundle points at a closed port, so every connection attempt fails and the
 * protocol socket's retry loop acquires the address again. The recorded urls
 * are what each reconnect actually dialled.
 */
class ConnectionAddress extends Context.Service<ConnectionAddress, {
  readonly next: Effect.Effect<string>
}>()("@lucas-barake/effect-local-rpc/test/ConnectionAddress") {}

const nextAddress = ConnectionAddress.use((address) => address.next)

const layerStaticCredential = Authentication.layerCredentialProviderStatic(Redacted.make("token"))

const layerConnectionAddress = Layer.sync(ConnectionAddress, () => {
  let attempt = 0
  const next = Effect.sync(() => `ws://127.0.0.1:1/sync?attempt=${attempt++}`)
  return ConnectionAddress.of({ next })
})

// Records every dialled url and resolves once the retry loop has dialled
// `expected` times, so the test rendezvouses on observed connections rather
// than on wall clock time.
const makeRecordingConstructor = Effect.fnUntraced(function*(expected: number) {
  const dialled: Array<string> = []
  const reached = yield* Deferred.make<void>()
  const layer = Layer.effect(
    Socket.WebSocketConstructor,
    Effect.gen(function*() {
      const makeWebSocket = yield* Socket.WebSocketConstructor
      return (url: string, options?: Socket.WebSocketConstructorOptions) => {
        dialled.push(url)
        if (dialled.length === expected) Deferred.doneUnsafe(reached, Exit.void)
        return makeWebSocket(url, options)
      }
    })
  ).pipe(Layer.provide(NodeSocket.layerWebSocketConstructor))
  return { dialled, reached, layer } as const
})

const buildAndDial = (
  layer: Layer.Layer<
    SyncEngine.SyncEngine | EphemeralClient.EphemeralClient,
    ReplicaError.InvalidConfiguration
  >,
  reached: Deferred.Deferred<void>
) => Layer.build(layer).pipe(Effect.andThen(Deferred.await(reached)), Effect.scoped)

type WebSocketEventType = "open" | "message" | "error" | "close"

class OpensAfterFirstStateRead implements Socket.WebSocketLike {
  readonly listeners = new Map<WebSocketEventType, Set<(event: Socket.WebSocketEvent) => void>>()
  state = 0
  openScheduled = false

  constructor(readonly deliver: (effect: Effect.Effect<void>) => unknown) {}

  get readyState(): number {
    if (!this.openScheduled) {
      this.openScheduled = true
      this.deliver(Effect.yieldNow.pipe(Effect.andThen(Effect.sync(() => {
        this.state = 1
        for (const listener of Array.from(this.listeners.get("open") ?? [])) listener({ type: "open" })
      }))))
    }
    return this.state
  }

  addEventListener(type: WebSocketEventType, listener: (event: Socket.WebSocketEvent) => void): void {
    const listeners = this.listeners.get(type) ?? new Set()
    listeners.add(listener)
    this.listeners.set(type, listeners)
  }

  removeEventListener(type: WebSocketEventType, listener: (event: Socket.WebSocketEvent) => void): void {
    this.listeners.get(type)?.delete(listener)
  }

  close(): void {}
  send(): void {}
}

const connectsWhenOpenArrivesDuringAcquisition = Effect.fnUntraced(function*() {
  const connected = yield* Deferred.make<void>()
  const deliver = yield* FiberSet.makeRuntime()
  const hooks = RpcClient.ConnectionHooks.of({
    onConnect: Deferred.succeed(connected, undefined),
    onDisconnect: Effect.void
  })
  const layer = SyncClient.layerWebSocket({ url: "ws://127.0.0.1:1/sync" }).pipe(
    Layer.provide(Layer.succeed(Socket.WebSocketConstructor, () => new OpensAfterFirstStateRead(deliver))),
    Layer.provide(Layer.succeed(RpcClient.ConnectionHooks, hooks)),
    Layer.provide(layerStaticCredential)
  )
  yield* Layer.build(layer)
  yield* Deferred.await(connected)
}, Effect.scoped)

describe("SyncClient.layerWebSocket", () => {
  it.effect(
    "connects when the socket opens while the reader is still being acquired",
    () => connectsWhenOpenArrivesDuringAcquisition().pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 5))
  )

  it.live(
    "re-runs an Effect url on every reconnect instead of pinning the first address",
    Effect.fnUntraced(function*() {
      const recorder = yield* makeRecordingConstructor(3)
      const layer = SyncClient.layerWebSocket({
        url: nextAddress,
        retryPolicy: Schedule.spaced(1)
      }).pipe(
        Layer.provide(recorder.layer),
        Layer.provide(layerConnectionAddress),
        Layer.provide(layerStaticCredential)
      )
      yield* buildAndDial(layer, recorder.reached)
      assert.deepStrictEqual(recorder.dialled.slice(0, 3), [
        "ws://127.0.0.1:1/sync?attempt=0",
        "ws://127.0.0.1:1/sync?attempt=1",
        "ws://127.0.0.1:1/sync?attempt=2"
      ])
    }, Effect.scoped)
  )

  it.live(
    "dials a string url unchanged on every reconnect",
    Effect.fnUntraced(function*() {
      const recorder = yield* makeRecordingConstructor(3)
      const layer = SyncClient.layerWebSocket({
        url: "ws://127.0.0.1:1/sync",
        retryPolicy: Schedule.spaced(1)
      }).pipe(
        Layer.provide(recorder.layer),
        Layer.provide(layerStaticCredential)
      )
      yield* buildAndDial(layer, recorder.reached)
      assert.deepStrictEqual(recorder.dialled.slice(0, 3), [
        "ws://127.0.0.1:1/sync",
        "ws://127.0.0.1:1/sync",
        "ws://127.0.0.1:1/sync"
      ])
    }, Effect.scoped)
  )

  it.live(
    "keeps reconnecting after the url Effect crashes on one attempt",
    Effect.fnUntraced(function*() {
      const recorder = yield* makeRecordingConstructor(4)
      let attempt = 0
      // A url resolved from another service can crash transiently. That must not
      // permanently stop the socket from ever dialling again.
      const url = Effect.suspend(() => {
        const current = attempt++
        if (current === 2) return Effect.die(new Error("url resolution crashed"))
        return Effect.succeed(`ws://127.0.0.1:1/sync?attempt=${current}`)
      })
      const layer = SyncClient.layerWebSocket({
        url,
        retryPolicy: Schedule.spaced(1)
      }).pipe(
        Layer.provide(recorder.layer),
        Layer.provide(layerStaticCredential)
      )
      yield* buildAndDial(layer, recorder.reached)
      assert.deepStrictEqual(recorder.dialled.slice(0, 4), [
        "ws://127.0.0.1:1/sync?attempt=0",
        "ws://127.0.0.1:1/sync?attempt=1",
        "ws://127.0.0.1:1/sync?attempt=3",
        "ws://127.0.0.1:1/sync?attempt=4"
      ])
    }, Effect.scoped)
  )
})
