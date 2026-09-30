import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Socket from "effect/socket/Socket"
import * as TestClock from "effect/testing/TestClock"
import * as ReactNativeSocket from "../src/ReactNativeSocket.js"

class AndroidWebSocket extends EventTarget {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3
  static readonly created: Array<AndroidWebSocket> = []
  readonly url: string
  readonly protocols: string | Array<string> | undefined
  readonly options: { readonly headers?: Readonly<Record<string, string>> | undefined } | undefined
  readyState = AndroidWebSocket.CONNECTING
  binaryType = ""
  readonly closed: Array<readonly [number | undefined, string | undefined]> = []
  constructor(
    url: string,
    protocols?: string | Array<string>,
    options?: { readonly headers?: Readonly<Record<string, string>> | undefined }
  ) {
    super()
    this.url = url
    this.protocols = protocols
    this.options = options
    AndroidWebSocket.created.push(this)
  }
  send(_data: string | Uint8Array) {}
  close(code?: number, reason?: string) {
    if (this.readyState === AndroidWebSocket.CLOSING || this.readyState === AndroidWebSocket.CLOSED) return
    const connecting = this.readyState === AndroidWebSocket.CONNECTING
    this.readyState = AndroidWebSocket.CLOSING
    if (connecting) return
    this.closed.push([code, reason])
  }
  open() {
    this.readyState = AndroidWebSocket.OPEN
    this.dispatchEvent(new Event("open"))
  }
}

const withAndroidWebSocket = Effect.acquireRelease(
  Effect.sync(() => {
    const previous = globalThis.WebSocket
    AndroidWebSocket.created.length = 0
    Object.defineProperty(globalThis, "WebSocket", { configurable: true, writable: true, value: AndroidWebSocket })
    return previous
  }),
  (previous) =>
    Effect.sync(() => {
      Object.defineProperty(globalThis, "WebSocket", { configurable: true, writable: true, value: previous })
    })
)

const constructorFrom = Effect.gen(function*() {
  yield* withAndroidWebSocket
  return yield* Socket.WebSocketConstructor.pipe(Effect.provide(ReactNativeSocket.layerWebSocketConstructor))
})

describe("ReactNativeSocket", () => {
  it.effect(
    "forwards protocols and handshake headers to the React Native WebSocket",
    Effect.fnUntraced(function*() {
      const make = yield* constructorFrom
      make("ws://example.test/sync", ["chat"])
      make("ws://example.test/sync", { headers: { authorization: "Bearer token" } })
      const [withProtocols, withHeaders] = AndroidWebSocket.created
      assert.deepStrictEqual(withProtocols.protocols, ["chat"])
      assert.deepStrictEqual(withHeaders.options, { headers: { authorization: "Bearer token" } })
    }, Effect.scoped)
  )

  it.effect(
    "closes a socket released while connecting once it opens",
    Effect.fnUntraced(function*() {
      const make = yield* constructorFrom
      const socket = make("ws://example.test/sync")
      socket.close(4000, "released")
      const [native] = AndroidWebSocket.created
      assert.deepStrictEqual(native.closed, [])
      native.open()
      assert.deepStrictEqual(native.closed, [[4000, "released"]])
    }, Effect.scoped)
  )

  it.effect(
    "closes the native socket when an Effect socket times out before it opens",
    Effect.fnUntraced(function*() {
      yield* withAndroidWebSocket
      const socket = yield* Socket.makeWebSocket("ws://example.test/sync", { openTimeout: "1 second" }).pipe(
        Effect.provide(ReactNativeSocket.layerWebSocketConstructor)
      )
      const run = yield* Effect.scoped(socket.reader).pipe(Effect.forkChild)
      yield* TestClock.adjust("1 second")
      const exit = yield* Fiber.await(run)
      assert.strictEqual(exit._tag, "Failure")
      const [native] = AndroidWebSocket.created
      assert.deepStrictEqual(native.closed, [])
      native.open()
      assert.strictEqual(native.closed.length, 1)
    }, Effect.scoped)
  )
})
