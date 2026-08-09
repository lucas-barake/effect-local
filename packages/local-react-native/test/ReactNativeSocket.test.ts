import { assert, describe, it } from "@effect/vitest"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Socket from "effect/unstable/socket/Socket"
import type { AddressInfo } from "node:net"
import { WebSocketServer } from "ws"
import * as ReactNativeSocket from "../src/ReactNativeSocket.js"

describe("ReactNativeSocket", () => {
  it.effect("echoes text frames through the global WebSocket constructor", () =>
    Effect.gen(function*() {
      const server = new WebSocketServer({ host: "127.0.0.1", port: 0 })
      yield* Effect.addFinalizer(() => Effect.sync(() => server.close()))
      server.on("connection", (ws) => ws.on("message", (data, isBinary) => ws.send(data, { binary: isBinary })))
      yield* Effect.callback<void>((resume) => {
        server.once("listening", () => resume(Effect.void))
      })
      const { port } = server.address() as AddressInfo

      yield* Effect.gen(function*() {
        const socket = yield* Socket.Socket
        const received = yield* Deferred.make<string | Uint8Array>()
        yield* socket.runRaw((data) => Deferred.succeed(received, data).pipe(Effect.asVoid)).pipe(Effect.forkChild)
        const writer = yield* socket.writer
        yield* writer("ping")
        assert.strictEqual(yield* Deferred.await(received), "ping")
      }).pipe(Effect.provide(ReactNativeSocket.layerWebSocket(`ws://127.0.0.1:${port}/relay`)))
    }))
})
