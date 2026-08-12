import { assert, describe, it } from "@effect/vitest"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as TestClock from "effect/testing/TestClock"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as Socket from "effect/unstable/socket/Socket"
import * as SyncClient from "../src/SyncClient.js"

describe("SyncClient", () => {
  it.effect("uses the configured socket retry policy", () =>
    Effect.gen(function*() {
      const attempts = yield* Ref.make(0)
      const firstAttempt = yield* Deferred.make<void>()
      const openError = new Socket.SocketError({
        reason: new Socket.SocketOpenError({
          kind: "Unknown",
          cause: "controlled open failure"
        })
      })
      const socket = Socket.make({
        runRaw: () =>
          Ref.updateAndGet(attempts, (attempt) => attempt + 1).pipe(
            Effect.tap((attempt) => {
              if (attempt === 1) return Deferred.succeed(firstAttempt, undefined)
              return Effect.void
            }),
            Effect.andThen(Effect.fail(openError))
          ),
        writer: Effect.succeed(() => Effect.void)
      })
      const live = SyncClient.layerProtocolSocket({
        retryPolicy: Schedule.spaced("5 seconds")
      }).pipe(
        Layer.provide(Layer.succeed(Socket.Socket, socket)),
        Layer.provide(RpcSerialization.layerJson)
      )

      yield* Layer.build(live)
      yield* Deferred.await(firstAttempt)
      assert.strictEqual(yield* Ref.get(attempts), 1)

      yield* TestClock.adjust("4999 millis")
      assert.strictEqual(yield* Ref.get(attempts), 1)

      yield* TestClock.adjust("1 millis")
      assert.strictEqual(yield* Ref.get(attempts), 2)
    }))
})
