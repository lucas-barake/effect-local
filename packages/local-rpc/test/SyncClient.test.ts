import { assert, describe, it } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as TestClock from "effect/testing/TestClock"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
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

  it.effect("forgets routing for an interrupted socket request", () =>
    Effect.scoped(Effect.gen(function*() {
      const incoming = yield* Queue.unbounded<{
        readonly message: string | Uint8Array
        readonly processed: Deferred.Deferred<void>
      }>()
      const socketReady = yield* Deferred.make<void>()
      const socket = Socket.make({
        runRaw: (handler, options) =>
          Effect.gen(function*() {
            yield* options?.onOpen ?? Effect.void
            yield* Deferred.succeed(socketReady, undefined)
            yield* Queue.take(incoming).pipe(
              Effect.flatMap(({ message, processed }) =>
                Effect.gen(function*() {
                  const result = handler(message)
                  if (Effect.isEffect(result)) yield* result
                  yield* Deferred.succeed(processed, undefined)
                })
              ),
              Effect.forever
            )
          }),
        writer: Effect.succeed(() => Effect.void)
      })
      const live = SyncClient.layerProtocolSocket().pipe(
        Layer.provide(Layer.succeed(Socket.Socket, socket)),
        Layer.provide(RpcSerialization.layerJson)
      )
      const context = yield* Layer.build(live)
      const protocol = Context.get(context, RpcClient.Protocol)
      const firstClientResponses = yield* Ref.make(0)
      const secondClientResponses = yield* Ref.make(0)
      yield* protocol.run(1, () => Ref.update(firstClientResponses, (count) => count + 1)).pipe(
        Effect.forkScoped({ startImmediately: true })
      )
      yield* protocol.run(2, () => Ref.update(secondClientResponses, (count) => count + 1)).pipe(
        Effect.forkScoped({ startImmediately: true })
      )
      yield* Deferred.await(socketReady)

      yield* protocol.send(1, {
        _tag: "Request",
        id: 1,
        tag: "Test",
        payload: undefined,
        headers: []
      })
      yield* protocol.send(1, { _tag: "Interrupt", requestId: 1 })
      const processed = yield* Deferred.make<void>()
      yield* Queue.offer(incoming, {
        message: RpcSerialization.json.makeUnsafe().encode({
          _tag: "Exit",
          requestId: 1,
          exit: { _tag: "Success", value: undefined }
        })!,
        processed
      })
      yield* Deferred.await(processed)

      assert.strictEqual(yield* Ref.get(firstClientResponses), 1)
      assert.strictEqual(yield* Ref.get(secondClientResponses), 1)
    })))
})
