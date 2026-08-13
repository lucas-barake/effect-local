import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import { pipe } from "effect/Function"
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
  it.effect(
    "uses the configured socket retry policy",
    Effect.fnUntraced(function*() {
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
      const layerLive = SyncClient.layerProtocolSocket({
        retryPolicy: Schedule.spaced("5 seconds")
      }).pipe(
        Layer.provide(Layer.succeed(Socket.Socket, socket)),
        Layer.provide(RpcSerialization.layerJson)
      )

      yield* Layer.build(layerLive)
      yield* Deferred.await(firstAttempt)
      assert.strictEqual(yield* Ref.get(attempts), 1)

      yield* TestClock.adjust("4999 millis")
      assert.strictEqual(yield* Ref.get(attempts), 1)

      yield* TestClock.adjust("1 millis")
      assert.strictEqual(yield* Ref.get(attempts), 2)
    })
  )

  it.effect("forgets routing for an interrupted socket request", () =>
    Effect.scoped(Effect.gen(function*() {
      const incoming = yield* Queue.unbounded<{
        readonly message: string | Uint8Array
        readonly processed: Deferred.Deferred<void>
      }>()
      const socketReady = yield* Deferred.make<void>()
      const socket = Socket.make({
        runRaw: Effect.fnUntraced(function*(handler, options) {
          yield* options?.onOpen ?? Effect.void
          yield* Deferred.succeed(socketReady, undefined)
          yield* Queue.take(incoming).pipe(
            Effect.flatMap(Effect.fnUntraced(function*({ message, processed }) {
              const result = handler(message)
              if (Effect.isEffect(result)) yield* result
              yield* Deferred.succeed(processed, undefined)
            })),
            Effect.forever
          )
        }),
        writer: Effect.succeed(() => Effect.void)
      })
      const layerLive = SyncClient.layerProtocolSocket().pipe(
        Layer.provide(Layer.succeed(Socket.Socket, socket)),
        Layer.provide(RpcSerialization.layerJson)
      )
      const context = yield* Layer.build(layerLive)
      const protocol = Context.get(context, RpcClient.Protocol)
      const firstClientResponses = yield* Ref.make<Array<string>>([])
      const secondClientResponses = yield* Ref.make<Array<string>>([])
      yield* protocol.run(1, (response) => Ref.update(firstClientResponses, (tags) => [...tags, response._tag])).pipe(
        Effect.forkScoped({ startImmediately: true })
      )
      yield* protocol.run(2, (response) => Ref.update(secondClientResponses, (tags) => [...tags, response._tag])).pipe(
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
          exit: { _tag: "Success", value: null }
        })!,
        processed
      })
      yield* Deferred.await(processed)

      assert.deepStrictEqual(yield* Ref.get(firstClientResponses), ["Exit"])
      assert.deepStrictEqual(yield* Ref.get(secondClientResponses), ["Exit"])
    })))

  it.effect("forgets routing after a socket failure", () =>
    Effect.scoped(Effect.gen(function*() {
      const incoming = yield* Queue.unbounded<{
        readonly message: string | Uint8Array
        readonly processed: Deferred.Deferred<void>
      }>()
      const firstSocketReady = yield* Deferred.make<void>()
      const failFirstSocket = yield* Deferred.make<void>()
      const secondSocketReady = yield* Deferred.make<void>()
      const secondClientSawFailure = yield* Deferred.make<void>()
      const connections = yield* Ref.make(0)
      const socketError = new Socket.SocketError({
        reason: new Socket.SocketCloseError({ code: 1006 })
      })
      const socket = Socket.make({
        runRaw: (handler, options) =>
          Ref.updateAndGet(connections, (count) => count + 1).pipe(
            Effect.flatMap(Effect.fnUntraced(function*(connection) {
              yield* options?.onOpen ?? Effect.void
              if (connection === 1) {
                yield* Deferred.succeed(firstSocketReady, undefined)
                yield* Deferred.await(failFirstSocket)
                return yield* socketError
              }
              yield* Deferred.succeed(secondSocketReady, undefined)
              return yield* Queue.take(incoming).pipe(
                Effect.flatMap(Effect.fnUntraced(function*({ message, processed }) {
                  const result = handler(message)
                  if (Effect.isEffect(result)) yield* result
                  yield* Deferred.succeed(processed, undefined)
                })),
                Effect.forever
              )
            }))
          ),
        writer: Effect.succeed(() => Effect.void)
      })
      const layerLive = SyncClient.layerProtocolSocket({
        retryPolicy: Schedule.spaced("1 second")
      }).pipe(
        Layer.provide(Layer.succeed(Socket.Socket, socket)),
        Layer.provide(RpcSerialization.layerJson)
      )
      const context = yield* Layer.build(layerLive)
      const protocol = Context.get(context, RpcClient.Protocol)
      const firstClientResponses = yield* Ref.make<Array<string>>([])
      const secondClientResponses = yield* Ref.make<Array<string>>([])
      yield* protocol.run(1, (response) => Ref.update(firstClientResponses, (tags) => [...tags, response._tag])).pipe(
        Effect.forkScoped({ startImmediately: true })
      )
      yield* protocol.run(
        2,
        Effect.fnUntraced(function*(response) {
          yield* Ref.update(secondClientResponses, (tags) => [...tags, response._tag])
          if (response._tag === "ClientProtocolError") {
            yield* Deferred.succeed(secondClientSawFailure, undefined)
          }
        })
      ).pipe(Effect.forkScoped({ startImmediately: true }))
      yield* Deferred.await(firstSocketReady)

      yield* protocol.send(1, {
        _tag: "Request",
        id: 1,
        tag: "Test",
        payload: undefined,
        headers: []
      })
      yield* Deferred.succeed(failFirstSocket, undefined)
      yield* Deferred.await(secondClientSawFailure)
      yield* TestClock.adjust("1 second")
      yield* Deferred.await(secondSocketReady)

      const processed = yield* Deferred.make<void>()
      yield* Queue.offer(incoming, {
        message: RpcSerialization.json.makeUnsafe().encode({
          _tag: "Exit",
          requestId: 1,
          exit: { _tag: "Success", value: null }
        })!,
        processed
      })
      yield* Deferred.await(processed)

      assert.deepStrictEqual(yield* Ref.get(firstClientResponses), ["ClientProtocolError", "Exit"])
      assert.deepStrictEqual(yield* Ref.get(secondClientResponses), ["ClientProtocolError", "Exit"])
    })))

  it.effect("fails active routes before retrying a socket open error", () =>
    Effect.scoped(Effect.gen(function*() {
      const incoming = yield* Queue.unbounded<{
        readonly message: string | Uint8Array
        readonly processed: Deferred.Deferred<void>
      }>()
      const firstSocketReady = yield* Deferred.make<void>()
      const failFirstSocket = yield* Deferred.make<void>()
      const secondSocketReady = yield* Deferred.make<void>()
      const firstClientSawFailure = yield* Deferred.make<void>()
      const connections = yield* Ref.make(0)
      const socketError = new Socket.SocketError({
        reason: new Socket.SocketOpenError({ kind: "Timeout", cause: "ping timeout" })
      })
      const socket = Socket.make({
        runRaw: (handler, options) =>
          Ref.updateAndGet(connections, (count) => count + 1).pipe(
            Effect.flatMap(Effect.fnUntraced(function*(connection) {
              yield* options?.onOpen ?? Effect.void
              if (connection === 1) {
                yield* Deferred.succeed(firstSocketReady, undefined)
                yield* Deferred.await(failFirstSocket)
                return yield* socketError
              }
              yield* Deferred.succeed(secondSocketReady, undefined)
              return yield* Queue.take(incoming).pipe(
                Effect.flatMap(Effect.fnUntraced(function*({ message, processed }) {
                  const result = handler(message)
                  if (Effect.isEffect(result)) yield* result
                  yield* Deferred.succeed(processed, undefined)
                })),
                Effect.forever
              )
            }))
          ),
        writer: Effect.succeed(() => Effect.void)
      })
      const layerLive = SyncClient.layerProtocolSocket({
        retryTransientErrors: true,
        retryPolicy: Schedule.spaced("1 second")
      }).pipe(
        Layer.provide(Layer.succeed(Socket.Socket, socket)),
        Layer.provide(RpcSerialization.layerJson)
      )
      const context = yield* Layer.build(layerLive)
      const protocol = Context.get(context, RpcClient.Protocol)
      const firstClientResponses = yield* Ref.make<Array<string>>([])
      const secondClientResponses = yield* Ref.make<Array<string>>([])
      yield* protocol.run(
        1,
        Effect.fnUntraced(function*(response) {
          yield* Ref.update(firstClientResponses, (tags) => [...tags, response._tag])
          if (response._tag === "ClientProtocolError") {
            yield* Deferred.succeed(firstClientSawFailure, undefined)
          }
        })
      ).pipe(Effect.forkScoped({ startImmediately: true }))
      yield* protocol.run(2, (response) => Ref.update(secondClientResponses, (tags) => [...tags, response._tag])).pipe(
        Effect.forkScoped({ startImmediately: true })
      )
      yield* Deferred.await(firstSocketReady)

      yield* protocol.send(1, {
        _tag: "Request",
        id: 1,
        tag: "Test",
        payload: undefined,
        headers: []
      })
      yield* Deferred.succeed(failFirstSocket, undefined)
      yield* Deferred.await(firstClientSawFailure)
      yield* TestClock.adjust("1 second")
      yield* Deferred.await(secondSocketReady)

      const processed = yield* Deferred.make<void>()
      yield* Queue.offer(incoming, {
        message: RpcSerialization.json.makeUnsafe().encode({
          _tag: "Exit",
          requestId: 1,
          exit: { _tag: "Success", value: null }
        })!,
        processed
      })
      yield* Deferred.await(processed)

      assert.deepStrictEqual(yield* Ref.get(firstClientResponses), ["ClientProtocolError", "Exit"])
      assert.deepStrictEqual(yield* Ref.get(secondClientResponses), ["ClientProtocolError", "Exit"])
    })))

  it.effect("accepts a valid interrupted exit", () =>
    Effect.scoped(Effect.gen(function*() {
      const incoming = yield* Queue.unbounded<string | Uint8Array>()
      const socketReady = yield* Deferred.make<void>()
      const responseReceived = yield* Deferred.make<void>()
      const socket = Socket.make({
        runRaw: Effect.fnUntraced(function*(handler, options) {
          yield* options?.onOpen ?? Effect.void
          yield* Deferred.succeed(socketReady, undefined)
          yield* Queue.take(incoming).pipe(
            Effect.flatMap((message) => {
              const result = handler(message)
              if (Effect.isEffect(result)) return result
              return Effect.void
            }),
            Effect.forever
          )
        }),
        writer: Effect.succeed(() => Effect.void)
      })
      const context = yield* Layer.build(
        SyncClient.layerProtocolSocket().pipe(
          Layer.provide(Layer.succeed(Socket.Socket, socket)),
          Layer.provide(RpcSerialization.layerJson)
        )
      )
      const protocol = Context.get(context, RpcClient.Protocol)
      const responses = yield* Ref.make<Array<string>>([])
      yield* protocol.run(1, (response) =>
        Ref.update(responses, (tags) => [...tags, response._tag]).pipe(
          Effect.andThen(Deferred.succeed(responseReceived, undefined))
        )).pipe(Effect.forkScoped({ startImmediately: true }))
      yield* Deferred.await(socketReady)
      yield* protocol.send(1, {
        _tag: "Request",
        id: 1,
        tag: "Test",
        payload: undefined,
        headers: []
      })
      yield* pipe(
        RpcSerialization.json.makeUnsafe().encode({
          _tag: "Exit",
          requestId: 1,
          exit: { _tag: "Failure", cause: [{ _tag: "Interrupt", fiberId: null }] }
        })!,
        (message) => Queue.offer(incoming, message)
      )
      yield* Deferred.await(responseReceived)

      assert.deepStrictEqual(yield* Ref.get(responses), ["Exit"])
    })))

  it.effect("preserves an unknown socket failure cause", () =>
    Effect.scoped(Effect.gen(function*() {
      const socketReady = yield* Deferred.make<void>()
      const failSocket = yield* Deferred.make<void>()
      const responseReceived = yield* Deferred.make<void>()
      const socket = Socket.make({
        runRaw: Effect.fnUntraced(function*(_handler, options) {
          yield* options?.onOpen ?? Effect.void
          yield* Deferred.succeed(socketReady, undefined)
          yield* Deferred.await(failSocket)
          return yield* Effect.die("socket defect")
        }),
        writer: Effect.succeed(() => Effect.void)
      })
      const context = yield* Layer.build(
        SyncClient.layerProtocolSocket({ retryPolicy: Schedule.recurs(0) }).pipe(
          Layer.provide(Layer.succeed(Socket.Socket, socket)),
          Layer.provide(RpcSerialization.layerJson)
        )
      )
      const protocol = Context.get(context, RpcClient.Protocol)
      const causes = yield* Ref.make<Array<unknown>>([])
      yield* protocol.run(1, (response) => {
        if (response._tag !== "ClientProtocolError") return Effect.void
        if (response.error.reason._tag !== "RpcClientDefect") return Effect.void
        return Ref.update(causes, (values) => [...values, response.error.reason.cause]).pipe(
          Effect.andThen(Deferred.succeed(responseReceived, undefined))
        )
      }).pipe(Effect.forkScoped({ startImmediately: true }))
      yield* Deferred.await(socketReady)
      yield* protocol.send(1, {
        _tag: "Request",
        id: 1,
        tag: "Test",
        payload: undefined,
        headers: []
      })
      yield* Deferred.succeed(failSocket, undefined)
      yield* Deferred.await(responseReceived)

      const [cause] = yield* Ref.get(causes)
      assert.isTrue(Cause.isCause(cause))
      if (!Cause.isCause(cause)) return
      const defect = Cause.findDefect(cause)
      assert.isTrue(defect._tag === "Success")
      if (defect._tag === "Success") assert.strictEqual(defect.success, "socket defect")
    })))
})
