import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as RpcClient from "effect/rpc/RpcClient"
import * as RpcSerialization from "effect/rpc/RpcSerialization"
import * as Schedule from "effect/Schedule"
import * as Socket from "effect/socket/Socket"
import * as TestClock from "effect/testing/TestClock"
import * as LosslessQueue from "../src/internal/losslessQueue.js"
import * as SyncClient from "../src/SyncClient.js"
import * as Transport from "../src/Transport.js"
import * as RecordingClock from "./fixtures/recordingClock.js"

const keepalivePingMillis = 5_000

const noopWriter: Socket.Writer = { write: () => Effect.void, writeAll: () => Effect.void }
const pong = RpcSerialization.json.makeUnsafe().encode({ _tag: "Pong" })

interface Frame {
  readonly message: string | Uint8Array
  readonly processed?: Deferred.Deferred<void> | undefined
}

const queueReader = (incoming: Queue.Dequeue<Frame>): Socket.Reader => {
  let previous: Deferred.Deferred<void> | undefined
  return {
    pull: Effect.suspend(() => {
      const handled = previous
      previous = undefined
      if (handled === undefined) return Effect.void
      return Deferred.succeed(handled, undefined)
    }).pipe(
      Effect.andThen(Queue.take(incoming)),
      Effect.map((frame) => {
        previous = frame.processed
        return [frame.message] as const
      })
    ),
    upgrade: () => Effect.void
  }
}

const failingReader = (
  gate: Deferred.Deferred<void>,
  failure: Effect.Effect<never, Socket.SocketError>
): Socket.Reader => ({
  pull: Deferred.await(gate).pipe(Effect.andThen(failure)),
  upgrade: () => Effect.void
})

describe("SyncClient", () => {
  it.effect(
    "keeps every reconnect delay within the cap and restarts the backoff after a successful connection",
    Effect.fnUntraced(function*() {
      const attempts = yield* Queue.unbounded<number>()
      const disconnects = yield* Queue.unbounded<number>()
      const count = yield* Ref.make(0)
      const connectingAttempt = 13
      const openError = new Socket.SocketError({
        reason: new Socket.SocketOpenError({ kind: "Unknown", cause: "server unreachable" })
      })
      const closeError = new Socket.SocketError({ reason: new Socket.SocketCloseError({ code: 1006 }) })
      const socket = Socket.make({
        reader: Effect.gen(function*() {
          const attempt = yield* Ref.updateAndGet(count, (current) => current + 1)
          yield* Queue.offer(attempts, yield* Clock.currentTimeMillis)
          if (attempt !== connectingAttempt) return yield* Effect.fail(openError)
          let delivered = false
          return {
            pull: Effect.suspend(() => {
              if (delivered) return Effect.fail(closeError)
              delivered = true
              if (pong === undefined) return Effect.die("the JSON serializer did not encode a Pong frame")
              return Effect.succeed([pong] as const)
            }),
            upgrade: () => Effect.void
          }
        }),
        writer: Effect.succeed(noopWriter)
      })
      const hooks = RpcClient.ConnectionHooks.of({
        onConnect: Effect.void,
        onDisconnect: Clock.currentTimeMillis.pipe(Effect.flatMap((now) => Queue.offer(disconnects, now)))
      })
      const layerLive = SyncClient.layerProtocolSocket().pipe(
        Layer.provide(Layer.succeed(Socket.Socket, socket)),
        Layer.provide(Layer.succeed(RpcClient.ConnectionHooks, hooks)),
        Layer.provide(RpcSerialization.layerJson)
      )
      const recording = yield* RecordingClock.make
      yield* Layer.build(layerLive).pipe(Effect.provideService(Clock.Clock, recording.clock))

      const delays: Array<number> = []
      let failedAt = yield* LosslessQueue.take(disconnects)
      yield* LosslessQueue.take(attempts)
      for (let attempt = 2; attempt <= connectingAttempt + 1; attempt++) {
        const reconnect = yield* recording.nextSleep((request) => request.millis !== keepalivePingMillis)
        yield* recording.advanceTo(reconnect.deadline)
        const attemptedAt = yield* LosslessQueue.take(attempts)
        delays.push(attemptedAt - failedAt)
        failedAt = yield* LosslessQueue.take(disconnects)
      }

      const cap = 2_000
      assert.isTrue(delays.every((delay) => delay <= cap), `delays ${delays.join(",")} exceed the ${cap} ms cap`)
      assert.isAtLeast(delays[connectingAttempt - 2], cap / 2)
      assert.isAtMost(delays[connectingAttempt - 1], 250)
    })
  )

  it.effect(
    "keeps backing off while the server accepts the upgrade and closes before sending a frame",
    Effect.fnUntraced(function*() {
      const attempts = yield* Queue.unbounded<number>()
      const disconnects = yield* Queue.unbounded<number>()
      const closeError = new Socket.SocketError({ reason: new Socket.SocketCloseError({ code: 1013 }) })
      const socket = Socket.make({
        reader: Clock.currentTimeMillis.pipe(
          Effect.flatMap((now) => Queue.offer(attempts, now)),
          Effect.as({ pull: Effect.fail(closeError), upgrade: () => Effect.void })
        ),
        writer: Effect.succeed(noopWriter)
      })
      const hooks = RpcClient.ConnectionHooks.of({
        onConnect: Effect.void,
        onDisconnect: Clock.currentTimeMillis.pipe(Effect.flatMap((now) => Queue.offer(disconnects, now)))
      })
      const layerLive = SyncClient.layerProtocolSocket().pipe(
        Layer.provide(Layer.succeed(Socket.Socket, socket)),
        Layer.provide(Layer.succeed(RpcClient.ConnectionHooks, hooks)),
        Layer.provide(RpcSerialization.layerJson)
      )
      const context = yield* Layer.build(layerLive)
      const transport = Context.get(context, Transport.Transport)
      const clock = yield* TestClock.adjust("100 millis").pipe(
        Effect.forever,
        Effect.forkChild({ startImmediately: true })
      )

      const delays: Array<number> = []
      let failedAt = yield* Queue.take(disconnects)
      yield* Queue.take(attempts)
      for (let cycle = 0; cycle < 8; cycle++) {
        const attemptedAt = yield* Queue.take(attempts)
        delays.push(attemptedAt - failedAt)
        failedAt = yield* Queue.take(disconnects)
      }
      yield* Fiber.interrupt(clock)

      assert.isAtLeast(delays[delays.length - 1], 1_000, `delays ${delays.join(",")} never grew`)
      assert.strictEqual(yield* transport.generation, 0)
    })
  )

  it.effect(
    "uses the configured socket retry policy",
    Effect.fnUntraced(function*() {
      const attempts = yield* Ref.make(0)
      const firstAttempt = yield* Deferred.make<void>()
      const secondAttemptAt = yield* Deferred.make<number>()
      const openError = new Socket.SocketError({
        reason: new Socket.SocketOpenError({
          kind: "Unknown",
          cause: "controlled open failure"
        })
      })
      const socket = Socket.make({
        reader: Ref.updateAndGet(attempts, (attempt) => attempt + 1).pipe(
          Effect.tap((attempt) => {
            if (attempt === 1) return Deferred.succeed(firstAttempt, undefined)
            if (attempt === 2) {
              return Clock.currentTimeMillis.pipe(Effect.flatMap((now) => Deferred.succeed(secondAttemptAt, now)))
            }
            return Effect.void
          }),
          Effect.andThen(Effect.fail(openError))
        ),
        writer: Effect.succeed(noopWriter)
      })
      const layerLive = SyncClient.layerProtocolSocket({
        retryPolicy: Schedule.spaced("7 seconds")
      }).pipe(
        Layer.provide(Layer.succeed(Socket.Socket, socket)),
        Layer.provide(RpcSerialization.layerJson)
      )
      const recording = yield* RecordingClock.make

      yield* Layer.build(layerLive).pipe(Effect.provideService(Clock.Clock, recording.clock))
      yield* Deferred.await(firstAttempt)
      const retry = yield* recording.nextSleep((request) => request.millis === 7_000)
      assert.strictEqual(retry.deadline, 7_000)
      assert.strictEqual(yield* Ref.get(attempts), 1)

      yield* recording.advanceTo(retry.deadline - 1)
      assert.strictEqual(yield* Ref.get(attempts), 1)

      yield* recording.advanceTo(retry.deadline)
      assert.strictEqual(yield* Deferred.await(secondAttemptAt), retry.deadline)
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
        reader: Deferred.succeed(socketReady, undefined).pipe(Effect.as(queueReader(incoming))),
        writer: Effect.succeed(noopWriter)
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
      const firstReader = failingReader(failFirstSocket, Effect.fail(socketError))
      const socket = Socket.make({
        reader: Ref.updateAndGet(connections, (count) => count + 1).pipe(
          Effect.flatMap((connection) => {
            if (connection === 1) {
              return Deferred.succeed(firstSocketReady, undefined).pipe(Effect.as(firstReader))
            }
            return Deferred.succeed(secondSocketReady, undefined).pipe(Effect.as(queueReader(incoming)))
          })
        ),
        writer: Effect.succeed(noopWriter)
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
      const firstReader = failingReader(failFirstSocket, Effect.fail(socketError))
      const socket = Socket.make({
        reader: Ref.updateAndGet(connections, (count) => count + 1).pipe(
          Effect.flatMap((connection) => {
            if (connection === 1) {
              return Deferred.succeed(firstSocketReady, undefined).pipe(Effect.as(firstReader))
            }
            return Deferred.succeed(secondSocketReady, undefined).pipe(Effect.as(queueReader(incoming)))
          })
        ),
        writer: Effect.succeed(noopWriter)
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
      const incoming = yield* Queue.unbounded<Frame>()
      const socketReady = yield* Deferred.make<void>()
      const responseReceived = yield* Deferred.make<void>()
      const socket = Socket.make({
        reader: Deferred.succeed(socketReady, undefined).pipe(Effect.as(queueReader(incoming))),
        writer: Effect.succeed(noopWriter)
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
        (message) => Queue.offer(incoming, { message })
      )
      yield* Deferred.await(responseReceived)

      assert.deepStrictEqual(yield* Ref.get(responses), ["Exit"])
    })))

  it.effect("preserves an unknown socket failure cause", () =>
    Effect.scoped(Effect.gen(function*() {
      const socketReady = yield* Deferred.make<void>()
      const failSocket = yield* Deferred.make<void>()
      const responseReceived = yield* Deferred.make<void>()
      const defectReader = failingReader(failSocket, Effect.die("socket defect"))
      const socket = Socket.make({
        reader: Deferred.succeed(socketReady, undefined).pipe(Effect.as(defectReader)),
        writer: Effect.succeed(noopWriter)
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

  it.effect("reconnects and keeps serving requests after the server sends a frame that cannot be decoded", () =>
    Effect.scoped(Effect.gen(function*() {
      const incoming = yield* Queue.unbounded<Frame>()
      const pings = yield* Queue.unbounded<void>()
      const sawProtocolError = yield* Deferred.make<void>()
      const answered = yield* Deferred.make<void>()
      const socket = Socket.make({
        reader: Effect.succeed(queueReader(incoming)),
        writer: Effect.succeed({ write: () => Queue.offer(pings, undefined), writeAll: () => Effect.void })
      })
      const recording = yield* RecordingClock.make
      const context = yield* Layer.build(
        SyncClient.layerProtocolSocket({ retryPolicy: Schedule.spaced("1 second") }).pipe(
          Layer.provide(Layer.succeed(Socket.Socket, socket)),
          Layer.provide(RpcSerialization.layerJson)
        )
      ).pipe(Effect.provideService(Clock.Clock, recording.clock))
      const protocol = Context.get(context, RpcClient.Protocol)
      yield* protocol.run(1, (response) => {
        if (response._tag === "ClientProtocolError") return Deferred.succeed(sawProtocolError, undefined)
        if (response._tag === "Exit" && response.requestId === 2) return Deferred.succeed(answered, undefined)
        return Effect.void
      }).pipe(Effect.forkScoped({ startImmediately: true }))
      yield* Queue.take(pings)

      yield* Queue.offer(incoming, { message: "not a json frame" })
      yield* Deferred.await(sawProtocolError)
      const retry = yield* recording.nextSleep((request) => request.millis === 1_000)
      yield* recording.advanceTo(retry.deadline)
      yield* Queue.take(pings)

      yield* protocol.send(1, {
        _tag: "Request",
        id: 2,
        tag: "Test",
        payload: undefined,
        headers: []
      })
      yield* Queue.offer(incoming, {
        message: RpcSerialization.json.makeUnsafe().encode({
          _tag: "Exit",
          requestId: 2,
          exit: { _tag: "Success", value: null }
        })!
      })
      yield* Deferred.await(answered)
    })))
})
