import { NodeSocket, NodeSocketServer } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import { Context, Deferred, Effect, Exit, Fiber, Layer, Scope } from "effect"
import { TestClock } from "effect/testing"
import type * as RpcMessage from "effect/unstable/rpc/RpcMessage"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import { Socket } from "effect/unstable/socket"
import type { SocketServer } from "effect/unstable/socket"
import * as PeerRelayIngress from "../src/PeerRelayIngress.ts"
import * as PeerRelayLimits from "../src/PeerRelayLimits.ts"

const frame = (value: unknown) => {
  const body = new TextEncoder().encode(JSON.stringify(value))
  const result = new Uint8Array(body.byteLength + 4)
  new DataView(result.buffer).setUint32(0, body.byteLength, false)
  result.set(body, 4)
  return result
}

const request = (id: number, padding = ""): RpcMessage.RequestEncoded => ({
  _tag: "Request",
  id,
  tag: "Test",
  payload: { padding },
  headers: []
})

const testLimits = (
  overrides: Partial<PeerRelayLimits.Values> = {}
): PeerRelayLimits.Values => ({
  ...PeerRelayLimits.defaults,
  maxRelayConnections: 4,
  maximumRawChunkBytes: 512,
  maximumDeclaredFrameBytes: 4 * 1_024 * 1_024,
  maximumIncompleteFrameBytes: 4 * 1_024 * 1_024 + 4,
  incompleteFrameTimeoutMillis: 1_000,
  maximumByteReservationWaiters: 4,
  maxSessionsPerSubject: 4,
  maxInFlightOpen: 4,
  ...overrides
})

const buildServer = Effect.fnUntraced(function*(
  values: PeerRelayLimits.Values,
  port = 0
) {
  const scope = yield* Scope.make("sequential")
  const context = yield* Layer.buildWithScope(
    PeerRelayIngress.layerProtocolSocketServer(NodeSocketServer.layer({ port })).pipe(
      Layer.provide(PeerRelayLimits.layer(values))
    ),
    scope
  )
  const ingress = Context.get(context, PeerRelayIngress.PeerRelayIngress)
  const protocol = Context.get(context, RpcServer.Protocol)
  return { ingress, protocol, scope }
})

const connect = Effect.fnUntraced(function*(
  port: number,
  scope: Scope.Closeable,
  read = true
) {
  const context = yield* Layer.buildWithScope(NodeSocket.layerNet({ port }), scope)
  const socket = Context.get(context, Socket.Socket)
  const readFiber = read
    ? yield* Effect.forkIn(socket.runRaw(() => Effect.void), scope)
    : undefined
  const write = yield* Scope.provide(socket.writer, scope)
  return { readFiber, socket, write }
})

const tcpPort = (address: SocketServer.Address) => {
  assert.strictEqual(address._tag, "TcpAddress")
  return (address as SocketServer.TcpAddress).port
}

const awaitConnections = (ingress: PeerRelayIngress.PeerRelayIngress["Service"], expected: number) =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 10_000; attempt++) {
      const usage = yield* ingress.usage
      if (usage.connections === expected) return usage
      yield* Effect.yieldNow
    }
    return yield* Effect.die(new Error("Connection count did not converge"))
  })

const awaitReservedBytes = (ingress: PeerRelayIngress.PeerRelayIngress["Service"]) =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 10_000; attempt++) {
      const usage = yield* ingress.usage
      if (usage.reservedBytes > 0) return usage
      yield* Effect.yieldNow
    }
    return yield* Effect.die(new Error("Byte reservation did not become observable"))
  })

const awaitByteReservationWaiters = (
  ingress: PeerRelayIngress.PeerRelayIngress["Service"],
  expected: number
) =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 10_000; attempt++) {
      const usage = yield* ingress.usage
      if (usage.byteReservationWaiters === expected) return usage
      yield* Effect.yieldNow
    }
    return yield* Effect.die(new Error("Byte reservation waiter count did not converge"))
  })

describe("PeerRelayIngress", () => {
  it.effect("rejects an oversized declared frame before dispatch", () =>
    Effect.scoped(Effect.gen(function*() {
      const received = Deferred.makeUnsafe<void>()
      const values = testLimits()
      const server = yield* buildServer(values)
      yield* Effect.forkIn(
        server.protocol.run(() => Deferred.succeed(received, undefined)),
        server.scope
      )
      const clientScope = yield* Scope.make()
      const client = yield* connect(tcpPort(server.ingress.address), clientScope)

      const header = new Uint8Array(4)
      new DataView(header.buffer).setUint32(0, values.maximumDeclaredFrameBytes + 1, false)
      yield* client.write(header)
      const clientExit = yield* Fiber.await(client.readFiber!)

      assert.match(clientExit._tag, /^(Failure|Success)$/)
      assert.strictEqual(Deferred.isDoneUnsafe(received), false)
      assert.deepStrictEqual(yield* server.ingress.usage, {
        connections: 0,
        reservedBytes: 0,
        byteReservationWaiters: 0
      })
      yield* Scope.close(clientScope, Exit.void)
      yield* Scope.close(server.scope, Exit.void)
    })))

  it.effect("rejects an oversized raw chunk before JSON decode", () =>
    Effect.scoped(Effect.gen(function*() {
      const received = Deferred.makeUnsafe<void>()
      const server = yield* buildServer(testLimits({
        maximumRawChunkBytes: 8
      }))
      yield* Effect.forkIn(
        server.protocol.run(() => Deferred.succeed(received, undefined)),
        server.scope
      )
      const clientScope = yield* Scope.make()
      const client = yield* connect(tcpPort(server.ingress.address), clientScope)

      yield* client.write(frame(request(1)))
      const clientExit = yield* Fiber.await(client.readFiber!)

      assert.match(clientExit._tag, /^(Failure|Success)$/)
      assert.strictEqual(Deferred.isDoneUnsafe(received), false)
      assert.strictEqual((yield* server.ingress.usage).reservedBytes, 0)
      yield* Scope.close(clientScope, Exit.void)
      yield* Scope.close(server.scope, Exit.void)
    })))

  it.effect("accepts a frame fragmented across every header and body byte", () =>
    Effect.scoped(Effect.gen(function*() {
      const received = Deferred.makeUnsafe<readonly [number, RpcMessage.FromClientEncoded]>()
      const server = yield* buildServer(testLimits())
      yield* Effect.forkIn(
        server.protocol.run((clientId, message) => Deferred.succeed(received, [clientId, message] as const)),
        server.scope
      )
      const clientScope = yield* Scope.make()
      const client = yield* connect(tcpPort(server.ingress.address), clientScope)
      const encoded = frame(request(7, "fragmented"))

      for (const byte of encoded) {
        yield* client.write(Uint8Array.of(byte))
      }
      const [clientId, message] = yield* Deferred.await(received)

      assert.strictEqual(clientId, 0)
      assert.strictEqual(message._tag, "Request")
      assert.strictEqual(
        message._tag === "Request" ? message.id : undefined,
        7
      )
      yield* Scope.close(clientScope, Exit.void)
      yield* Scope.close(server.scope, Exit.void)
      assert.strictEqual((yield* server.ingress.usage).reservedBytes, 0)
    })))

  it.effect("times out a slow incomplete frame and releases its connection", () =>
    Effect.scoped(Effect.gen(function*() {
      const server = yield* buildServer(testLimits({ incompleteFrameTimeoutMillis: 500 }))
      yield* Effect.forkIn(server.protocol.run(() => Effect.void), server.scope)
      const clientScope = yield* Scope.make()
      const client = yield* connect(tcpPort(server.ingress.address), clientScope)

      const partial = new Uint8Array(5)
      new DataView(partial.buffer).setUint32(0, 10, false)
      partial[4] = 123
      yield* client.write(partial)
      yield* awaitConnections(server.ingress, 1)
      yield* awaitReservedBytes(server.ingress)
      yield* TestClock.adjust(500)
      const clientExit = yield* Fiber.await(client.readFiber!)

      assert.match(clientExit._tag, /^(Failure|Success)$/)
      assert.deepStrictEqual(yield* server.ingress.usage, {
        connections: 0,
        reservedBytes: 0,
        byteReservationWaiters: 0
      })
      yield* Scope.close(clientScope, Exit.void)
      yield* Scope.close(server.scope, Exit.void)
    })))

  it.effect("decodes many maximum sized fragmented frames without exceeding the shared budget", () =>
    Effect.scoped(Effect.gen(function*() {
      const count = 8
      const maximumFrameBytes = 4 * 1_024 * 1_024
      const received = yield* Deferred.make<number>()
      let seen = 0
      const server = yield* buildServer(testLimits({
        maximumDeclaredFrameBytes: maximumFrameBytes,
        maximumRawChunkBytes: maximumFrameBytes + 4,
        maximumIncompleteFrameBytes: maximumFrameBytes + 4
      }))
      yield* Effect.forkIn(
        server.protocol.run(() =>
          Effect.sync(() => {
            seen++
            if (seen === count) Deferred.doneUnsafe(received, Effect.succeed(seen))
          })
        ),
        server.scope
      )
      const clientScope = yield* Scope.make()
      const client = yield* connect(tcpPort(server.ingress.address), clientScope)

      for (let id = 0; id < count; id++) {
        const empty = frame(request(id))
        const encoded = frame(request(id, "x".repeat(maximumFrameBytes + 4 - empty.byteLength)))
        assert.strictEqual(encoded.byteLength, maximumFrameBytes + 4)
        yield* client.write(encoded.subarray(0, 4))
        yield* client.write(encoded.subarray(4))
      }
      assert.strictEqual(yield* Deferred.await(received), count)
      const usage = yield* server.ingress.usage
      assert.isAtMost(usage.reservedBytes, PeerRelayLimits.defaults.maximumSharedPayloadBytes)
      assert.strictEqual(usage.byteReservationWaiters, 0)

      yield* Scope.close(clientScope, Exit.void)
      yield* Scope.close(server.scope, Exit.void)
      assert.strictEqual((yield* server.ingress.usage).reservedBytes, 0)
    })))

  it.effect("caps connections and closes saturated clients", () =>
    Effect.scoped(Effect.gen(function*() {
      const values = testLimits({
        maxRelayConnections: 1,
        maximumByteReservationWaiters: 1,
        maxSessionsPerSubject: 1,
        maxInFlightOpen: 1,
        maxInFlightOpenPerSubject: 1
      })
      const server = yield* buildServer(values)
      yield* Effect.forkIn(server.protocol.run(() => Effect.void), server.scope)
      const firstScope = yield* Scope.make()
      const first = yield* connect(tcpPort(server.ingress.address), firstScope)
      yield* first.write(Uint8Array.of(0))
      yield* awaitConnections(server.ingress, 1)

      const secondScope = yield* Scope.make()
      const second = yield* connect(tcpPort(server.ingress.address), secondScope)
      const secondExit = yield* Fiber.await(second.readFiber!)

      assert.match(secondExit._tag, /^(Failure|Success)$/)
      assert.strictEqual((yield* server.ingress.usage).connections, 1)
      yield* Scope.close(secondScope, Exit.void)
      yield* Scope.close(firstScope, Exit.void)
      yield* Scope.close(server.scope, Exit.void)
    })))

  it.effect("bounds stalled outbound reservations and releases every waiter on cleanup", () =>
    Effect.scoped(Effect.gen(function*() {
      const server = yield* buildServer(testLimits())
      const reservation = yield* server.ingress.reserveOutbound(
        PeerRelayLimits.defaults.maximumSharedPayloadBytes
      )
      const waiting = yield* Effect.forkChild(
        server.ingress.reserveOutbound(1)
      )
      yield* Effect.yieldNow

      assert.deepStrictEqual(yield* server.ingress.usage, {
        connections: 0,
        reservedBytes: PeerRelayLimits.defaults.maximumSharedPayloadBytes,
        byteReservationWaiters: 1
      })
      yield* Fiber.interrupt(waiting)
      yield* reservation.release
      assert.deepStrictEqual(yield* server.ingress.usage, {
        connections: 0,
        reservedBytes: 0,
        byteReservationWaiters: 0
      })
      yield* Scope.close(server.scope, Exit.void)
    })))

  it.effect("reserves outbound capacity before serializing a response", () =>
    Effect.scoped(Effect.gen(function*() {
      const values = testLimits()
      const server = yield* buildServer(values)
      yield* Effect.forkIn(server.protocol.run(() => Effect.void), server.scope)
      const clientScope = yield* Scope.make()
      yield* connect(tcpPort(server.ingress.address), clientScope)
      yield* awaitConnections(server.ingress, 1)
      const [clientId] = yield* server.protocol.clientIds
      assert.isDefined(clientId)
      const blocker = yield* server.ingress.reserveOutbound(
        values.maximumSharedPayloadBytes
      )
      const serialized = Deferred.makeUnsafe<void>()
      const response: RpcMessage.FromServerEncoded = {
        _tag: "Defect",
        defect: {
          toJSON() {
            Deferred.doneUnsafe(serialized, Effect.void)
            return "serialized"
          }
        }
      }

      const send = yield* Effect.forkChild(server.protocol.send(clientId!, response))
      yield* awaitByteReservationWaiters(server.ingress, 1)

      assert.strictEqual(Deferred.isDoneUnsafe(serialized), false)
      yield* Fiber.interrupt(send)
      yield* blocker.release
      assert.deepStrictEqual(yield* server.ingress.usage, {
        connections: 1,
        reservedBytes: 0,
        byteReservationWaiters: 0
      })
      yield* Scope.close(clientScope, Exit.void)
      yield* Scope.close(server.scope, Exit.void)
    })))

  it.effect("rejects overlapping raw chunks instead of accumulating parser fibers", () =>
    Effect.scoped(Effect.gen(function*() {
      const values = testLimits()
      const server = yield* buildServer(values)
      yield* Effect.forkIn(server.protocol.run(() => Effect.void), server.scope)
      const blocker = yield* server.ingress.reserveOutbound(
        values.maximumSharedPayloadBytes
      )
      const clientScope = yield* Scope.make()
      const client = yield* connect(tcpPort(server.ingress.address), clientScope)
      const header = new Uint8Array(4)
      new DataView(header.buffer).setUint32(0, 10, false)

      yield* client.write(header)
      yield* awaitByteReservationWaiters(server.ingress, 1)
      yield* client.write(Uint8Array.of(123)).pipe(Effect.ignore)
      yield* Fiber.await(client.readFiber!)
      yield* awaitConnections(server.ingress, 0)

      assert.deepStrictEqual(yield* server.ingress.usage, {
        connections: 0,
        reservedBytes: values.maximumSharedPayloadBytes,
        byteReservationWaiters: 0
      })
      yield* blocker.release
      assert.deepStrictEqual(yield* server.ingress.usage, {
        connections: 0,
        reservedBytes: 0,
        byteReservationWaiters: 0
      })
      yield* Scope.close(clientScope, Exit.void)
      yield* Scope.close(server.scope, Exit.void)
    })))

  it.effect("releases a transferred reservation when an outbound write wait is interrupted", () =>
    Effect.scoped(Effect.gen(function*() {
      const values = testLimits()
      const server = yield* buildServer(values)
      const ready = Deferred.makeUnsafe<{
        readonly blocker: PeerRelayIngress.Reservation
        readonly clientId: number
      }>()
      const input = request(1)
      const inboundBytes = frame(input).byteLength - 4
      yield* Effect.forkIn(
        server.protocol.run((clientId) =>
          Effect.gen(function*() {
            const transferred = yield* server.ingress.reserveOutbound(1)
            yield* transferred.transferToCurrentRequest
            const blocker = yield* server.ingress.reserveOutbound(
              values.maximumSharedPayloadBytes - inboundBytes - 1
            )
            yield* Deferred.succeed(ready, { blocker, clientId })
          })
        ),
        server.scope
      )
      const clientScope = yield* Scope.make()
      const client = yield* connect(tcpPort(server.ingress.address), clientScope)
      yield* client.write(frame(input))
      const { blocker, clientId } = yield* Deferred.await(ready)

      const send = yield* Effect.forkChild(
        server.protocol.send(clientId, {
          _tag: "Chunk",
          requestId: 1,
          values: ["requires-more-than-one-byte"]
        })
      )
      yield* Effect.yieldNow
      assert.strictEqual(
        (yield* server.ingress.usage).reservedBytes,
        values.maximumSharedPayloadBytes
      )
      yield* Fiber.interrupt(send)
      yield* blocker.release
      yield* Scope.close(clientScope, Exit.void)
      yield* awaitConnections(server.ingress, 0)
      assert.deepStrictEqual(yield* server.ingress.usage, {
        connections: 0,
        reservedBytes: 0,
        byteReservationWaiters: 0
      })
      yield* Scope.close(server.scope, Exit.void)
    })))

  it.effect("rejects reservation transfer after client disconnect cleanup", () =>
    Effect.scoped(Effect.gen(function*() {
      const server = yield* buildServer(testLimits())
      const ready = Deferred.makeUnsafe<number>()
      const transfer = Deferred.makeUnsafe<void>()
      const transferSucceeded = Deferred.makeUnsafe<boolean>()
      yield* Effect.forkIn(
        server.protocol.run((clientId) =>
          Effect.gen(function*() {
            const reservation = yield* server.ingress.reserveOutbound(1)
            yield* Deferred.succeed(ready, clientId)
            yield* Effect.uninterruptible(
              Effect.gen(function*() {
                yield* Deferred.await(transfer)
                const succeeded = yield* reservation.transferToCurrentRequest.pipe(
                  Effect.match({
                    onFailure: () => false,
                    onSuccess: () => true
                  })
                )
                if (!succeeded) yield* reservation.release
                yield* Deferred.succeed(transferSucceeded, succeeded)
              })
            )
          })
        ),
        server.scope
      )
      const clientScope = yield* Scope.make()
      const client = yield* connect(tcpPort(server.ingress.address), clientScope)
      yield* client.write(frame(request(1)))
      const clientId = yield* Deferred.await(ready)

      yield* server.protocol.end(clientId)
      yield* Deferred.succeed(transfer, undefined)
      assert.strictEqual(yield* Deferred.await(transferSucceeded), false)
      yield* Fiber.await(client.readFiber!)
      yield* awaitConnections(server.ingress, 0)

      assert.deepStrictEqual(yield* server.ingress.usage, {
        connections: 0,
        reservedBytes: 0,
        byteReservationWaiters: 0
      })
      yield* Scope.close(clientScope, Exit.void)
      yield* Scope.close(server.scope, Exit.void)
    })))

  it.effect("rejects pre-run connection churn without filling the disconnect queue", () =>
    Effect.scoped(Effect.gen(function*() {
      const values = testLimits({
        maxRelayConnections: 1,
        maximumByteReservationWaiters: 1,
        maxSessionsPerSubject: 1,
        maxInFlightOpen: 1,
        maxInFlightOpenPerSubject: 1
      })
      const server = yield* buildServer(values)
      for (let attempt = 0; attempt < 8; attempt++) {
        const rejectedScope = yield* Scope.make()
        const rejected = yield* connect(tcpPort(server.ingress.address), rejectedScope)
        yield* Fiber.await(rejected.readFiber!)
        yield* Scope.close(rejectedScope, Exit.void)
      }
      assert.strictEqual((yield* server.ingress.usage).connections, 0)

      yield* Effect.forkIn(server.protocol.run(() => Effect.void), server.scope)
      const acceptedScope = yield* Scope.make()
      const accepted = yield* connect(tcpPort(server.ingress.address), acceptedScope)
      yield* accepted.write(Uint8Array.of(0))
      yield* awaitConnections(server.ingress, 1)
      yield* Scope.close(acceptedScope, Exit.void)
      yield* Scope.close(server.scope, Exit.void)
    })))

  it.effect("reports normal client EOF and fails later sends promptly", () =>
    Effect.scoped(Effect.gen(function*() {
      const values = testLimits()
      const server = yield* buildServer(values)
      yield* Effect.forkIn(server.protocol.run(() => Effect.void), server.scope)
      const clientScope = yield* Scope.make()
      const socketContext = yield* Layer.buildWithScope(
        NodeSocket.layerNet({ port: tcpPort(server.ingress.address) }),
        clientScope
      )
      const socket = Context.get(socketContext, Socket.Socket)
      const clientProtocol = yield* Scope.provide(
        PeerRelayIngress.makeProtocolSocket.pipe(
          Effect.provideService(Socket.Socket, socket),
          Effect.provideService(PeerRelayLimits.PeerRelayLimits, values)
        ),
        clientScope
      )
      const protocolFailure = Deferred.makeUnsafe<RpcMessage.FromServerEncoded>()
      yield* Effect.forkIn(
        clientProtocol.run(0, (message) =>
          message._tag === "ClientProtocolError"
            ? Deferred.succeed(protocolFailure, message)
            : Effect.void),
        clientScope
      )
      yield* awaitConnections(server.ingress, 1)
      yield* Scope.close(server.scope, Exit.void)

      const failure = yield* Deferred.await(protocolFailure)
      assert.strictEqual(failure._tag, "ClientProtocolError")
      const sendExit = yield* Effect.exit(
        clientProtocol.send(0, { _tag: "Ping" })
      )
      assert.strictEqual(sendExit._tag, "Failure")
      yield* Scope.close(clientScope, Exit.void)
    })))

  it.effect("closes a partially built socket layer and can bind the same address again", () =>
    Effect.scoped(Effect.gen(function*() {
      const probeScope = yield* Scope.make()
      const probe = yield* Scope.provide(NodeSocketServer.make({ port: 0 }), probeScope)
      const port = tcpPort(probe.address)
      yield* Scope.close(probeScope, Exit.void)

      const partial = Layer.effectContext(
        Effect.gen(function*() {
          yield* NodeSocketServer.make({ port })
          return yield* Effect.fail("partial-build")
        })
      ) as Layer.Layer<SocketServer.SocketServer, string>
      const failedScope = yield* Scope.make()
      const failed = yield* Effect.exit(
        Layer.buildWithScope(
          PeerRelayIngress.layerProtocolSocketServer(partial).pipe(
            Layer.provide(PeerRelayLimits.layer(testLimits()))
          ),
          failedScope
        )
      )
      assert.strictEqual(failed._tag, "Failure")
      yield* Scope.close(failedScope, failed)

      const restarted = yield* buildServer(testLimits(), port)
      assert.strictEqual(tcpPort(restarted.ingress.address), port)
      yield* Scope.close(restarted.scope, Exit.void)
    })))

  it.effect("closes the child listener scope and restarts on the retained port", () =>
    Effect.scoped(Effect.gen(function*() {
      const first = yield* buildServer(testLimits())
      const port = tcpPort(first.ingress.address)
      yield* Scope.close(first.scope, Exit.void)
      const firstExit = yield* Effect.exit(first.ingress.await)
      assert.strictEqual(firstExit._tag, "Failure")

      const second = yield* buildServer(testLimits(), port)
      assert.strictEqual(tcpPort(second.ingress.address), port)
      yield* Scope.close(second.scope, Exit.void)
    })))
})
