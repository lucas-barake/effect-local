import { NodeSocket, NodeSocketServer } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as SocketServer from "effect/unstable/socket/SocketServer"
import * as RelayConnectionStatus from "../src/RelayConnectionStatus.js"

/**
 * `RpcClient.ConnectionHooks` has no upstream tests and lives under `unstable`, so this file is what
 * tells us if a future Effect release changes or drops it. Every assertion is driven by a real TCP
 * socket and rendezvouses on a `Queue`. The retry backoff is the only time-based part and it sleeps
 * on the Clock, so nothing here waits on wall clock.
 *
 * One contract is deliberately not covered here: that `attempts` restarts at one after the link
 * comes back. Proving it needs a connect that fails and then succeeds on the same port, which means
 * releasing a port and rebinding it, and that race made the assertion fail roughly one run in five.
 * A flaky guard is worse than a stated gap, so it is stated instead.
 */
const tcpPort = (address: SocketServer.Address) => {
  assert.strictEqual(address._tag, "TcpAddress")
  return (address as SocketServer.TcpAddress).port
}

/**
 * Binding is what makes the connect complete, so `onConnect` fires with or without `run`. The accept
 * loop is here so the connection is actually serviced rather than left in the backlog, which is what
 * a real relay does.
 */
const acceptOn = (port: number) =>
  Effect.gen(function*() {
    const scope = yield* Scope.make()
    const listener = yield* Layer.buildWithScope(NodeSocketServer.layer({ port }), scope)
    const server = Context.get(listener, SocketServer.SocketServer)
    yield* server.run(() => Effect.never).pipe(Effect.forkChild)
    return { port: tcpPort(server.address), close: Scope.close(scope, Exit.void) }
  })

/** Borrows a port from a listener that is then released, so a connect to it genuinely fails. */
const closedPort = Effect.gen(function*() {
  const listener = yield* acceptOn(0)
  yield* listener.close
  return listener.port
})

const observe = (port: number, scope: Scope.Scope) =>
  Effect.gen(function*() {
    const context = yield* Scope.provide(
      Layer.build(
        RelayConnectionStatus.layerProtocolSocket().pipe(
          Layer.provide(NodeSocket.layerNet({ port })),
          Layer.provide(RpcSerialization.layerJson)
        )
      ),
      scope
    )
    const seen = yield* Queue.unbounded<RelayConnectionStatus.Status>()
    yield* Effect.addFinalizer(() => Queue.shutdown(seen))
    const reader = Context.get(context, RelayConnectionStatus.RelayConnectionStatus)
    const fiber = yield* Stream.runForEach(reader.status, (status) => Queue.offer(seen, status))
      .pipe(Effect.forkChild)
    return { fiber, seen }
  })

describe("RelayConnectionStatus", () => {
  it.effect("reports Connected once the socket is actually open", () =>
    Effect.gen(function*() {
      const server = yield* acceptOn(0)
      const scope = yield* Scope.make()
      const { seen } = yield* observe(server.port, scope)

      // Seeded before anything can subscribe, so a page that mounts the indicator before the socket
      // resolves renders a state instead of nothing.
      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.disconnected)
      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.connected)

      yield* Scope.close(scope, Exit.void)
      yield* server.close
    }).pipe(Effect.scoped))

  it.effect("ends the stream with a terminal Disconnected when the Layer scope closes", () =>
    Effect.gen(function*() {
      const server = yield* acceptOn(0)
      const scope = yield* Scope.make()
      const { fiber, seen } = yield* observe(server.port, scope)

      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.disconnected)
      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.connected)

      yield* Scope.close(scope, Exit.void)

      // A subscriber live at teardown has to be told the link is gone and then released, and it must
      // not first be told the client is reconnecting. The socket fiber's own disconnect hook fires
      // during close, so the owner is retired ahead of it; without that this reads
      // `Connected -> Connecting(1) -> Disconnected`.
      yield* Fiber.join(fiber)
      const tail: Array<RelayConnectionStatus.Status> = []
      while (true) {
        const next = yield* Queue.poll(seen)
        if (next._tag === "None") break
        tail.push(next.value)
      }
      assert.deepStrictEqual(tail, [RelayConnectionStatus.disconnected])

      yield* server.close
    }).pipe(Effect.scoped))

  it.effect("counts attempts while the relay is unreachable, and never claims Disconnected", () =>
    Effect.gen(function*() {
      const port = yield* closedPort
      const scope = yield* Scope.make()
      const { seen } = yield* observe(port, scope)

      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.disconnected)
      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.connecting(1))

      yield* TestClock.adjust("500 millis")
      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.connecting(2))

      yield* TestClock.adjust("750 millis")
      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.connecting(3))

      yield* Scope.close(scope, Exit.void)
    }).pipe(Effect.scoped))

  it.effect("reports NotConfigured on a replica with no relay, and keeps the stream open", () =>
    Effect.gen(function*() {
      const context = yield* Layer.build(RelayConnectionStatus.layerNotConfigured)
      const reader = Context.get(context, RelayConnectionStatus.RelayConnectionStatus)
      const seen = yield* Queue.unbounded<RelayConnectionStatus.Status>()
      yield* Effect.addFinalizer(() => Queue.shutdown(seen))
      const fiber = yield* Stream.runForEach(reader.status, (status) => Queue.offer(seen, status))
        .pipe(Effect.forkChild)

      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.notConfigured)

      // Reporting Disconnected here would claim a relay exists and is down. Ending the stream would
      // be worse: an observer cannot tell that from a stream that is open with nothing to say.
      yield* TestClock.adjust("1 minute")
      assert.isUndefined(fiber.pollUnsafe())
    }).pipe(Effect.scoped))
})
