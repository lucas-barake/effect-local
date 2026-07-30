import { NodeSocket, NodeSocketServer } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as SocketServer from "effect/unstable/socket/SocketServer"

const tcpPort = (address: SocketServer.Address) => {
  assert.strictEqual(address._tag, "TcpAddress")
  return (address as SocketServer.TcpAddress).port
}

/**
 * `RpcClient.ConnectionHooks` has no upstream tests and lives under `unstable`, so this is what
 * tells us if a future Effect release changes or drops it.
 *
 * Only the disconnect half is asserted here, because it is the half that can be driven
 * deterministically: a refused connect is immediate and the retry backoff runs on the Clock. The
 * connect half needs a socket that genuinely opens, and it is asserted end to end against a real
 * relay in `test-browser/relay/tests/relay.spec.ts`, which is closer to how a consumer meets it.
 */
/** Binds a listener only to borrow a port that is then released, so the connect genuinely fails. */
const withClosedPort = <A, E, R,>(use: (port: number) => Effect.Effect<A, E, R>) =>
  Effect.gen(function*() {
    const scope = yield* Scope.make()
    const listener = yield* Layer.buildWithScope(NodeSocketServer.layer({ port: 0 }), scope)
    const port = tcpPort(Context.get(listener, SocketServer.SocketServer).address)
    yield* Scope.close(scope, Exit.void)
    return yield* use(port)
  })

const observe = (port: number) =>
  Effect.gen(function*() {
    const context = yield* Layer.build(
      RelayConnectionStatus.layerProtocolSocket().pipe(
        Layer.provide(NodeSocket.layerNet({ port })),
        Layer.provide(RpcSerialization.layerJson)
      )
    )
    const seen = yield* Queue.unbounded<RelayConnectionStatus.Status>()
    yield* Effect.addFinalizer(() => Queue.shutdown(seen))
    const reader = Context.get(context, RelayConnectionStatus.RelayConnectionStatus)
    yield* Stream.runForEach(reader.status, (status) => Queue.offer(seen, status)).pipe(Effect.forkChild)
    return seen
  })

describe("RelayConnectionStatus", () => {
  it.effect("counts attempts while the relay is unreachable, and never claims Disconnected", () =>
    Effect.scoped(Effect.gen(function*() {
      const port = yield* withClosedPort((port) => Effect.succeed(port))
      const seen = yield* observe(port)

      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.disconnected)
      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.connecting(1))

      // The retry backoff sleeps on the Clock, so virtual time drives the next attempt. Nothing
      // here waits on wall clock.
      yield* TestClock.adjust("500 millis")
      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.connecting(2))

      yield* TestClock.adjust("750 millis")
      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.connecting(3))
    })))

  it.effect("reports NotConfigured on a replica with no relay, and keeps the stream open", () =>
    Effect.gen(function*() {
      const context = yield* Effect.scoped(
        Layer.build(RelayConnectionStatus.layerNotConfigured).pipe(Effect.map((built) => built))
      )
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
