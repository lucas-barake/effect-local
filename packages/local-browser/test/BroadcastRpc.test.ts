import { assert, describe, it } from "@effect/vitest"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as Rpc from "effect/unstable/rpc/Rpc"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import * as RpcGroup from "effect/unstable/rpc/RpcGroup"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as broadcastRpc from "../src/internal/broadcastRpc.js"
import * as Wire from "../src/internal/multiTabWire.js"
import * as testKit from "./multiTabKit.js"

class EchoError extends Schema.TaggedErrorClass<EchoError>(
  "test/EchoError"
)("EchoError", { message: Schema.String }) {}

class Echo extends Rpc.make("Echo", {
  payload: { text: Schema.String },
  success: Schema.String,
  error: Schema.Never
}) {}

class Boom extends Rpc.make("Boom", {
  payload: {},
  success: Schema.String,
  error: EchoError
}) {}

class Count extends Rpc.make("Count", {
  payload: { n: Schema.Int },
  success: Schema.Int,
  error: Schema.Never,
  stream: true
}) {}

class Hang extends Rpc.make("Hang", {
  payload: {},
  success: Schema.String,
  error: Schema.Never
}) {}

const TestRpcs = RpcGroup.make(Echo, Boom, Count, Hang)

const leaderTab = Wire.TabId.make("tab-leader")
const followerTab = Wire.TabId.make("tab-follower")
const channelName = "broadcast-rpc-test"

const clientTimings = {
  heartbeatInterval: 1000,
  pingInterval: 2000,
  pingTimeout: 4000
} as const

const serverTimings = {
  clientTimeout: 10_000,
  sweepInterval: 1000
} as const

const startServer = Effect.fnUntraced(function*(kit: testKit.MemoryPlatform, epoch: number) {
  const connection = yield* kit.tabChannel.open(channelName)
  const server = yield* broadcastRpc.makeServerProtocol({
    epoch: Wire.Epoch.make(epoch),
    leaderId: leaderTab,
    fingerprint: "fp-test",
    connection,
    ...serverTimings
  })
  const layerHandlers = TestRpcs.toLayer(Effect.succeed({
    Echo: (request: { readonly text: string }) => Effect.succeed(request.text),
    Boom: () => Effect.fail(new EchoError({ message: "boom" })),
    Count: (request: { readonly n: number }) =>
      Stream.fromIterable(Array.from({ length: request.n }, (_, index) => request.n - index)),
    Hang: () => Effect.never
  }))
  yield* Layer.build(
    RpcServer.layer(TestRpcs).pipe(
      Layer.provide(layerHandlers),
      Layer.provide(Layer.succeed(RpcServer.Protocol, server.protocol))
    )
  )
  return server
})

const startClient = Effect.fnUntraced(function*(
  kit: testKit.MemoryPlatform,
  options?: { readonly onLeaderSilent?: Effect.Effect<void> }
) {
  const connection = yield* kit.tabChannel.open(channelName)
  let silentOptions = {}
  if (options?.onLeaderSilent !== undefined) {
    silentOptions = { onLeaderSilent: options.onLeaderSilent }
  }
  const protocol = yield* broadcastRpc.makeClientProtocol({
    tabId: followerTab,
    fingerprint: "fp-test",
    connection,
    ...clientTimings,
    ...silentOptions
  })
  const client = yield* RpcClient.make(TestRpcs).pipe(
    Effect.provideService(RpcClient.Protocol, protocol)
  )
  return { client, connection }
})

describe("broadcastRpc", () => {
  it.effect(
    "sends one client hello for a ready leader epoch",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const observer = yield* kit.tabChannel.open(channelName)
      const hellos = yield* Queue.make<Wire.ClientHello>()
      yield* broadcastRpc.subscribeFrames(observer, {
        onFrame: (frame) => {
          if (frame._tag !== "ClientHello") return Effect.void
          return Queue.offer(hellos, frame)
        }
      })
      yield* startServer(kit, 1)
      const { client } = yield* startClient(kit)
      yield* client.Echo({ text: "ready" })
      yield* Queue.take(hellos)
      const extra = yield* Queue.take(hellos).pipe(
        Effect.timeoutOption(1000),
        Effect.forkChild({ startImmediately: true })
      )
      yield* TestClock.adjust(1000)
      assert.isTrue(Option.isNone(yield* Fiber.join(extra)))
    }, Effect.scoped)
  )

  it.effect(
    "does not adopt a ready frame below the highest elected epoch",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const connection = yield* kit.tabChannel.open(channelName)
      const sender = yield* kit.tabChannel.open(channelName)
      const hellos = yield* Queue.make<Wire.ClientHello>()
      yield* broadcastRpc.subscribeFrames(sender, {
        onFrame: (frame) => {
          if (frame._tag !== "ClientHello") return Effect.void
          return Queue.offer(hellos, frame)
        }
      })
      yield* startClient(kit)
      yield* broadcastRpc.postFrame(
        connection,
        Wire.Ready.make({ epoch: Wire.Epoch.make(1), leaderId: leaderTab, fingerprint: "fp-test" })
      )
      yield* Queue.take(hellos)
      yield* broadcastRpc.postFrame(
        connection,
        Wire.Elected.make({ epoch: Wire.Epoch.make(2), leaderId: Wire.TabId.make("tab-next") })
      )
      yield* broadcastRpc.postFrame(
        connection,
        Wire.Ready.make({ epoch: Wire.Epoch.make(1), leaderId: leaderTab, fingerprint: "fp-test" })
      )
      yield* broadcastRpc.postFrame(
        connection,
        Wire.Ready.make({
          epoch: Wire.Epoch.make(2),
          leaderId: Wire.TabId.make("tab-next"),
          fingerprint: "fp-test"
        })
      )
      const hello = yield* Queue.take(hellos)
      assert.strictEqual(hello.epoch, 2)
    }, Effect.scoped)
  )

  it.effect(
    "keeps receiving rpc responses while the leader-ready callback runs",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const observer = yield* kit.tabChannel.open(channelName)
      const responseSent = yield* Deferred.make<void>()
      yield* broadcastRpc.subscribeFrames(observer, {
        onFrame: (frame) => {
          if (frame._tag !== "RpcResponse") return Effect.void
          return Deferred.succeed(responseSent, undefined)
        }
      })
      let readyCall: Effect.Effect<void> = Effect.void
      const callbackDone = yield* Deferred.make<void>()
      const connection = yield* kit.tabChannel.open(channelName)
      const protocol = yield* broadcastRpc.makeClientProtocol({
        tabId: followerTab,
        fingerprint: "fp-test",
        connection,
        ...clientTimings,
        onLeaderReady: Effect.suspend(() =>
          readyCall.pipe(
            Effect.andThen(Deferred.succeed(callbackDone, undefined))
          )
        )
      })
      const client = yield* RpcClient.make(TestRpcs).pipe(
        Effect.provideService(RpcClient.Protocol, protocol)
      )
      readyCall = client.Echo({ text: "reregister" }).pipe(Effect.ignore)
      yield* startServer(kit, 1)
      yield* Deferred.await(responseSent)
      yield* Effect.yieldNow
      assert.isTrue(yield* Deferred.isDone(callbackDone))
    }, Effect.scoped)
  )

  it.effect(
    "request round-trips typed success",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      yield* startServer(kit, 1)
      const { client } = yield* startClient(kit)
      const result = yield* client.Echo({ text: "hello" })
      assert.strictEqual(result, "hello")
    }, Effect.scoped)
  )

  it.effect(
    "typed failure arrives in the error channel",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      yield* startServer(kit, 1)
      const { client } = yield* startClient(kit)
      const outcome = yield* client.Boom({}).pipe(
        Effect.catchTag("EchoError", (error) => Effect.succeed(`caught:${error.message}`))
      )
      assert.strictEqual(outcome, "caught:boom")
    }, Effect.scoped)
  )

  it.effect(
    "stream emits across the channel and interrupts cleanly",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      yield* startServer(kit, 1)
      const { client } = yield* startClient(kit)
      const all = yield* Stream.runCollect(client.Count({ n: 3 }))
      assert.deepStrictEqual(all, [3, 2, 1])
      const first = yield* Stream.runHead(client.Count({ n: 3 }))
      assert.isTrue(Option.isSome(first))
      assert.deepStrictEqual(Option.getOrUndefined(first), 3)
    }, Effect.scoped)
  )

  it.effect(
    "stale-epoch frames are dropped",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      yield* startServer(kit, 2)
      const { client, connection } = yield* startClient(kit)
      yield* broadcastRpc.postFrame(
        connection,
        Wire.RpcResponse.make({
          epoch: Wire.Epoch.make(1),
          to: followerTab,
          message: { _tag: "Exit", requestId: "1", exit: { _tag: "Success", value: "forged" } }
        })
      )
      const result = yield* client.Echo({ text: "real" })
      assert.strictEqual(result, "real")
    }, Effect.scoped)
  )

  it.effect(
    "leader change fails in-flight requests",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      yield* startServer(kit, 1)
      const { client } = yield* startClient(kit)
      const hangOutcome = Effect.exit(client.Hang({}))
      const inflight = yield* Effect.forkChild(hangOutcome)
      yield* client.Echo({ text: "sync" })
      const serverConnection = yield* kit.tabChannel.open(channelName)
      yield* broadcastRpc.postFrame(
        serverConnection,
        Wire.Elected.make({ epoch: Wire.Epoch.make(2), leaderId: Wire.TabId.make("tab-next") })
      )
      const exit = yield* Fiber.await(inflight)
      assert.isTrue(Exit.isSuccess(exit))
      if (Exit.isSuccess(exit)) {
        assert.isTrue(Exit.isFailure(exit.value))
      }
    }, Effect.scoped)
  )

  it.effect(
    "heartbeat loss surfaces a tab disconnect",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const server = yield* startServer(kit, 1)
      const ghostConnection = yield* kit.tabChannel.open(channelName)
      yield* broadcastRpc.postFrame(
        ghostConnection,
        Wire.ClientHello.make({ epoch: Wire.Epoch.make(1), tabId: Wire.TabId.make("tab-ghost") })
      )
      yield* TestClock.adjust(serverTimings.clientTimeout + serverTimings.sweepInterval + 1)
      const disconnected = yield* Queue.take(server.tabDisconnects)
      assert.strictEqual(disconnected, "tab-ghost")
    }, Effect.scoped)
  )

  it.effect(
    "leader silence triggers the silent callback",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const silent = yield* Deferred.make<void>()
      const serverScope = yield* Scope.make()
      yield* Scope.provide(startServer(kit, 1), serverScope)
      const { client } = yield* startClient(kit, { onLeaderSilent: Deferred.succeed(silent, undefined) })
      yield* client.Echo({ text: "warm" })
      yield* Scope.close(serverScope, Exit.void)
      yield* TestClock.adjust(clientTimings.pingInterval + clientTimings.pingTimeout + clientTimings.pingInterval)
      yield* Deferred.await(silent)
    }, Effect.scoped)
  )
})
