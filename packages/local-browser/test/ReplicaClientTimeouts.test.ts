import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import { RpcTest } from "effect/unstable/rpc"
import * as ReplicaClient from "../src/ReplicaClient.js"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import { definition, documentId, replica, Task } from "./fixtures.js"

it.layer(NodeCrypto.layer)("ReplicaClient timeouts", (it) => {
  const limits = {
    maxBackupBytes: 1024,
    maxChunkBytes: 128,
    maxArchiveRecords: 100,
    maxJsonDepth: 16,
    maxSyncMessageBytes: 1024,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 10,
    maxSyncDependencyEdgesPerMessage: 20,
    maxSyncOperationsPerMessage: 100,
    maxPendingBytesPerDocument: 1024,
    maxPendingBytesPerPeer: 2048,
    maxPendingBytesPerReplica: 4096,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 10,
    maxPendingChangesPerPeer: 20,
    maxPendingChangesPerReplica: 40,
    maxPendingDependencyEdgesPerDocument: 100,
    maxPendingDependencyEdgesPerPeer: 200,
    maxPendingDependencyEdgesPerReplica: 400,
    maxSessions: 2,
    maxStreamsPerSession: 2,
    maxInFlightPerSession: 2,
    maxQueuedRpc: 4
  } satisfies ReplicaLimits.Values
  const Sessions = SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(limits)))
  const Publisher = Layer.succeed(
    CommitPublisher.CommitPublisher,
    CommitPublisher.CommitPublisher.of({
      publishPending: Effect.succeed(0),
      invalidate: () => Effect.void,
      subscribe: Effect.succeed({
        watermark: Identity.CommitSequence.make(0),
        refreshGeneration: 0,
        events: Stream.never
      })
    })
  )
  const Owner = ReplicaOwner.layerHandlers(definition).pipe(
    Layer.provideMerge(Sessions),
    Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, replica)))
  )

  const timeouts = { sessionTimeout: 1_000, operationTimeout: 2_000 }

  it.effect("fails a never-responding session acquire with a typed OperationTimeout", () =>
    Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "OpenSession") return () => Effect.never
          return Reflect.get(target, property, receiver)
        }
      })
      const fiber = yield* Effect.scoped(ReplicaClient.fromRpcClient(definition, wedged, timeouts))
        .pipe(Effect.forkChild)
      yield* TestClock.adjust(timeouts.sessionTimeout)
      const exit = fiber.pollUnsafe()
      assert.isDefined(exit)
      assert.isTrue(Exit.isFailure(exit!))
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.operation, "OpenSession")
        assert.strictEqual(error.reason.timeoutMillis, 1_000)
      }
    }).pipe(Effect.provide(Owner)))

  it.effect("fails a never-responding per-operation RPC with a typed OperationTimeout", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "Get") return () => Effect.never
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, wedged, timeouts)
      const fiber = yield* client.get(Task, documentId).pipe(Effect.forkChild)
      yield* TestClock.adjust(timeouts.operationTimeout)
      const exit = fiber.pollUnsafe()
      assert.isDefined(exit)
      assert.isTrue(Exit.isFailure(exit!))
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.operation, "Get")
        assert.strictEqual(error.reason.timeoutMillis, 2_000)
      }
    })).pipe(Effect.provide(Owner)))

  it.effect("bounds a reopen whose fresh session acquire never responds", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let opens = 0
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "OpenSession") {
            return (payload: never) => {
              opens += 1
              return opens === 1 ? Reflect.get(target, property, receiver)(payload) : Effect.never
            }
          }
          if (property === "Flush") {
            return () =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({ expected: "active session", observed: "fenced" })
                })
              )
          }
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, wedged, timeouts)
      const fiber = yield* client.flush.pipe(Effect.forkChild)
      yield* TestClock.adjust(timeouts.sessionTimeout)
      const exit = fiber.pollUnsafe()
      assert.isDefined(exit)
      assert.isTrue(Exit.isFailure(exit!))
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "OperationTimeout")
      if (error.reason._tag === "OperationTimeout") {
        assert.strictEqual(error.reason.operation, "OpenSession")
      }
    })).pipe(Effect.provide(Owner)))

  it.effect("bounds teardown when CloseSession never responds", () =>
    Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const wedged = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "CloseSession") return () => Effect.never
          return Reflect.get(target, property, receiver)
        }
      })
      const ready = yield* Deferred.make<void>()
      const fiber = yield* Effect.scoped(Effect.gen(function*() {
        yield* ReplicaClient.fromRpcClient(definition, wedged, timeouts)
        yield* Deferred.succeed(ready, undefined)
        yield* Effect.never
      })).pipe(Effect.forkChild)
      yield* Deferred.await(ready)
      const interrupting = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      yield* TestClock.adjust(timeouts.sessionTimeout)
      yield* Fiber.join(interrupting)
      const exit = fiber.pollUnsafe()
      assert.isDefined(exit)
      assert.isTrue(Exit.hasInterrupts(exit!))
    }).pipe(Effect.provide(Owner)))
})
