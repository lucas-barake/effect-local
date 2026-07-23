import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import { RpcTest } from "effect/unstable/rpc"
import * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import * as ReplicaClient from "../src/ReplicaClient.js"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import { definition, documentId, replica, Task } from "./fixtures.js"

it.layer(NodeCrypto.layer)("ReplicaClient coverage", (it) => {
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
  const protocolMismatch = (observed: string) =>
    new ReplicaError.ReplicaError({
      reason: new ReplicaError.ProtocolMismatch({ expected: "active session", observed })
    })
  const disconnected = () =>
    new RpcClientError.RpcClientError({
      reason: new RpcClientError.RpcClientDefect({ message: "disconnected", cause: "disconnected" })
    })

  it.effect("recovers the status stream after a transient failure and surfaces a degraded status", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      let statusCalls = 0
      let activeStatus = 0
      let concurrentStatus = 0
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Status") return Reflect.get(target, property, receiver)
          return () =>
            Stream.unwrap(Effect.sync(() => {
              statusCalls++
              activeStatus++
              concurrentStatus = Math.max(concurrentStatus, activeStatus)
              const attempt = statusCalls
              return (attempt === 1
                ? Stream.make(ready).pipe(Stream.concat(Stream.fail(disconnected())))
                : Stream.make(ready).pipe(Stream.concat(Stream.never))).pipe(
                  Stream.ensuring(Effect.sync(() => {
                    activeStatus--
                  }))
                )
            }))
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, reconnecting)
      const fiber = yield* client.status.pipe(Stream.take(3), Stream.runCollect, Effect.forkChild)
      yield* TestClock.adjust("1 second")
      const collected = Array.from(yield* Fiber.join(fiber))
      assert.deepStrictEqual(collected, [
        ready,
        { _tag: "Degraded", reason: "StorageUnavailable" },
        ready
      ])
      assert.strictEqual(statusCalls, 2)
      assert.strictEqual(concurrentStatus, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("drops duplicate and stale invalidation sequences", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const replayed = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Invalidations") return Reflect.get(target, property, receiver)
          return ({ ownerEpoch }: { readonly ownerEpoch: string }) =>
            Stream.make(
              {
                _tag: "InvalidationsReady" as const,
                ownerEpoch,
                watermark: Identity.CommitSequence.make(0),
                refreshGeneration: 0
              },
              {
                _tag: "Invalidation" as const,
                ownerEpoch,
                sequence: Identity.CommitSequence.make(1),
                keys: [Task.name]
              },
              {
                _tag: "Invalidation" as const,
                ownerEpoch,
                sequence: Identity.CommitSequence.make(1),
                keys: [Task.name]
              },
              {
                _tag: "Invalidation" as const,
                ownerEpoch,
                sequence: Identity.CommitSequence.make(2),
                keys: [Task.name]
              }
            )
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, replayed)
      assert.deepStrictEqual(Array.from(yield* Stream.runCollect(client.invalidations)), [
        {
          _tag: "Invalidation",
          ownerEpoch: client.ownerEpoch,
          sequence: Identity.CommitSequence.make(1),
          keys: [Task.name]
        },
        {
          _tag: "Invalidation",
          ownerEpoch: client.ownerEpoch,
          sequence: Identity.CommitSequence.make(2),
          keys: [Task.name]
        }
      ])
    })).pipe(Effect.provide(Owner)))

  it.effect("fails an already-emitting stream on ProtocolMismatch instead of restarting", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let statusCalls = 0
      let openSessions = 0
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      const flaky = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return value(payload)
            }
          }
          if (property === "Status") {
            return () => {
              statusCalls++
              return statusCalls === 1
                ? Stream.make(ready).pipe(Stream.concat(Stream.fail(protocolMismatch("owner restarted"))))
                : Stream.make(ready)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, flaky)
      const collected: Array<ReplicaStatus.ReplicaStatus> = []
      const error = yield* client.status.pipe(
        Stream.runForEach((status) => Effect.sync(() => collected.push(status))),
        Effect.flip
      )
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      assert.deepStrictEqual(collected, [ready])
      assert.strictEqual(statusCalls, 1)
      assert.strictEqual(openSessions, 2)
    })).pipe(Effect.provide(Owner)))

  it.effect("reopens the session when lease renewal reports a protocol mismatch", () =>
    Effect.scoped(Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let renewals = 0
      let openSessions = 0
      const expiring = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return value(payload)
            }
          }
          if (property === "RenewSession") {
            return (payload: never) => {
              renewals++
              return renewals === 1 ? Effect.fail(protocolMismatch("lease expired")) : value(payload)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, expiring)
      yield* TestClock.adjust(SessionManager.leaseDurationMillis / 2 + 1)
      assert.strictEqual(renewals, 1)
      assert.strictEqual(openSessions, 2)
      assert.strictEqual(yield* sessions.activeCount, 1)
      assert.strictEqual((yield* client.get(Task, documentId)).documentId, documentId)
    })).pipe(Effect.provide(Owner)))

  it.effect("emits a final full refresh when the invalidation stream fails terminally", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const terminal = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Invalidations") return Reflect.get(target, property, receiver)
          return () =>
            Stream.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.QuotaExceeded({ resource: "invalidations", limit: 1 })
              })
            )
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, terminal)
      const collected: Array<ReplicaRpc.Invalidation> = []
      const error = yield* client.invalidations.pipe(
        Stream.runForEach((event) => Effect.sync(() => collected.push(event))),
        Effect.flip
      )
      assert.deepStrictEqual(collected, [
        { _tag: "FullRefreshRequired", ownerEpoch: client.ownerEpoch, keys: [Task.name] }
      ])
      assert.strictEqual(error.reason._tag, "QuotaExceeded")
    })).pipe(Effect.provide(Owner)))

  it.effect("tears down invalidations with a full refresh when lease renewal dies", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const doomed = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property === "RenewSession") {
            return () =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.QuotaExceeded({ resource: "renew", limit: 1 })
                })
              )
          }
          if (property === "Invalidations") {
            return ({ ownerEpoch }: { readonly ownerEpoch: string }) =>
              Stream.make({
                _tag: "InvalidationsReady" as const,
                ownerEpoch,
                watermark: Identity.CommitSequence.make(0),
                refreshGeneration: 0
              }).pipe(Stream.concat(Stream.never))
          }
          return Reflect.get(target, property, receiver)
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, doomed)
      const collected: Array<ReplicaRpc.Invalidation> = []
      const fiber = yield* client.invalidations.pipe(
        Stream.runForEach((event) => Effect.sync(() => collected.push(event))),
        Effect.flip,
        Effect.forkChild
      )
      yield* TestClock.adjust(SessionManager.leaseDurationMillis / 2 + 1)
      const error = yield* Fiber.join(fiber)
      assert.deepStrictEqual(collected, [
        { _tag: "FullRefreshRequired", ownerEpoch: client.ownerEpoch, keys: [Task.name] }
      ])
      assert.strictEqual(error.reason._tag, "QuotaExceeded")
    })).pipe(Effect.provide(Owner)))
})
