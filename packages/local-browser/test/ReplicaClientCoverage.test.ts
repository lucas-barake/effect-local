import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as HttpClientError from "effect/unstable/http/HttpClientError"
import { RpcTest } from "effect/unstable/rpc"
import * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import * as WorkerError from "effect/unstable/workers/WorkerError"
import * as ReplicaClient from "../src/ReplicaClient.js"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import { definition, DeliveryPublisher, documentId, replica, Task } from "./fixtures.js"

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
    maxQueuedRpc: 4,
    maxQueuedPermits: 4,
    maxActiveRestores: 4,
    maxRestoresPerSession: 2,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  } satisfies ReplicaLimits.Values
  const Sessions = SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(limits)))
  const Publisher = Layer.merge(
    Layer.succeed(
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
    ),
    DeliveryPublisher
  )
  const Owner = ReplicaOwner.layerHandlers(definition).pipe(
    Layer.provide(PeerConnectionStatus.layer),
    Layer.provide(RelayConnectionStatus.layerNotConfigured),
    Layer.provideMerge(Sessions),
    Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, replica)))
  )
  const protocolMismatch = (observed: string) =>
    new ReplicaError.ReplicaError({
      reason: new ReplicaError.ProtocolMismatch({ expected: "active session", observed })
    })
  const disconnected = () =>
    new RpcClientError.RpcClientError({
      reason: new WorkerError.WorkerReceiveError({ message: "disconnected", cause: "disconnected" })
    })

  it.effect("recovers the status stream after a transient failure and surfaces a degraded status", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      let statusCalls = 0
      let activeStatus = 0
      let concurrentStatus = 0
      const attempts = yield* Queue.unbounded<number>()
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Status") return Reflect.get(target, property, receiver)
          return () =>
            Stream.unwrap(Effect.sync(() => {
              statusCalls++
              activeStatus++
              concurrentStatus = Math.max(concurrentStatus, activeStatus)
              const attempt = statusCalls
              return Stream.unwrap(
                Queue.offer(attempts, attempt).pipe(
                  Effect.as(
                    (attempt === 1
                      ? Stream.make(ready).pipe(Stream.concat(Stream.fail(disconnected())))
                      : Stream.make(ready).pipe(Stream.concat(Stream.never))).pipe(
                        Stream.ensuring(Effect.sync(() => {
                          activeStatus--
                        }))
                      )
                  )
                )
              )
            }))
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, reconnecting)
      const fiber = yield* client.status.pipe(Stream.take(3), Stream.runCollect, Effect.forkChild)
      assert.strictEqual(yield* Queue.take(attempts), 1)
      yield* TestClock.adjust("1 second")
      assert.strictEqual(yield* Queue.take(attempts), 2)
      const collected = Array.from(yield* Fiber.join(fiber))
      assert.deepStrictEqual(collected, [
        ready,
        { _tag: "Degraded", reason: "StorageUnavailable" },
        ready
      ])
      assert.strictEqual(statusCalls, 2)
      assert.strictEqual(concurrentStatus, 1)
      assert.strictEqual(activeStatus, 0)
    })).pipe(Effect.provide(Owner)))

  it.effect("spaces repeated transient status failures and reuses the session", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      const queued = new ReplicaError.ReplicaError({
        reason: new ReplicaError.QuotaExceeded({ resource: "queued RPCs", limit: 4 })
      })
      const unavailable = new ReplicaError.ReplicaError({
        reason: new ReplicaError.StorageUnavailable({ cause: "database unavailable" })
      })
      let openSessions = 0
      let statusCalls = 0
      let activeStatus = 0
      const attempts = yield* Queue.unbounded<number>()
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return value(payload)
            }
          }
          if (property !== "Status") return value
          return () =>
            Stream.unwrap(Effect.sync(() => {
              statusCalls++
              activeStatus++
              const attempt = statusCalls
              const stream: Stream.Stream<
                ReplicaStatus.ReplicaStatus,
                ReplicaError.ReplicaError | RpcClientError.RpcClientError
              > = attempt === 1
                ? Stream.fail(disconnected())
                : attempt === 2
                ? Stream.fail(unavailable)
                : attempt === 3
                ? Stream.fail(queued)
                : Stream.make(ready).pipe(Stream.concat(Stream.never))
              return Stream.unwrap(
                Queue.offer(attempts, attempt).pipe(
                  Effect.as(
                    stream.pipe(
                      Stream.ensuring(Effect.sync(() => {
                        activeStatus--
                      }))
                    )
                  )
                )
              )
            }))
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, reconnecting)
      const fiber = yield* client.status.pipe(Stream.take(4), Stream.runCollect, Effect.forkChild)
      assert.strictEqual(yield* Queue.take(attempts), 1)
      assert.strictEqual(statusCalls, 1)
      yield* TestClock.adjust("999 millis")
      assert.strictEqual(statusCalls, 1)
      yield* TestClock.adjust("1 millis")
      assert.strictEqual(yield* Queue.take(attempts), 2)
      assert.strictEqual(statusCalls, 2)
      yield* TestClock.adjust("999 millis")
      assert.strictEqual(statusCalls, 2)
      yield* TestClock.adjust("1 millis")
      assert.strictEqual(yield* Queue.take(attempts), 3)
      assert.strictEqual(statusCalls, 3)
      yield* TestClock.adjust("999 millis")
      assert.strictEqual(statusCalls, 3)
      yield* TestClock.adjust("1 millis")
      assert.strictEqual(yield* Queue.take(attempts), 4)
      const collected = Array.from(yield* Fiber.join(fiber))
      assert.deepStrictEqual(collected, [
        { _tag: "Degraded", reason: "StorageUnavailable" },
        { _tag: "Degraded", reason: "StorageUnavailable" },
        { _tag: "Degraded", reason: "QuotaExceeded" },
        ready
      ])
      assert.strictEqual(statusCalls, 4)
      assert.strictEqual(openSessions, 1)
      assert.strictEqual(activeStatus, 0)
    })).pipe(Effect.provide(Owner)))

  it.effect("retries an HTTP transport failure and recovers status", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      const transport = new RpcClientError.RpcClientError({
        reason: new HttpClientError.HttpClientErrorSchema({
          _tag: "HttpError",
          kind: "TransportError",
          cause: "connection reset"
        })
      })
      let statusCalls = 0
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Status") return Reflect.get(target, property, receiver)
          return () => {
            statusCalls++
            return statusCalls === 1 ? Stream.fail(transport) : Stream.make(ready)
          }
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, reconnecting)
      const fiber = yield* client.status.pipe(Stream.take(2), Stream.runCollect, Effect.forkChild)
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(Array.from(yield* Fiber.join(fiber)), [
        { _tag: "Degraded", reason: "StorageUnavailable" },
        ready
      ])
      assert.strictEqual(statusCalls, 2)
    })).pipe(Effect.provide(Owner)))

  it.effect("does not degrade or retry an HTTP decoding failure", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const decode = new RpcClientError.RpcClientError({
        reason: new HttpClientError.HttpClientErrorSchema({
          _tag: "HttpError",
          kind: "DecodeError",
          cause: "invalid status payload"
        })
      })
      const collected: Array<ReplicaStatus.ReplicaStatus> = []
      let statusCalls = 0
      const malformed = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Status") return Reflect.get(target, property, receiver)
          return () => {
            statusCalls++
            return statusCalls === 1 ? Stream.fail(decode) : Stream.fail(protocolMismatch("unexpected retry"))
          }
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, malformed)
      const fiber = yield* client.status.pipe(
        Stream.runForEach((status) => Effect.sync(() => collected.push(status))),
        Effect.flip,
        Effect.forkChild
      )
      yield* TestClock.adjust("1 second")
      const error = yield* Fiber.join(fiber)
      assert.deepStrictEqual(collected, [])
      assert.strictEqual(statusCalls, 1)
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      if (error.reason._tag === "StorageUnavailable") assert.strictEqual(error.reason.cause, decode)
    })).pipe(Effect.provide(Owner)))

  it.effect("finalizes the active status attempt when its consumer is interrupted", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const started = yield* Deferred.make<void>()
      const finalized = yield* Deferred.make<void>()
      let activeStatus = 0
      let statusCalls = 0
      const hanging = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Status") return Reflect.get(target, property, receiver)
          return () =>
            Stream.unwrap(Effect.sync(() => {
              statusCalls++
              activeStatus++
              return Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
                Stream.drain,
                Stream.concat(Stream.never),
                Stream.ensuring(
                  Effect.sync(() => {
                    activeStatus--
                  }).pipe(Effect.andThen(Deferred.succeed(finalized, undefined)))
                )
              )
            }))
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, hanging)
      const fiber = yield* client.status.pipe(Stream.runDrain, Effect.forkChild)
      yield* Deferred.await(started)
      yield* Fiber.interrupt(fiber)
      yield* Deferred.await(finalized)
      assert.strictEqual(statusCalls, 1)
      assert.strictEqual(activeStatus, 0)
    })).pipe(Effect.provide(Owner)))

  it.effect("reopens the session when status reports a protocol mismatch before emitting", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      const opened: Array<Identity.SessionId> = []
      const observed: Array<Identity.SessionId> = []
      const replacing = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              opened.push(payload.sessionId)
              return value(payload)
            }
          }
          if (property === "Status") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              observed.push(payload.sessionId)
              return observed.length === 1 ? Stream.fail(protocolMismatch("owner restarted")) : Stream.make(ready)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, replacing)
      assert.deepStrictEqual(Array.from(yield* Stream.runCollect(client.status)), [ready])
      assert.strictEqual(opened.length, 2)
      assert.strictEqual(observed.length, 2)
      assert.notStrictEqual(opened[1], opened[0])
      assert.strictEqual(observed[0], opened[0])
      assert.strictEqual(observed[1], opened[1])
    })).pipe(Effect.provide(Owner)))

  it.effect("counts statuses emitted by a replacement before a later mismatch", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      let openSessions = 0
      let statusCalls = 0
      const attempts = yield* Queue.unbounded<number>()
      const replacing = new Proxy(rpc, {
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
              const attempt = statusCalls
              const stream: Stream.Stream<
                ReplicaStatus.ReplicaStatus,
                ReplicaError.ReplicaError | RpcClientError.RpcClientError
              > = attempt === 1
                ? Stream.fail(protocolMismatch("initial mismatch"))
                : attempt === 2
                ? Stream.make(ready).pipe(Stream.concat(Stream.fail(disconnected())))
                : Stream.fail(
                  protocolMismatch(attempt === 3 ? "mismatch after recovery" : "replacement incorrectly resumed")
                )
              return Stream.unwrap(
                Queue.offer(attempts, attempt).pipe(
                  Effect.as(stream)
                )
              )
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, replacing)
      const collected: Array<ReplicaStatus.ReplicaStatus> = []
      const fiber = yield* client.status.pipe(
        Stream.runForEach((status) => Effect.sync(() => collected.push(status))),
        Effect.flip,
        Effect.forkChild
      )
      assert.strictEqual(yield* Queue.take(attempts), 1)
      assert.strictEqual(yield* Queue.take(attempts), 2)
      yield* TestClock.adjust("1 second")
      assert.strictEqual(yield* Queue.take(attempts), 3)
      const error = yield* Fiber.join(fiber)
      assert.deepStrictEqual(collected, [
        ready,
        { _tag: "Degraded", reason: "StorageUnavailable" }
      ])
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") {
        assert.strictEqual(error.reason.observed, "mismatch after recovery")
      }
      assert.strictEqual(openSessions, 3)
      assert.strictEqual(statusCalls, 3)
    })).pipe(Effect.provide(Owner)))

  it.effect("propagates a repeated pre-emission mismatch after one replacement", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      let openSessions = 0
      let statusCalls = 0
      const mismatched = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return openSessions === 3
                ? Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.QuotaExceeded({ resource: "unexpected replacement", limit: 1 })
                  })
                )
                : value(payload)
            }
          }
          if (property === "Status") {
            return () => {
              statusCalls++
              return Stream.fail(protocolMismatch(`mismatch ${statusCalls}`))
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, mismatched)
      const error = yield* client.status.pipe(Stream.runDrain, Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") assert.strictEqual(error.reason.observed, "mismatch 2")
      assert.strictEqual(openSessions, 2)
      assert.strictEqual(statusCalls, 2)
    })).pipe(Effect.provide(Owner)))

  it.effect("reuses one replacement id when an ambiguous status open loses transport", () =>
    Effect.scoped(Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const ambiguousOpen = yield* Deferred.make<void>()
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      const opened: Array<Identity.SessionId> = []
      const observed: Array<Identity.SessionId> = []
      let candidateCleanupFailed = false
      const replacing = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              opened.push(payload.sessionId)
              if (opened.length === 2) {
                return value(payload).pipe(
                  Effect.andThen(Deferred.succeed(ambiguousOpen, undefined)),
                  Effect.andThen(Effect.fail(disconnected()))
                )
              }
              return value(payload)
            }
          }
          if (property === "CloseSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              if (payload.sessionId === opened[1] && !candidateCleanupFailed) {
                candidateCleanupFailed = true
                return Effect.fail(disconnected())
              }
              return value(payload)
            }
          }
          if (property === "Status") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              observed.push(payload.sessionId)
              return payload.sessionId === opened[0]
                ? Stream.fail(protocolMismatch("owner restarted"))
                : Stream.make(ready)
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, replacing)
      const fiber = yield* client.status.pipe(Stream.take(2), Stream.runCollect, Effect.forkChild)
      yield* Deferred.await(ambiguousOpen)
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(Array.from(yield* Fiber.join(fiber)), [
        { _tag: "Degraded", reason: "StorageUnavailable" },
        ready
      ])
      assert.strictEqual(opened.length, 3)
      assert.notStrictEqual(opened[1], opened[0])
      assert.strictEqual(opened[2], opened[1])
      assert.deepStrictEqual(observed, [opened[0], opened[0], opened[1]])
      assert.strictEqual(yield* sessions.activeCount, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("shares an ambiguous status replacement with a normal caller", () =>
    Effect.scoped(Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const ambiguousOpen = yield* Deferred.make<void>()
      const opened: Array<Identity.SessionId> = []
      let candidateCleanupFailed = false
      const replacing = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              opened.push(payload.sessionId)
              if (opened.length === 2) {
                return value(payload).pipe(
                  Effect.andThen(Deferred.succeed(ambiguousOpen, undefined)),
                  Effect.andThen(Effect.fail(disconnected()))
                )
              }
              return value(payload)
            }
          }
          if (property === "CloseSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              if (payload.sessionId === opened[1] && !candidateCleanupFailed) {
                candidateCleanupFailed = true
                return Effect.fail(disconnected())
              }
              return value(payload)
            }
          }
          if (property === "Status") return () => Stream.fail(protocolMismatch("owner restarted"))
          if (property === "Get") {
            return (payload: { readonly sessionId: Identity.SessionId }) =>
              payload.sessionId === opened[0]
                ? Effect.fail(protocolMismatch("owner restarted"))
                : value(payload)
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, replacing)
      const status = yield* client.status.pipe(Stream.runDrain, Effect.forkChild)
      yield* Deferred.await(ambiguousOpen)
      assert.strictEqual((yield* client.get(Task, documentId)).documentId, documentId)
      assert.strictEqual(opened.length, 3)
      assert.notStrictEqual(opened[1], opened[0])
      assert.strictEqual(opened[2], opened[1])
      assert.strictEqual(yield* sessions.activeCount, 1)
      yield* Fiber.interrupt(status)
    })).pipe(Effect.provide(Owner)))

  it.effect("does not let an older stale caller clear the current replacement candidate", () =>
    Effect.scoped(Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const staleGetStarted = yield* Deferred.make<void>()
      const releaseStaleGet = yield* Deferred.make<void>()
      const ambiguousOpen = yield* Deferred.make<void>()
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      const opened: Array<Identity.SessionId> = []
      let candidateCleanupFailed = false
      const replacing = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              opened.push(payload.sessionId)
              if (opened.length === 3) {
                return value(payload).pipe(
                  Effect.andThen(Deferred.succeed(ambiguousOpen, undefined)),
                  Effect.andThen(Effect.fail(disconnected()))
                )
              }
              return value(payload)
            }
          }
          if (property === "CloseSession") {
            return (payload: { readonly sessionId: Identity.SessionId }) => {
              if (payload.sessionId === opened[2] && !candidateCleanupFailed) {
                candidateCleanupFailed = true
                return Effect.fail(disconnected())
              }
              return value(payload)
            }
          }
          if (property === "Get") {
            return (payload: { readonly sessionId: Identity.SessionId }) =>
              payload.sessionId === opened[0]
                ? Deferred.succeed(staleGetStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseStaleGet)),
                  Effect.andThen(Effect.fail(protocolMismatch("older stale session")))
                )
                : value(payload)
          }
          if (property === "Flush") {
            return (payload: { readonly sessionId: Identity.SessionId }) =>
              payload.sessionId === opened[0]
                ? Effect.fail(protocolMismatch("owner restarted"))
                : value(payload)
          }
          if (property === "Status") {
            return (payload: { readonly sessionId: Identity.SessionId }) =>
              payload.sessionId === opened[1]
                ? Stream.fail(protocolMismatch("owner restarted again"))
                : Stream.make(ready)
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, replacing)
      const staleGet = yield* client.get(Task, documentId).pipe(Effect.forkChild)
      yield* Deferred.await(staleGetStarted)
      yield* client.flush
      const status = yield* client.status.pipe(Stream.take(2), Stream.runCollect, Effect.forkChild)
      yield* Deferred.await(ambiguousOpen)
      yield* Deferred.succeed(releaseStaleGet, undefined)
      assert.strictEqual((yield* Fiber.join(staleGet)).documentId, documentId)
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(Array.from(yield* Fiber.join(status)), [
        { _tag: "Degraded", reason: "StorageUnavailable" },
        ready
      ])
      assert.strictEqual(opened.length, 4)
      assert.notStrictEqual(opened[1], opened[0])
      assert.notStrictEqual(opened[2], opened[1])
      assert.strictEqual(opened[3], opened[2])
      assert.strictEqual(yield* sessions.activeCount, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("interrupts a status consumer waiting for another replacement", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const firstReplacementStarted = yield* Deferred.make<void>()
      const releaseFirstReplacement = yield* Deferred.make<void>()
      const secondStatusStarted = yield* Deferred.make<void>()
      const secondCancelled = yield* Deferred.make<void>()
      let openSessions = 0
      let statusCalls = 0
      const hanging = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return openSessions === 2
                ? Deferred.succeed(firstReplacementStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseFirstReplacement)),
                  Effect.andThen(Effect.fail(disconnected()))
                )
                : value(payload)
            }
          }
          if (property === "Status") {
            return () =>
              Stream.unwrap(Effect.gen(function*() {
                statusCalls++
                if (statusCalls === 2) yield* Deferred.succeed(secondStatusStarted, undefined)
                return Stream.fail(protocolMismatch("owner restarted"))
              }))
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, hanging)
      const first = yield* client.status.pipe(Stream.runDrain, Effect.forkChild)
      yield* Deferred.await(firstReplacementStarted)
      const second = yield* client.status.pipe(Stream.runDrain, Effect.forkChild)
      yield* Deferred.await(secondStatusStarted)
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      assert.strictEqual(openSessions, 2)
      yield* Fiber.interrupt(second).pipe(
        Effect.andThen(Deferred.succeed(secondCancelled, undefined)),
        Effect.forkChild
      )
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      assert.isTrue(yield* Deferred.isDone(secondCancelled))
      yield* Deferred.succeed(releaseFirstReplacement, undefined)
      yield* Fiber.interrupt(first)
    })).pipe(Effect.provide(Owner)))

  it.effect("does not degrade or retry an RPC protocol defect", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const defect = new RpcClientError.RpcClientError({
        reason: new RpcClientError.RpcClientDefect({
          message: "invalid status response",
          cause: "invalid status response"
        })
      })
      const collected: Array<ReplicaStatus.ReplicaStatus> = []
      let statusCalls = 0
      const malformed = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Status") return Reflect.get(target, property, receiver)
          return () => {
            statusCalls++
            return statusCalls === 1 ? Stream.fail(defect) : Stream.fail(protocolMismatch("unexpected retry"))
          }
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, malformed)
      const fiber = yield* client.status.pipe(
        Stream.runForEach((status) => Effect.sync(() => collected.push(status))),
        Effect.flip,
        Effect.forkChild
      )
      yield* TestClock.adjust("1 second")
      const error = yield* Fiber.join(fiber)
      assert.deepStrictEqual(collected, [])
      assert.strictEqual(statusCalls, 1)
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      if (error.reason._tag === "StorageUnavailable") assert.strictEqual(error.reason.cause, defect)
    })).pipe(Effect.provide(Owner)))

  it.effect("does not degrade or retry a permanent status quota", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const quota = new ReplicaError.ReplicaError({
        reason: new ReplicaError.QuotaExceeded({ resource: "status history bytes", limit: 1 })
      })
      const collected: Array<ReplicaStatus.ReplicaStatus> = []
      let statusCalls = 0
      const exhausted = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "Status") return Reflect.get(target, property, receiver)
          return () => {
            statusCalls++
            return statusCalls === 1 ? Stream.fail(quota) : Stream.fail(protocolMismatch("unexpected retry"))
          }
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, exhausted)
      const fiber = yield* client.status.pipe(
        Stream.runForEach((status) => Effect.sync(() => collected.push(status))),
        Effect.flip,
        Effect.forkChild
      )
      yield* TestClock.adjust("1 second")
      const error = yield* Fiber.join(fiber)
      assert.deepStrictEqual(collected, [])
      assert.strictEqual(statusCalls, 1)
      assert.strictEqual(error.reason._tag, "QuotaExceeded")
      if (error.reason._tag === "QuotaExceeded") {
        assert.strictEqual(error.reason.resource, "status history bytes")
      }
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
                refreshGeneration: 0,
                deliveryWatermark: 0,
                deliveryRefreshEpoch: 0
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

  it.effect("keeps an emitted ProtocolMismatch terminal across a replacement outage", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const replacementFailed = yield* Deferred.make<void>()
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      let openSessions = 0
      let statusCalls = 0
      const flaky = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return openSessions === 2
                ? Deferred.succeed(replacementFailed, undefined).pipe(
                  Effect.andThen(Effect.fail(disconnected()))
                )
                : value(payload)
            }
          }
          if (property === "Status") {
            return () => {
              statusCalls++
              if (statusCalls === 1) {
                return Stream.make(ready).pipe(Stream.concat(Stream.fail(protocolMismatch("owner restarted"))))
              }
              return Stream.fail(
                protocolMismatch(statusCalls === 2 ? "owner restarted" : "replacement incorrectly resumed")
              )
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, flaky)
      const collected: Array<ReplicaStatus.ReplicaStatus> = []
      const fiber = yield* client.status.pipe(
        Stream.runForEach((status) => Effect.sync(() => collected.push(status))),
        Effect.flip,
        Effect.forkChild
      )
      yield* Deferred.await(replacementFailed)
      yield* TestClock.adjust("1 second")
      const error = yield* Fiber.join(fiber)
      assert.deepStrictEqual(collected, [
        ready,
        { _tag: "Degraded", reason: "StorageUnavailable" }
      ])
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") assert.strictEqual(error.reason.observed, "owner restarted")
      assert.strictEqual(openSessions, 3)
      assert.strictEqual(statusCalls, 1)
    })).pipe(Effect.provide(Owner)))

  it.effect("keeps a mismatch terminal when another status consumer replaces the session", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const firstReplacementFailed = yield* Deferred.make<void>()
      const ready = { _tag: "Ready" as const, pendingCommands: 0 }
      let openSessions = 0
      let statusCalls = 0
      const replacing = new Proxy(rpc, {
        get(target, property, receiver) {
          const value = Reflect.get(target, property, receiver)
          if (property === "OpenSession") {
            return (payload: never) => {
              openSessions++
              return openSessions === 2
                ? Deferred.succeed(firstReplacementFailed, undefined).pipe(
                  Effect.andThen(Effect.fail(disconnected()))
                )
                : value(payload)
            }
          }
          if (property === "Status") {
            return () => {
              statusCalls++
              if (statusCalls === 1) {
                return Stream.make(ready).pipe(Stream.concat(Stream.fail(protocolMismatch("first mismatch"))))
              }
              if (statusCalls === 2) return Stream.fail(protocolMismatch("second mismatch"))
              if (statusCalls === 3) return Stream.make(ready)
              return Stream.fail(protocolMismatch("first consumer incorrectly resumed"))
            }
          }
          return value
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, replacing)
      const firstCollected: Array<ReplicaStatus.ReplicaStatus> = []
      const first = yield* client.status.pipe(
        Stream.runForEach((status) => Effect.sync(() => firstCollected.push(status))),
        Effect.flip,
        Effect.forkChild
      )
      yield* Deferred.await(firstReplacementFailed)
      assert.deepStrictEqual(Array.from(yield* client.status.pipe(Stream.take(1), Stream.runCollect)), [ready])
      yield* TestClock.adjust("1 second")
      const error = yield* Fiber.join(first)
      assert.deepStrictEqual(firstCollected, [
        ready,
        { _tag: "Degraded", reason: "StorageUnavailable" }
      ])
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      if (error.reason._tag === "ProtocolMismatch") assert.strictEqual(error.reason.observed, "first mismatch")
      assert.strictEqual(openSessions, 3)
      assert.strictEqual(statusCalls, 3)
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
        {
          _tag: "FullRefreshRequired",
          ownerEpoch: client.ownerEpoch,
          keys: [Task.name, ReplicaRpc.commandDeliveryInvalidationKey]
        }
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
                refreshGeneration: 0,
                deliveryWatermark: 0,
                deliveryRefreshEpoch: 0
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
        {
          _tag: "FullRefreshRequired",
          ownerEpoch: client.ownerEpoch,
          keys: [Task.name, ReplicaRpc.commandDeliveryInvalidationKey]
        }
      ])
      assert.strictEqual(error.reason._tag, "QuotaExceeded")
    })).pipe(Effect.provide(Owner)))
})
