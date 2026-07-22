import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as SessionManager from "../src/SessionManager.js"

it.layer(NodeCrypto.layer)("SessionManager", (it) => {
  const clientId = 1
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
    maxStreamsPerSession: 1,
    maxInFlightPerSession: 1,
    maxQueuedRpc: 2
  } satisfies ReplicaLimits.Values

  it.effect("tracks idempotent session registration", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const sessionId = yield* Identity.makeSessionId
      yield* sessions.open(sessionId, clientId)
      yield* sessions.open(sessionId, clientId)
      assert.strictEqual(yield* sessions.activeCount, 1)
      assert.isTrue(yield* sessions.contains(sessionId))
      yield* sessions.close(sessionId, clientId)
      assert.strictEqual(yield* sessions.activeCount, 0)
    }).pipe(Effect.provide(SessionManager.layer), Effect.provideService(ReplicaLimits.ReplicaLimits, limits)))

  it.effect("rejects sessions above the configured limit", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      yield* sessions.open(yield* Identity.makeSessionId, clientId)
      yield* sessions.open(yield* Identity.makeSessionId, clientId)
      const error = yield* Effect.flip(sessions.open(yield* Identity.makeSessionId, clientId))
      assert.strictEqual(error.reason._tag, "QuotaExceeded")
      if (error.reason._tag === "QuotaExceeded") assert.strictEqual(error.reason.limit, limits.maxSessions)
    }).pipe(Effect.provide(SessionManager.layer), Effect.provideService(ReplicaLimits.ReplicaLimits, limits)))

  it.effect("expires and renews leases using the Effect clock", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const expired = yield* Identity.makeSessionId
      const renewed = yield* Identity.makeSessionId
      yield* sessions.open(expired, clientId)
      yield* sessions.open(renewed, clientId)
      yield* TestClock.adjust(SessionManager.leaseDurationMillis / 2)
      yield* sessions.renew(renewed, clientId)
      yield* TestClock.adjust(SessionManager.leaseDurationMillis / 2 + 1)
      assert.isFalse(yield* sessions.contains(expired))
      assert.isTrue(yield* sessions.contains(renewed))
      const error = yield* Effect.flip(sessions.run(expired, clientId, Effect.void))
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
    }).pipe(Effect.provide(SessionManager.layer), Effect.provideService(ReplicaLimits.ReplicaLimits, limits)))

  it.effect("bounds per session execution and the aggregate admitted queue", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const sessionId = yield* Identity.makeSessionId
      const release = yield* Deferred.make<void>()
      const entered = yield* Deferred.make<void>()
      yield* sessions.open(sessionId, clientId)
      const first = yield* sessions.run(
        sessionId,
        clientId,
        Deferred.succeed(entered, void 0).pipe(Effect.andThen(Deferred.await(release)))
      ).pipe(Effect.forkChild)
      yield* Deferred.await(entered)
      const second = yield* sessions.run(sessionId, clientId, Effect.void).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      const error = yield* Effect.flip(sessions.run(sessionId, clientId, Effect.void))
      assert.strictEqual(error.reason._tag, "QuotaExceeded")
      if (error.reason._tag === "QuotaExceeded") assert.strictEqual(error.reason.limit, limits.maxQueuedRpc)
      yield* Deferred.succeed(release, void 0)
      yield* Fiber.join(first)
      yield* Fiber.join(second)
    }).pipe(Effect.provide(SessionManager.layer), Effect.provideService(ReplicaLimits.ReplicaLimits, limits)))

  it.effect("holds stream admission until consumption ends", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const sessionId = yield* Identity.makeSessionId
      const release = yield* Deferred.make<void>()
      const entered = yield* Deferred.make<void>()
      yield* sessions.open(sessionId, clientId)
      const first = yield* sessions.stream(
        sessionId,
        clientId,
        Stream.fromEffect(Deferred.succeed(entered, void 0).pipe(Effect.andThen(Deferred.await(release))))
      ).pipe(Stream.runDrain, Effect.forkChild)
      yield* Deferred.await(entered)
      const second = yield* sessions.stream(sessionId, clientId, Stream.empty).pipe(Stream.runDrain, Effect.forkChild)
      yield* Effect.yieldNow
      const error = yield* sessions.stream(sessionId, clientId, Stream.empty).pipe(Stream.runDrain, Effect.flip)
      assert.strictEqual(error.reason._tag, "QuotaExceeded")
      yield* Deferred.succeed(release, void 0)
      yield* Fiber.join(first)
      yield* Fiber.join(second)
    }).pipe(Effect.provide(SessionManager.layer), Effect.provideService(ReplicaLimits.ReplicaLimits, limits)))

  it.effect("does not let a queued stream consume unary admission", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const sessionId = yield* Identity.makeSessionId
      const release = yield* Deferred.make<void>()
      const entered = yield* Deferred.make<void>()
      const unaryEntered = yield* Deferred.make<void>()
      yield* sessions.open(sessionId, clientId)
      const activeStream = yield* sessions.stream(
        sessionId,
        clientId,
        Stream.fromEffect(Deferred.succeed(entered, void 0).pipe(Effect.andThen(Deferred.await(release))))
      ).pipe(Stream.runDrain, Effect.forkChild)
      yield* Deferred.await(entered)
      const queuedStream = yield* sessions.stream(sessionId, clientId, Stream.empty).pipe(
        Stream.runDrain,
        Effect.forkChild
      )
      yield* Effect.yieldNow
      yield* sessions.run(sessionId, clientId, Deferred.succeed(unaryEntered, void 0))
      yield* Deferred.await(unaryEntered)
      yield* Deferred.succeed(release, void 0)
      yield* Fiber.join(activeStream)
      yield* Fiber.join(queuedStream)
    }).pipe(
      Effect.provide(SessionManager.layer),
      Effect.provideService(ReplicaLimits.ReplicaLimits, {
        ...limits,
        maxInFlightPerSession: 2,
        maxQueuedRpc: 3
      })
    ))

  it.effect("interrupts active streams when their lease expires", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const sessionId = yield* Identity.makeSessionId
      yield* sessions.open(sessionId, clientId)
      const fiber = yield* sessions.stream(sessionId, clientId, Stream.never).pipe(Stream.runDrain, Effect.forkChild)
      yield* TestClock.adjust(SessionManager.leaseDurationMillis)
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      assert.strictEqual(yield* sessions.activeCount, 0)
    }).pipe(Effect.provide(SessionManager.layer), Effect.provideService(ReplicaLimits.ReplicaLimits, limits)))

  it.effect("interrupts active unary work when its lease expires", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const sessionId = yield* Identity.makeSessionId
      const entered = yield* Deferred.make<void>()
      const pending = yield* Deferred.make<void>()
      yield* sessions.open(sessionId, clientId)
      const fiber = yield* sessions.run(
        sessionId,
        clientId,
        Deferred.succeed(entered, void 0).pipe(Effect.andThen(Deferred.await(pending)))
      ).pipe(Effect.forkChild)
      yield* Deferred.await(entered)
      yield* TestClock.adjust(SessionManager.leaseDurationMillis)
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
      assert.strictEqual(yield* sessions.activeCount, 0)
    }).pipe(Effect.provide(SessionManager.layer), Effect.provideService(ReplicaLimits.ReplicaLimits, limits)))
})
