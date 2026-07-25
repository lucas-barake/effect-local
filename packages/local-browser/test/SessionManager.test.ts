import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Result from "effect/Result"
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
    maxQueuedRpc: 2,
    maxActiveRestores: 2,
    maxRestoresPerSession: 1,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
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

  it.effect("exposes the validated restore transport limits", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      assert.strictEqual(sessions.maxChunkBytes, limits.maxChunkBytes)
      assert.strictEqual(sessions.maxActiveRestores, limits.maxActiveRestores)
      assert.strictEqual(sessions.maxRestoresPerSession, limits.maxRestoresPerSession)
      assert.strictEqual(sessions.maxRestoreMillis, limits.maxRestoreMillis)
      assert.strictEqual(sessions.maxRestorePullMillis, limits.maxRestorePullMillis)
      assert.strictEqual(sessions.maxRestoreCoalesceMillis, limits.maxRestoreCoalesceMillis)
      assert.strictEqual(sessions.maxRestoreErrorBytes, limits.maxRestoreErrorBytes)
    }).pipe(Effect.provide(SessionManager.layer), Effect.provideService(ReplicaLimits.ReplicaLimits, limits)))

  it.effect("acquires restore admission independently and releases it idempotently", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const sessionId = yield* Identity.makeSessionId
      yield* sessions.open(sessionId, clientId)

      const lease = yield* sessions.acquireRestore(sessionId, clientId)
      assert.strictEqual(yield* sessions.activeRestoreCount, 1)
      assert.strictEqual(yield* sessions.activeRestoreCountForSession(sessionId), 1)

      yield* sessions.run(sessionId, clientId, Effect.void)
      yield* sessions.stream(sessionId, clientId, Stream.empty).pipe(Stream.runDrain)

      yield* lease.release
      yield* lease.release
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      assert.strictEqual(yield* sessions.activeRestoreCountForSession(sessionId), 0)
    }).pipe(Effect.provide(SessionManager.layer), Effect.provideService(ReplicaLimits.ReplicaLimits, limits)))

  it.effect("admits no more than the effective restore capacity under concurrent acquisition", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const sessionIds = yield* Effect.all([
        Identity.makeSessionId,
        Identity.makeSessionId,
        Identity.makeSessionId
      ])
      yield* Effect.forEach(sessionIds, (sessionId) => sessions.open(sessionId, clientId), { discard: true })

      assert.strictEqual(sessions.effectiveRestoreCapacity, 5)
      const start = yield* Deferred.make<void>()
      const attempts = sessionIds.flatMap((sessionId) => Array.from({ length: 4 }, () => sessionId))
      const fibers = yield* Effect.forEach(attempts, (sessionId) =>
        Effect.gen(function*() {
          yield* Deferred.await(start)
          const result = yield* Effect.result(sessions.acquireRestore(sessionId, clientId))
          return {
            sessionId,
            result,
            observedTotal: yield* sessions.activeRestoreCount,
            observedForSession: yield* sessions.activeRestoreCountForSession(sessionId)
          }
        }).pipe(Effect.forkChild({ startImmediately: true })))

      yield* Deferred.succeed(start, void 0)
      const outcomes = yield* Effect.forEach(fibers, Fiber.join, { concurrency: "unbounded" })
      for (const outcome of outcomes) {
        assert.isAtMost(outcome.observedTotal, sessions.effectiveRestoreCapacity)
        assert.isAtMost(outcome.observedForSession, sessions.maxRestoresPerSession)
      }

      const successes = outcomes.flatMap((outcome) =>
        Result.isSuccess(outcome.result)
          ? [{ sessionId: outcome.sessionId, lease: outcome.result.success }]
          : []
      )
      const failures = outcomes.flatMap((outcome) => Result.isFailure(outcome.result) ? [outcome.result.failure] : [])
      assert.strictEqual(successes.length, sessions.effectiveRestoreCapacity)
      assert.strictEqual(failures.length, attempts.length - sessions.effectiveRestoreCapacity)
      for (const error of failures) {
        assert.strictEqual(error.reason._tag, "QuotaExceeded")
        if (error.reason._tag === "QuotaExceeded") {
          if (error.reason.resource === "active restores") {
            assert.strictEqual(error.reason.limit, sessions.effectiveRestoreCapacity)
          } else {
            assert.strictEqual(error.reason.resource, "active restores per session")
            assert.strictEqual(error.reason.limit, sessions.maxRestoresPerSession)
          }
        }
      }

      assert.strictEqual(yield* sessions.activeRestoreCount, sessions.effectiveRestoreCapacity)
      const activeBySession = yield* Effect.forEach(
        sessionIds,
        sessions.activeRestoreCountForSession
      )
      assert.deepStrictEqual(activeBySession.toSorted((left, right) => left - right), [1, 2, 2])

      const regeneratedSession = sessionIds[0]
      const capacityLease = successes.find(({ sessionId }) => sessionId !== regeneratedSession)
      assert.isDefined(capacityLease)
      if (capacityLease === undefined) return
      yield* capacityLease.lease.release
      yield* capacityLease.lease.release
      assert.strictEqual(yield* sessions.activeRestoreCount, sessions.effectiveRestoreCapacity - 1)

      yield* sessions.close(regeneratedSession, clientId)
      yield* sessions.open(regeneratedSession, clientId)
      assert.strictEqual(yield* sessions.activeRestoreCountForSession(regeneratedSession), 0)
      const replacement = yield* sessions.acquireRestore(regeneratedSession, clientId)

      const staleLeases = successes.filter(({ sessionId }) => sessionId === regeneratedSession)
      for (const stale of staleLeases) {
        yield* stale.lease.release
        yield* stale.lease.release
        assert.strictEqual(yield* sessions.activeRestoreCountForSession(regeneratedSession), 1)
      }

      const remainingLeases = successes.filter(
        (success) => success !== capacityLease && success.sessionId !== regeneratedSession
      )
      yield* Effect.forEach(
        remainingLeases,
        ({ lease }) => lease.release.pipe(Effect.andThen(lease.release)),
        { discard: true }
      )
      assert.strictEqual(yield* sessions.activeRestoreCount, 1)

      yield* replacement.release
      yield* replacement.release
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
      for (const sessionId of sessionIds) {
        assert.strictEqual(yield* sessions.activeRestoreCountForSession(sessionId), 0)
      }
    }).pipe(
      Effect.provide(SessionManager.layer),
      Effect.provideService(ReplicaLimits.ReplicaLimits, {
        ...limits,
        maxSessions: 3,
        maxActiveRestores: 5,
        maxRestoresPerSession: 2
      })
    ))

  it.effect("fails restore admission fast with session then global then per session precedence", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      assert.strictEqual(sessions.maxActiveRestores, 10)
      assert.strictEqual(sessions.effectiveRestoreCapacity, 2)
      const firstSession = yield* Identity.makeSessionId
      const secondSession = yield* Identity.makeSessionId
      const missingSession = yield* Identity.makeSessionId
      yield* sessions.open(firstSession, clientId)
      yield* sessions.open(secondSession, clientId)

      const firstLease = yield* sessions.acquireRestore(firstSession, clientId)
      const perSessionError = yield* Effect.flip(sessions.acquireRestore(firstSession, clientId))
      assert.strictEqual(perSessionError.reason._tag, "QuotaExceeded")
      if (perSessionError.reason._tag === "QuotaExceeded") {
        assert.strictEqual(perSessionError.reason.resource, "active restores per session")
        assert.strictEqual(perSessionError.reason.limit, limits.maxRestoresPerSession)
      }

      const secondLease = yield* sessions.acquireRestore(secondSession, clientId)
      const sessionError = yield* Effect.flip(sessions.acquireRestore(missingSession, clientId))
      assert.strictEqual(sessionError.reason._tag, "ProtocolMismatch")
      const ownershipError = yield* Effect.flip(sessions.acquireRestore(firstSession, clientId + 1))
      assert.strictEqual(ownershipError.reason._tag, "ProtocolMismatch")

      const globalError = yield* Effect.flip(sessions.acquireRestore(firstSession, clientId))
      assert.strictEqual(globalError.reason._tag, "QuotaExceeded")
      if (globalError.reason._tag === "QuotaExceeded") {
        assert.strictEqual(globalError.reason.resource, "active restores")
        assert.strictEqual(globalError.reason.limit, 2)
      }
      assert.strictEqual(yield* sessions.activeRestoreCount, 2)
      assert.strictEqual(yield* sessions.activeRestoreCountForSession(firstSession), 1)

      yield* firstLease.release
      yield* secondLease.release
    }).pipe(
      Effect.provide(SessionManager.layer),
      Effect.provideService(ReplicaLimits.ReplicaLimits, {
        ...limits,
        maxActiveRestores: 10
      })
    ))

  it.effect("keeps expired restore admission reserved until its worker releases", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const sessionId = yield* Identity.makeSessionId
      yield* sessions.open(sessionId, clientId)
      const staleLease = yield* sessions.acquireRestore(sessionId, clientId)

      yield* TestClock.adjust(SessionManager.leaseDurationMillis)
      const expiry = yield* Deferred.await(staleLease.expired).pipe(Effect.flip)
      assert.strictEqual(expiry.reason._tag, "ProtocolMismatch")
      assert.strictEqual(yield* sessions.activeRestoreCount, 1)

      yield* sessions.open(sessionId, clientId)
      const replacementLease = yield* sessions.acquireRestore(sessionId, clientId)
      const globalError = yield* Effect.flip(sessions.acquireRestore(sessionId, clientId))
      assert.strictEqual(globalError.reason._tag, "QuotaExceeded")
      if (globalError.reason._tag === "QuotaExceeded") {
        assert.strictEqual(globalError.reason.resource, "active restores")
      }
      assert.strictEqual(yield* sessions.activeRestoreCount, 2)

      yield* staleLease.release
      assert.strictEqual(yield* sessions.activeRestoreCount, 1)
      assert.strictEqual(yield* sessions.activeRestoreCountForSession(sessionId), 1)
      yield* replacementLease.release
      assert.strictEqual(yield* sessions.activeRestoreCount, 0)
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
