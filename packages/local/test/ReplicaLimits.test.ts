import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as ReplicaLimits from "../src/ReplicaLimits.js"

describe("ReplicaLimits", () => {
  const values: ReplicaLimits.Values = {
    maxBackupBytes: 1024,
    maxChunkBytes: 256,
    maxArchiveRecords: 100,
    maxJsonDepth: 16,
    maxSyncMessageBytes: 512,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 10,
    maxSyncDependencyEdgesPerMessage: 100,
    maxSyncOperationsPerMessage: 1000,
    maxPendingBytesPerDocument: 1024,
    maxPendingBytesPerPeer: 2048,
    maxPendingBytesPerReplica: 4096,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 100,
    maxPendingChangesPerPeer: 200,
    maxPendingChangesPerReplica: 400,
    maxPendingDependencyEdgesPerDocument: 1000,
    maxPendingDependencyEdgesPerPeer: 2000,
    maxPendingDependencyEdgesPerReplica: 4000,
    maxSessions: 4,
    maxStreamsPerSession: 2,
    maxInFlightPerSession: 8,
    maxQueuedRpc: 32,
    maxQueuedPermits: 32,
    maxActiveRestores: 32,
    maxRestoresPerSession: 8,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  }
  const restoreLimitKeys = [
    "maxActiveRestores",
    "maxRestoresPerSession",
    "maxRestoreMillis",
    "maxRestorePullMillis",
    "maxRestoreCoalesceMillis",
    "maxRestoreErrorBytes"
  ] as const

  it.effect("requires and provides validated owner limits", () =>
    Effect.gen(function*() {
      assert.deepStrictEqual(yield* ReplicaLimits.ReplicaLimits, values)
    }).pipe(Effect.provide(ReplicaLimits.layer(values))))

  it.effect("accepts feasible safe integer boundaries and rejects fractions", () =>
    Effect.gen(function*() {
      const lowerBoundaries = {
        ...values,
        maxActiveRestores: 1,
        maxRestoresPerSession: 1,
        maxRestoreMillis: 1,
        maxRestorePullMillis: 2,
        maxRestoreCoalesceMillis: 1,
        maxRestoreErrorBytes: ReplicaLimits.minimumRestoreErrorBytes
      }
      const upperBoundaries = {
        ...values,
        maxActiveRestores: Number.MAX_SAFE_INTEGER,
        maxRestoresPerSession: Number.MAX_SAFE_INTEGER,
        maxRestoreMillis: Number.MAX_SAFE_INTEGER,
        maxRestorePullMillis: Number.MAX_SAFE_INTEGER,
        maxRestoreCoalesceMillis: Number.MAX_SAFE_INTEGER - 1,
        maxRestoreErrorBytes: Number.MAX_SAFE_INTEGER
      }

      assert.strictEqual((yield* Effect.exit(ReplicaLimits.make(lowerBoundaries)))._tag, "Success")
      assert.strictEqual((yield* Effect.exit(ReplicaLimits.make(upperBoundaries)))._tag, "Success")

      for (const key of restoreLimitKeys) {
        assert.strictEqual(
          (yield* Effect.exit(ReplicaLimits.make({
            ...lowerBoundaries,
            [key]: lowerBoundaries[key] + 0.5
          })))._tag,
          "Failure"
        )
      }
    }))

  it.effect("requires every restore limit at the runtime schema boundary", () =>
    Effect.gen(function*() {
      for (const key of restoreLimitKeys) {
        const input: Record<string, unknown> = { ...values }
        delete input[key]

        const error = yield* Schema.decodeUnknownEffect(ReplicaLimits.Values)(input).pipe(Effect.flip)
        assert.include(error.message, "Missing key")
        assert.include(error.message, `at ["${key}"]`)
      }
    }))

  it.effect("rejects nonpositive and unsafe limits", () =>
    Effect.gen(function*() {
      assert.strictEqual((yield* Effect.exit(ReplicaLimits.make({ ...values, maxSessions: 0 })))._tag, "Failure")
      assert.strictEqual((yield* Effect.exit(ReplicaLimits.make({ ...values, maxQueuedPermits: 0 })))._tag, "Failure")
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxBackupBytes: Number.MAX_VALUE })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxActiveRestores: 0 })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxRestoresPerSession: 0 })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxRestoreMillis: 0 })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxRestorePullMillis: 0 })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxRestoreCoalesceMillis: 0 })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxActiveRestores: Number.MAX_VALUE })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxRestoresPerSession: Number.MAX_VALUE })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxRestoreMillis: Number.MAX_VALUE })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxRestorePullMillis: Number.MAX_VALUE })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxRestoreCoalesceMillis: Number.MAX_VALUE })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxRestoreErrorBytes: Number.MAX_VALUE })))._tag,
        "Failure"
      )
    }))

  it.effect("requires enough restore error bytes to preserve every wire error shape", () =>
    Effect.gen(function*() {
      assert.strictEqual(ReplicaLimits.minimumRestoreErrorBytes, 111)
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({
          ...values,
          maxRestoreErrorBytes: ReplicaLimits.minimumRestoreErrorBytes
        })))._tag,
        "Success"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({
          ...values,
          maxRestoreErrorBytes: ReplicaLimits.minimumRestoreErrorBytes - 1
        })))._tag,
        "Failure"
      )
    }))

  it.effect("requires restore coalescing to be shorter than the pull deadline", () =>
    Effect.gen(function*() {
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({
          ...values,
          maxRestoreCoalesceMillis: values.maxRestorePullMillis
        })))._tag,
        "Failure"
      )
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({
          ...values,
          maxRestoreCoalesceMillis: values.maxRestorePullMillis + 1
        })))._tag,
        "Failure"
      )
    }))
})
