import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as ReplicaLimits from "../src/ReplicaLimits.js"

const assertSchemaFailure = <A,>(
  effect: Effect.Effect<A, Schema.SchemaError>,
  message?: string
) =>
  Effect.gen(function*() {
    const error = yield* Effect.flip(effect)
    assert.isTrue(Schema.isSchemaError(error), message)
    return error
  })

describe("ReplicaLimits", () => {
  const values: ReplicaLimits.Values = {
    maxBackupBytes: 1024,
    maxChunkBytes: 256,
    maxArchiveRecords: 100,
    maxJsonDepth: 16,
    maxConflictDepth: 16,
    maxConflictNodes: 1_000,
    maxConflictAlternatives: 100,
    maxConflictPathSegments: 16,
    maxConflictValueBytes: 1024,
    maxConflictSourceChanges: 1_000,
    maxConflictSourceOperations: 1_000,
    maxConflictSourceBytes: 4096,
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
  ] satisfies ReadonlyArray<keyof ReplicaLimits.Values>
  const conflictLimitKeys = [
    "maxConflictDepth",
    "maxConflictNodes",
    "maxConflictAlternatives",
    "maxConflictPathSegments",
    "maxConflictValueBytes",
    "maxConflictSourceChanges",
    "maxConflictSourceOperations",
    "maxConflictSourceBytes"
  ] satisfies ReadonlyArray<keyof ReplicaLimits.Values>

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
        const error = yield* assertSchemaFailure(
          ReplicaLimits.make({
            ...lowerBoundaries,
            [key]: lowerBoundaries[key] + 0.5
          }),
          key
        )
        assert.include(error.message, `["${key}"]`)
      }
    }))

  it.effect("requires every restore limit at the runtime schema boundary", () =>
    Effect.gen(function*() {
      for (const key of restoreLimitKeys) {
        const input: Record<string, unknown> = { ...values }
        delete input[key]

        const error = yield* Schema.decodeUnknownEffect(ReplicaLimits.Values)(input).pipe(Effect.flip)
        assert.isTrue(Schema.isSchemaError(error), key)
        assert.include(error.message, "Missing key")
        assert.include(error.message, `at ["${key}"]`)
      }
    }))

  it.effect("requires every conflict limit at the runtime schema boundary", () =>
    Effect.gen(function*() {
      for (const key of conflictLimitKeys) {
        const input: Record<string, unknown> = { ...values }
        delete input[key]

        const error = yield* Schema.decodeUnknownEffect(ReplicaLimits.Values)(input).pipe(Effect.flip)
        assert.isTrue(Schema.isSchemaError(error), key)
        assert.include(error.message, "Missing key")
        assert.include(error.message, `at ["${key}"]`)
      }
    }))

  it.effect("accepts exact conflict ceilings and rejects values above them", () =>
    Effect.gen(function*() {
      const ceilings = {
        ...values,
        maxConflictDepth: ReplicaLimits.maxConflictDepthHardLimit,
        maxConflictNodes: ReplicaLimits.maxConflictNodesHardLimit,
        maxConflictAlternatives: ReplicaLimits.maxConflictAlternativesHardLimit,
        maxConflictPathSegments: ReplicaLimits.maxConflictPathSegmentsHardLimit,
        maxConflictValueBytes: ReplicaLimits.maxConflictValueBytesHardLimit,
        maxConflictSourceChanges: ReplicaLimits.maxConflictSourceChangesHardLimit,
        maxConflictSourceOperations: ReplicaLimits.maxConflictSourceOperationsHardLimit,
        maxConflictSourceBytes: ReplicaLimits.maxConflictSourceBytesHardLimit
      }
      assert.strictEqual((yield* Effect.exit(ReplicaLimits.make(ceilings)))._tag, "Success")

      for (const key of conflictLimitKeys) {
        const above = yield* assertSchemaFailure(
          ReplicaLimits.make({
            ...ceilings,
            [key]: ceilings[key] + 1
          }),
          key
        )
        assert.include(above.message, `["${key}"]`)
        const zero = yield* assertSchemaFailure(
          ReplicaLimits.make({
            ...values,
            [key]: 0
          }),
          key
        )
        assert.include(zero.message, `["${key}"]`)
      }
    }))

  it.effect("rejects nonpositive and unsafe limits", () =>
    Effect.gen(function*() {
      const invalid = [
        ["maxSessions", 0],
        ["maxQueuedPermits", 0],
        ["maxBackupBytes", Number.MAX_VALUE],
        ["maxActiveRestores", 0],
        ["maxRestoresPerSession", 0],
        ["maxRestoreMillis", 0],
        ["maxRestorePullMillis", 0],
        ["maxRestoreCoalesceMillis", 0],
        ["maxActiveRestores", Number.MAX_VALUE],
        ["maxRestoresPerSession", Number.MAX_VALUE],
        ["maxRestoreMillis", Number.MAX_VALUE],
        ["maxRestorePullMillis", Number.MAX_VALUE],
        ["maxRestoreCoalesceMillis", Number.MAX_VALUE],
        ["maxRestoreErrorBytes", Number.MAX_VALUE]
      ] satisfies ReadonlyArray<readonly [keyof ReplicaLimits.Values, number]>
      for (const [key, value] of invalid) {
        const error = yield* assertSchemaFailure(
          ReplicaLimits.make({ ...values, [key]: value }),
          key
        )
        assert.include(error.message, `["${key}"]`)
      }
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
      const error = yield* assertSchemaFailure(
        ReplicaLimits.make({
          ...values,
          maxRestoreErrorBytes: ReplicaLimits.minimumRestoreErrorBytes - 1
        })
      )
      assert.include(error.message, `["maxRestoreErrorBytes"]`)
    }))

  it.effect("requires restore coalescing to be shorter than the pull deadline", () =>
    Effect.gen(function*() {
      const equal = yield* assertSchemaFailure(
        ReplicaLimits.make({
          ...values,
          maxRestoreCoalesceMillis: values.maxRestorePullMillis
        })
      )
      assert.include(equal.message, "maxRestoreCoalesceMillis being less than maxRestorePullMillis")
      const greater = yield* assertSchemaFailure(
        ReplicaLimits.make({
          ...values,
          maxRestoreCoalesceMillis: values.maxRestorePullMillis + 1
        })
      )
      assert.include(greater.message, "maxRestoreCoalesceMillis being less than maxRestorePullMillis")
    }))
})
