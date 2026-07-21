import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
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
    maxQueuedRpc: 32
  }

  it.effect("requires and provides validated owner limits", () =>
    Effect.gen(function*() {
      assert.deepStrictEqual(yield* ReplicaLimits.ReplicaLimits, values)
    }).pipe(Effect.provide(ReplicaLimits.layer(values))))

  it.effect("rejects nonpositive and unsafe limits", () =>
    Effect.gen(function*() {
      assert.strictEqual((yield* Effect.exit(ReplicaLimits.make({ ...values, maxSessions: 0 })))._tag, "Failure")
      assert.strictEqual(
        (yield* Effect.exit(ReplicaLimits.make({ ...values, maxBackupBytes: Number.MAX_VALUE })))._tag,
        "Failure"
      )
    }))
})
