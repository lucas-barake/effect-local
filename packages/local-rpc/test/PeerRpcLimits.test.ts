import { assert, describe, it } from "@effect/vitest"
import * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as PeerRpcLimits from "../src/PeerRpcLimits.js"

const replicaLimits = ReplicaLimits.Values.make({
  maxBackupBytes: 1,
  maxChunkBytes: 1,
  maxArchiveRecords: 1,
  maxJsonDepth: 1,
  maxSyncMessageBytes: 1_024,
  maxPeerSendMillis: 1,
  maxSyncChangesPerMessage: 1,
  maxSyncDependencyEdgesPerMessage: 1,
  maxSyncOperationsPerMessage: 1,
  maxPendingBytesPerDocument: 1,
  maxPendingBytesPerPeer: 1,
  maxPendingBytesPerReplica: 1,
  maxPendingAgeMillis: 1,
  maxPendingChangesPerDocument: 1,
  maxPendingChangesPerPeer: 1,
  maxPendingChangesPerReplica: 1,
  maxPendingDependencyEdgesPerDocument: 1,
  maxPendingDependencyEdgesPerPeer: 1,
  maxPendingDependencyEdgesPerReplica: 1,
  maxSessions: 1,
  maxStreamsPerSession: 1,
  maxInFlightPerSession: 1,
  maxQueuedRpc: 1
})

describe("PeerRpcLimits", () => {
  it.effect("publishes every conservative default exactly", () =>
    Effect.gen(function*() {
      const limits = yield* PeerRpcLimits.make(PeerRpcLimits.defaults).pipe(
        Effect.provideService(ReplicaLimits.ReplicaLimits, replicaLimits)
      )
      assert.deepStrictEqual(limits, {
        maxSessionsPerSubject: 4,
        inboundItemCapacity: 1,
        outboundItemCapacity: 1,
        maxInboundBufferedBytesPerSession: 4 * 1_024 * 1_024,
        maxOutboundBufferedBytesPerSession: 4 * 1_024 * 1_024,
        maxBufferedBytes: 64 * 1_024 * 1_024,
        maxInFlightAuthentication: 64,
        authenticationRatePerSecond: 16,
        authenticationBurst: 32,
        maxInFlightOpen: 16,
        maxInFlightOpenPerSubject: 2,
        maxInFlightPush: 128,
        maxInFlightPushPerSubject: 8,
        openRatePerSecond: 2,
        openBurst: 4,
        pushRatePerSecond: 64,
        pushBurst: 128,
        maxRetainedRateLimitedConnections: 10_000,
        maxRetainedRateLimitedSubjects: 10_000,
        rateLimitIdleRetention: 10 * 60_000,
        maximumReauthorizationInterval: 5 * 60_000,
        commitFlushConcurrency: 8,
        shutdownCleanupConcurrency: 16
      })
    }))

  it.effect("provides equivalent defaults through layerDefaults", () =>
    Effect.gen(function*() {
      const limits = yield* PeerRpcLimits.PeerRpcLimits
      assert.deepStrictEqual(limits, PeerRpcLimits.defaults)
    }).pipe(
      Effect.provide(PeerRpcLimits.layerDefaults),
      Effect.provideService(ReplicaLimits.ReplicaLimits, replicaLimits)
    ))

  it.effect("preserves native SchemaError trees and messages for invalid scalar limits", () =>
    Effect.gen(function*() {
      const invalid = [
        [
          "maxSessionsPerSubject",
          "Expected a value greater than 0, got 0",
          { ...PeerRpcLimits.defaults, maxSessionsPerSubject: 0 }
        ],
        [
          "maxSessionsPerSubject",
          "Expected a value greater than 0, got -1",
          { ...PeerRpcLimits.defaults, maxSessionsPerSubject: -1 }
        ],
        [
          "authenticationRatePerSecond",
          "Expected a finite number, got Infinity",
          { ...PeerRpcLimits.defaults, authenticationRatePerSecond: Number.POSITIVE_INFINITY }
        ],
        [
          "inboundItemCapacity",
          "Expected an integer, got 1.5",
          { ...PeerRpcLimits.defaults, inboundItemCapacity: 1.5 }
        ],
        [
          "shutdownCleanupConcurrency",
          "Expected a value greater than 0, got 0",
          { ...PeerRpcLimits.defaults, shutdownCleanupConcurrency: 0 }
        ]
      ] as const
      for (const [field, expected, values] of invalid) {
        const error = yield* PeerRpcLimits.make(values).pipe(
          Effect.provideService(ReplicaLimits.ReplicaLimits, replicaLimits),
          Effect.flip
        )
        assert.strictEqual(error._tag, "SchemaError")
        if (error._tag !== "SchemaError") continue
        assert.strictEqual(error.issue._tag, "Composite")
        if (error.issue._tag !== "Composite") continue
        assert.strictEqual(error.issue.issues[0]?._tag, "Pointer")
        if (error.issue.issues[0]?._tag === "Pointer") {
          assert.deepStrictEqual(error.issue.issues[0].path, [field])
        }
        assert.strictEqual(error.message, `${expected}\n  at ["${field}"]`)
      }
    }))

  it.effect("rejects incompatible byte budgets with the exact field", () =>
    Effect.gen(function*() {
      const envelope = PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes)
      assert.strictEqual(envelope, 6_144)
      const cases = [
        ["maxInboundBufferedBytesPerSession", {
          ...PeerRpcLimits.defaults,
          maxInboundBufferedBytesPerSession: envelope - 1
        }],
        ["maxOutboundBufferedBytesPerSession", {
          ...PeerRpcLimits.defaults,
          maxOutboundBufferedBytesPerSession: envelope - 1
        }],
        ["inboundItemCapacity", {
          ...PeerRpcLimits.defaults,
          inboundItemCapacity: 2,
          maxInboundBufferedBytesPerSession: envelope
        }],
        ["outboundItemCapacity", {
          ...PeerRpcLimits.defaults,
          outboundItemCapacity: 2,
          maxOutboundBufferedBytesPerSession: envelope
        }],
        ["maxBufferedBytes", {
          ...PeerRpcLimits.defaults,
          maxBufferedBytes: envelope - 1
        }]
      ] as const

      for (const [field, values] of cases) {
        const error = yield* PeerRpcLimits.make(values).pipe(
          Effect.provideService(ReplicaLimits.ReplicaLimits, replicaLimits),
          Effect.flip
        )
        assert.strictEqual(error._tag, "InvalidPeerRpcLimits")
        if (error._tag === "InvalidPeerRpcLimits") assert.strictEqual(error.field, field)
      }
    }))
})
