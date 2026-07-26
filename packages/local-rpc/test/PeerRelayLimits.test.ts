import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRpc from "../src/PeerRpc.js"

describe("PeerRelayLimits", () => {
  it.effect("publishes the complete validated production defaults through its Layer", () =>
    Effect.gen(function*() {
      const limits = yield* PeerRelayLimits.PeerRelayLimits
      assert.deepStrictEqual(limits, PeerRelayLimits.defaults)
      assert.strictEqual(Object.keys(limits).length, 97)
      assert.strictEqual(limits.maximumDeliveryAttempts, 16)
    }).pipe(Effect.provide(PeerRelayLimits.layerDefaults)))

  it.effect.each(
    [
      ["maxActiveMessagesPerShard", 0, "Expected a value greater than 0, got 0"],
      ["maxRetainedBytesPerTenant", -1, "Expected a value greater than 0, got -1"],
      ["claimLeaseMillis", 1.5, "Expected an integer, got 1.5"],
      ["maximumDeliveryAttempts", 0, "Expected a value greater than 0, got 0"],
      ["openRatePerSecond", Number.NaN, "Expected a finite number, got NaN"],
      [
        "terminalResponseRatePerSecond",
        Number.POSITIVE_INFINITY,
        "Expected a finite number, got Infinity"
      ],
      [
        "maximumReceiptRetentionMillis",
        PeerRpc.maximumNegotiatedDurationMillis + 1,
        `Expected a value less than or equal to ${PeerRpc.maximumNegotiatedDurationMillis}, got ${
          PeerRpc.maximumNegotiatedDurationMillis + 1
        }`
      ],
      ["sqliteCapacityHeadroomPercent", 101, "Expected a value less than or equal to 100, got 101"],
      ["shutdownReleaseConcurrency", Number.MAX_SAFE_INTEGER + 1, "Expected an integer"]
    ] as const
  )("rejects scalar %s with the stable configuration error", ([field, value]) =>
    Effect.gen(function*() {
      const error = yield* PeerRelayLimits.make({
        ...PeerRelayLimits.defaults,
        [field]: value
      }).pipe(Effect.flip)
      assert.strictEqual(error._tag, "InvalidPeerRelayLimits")
      if (error._tag === "InvalidPeerRelayLimits") assert.strictEqual(error.field, field)
    }))

  const crossFieldCases = [
    ["maxActiveMessagesPerRecipientSubject", { maxActiveMessagesPerRecipientSubject: 9_999 }],
    ["maxActiveBytesPerRecipientSubject", { maxActiveBytesPerRecipientSubject: 1 }],
    ["maxActiveMessagesPerTenant", { maxActiveMessagesPerTenant: 9_999 }],
    ["maxActiveBytesPerTenant", { maxActiveBytesPerTenant: 1 }],
    ["maxActiveMessagesPerShard", { maxActiveMessagesPerShard: 99_999 }],
    ["maxActiveBytesPerShard", { maxActiveBytesPerShard: 1 }],
    ["maxRetainedRowsPerRecipientSubject", { maxRetainedRowsPerRecipientSubject: 9_999 }],
    ["maxRetainedBytesPerRecipientSubject", { maxRetainedBytesPerRecipientSubject: 1 }],
    ["maxRetainedRowsPerTenant", { maxRetainedRowsPerTenant: 9_999 }],
    ["maxRetainedBytesPerTenant", { maxRetainedBytesPerTenant: 1 }],
    ["maxRetainedRowsPerShard", { maxRetainedRowsPerShard: 99_999 }],
    ["maxRetainedBytesPerShard", { maxRetainedBytesPerShard: 1 }],
    [
      "maximumReceiptRetentionMillis",
      { maximumReceiptRetentionMillis: PeerRelayLimits.defaults.maximumReceiptRetentionMillis - 1 }
    ],
    ["claimLeaseMillis", { maximumRecipientProcessingMillis: 45_000 }],
    ["claimLeaseMillis", { claimLeaseMillis: PeerRelayLimits.defaults.messageTtlMillis }],
    ["retryMaximumDelayMillis", { retryMaximumDelayMillis: 249 }],
    ["retryMaximumDelayMillis", { retryMaximumDelayMillis: 60_000 }],
    ["sqliteLockRetryMaximumDelayMillis", { sqliteLockRetryMaximumDelayMillis: 9 }],
    [
      "maximumRawChunkBytes",
      {
        maximumRawChunkBytes: PeerRelayLimits.defaults.maximumIncompleteFrameBytes + 1
      }
    ],
    [
      "maximumDeclaredFrameBytes",
      {
        maximumDeclaredFrameBytes: PeerRelayLimits.defaults.maximumIncompleteFrameBytes - 3
      }
    ],
    [
      "maximumDeclaredFrameBytes",
      { maximumDeclaredFrameBytes: PeerRpc.maximumRelayPayloadBytes - 1 }
    ],
    [
      "maximumSharedPayloadBytes",
      {
        maximumSharedPayloadBytes: PeerRelayLimits.defaults.maximumIncompleteFrameBytes - 1
      }
    ],
    ["maximumSharedPayloadBytes", { relayWorkerConcurrency: 17 }],
    ["maximumByteReservationWaiters", { maximumByteReservationWaiters: 1_023 }],
    ["maxSessionsPerSubject", { maxSessionsPerSubject: 1_025 }],
    ["maxInFlightOpen", { maxInFlightOpen: 1_025, openBurst: 1_025 }],
    ["maxInFlightOpenPerSubject", { maxInFlightOpenPerSubject: 33 }],
    ["openBurst", { openBurst: 31 }],
    ["maxInFlightPushPerSubject", { maxInFlightPushPerSubject: 129 }],
    ["admissionBurst", { admissionBurst: 127 }],
    ["maxRetainedRateLimitedConnections", { maxRetainedRateLimitedConnections: 1_023 }],
    ["maxRetainedRateLimitedSubjects", { maxRetainedRateLimitedSubjects: 31 }],
    [
      "maxInFlightTerminalResponsesPerSubject",
      { maxInFlightTerminalResponsesPerSubject: 65 }
    ],
    ["terminalResponseQueueCapacity", { terminalResponseQueueCapacity: 63 }],
    ["terminalResponseBurst", { terminalResponseBurst: 63 }],
    ["terminalResponseRatePerSecond", { terminalResponseRatePerSecond: 99 }],
    ["maxRetainedTerminalResponseSubjects", { maxRetainedTerminalResponseSubjects: 63 }],
    ["compensationBatchSize", { compensationBatchSize: 10_001 }],
    ["sqlAdmissionQueueCapacity", { sqlAdmissionQueueCapacity: 3 }],
    ["sqlTerminalQueueCapacity", { sqlTerminalQueueCapacity: 3 }],
    ["sqlDeliveryQueueCapacity", { sqlDeliveryQueueCapacity: 3 }],
    ["sqlMaintenanceQueueCapacity", { sqlMaintenanceQueueCapacity: 3 }],
    ["maxInFlightSqlTransactions", { maxInFlightSqlTransactions: 15 }],
    ["claimRecoveryRowsPerSecond", { claimRecoveryRowsPerSecond: 99 }],
    ["expiryRowsPerSecond", { expiryRowsPerSecond: 99 }],
    ["integrityRowsPerSecond", { integrityRowsPerSecond: 99 }],
    ["reconciliationRowsPerSecond", { reconciliationRowsPerSecond: 99 }],
    ["terminalCollectionRowsPerSecond", { terminalCollectionRowsPerSecond: 99 }],
    ["orphanChannelCleanupRowsPerSecond", { orphanChannelCleanupRowsPerSecond: 99 }],
    ["shutdownReleaseConcurrency", { shutdownReleaseConcurrency: 17 }],
    ["shutdownReleaseTimeoutMillis", { shutdownReleaseTimeoutMillis: 60_000 }],
    ["sqliteTransactionCapacityPerSecond", { sqliteTransactionCapacityPerSecond: 127 }]
  ] as const satisfies ReadonlyArray<
    readonly [keyof PeerRelayLimits.Values, Partial<PeerRelayLimits.Values>]
  >

  it.effect.each(crossFieldCases)(
    "rejects the %s cross field constraint through the production Layer",
    ([field, patch]) =>
      Effect.gen(function*() {
        const error = yield* Layer.build(
          PeerRelayLimits.layer({
            ...PeerRelayLimits.defaults,
            ...patch
          })
        ).pipe(Effect.scoped, Effect.flip)
        assert.strictEqual(error._tag, "InvalidPeerRelayLimits")
        if (error._tag === "InvalidPeerRelayLimits") assert.strictEqual(error.field, field)
      })
  )

  it.effect("accepts an exact aggregate SQLite capacity boundary with configured headroom", () =>
    Effect.gen(function*() {
      const required = PeerRelayLimits.defaults.admissionRatePerSecond +
        PeerRelayLimits.defaults.claimRecoveryRowsPerSecond /
          PeerRelayLimits.defaults.claimRecoveryBatchSize +
        PeerRelayLimits.defaults.expiryRowsPerSecond /
          PeerRelayLimits.defaults.expiryBatchSize +
        PeerRelayLimits.defaults.integrityRowsPerSecond /
          PeerRelayLimits.defaults.integrityBatchSize +
        PeerRelayLimits.defaults.reconciliationRowsPerSecond /
          PeerRelayLimits.defaults.reconciliationBatchSize +
        PeerRelayLimits.defaults.terminalCollectionRowsPerSecond /
          PeerRelayLimits.defaults.terminalCollectionBatchSize +
        PeerRelayLimits.defaults.orphanChannelCleanupRowsPerSecond /
          PeerRelayLimits.defaults.orphanChannelCleanupBatchSize
      const exact = required * (1 + PeerRelayLimits.defaults.sqliteCapacityHeadroomPercent / 100)
      const limits = yield* PeerRelayLimits.PeerRelayLimits
      assert.isAtLeast(limits.sqliteTransactionCapacityPerSecond, exact)
    }).pipe(
      Effect.provide(
        PeerRelayLimits.layer({
          ...PeerRelayLimits.defaults,
          sqliteTransactionCapacityPerSecond: 127.2
        })
      )
    ))

  it.effect("rejects a maintenance interval that exceeds the declared row rate", () =>
    Effect.gen(function*() {
      const exit = yield* Effect.exit(PeerRelayLimits.make({
        ...PeerRelayLimits.defaults,
        maintenanceIntervalMillis: 1
      }))
      assert.strictEqual(exit._tag, "Failure")
      if (exit._tag === "Failure") {
        const error = yield* Effect.failCause(exit.cause).pipe(Effect.flip)
        assert.strictEqual(error._tag, "InvalidPeerRelayLimits")
        if (error._tag === "InvalidPeerRelayLimits") {
          assert.strictEqual(error.field, "claimRecoveryRowsPerSecond")
        }
      }
    }))

  it.effect("rejects a maintenance interval that cannot keep up with admission", () =>
    Effect.gen(function*() {
      const exit = yield* Effect.exit(PeerRelayLimits.make({
        ...PeerRelayLimits.defaults,
        maintenanceIntervalMillis: 2_000
      }))
      assert.strictEqual(exit._tag, "Failure")
      if (exit._tag === "Failure") {
        const error = yield* Effect.failCause(exit.cause).pipe(Effect.flip)
        assert.strictEqual(error._tag, "InvalidPeerRelayLimits")
        if (error._tag === "InvalidPeerRelayLimits") {
          assert.strictEqual(error.field, "maintenanceIntervalMillis")
        }
      }
    }))
})
