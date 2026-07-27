import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRpc from "../src/PeerRpc.js"

describe("PeerRelayLimits", () => {
  it.effect("publishes the validated production defaults through its Layer", () =>
    Effect.gen(function*() {
      const limits = yield* PeerRelayLimits.PeerRelayLimits
      assert.deepStrictEqual(limits, PeerRelayLimits.defaults)
      // Pinned so a value cannot be added back without a reader, and cannot quietly disappear from
      // under a deployment that sets it. Every one of these is read by production code today.
      assert.deepStrictEqual(Object.keys(limits).toSorted(), [
        "authenticationBurst",
        "authenticationRatePerSecond",
        "maxInFlightAuthentication",
        "maxRetainedRateLimitedConnections",
        "maxSessionsPerSubject",
        "maximumReceiptRetentionMillis",
        "maximumSenderRetryHorizonMillis",
        "messageTtlMillis",
        "minimumTerminalRetentionMillis",
        "rateLimitIdleRetentionMillis"
      ])
    }).pipe(Effect.provide(PeerRelayLimits.layerDefaults)))

  it.effect.each(
    [
      ["maxSessionsPerSubject", 0],
      ["messageTtlMillis", 1.5],
      ["authenticationRatePerSecond", Number.NaN],
      ["authenticationRatePerSecond", Number.POSITIVE_INFINITY],
      ["maximumReceiptRetentionMillis", PeerRpc.maximumNegotiatedDurationMillis + 1],
      ["rateLimitIdleRetentionMillis", Number.MAX_SAFE_INTEGER + 1]
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

  it.effect.each(
    [
      // A receipt that lapses inside its sender's replay window lets the replay land after the
      // deduplication record is gone, and the recipient applies the message twice.
      ["maximumReceiptRetentionMillis", { minimumTerminalRetentionMillis: 7 * 24 * 60 * 60 * 1_000 }],
      ["maximumReceiptRetentionMillis", { maximumSenderRetryHorizonMillis: 8 * 24 * 60 * 60 * 1_000 }],
      // A burst under the concurrency cap makes the rate limiter, not the pool, the binding
      // constraint — so the pool is sized for a concurrency it is never allowed to reach.
      ["authenticationBurst", { authenticationBurst: 1 }]
    ] as const
  )(
    "rejects the %s cross field constraint through the production Layer",
    ([field, overrides]) =>
      Effect.gen(function*() {
        const error = yield* PeerRelayLimits.make({
          ...PeerRelayLimits.defaults,
          ...overrides
        }).pipe(Effect.flip)
        assert.strictEqual(error._tag, "InvalidPeerRelayLimits")
        if (error._tag === "InvalidPeerRelayLimits") assert.strictEqual(error.field, field)
      })
  )

  it.effect("accepts a retention window that exactly covers its retry horizon", () =>
    Effect.gen(function*() {
      // The boundary itself is legal: the relation is stated as `>=`, and a deployment that sizes
      // retention to exactly cover the horizon plus the required slack is correctly configured.
      const values = yield* PeerRelayLimits.make({
        ...PeerRelayLimits.defaults,
        messageTtlMillis: 1_000,
        maximumSenderRetryHorizonMillis: 2_000,
        minimumTerminalRetentionMillis: 500,
        maximumReceiptRetentionMillis: 2_500
      })
      assert.strictEqual(values.maximumReceiptRetentionMillis, 2_500)
    }))
})
