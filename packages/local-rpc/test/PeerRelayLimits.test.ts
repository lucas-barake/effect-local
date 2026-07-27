import { assert, describe, it } from "@effect/vitest"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"

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
        "maximumReceiptRetention",
        "maximumSenderRetryHorizon",
        "messageTtl",
        "minimumTerminalRetention",
        "rateLimitIdleRetention"
      ])
    }).pipe(Effect.provide(PeerRelayLimits.layerDefaults)))

  it.effect.each(
    [
      // A duration has to be finite and positive to bound anything at all.
      ["messageTtl", 0],
      ["messageTtl", Duration.infinity],
      ["rateLimitIdleRetention", -1],
      ["rateLimitIdleRetention", "not a duration"],
      // Negotiated windows also have to fit in what the wire contract can carry.
      ["maximumReceiptRetention", Duration.days(91)],
      ["maxSessionsPerSubject", 0],
      ["authenticationRatePerSecond", Number.NaN]
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
      ["maximumReceiptRetention", { minimumTerminalRetention: Duration.days(7) }],
      ["maximumReceiptRetention", { maximumSenderRetryHorizon: Duration.days(8) }],
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
        messageTtl: Duration.seconds(1),
        maximumSenderRetryHorizon: Duration.seconds(2),
        minimumTerminalRetention: Duration.millis(500),
        maximumReceiptRetention: Duration.millis(2_500)
      })
      assert.strictEqual(Duration.toMillis(values.maximumReceiptRetention), 2_500)
    }))

  it.effect("accepts every shape Duration.Input allows for one value", () =>
    Effect.gen(function*() {
      // The point of taking `Duration.Input` rather than a number of milliseconds: a deployment
      // writes whichever form reads best, and none of them is a different amount of time.
      for (const horizon of [Duration.days(7), "7 days", 7 * 24 * 60 * 60 * 1_000] as const) {
        const values = yield* PeerRelayLimits.make({
          ...PeerRelayLimits.defaults,
          maximumSenderRetryHorizon: horizon
        })
        assert.strictEqual(
          Duration.toMillis(values.maximumSenderRetryHorizon),
          7 * 24 * 60 * 60 * 1_000
        )
      }
    }))
})
