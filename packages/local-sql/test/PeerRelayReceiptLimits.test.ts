import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as PeerRelayReceiptLimits from "../src/PeerRelayReceiptLimits.js"

describe("PeerRelayReceiptLimits", () => {
  it.effect("accepts the bounded defaults", () =>
    Effect.gen(function*() {
      assert.deepStrictEqual(
        yield* PeerRelayReceiptLimits.make(PeerRelayReceiptLimits.defaults),
        PeerRelayReceiptLimits.defaults
      )
    }))

  it.effect("rejects invalid scalar and aggregate relationships", () =>
    Effect.gen(function*() {
      for (
        const values of [
          { ...PeerRelayReceiptLimits.defaults, maxReceiptsPerRemote: 0 },
          { ...PeerRelayReceiptLimits.defaults, maxEncodedBytesPerRemote: Number.POSITIVE_INFINITY },
          {
            ...PeerRelayReceiptLimits.defaults,
            receiptRetentionMillis: PeerRelayReceiptLimits.maximumReceiptRetentionMillis + 1
          },
          {
            ...PeerRelayReceiptLimits.defaults,
            maxReceiptsPerRemote: PeerRelayReceiptLimits.defaults.maxReceiptsPerReplica + 1
          },
          {
            ...PeerRelayReceiptLimits.defaults,
            maxEncodedBytesPerRemote: PeerRelayReceiptLimits.defaults.maxEncodedBytesPerReplica + 1
          }
        ]
      ) {
        assert.strictEqual(
          (yield* Effect.exit(PeerRelayReceiptLimits.make(
            values satisfies PeerRelayReceiptLimits.Values
          )))._tag,
          "Failure"
        )
      }
    }))
})
