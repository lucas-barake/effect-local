import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as PeerRelayOutboxLimits from "../src/PeerRelayOutboxLimits.js"

describe("PeerRelayOutboxLimits", () => {
  it.effect("accepts the bounded defaults", () =>
    Effect.gen(function*() {
      assert.deepStrictEqual(
        yield* PeerRelayOutboxLimits.make(PeerRelayOutboxLimits.defaults),
        PeerRelayOutboxLimits.defaults
      )
    }))

  it.effect("rejects invalid scalar and aggregate relationships", () =>
    Effect.gen(function*() {
      for (
        const values of [
          { ...PeerRelayOutboxLimits.defaults, maxMessagesPerRemote: 0 },
          { ...PeerRelayOutboxLimits.defaults, pruneRowsPerSecond: Number.NaN },
          {
            ...PeerRelayOutboxLimits.defaults,
            maxRetryHorizonMillis: PeerRelayOutboxLimits.maximumRetryHorizonMillis + 1
          },
          {
            ...PeerRelayOutboxLimits.defaults,
            maxMessagesPerRemote: PeerRelayOutboxLimits.defaults.maxMessagesPerReplica + 1
          },
          {
            ...PeerRelayOutboxLimits.defaults,
            maxEncodedBytesPerRemote: PeerRelayOutboxLimits.defaults.maxEncodedBytesPerReplica + 1
          }
        ]
      ) {
        assert.strictEqual(
          (yield* Effect.exit(PeerRelayOutboxLimits.make(
            values as PeerRelayOutboxLimits.Values
          )))._tag,
          "Failure"
        )
      }
    }))
})
