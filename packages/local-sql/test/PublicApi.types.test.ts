import { assert, describe, it } from "@effect/vitest"
import * as PublicApi from "../src/index.js"

describe("public API", () => {
  it("exports the opt in relay persistence services", () => {
    const publicApi: Record<string, unknown> = PublicApi

    for (
      const name of [
        "PeerRelayClientRuntime",
        "PeerRelayOutbox",
        "PeerRelayOutboxLimits",
        "PeerRelayReceiptLimits",
        "PeerConnectionStatus",
        "PeerSyncEnvelope"
      ]
    ) {
      assert.property(publicApi, name)
    }
  })
})
