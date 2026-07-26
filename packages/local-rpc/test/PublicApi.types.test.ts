import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as PublicApi from "../src/index.js"

describe("public API", () => {
  it("exports the version 3 relay protocol without widening version 2", () => {
    const publicApi: Record<string, unknown> = PublicApi

    for (
      const name of [
        "PeerRelayAuthorization",
        "PeerRelayIngress",
        "PeerRelayLimits",
        "PeerRelayRpc",
        "PeerRelayStore"
      ]
    ) {
      assert.property(publicApi, name)
    }

    const peerRelayRpc = publicApi.PeerRelayRpc as Record<string, unknown>
    const relayOpened = peerRelayRpc.RelayOpened as Schema.Schema<unknown>
    const decoded = Schema.decodeUnknownSync(relayOpened)({
      _tag: "RelayOpened",
      version: 3,
      sessionId: "ses_00000000-0000-4000-8000-000000000001",
      remotePeerId: "peer_00000000-0000-4000-8000-000000000002",
      authenticatedLocal: {
        tenantId: "tenant",
        subjectId: "subject",
        peerId: "peer_00000000-0000-4000-8000-000000000001"
      },
      capabilities: {
        storeAndForward: true
      }
    })

    assert.deepStrictEqual(decoded, {
      _tag: "RelayOpened",
      version: 3,
      sessionId: "ses_00000000-0000-4000-8000-000000000001",
      remotePeerId: "peer_00000000-0000-4000-8000-000000000002",
      authenticatedLocal: {
        tenantId: "tenant",
        subjectId: "subject",
        peerId: "peer_00000000-0000-4000-8000-000000000001"
      },
      capabilities: {
        storeAndForward: true
      }
    })
  })
})
