import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as PublicApi from "../src/index.js"

describe("public API", () => {
  it("exports the durable protocol as version 1", () => {
    for (
      const name of [
        "PeerRelayAuthorization",
        "PeerRelayIngress",
        "PeerRelayLimits",
        "PeerRpc",
        "PeerRelayStore",
        "PeerRpcServer",
        "SqlPeerRelayStore"
      ]
    ) {
      assert.property(PublicApi, name)
    }

    for (const name of ["PeerRelayRpc", "PeerAuthorization", "PeerRpcLimits"]) {
      assert.notProperty(PublicApi, name)
    }

    const decoded = Schema.decodeUnknownSync(PublicApi.PeerRpc.Opened)({
      _tag: "Opened",
      protocolVersion: 1,
      sessionId: "ses_00000000-0000-4000-8000-000000000001",
      remotePeerId: "peer_00000000-0000-4000-8000-000000000002",
      authenticatedLocal: {
        tenantId: "tenant",
        subjectId: "subject",
        peerId: "peer_00000000-0000-4000-8000-000000000001"
      }
    })

    assert.deepStrictEqual(decoded, {
      _tag: "Opened",
      protocolVersion: 1,
      sessionId: "ses_00000000-0000-4000-8000-000000000001",
      remotePeerId: "peer_00000000-0000-4000-8000-000000000002",
      authenticatedLocal: {
        tenantId: "tenant",
        subjectId: "subject",
        peerId: "peer_00000000-0000-4000-8000-000000000001"
      }
    })
  })
})
