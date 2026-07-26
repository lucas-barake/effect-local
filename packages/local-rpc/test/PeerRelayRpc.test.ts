import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Schema from "effect/Schema"
import * as PeerRelayRpc from "../src/PeerRelayRpc.js"
import * as PeerRpc from "../src/PeerRpc.js"

const localPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const remotePeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
const sessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")
const relayMessageId = Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000001")
const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const claimToken = PeerRelayRpc.ClaimToken.make("clm_00000000-0000-4000-8000-000000000001")
const hash = "0".repeat(64)

describe("PeerRelayRpc", () => {
  it("roundtrips the strict version 3 handshake", () => {
    const opened = PeerRelayRpc.RelayOpened.make({
      _tag: "RelayOpened",
      version: PeerRelayRpc.protocolVersion,
      sessionId,
      remotePeerId,
      authenticatedLocal: {
        tenantId: "tenant",
        subjectId: "subject",
        peerId: localPeerId
      },
      capabilities: { storeAndForward: true }
    })

    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRelayRpc.OpenRelayEvent)(opened), opened)
    assert.throws(() => Schema.decodeUnknownSync(PeerRpc.OpenEvent)(opened))
  })

  it("roundtrips every relay request and stored message field", () => {
    const open = PeerRelayRpc.OpenRelayRpc.payloadSchema.make({
      version: PeerRelayRpc.protocolVersion,
      expectedRelayPeerId: relayPeerId,
      expectedLocal: {
        tenantId: "tenant",
        subjectId: "subject",
        peerId: localPeerId
      },
      senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
      remote: {
        subjectId: "remote",
        peerId: remotePeerId
      },
      documents: [{ documentType: "Task", documentId }],
      receiptRetentionMillis: 8 * 24 * 60 * 60 * 1_000,
      senderRetryHorizonMillis: 7 * 24 * 60 * 60 * 1_000
    })
    const stored = PeerRelayRpc.StoredMessage.make({
      _tag: "StoredMessage",
      relayMessageId,
      claimToken,
      relayPeerId,
      sender: {
        tenantId: "tenant",
        subjectId: "subject",
        peerId: localPeerId,
        replicaIncarnation: Identity.ReplicaIncarnation.make(1),
        connectionEpoch: "epoch",
        sequence: 0
      },
      recipient: {
        tenantId: "tenant",
        subjectId: "remote",
        peerId: remotePeerId
      },
      payloadVersion: 1,
      document: { documentType: "Task", documentId },
      writerProvenance: [],
      messageHash: hash,
      outerEnvelopeDigest: hash,
      payload: Uint8Array.of(1, 2, 3)
    })
    const push = PeerRelayRpc.PushRelayRpc.payloadSchema.make({
      sessionId,
      relayMessageId,
      payload: Uint8Array.of(1, 2, 3)
    })
    const acknowledge = PeerRelayRpc.AcknowledgeRelayRpc.payloadSchema.make({
      sessionId,
      relayMessageId,
      claimToken,
      messageHash: hash
    })
    const reject = PeerRelayRpc.RejectRelayRpc.payloadSchema.make({
      ...acknowledge,
      reason: "ProtocolInvalid"
    })

    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRelayRpc.OpenRelayRpc.payloadSchema)(open), open)
    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRelayRpc.OpenRelayEvent)(stored), stored)
    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRelayRpc.PushRelayRpc.payloadSchema)(push), push)
    assert.deepStrictEqual(
      Schema.decodeUnknownSync(PeerRelayRpc.AcknowledgeRelayRpc.payloadSchema)(acknowledge),
      acknowledge
    )
    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRelayRpc.RejectRelayRpc.payloadSchema)(reject), reject)
  })

  it("rejects invalid durations, empty document sets, identifiers, hashes, and oversized payloads", () => {
    const baseOpen = {
      version: PeerRelayRpc.protocolVersion,
      expectedRelayPeerId: relayPeerId,
      expectedLocal: {
        tenantId: "tenant",
        subjectId: "subject",
        peerId: localPeerId
      },
      senderReplicaIncarnation: 1,
      remote: {
        subjectId: "remote",
        peerId: remotePeerId
      },
      documents: [{ documentType: "Task", documentId }],
      receiptRetentionMillis: 1,
      senderRetryHorizonMillis: 1
    }

    for (const duration of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      assert.throws(() =>
        Schema.decodeUnknownSync(PeerRelayRpc.OpenRelayRpc.payloadSchema)({
          ...baseOpen,
          receiptRetentionMillis: duration
        })
      )
    }
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRelayRpc.OpenRelayRpc.payloadSchema)({ ...baseOpen, documents: [] })
    )
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRelayRpc.AcknowledgeRelayRpc.payloadSchema)({
        sessionId,
        relayMessageId: "rly_invalid",
        claimToken,
        messageHash: hash
      })
    )
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRelayRpc.AcknowledgeRelayRpc.payloadSchema)({
        sessionId,
        relayMessageId,
        claimToken,
        messageHash: "not-a-hash"
      })
    )
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRelayRpc.PushRelayRpc.payloadSchema)({
        sessionId,
        relayMessageId,
        payload: new Uint8Array(PeerRelayRpc.maximumRelayPayloadBytes + 1)
      })
    )
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRelayRpc.OpenRelayEvent)(
        PeerRelayRpc.StoredMessage.make({
          _tag: "StoredMessage",
          relayMessageId,
          claimToken,
          relayPeerId,
          sender: {
            tenantId: "tenant",
            subjectId: "subject",
            peerId: localPeerId,
            replicaIncarnation: Identity.ReplicaIncarnation.make(1),
            connectionEpoch: "epoch",
            sequence: 0
          },
          recipient: {
            tenantId: "tenant",
            subjectId: "remote",
            peerId: remotePeerId
          },
          payloadVersion: 1,
          document: { documentType: "Task", documentId },
          writerProvenance: [{
            changeHash: hash,
            writerSchemaVersion: 1,
            writerDefinitionHash: "invalid\"definition"
          }],
          messageHash: hash,
          outerEnvelopeDigest: hash,
          payload: Uint8Array.of(1)
        })
      )
    )
  })
})
