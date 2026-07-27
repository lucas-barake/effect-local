import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Schema from "effect/Schema"
import * as PeerRpc from "../src/PeerRpc.js"

const localPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const remotePeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
const sessionId = Identity.SessionId.make("ses_00000000-0000-4000-8000-000000000001")
const relayMessageId = Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000001")
const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const claimToken = PeerRpc.ClaimToken.make("clm_00000000-0000-4000-8000-000000000001")
const hash = "0".repeat(64)

describe("PeerRpc", () => {
  it("roundtrips the strict version 1 handshake", () => {
    const opened = PeerRpc.Opened.make({
      _tag: "Opened",
      protocolVersion: PeerRpc.protocolVersion,
      sessionId,
      remotePeerId,
      authenticatedLocal: {
        tenantId: "tenant",
        subjectId: "subject",
        peerId: localPeerId
      }
    })

    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRpc.OpenEvent)(opened), opened)
  })

  it("pins the handshake protocol version so a foreign version fails to decode", () => {
    // `Opened` carries a literal, not a number, so a client cannot be talked into continuing
    // against a relay speaking a version it does not implement.
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRpc.OpenEvent)({
        _tag: "Opened",
        protocolVersion: PeerRpc.protocolVersion + 1,
        sessionId,
        remotePeerId,
        authenticatedLocal: { tenantId: "tenant", subjectId: "subject", peerId: localPeerId }
      })
    )
  })

  it("roundtrips every relay request and stored message field", () => {
    const open = PeerRpc.OpenRpc.payloadSchema.make({
      protocolVersion: PeerRpc.protocolVersion,
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
    const stored = PeerRpc.StoredMessage.make({
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
    const push = PeerRpc.PushRpc.payloadSchema.make({
      sessionId,
      relayMessageId,
      payload: Uint8Array.of(1, 2, 3)
    })
    const acknowledge = PeerRpc.AcknowledgeRpc.payloadSchema.make({
      sessionId,
      relayMessageId,
      claimToken,
      messageHash: hash
    })
    const reject = PeerRpc.RejectRpc.payloadSchema.make({
      ...acknowledge,
      reason: "ProtocolInvalid"
    })

    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRpc.OpenRpc.payloadSchema)(open), open)
    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRpc.OpenEvent)(stored), stored)
    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRpc.PushRpc.payloadSchema)(push), push)
    assert.deepStrictEqual(
      Schema.decodeUnknownSync(PeerRpc.AcknowledgeRpc.payloadSchema)(acknowledge),
      acknowledge
    )
    assert.deepStrictEqual(Schema.decodeUnknownSync(PeerRpc.RejectRpc.payloadSchema)(reject), reject)
  })

  it("rejects invalid durations, empty document sets, identifiers, hashes, and oversized payloads", () => {
    const baseOpen = {
      protocolVersion: PeerRpc.protocolVersion,
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
        Schema.decodeUnknownSync(PeerRpc.OpenRpc.payloadSchema)({
          ...baseOpen,
          receiptRetentionMillis: duration
        })
      )
    }
    assert.throws(() => Schema.decodeUnknownSync(PeerRpc.OpenRpc.payloadSchema)({ ...baseOpen, documents: [] }))
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRpc.AcknowledgeRpc.payloadSchema)({
        sessionId,
        relayMessageId: "rly_invalid",
        claimToken,
        messageHash: hash
      })
    )
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRpc.AcknowledgeRpc.payloadSchema)({
        sessionId,
        relayMessageId,
        claimToken,
        messageHash: "not-a-hash"
      })
    )
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRpc.PushRpc.payloadSchema)({
        sessionId,
        relayMessageId,
        payload: new Uint8Array(PeerRpc.maximumRelayPayloadBytes + 1)
      })
    )
    assert.throws(() =>
      Schema.decodeUnknownSync(PeerRpc.OpenEvent)(
        PeerRpc.StoredMessage.make({
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
