import { assert } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRelayStore from "../src/PeerRelayStore.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as SqlPeerRelayStore from "../src/SqlPeerRelayStore.js"

const peerId = (suffix: string) => Identity.PeerId.make(`peer_00000000-0000-4000-8000-${suffix}`)

const relayMessageId = (suffix: string) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${suffix}`)

const documentId = (suffix: string) => Identity.DocumentId.make(`doc_00000000-0000-4000-8000-${suffix}`)

const makeFixture = (index: number, overrides: Partial<PeerRelayStore.Admission> = {}) => {
  const suffix = String(index).padStart(12, "0")
  const channel = PeerRelayStore.ChannelKey.make({
    tenantId: `tenant-contract-${index}`,
    senderSubjectId: `sender-contract-${index}`,
    senderPeerId: peerId(suffix),
    senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
    recipientSubjectId: `recipient-contract-${index}`,
    recipientPeerId: peerId(String(index + 100).padStart(12, "0"))
  })
  const admission = PeerRelayStore.Admission.make({
    channel,
    relayMessageId: relayMessageId(suffix),
    relayPeerId: peerId(String(index + 200).padStart(12, "0")),
    documentIds: [documentId(suffix)],
    senderConnectionEpoch: `epoch-contract-${index}`,
    senderSequence: 0,
    payloadVersion: 1,
    messageHash: `message-hash-contract-${index}`,
    outerEnvelopeDigest: PeerRpc.RelayDigest.make(
      (index % 16).toString(16).repeat(64)
    ),
    payload: new Uint8Array([index]),
    messageTtlMillis: PeerRelayLimits.defaults.messageTtlMillis,
    senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis,
    minimumTerminalRetentionMillis: PeerRelayLimits.defaults.minimumTerminalRetentionMillis,
    ...overrides
  })
  const claim = (sessionGeneration: number): PeerRelayStore.ClaimRequest =>
    PeerRelayStore.ClaimRequest.make({
      recipient: {
        tenantId: admission.channel.tenantId,
        subjectId: admission.channel.recipientSubjectId,
        peerId: admission.channel.recipientPeerId
      },
      sender: {
        subjectId: admission.channel.senderSubjectId,
        peerId: admission.channel.senderPeerId
      },
      sessionGeneration,
      authorizedDocumentIds: admission.documentIds
    })
  return { admission, channel: admission.channel, claim }
}

const terminalRequest = (
  message: PeerRelayStore.ClaimedMessage
): PeerRelayStore.TerminalRequest =>
  PeerRelayStore.TerminalRequest.make({
    channel: message.channel,
    relayMessageId: message.relayMessageId,
    claimToken: message.claimToken,
    messageHash: message.messageHash,
    sessionGeneration: message.sessionGeneration,
    recipient: {
      tenantId: message.channel.tenantId,
      subjectId: message.channel.recipientSubjectId,
      peerId: message.channel.recipientPeerId
    }
  })

const claimedMessage = (
  result: PeerRelayStore.ClaimResult
): PeerRelayStore.ClaimedMessage => {
  assert.strictEqual(Option.isSome(result.message), true)
  if (Option.isNone(result.message)) {
    throw new Error("Expected the relay message to be claimed")
  }
  return result.message.value
}

export const runPeerRelayStoreContract = Effect.gen(function*() {
  const store = yield* PeerRelayStore.PeerRelayStore
  const sql = yield* SqlClient.SqlClient

  assert.deepStrictEqual(yield* store.usage(), {
    activeCount: 0,
    activeBytes: 0,
    retainedCount: 0,
    retainedBytes: 0
  })

  const lifecycle = makeFixture(1)
  const accepted = yield* store.admit(lifecycle.admission)
  assert.strictEqual(accepted.status, "Accepted")
  assert.strictEqual((yield* store.admit(lifecycle.admission)).status, "Duplicate")

  const overQuota = PeerRelayStore.Admission.make({
    ...lifecycle.admission,
    relayMessageId: relayMessageId("000000000002"),
    senderSequence: 1,
    messageHash: "message-hash-contract-quota",
    outerEnvelopeDigest: PeerRpc.RelayDigest.make("f".repeat(64))
  })
  const quotaFailure = yield* Effect.flip(store.admit(overQuota))
  assert.strictEqual(quotaFailure.reason._tag, "QuotaExceeded")
  assert.deepStrictEqual(yield* store.usage(), {
    activeCount: 1,
    activeBytes: 1,
    retainedCount: 1,
    retainedBytes: 1
  })

  const firstClaim = claimedMessage(yield* store.claim(lifecycle.claim(1)))
  assert.deepStrictEqual(
    yield* store.loadClaimedPayload(PeerRelayStore.LoadClaimedPayloadRequest.make({
      rowId: firstClaim.rowId,
      channel: firstClaim.channel,
      relayMessageId: firstClaim.relayMessageId,
      claimToken: firstClaim.claimToken,
      sessionGeneration: firstClaim.sessionGeneration,
      payloadBytes: firstClaim.payloadBytes
    })),
    lifecycle.admission.payload
  )
  const firstTerminal = terminalRequest(firstClaim)
  const released = yield* store.release(PeerRelayStore.ReleaseRequest.make({
    channel: firstClaim.channel,
    relayMessageId: firstClaim.relayMessageId,
    claimToken: firstClaim.claimToken,
    sessionGeneration: firstClaim.sessionGeneration
  }))
  assert.strictEqual(released.status, "Changed")
  assert.strictEqual(released.lane, "Retry")
  yield* sql`UPDATE effect_local_relay_messages
    SET next_eligible_at = 0
    WHERE message_id = ${firstClaim.rowId}`

  const retryResult = yield* store.claim(lifecycle.claim(2))
  assert.strictEqual(retryResult.lane, "Retry")
  const retryClaim = claimedMessage(retryResult)
  assert.strictEqual((yield* store.acknowledge(firstTerminal)).status, "Stale")
  const retryTerminal = terminalRequest(retryClaim)
  assert.strictEqual((yield* store.acknowledge(retryTerminal)).status, "Changed")
  assert.strictEqual((yield* store.acknowledge(retryTerminal)).status, "Duplicate")

  const reopened = yield* SqlPeerRelayStore.make
  assert.deepStrictEqual(yield* reopened.usage(), {
    activeCount: 0,
    activeBytes: 0,
    retainedCount: 1,
    retainedBytes: 1
  })

  const rejectedFixture = makeFixture(3)
  yield* store.admit(rejectedFixture.admission)
  const rejectedClaim = claimedMessage(yield* store.claim(rejectedFixture.claim(1)))
  const rejection = PeerRelayStore.RejectRequest.make({
    ...terminalRequest(rejectedClaim),
    reason: "ApplicationRejected"
  })
  assert.strictEqual((yield* store.reject(rejection)).status, "Changed")
  assert.strictEqual((yield* store.reject(rejection)).status, "Duplicate")
  const reopenedAfterReject = yield* SqlPeerRelayStore.make
  assert.deepStrictEqual(yield* reopenedAfterReject.usage(), {
    activeCount: 0,
    activeBytes: 0,
    retainedCount: 2,
    retainedBytes: 2
  })

  const recoveredFixture = makeFixture(4)
  yield* store.admit(recoveredFixture.admission)
  const abandoned = claimedMessage(yield* store.claim(recoveredFixture.claim(1)))
  yield* sql`UPDATE effect_local_relay_messages
    SET claim_deadline = 0
    WHERE message_id = ${abandoned.rowId}`
  yield* sql`UPDATE effect_local_relay_channels
    SET claim_deadline = 0
    WHERE claimed_message_id = ${abandoned.rowId}`
  assert.strictEqual((yield* store.recover({ batchSize: 1 })).processed, 1)
  yield* sql`UPDATE effect_local_relay_messages
    SET next_eligible_at = 0
    WHERE message_id = ${abandoned.rowId}`
  const recoveredResult = yield* store.claim(recoveredFixture.claim(2))
  assert.strictEqual(recoveredResult.lane, "Retry")
  const recoveredClaim = claimedMessage(recoveredResult)
  assert.strictEqual((yield* store.acknowledge(terminalRequest(recoveredClaim))).status, "Changed")

  const expiringFixture = makeFixture(5)
  yield* store.admit(expiringFixture.admission)
  const expiringClaim = claimedMessage(yield* store.claim(expiringFixture.claim(1)))
  yield* sql`UPDATE effect_local_relay_messages
    SET expires_at = 0
    WHERE message_id = ${expiringClaim.rowId}`
  assert.strictEqual((yield* store.expire({ batchSize: 1 })).processed, 1)
  assert.strictEqual((yield* store.acknowledge(terminalRequest(expiringClaim))).status, "Stale")

  yield* sql`UPDATE effect_local_relay_messages
    SET deduplicate_until = 0
    WHERE message_id = ${rejectedClaim.rowId}`
  assert.strictEqual((yield* store.collect({ batchSize: 1 })).processed, 1)
  assert.strictEqual(
    Exit.isFailure(
      yield* Effect.exit(store.loadClaimedPayload({
        rowId: rejectedClaim.rowId,
        channel: rejectedClaim.channel,
        relayMessageId: rejectedClaim.relayMessageId,
        claimToken: rejectedClaim.claimToken,
        sessionGeneration: rejectedClaim.sessionGeneration,
        payloadBytes: rejectedClaim.payloadBytes
      }))
    ),
    true
  )

  const corruptFixture = makeFixture(6)
  yield* store.admit(corruptFixture.admission)
  yield* sql`UPDATE effect_local_relay_messages
    SET tenant_id = ${"corrupt-contract-tenant"}
    WHERE relay_message_id = ${corruptFixture.admission.relayMessageId}`
  const repaired = yield* store.repair({ batchSize: 100 })
  assert.strictEqual(repaired.processed > 0, true)
  assert.strictEqual(
    Option.isNone((yield* store.claim(corruptFixture.claim(1))).message),
    true
  )

  const reconciled = yield* store.reconcile({ batchSize: 1 })
  assert.strictEqual(reconciled.processed, 1)
  assert.strictEqual(reconciled.hasMore, true)
})
