import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as RelayInboxStore from "../src/RelayInboxStore.js"
import * as SqlRelayInboxStore from "../src/SqlRelayInboxStore.js"

const peer = (value: string) => Identity.PeerId.make(`peer_00000000-0000-4000-8000-${value}`)
const relayId = (value: string) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${value}`)
const documentId = (value: string) => Identity.DocumentId.make(`doc_00000000-0000-4000-8000-${value}`)

const layer = SqlRelayInboxStore.layer.pipe(
  Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
  Layer.provide(NodeCrypto.layer),
  Layer.orDie
)

const quota = { maxPendingMessages: 100, maxPendingBytes: 10_000_000 }

const channel = (
  options?: { readonly epoch?: string | undefined; readonly subject?: string | undefined }
) => ({
  tenantId: "tenant-a",
  senderSubjectId: options?.subject ?? "sender-a",
  senderPeerId: peer("00000000aaa1"),
  senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
  senderConnectionEpoch: options?.epoch ?? "epoch-1"
})

const envelope = (options: {
  readonly id: string
  readonly sequence: number
  readonly digest?: string
  readonly epoch?: string
  readonly subject?: string
}) => {
  const source = channel({ epoch: options.epoch, subject: options.subject })
  return {
    relayMessageId: relayId(options.id),
    relayPeerId: peer("00000000ffff"),
    sender: {
      tenantId: source.tenantId,
      subjectId: source.senderSubjectId,
      peerId: source.senderPeerId,
      replicaIncarnation: source.senderReplicaIncarnation,
      connectionEpoch: source.senderConnectionEpoch,
      sequence: options.sequence
    },
    recipient: {
      tenantId: "tenant-a",
      subjectId: "recipient-a",
      peerId: peer("00000000bbb1")
    },
    payloadVersion: 1 as const,
    document: { documentId: documentId("00000000dddd"), documentType: "note" },
    writerProvenance: [],
    messageHash: "a".repeat(64),
    outerEnvelopeDigest: (options.digest ?? "b").repeat(64).slice(0, 64),
    payload: new Uint8Array([1, 2, 3])
  }
}

const admission = (options: {
  readonly inboxKey: string
  readonly id: string
  readonly sequence: number
  readonly now: number
  readonly ttl?: number
  readonly horizon?: number
  readonly digest?: string
  readonly epoch?: string
  readonly subject?: string
  readonly quota?: RelayInboxStore.AdmissionQuota
}) => ({
  inboxKey: options.inboxKey,
  channel: channel({ epoch: options.epoch, subject: options.subject }),
  envelope: envelope(options),
  now: options.now,
  messageTtlMillis: options.ttl ?? 1_000,
  senderRetryHorizonMillis: options.horizon ?? 1_000,
  quota: options.quota ?? quota
})

describe("RelayInboxStore", () => {
  it.effect("admits a message and reports it as the pending head", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      const result = yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 }))
      assert.strictEqual(result._tag, "Admitted")

      const heads = yield* store.pendingHeads("a", { limit: 10 })
      assert.strictEqual(heads.length, 1)
      assert.strictEqual(heads[0]!.relayMessageId, relayId("000000000001"))
    }).pipe(Effect.provide(layer)))

  it.effect("reports a replay of the same identity and digest as a duplicate", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 }))
      const replay = yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 1 }))

      assert.strictEqual(replay._tag, "Duplicate")
      assert.strictEqual(replay._tag === "Duplicate" ? replay.state : "", "Pending")
      const heads = yield* store.pendingHeads("a", { limit: 10 })
      assert.strictEqual(heads.length, 1, "a replay must not create a second row")
    }).pipe(Effect.provide(layer)))

  it.effect("rejects the same identity carrying a different envelope digest", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 }))
      const conflict = yield* store.admit(
        admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 1, digest: "c" })
      )
      assert.strictEqual(conflict._tag, "Conflict")
    }).pipe(Effect.provide(layer)))

  it.effect("refuses a message whose channel disagrees with its envelope", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      const request = admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 })
      const forged = {
        ...request,
        channel: { ...request.channel, senderConnectionEpoch: "another-epoch" }
      }
      const result = yield* store.admit(forged)
      assert.strictEqual(
        result._tag,
        "Conflict",
        "a fabricated channel would file the message under an ordering stream it does not belong to"
      )
    }).pipe(Effect.provide(layer)))

  it.effect("expiring one inbox leaves another inbox's copy of the same identity untouched", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      // The same relay message id addressed to two devices. The key is (inbox, id), so a sweep
      // that filters on the id alone would reach across inboxes.
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0, ttl: 1_000 }))
      yield* store.admit(
        admission({ inboxKey: "b", id: "000000000001", sequence: 0, now: 100_000, ttl: 1_000_000 })
      )

      const expired = yield* store.expire({ now: 5_000, limit: 10, terminalRetentionMillis: 1_000 })
      assert.strictEqual(expired, 1)

      const stale = yield* store.pendingHeads("a", { limit: 10 })
      assert.strictEqual(stale.length, 0, "the overdue message is expired")
      const live = yield* store.pendingHeads("b", { limit: 10 })
      assert.strictEqual(live.length, 1, "the other inbox still holds a message with time left")
    }).pipe(Effect.provide(layer)))

  it.effect("delivers one head per channel and does not starve a channel with high sequences", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      // `senderSequence` restarts per connection epoch, so it cannot order heads across channels.
      yield* store.admit(
        admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0, subject: "sender-a" })
      )
      yield* store.admit(
        admission({ inboxKey: "a", id: "000000000002", sequence: 1, now: 1, subject: "sender-b" })
      )
      yield* store.admit(
        admission({ inboxKey: "a", id: "000000000003", sequence: 500, now: 2, subject: "sender-c" })
      )

      const heads = yield* store.pendingHeads("a", { limit: 2 })
      assert.strictEqual(heads.length, 2)

      // Oldest waiting head first, so every channel is reachable regardless of its own numbering.
      const all = yield* store.pendingHeads("a", { limit: 10 })
      assert.deepStrictEqual(
        all.map((head) => head.channel.senderSubjectId).toSorted(),
        ["sender-a", "sender-b", "sender-c"]
      )
    }).pipe(Effect.provide(layer)))

  it.effect("orders within a channel by sender sequence", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000002", sequence: 5, now: 0 }))
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 1, now: 1 }))

      const heads = yield* store.pendingHeads("a", { limit: 10 })
      assert.strictEqual(heads.length, 1, "one head per channel")
      assert.strictEqual(heads[0]!.relayMessageId, relayId("000000000001"))
    }).pipe(Effect.provide(layer)))

  it.effect("spends the full delivery budget before dead lettering", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 }))

      const first = yield* store.recordDelivery("a", relayId("000000000001"), {
        maxDeliveries: 1,
        now: 1
      })
      assert.strictEqual(
        first._tag,
        "Recorded",
        "the first delivery of a budget of one must be allowed to be settled"
      )

      const second = yield* store.recordDelivery("a", relayId("000000000001"), {
        maxDeliveries: 1,
        now: 2
      })
      assert.strictEqual(second._tag, "DeadLettered")

      const heads = yield* store.pendingHeads("a", { limit: 10 })
      assert.strictEqual(heads.length, 0, "a dead lettered message stops blocking its channel")
    }).pipe(Effect.provide(layer)))

  it.effect("makes an abandoned message answerable after the fact", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 }))
      yield* store.recordDelivery("a", relayId("000000000001"), { maxDeliveries: 1, now: 1 })
      yield* store.recordDelivery("a", relayId("000000000001"), { maxDeliveries: 1, now: 2 })

      const abandoned = yield* store.abandoned("a", { limit: 10 })
      assert.strictEqual(abandoned.length, 1)
      assert.strictEqual(abandoned[0]!.state, "DeadLettered")
    }).pipe(Effect.provide(layer)))

  it.effect("settles on acknowledgement and does not redeliver", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 }))

      const settled = yield* store.settle("a", relayId("000000000001"), {
        outcome: "Acknowledged",
        messageHash: "a".repeat(64),
        now: 10,
        terminalRetentionMillis: 1_000
      })
      assert.strictEqual(settled, "Settled")

      const heads = yield* store.pendingHeads("a", { limit: 10 })
      assert.strictEqual(heads.length, 0)
    }).pipe(Effect.provide(layer)))

  it.effect("refuses a settlement whose content does not match", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 }))

      const settled = yield* store.settle("a", relayId("000000000001"), {
        outcome: "Acknowledged",
        messageHash: "f".repeat(64),
        now: 10,
        terminalRetentionMillis: 1_000
      })
      assert.strictEqual(settled, "HashMismatch")

      const heads = yield* store.pendingHeads("a", { limit: 10 })
      assert.strictEqual(heads.length, 1, "the message stays deliverable")
    }).pipe(Effect.provide(layer)))

  it.effect("reports a settlement that no longer applies", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 }))
      const options = {
        outcome: "Acknowledged" as const,
        messageHash: "a".repeat(64),
        now: 10,
        terminalRetentionMillis: 1_000
      }
      yield* store.settle("a", relayId("000000000001"), options)
      const again = yield* store.settle("a", relayId("000000000001"), options)
      assert.strictEqual(again, "NotPending")
    }).pipe(Effect.provide(layer)))

  it.effect("still deduplicates a replay that arrives after settlement", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(
        admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0, horizon: 100_000 })
      )
      yield* store.settle("a", relayId("000000000001"), {
        outcome: "Acknowledged",
        messageHash: "a".repeat(64),
        now: 10,
        terminalRetentionMillis: 1_000
      })

      const replay = yield* store.admit(
        admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 20, horizon: 100_000 })
      )
      assert.strictEqual(replay._tag, "Duplicate")
      const heads = yield* store.pendingHeads("a", { limit: 10 })
      assert.strictEqual(heads.length, 0, "an acknowledged message must not become deliverable again")
    }).pipe(Effect.provide(layer)))

  it.effect("revives a message the inbox had given up on rather than telling the sender it landed", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0, ttl: 1_000 }))
      yield* store.expire({ now: 5_000, limit: 10, terminalRetentionMillis: 1_000 })

      // The sender still holds custody and is replaying. Reporting a duplicate would make it drop
      // the last copy of a message this inbox can no longer deliver.
      const replay = yield* store.admit(
        admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 6_000, ttl: 1_000 })
      )
      assert.strictEqual(replay._tag, "Admitted")

      const heads = yield* store.pendingHeads("a", { limit: 10 })
      assert.strictEqual(heads.length, 1, "the revived message is deliverable again")
    }).pipe(Effect.provide(layer)))

  it.effect("charges a revived message against the inbox quota", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0, ttl: 1_000 }))
      yield* store.expire({ now: 5_000, limit: 10, terminalRetentionMillis: 1_000 })

      const revived = yield* store.admit(admission({
        inboxKey: "a",
        id: "000000000001",
        sequence: 0,
        now: 6_000,
        ttl: 1_000,
        quota: { maxPendingMessages: 0, maxPendingBytes: 0 }
      }))
      assert.strictEqual(
        revived._tag,
        "QuotaExceeded",
        "reviving creates pending work, so it cannot bypass the cap a first admission obeys"
      )
    }).pipe(Effect.provide(layer)))

  it.effect("keeps a revived message deduplicated for the sender's replay window", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(
        admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0, ttl: 1_000, horizon: 1_000 })
      )
      yield* store.expire({ now: 2_000, limit: 10, terminalRetentionMillis: 1_000 })
      yield* store.admit(
        admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 2_000, ttl: 1_000, horizon: 10_000 })
      )
      yield* store.expire({ now: 3_100, limit: 10, terminalRetentionMillis: 1_000 })

      // The horizon was extended by the revive, so collection at this point must not remove it.
      const collected = yield* store.collect({ now: 3_200, limit: 10 })
      assert.strictEqual(
        collected,
        0,
        "collecting inside the sender's retry window would let a replay be applied twice"
      )
    }).pipe(Effect.provide(layer)))

  it.effect("removes terminal rows only once their deduplication horizon lapses", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(
        admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0, horizon: 1_000 })
      )
      yield* store.settle("a", relayId("000000000001"), {
        outcome: "Acknowledged",
        messageHash: "a".repeat(64),
        now: 10,
        terminalRetentionMillis: 100
      })

      assert.strictEqual(yield* store.collect({ now: 500, limit: 10 }), 0)
      assert.strictEqual(yield* store.collect({ now: 5_000, limit: 10 }), 1)
    }).pipe(Effect.provide(layer)))

  it.effect("refuses admission beyond the inbox quota", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 }))
      const refused = yield* store.admit(admission({
        inboxKey: "a",
        id: "000000000002",
        sequence: 1,
        now: 1,
        quota: { maxPendingMessages: 1, maxPendingBytes: 10_000_000 }
      }))
      assert.strictEqual(refused._tag, "QuotaExceeded")
    }).pipe(Effect.provide(layer)))

  it.effect("counts a replay once against usage", () =>
    Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 0 }))
      yield* store.admit(admission({ inboxKey: "a", id: "000000000001", sequence: 0, now: 1 }))

      const usage = yield* store.usage("a")
      assert.strictEqual(usage.pendingCount, 1, "quota is derived from rows, so replays cannot inflate it")
    }).pipe(Effect.provide(layer)))
})
