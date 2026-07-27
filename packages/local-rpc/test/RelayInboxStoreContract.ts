import { assert } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as RelayInboxStore from "../src/RelayInboxStore.js"

/**
 * The `RelayInboxStore` contract, run unchanged against every dialect the relay supports.
 *
 * A single dialect is not enough to establish this contract. The PostgreSQL driver returns
 * `BIGINT`, `COUNT` and `SUM` as strings with no default parser, which made every numeric column
 * fail to decode and left the store non-functional on that dialect while SQLite stayed green; and
 * MySQL compares identity columns case-insensitively unless they carry a binary collation, which
 * caused real route crossover. Both are invisible to a SQLite-only suite.
 *
 * Every check owns a distinct `inboxKey`, because `expire` and `collect` are deployment-wide sweeps
 * rather than per-inbox operations and the dialect containers are shared across the whole block.
 * For the same reason no check asserts a sweep's returned count: that count spans other checks'
 * leftovers. Each asserts the effect on its own inbox instead, which is the real guarantee anyway.
 */

const peer = (value: string) => Identity.PeerId.make(`peer_00000000-0000-4000-8000-${value}`)
const relayId = (value: string) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${value}`)
const documentId = (value: string) => Identity.DocumentId.make(`doc_00000000-0000-4000-8000-${value}`)

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

export interface ContractCheck {
  readonly name: string
  readonly run: Effect.Effect<void, ReplicaError.ReplicaError, RelayInboxStore.RelayInboxStore>
}

export const relayInboxStoreContract: ReadonlyArray<ContractCheck> = [
  {
    name: "admits a message and reports it as the pending head",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      const result = yield* store.admit(admission({ inboxKey: "admit", id: "000000000001", sequence: 0, now: 0 }))
      assert.strictEqual(result._tag, "Admitted")

      const heads = yield* store.pendingHeads("admit", { limit: 10 })
      assert.strictEqual(heads.length, 1)
      assert.strictEqual(heads[0]!.relayMessageId, relayId("000000000001"))
    })
  },
  {
    name: "reports a replay of the same identity and digest as a duplicate",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "replay", id: "000000000001", sequence: 0, now: 0 }))
      const replay = yield* store.admit(admission({ inboxKey: "replay", id: "000000000001", sequence: 0, now: 1 }))

      assert.strictEqual(replay._tag, "Duplicate")
      assert.strictEqual(replay._tag === "Duplicate" ? replay.state : "", "Pending")
      const heads = yield* store.pendingHeads("replay", { limit: 10 })
      assert.strictEqual(heads.length, 1, "a replay must not create a second row")
    })
  },
  {
    name: "rejects the same identity carrying a different envelope digest",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "conflict", id: "000000000001", sequence: 0, now: 0 }))
      const conflict = yield* store.admit(
        admission({ inboxKey: "conflict", id: "000000000001", sequence: 0, now: 1, digest: "c" })
      )
      assert.strictEqual(conflict._tag, "Conflict")
    })
  },
  {
    name: "refuses a message whose channel disagrees with its envelope",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      const request = admission({ inboxKey: "forged", id: "000000000001", sequence: 0, now: 0 })
      const result = yield* store.admit({
        ...request,
        channel: { ...request.channel, senderConnectionEpoch: "another-epoch" }
      })
      assert.strictEqual(
        result._tag,
        "Conflict",
        "a fabricated channel would file the message under an ordering stream it does not belong to"
      )
    })
  },
  {
    name: "expiring one inbox leaves another inbox's copy of the same identity untouched",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      // The same relay message id addressed to two devices. The key is (inbox, id), so a sweep that
      // filters on the id alone would reach across inboxes.
      yield* store.admit(
        admission({ inboxKey: "sweep-overdue", id: "000000000001", sequence: 0, now: 1_000_000, ttl: 1_000 })
      )
      yield* store.admit(
        admission({ inboxKey: "sweep-live", id: "000000000001", sequence: 0, now: 1_000_000, ttl: 10_000_000 })
      )

      yield* store.expire({ now: 1_005_000, limit: 1_000, terminalRetentionMillis: 1_000 })

      assert.strictEqual(
        (yield* store.pendingHeads("sweep-overdue", { limit: 10 })).length,
        0,
        "the overdue message is expired"
      )
      assert.strictEqual(
        (yield* store.pendingHeads("sweep-live", { limit: 10 })).length,
        1,
        "the other inbox still holds a message with time left"
      )
    })
  },
  {
    name: "delivers one head per channel and does not starve a channel with high sequences",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      // `senderSequence` restarts per connection epoch, so it cannot order heads across channels.
      yield* store.admit(
        admission({ inboxKey: "heads", id: "000000000001", sequence: 0, now: 0, subject: "sender-a" })
      )
      yield* store.admit(
        admission({ inboxKey: "heads", id: "000000000002", sequence: 1, now: 1, subject: "sender-b" })
      )
      yield* store.admit(
        admission({ inboxKey: "heads", id: "000000000003", sequence: 500, now: 2, subject: "sender-c" })
      )

      assert.strictEqual((yield* store.pendingHeads("heads", { limit: 2 })).length, 2)

      const all = yield* store.pendingHeads("heads", { limit: 10 })
      assert.deepStrictEqual(
        all.map((head) => head.channel.senderSubjectId).toSorted(),
        ["sender-a", "sender-b", "sender-c"]
      )
    })
  },
  {
    name: "orders within a channel by sender sequence",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "order", id: "000000000002", sequence: 5, now: 0 }))
      yield* store.admit(admission({ inboxKey: "order", id: "000000000001", sequence: 1, now: 1 }))

      const heads = yield* store.pendingHeads("order", { limit: 10 })
      assert.strictEqual(heads.length, 1, "one head per channel")
      assert.strictEqual(heads[0]!.relayMessageId, relayId("000000000001"))
    })
  },
  {
    name: "spends the full delivery budget before dead lettering",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "budget", id: "000000000001", sequence: 0, now: 0 }))

      const first = yield* store.recordDelivery("budget", relayId("000000000001"), {
        maxDeliveries: 1,
        now: 1
      })
      assert.strictEqual(
        first._tag,
        "Recorded",
        "the first delivery of a budget of one must be allowed to be settled"
      )

      const second = yield* store.recordDelivery("budget", relayId("000000000001"), {
        maxDeliveries: 1,
        now: 2
      })
      assert.strictEqual(second._tag, "DeadLettered")

      const heads = yield* store.pendingHeads("budget", { limit: 10 })
      assert.strictEqual(heads.length, 0, "a dead lettered message stops blocking its channel")
    })
  },
  {
    name: "makes an abandoned message answerable after the fact",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "abandoned", id: "000000000001", sequence: 0, now: 0 }))
      yield* store.recordDelivery("abandoned", relayId("000000000001"), { maxDeliveries: 1, now: 1 })
      yield* store.recordDelivery("abandoned", relayId("000000000001"), { maxDeliveries: 1, now: 2 })

      const abandoned = yield* store.abandoned("abandoned", { limit: 10 })
      assert.strictEqual(abandoned.length, 1)
      assert.strictEqual(abandoned[0]!.state, "DeadLettered")
    })
  },
  {
    name: "settles on acknowledgement and does not redeliver",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "ack", id: "000000000001", sequence: 0, now: 0 }))

      const settled = yield* store.settle("ack", relayId("000000000001"), {
        outcome: "Acknowledged",
        messageHash: "a".repeat(64),
        now: 10,
        terminalRetentionMillis: 1_000
      })
      assert.strictEqual(settled, "Settled")
      assert.strictEqual((yield* store.pendingHeads("ack", { limit: 10 })).length, 0)
    })
  },
  {
    name: "refuses a settlement whose content does not match",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "hash", id: "000000000001", sequence: 0, now: 0 }))

      const settled = yield* store.settle("hash", relayId("000000000001"), {
        outcome: "Acknowledged",
        messageHash: "f".repeat(64),
        now: 10,
        terminalRetentionMillis: 1_000
      })
      assert.strictEqual(settled, "HashMismatch")
      assert.strictEqual(
        (yield* store.pendingHeads("hash", { limit: 10 })).length,
        1,
        "the message stays deliverable"
      )
    })
  },
  {
    name: "reports a settlement that no longer applies",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "resettle", id: "000000000001", sequence: 0, now: 0 }))
      const options = {
        outcome: "Acknowledged" as const,
        messageHash: "a".repeat(64),
        now: 10,
        terminalRetentionMillis: 1_000
      }
      yield* store.settle("resettle", relayId("000000000001"), options)
      const again = yield* store.settle("resettle", relayId("000000000001"), options)
      assert.strictEqual(again, "NotPending")
    })
  },
  {
    name: "still deduplicates a replay that arrives after settlement",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(
        admission({ inboxKey: "dedupe", id: "000000000001", sequence: 0, now: 0, horizon: 100_000 })
      )
      yield* store.settle("dedupe", relayId("000000000001"), {
        outcome: "Acknowledged",
        messageHash: "a".repeat(64),
        now: 10,
        terminalRetentionMillis: 1_000
      })

      const replay = yield* store.admit(
        admission({ inboxKey: "dedupe", id: "000000000001", sequence: 0, now: 20, horizon: 100_000 })
      )
      assert.strictEqual(replay._tag, "Duplicate")
      assert.strictEqual(
        (yield* store.pendingHeads("dedupe", { limit: 10 })).length,
        0,
        "an acknowledged message must not become deliverable again"
      )
    })
  },
  {
    name: "revives a message the inbox had given up on rather than telling the sender it landed",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(
        admission({ inboxKey: "revive", id: "000000000001", sequence: 0, now: 2_000_000, ttl: 1_000 })
      )
      yield* store.expire({ now: 2_005_000, limit: 1_000, terminalRetentionMillis: 1_000 })

      // The sender still holds custody and is replaying. Reporting a duplicate would make it drop
      // the last copy of a message this inbox can no longer deliver.
      const replay = yield* store.admit(
        admission({ inboxKey: "revive", id: "000000000001", sequence: 0, now: 2_006_000, ttl: 1_000 })
      )
      assert.strictEqual(replay._tag, "Admitted")
      assert.strictEqual(
        (yield* store.pendingHeads("revive", { limit: 10 })).length,
        1,
        "the revived message is deliverable again"
      )
    })
  },
  {
    name: "charges a revived message against the inbox quota",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(
        admission({ inboxKey: "revive-quota", id: "000000000001", sequence: 0, now: 3_000_000, ttl: 1_000 })
      )
      yield* store.expire({ now: 3_005_000, limit: 1_000, terminalRetentionMillis: 1_000 })

      const revived = yield* store.admit(admission({
        inboxKey: "revive-quota",
        id: "000000000001",
        sequence: 0,
        now: 3_006_000,
        ttl: 1_000,
        quota: { maxPendingMessages: 0, maxPendingBytes: 0 }
      }))
      assert.strictEqual(
        revived._tag,
        "QuotaExceeded",
        "reviving creates pending work, so it cannot bypass the cap a first admission obeys"
      )
    })
  },
  {
    name: "charges a revived message against the inbox byte quota",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(
        admission({ inboxKey: "revive-bytes", id: "000000000001", sequence: 0, now: 3_100_000, ttl: 1_000 })
      )
      yield* store.expire({ now: 3_105_000, limit: 1_000, terminalRetentionMillis: 1_000 })

      // The message cap alone is generous, so only the byte cap can refuse this. A revive restores
      // the row's stored bytes to pending, so a sender replaying identities the inbox already gave
      // up on would otherwise be bounded only by the message count times the maximum payload —
      // which is the number the byte cap exists to be lower than.
      const revived = yield* store.admit(admission({
        inboxKey: "revive-bytes",
        id: "000000000001",
        sequence: 0,
        now: 3_106_000,
        ttl: 1_000,
        quota: { maxPendingMessages: 100, maxPendingBytes: 1 }
      }))
      assert.strictEqual(revived._tag, "QuotaExceeded")
    })
  },
  {
    name: "keeps a revived message deduplicated for the sender's replay window",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(
        admission({
          inboxKey: "revive-horizon",
          id: "000000000001",
          sequence: 0,
          now: 4_000_000,
          ttl: 1_000,
          horizon: 1_000
        })
      )
      yield* store.expire({ now: 4_002_000, limit: 1_000, terminalRetentionMillis: 1_000 })
      yield* store.admit(
        admission({
          inboxKey: "revive-horizon",
          id: "000000000001",
          sequence: 0,
          now: 4_002_000,
          ttl: 1_000,
          horizon: 10_000
        })
      )
      yield* store.expire({ now: 4_003_100, limit: 1_000, terminalRetentionMillis: 1_000 })

      // The horizon was extended by the revive, so collection at this point must not remove it.
      yield* store.collect({ now: 4_003_200, limit: 1_000 })
      assert.strictEqual(
        (yield* store.usage("revive-horizon")).retainedCount,
        1,
        "collecting inside the sender's retry window would let a replay be applied twice"
      )
    })
  },
  {
    name: "removes terminal rows only once their deduplication horizon lapses",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(
        admission({ inboxKey: "collect", id: "000000000001", sequence: 0, now: 5_000_000, horizon: 1_000 })
      )
      yield* store.settle("collect", relayId("000000000001"), {
        outcome: "Acknowledged",
        messageHash: "a".repeat(64),
        now: 5_000_010,
        terminalRetentionMillis: 100
      })

      yield* store.collect({ now: 5_000_500, limit: 1_000 })
      assert.strictEqual(
        (yield* store.usage("collect")).retainedCount,
        1,
        "the identity is retained while its sender may still replay it"
      )

      yield* store.collect({ now: 5_005_000, limit: 1_000 })
      assert.strictEqual((yield* store.usage("collect")).retainedCount, 0)
    })
  },
  {
    name: "refuses admission beyond the inbox quota",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "quota", id: "000000000001", sequence: 0, now: 0 }))
      const refused = yield* store.admit(admission({
        inboxKey: "quota",
        id: "000000000002",
        sequence: 1,
        now: 1,
        quota: { maxPendingMessages: 1, maxPendingBytes: 10_000_000 }
      }))
      assert.strictEqual(refused._tag, "QuotaExceeded")
    })
  },
  {
    name: "counts a replay once against usage",
    run: Effect.gen(function*() {
      const store = yield* RelayInboxStore.RelayInboxStore
      yield* store.admit(admission({ inboxKey: "usage", id: "000000000001", sequence: 0, now: 0 }))
      yield* store.admit(admission({ inboxKey: "usage", id: "000000000001", sequence: 0, now: 1 }))

      const usage = yield* store.usage("usage")
      assert.strictEqual(usage.pendingCount, 1, "quota is derived from rows, so replays cannot inflate it")
    })
  }
]
