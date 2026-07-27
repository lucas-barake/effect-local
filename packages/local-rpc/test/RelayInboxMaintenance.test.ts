import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Clock from "effect/Clock"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { TestClock } from "effect/testing"
import * as MessageStorage from "effect/unstable/cluster/MessageStorage"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as RelayInboxMaintenance from "../src/RelayInboxMaintenance.js"
import * as RelayInboxStore from "../src/RelayInboxStore.js"
import * as SqlRelayInboxStore from "../src/SqlRelayInboxStore.js"

const peer = (value: string) => Identity.PeerId.make(`peer_00000000-0000-4000-8000-${value}`)
const relayId = (value: string) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${value}`)
const documentId = (value: string) => Identity.DocumentId.make(`doc_00000000-0000-4000-8000-${value}`)

const inboxKey = "inbox-a"

const options: RelayInboxMaintenance.Options = {
  interval: Duration.seconds(1),
  batchLimit: 100,
  terminalRetention: Duration.seconds(10),
  enabled: true
}

const TestShardingConfig = ShardingConfig.layer({
  entityTerminationTimeout: 0,
  entityMessagePollInterval: 5000,
  sendRetryInterval: 100
})

const store = SqlRelayInboxStore.layer.pipe(
  Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
  Layer.provide(NodeCrypto.layer),
  Layer.orDie
)

const relay = (maintenance: RelayInboxMaintenance.Options) =>
  RelayInboxMaintenance.layer(maintenance).pipe(
    Layer.provideMerge(Sharding.layer),
    Layer.provide(Runners.layerNoop),
    Layer.provideMerge(MessageStorage.layerMemory),
    Layer.provide(RunnerStorage.layerMemory),
    Layer.provide(RunnerHealth.layerNoop),
    Layer.provide(TestShardingConfig),
    Layer.provideMerge(store)
  )

const channel = {
  tenantId: "tenant-a",
  senderSubjectId: "sender-a",
  senderPeerId: peer("00000000aaa1"),
  senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
  senderConnectionEpoch: "epoch-1"
}

const admission = (now: number, ttl: number) => ({
  inboxKey,
  channel,
  envelope: {
    relayMessageId: relayId("000000000001"),
    relayPeerId: peer("00000000ffff"),
    sender: {
      tenantId: channel.tenantId,
      subjectId: channel.senderSubjectId,
      peerId: channel.senderPeerId,
      replicaIncarnation: channel.senderReplicaIncarnation,
      connectionEpoch: channel.senderConnectionEpoch,
      sequence: 0
    },
    recipient: { tenantId: "tenant-a", subjectId: "recipient-a", peerId: peer("00000000bbb1") },
    payloadVersion: 1 as const,
    document: { documentId: documentId("00000000dddd"), documentType: "note" },
    writerProvenance: [],
    messageHash: "a".repeat(64),
    outerEnvelopeDigest: "b".repeat(64),
    payload: new Uint8Array([1, 2, 3])
  },
  now,
  messageTtlMillis: ttl,
  senderRetryHorizonMillis: 1_000,
  quota: { maxPendingMessages: 100, maxPendingBytes: 10_000_000 }
})

describe("RelayInboxMaintenance", () => {
  it.effect("expires overdue messages and later collects them, with no inbox connected", () =>
    Effect.gen(function*() {
      // Shard acquisition has to complete before the singleton is started by its owner.
      yield* TestClock.adjust(5_000)
      const inbox = yield* RelayInboxStore.RelayInboxStore

      const now = yield* Clock.currentTimeMillis
      yield* inbox.admit(admission(now, 2_000))
      assert.strictEqual((yield* inbox.pendingHeads(inboxKey, { limit: 10 })).length, 1)

      // Nothing is subscribed to this inbox and its entity is passivated, which is precisely the
      // state in which TTL matters: only a cluster-wide owner can sweep it.
      yield* TestClock.adjust(4_000)
      assert.strictEqual(
        (yield* inbox.pendingHeads(inboxKey, { limit: 10 })).length,
        0,
        "an overdue message is expired by the singleton"
      )
      const expired = yield* inbox.usage(inboxKey)
      assert.strictEqual(expired.retainedCount, 1, "its identity is retained for the replay window")

      // Past the retention horizon the identity itself is collected, which is what bounds the table.
      yield* TestClock.adjust(20_000)
      const collected = yield* inbox.usage(inboxKey)
      assert.strictEqual(collected.retainedCount, 0)
    }).pipe(Effect.provide(relay(options))))

  it.effect("sweeps nothing when retention is disabled", () =>
    Effect.gen(function*() {
      yield* TestClock.adjust(5_000)
      const inbox = yield* RelayInboxStore.RelayInboxStore

      const now = yield* Clock.currentTimeMillis
      yield* inbox.admit(admission(now, 2_000))
      yield* TestClock.adjust(30_000)

      // The flag is load bearing in both directions: a deployment that did not ask for retention
      // must not have its durable rows deleted on a schedule it never chose.
      assert.strictEqual(
        (yield* inbox.pendingHeads(inboxKey, { limit: 10 })).length,
        1,
        "no sweep runs when retention is disabled"
      )
    }).pipe(Effect.provide(relay({ ...options, enabled: false }))))
})
