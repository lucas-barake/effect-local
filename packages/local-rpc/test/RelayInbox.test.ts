import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as MessageStorage from "effect/unstable/cluster/MessageStorage"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as PeerRpc from "../src/PeerRpc.js"
import * as RelayInbox from "../src/RelayInbox.js"
import * as RelayInboxStore from "../src/RelayInboxStore.js"
import * as SqlRelayInboxStore from "../src/SqlRelayInboxStore.js"

const peer = (value: string) => Identity.PeerId.make(`peer_00000000-0000-4000-8000-${value}`)
const relayId = (value: string) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${value}`)
const documentId = (value: string) => Identity.DocumentId.make(`doc_00000000-0000-4000-8000-${value}`)
const sessionId = (value: string) => Identity.SessionId.make(`ses_00000000-0000-4000-8000-${value}`)

const inboxKey = "inbox-a"

const baseOptions: RelayInbox.Options = {
  maxDeliveries: 10,
  messageTtlMillis: 600_000,
  terminalRetentionMillis: 600_000,
  sessionDeadlineMillis: 90_000,
  sessionSweepMillis: 1_000,
  maxConcurrentChannels: 4,
  storeRetryMillis: 0,
  maxPendingMessages: 100,
  maxPendingBytes: 10_000_000,
  mailboxCapacity: 16,
  maxIdleTimeMillis: 3_600_000
}

/**
 * The explicit cluster composition rather than `TestRunner.layer`.
 *
 * `TestRunner` bakes `ShardingConfig` defaults that cannot be overridden, and two of them — a 15s
 * entity termination timeout and a 10s message poll interval — stall every test under the virtual
 * clock `it.effect` installs.
 */
const TestShardingConfig = ShardingConfig.layer({
  entityTerminationTimeout: 0,
  entityMessagePollInterval: 5000,
  sendRetryInterval: 100
})

/**
 * The real SQL store over in-memory SQLite, so the entity is exercised against the durability it
 * actually ships with. A hand written in-memory stand-in would let the entity's custody claims pass
 * against semantics no production deployment has.
 */
const sqliteStore = SqlRelayInboxStore.layer.pipe(
  Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
  Layer.provide(NodeCrypto.layer),
  Layer.orDie
)

const relay = (
  options?: {
    readonly entity?: Partial<RelayInbox.Options>
    readonly store?: Layer.Layer<RelayInboxStore.RelayInboxStore>
  }
) =>
  RelayInbox.layer({ ...baseOptions, ...options?.entity }).pipe(
    Layer.provideMerge(Sharding.layer),
    Layer.provide(Runners.layerNoop),
    Layer.provideMerge(MessageStorage.layerMemory),
    Layer.provide(RunnerStorage.layerMemory),
    Layer.provide(RunnerHealth.layerNoop),
    Layer.provide(TestShardingConfig),
    // Merged rather than provided so a test can assert against the same durable rows the entity
    // wrote. Delivery budgets and terminal states are durable guarantees, not internals.
    Layer.provideMerge(options?.store ?? sqliteStore)
  )

const layer = relay()

/**
 * Replaces one store operation with a failure, leaving the rest of the real store intact.
 *
 * Lives here rather than in `src` on purpose: the durable store is the relay's only custody, and a
 * fault injecting one must never be reachable from a published entry point.
 */
const storeFailing = (
  override: (
    real: RelayInboxStore.RelayInboxStore["Service"]
  ) => Partial<RelayInboxStore.RelayInboxStore["Service"]>
) =>
  Layer.effect(RelayInboxStore.RelayInboxStore)(
    Effect.gen(function*() {
      const real = yield* RelayInboxStore.RelayInboxStore
      return RelayInboxStore.RelayInboxStore.of({ ...real, ...override(real) })
    })
  ).pipe(Layer.provide(sqliteStore))

const unavailable = new ReplicaError.ReplicaError({
  reason: new ReplicaError.StorageUnavailable({ cause: new Error("injected") })
})

const channel = (options?: { readonly epoch?: string; readonly subject?: string }) => ({
  tenantId: "tenant-a",
  senderSubjectId: options?.subject ?? "sender-a",
  senderPeerId: peer("00000000aaa1"),
  senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
  senderConnectionEpoch: options?.epoch ?? "epoch-1"
})

interface Message {
  readonly id: string
  readonly sequence: number
  readonly epoch?: string
  readonly subject?: string
}

const deliver = (message: Message) => {
  const source = channel(message)
  return {
    channel: source,
    envelope: {
      relayMessageId: relayId(message.id),
      relayPeerId: peer("00000000ffff"),
      sender: {
        tenantId: source.tenantId,
        subjectId: source.senderSubjectId,
        peerId: source.senderPeerId,
        replicaIncarnation: source.senderReplicaIncarnation,
        connectionEpoch: source.senderConnectionEpoch,
        sequence: message.sequence
      },
      recipient: {
        tenantId: "tenant-a",
        subjectId: "recipient-a",
        peerId: peer("00000000bbb1")
      },
      payloadVersion: 1 as const,
      document: { documentId: documentId("00000000dddd"), documentType: "note" },
      writerProvenance: [],
      messageHash: message.id.padStart(64, "a"),
      outerEnvelopeDigest: message.id.padStart(64, "b"),
      payload: new Uint8Array([1, 2, 3])
    },
    senderRetryHorizonMillis: 60_000
  }
}

const inboxFor = Effect.map(RelayInbox.RelayInbox.client, (make) => make(inboxKey))
type Inbox = Effect.Success<typeof inboxFor>

/**
 * Brings the runner up: shard assignment and acquisition are driven by scheduled fibers, so under
 * the virtual clock no entity is reachable until time is advanced.
 */
const inbox = Effect.gen(function*() {
  yield* TestClock.adjust(5000)
  return yield* inboxFor
})

/** Receives `count` messages and settles each, keeping the session alive across the settlements. */
const receiveAndSettle = (
  client: Inbox,
  session: Identity.SessionId,
  count: number,
  outcome: RelayInboxStore.TerminalOutcome = "Acknowledged"
) =>
  client.Subscribe({ sessionId: session }).pipe(
    Stream.take(count),
    Stream.mapEffect((message) =>
      client.Settle({
        sessionId: session,
        relayMessageId: message.relayMessageId,
        claimToken: message.claimToken,
        messageHash: message.messageHash,
        outcome
      }).pipe(Effect.as(message))
    ),
    Stream.runCollect
  )

/** Receives `count` messages and drops the session without settling any of them. */
const receiveOnly = (client: Inbox, session: Identity.SessionId, count: number) =>
  client.Subscribe({ sessionId: session }).pipe(Stream.take(count), Stream.runCollect)

const ids = (messages: ReadonlyArray<PeerRpc.StoredMessage>) => messages.map((message) => message.relayMessageId)

describe("RelayInbox", () => {
  it.effect("delivers a message admitted while the recipient had no session", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      // Admitted with nobody listening. This is the store-and-forward promise: the sender is never
      // required to overlap with the recipient.
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))

      const received = yield* receiveOnly(client, sessionId("000000000001"), 1)

      assert.deepStrictEqual(ids(received), [relayId("000000000001")])
    }).pipe(Effect.provide(layer)))

  it.effect("redelivers a message whose session dropped without settling it", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))

      const first = yield* receiveOnly(client, sessionId("000000000001"), 1)
      const second = yield* receiveOnly(client, sessionId("000000000002"), 1)

      assert.deepStrictEqual(ids(first), [relayId("000000000001")])
      assert.deepStrictEqual(
        ids(second),
        [relayId("000000000001")],
        "an unsettled message is still this inbox's responsibility"
      )
    }).pipe(Effect.provide(layer)))

  it.effect("treats acknowledgement and rejection as equally terminal", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0, subject: "sender-a" }))
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 0, subject: "sender-b" }))

      const settled = yield* receiveAndSettle(client, sessionId("000000000001"), 1, "Acknowledged").pipe(
        Effect.zipWith(
          receiveAndSettle(client, sessionId("000000000002"), 1, "Rejected"),
          (first, second) => [...first, ...second]
        )
      )
      assert.strictEqual(settled.length, 2)

      const store = yield* RelayInboxStore.RelayInboxStore
      const pending = yield* store.pendingHeads(inboxKey, { limit: 10 })
      assert.strictEqual(
        pending.length,
        0,
        "the recipient made a durable decision either way, so neither is redelivered"
      )
    }).pipe(Effect.provide(layer)))

  it.effect("admits nothing when the durable write fails", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      // The at-least-once guarantee rests entirely on this seam: `Deliver` is deliberately not a
      // persisted cluster message, so a write that did not land must surface as a failed rpc and
      // leave the sender holding the only copy.
      const exit = yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 })).pipe(Effect.exit)
      assert.isTrue(exit._tag === "Failure")

      const store = yield* RelayInboxStore.RelayInboxStore
      const pending = yield* store.pendingHeads(inboxKey, { limit: 10 })
      assert.strictEqual(pending.length, 0, "a failed admission must leave no durable trace")
    }).pipe(Effect.provide(relay({
      store: storeFailing(() => ({ admit: () => Effect.fail(unavailable) }))
    }))))

  it.effect("does not let one unsettled channel block another", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0, subject: "sender-a" }))
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 0, subject: "sender-b" }))

      // Neither is settled. A single ordered queue per device would stall on the first.
      const received = yield* receiveOnly(client, sessionId("000000000001"), 2)

      assert.deepStrictEqual(
        ids(received).toSorted(),
        [relayId("000000000001"), relayId("000000000002")].toSorted()
      )
    }).pipe(Effect.provide(layer)))

  it.effect("does not let one connection epoch hold another epoch's stream", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      // Two epochs of the SAME sender, so they differ only by the field the in-memory busy-channel
      // key is most likely to drop. The store partitions heads by epoch, so both are heads at once;
      // if the entity's own key is coarser than the store's, the stale epoch's unsettled head holds
      // the live epoch's entire stream for the life of the session.
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0, epoch: "epoch-1" }))
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 0, epoch: "epoch-2" }))

      const received = yield* receiveOnly(client, sessionId("000000000001"), 2)
      assert.deepStrictEqual(
        ids(received).toSorted(),
        [relayId("000000000001"), relayId("000000000002")].toSorted()
      )
    }).pipe(Effect.provide(layer)))

  it.effect("answers a settle inside its declared failure channel when the session is released", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))

      const session = sessionId("000000000001")
      const delivered = yield* receiveOnly(client, session, 1)

      // Parked on the durable write when its session is released. `Settle` declares a closed error
      // union, so the recipient must be able to discriminate the outcome and retry. Answering with
      // an interrupt escapes that union entirely: it is uncatchable at the front door and takes the
      // peer's acknowledgement fiber down with it instead of producing a retryable failure.
      const settling = yield* Effect.forkChild(
        client.Settle({
          sessionId: session,
          relayMessageId: delivered[0]!.relayMessageId,
          claimToken: delivered[0]!.claimToken,
          messageHash: delivered[0]!.messageHash,
          outcome: "Acknowledged"
          // Covers the complete declared failure channel, so only an out-of-channel outcome fails.
        }).pipe(Effect.catch(() => Effect.void), Effect.exit)
      )
      yield* TestClock.adjust(10)
      yield* TestClock.adjust(baseOptions.sessionDeadlineMillis + baseOptions.sessionSweepMillis * 3)

      const outcome = yield* Fiber.join(settling)
      assert.strictEqual(
        outcome._tag,
        "Success",
        "releasing a session must fail a waiting settle, not interrupt it"
      )
    }).pipe(Effect.provide(relay({
      // Never completes, so the settlement is still in flight when the deadline releases it.
      store: storeFailing(() => ({ settle: () => Effect.never }))
    }))))

  it.effect("keeps a channel in sender sequence order across a redelivery", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 1 }))

      const dropped = yield* receiveOnly(client, sessionId("000000000001"), 1)
      assert.deepStrictEqual(ids(dropped), [relayId("000000000001")], "the head goes first")

      const resumed = yield* receiveAndSettle(client, sessionId("000000000002"), 2)
      assert.deepStrictEqual(
        ids(resumed),
        [relayId("000000000001"), relayId("000000000002")],
        "a redelivery resumes at the head rather than skipping past it"
      )
    }).pipe(Effect.provide(layer)))

  it.effect("orders each connection epoch of one sender by its own sequence", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      // `senderSequence` restarts at zero on every reconnect, so both epochs carry {0, 1}. Treated
      // as one channel, one epoch's run would be ordered behind the other's and a message could be
      // starved. This pins per-epoch order only; that the two epochs are independently deliverable
      // at all is owned by "does not let one connection epoch hold another epoch's stream", because
      // relative order within an epoch survives any interleaving and cannot detect a merged key.
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0, epoch: "epoch-1" }))
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 1, epoch: "epoch-1" }))
      yield* client.Deliver(deliver({ id: "000000000003", sequence: 0, epoch: "epoch-2" }))
      yield* client.Deliver(deliver({ id: "000000000004", sequence: 1, epoch: "epoch-2" }))

      const received = yield* receiveAndSettle(client, sessionId("000000000001"), 4)

      const perEpoch = (epoch: string) =>
        received
          .filter((message) => message.sender.connectionEpoch === epoch)
          .map((message) => message.sender.sequence)
      assert.deepStrictEqual(perEpoch("epoch-1"), [0, 1])
      assert.deepStrictEqual(perEpoch("epoch-2"), [0, 1])
    }).pipe(Effect.provide(layer)))

  it.effect("delivers each message exactly once across a session replacement", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 1 }))
      yield* client.Deliver(deliver({ id: "000000000003", sequence: 2 }))

      const first = yield* receiveAndSettle(client, sessionId("000000000001"), 1)
      const second = yield* receiveAndSettle(client, sessionId("000000000002"), 2)

      const seen = [...ids(first), ...ids(second)]
      assert.deepStrictEqual(
        seen.toSorted(),
        [relayId("000000000001"), relayId("000000000002"), relayId("000000000003")],
        "nothing is stranded by the replacement and nothing is delivered twice"
      )
    }).pipe(Effect.provide(layer)))

  it.effect("leaves no dispatcher behind when a subscribe is interrupted", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 1 }))

      // An interrupt between the session scope being created and the session becoming reachable
      // would strand a dispatcher that no one can close, and it would keep delivering alongside
      // whichever session replaces it.
      for (const attempt of ["000000000001", "000000000002", "000000000003"]) {
        const fiber = yield* Effect.forkChild(
          Stream.runDrain(client.Subscribe({ sessionId: sessionId(attempt) }))
        )
        yield* Fiber.interrupt(fiber)
      }

      const received = yield* receiveAndSettle(client, sessionId("000000000009"), 2)
      assert.deepStrictEqual(
        ids(received),
        [relayId("000000000001"), relayId("000000000002")],
        "an orphaned dispatcher would have consumed or duplicated these"
      )
    }).pipe(Effect.provide(layer)))

  it.effect("releases a session whose liveness deadline lapses", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      const session = sessionId("000000000001")

      const subscriber = yield* Effect.forkChild(
        Stream.runCollect(client.Subscribe({ sessionId: session }))
      )
      yield* TestClock.adjust(10)

      // Nothing heartbeats it, so the entity's own deadline is what bounds it. A cluster cannot
      // rely on disconnects announcing themselves.
      yield* TestClock.adjust(baseOptions.sessionDeadlineMillis + baseOptions.sessionSweepMillis * 2)

      const collected = yield* Fiber.join(subscriber)
      assert.strictEqual(collected.length, 0, "the released session's stream ends")

      const exit = yield* client.Heartbeat({ sessionId: session }).pipe(Effect.exit)
      assert.isTrue(exit._tag === "Failure")
    }).pipe(Effect.provide(layer)))

  it.effect("refuses a settlement carrying a claim token from another attempt", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))

      const session = sessionId("000000000001")
      const outcome = yield* client.Subscribe({ sessionId: session }).pipe(
        Stream.take(1),
        Stream.mapEffect((message) =>
          client.Settle({
            sessionId: session,
            relayMessageId: message.relayMessageId,
            // Minted per delivery attempt, so a stale token is also a replay of an earlier attempt.
            claimToken: PeerRpc.ClaimToken.make("clm_00000000-0000-4000-8000-000000000000"),
            messageHash: message.messageHash,
            outcome: "Acknowledged"
          }).pipe(Effect.exit)
        ),
        Stream.runCollect
      )
      assert.isTrue(outcome[0]!._tag === "Failure")

      const redelivered = yield* receiveOnly(client, sessionId("000000000002"), 1)
      assert.deepStrictEqual(
        ids(redelivered),
        [relayId("000000000001")],
        "a refused settlement leaves the message this inbox's responsibility"
      )
    }).pipe(Effect.provide(layer)))

  it.effect("refuses a settlement whose message hash does not match", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))

      const session = sessionId("000000000001")
      const outcome = yield* client.Subscribe({ sessionId: session }).pipe(
        Stream.take(1),
        Stream.mapEffect((message) =>
          client.Settle({
            sessionId: session,
            relayMessageId: message.relayMessageId,
            claimToken: message.claimToken,
            messageHash: "f".repeat(64),
            outcome: "Acknowledged"
          }).pipe(Effect.exit)
        ),
        Stream.runCollect
      )
      assert.isTrue(outcome[0]!._tag === "Failure")

      const store = yield* RelayInboxStore.RelayInboxStore
      const pending = yield* store.pendingHeads(inboxKey, { limit: 10 })
      assert.strictEqual(pending.length, 1, "the message stays deliverable")
    }).pipe(Effect.provide(layer)))

  it.effect("refuses a settlement presented under a session that is not the live one", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))

      // Presented under a session id that was never opened, while the real session is still live
      // and the delivery in flight. The claim token and hash both match, so the session identity
      // check is the only thing that can refuse it — settling against a replaced session instead
      // would be refused by the stale token and prove nothing about this guard.
      const session = sessionId("000000000001")
      const outcome = yield* client.Subscribe({ sessionId: session }).pipe(
        Stream.take(1),
        Stream.mapEffect((message) =>
          client.Settle({
            sessionId: sessionId("000000000099"),
            relayMessageId: message.relayMessageId,
            claimToken: message.claimToken,
            messageHash: message.messageHash,
            outcome: "Acknowledged"
          }).pipe(Effect.exit)
        ),
        Stream.runCollect
      )
      assert.isTrue(outcome[0]!._tag === "Failure")

      const store = yield* RelayInboxStore.RelayInboxStore
      const pending = yield* store.pendingHeads(inboxKey, { limit: 10 })
      assert.strictEqual(pending.length, 1, "the message stays this inbox's responsibility")
    }).pipe(Effect.provide(layer)))

  it.effect("does not report an acknowledgement the durable write never made", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))

      const session = sessionId("000000000001")
      // The recipient prunes its own relay receipt on a successful acknowledgement, so replying
      // before the row is terminal would leave neither side holding the message.
      const outcome = yield* client.Subscribe({ sessionId: session }).pipe(
        Stream.take(1),
        Stream.mapEffect((message) =>
          client.Settle({
            sessionId: session,
            relayMessageId: message.relayMessageId,
            claimToken: message.claimToken,
            messageHash: message.messageHash,
            outcome: "Acknowledged"
          }).pipe(Effect.exit)
        ),
        Stream.runCollect
      )
      assert.isTrue(outcome[0]!._tag === "Failure")
    }).pipe(Effect.provide(relay({
      store: storeFailing(() => ({ settle: () => Effect.fail(unavailable) }))
    }))))

  it.effect("dead letters a message that exhausts its delivery budget and unblocks its channel", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 1 }))

      // A recipient that reads and disconnects without settling is not a failure at any layer, so
      // without a budget the head would be redelivered forever and its channel would never advance.
      const first = yield* receiveOnly(client, sessionId("000000000001"), 1)
      assert.deepStrictEqual(ids(first), [relayId("000000000001")])

      const second = yield* receiveOnly(client, sessionId("000000000002"), 2)
      assert.deepStrictEqual(
        ids(second),
        [relayId("000000000001"), relayId("000000000002")],
        "the exhausted head is dead lettered and the channel moves on"
      )

      const store = yield* RelayInboxStore.RelayInboxStore
      const abandoned = yield* store.abandoned(inboxKey, { limit: 10 })
      assert.deepStrictEqual(
        abandoned.map((message) => [message.relayMessageId, message.state]),
        [[relayId("000000000001"), "DeadLettered"]],
        "an abandoned message stays answerable, because its sender already released custody"
      )
    }).pipe(Effect.provide(relay({ entity: { maxDeliveries: 1 } }))))

  it.effect("does not charge a delivery the recipient never took", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0, subject: "sender-a" }))

      const session = sessionId("000000000001")
      const received = yield* receiveOnly(client, session, 1)
      assert.deepStrictEqual(ids(received), [relayId("000000000001")])

      // Admitted after the recipient stopped reading. It is prepared for its channel and offered to
      // a rendezvous nobody is taking from, so it never reaches the transport. Charging it here is
      // what would dead letter messages on an ordinary flaky connection.
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 0, subject: "sender-b" }))
      // Hands the prior session's dispatcher a scheduler pass so it really does prepare the message
      // and park on the offer. Nothing is due at this point, so the amount is irrelevant.
      yield* TestClock.adjust(10)

      const store = yield* RelayInboxStore.RelayInboxStore
      const untaken = (yield* store.pendingHeads(inboxKey, { limit: 10 }))
        .find((message) => message.relayMessageId === relayId("000000000002"))
      assert.strictEqual(untaken?.deliveries, 0, "an untransmitted attempt costs the message nothing")

      // The one transmission it ever gets. Asserting the positive as well keeps this test honest if
      // the nudge above ever stops landing: a charge taken while it sat unread shows up as a second.
      const later = yield* receiveOnly(client, sessionId("000000000002"), 2)
      assert.isTrue(ids(later).includes(relayId("000000000002")))

      const transmitted = (yield* store.pendingHeads(inboxKey, { limit: 10 }))
        .find((message) => message.relayMessageId === relayId("000000000002"))
      assert.strictEqual(
        transmitted?.deliveries,
        1,
        "only the transmission that actually reached the recipient is charged"
      )
    }).pipe(Effect.provide(layer)))

  it.effect("charges one delivery per transmission and none for merely reaching the head", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0, subject: "sender-a" }))
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 1, subject: "sender-a" }))
      yield* client.Deliver(deliver({ id: "000000000003", sequence: 0, subject: "sender-b" }))

      // One session, two channels. `sender-a` is stop-and-wait, so its second message never
      // becomes a head while the first is unsettled and must therefore cost nothing.
      const received = yield* receiveOnly(client, sessionId("000000000001"), 2)
      assert.strictEqual(received.length, 2)

      const store = yield* RelayInboxStore.RelayInboxStore
      const pending = yield* store.pendingHeads(inboxKey, { limit: 10 })
      assert.deepStrictEqual(
        pending.map((message) => [message.relayMessageId, message.deliveries]).toSorted(),
        [
          [relayId("000000000001"), 1],
          [relayId("000000000003"), 1]
        ].toSorted(),
        "each transmitted head is charged exactly once"
      )

      const queued = yield* store.usage(inboxKey)
      assert.strictEqual(queued.pendingCount, 3, "the queued message is still waiting behind its head")

      // `pendingHeads` returns one row per channel, so the queued message is structurally absent
      // from the assertion above and its count went unobserved. Settling the head promotes it, and
      // only then can "none for merely reaching the head" actually be checked.
      yield* store.settle(inboxKey, relayId("000000000001"), {
        outcome: "Acknowledged",
        messageHash: "000000000001".padStart(64, "a"),
        now: 1_000,
        terminalRetentionMillis: baseOptions.terminalRetentionMillis
      })
      const promoted = (yield* store.pendingHeads(inboxKey, { limit: 10 }))
        .find((message) => message.relayMessageId === relayId("000000000002"))
      assert.strictEqual(
        promoted?.deliveries,
        0,
        "a message queued behind its channel head is never charged while it waits"
      )
    }).pipe(Effect.provide(layer)))
})
