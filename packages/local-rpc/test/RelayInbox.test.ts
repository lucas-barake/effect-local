import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as MessageStorage from "effect/unstable/cluster/MessageStorage"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
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
  messageTtl: Duration.minutes(10),
  terminalRetention: Duration.minutes(10),
  sessionDeadline: Duration.seconds(90),
  sessionSweep: Duration.seconds(1),
  settleDeadline: Duration.seconds(30),
  maxConcurrentChannels: 4,
  storeRetry: Duration.zero,
  maxPendingMessages: 100,
  maxPendingBytes: 10_000_000,
  mailboxCapacity: 16,
  maxIdleTime: Duration.hours(1)
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
    Layer.provideMerge(options?.store ?? sqliteStore),
    // The entity mints claim tokens from the deployment's own randomness, not the ambient global.
    Layer.provide(NodeCrypto.layer),
    Layer.provide(PeerRelayLimits.layerDefaults)
  )

const layer = relay()

// The sweeper's own arithmetic is in milliseconds; the options state durations.
const sessionDeadlineMillis = Duration.toMillis(baseOptions.sessionDeadline)
const sessionSweepMillis = Duration.toMillis(baseOptions.sessionSweep)

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
  reason: new ReplicaError.StorageUnavailable({ cause: "injected" })
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

const deliver = (message: Message): {
  readonly channel: RelayInboxStore.ChannelKey
  readonly envelope: RelayInboxStore.InboxEnvelope
  readonly senderRetryHorizonMillis: number
} => {
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
      payloadVersion: 1,
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

const storedMessages = Stream.filter<PeerRpc.RelayMessage, PeerRpc.StoredMessage>(
  (message: PeerRpc.RelayMessage): message is PeerRpc.StoredMessage => message._tag === "StoredMessage"
)

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
    storedMessages,
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
  client.Subscribe({ sessionId: session }).pipe(storedMessages, Stream.take(count), Stream.runCollect)

const ids = (messages: ReadonlyArray<PeerRpc.StoredMessage>) => messages.map((message) => message.relayMessageId)

const transient = (value: number): PeerRpc.TransientMessage =>
  PeerRpc.TransientMessage.make({
    sender: {
      tenantId: "tenant-a",
      subjectId: "sender-a",
      peerId: peer("00000000aaa1")
    },
    document: { documentId: documentId("00000000dddd"), documentType: "note" },
    payload: Uint8Array.of(value)
  })

describe("RelayInbox", () => {
  it.effect("delivers transient messages only to the live subscriber", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      const currentSession = sessionId("000000000001")
      const receivedFiber = yield* client.Subscribe({ sessionId: currentSession }).pipe(
        Stream.filter((event) => event._tag === "TransientMessage"),
        Stream.runHead,
        Effect.forkChild
      )
      yield* Effect.yieldNow

      yield* client.Transient({ message: transient(1) })
      const received = yield* Fiber.join(receivedFiber)
      assert.deepStrictEqual(Option.getOrThrow(received), transient(1))
    }).pipe(Effect.provide(layer)))

  it.effect("drops transient messages sent while offline without replay", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Transient({ message: transient(1) })

      const fiber = yield* client.Subscribe({ sessionId: sessionId("000000000001") }).pipe(
        Stream.filter((event) => event._tag === "TransientMessage"),
        Stream.runHead,
        Effect.forkChild
      )
      yield* Effect.yieldNow
      assert.isUndefined(fiber.pollUnsafe())

      yield* client.Transient({ message: transient(2) })
      assert.deepStrictEqual(Option.getOrThrow(yield* Fiber.join(fiber)), transient(2))
    }).pipe(Effect.provide(layer)))

  it.effect("drops overflow without blocking the sender", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      const blocked = yield* Deferred.make<void>()
      const subscriber = yield* client.Subscribe({ sessionId: sessionId("000000000001") }).pipe(
        Stream.filter((event) => event._tag === "TransientMessage"),
        Stream.runForEach(() => Deferred.await(blocked)),
        Effect.forkChild
      )
      yield* Effect.yieldNow

      for (
        let value = 0;
        value <= PeerRelayLimits.defaults.transientRecipientQueueCapacity + 1;
        value++
      ) {
        yield* client.Transient({ message: transient(value) })
      }
      yield* Fiber.interrupt(subscriber)
    }).pipe(Effect.provide(layer)))

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
      const pending = yield* store.pendingHeads(inboxKey, { limit: 10, now: 0 })
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
      const pending = yield* store.pendingHeads(inboxKey, { limit: 10, now: 0 })
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

  it.effect("delivers the live epoch ahead of a crash looper's stale epochs", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      // A crash looping sender mints a fresh epoch per reconnect and leaves one channel per lapsed
      // epoch. With one delivery slot, age ordered selection spends it on the oldest stale epoch
      // and the epoch the sender is connected on right now waits behind the whole trail until the
      // stale heads dead letter or expire.
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0, epoch: "epoch-1" }))
      yield* TestClock.adjust(1000)
      yield* client.Deliver(deliver({ id: "000000000002", sequence: 0, epoch: "epoch-2" }))
      yield* TestClock.adjust(1000)
      yield* client.Deliver(deliver({ id: "000000000003", sequence: 0, epoch: "epoch-3" }))

      const first = yield* receiveOnly(client, sessionId("000000000001"), 1)
      assert.deepStrictEqual(
        ids(first),
        [relayId("000000000003")],
        "the one slot goes to the sender's live epoch, not the oldest stale one"
      )

      // Deprioritized, never abandoned: once the live head is settled the leftover capacity
      // drains the stale trail.
      const drained = yield* receiveAndSettle(client, sessionId("000000000002"), 3)
      assert.strictEqual(ids(drained)[0], relayId("000000000003"))
      assert.deepStrictEqual(
        ids(drained).toSorted(),
        [relayId("000000000001"), relayId("000000000002"), relayId("000000000003")].toSorted()
      )
    }).pipe(Effect.provide(relay({ entity: { maxConcurrentChannels: 1 } }))))

  it.effect("keeps draining a superseded channel while the live epoch keeps producing", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      // One stale-epoch head the recipient is entitled to, behind a live epoch whose channel is
      // never empty. Strict live-first priority would keep the single slot on the live channel at
      // every poll, so the stale message would never be handed over and would eventually expire —
      // and the sender only replays its current epoch's outbox, so expiry destroys the last copy.
      // Aging bounds that: once the stale head has burned half its TTL it must outrank the live
      // channel.
      yield* client.Deliver(deliver({ id: "0000000000ff", sequence: 0, epoch: "epoch-1" }))
      yield* TestClock.adjust(1000)
      for (let sequence = 0; sequence < 3; sequence++) {
        yield* client.Deliver(
          deliver({ id: `0000000000a${sequence.toString(16)}`, sequence, epoch: "epoch-2" })
        )
      }

      // The stale head crosses half of its four second TTL after the first live settlement.
      const session = sessionId("000000000001")
      const received = yield* client.Subscribe({ sessionId: session }).pipe(
        storedMessages,
        Stream.take(3),
        Stream.mapEffect((message) =>
          client.Settle({
            sessionId: session,
            relayMessageId: message.relayMessageId,
            claimToken: message.claimToken,
            messageHash: message.messageHash,
            outcome: "Acknowledged"
          }).pipe(
            Effect.andThen(TestClock.adjust(1_000)),
            Effect.as(message)
          )
        ),
        Stream.runCollect
      )

      assert.isTrue(
        ids(received).includes(relayId("0000000000ff")),
        "a superseded head must be promoted before it expires, however busy the live epoch is"
      )
    }).pipe(
      Effect.provide(relay({
        entity: {
          maxConcurrentChannels: 1,
          sessionDeadline: Duration.hours(2),
          messageTtl: Duration.seconds(4),
          // Must exceed the second the clock jumps per settlement: the transport can already hold
          // the next delivery attempt when the clock moves, and a deadline inside the jump
          // abandons that attempt, so its redelivered claim token no longer matches the one the
          // recipient is settling with.
          settleDeadline: Duration.seconds(2)
        }
      }))
    ), 0)

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
          relayMessageId: delivered[0].relayMessageId,
          claimToken: delivered[0].claimToken,
          messageHash: delivered[0].messageHash,
          outcome: "Acknowledged"
          // Covers the complete declared failure channel, so only an out-of-channel outcome fails.
        }).pipe(Effect.catch(() => Effect.void), Effect.exit)
      )
      yield* TestClock.adjust(10)
      yield* TestClock.adjust(sessionDeadlineMillis + sessionSweepMillis * 3)

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

  // Its own timeout: advancing four hours of virtual time makes the runtime walk every task
  // scheduled in that window, which costs ~845ms against 5-27ms for every other check here.
  it.effect("closes a replaced session even when the replacing subscribe is interrupted", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))

      const incumbent = sessionId("00000000000a")
      const delivered = yield* Queue.unbounded<PeerRpc.StoredMessage>()
      const stream = yield* Effect.forkChild(
        Stream.runForEach(
          client.Subscribe({ sessionId: incumbent }).pipe(storedMessages),
          (message) => Queue.offer(delivered, message)
        )
      )
      const head = yield* Queue.take(delivered)

      // Parks the incumbent's delivering fiber inside a durable write that interruption cannot cut
      // short. This is faithful rather than convenient: `sql.withTransaction` runs connection
      // acquisition, BEGIN and COMMIT inside an uninterruptible mask, so a real delivering fiber
      // genuinely sits in a region that `Scope.close` has to wait out.
      yield* Effect.forkChild(
        client.Settle({
          sessionId: incumbent,
          relayMessageId: head.relayMessageId,
          claimToken: head.claimToken,
          messageHash: head.messageHash,
          outcome: "Acknowledged"
        }).pipe(Effect.exit)
      )
      yield* TestClock.adjust(1)

      // The replacement takes the incumbent out of `sessionRef` — the only handle anything holds on
      // it — and then blocks closing its scope. A front door that loses its socket at that instant
      // interrupts this call, and everything the close had not reached yet is orphaned: the
      // dispatcher keeps draining this inbox and the outbound queue is never ended, so the
      // recipient's stream never terminates and it is never told to reconnect.
      const replacement = yield* Effect.forkChild(
        Stream.runDrain(client.Subscribe({ sessionId: sessionId("00000000000b") }))
      )
      yield* TestClock.adjust(1)
      yield* Fiber.interrupt(replacement)
      yield* TestClock.adjust(Duration.toMillis(Duration.hours(4)))

      assert.isTrue(
        stream.pollUnsafe() !== undefined,
        "a replaced session's stream must end; a half closed session leaves nothing able to close it"
      )
    }).pipe(Effect.provide(relay({
      store: storeFailing((real) => ({
        settle: (key, relayMessageId, options) =>
          Effect.uninterruptible(Effect.sleep(Duration.hours(1))).pipe(
            Effect.andThen(real.settle(key, relayMessageId, options))
          )
      }))
    }))), 60_000)

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

  it.effect("ends the delivery stream when its dispatcher dies", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))

      // The dispatcher runs on a forked fiber nobody observes, so a defect in it simply makes the
      // fiber disappear. Left unnoticed the session stays installed and looks perfectly healthy:
      // the front door keeps heartbeating it, `Deliver` keeps reporting success to senders, and the
      // device receives nothing at all until it happens to reconnect. The framework does not catch
      // it either — a forked fiber's defect never reaches the entity's own defect restart — so the
      // dispatcher has to hand its cause to the one thing the recipient is watching.
      const exit = yield* client.Subscribe({ sessionId: sessionId("000000000001") }).pipe(
        storedMessages,
        Stream.runDrain,
        Effect.exit
      )
      assert.isTrue(Exit.isFailure(exit), "the recipient is told, rather than waiting forever")
      if (Exit.isSuccess(exit)) return
      // Retryable, and never a clean end of stream. `Subscribe` declares a closed error union, so a
      // raw defect on the queue reaches nobody; the defect itself goes to the log at fatal with its
      // whole cause, and the recipient is told the one thing it can act on — come back.
      assert.deepStrictEqual(
        Cause.findErrorOption(exit.cause).pipe(Option.map((error) => error._tag)),
        Option.some("ServerUnavailable")
      )
    }).pipe(Effect.provide(relay({
      store: storeFailing(() => ({
        pendingHeads: () => Effect.die(new Error("dispatcher fault"))
      }))
    }))))

  it.effect("releases a session whose liveness deadline lapses", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      const session = sessionId("000000000001")

      const subscriber = yield* Effect.forkChild(
        Stream.runCollect(client.Subscribe({ sessionId: session }).pipe(storedMessages))
      )
      yield* TestClock.adjust(10)

      // Nothing heartbeats it, so the entity's own deadline is what bounds it. A cluster cannot
      // rely on disconnects announcing themselves.
      yield* TestClock.adjust(sessionDeadlineMillis + sessionSweepMillis * 2)

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
        storedMessages,
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
      assert.isTrue(outcome[0]._tag === "Failure")

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
        storedMessages,
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
      assert.isTrue(outcome[0]._tag === "Failure")

      const store = yield* RelayInboxStore.RelayInboxStore
      const pending = yield* store.pendingHeads(inboxKey, { limit: 10, now: 0 })
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
        storedMessages,
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
      assert.isTrue(outcome[0]._tag === "Failure")

      const store = yield* RelayInboxStore.RelayInboxStore
      const pending = yield* store.pendingHeads(inboxKey, { limit: 10, now: 0 })
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
        storedMessages,
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
      assert.isTrue(outcome[0]._tag === "Failure")
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

      // The exhausted head is never transmitted again: a recipient handed a message whose
      // settlement the relay already refuses can only fail its acknowledgement, and that failure
      // used to tear down the whole connection. Dead lettering happens before the offer.
      const second = yield* receiveOnly(client, sessionId("000000000002"), 1)
      assert.deepStrictEqual(
        ids(second),
        [relayId("000000000002")],
        "the exhausted head is dead lettered without another transmission and the channel moves on"
      )

      const store = yield* RelayInboxStore.RelayInboxStore
      const abandoned = yield* store.abandoned(inboxKey, { limit: 10 })
      assert.deepStrictEqual(
        abandoned.map((message) => [message.relayMessageId, message.state]),
        [[relayId("000000000001"), "DeadLettered"]],
        "an abandoned message stays answerable, because its sender already released custody"
      )
    }).pipe(Effect.provide(relay({ entity: { maxDeliveries: 1 } }))))

  it.effect("abandons an unsettled delivery at the settle deadline and redelivers it on the same session", () =>
    Effect.gen(function*() {
      const client = yield* inbox
      yield* client.Deliver(deliver({ id: "000000000001", sequence: 0 }))

      // The recipient takes the message and never settles it — the shape of an acknowledgement
      // that failed in transit while the session survives. Without the deadline the delivering
      // fiber would hold this channel for the whole session and nothing behind it would flow.
      const session = sessionId("000000000001")
      const collector = yield* client.Subscribe({ sessionId: session }).pipe(
        storedMessages,
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild
      )
      yield* TestClock.adjust(1000)
      assert.isUndefined(collector.pollUnsafe(), "no redelivery before the deadline")

      yield* TestClock.adjust(Duration.toMillis(baseOptions.settleDeadline))
      const messages = [...(yield* Fiber.join(collector))]
      assert.deepStrictEqual(
        messages.map((message) => message.relayMessageId),
        [relayId("000000000001"), relayId("000000000001")],
        "the abandoned head is redelivered on the same session"
      )
      assert.notStrictEqual(
        messages[0].claimToken,
        messages[1].claimToken,
        "the redelivery is a fresh attempt"
      )

      // The redelivered attempt is fully settleable: abandonment cost the attempt, not the row.
      yield* client.Settle({
        sessionId: session,
        relayMessageId: messages[1].relayMessageId,
        claimToken: messages[1].claimToken,
        messageHash: messages[1].messageHash,
        outcome: "Acknowledged"
      })
      const store = yield* RelayInboxStore.RelayInboxStore
      assert.strictEqual((yield* store.pendingHeads(inboxKey, { limit: 10, now: 0 })).length, 0)
    }).pipe(Effect.provide(layer)))

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
      const untaken = (yield* store.pendingHeads(inboxKey, { limit: 10, now: 0 }))
        .find((message) => message.relayMessageId === relayId("000000000002"))
      assert.strictEqual(untaken?.deliveries, 0, "an untransmitted attempt costs the message nothing")

      // The one transmission it ever gets. Asserting the positive as well keeps this test honest if
      // the nudge above ever stops landing: a charge taken while it sat unread shows up as a second.
      const later = yield* receiveOnly(client, sessionId("000000000002"), 2)
      assert.isTrue(ids(later).includes(relayId("000000000002")))

      const transmitted = (yield* store.pendingHeads(inboxKey, { limit: 10, now: 0 }))
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
      const pending = yield* store.pendingHeads(inboxKey, { limit: 10, now: 0 })
      assert.deepStrictEqual(
        pending.map((message): readonly [string, number] => [message.relayMessageId, message.deliveries]).toSorted((
          left,
          right
        ) => left[0].localeCompare(right[0])),
        ([
          [relayId("000000000001"), 1],
          [relayId("000000000003"), 1]
        ] satisfies ReadonlyArray<readonly [string, number]>).toSorted((left, right) =>
          left[0].localeCompare(right[0])
        ),
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
        terminalRetentionMillis: Duration.toMillis(baseOptions.terminalRetention)
      })
      const promoted = (yield* store.pendingHeads(inboxKey, { limit: 10, now: 0 }))
        .find((message) => message.relayMessageId === relayId("000000000002"))
      assert.strictEqual(
        promoted?.deliveries,
        0,
        "a message queued behind its channel head is never charged while it waits"
      )
    }).pipe(Effect.provide(layer)))
})
