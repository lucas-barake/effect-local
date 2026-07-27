import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as MessageStorage from "effect/unstable/cluster/MessageStorage"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as RpcTest from "effect/unstable/rpc/RpcTest"
import { encodeInboxKey } from "../src/internal/relayInboxKey.js"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerCredentials from "../src/PeerCredentials.js"
import * as PeerRelayAuthorization from "../src/PeerRelayAuthorization.js"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as RelayInbox from "../src/RelayInbox.js"
import * as RelayInboxStore from "../src/RelayInboxStore.js"
import * as RelayServer from "../src/RelayServer.js"
import * as SqlRelayInboxStore from "../src/SqlRelayInboxStore.js"

const Task = Document.make("Task", {
  schema: Schema.Struct({ title: Schema.String }),
  version: 1
})

const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const otherDocumentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const senderPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const recipientPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
const relayId = (value: string) => Identity.RelayMessageId.make(`rly_00000000-0000-4000-8000-${value}`)

const sender = PeerAuthentication.PeerPrincipal.make({
  tenantId: "tenant",
  subjectId: "sender",
  peerId: senderPeerId
})
const recipient = PeerAuthentication.PeerPrincipal.make({
  tenantId: "tenant",
  subjectId: "recipient",
  peerId: recipientPeerId
})

/**
 * A third device in the same tenant.
 *
 * The recipient's inbox is keyed by the recipient alone, so every peer holding a Send grant to it
 * writes into that one inbox. A session opened against one counterparty therefore drains messages
 * from all of them, which is what makes "who sent this" a question the front door has to ask per
 * message rather than once at the handshake.
 */
const intruder = PeerAuthentication.PeerPrincipal.make({
  tenantId: "tenant",
  subjectId: "intruder",
  peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000005")
})

/** A real, authenticated peer that simply belongs to a tenant this relay does not serve. */
const outsider = PeerAuthentication.PeerPrincipal.make({
  tenantId: "other",
  subjectId: "outsider",
  peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000004")
})

const serverOptions: RelayServer.Options = {
  tenantId: "tenant",
  peerId: relayPeerId,
  heartbeatInterval: Duration.seconds(30),
  entityCallTimeout: Duration.seconds(30)
}

const inboxOptions: RelayInbox.Options = {
  maxDeliveries: 10,
  messageTtl: Duration.minutes(10),
  terminalRetention: Duration.minutes(10),
  sessionDeadline: Duration.seconds(90),
  sessionSweep: Duration.seconds(1),
  maxConcurrentChannels: 4,
  storeRetry: Duration.zero,
  maxPendingMessages: 100,
  maxPendingBytes: 10_000_000,
  mailboxCapacity: 16,
  maxIdleTime: Duration.hours(1)
}

const TestShardingConfig = ShardingConfig.layer({
  entityTerminationTimeout: 0,
  entityMessagePollInterval: 5000,
  sendRetryInterval: 100
})

const RelaySyncEnvelopeJson = Schema.fromJsonString(
  Schema.toCodecJson(PeerSyncEnvelope.SyncEnvelope)
)

const openRequest = (
  principal: PeerAuthentication.PeerPrincipal,
  remote: PeerAuthentication.PeerPrincipal,
  documents: ReadonlyArray<PeerRpc.RequestedDocument> = [{ documentType: Task.name, documentId }]
) =>
  PeerRpc.OpenRpc.payloadSchema.make({
    protocolVersion: PeerRpc.protocolVersion,
    expectedRelayPeerId: relayPeerId,
    expectedLocal: principal,
    senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
    remote: { subjectId: remote.subjectId, peerId: remote.peerId },
    documents,
    receiptRetentionMillis: Duration.toMillis(PeerRelayLimits.defaults.maximumReceiptRetention),
    senderRetryHorizonMillis: Duration.toMillis(PeerRelayLimits.defaults.maximumSenderRetryHorizon)
  })

/**
 * Mutable knobs the tests turn.
 *
 * Authorization and credential validity are the two things the relay is supposed to keep watching
 * for the life of a session, so they have to be changeable while a session is open.
 */
interface Knobs {
  allow: (request: PeerRelayAuthorization.Request) => boolean
  /**
   * The separate acknowledgement that relaying a document commits its recipient to an
   * allocation-unbounded decode. It denies by default in production, so it is its own knob.
   */
  allowRisk: (request: PeerRelayAuthorization.UnsafeUnboundedAutomerge3DecodeRequest) => boolean
  /**
   * How long a granted authorization stays good.
   *
   * Per request rather than a single value, because the handshake takes two grants and the thing
   * worth pinning is that each is re-checked against the clock on its own.
   */
  grantValidUntil: (request: PeerRelayAuthorization.Request) => number
  /**
   * Run inside the authorization port, before it answers.
   *
   * Authorizing is an I/O call to a policy backend, so time passes while it is in flight. This is
   * the only way to reach the checks the relay makes *after* authorization returns, because the
   * port itself refuses an already-expired grant and would otherwise answer first.
   */
  onAuthorize: (request: PeerRelayAuthorization.Request) => Effect.Effect<void>
  authority: { validUntil: number; invalidated: Effect.Effect<void> }
}

const harness = (options?: {
  readonly limits?: PeerRelayLimits.Values
  readonly knobs?: Partial<Knobs>
  /**
   * Wraps the real store.
   *
   * Used only to make a durable operation hang, which is the one thing real SQLite will not do and
   * the entity-call timeout exists for.
   */
  readonly store?: (
    real: RelayInboxStore.RelayInboxStore["Service"]
  ) => RelayInboxStore.RelayInboxStore["Service"]
  /** Builds the exported `RelayServer.layer` instead of the handlers on their own. */
  readonly composed?: Partial<RelayServer.Options>
}) =>
  Effect.gen(function*() {
    const knobs: Knobs = {
      allow: options?.knobs?.allow ?? (() => true),
      allowRisk: options?.knobs?.allowRisk ?? (() => true),
      grantValidUntil: options?.knobs?.grantValidUntil ?? (() => Number.MAX_SAFE_INTEGER),
      onAuthorize: options?.knobs?.onAuthorize ?? (() => Effect.void),
      authority: options?.knobs?.authority ?? {
        validUntil: Number.MAX_SAFE_INTEGER,
        invalidated: Effect.never
      }
    }
    const limits = options?.limits ?? PeerRelayLimits.defaults
    const crypto = yield* Crypto.Crypto.pipe(Effect.provide(NodeCrypto.layer))

    const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
      Effect.provide(PeerRelayAuthorization.layer(
        (request) =>
          knobs.onAuthorize(request).pipe(Effect.andThen(
            knobs.allow(request)
              ? Effect.succeed({
                remote: {
                  tenantId: request.principal.tenantId,
                  subjectId: request.remote.subjectId,
                  peerId: request.remote.peerId
                },
                documents: request.documents.map((requested) => ({
                  document: Task,
                  documentId: requested.documentId
                })),
                validUntil: knobs.grantValidUntil(request),
                invalidated: Effect.never
              })
              : Effect.fail(new PeerRpcError.AccessDenied())
          )),
        // Deliberately independent of `allow`. Tying the two together would make removing either
        // port from production invisible, since revoking one knob would revoke both gates at once.
        (request) =>
          knobs.allowRisk(request)
            ? Effect.succeed({
              _tag: "UnsafeUnboundedAutomerge3DecodeGrant" as const,
              risk: PeerRelayAuthorization.unsafeUnboundedAutomerge3DecodeRisk,
              principal: request.principal,
              remote: {
                tenantId: request.principal.tenantId,
                subjectId: request.remote.subjectId,
                peerId: request.remote.peerId
              },
              direction: request.direction,
              documents: request.documents,
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.never
            })
            : Effect.fail(new PeerRpcError.AccessDenied())
      ))
    )

    // The real durable store behind the real entity, so the front door is exercised against the
    // custody it actually forwards to rather than a stand-in that always says yes.
    const realStore = SqlRelayInboxStore.layer.pipe(
      Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
      Layer.provide(Layer.succeed(Crypto.Crypto)(crypto)),
      Layer.orDie
    )
    const decorate = options?.store
    const storeLayer = decorate === undefined
      ? realStore
      : Layer.effect(RelayInboxStore.RelayInboxStore)(
        Effect.gen(function*() {
          return decorate(yield* RelayInboxStore.RelayInboxStore)
        })
      ).pipe(Layer.provide(realStore))

    const cluster = Sharding.layer.pipe(
      Layer.provide(Runners.layerNoop),
      Layer.provideMerge(MessageStorage.layerMemory),
      Layer.provide(RunnerStorage.layerMemory),
      Layer.provide(RunnerHealth.layerNoop),
      Layer.provide(TestShardingConfig),
      Layer.provideMerge(storeLayer)
    )

    // Either the handlers alone, which most tests want because they can then drive the entity
    // directly, or the exported layer a deployment actually builds.
    const relay = options?.composed === undefined
      ? RelayServer.layerHandlers(serverOptions).pipe(
        Layer.provideMerge(RelayInbox.layer(inboxOptions))
      )
      : RelayServer.layer({
        ...serverOptions,
        ...options.composed,
        inbox: inboxOptions,
        maintenance: {
          interval: Duration.minutes(1),
          batchLimit: 100,
          terminalRetention: inboxOptions.terminalRetention,
          enabled: true
        }
      })

    const context = yield* Layer.build(
      relay.pipe(
        Layer.provideMerge(cluster),
        Layer.provide(Layer.mergeAll(
          Layer.succeed(Crypto.Crypto)(crypto),
          Layer.succeed(PeerRelayLimits.PeerRelayLimits)(limits),
          Layer.succeed(PeerRelayAuthorization.PeerRelayAuthorization)(authorization)
        ))
      )
    )

    const credential = yield* Ref.make("sender")
    const principals = new Map([
      ["sender", sender],
      ["recipient", recipient],
      ["outsider", outsider],
      ["intruder", intruder]
    ])
    const authentication = yield* PeerAuthentication.PeerAuthentication.pipe(
      Effect.provide(PeerAuthentication.layerServer),
      Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
        authenticate: (secret) => {
          const principal = principals.get(Redacted.value(secret))
          return principal === undefined
            ? Effect.fail(new PeerRpcError.AuthenticationFailure())
            : Effect.succeed({
              principal,
              validUntil: knobs.authority.validUntil,
              invalidated: knobs.authority.invalidated
            })
        }
      }),
      Effect.provideService(PeerRelayLimits.PeerRelayLimits, limits)
    )

    const client = yield* RpcTest.makeClient(PeerRpc.Rpcs).pipe(
      Effect.provideContext(
        Context.add(context, PeerAuthentication.PeerAuthentication, authentication)
      ),
      Effect.provide(PeerAuthentication.layerClient),
      Effect.provideService(PeerCredentials.PeerCredentials, {
        get: Ref.get(credential).pipe(Effect.map(Redacted.make))
      })
    )

    // Shard assignment and acquisition run on scheduled fibers, so no entity is reachable under
    // virtual time until the clock is advanced past them.
    yield* TestClock.adjust(5000)

    const store = Context.get(context, RelayInboxStore.RelayInboxStore)
    return { client, credential, crypto, knobs, store }
  })

type Harness = Effect.Success<ReturnType<typeof harness>>

/** The same harness, built from the exported layer a deployment wires up. */
const composed = (overrides?: Partial<RelayServer.Options>) => harness({ composed: overrides ?? {} })

/** Opens a session and drains its events into a queue, leaving the stream live. */
const open = (
  peer: Harness,
  principal: PeerAuthentication.PeerPrincipal,
  remote: PeerAuthentication.PeerPrincipal,
  documents?: ReadonlyArray<PeerRpc.RequestedDocument>
) =>
  Effect.gen(function*() {
    yield* Ref.set(peer.credential, principal.subjectId)
    const events = yield* Queue.unbounded<PeerRpc.OpenEvent>()
    const fiber = yield* peer.client.Open(openRequest(principal, remote, documents)).pipe(
      Stream.runForEach((event) => Queue.offer(events, event)),
      Effect.forkScoped
    )
    const opened = yield* Queue.take(events)
    assert.strictEqual(opened._tag, "Opened")
    if (opened._tag !== "Opened") return yield* Effect.die("expected Opened")
    return { opened, events, fiber }
  })

const encodePayload = (peer: Harness, options?: {
  readonly message?: Uint8Array
  readonly hash?: string
  readonly documentId?: Identity.DocumentId
  readonly sequence?: number
  /** Part of the channel identity, so two epochs from one sender are two ordered streams. */
  readonly epoch?: string
}) =>
  Effect.gen(function*() {
    const message = options?.message ?? Uint8Array.of(1, 2, 3)
    const messageHash = options?.hash ?? (yield* Canonical.digest(message).pipe(
      Effect.provideService(Crypto.Crypto, peer.crypto)
    ))
    return new TextEncoder().encode(
      yield* Schema.encodeEffect(RelaySyncEnvelopeJson)({
        connectionEpoch: options?.epoch ?? "epoch",
        sequence: options?.sequence ?? 0,
        documentId: options?.documentId ?? documentId,
        documentType: Task.name,
        messageHash,
        message,
        lineage: Identity.genesisLineage,
        writerProvenance: []
      })
    )
  })

/** The inbox a device's messages land in, derived exactly as the front door derives it. */
const inboxKeyOf = (peer: Harness, principal: PeerAuthentication.PeerPrincipal) =>
  encodeInboxKey(principal).pipe(
    Effect.provideService(Crypto.Crypto, peer.crypto),
    Effect.orDie
  )

const push = (peer: Harness, sessionId: Identity.SessionId, payload: Uint8Array, id = "000000000001") =>
  Ref.set(peer.credential, "sender").pipe(
    Effect.andThen(peer.client.Push({ sessionId, relayMessageId: relayId(id), payload }))
  )

describe("RelayServer", () => {
  it.effect("refuses an unsupported protocol version and opens no session", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const failure = yield* peer.client.Open({
        ...openRequest(sender, recipient),
        protocolVersion: 2
      }).pipe(Stream.runDrain, Effect.flip)
      assert.strictEqual(failure._tag, "UnsupportedVersion")
    })))

  it.effect("answers Open with a distinct session and the logical counterparty", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const first = yield* open(peer, sender, recipient)

      assert.strictEqual(first.opened.protocolVersion, PeerRpc.protocolVersion)
      assert.deepStrictEqual(first.opened.authenticatedLocal, sender)
      // The counterparty the client asked for, not the relay it is talking to. Reporting the relay
      // here would make the client address its own peer as the relay.
      assert.strictEqual(first.opened.remotePeerId, recipientPeerId)
      assert.notStrictEqual(first.opened.remotePeerId, relayPeerId)

      const second = yield* open(peer, recipient, sender)
      assert.notStrictEqual(first.opened.sessionId, second.opened.sessionId)
    })))

  it.effect("refuses a handshake that names another relay or another local peer", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()

      const wrongRelay = yield* peer.client.Open({
        ...openRequest(sender, recipient),
        expectedRelayPeerId: recipientPeerId
      }).pipe(Stream.runDrain, Effect.flip)
      assert.strictEqual(wrongRelay._tag, "PeerMismatch")

      // The client states who it believes it is; the credential decides. A mismatch must not be
      // resolved in favour of the payload, or a credential could open a session for another device.
      const wrongLocal = yield* peer.client.Open({
        ...openRequest(sender, recipient),
        expectedLocal: recipient
      }).pipe(Stream.runDrain, Effect.flip)
      assert.strictEqual(wrongLocal._tag, "PeerMismatch")
    })))

  it.effect("delivers a pushed message byte identically to the addressed recipient", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)
      const recipientSession = yield* open(peer, recipient, sender)

      const payload = yield* encodePayload(peer)
      yield* push(peer, senderSession.opened.sessionId, payload)

      const stored = yield* Queue.take(recipientSession.events)
      assert.strictEqual(stored._tag, "StoredMessage")
      if (stored._tag !== "StoredMessage") return
      assert.deepStrictEqual(stored.payload, payload)
      // The identity the sender releases custody against, so delivering the right bytes under the
      // wrong id would still lose the message.
      assert.strictEqual(stored.relayMessageId, relayId("000000000001"))
      assert.strictEqual(stored.relayPeerId, relayPeerId)
      assert.strictEqual(stored.sender.peerId, senderPeerId)
      assert.deepStrictEqual(stored.recipient, recipient)

      // Computed here independently and compared, rather than merely checked for shape. The digest
      // binds sender, recipient, relay, sequence, epoch, document, lineage and provenance, and it
      // is what the recipient verifies provenance against, so a digest over the wrong inputs is
      // exactly as wrong as a malformed one while still matching a hex pattern.
      const expected = yield* PeerSyncEnvelope.digestRelayOuterEnvelope({
        domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
        version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
        expectedLocal: sender,
        remote: recipient,
        relayPeerId,
        relayMessageId: relayId("000000000001"),
        protocolVersion: PeerRpc.protocolVersion,
        payloadVersion: 1,
        senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
        senderConnectionEpoch: "epoch",
        senderSequence: 0,
        document: { documentId, documentType: Task.name },
        lineage: Identity.genesisLineage,
        writerProvenance: [],
        messageHash: stored.messageHash,
        payload
      }).pipe(Effect.provideService(Crypto.Crypto, peer.crypto))
      assert.strictEqual(stored.outerEnvelopeDigest, expected)
    })))

  it.effect("retires an acknowledged message so a fresh session does not see it again", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)
      const recipientSession = yield* open(peer, recipient, sender)

      yield* push(peer, senderSession.opened.sessionId, yield* encodePayload(peer))
      const stored = yield* Queue.take(recipientSession.events)
      if (stored._tag !== "StoredMessage") return assert.fail("expected a delivery")

      yield* Ref.set(peer.credential, "recipient")
      yield* peer.client.Acknowledge({
        sessionId: recipientSession.opened.sessionId,
        relayMessageId: stored.relayMessageId,
        claimToken: stored.claimToken,
        messageHash: stored.messageHash
      })

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 0, "an acknowledged message is terminal")
    })))

  it.effect("replaces the incumbent session and refuses its session id afterwards", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const incumbent = yield* open(peer, sender, recipient)
      const replacement = yield* open(peer, sender, recipient)
      assert.notStrictEqual(incumbent.opened.sessionId, replacement.opened.sessionId)

      // Server initiated: the client never asked for the incumbent to end.
      yield* Fiber.await(incumbent.fiber)

      const payload = yield* encodePayload(peer)
      const stale = yield* push(peer, incumbent.opened.sessionId, payload).pipe(Effect.flip)
      assert.strictEqual(stale._tag, "SessionUnavailable")

      const staleAck = yield* peer.client.Acknowledge({
        sessionId: incumbent.opened.sessionId,
        relayMessageId: relayId("000000000001"),
        claimToken: PeerRpc.ClaimToken.make("clm_00000000-0000-4000-8000-000000000000"),
        messageHash: "a".repeat(64)
      }).pipe(Effect.flip)
      assert.strictEqual(staleAck._tag, "SessionUnavailable")
    })))

  it.effect("refuses a push whose send grant has been withdrawn and admits nothing", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)

      peer.knobs.allow = (request) => request.direction !== "Send"
      const payload = yield* encodePayload(peer)
      const denied = yield* push(peer, senderSession.opened.sessionId, payload).pipe(Effect.flip)
      assert.strictEqual(denied._tag, "AccessDenied")

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 0, "a denied push must not reach durable custody")
    })))

  it.effect("withholds a delivery the recipient may no longer receive without erroring the stream", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)
      const recipientSession = yield* open(peer, recipient, sender)

      // Anything holding a Send grant to this device can write into its inbox, and the entity knows
      // nothing of grants, so the handshake's Receive decision cannot stand in for the recipient's
      // right to see this particular document.
      peer.knobs.allow = (request) => request.direction !== "Receive"
      yield* push(peer, senderSession.opened.sessionId, yield* encodePayload(peer))
      yield* TestClock.adjust(100)

      assert.strictEqual(yield* Queue.size(recipientSession.events), 0, "withheld, not delivered")
      assert.strictEqual(
        recipientSession.fiber.pollUnsafe(),
        undefined,
        "and the stream neither ends nor errors"
      )

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 1, "the message stays durable for a later session")

      // Withheld, not lost. Restoring the grant and reconnecting must produce the message, which is
      // what distinguishes withholding from silently dropping it.
      peer.knobs.allow = () => true
      const restored = yield* open(peer, recipient, sender)
      const delivered = yield* Queue.take(restored.events)
      assert.strictEqual(delivered._tag, "StoredMessage")
    })))

  it.effect("refuses a push whose payload does not decode or does not match its own hash", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)
      const session = senderSession.opened.sessionId

      const undecodable = yield* push(peer, session, new TextEncoder().encode("{")).pipe(Effect.flip)
      assert.strictEqual(undecodable._tag, "InvalidRequest")

      // The hash is the identity the recipient deduplicates on, so the relay recomputes it rather
      // than believing the sender's label.
      const mislabelled = yield* encodePayload(peer, { hash: "a".repeat(64) })
      const wrongHash = yield* push(peer, session, mislabelled, "000000000002").pipe(Effect.flip)
      assert.strictEqual(wrongHash._tag, "InvalidRequest")

      const undeclared = yield* encodePayload(peer, { documentId: otherDocumentId })
      const notInSession = yield* push(peer, session, undeclared, "000000000003").pipe(Effect.flip)
      assert.strictEqual(notInSession._tag, "InvalidRequest")

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 0, "no malformed push reaches durable custody")
    })))

  it.effect("keys the delivery stream by the authenticated caller", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)
      const recipientSession = yield* open(peer, recipient, sender)

      yield* push(peer, senderSession.opened.sessionId, yield* encodePayload(peer))

      // Waits for the delivery rather than nudging the clock, so the sender's stream is checked at
      // a point where the message has provably already been routed somewhere.
      const stored = yield* Queue.take(recipientSession.events)
      assert.strictEqual(stored._tag, "StoredMessage")

      // The message is addressed to the recipient, so the sender's own stream must stay empty no
      // matter what it put in any payload field: its inbox key comes from its credential.
      assert.strictEqual(yield* Queue.size(senderSession.events), 0)
    })))

  it.effect("caps sessions per subject rather than across all of them", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness({
        limits: PeerRelayLimits.Values.make({
          ...PeerRelayLimits.defaults,
          maxSessionsPerSubject: 1
        })
      })
      yield* open(peer, sender, recipient)

      // A different subject at its own cap of one. A counter that simply totalled live sessions
      // would refuse this, which would let one busy subject lock every other client out.
      yield* open(peer, recipient, sender)

      // Sessions are the front door's only unbounded resource and an authenticated client can open
      // them in a loop, so the cap has to be refused rather than absorbed.
      yield* Ref.set(peer.credential, "sender")
      const failure = yield* peer.client.Open(openRequest(sender, recipient)).pipe(
        Stream.runDrain,
        Effect.flip
      )
      assert.strictEqual(failure._tag, "RequestCapacityExceeded")
    })))

  it.effect("leaves no session behind when a handshake is refused", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness({
        limits: PeerRelayLimits.Values.make({
          ...PeerRelayLimits.defaults,
          maxSessionsPerSubject: 1
        })
      })

      // Every way a handshake can be refused, against a cap of one. If any of them registered the
      // session before deciding, the accepted handshake at the end would be refused for capacity —
      // and in production a client could exhaust its own cap with requests that never succeeded.
      const refusals: ReadonlyArray<
        [string, Partial<(typeof PeerRpc.OpenRpc.payloadSchema)["Type"]>]
      > = [
        ["version", { protocolVersion: 2 }],
        ["relay", { expectedRelayPeerId: recipientPeerId }],
        ["local", { expectedLocal: recipient }],
        ["retention", { receiptRetentionMillis: 1_000 }]
      ]
      for (const [label, override] of refusals) {
        const failure = yield* peer.client.Open({ ...openRequest(sender, recipient), ...override })
          .pipe(Stream.runDrain, Effect.flip)
        assert.notStrictEqual(failure._tag, "RequestCapacityExceeded", label)
      }
      peer.knobs.allow = () => false
      const denied = yield* peer.client.Open(openRequest(sender, recipient)).pipe(
        Stream.runDrain,
        Effect.flip
      )
      assert.strictEqual(denied._tag, "AccessDenied")

      peer.knobs.allow = () => true
      const accepted = yield* open(peer, sender, recipient)
      assert.strictEqual(accepted.opened._tag, "Opened")
    })))

  it.effect("keeps delivering other channels when one channel's head is withheld", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const documents = [
        { documentType: Task.name, documentId },
        { documentType: Task.name, documentId: otherDocumentId }
      ]
      const senderSession = yield* open(peer, sender, recipient, documents)
      const recipientSession = yield* open(peer, recipient, sender, documents)

      // Denies only the per-delivery Receive check for one document. Both handshakes ask for both
      // documents at once and every Send check is untouched, so nothing else is affected.
      peer.knobs.allow = (request) =>
        !(
          request.direction === "Receive" &&
          request.documents.length === 1 &&
          request.documents[0]!.documentId === documentId
        )

      // One withheld channel per concurrent delivery slot, admitted first so they are the older
      // heads, then one channel the recipient is plainly entitled to. A withheld head that keeps
      // waiting for a settlement it can never get holds its slot, and enough of them hold every
      // slot — at which point the authorized channel is never even looked at.
      // (Within a single channel a withheld head does correctly block what follows it: passing over
      // it would break the ordering the channel exists to preserve.)
      for (let index = 0; index < inboxOptions.maxConcurrentChannels; index++) {
        yield* push(
          peer,
          senderSession.opened.sessionId,
          yield* encodePayload(peer, { epoch: `withheld-${index}` }),
          `00000000000${index + 1}`
        )
        // Heads are ordered by admission time, and under virtual time these would otherwise all
        // share one timestamp and fall back to an arbitrary tiebreak — which would sometimes float
        // the authorized channel into the first batch and hide the starvation.
        yield* TestClock.adjust(10)
      }
      yield* push(
        peer,
        senderSession.opened.sessionId,
        yield* encodePayload(peer, { documentId: otherDocumentId, epoch: "clean" }),
        "000000000009"
      )

      // Blocks until the authorized message arrives. If the withheld head still held its channel
      // and its delivery slot, this would wait forever.
      const delivered = yield* Queue.take(recipientSession.events)
      assert.strictEqual(delivered._tag, "StoredMessage")
      if (delivered._tag !== "StoredMessage") return
      assert.strictEqual(delivered.relayMessageId, relayId("000000000009"))

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.isTrue(
        pending.some((message) => message.relayMessageId === relayId("000000000001")),
        "the withheld message stays durable rather than being settled away"
      )
    })))

  it.effect("refuses a session id presented under another principal's credential", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)
      yield* open(peer, recipient, sender)

      // A session id is unguessable, but unguessable is not the same as authenticated. The recipient
      // holds a perfectly valid credential; it is simply not the credential this session was opened
      // with, and nothing else in the handler re-derives the caller from the session.
      const payload = yield* encodePayload(peer)
      yield* Ref.set(peer.credential, "recipient")
      const stolen = yield* peer.client.Push({
        sessionId: senderSession.opened.sessionId,
        relayMessageId: relayId("000000000001"),
        payload
      }).pipe(Effect.flip)
      assert.strictEqual(stolen._tag, "SessionUnavailable")

      const stolenAck = yield* peer.client.Acknowledge({
        sessionId: senderSession.opened.sessionId,
        relayMessageId: relayId("000000000001"),
        claimToken: PeerRpc.ClaimToken.make("clm_00000000-0000-4000-8000-000000000000"),
        messageHash: "a".repeat(64)
      }).pipe(Effect.flip)
      assert.strictEqual(stolenAck._tag, "SessionUnavailable")

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 0, "a borrowed session admits nothing")
    })))

  it.effect("refuses a principal authenticated into another tenant", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      // Authentication succeeded — this is a real peer with a real credential. It simply belongs to
      // a tenant this relay does not serve, which is an authorization decision, not a mismatch.
      yield* Ref.set(peer.credential, "outsider")
      const failure = yield* peer.client.Open(openRequest(outsider, recipient)).pipe(
        Stream.runDrain,
        Effect.flip
      )
      assert.strictEqual(failure._tag, "AccessDenied")
    })))

  it.effect("checks every field of the claimed local identity independently", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      // One field at a time, so no clause can be masked by another failing alongside it. A
      // credential reused for a sibling device of the same subject differs only in peerId.
      for (
        const claimed of [
          { ...sender, subjectId: "recipient" },
          { ...sender, peerId: recipientPeerId },
          { ...sender, tenantId: "other" }
        ]
      ) {
        const failure = yield* peer.client.Open({
          ...openRequest(sender, recipient),
          expectedLocal: PeerAuthentication.PeerPrincipal.make(claimed)
        }).pipe(Stream.runDrain, Effect.flip)
        assert.strictEqual(failure._tag, "PeerMismatch", `expectedLocal ${JSON.stringify(claimed)}`)
      }
    })))

  it.effect("refuses a push when the unbounded decode risk is not acknowledged", () =>
    Effect.scoped(Effect.gen(function*() {
      // Ordinary Send authorization is granted; only the separate risk acknowledgement is withheld.
      // Relaying commits the recipient to an allocation-unbounded decode, so it is gated on its own
      // port that denies by default, and an ordinary grant must not be able to stand in for it.
      const peer = yield* harness({ knobs: { allowRisk: () => false } })
      const senderSession = yield* open(peer, sender, recipient)

      const payload = yield* encodePayload(peer)
      const denied = yield* push(peer, senderSession.opened.sessionId, payload).pipe(Effect.flip)
      assert.strictEqual(denied._tag, "AccessDenied")

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 0, "an unacknowledged risk admits nothing")
    })))

  it.effect("refuses a settlement once the recipient's receive authority is withdrawn", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)
      const recipientSession = yield* open(peer, recipient, sender)

      yield* push(peer, senderSession.opened.sessionId, yield* encodePayload(peer))
      const stored = yield* Queue.take(recipientSession.events)
      if (stored._tag !== "StoredMessage") return assert.fail("expected a delivery")

      // Settling is a durable mutation. A peer whose Receive authority has been withdrawn must not
      // be able to perform one, and the refusal has to say so rather than blaming the session: the
      // session is perfectly live, it is the authority behind it that is gone.
      peer.knobs.allow = (request) => request.direction !== "Receive"
      yield* Ref.set(peer.credential, "recipient")
      const denied = yield* peer.client.Acknowledge({
        sessionId: recipientSession.opened.sessionId,
        relayMessageId: stored.relayMessageId,
        claimToken: stored.claimToken,
        messageHash: stored.messageHash
      }).pipe(Effect.flip)
      assert.strictEqual(denied._tag, "AccessDenied")

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 1, "the terminal transition never ran")
    })))

  it.effect("surfaces an internal authorization fault as a defect rather than a typed error", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness({
        knobs: {
          allow: () => {
            throw new Error("authorization backend is broken")
          }
        }
      })

      // A broken authorization backend is not a decision about this caller. Reporting it as
      // AccessDenied would tell a legitimate client it is unauthorized and make it stop retrying,
      // and the wire contract encodes a defect as InternalError precisely to keep them distinct.
      const exit = yield* peer.client.Open(openRequest(sender, recipient)).pipe(
        Stream.runDrain,
        Effect.exit
      )
      assert.isTrue(exit._tag === "Failure")
      if (exit._tag !== "Failure") return
      assert.isTrue(Cause.hasDies(exit.cause), "the fault stays a defect")
      assert.isFalse(
        Cause.hasFails(exit.cause),
        "and is never laundered into a typed PeerRpcError"
      )
    })))

  it.effect("ends a session cleanly when its credential is invalidated", () =>
    Effect.scoped(Effect.gen(function*() {
      const revoked = yield* Latch.make(false)
      const peer = yield* harness({
        knobs: {
          authority: { validUntil: Number.MAX_SAFE_INTEGER, invalidated: revoked.await }
        }
      })
      const senderSession = yield* open(peer, sender, recipient)
      const recipientSession = yield* open(peer, recipient, sender)

      yield* push(peer, senderSession.opened.sessionId, yield* encodePayload(peer))
      const stored = yield* Queue.take(recipientSession.events)
      assert.strictEqual(stored._tag, "StoredMessage")

      // Nothing observes the credential except the relay, so it is the only thing that can end the
      // session when authority is withdrawn. The stream must end, not error: revocation is an
      // orderly close, and the client reconnects on its own terms.
      yield* revoked.open
      yield* Fiber.join(recipientSession.fiber)

      // The message was delivered but never settled, so it is still the relay's responsibility.
      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 1, "an unsettled message survives the revocation")
    })))

  it.effect("refuses a handshake whose retention window cannot cover its retry horizon", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      // A receipt has to outlive both the message and the window in which its sender may replay it,
      // or a redelivery lands after the deduplication record is gone and is applied twice.
      const failure = yield* peer.client.Open({
        ...openRequest(sender, recipient),
        receiptRetentionMillis: 1_000
      }).pipe(Stream.runDrain, Effect.flip)
      assert.strictEqual(failure._tag, "InvalidRequest")
    })))

  it.effect("requires both directions to be authorized at the handshake", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()

      // A session is bidirectional the moment it opens: the client may push, and the relay attaches
      // a delivery stream. Each direction therefore has to be granted on its own, and denying one
      // must not be answered by the other having said yes.
      for (const denied of ["Send", "Receive"] as const) {
        peer.knobs.allow = (request) => request.direction !== denied
        const failure = yield* peer.client.Open(openRequest(sender, recipient)).pipe(
          Stream.runDrain,
          Effect.flip
        )
        assert.strictEqual(failure._tag, "AccessDenied", `${denied} denied`)
      }
    })))

  it.effect("refuses a handshake whose grant lapsed while the other direction was authorized", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const openedAt = yield* Clock.currentTimeMillis

      // Authorization is I/O, so the two grants are not obtained at the same instant and neither is
      // still fresh by the time the session is minted. The authorization port itself only refuses a
      // grant that was already expired when it answered, so the relay has to re-check both against
      // the clock afterwards or it opens a session on authority that has since run out.
      peer.knobs.grantValidUntil = (request) =>
        request.direction === "Send" ? openedAt + 1_000 : Number.MAX_SAFE_INTEGER
      peer.knobs.onAuthorize = (request) => request.direction === "Receive" ? TestClock.adjust(2_000) : Effect.void

      const failure = yield* peer.client.Open(openRequest(sender, recipient)).pipe(
        Stream.runDrain,
        Effect.flip
      )
      assert.strictEqual(failure._tag, "AccessDenied")
    })))

  it.effect("refuses a handshake whose credential lapsed while authorization was in flight", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const openedAt = yield* Clock.currentTimeMillis

      // The middleware only proves the credential was live when the request arrived. Authorizing
      // takes time, so the credential is re-checked before the session exists — and an expired
      // credential is an authentication problem, not an authorization one, because the client has
      // to renew rather than ask for different permissions.
      peer.knobs.authority.validUntil = openedAt + 1_000
      peer.knobs.onAuthorize = (request) => request.direction === "Receive" ? TestClock.adjust(2_000) : Effect.void

      const failure = yield* peer.client.Open(openRequest(sender, recipient)).pipe(
        Stream.runDrain,
        Effect.flip
      )
      assert.strictEqual(failure._tag, "AuthenticationFailure")
    })))

  it.effect("re-authorizes each delivery against the document that message carries", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const documents = [
        { documentType: Task.name, documentId },
        { documentType: Task.name, documentId: otherDocumentId }
      ]
      const senderSession = yield* open(peer, sender, recipient, documents)
      const recipientSession = yield* open(peer, recipient, sender, documents)

      // Denies Receive for any request mentioning the first document. The handshake asked for both
      // at once, so a re-check that reused the handshake's document set would deny every delivery
      // on this session — including the second document, which the recipient is plainly entitled
      // to. Only a check keyed on the message's own document lets that one through.
      peer.knobs.allow = (request) =>
        !(
          request.direction === "Receive" &&
          request.documents.some((entry) => entry.documentId === documentId)
        )

      yield* push(
        peer,
        senderSession.opened.sessionId,
        yield* encodePayload(peer, { epoch: "withheld" }),
        "000000000001"
      )
      // Distinct channels, so the withheld message cannot block the other by ordering alone.
      yield* TestClock.adjust(10)
      yield* push(
        peer,
        senderSession.opened.sessionId,
        yield* encodePayload(peer, { documentId: otherDocumentId, epoch: "allowed" }),
        "000000000002"
      )

      const delivered = yield* Queue.take(recipientSession.events)
      assert.strictEqual(delivered._tag, "StoredMessage")
      if (delivered._tag !== "StoredMessage") return
      assert.strictEqual(delivered.relayMessageId, relayId("000000000002"))
      assert.strictEqual(delivered.document.documentId, otherDocumentId)

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.isTrue(
        pending.some((message) => message.relayMessageId === relayId("000000000001")),
        "the withheld document stays durable"
      )
    })))

  it.effect("re-authorizes each delivery against the peer that actually sent it", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()

      // A third device writes into the recipient's inbox while it is fully entitled to. The inbox
      // is keyed by the recipient alone, so this message now sits alongside every other sender's.
      const intruderSession = yield* open(peer, intruder, recipient)
      yield* Ref.set(peer.credential, "intruder")
      yield* peer.client.Push({
        sessionId: intruderSession.opened.sessionId,
        relayMessageId: relayId("000000000001"),
        payload: yield* encodePayload(peer, { epoch: "intruder" })
      })
      yield* TestClock.adjust(10)

      // Every grant naming the intruder is withdrawn, both directions and both ports. The recipient
      // is no longer entitled to anything at all from that device.
      const withoutIntruder = (subject: string, remote: string) => subject !== "intruder" && remote !== "intruder"
      peer.knobs.allow = (request) => withoutIntruder(request.principal.subjectId, request.remote.subjectId)
      peer.knobs.allowRisk = (request) => withoutIntruder(request.principal.subjectId, request.remote.subjectId)

      // The recipient reconnects to a different, still-authorized counterparty, and a message from
      // that counterparty arrives. Taking it proves the stream is live and has been dispatched
      // past the intruder's older message, so the intruder's absence below is observed rather than
      // merely not-yet-delivered.
      const recipientSession = yield* open(peer, recipient, sender)
      const senderSession = yield* open(peer, sender, recipient)
      yield* push(
        peer,
        senderSession.opened.sessionId,
        yield* encodePayload(peer, { epoch: "sender" }),
        "000000000002"
      )
      const delivered = yield* Queue.take(recipientSession.events)
      assert.strictEqual(delivered._tag, "StoredMessage")
      if (delivered._tag !== "StoredMessage") return
      assert.strictEqual(
        delivered.relayMessageId,
        relayId("000000000002"),
        "the authorized counterparty's message, not the intruder's older one"
      )

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.isTrue(
        pending.some((message) => message.relayMessageId === relayId("000000000001")),
        "the intruder's message stays durable rather than being handed over"
      )
    })))

  it.effect("re-authorizes a settlement against the peer whose message it settles", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()

      // Delivered while the intruder was authorized, over a session whose counterparty is somebody
      // else entirely. That is the ordinary shape of a busy device, not a contrived one.
      const intruderSession = yield* open(peer, intruder, recipient)
      yield* Ref.set(peer.credential, "intruder")
      yield* peer.client.Push({
        sessionId: intruderSession.opened.sessionId,
        relayMessageId: relayId("000000000001"),
        payload: yield* encodePayload(peer, { epoch: "intruder" })
      })

      const recipientSession = yield* open(peer, recipient, sender)
      const stored = yield* Queue.take(recipientSession.events)
      if (stored._tag !== "StoredMessage") return assert.fail("expected a delivery")
      assert.strictEqual(stored.sender.peerId, intruder.peerId)

      // Settling is a durable mutation performed on the authority of a Receive grant. The recipient
      // now holds none for this message's sender — only for the unrelated counterparty this socket
      // was opened against, which must not stand in for it.
      peer.knobs.allow = (request) =>
        request.principal.subjectId !== "intruder" && request.remote.subjectId !== "intruder"
      peer.knobs.allowRisk = (request) =>
        request.principal.subjectId !== "intruder" && request.remote.subjectId !== "intruder"

      yield* Ref.set(peer.credential, "recipient")
      const denied = yield* peer.client.Acknowledge({
        sessionId: recipientSession.opened.sessionId,
        relayMessageId: stored.relayMessageId,
        claimToken: stored.claimToken,
        messageHash: stored.messageHash
      }).pipe(Effect.flip)
      assert.strictEqual(denied._tag, "AccessDenied")

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 1, "the terminal transition never ran")
    })))

  it.effect("withholds a delivery whose unbounded decode risk is not acknowledged", () =>
    Effect.scoped(Effect.gen(function*() {
      // Ordinary authorization is granted throughout; only the risk acknowledgement is withheld,
      // and only for the receiving side of one document so the push itself still lands. Accepting a
      // delivery commits this peer to the same allocation-unbounded decode the sender needed
      // acknowledged, so the ordinary grant must not be able to stand in for it here either.
      const peer = yield* harness({
        knobs: {
          allowRisk: (request) =>
            !(
              request.direction === "Receive" &&
              request.documents.some((entry) => entry.documentId === documentId)
            )
        }
      })
      const documents = [
        { documentType: Task.name, documentId },
        { documentType: Task.name, documentId: otherDocumentId }
      ]
      const senderSession = yield* open(peer, sender, recipient, documents)
      const recipientSession = yield* open(peer, recipient, sender, documents)

      yield* push(
        peer,
        senderSession.opened.sessionId,
        yield* encodePayload(peer, { epoch: "withheld" }),
        "000000000001"
      )
      // Older, and on its own channel. Requiring the second message to arrive first is what makes
      // the withholding observable: an absence assertion here would pass on a delivery that simply
      // had not been dispatched yet.
      yield* TestClock.adjust(10)
      yield* push(
        peer,
        senderSession.opened.sessionId,
        yield* encodePayload(peer, { documentId: otherDocumentId, epoch: "allowed" }),
        "000000000002"
      )

      const delivered = yield* Queue.take(recipientSession.events)
      assert.strictEqual(delivered._tag, "StoredMessage")
      if (delivered._tag !== "StoredMessage") return
      assert.strictEqual(delivered.relayMessageId, relayId("000000000002"))

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.isTrue(
        pending.some((message) => message.relayMessageId === relayId("000000000001")),
        "the unacknowledged document stays durable"
      )

      // Withheld rather than dropped: acknowledging the risk and reconnecting produces it.
      peer.knobs.allowRisk = () => true
      const restored = yield* open(peer, recipient, sender, documents)
      const released = yield* Queue.take(restored.events)
      assert.strictEqual(released._tag, "StoredMessage")
      if (released._tag !== "StoredMessage") return
      assert.strictEqual(released.relayMessageId, relayId("000000000001"))
    })))

  it.effect("refuses a settlement whose unbounded decode risk is no longer acknowledged", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)
      const recipientSession = yield* open(peer, recipient, sender)

      yield* push(peer, senderSession.opened.sessionId, yield* encodePayload(peer))
      const stored = yield* Queue.take(recipientSession.events)
      if (stored._tag !== "StoredMessage") return assert.fail("expected a delivery")

      // Settling is the durable decision that the recipient has taken the document, so it carries
      // the same risk the delivery did. Withdrawing only the risk acknowledgement, with ordinary
      // Receive authorization untouched, has to be enough to refuse it.
      peer.knobs.allowRisk = () => false
      yield* Ref.set(peer.credential, "recipient")
      const denied = yield* peer.client.Acknowledge({
        sessionId: recipientSession.opened.sessionId,
        relayMessageId: stored.relayMessageId,
        claimToken: stored.claimToken,
        messageHash: stored.messageHash
      }).pipe(Effect.flip)
      assert.strictEqual(denied._tag, "AccessDenied")

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 1, "the terminal transition never ran")
    })))

  it.effect("keeps a session past its liveness deadline for as long as the socket lives", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)
      const recipientSession = yield* open(peer, recipient, sender)

      // A cluster cannot rely on disconnects announcing themselves, so the entity reaps any session
      // that stops proving it is alive. Nothing on the wire refreshes it — the client does not know
      // the session exists — so the relay drives the heartbeat itself, and without it every session
      // would go dark one deadline after opening no matter how healthy the connection is.
      yield* TestClock.adjust(Duration.toMillis(inboxOptions.sessionDeadline) * 4)

      yield* push(peer, senderSession.opened.sessionId, yield* encodePayload(peer))
      const delivered = yield* Queue.take(recipientSession.events)
      assert.strictEqual(delivered._tag, "StoredMessage")
    })))

  it.effect("reports an entity call that outlives its timeout as unavailability", () =>
    Effect.scoped(Effect.gen(function*() {
      const admitting = yield* Latch.make(false)
      const peer = yield* harness({
        // `Sharding` retries `EntityNotAssignedToRunner` and `RunnerUnavailable` forever, so an
        // entity call during a rebalance never returns on its own. Held here at the durable write
        // because that is the one place real SQLite will not stall.
        store: (real) => ({ ...real, admit: (request) => admitting.await.pipe(Effect.andThen(real.admit(request))) })
      })
      const senderSession = yield* open(peer, sender, recipient)

      const pushing = yield* push(peer, senderSession.opened.sessionId, yield* encodePayload(peer))
        .pipe(Effect.flip, Effect.forkScoped)
      yield* TestClock.adjust(serverOptions.entityCallTimeout)

      const failure = yield* Fiber.join(pushing)
      // Retryable: the write may or may not have landed, and the sender still holds its outbox copy.
      assert.strictEqual(failure._tag, "ServerUnavailable")
      yield* admitting.open
    })))

  it.effect("serves a session through the composed relay layer", () =>
    Effect.scoped(Effect.gen(function*() {
      // The exported layer, not the handlers alone: it is what a deployment actually builds, and it
      // is the only thing that composes the retention singleton and checks that the heartbeat and
      // the session deadline agree.
      const peer = yield* composed()
      const senderSession = yield* open(peer, sender, recipient)
      const recipientSession = yield* open(peer, recipient, sender)

      yield* push(peer, senderSession.opened.sessionId, yield* encodePayload(peer))
      const delivered = yield* Queue.take(recipientSession.events)
      assert.strictEqual(delivered._tag, "StoredMessage")
    })))

  it.effect("refuses to build a relay whose heartbeat its session deadline cannot survive", () =>
    Effect.gen(function*() {
      // Two independently configured values that have to agree. If they do not, every session is
      // reaped on a fixed cycle and no client can hold a delivery stream — a total outage visible
      // only under one particular configuration, so it is refused where it is written down.
      for (
        const broken of [
          { heartbeatInterval: inboxOptions.sessionDeadline },
          { entityCallTimeout: 0 }
        ]
      ) {
        const exit = yield* Effect.scoped(composed(broken)).pipe(Effect.exit)
        assert.isTrue(exit._tag === "Failure", JSON.stringify(broken))
        if (exit._tag !== "Failure") continue
        // A defect, not a typed failure: nothing downstream can recover from a relay that was
        // configured to reap its own sessions.
        assert.isTrue(Cause.hasDies(exit.cause), JSON.stringify(broken))
      }
    }))
})
