import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
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

/** A real, authenticated peer that simply belongs to a tenant this relay does not serve. */
const outsider = PeerAuthentication.PeerPrincipal.make({
  tenantId: "other",
  subjectId: "outsider",
  peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000004")
})

const serverOptions: RelayServer.Options = {
  tenantId: "tenant",
  peerId: relayPeerId,
  heartbeatIntervalMillis: 30_000,
  entityCallTimeoutMillis: 30_000
}

const inboxOptions: RelayInbox.Options = {
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
    receiptRetentionMillis: PeerRelayLimits.defaults.maximumReceiptRetentionMillis,
    senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis
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
  authority: { validUntil: number; invalidated: Effect.Effect<void> }
}

const harness = (options?: {
  readonly limits?: PeerRelayLimits.Values
  readonly knobs?: Partial<Knobs>
}) =>
  Effect.gen(function*() {
    const knobs: Knobs = {
      allow: options?.knobs?.allow ?? (() => true),
      allowRisk: options?.knobs?.allowRisk ?? (() => true),
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
              validUntil: Number.MAX_SAFE_INTEGER,
              invalidated: Effect.never
            })
            : Effect.fail(new PeerRpcError.AccessDenied()),
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
    const cluster = RelayInbox.layer(inboxOptions).pipe(
      Layer.provideMerge(Sharding.layer),
      Layer.provide(Runners.layerNoop),
      Layer.provideMerge(MessageStorage.layerMemory),
      Layer.provide(RunnerStorage.layerMemory),
      Layer.provide(RunnerHealth.layerNoop),
      Layer.provide(TestShardingConfig),
      Layer.provideMerge(
        SqlRelayInboxStore.layer.pipe(
          Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
          Layer.provide(Layer.succeed(Crypto.Crypto)(crypto)),
          Layer.orDie
        )
      )
    )

    const context = yield* Layer.build(
      RelayServer.layerHandlers(serverOptions).pipe(
        Layer.provideMerge(cluster),
        Layer.provide(Layer.mergeAll(
          Layer.succeed(Crypto.Crypto)(crypto),
          Layer.succeed(PeerRelayLimits.PeerRelayLimits)(limits),
          Layer.succeed(PeerRelayAuthorization.PeerRelayAuthorization)(authorization)
        ))
      )
    )

    const credential = yield* Ref.make("sender")
    const principals = new Map([["sender", sender], ["recipient", recipient], ["outsider", outsider]])
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

  it.effect("honours a rejection end to end", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)
      const recipientSession = yield* open(peer, recipient, sender)

      yield* push(peer, senderSession.opened.sessionId, yield* encodePayload(peer))
      const stored = yield* Queue.take(recipientSession.events)
      if (stored._tag !== "StoredMessage") return assert.fail("expected a delivery")

      // A rejection is a durable decision too: the recipient looked at the message and refused it,
      // so redelivering it would loop forever.
      yield* Ref.set(peer.credential, "recipient")
      yield* peer.client.Reject({
        sessionId: recipientSession.opened.sessionId,
        relayMessageId: stored.relayMessageId,
        claimToken: stored.claimToken,
        messageHash: stored.messageHash,
        reason: "ApplicationRejected"
      })

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 0, "a rejected message is terminal")
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

  it.effect("fails Open with an authentication failure when the credential has expired", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness({ knobs: { authority: { validUntil: 0, invalidated: Effect.never } } })
      const failure = yield* peer.client.Open(openRequest(sender, recipient)).pipe(
        Stream.runDrain,
        Effect.flip
      )
      // Expiry at the handshake is an authentication problem, not an authorization one.
      assert.strictEqual(failure._tag, "AuthenticationFailure")
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

  it.effect("commits an admitted push exactly once even if the grant is withdrawn afterwards", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)

      const payload = yield* encodePayload(peer)
      yield* push(peer, senderSession.opened.sessionId, payload)
      // Revoked after admission. The message is already the relay's responsibility; withdrawing the
      // grant must not retroactively destroy custody the sender was told the relay had taken.
      peer.knobs.allow = () => false

      const inbox = yield* inboxKeyOf(peer, recipient)
      const usage = yield* peer.store.usage(inbox)
      assert.strictEqual(usage.pendingCount, 1, "the admitted message survives the revocation")
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

  it.effect("refuses an unauthenticated push and admits nothing", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness()
      const senderSession = yield* open(peer, sender, recipient)

      const payload = yield* encodePayload(peer)
      yield* Ref.set(peer.credential, "nobody")
      const failure = yield* peer.client.Push({
        sessionId: senderSession.opened.sessionId,
        relayMessageId: relayId("000000000001"),
        payload
      }).pipe(Effect.flip)
      assert.strictEqual(failure._tag, "AuthenticationFailure")

      const pending = yield* peer.store.pendingHeads(
        yield* inboxKeyOf(peer, recipient),
        { limit: 10 }
      )
      assert.strictEqual(pending.length, 0, "an unauthenticated push creates no inbox state")
    })))

  it.effect("refuses to open more sessions than a subject is allowed", () =>
    Effect.scoped(Effect.gen(function*() {
      const peer = yield* harness({
        limits: PeerRelayLimits.Values.make({
          ...PeerRelayLimits.defaults,
          maxSessionsPerSubject: 1
        })
      })
      yield* open(peer, sender, recipient)

      // Sessions are the front door's only unbounded resource and an authenticated client can open
      // them in a loop, so the cap has to be refused rather than absorbed.
      const failure = yield* peer.client.Open(openRequest(sender, recipient)).pipe(
        Stream.runDrain,
        Effect.flip
      )
      assert.strictEqual(failure._tag, "RequestCapacityExceeded")
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

  it.effect("pins the handshake protocol version so a foreign version fails to decode", () =>
    Effect.gen(function*() {
      // `Opened` carries a literal, not a number, so a client cannot be talked into continuing
      // against a relay speaking a version it does not implement.
      assert.throws(() =>
        Schema.decodeUnknownSync(PeerRpc.OpenEvent)({
          _tag: "Opened",
          protocolVersion: PeerRpc.protocolVersion + 1,
          sessionId: "ses_00000000-0000-4000-8000-000000000001",
          remotePeerId: recipientPeerId,
          authenticatedLocal: sender
        })
      )
    }))

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
})
