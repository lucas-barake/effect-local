import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as RpcTest from "effect/unstable/rpc/RpcTest"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerCredentials from "../src/PeerCredentials.js"
import * as PeerRelayAuthorization from "../src/PeerRelayAuthorization.js"
import * as PeerRelayIngress from "../src/PeerRelayIngress.js"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRelayStore from "../src/PeerRelayStore.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcServer from "../src/PeerRpcServer.js"
import * as SqlPeerRelayStore from "../src/SqlPeerRelayStore.js"

const Task = Document.make("Task", {
  schema: Schema.Struct({ title: Schema.String }),
  version: 1
})
const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const senderPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const recipientPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
const relayMessageId = Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000001")

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

const openRequest = (
  principal: PeerAuthentication.PeerPrincipal,
  remote: PeerAuthentication.PeerPrincipal
) =>
  PeerRpc.OpenRpc.payloadSchema.make({
    protocolVersion: PeerRpc.protocolVersion,
    expectedRelayPeerId: relayPeerId,
    expectedLocal: principal,
    senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
    remote: {
      subjectId: remote.subjectId,
      peerId: remote.peerId
    },
    documents: [{ documentType: Task.name, documentId }],
    receiptRetentionMillis: PeerRelayLimits.defaults.maximumReceiptRetentionMillis,
    senderRetryHorizonMillis: PeerRelayLimits.defaults.maximumSenderRetryHorizonMillis
  })

const authorize: PeerRelayAuthorization.Authorize = (request) =>
  Effect.succeed({
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

const authorizeUnsafe: PeerRelayAuthorization.AuthorizeUnsafeUnboundedAutomerge3Decode = (
  request
) =>
  Effect.succeed({
    _tag: "UnsafeUnboundedAutomerge3DecodeGrant",
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

const ingress = PeerRelayIngress.PeerRelayIngress.of({
  address: { _tag: "UnixAddress", path: "relay-rpc-integration" },
  reserveOutbound: (bytes) =>
    Effect.succeed({
      bytes,
      release: Effect.void,
      transferToCurrentRequest: Effect.void
    }),
  usage: Effect.succeed({
    connections: 0,
    reservedBytes: 0,
    byteReservationWaiters: 0
  }),
  await: Effect.never
})

const RelaySyncEnvelopeJson = Schema.fromJsonString(
  Schema.toCodecJson(PeerSyncEnvelope.SyncEnvelope)
)

describe("PeerRpc production composition", () => {
  it.effect("round trips durable custody and owns session cleanup through RpcTest", () =>
    Effect.scoped(Effect.gen(function*() {
      const limits = PeerRelayLimits.defaults
      const crypto = yield* Crypto.Crypto.pipe(Effect.provide(NodeCrypto.layer))
      const fs = yield* FileSystem.FileSystem.pipe(Effect.provide(NodeFileSystem.layer))
      const directory = yield* fs.makeTempDirectoryScoped()
      const sql = yield* SqliteClient.make({ filename: `${directory}/relay.sqlite` }).pipe(
        Effect.provide(Reactivity.layer)
      )
      const store = yield* SqlPeerRelayStore.make.pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provideService(PeerRelayLimits.PeerRelayLimits, limits)
      )
      const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
        Effect.provide(PeerRelayAuthorization.layer(authorize, authorizeUnsafe))
      )
      const handlers = yield* Layer.build(
        PeerRpcServer.layerHandlers({
          tenantId: "tenant",
          peerId: relayPeerId
        }).pipe(
          Layer.provide(Layer.mergeAll(
            Layer.succeed(Crypto.Crypto, crypto),
            Layer.succeed(PeerRelayLimits.PeerRelayLimits, limits),
            Layer.succeed(PeerRelayAuthorization.PeerRelayAuthorization, authorization),
            Layer.succeed(PeerRelayStore.PeerRelayStore, store),
            Layer.succeed(PeerRelayIngress.PeerRelayIngress, ingress)
          ))
        )
      )
      const runtime = Context.get(handlers, PeerRpcServer.PeerRpcServerRuntime)
      const credential = yield* Ref.make("sender")
      const principals = new Map([
        ["sender", sender],
        ["recipient", recipient]
      ])
      const authentication = yield* PeerAuthentication.PeerAuthentication.pipe(
        Effect.provide(PeerAuthentication.layerServer),
        Effect.provideService(PeerAuthenticator.PeerAuthenticator, {
          authenticate: (secret) => {
            const principal = principals.get(Redacted.value(secret))
            return principal === undefined
              ? Effect.die("Unknown integration credential")
              : Effect.succeed({
                principal,
                validUntil: Number.MAX_SAFE_INTEGER,
                invalidated: Effect.never
              })
          }
        }),
        Effect.provideService(PeerRelayLimits.PeerRelayLimits, limits)
      )
      const client = yield* RpcTest.makeClient(PeerRpc.Rpcs).pipe(
        Effect.provideContext(
          Context.add(
            handlers,
            PeerAuthentication.PeerAuthentication,
            authentication
          )
        ),
        Effect.provide(PeerAuthentication.layerClient),
        Effect.provideService(PeerCredentials.PeerCredentials, {
          get: Ref.get(credential).pipe(Effect.map(Redacted.make))
        })
      )

      const invalidVersion = yield* client.Open({
        ...openRequest(sender, recipient),
        protocolVersion: 2
      }).pipe(Stream.runDrain, Effect.flip)
      assert.strictEqual(invalidVersion._tag, "UnsupportedVersion")
      assert.strictEqual((yield* runtime.usage).sessions, 0)

      assert.throws(() =>
        Schema.decodeUnknownSync(PeerRpc.OpenEvent)({
          _tag: "Opened",
          protocolVersion: 2,
          sessionId: "ses_00000000-0000-4000-8000-000000000001",
          remotePeerId: recipientPeerId,
          authenticatedLocal: sender
        })
      )
      assert.strictEqual((yield* runtime.usage).sessions, 0)

      const senderEvents = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      yield* Ref.set(credential, "sender")
      const senderOpen = yield* client.Open(openRequest(sender, recipient)).pipe(
        Stream.runForEach((event) => Queue.offer(senderEvents, event)),
        Effect.forkScoped
      )
      const senderOpened = yield* Queue.take(senderEvents)
      assert.strictEqual(senderOpened._tag, "Opened")
      if (senderOpened._tag !== "Opened") return assert.fail("Expected sender Opened event")

      const recipientEvents = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      yield* Ref.set(credential, "recipient")
      const recipientOpen = yield* client.Open(openRequest(recipient, sender)).pipe(
        Stream.runForEach((event) => Queue.offer(recipientEvents, event)),
        Effect.forkScoped
      )
      const recipientOpened = yield* Queue.take(recipientEvents)
      assert.strictEqual(recipientOpened._tag, "Opened")
      if (recipientOpened._tag !== "Opened") return assert.fail("Expected recipient Opened event")

      const message = Uint8Array.of(1, 2, 3)
      const messageHash = yield* Canonical.digest(message).pipe(
        Effect.provideService(Crypto.Crypto, crypto)
      )
      const payload = new TextEncoder().encode(
        yield* Schema.encodeEffect(RelaySyncEnvelopeJson)({
          connectionEpoch: "epoch",
          sequence: 0,
          documentId,
          documentType: Task.name,
          messageHash,
          message,
          lineage: Identity.genesisLineage,
          writerProvenance: []
        })
      )

      yield* Ref.set(credential, "sender")
      yield* client.Push({
        sessionId: senderOpened.sessionId,
        relayMessageId,
        payload
      })

      const stored = yield* Queue.take(recipientEvents)
      assert.strictEqual(stored._tag, "StoredMessage")
      if (stored._tag !== "StoredMessage") return assert.fail("Expected durable delivery")
      assert.deepStrictEqual(stored.payload, payload)
      assert.strictEqual((yield* store.usage()).activeCount, 1)

      yield* Ref.set(credential, "recipient")
      yield* client.Acknowledge({
        sessionId: recipientOpened.sessionId,
        relayMessageId: stored.relayMessageId,
        claimToken: stored.claimToken,
        messageHash: stored.messageHash
      })
      const afterAcknowledge = yield* store.usage()
      assert.strictEqual(afterAcknowledge.activeCount, 0)
      assert.strictEqual(afterAcknowledge.retainedCount, 1)

      yield* Fiber.interrupt(senderOpen)
      yield* Fiber.interrupt(recipientOpen)
      assert.strictEqual((yield* runtime.usage).sessions, 0)

      const incumbentEvents = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      yield* Ref.set(credential, "sender")
      const incumbent = yield* client.Open(openRequest(sender, recipient)).pipe(
        Stream.runForEach((event) => Queue.offer(incumbentEvents, event)),
        Effect.forkScoped
      )
      assert.strictEqual((yield* Queue.take(incumbentEvents))._tag, "Opened")

      const replacementEvents = yield* Queue.unbounded<PeerRpc.OpenEvent>()
      const replacement = yield* client.Open(openRequest(sender, recipient)).pipe(
        Stream.runForEach((event) => Queue.offer(replacementEvents, event)),
        Effect.forkScoped
      )
      assert.strictEqual((yield* Queue.take(replacementEvents))._tag, "Opened")
      yield* Fiber.await(incumbent)
      assert.strictEqual((yield* runtime.usage).sessions, 1)

      yield* Fiber.interrupt(replacement)
      assert.strictEqual((yield* runtime.usage).sessions, 0)
    })))
})
