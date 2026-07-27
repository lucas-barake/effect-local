import { NodeCrypto, NodeSocket, NodeSocketServer } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as MessageStorage from "effect/unstable/cluster/MessageStorage"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as SocketServer from "effect/unstable/socket/SocketServer"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerCredentials from "../src/PeerCredentials.js"
import * as PeerRelayAuthorization from "../src/PeerRelayAuthorization.js"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import * as RelayInbox from "../src/RelayInbox.js"
import * as RelayServer from "../src/RelayServer.js"
import * as SqlRelayInboxStore from "../src/SqlRelayInboxStore.js"

/**
 * The wire format, end to end.
 *
 * Every other relay suite drives the front door through `RpcTest.makeClient` or the entity through
 * `Entity.makeTestClient`, and both of those bypass serialization entirely — they hand the handler
 * the decoded value. So nothing else in this package proves that the relay's schemas survive a real
 * encode and decode, which is exactly what changed when the bespoke length-prefixed framing was
 * replaced by standard Effect RPC. This is that proof, over a real socket with real JSON codecs.
 */

const Task = Document.make("Task", {
  schema: Schema.Struct({ title: Schema.String }),
  version: 1
})

const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const senderPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const recipientPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")
const relayMessageId = Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000001")
const secondRelayMessageId = Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000002")

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

const openRequest = (
  principal: PeerAuthentication.PeerPrincipal,
  remote: PeerAuthentication.PeerPrincipal
) =>
  PeerRpc.OpenRpc.payloadSchema.make({
    protocolVersion: PeerRpc.protocolVersion,
    expectedRelayPeerId: relayPeerId,
    expectedLocal: principal,
    senderReplicaIncarnation: Identity.ReplicaIncarnation.make(1),
    remote: { subjectId: remote.subjectId, peerId: remote.peerId },
    documents: [{ documentType: Task.name, documentId }],
    receiptRetentionMillis: Duration.toMillis(PeerRelayLimits.defaults.maximumReceiptRetention),
    senderRetryHorizonMillis: Duration.toMillis(PeerRelayLimits.defaults.maximumSenderRetryHorizon)
  })

const RelaySyncEnvelopeJson = Schema.fromJsonString(
  Schema.toCodecJson(PeerSyncEnvelope.SyncEnvelope)
)

const tcpPort = (address: SocketServer.Address) => {
  assert.strictEqual(address._tag, "TcpAddress")
  return (address as SocketServer.TcpAddress).port
}

describe("relay wire format", () => {
  it.live(
    "round trips Open, Push and Acknowledge over a socket with real serialization",
    () =>
      Effect.scoped(Effect.gen(function*() {
        const scope = yield* Effect.scope
        const crypto = yield* Crypto.Crypto.pipe(Effect.provide(NodeCrypto.layer))
        const limits = PeerRelayLimits.defaults

        const authorization = yield* PeerRelayAuthorization.PeerRelayAuthorization.pipe(
          Effect.provide(PeerRelayAuthorization.layer(
            (request) =>
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
              }),
            (request) =>
              Effect.succeed({
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
          ))
        )

        const principals = new Map([["sender", sender], ["recipient", recipient]])
        const authenticator = PeerAuthenticator.PeerAuthenticator.of({
          authenticate: (secret) => {
            const principal = principals.get(Redacted.value(secret))
            return principal === undefined
              ? Effect.fail(new PeerRpcError.AuthenticationFailure())
              : Effect.succeed({
                principal,
                validUntil: Number.MAX_SAFE_INTEGER,
                invalidated: Effect.never
              })
          }
        })

        const cluster = RelayInbox.layer(inboxOptions).pipe(
          Layer.provideMerge(Sharding.layer),
          Layer.provide(Runners.layerNoop),
          Layer.provideMerge(MessageStorage.layerMemory),
          Layer.provide(RunnerStorage.layerMemory),
          Layer.provide(RunnerHealth.layerNoop),
          Layer.provide(ShardingConfig.layer({ entityTerminationTimeout: 0 })),
          Layer.provide(
            SqlRelayInboxStore.layer.pipe(
              Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
              Layer.provide(Layer.succeed(Crypto.Crypto)(crypto)),
              Layer.orDie
            )
          )
        )

        // The socket server is built first so its assigned port can be handed to the client.
        const listener = yield* Layer.buildWithScope(NodeSocketServer.layer({ port: 0 }), scope)
        const socketServer = Context.get(listener, SocketServer.SocketServer)
        const port = tcpPort(socketServer.address)

        yield* Layer.buildWithScope(
          RpcServer.layer(PeerRpc.Rpcs).pipe(
            Layer.provide(RelayServer.layerHandlers({
              tenantId: "tenant",
              peerId: relayPeerId,
              heartbeatInterval: Duration.seconds(30),
              entityCallTimeout: Duration.seconds(30)
            })),
            Layer.provide(PeerAuthentication.layerServer),
            Layer.provide(RpcServer.layerProtocolSocketServer),
            Layer.provide(Layer.succeed(SocketServer.SocketServer)(socketServer)),
            // The real codec. This is the whole point of the test.
            Layer.provide(RpcSerialization.layerJson),
            Layer.provide(cluster),
            Layer.provide(Layer.mergeAll(
              Layer.succeed(Crypto.Crypto)(crypto),
              Layer.succeed(PeerRelayLimits.PeerRelayLimits)(limits),
              Layer.succeed(PeerRelayAuthorization.PeerRelayAuthorization)(authorization),
              Layer.succeed(PeerAuthenticator.PeerAuthenticator)(authenticator)
            ))
          ),
          scope
        )

        const clientFor = (subject: string) =>
          Effect.gen(function*() {
            const credential = yield* Ref.make(subject)
            const context = yield* Layer.buildWithScope(
              RpcClient.layerProtocolSocket().pipe(
                Layer.provide(NodeSocket.layerNet({ port })),
                Layer.provide(RpcSerialization.layerJson)
              ),
              scope
            )
            return yield* PeerRpc.makeRpcClient.pipe(
              Effect.provideService(RpcClient.Protocol, Context.get(context, RpcClient.Protocol)),
              Effect.provide(PeerAuthentication.layerClient),
              Effect.provideService(PeerCredentials.PeerCredentials, {
                get: Ref.get(credential).pipe(Effect.map(Redacted.make))
              })
            )
          })

        const senderClient = yield* clientFor("sender")
        const recipientClient = yield* clientFor("recipient")

        const senderEvents = yield* Queue.unbounded<PeerRpc.OpenEvent>()
        yield* senderClient.Open(openRequest(sender, recipient)).pipe(
          Stream.runForEach((event) => Queue.offer(senderEvents, event)),
          Effect.forkScoped
        )
        const senderOpened = yield* Queue.take(senderEvents)
        assert.strictEqual(senderOpened._tag, "Opened")
        if (senderOpened._tag !== "Opened") return

        const recipientEvents = yield* Queue.unbounded<PeerRpc.OpenEvent>()
        yield* recipientClient.Open(openRequest(recipient, sender)).pipe(
          Stream.runForEach((event) => Queue.offer(recipientEvents, event)),
          Effect.forkScoped
        )
        const recipientOpened = yield* Queue.take(recipientEvents)
        assert.strictEqual(recipientOpened._tag, "Opened")

        const message = Uint8Array.of(1, 2, 3, 250, 251, 252)
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

        yield* senderClient.Push({
          sessionId: senderOpened.sessionId,
          relayMessageId,
          payload
        })

        const stored = yield* Queue.take(recipientEvents)
        assert.strictEqual(stored._tag, "StoredMessage")
        if (stored._tag !== "StoredMessage") return

        // Bytes, not a structural near-miss. A `Uint8Array` has to survive JSON in both directions,
        // and the high bytes above are there to catch an encoding that mangles them.
        assert.deepStrictEqual(stored.payload, payload)
        assert.strictEqual(stored.relayMessageId, relayMessageId)
        assert.strictEqual(stored.relayPeerId, relayPeerId)
        assert.strictEqual(stored.messageHash, messageHash)
        assert.deepStrictEqual(stored.recipient, recipient)
        assert.strictEqual(stored.sender.peerId, senderPeerId)

        yield* recipientClient.Acknowledge({
          sessionId: (recipientOpened as PeerRpc.Opened).sessionId,
          relayMessageId: stored.relayMessageId,
          claimToken: stored.claimToken,
          messageHash: stored.messageHash
        })

        // Reconnecting proves the acknowledgement crossed the wire and was applied durably. Rather
        // than waiting a while and asserting nothing arrived, push a second message and require it
        // to be the first thing the new session sees: a redelivery of the acknowledged message
        // would necessarily arrive ahead of it, being the older head of the same channel.
        const afterEvents = yield* Queue.unbounded<PeerRpc.OpenEvent>()
        yield* recipientClient.Open(openRequest(recipient, sender)).pipe(
          Stream.runForEach((event) => Queue.offer(afterEvents, event)),
          Effect.forkScoped
        )
        assert.strictEqual((yield* Queue.take(afterEvents))._tag, "Opened")

        const second = Uint8Array.of(9, 8, 7)
        const secondHash = yield* Canonical.digest(second).pipe(
          Effect.provideService(Crypto.Crypto, crypto)
        )
        yield* senderClient.Push({
          sessionId: senderOpened.sessionId,
          relayMessageId: secondRelayMessageId,
          payload: new TextEncoder().encode(
            yield* Schema.encodeEffect(RelaySyncEnvelopeJson)({
              connectionEpoch: "epoch",
              sequence: 1,
              documentId,
              documentType: Task.name,
              messageHash: secondHash,
              message: second,
              lineage: Identity.genesisLineage,
              writerProvenance: []
            })
          )
        })

        const next = yield* Queue.take(afterEvents)
        assert.strictEqual(next._tag, "StoredMessage")
        if (next._tag !== "StoredMessage") return
        assert.strictEqual(
          next.relayMessageId,
          secondRelayMessageId,
          "an acknowledged message must not be redelivered"
        )
      })),
    { timeout: 30_000 }
  )
})
