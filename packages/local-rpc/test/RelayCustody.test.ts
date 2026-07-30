import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as DocumentStore from "@lucas-barake/effect-local-sql/DocumentStore"
import * as PeerRelayClientRuntime from "@lucas-barake/effect-local-sql/PeerRelayClientRuntime"
import * as PeerRelayOutboxLimits from "@lucas-barake/effect-local-sql/PeerRelayOutboxLimits"
import * as PeerRelayReceiptLimits from "@lucas-barake/effect-local-sql/PeerRelayReceiptLimits"
import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import * as ReplicaGate from "@lucas-barake/effect-local-sql/ReplicaGate"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as TestReplica from "@lucas-barake/effect-local-test/TestReplica"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
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
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { encodeInboxKey } from "../src/internal/relayInboxKey.js"
import * as PeerAuthentication from "../src/PeerAuthentication.js"
import * as PeerAuthenticator from "../src/PeerAuthenticator.js"
import * as PeerCredentials from "../src/PeerCredentials.js"
import * as PeerRelayAuthorization from "../src/PeerRelayAuthorization.js"
import * as PeerRelayLimits from "../src/PeerRelayLimits.js"
import * as PeerRpc from "../src/PeerRpc.js"
import * as PeerRpcError from "../src/PeerRpcError.js"
import type * as RelayInbox from "../src/RelayInbox.js"
import * as RelayInboxStore from "../src/RelayInboxStore.js"
import * as RelayServer from "../src/RelayServer.js"
import * as RpcPeerTransport from "../src/RpcPeerTransport.js"
import * as SqlRelayInboxStore from "../src/SqlRelayInboxStore.js"

const Task = Document.make("Task", {
  // `labels` rather than a scalar alone: a list merges additively, so the recipient holding the
  // sender's label is evidence the change arrived rather than evidence of who won a coin toss.
  schema: Schema.Struct({ title: Schema.String, labels: Schema.Array(Schema.String) }),
  version: 1
})

const AddLabel = Mutation.make("Task.AddLabel", {
  document: Task,
  payload: Schema.String
})

const definition = ReplicaDefinition.make({
  name: "relay-custody",
  documents: DocumentSet.make(Task),
  mutations: [AddLabel],
  projections: [],
  queries: []
})

const Handlers = AddLabel.toLayer(({ draft, payload }) => {
  draft.labels.push(payload)
  return undefined
})

const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
const localPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
const remotePeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000003")

const localPrincipal = PeerAuthentication.PeerPrincipal.make({
  tenantId: "tenant",
  subjectId: "local",
  peerId: localPeerId
})
const remotePrincipal = PeerAuthentication.PeerPrincipal.make({
  tenantId: "tenant",
  subjectId: "remote",
  peerId: remotePeerId
})

const replicaLimits: ReplicaLimits.Values = {
  ...TestReplica.defaultLimits,
  // Deliberately large. Every `TestClock.adjust` that brings a runner up also advances the
  // `Effect.timeout` in `PeerSession.send` and in `PeerSession` relay settlement.
  maxPeerSendMillis: 3_600_000
}

const outboxLimits: PeerRelayOutboxLimits.Values = PeerRelayOutboxLimits.defaults
const receiptLimits: PeerRelayReceiptLimits.Values = PeerRelayReceiptLimits.defaults

/**
 * The explicit cluster configuration. `internal/clusterStorage.layer` hardcodes
 * `ShardingConfig.layerFromEnv()`, whose defaults (15s entity termination, 10s message poll)
 * stall under the virtual clock `it.effect` installs.
 */
const TestShardingConfig = ShardingConfig.layer({
  shardsPerGroup: 16,
  entityMailboxCapacity: replicaLimits.maxQueuedRpc,
  entityTerminationTimeout: 0,
  entityMessagePollInterval: 5_000,
  sendRetryInterval: 100
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

const serverOptions: RelayServer.Options = {
  tenantId: "tenant",
  peerId: relayPeerId,
  heartbeatInterval: Duration.seconds(30),
  entityCallTimeout: Duration.seconds(30)
}

// ---------------------------------------------------------------------------
// Relay backend. Real RelayServer front door, real RelayInbox entity on its own
// real Sharding, real SqlRelayInboxStore over its own SQLite database.
// ---------------------------------------------------------------------------

const relayBackend = Effect.gen(function*() {
  const crypto = yield* Crypto.Crypto.pipe(Effect.provide(NodeCrypto.layer))

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

  // A second, separate database. The relay's inbox custody is not the client's replica.
  const relayStore = SqlRelayInboxStore.layer.pipe(
    Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
    Layer.provide(Layer.succeed(Crypto.Crypto)(crypto)),
    Layer.orDie
  )

  const cluster = Sharding.layer.pipe(
    Layer.provide(Runners.layerNoop),
    Layer.provideMerge(MessageStorage.layerMemory),
    Layer.provide(RunnerStorage.layerMemory),
    Layer.provide(RunnerHealth.layerNoop),
    Layer.provide(TestShardingConfig),
    Layer.provideMerge(relayStore)
  )

  const context = yield* Layer.build(
    RelayServer.layer({
      ...serverOptions,
      inbox: inboxOptions,
      maintenance: {
        interval: Duration.minutes(1),
        batchLimit: 100,
        terminalRetention: inboxOptions.terminalRetention,
        enabled: true
      }
    }).pipe(
      Layer.provideMerge(cluster),
      Layer.provide(Layer.mergeAll(
        Layer.succeed(Crypto.Crypto)(crypto),
        Layer.succeed(PeerRelayLimits.PeerRelayLimits)(PeerRelayLimits.defaults),
        Layer.succeed(PeerRelayAuthorization.PeerRelayAuthorization)(authorization)
      ))
    )
  )

  const principals = new Map([
    ["local", localPrincipal],
    ["remote", remotePrincipal]
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
            validUntil: Number.MAX_SAFE_INTEGER,
            invalidated: Effect.never
          })
      }
    }),
    Effect.provideService(PeerRelayLimits.PeerRelayLimits, PeerRelayLimits.defaults)
  )

  /** One `PeerRpc.RpcClient` per authenticated subject, over the same relay handlers. */
  const clientFor = (subject: string) =>
    RpcTest.makeClient(PeerRpc.Rpcs).pipe(
      Effect.provideContext(
        Context.add(context, PeerAuthentication.PeerAuthentication, authentication)
      ),
      Effect.provide(PeerAuthentication.layerClient),
      Effect.provideService(PeerCredentials.PeerCredentials, {
        get: Effect.succeed(Redacted.make(subject))
      })
    )

  // Shard assignment and acquisition run on scheduled fibers.
  yield* TestClock.adjust(5000)

  const store = Context.get(context, RelayInboxStore.RelayInboxStore)
  return { clientFor, crypto, store }
})

// ---------------------------------------------------------------------------
// Client replica. Real PeerSync.layerRelay, real PeerRelayOutbox, real
// PeerRelayClientRuntime, real DocumentEntity on its own real Sharding.
// ---------------------------------------------------------------------------

/**
 * A function rather than module constants: layer memoization is by reference, so two peers
 * sharing one value would silently be one replica and the exchange would prove nothing.
 */
const clientStack = () => {
  const Base = Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    ReplicaLimits.layer(replicaLimits),
    PeerRelayReceiptLimits.layer(receiptLimits),
    PeerRelayOutboxLimits.layer(outboxLimits)
  )
  const ReplicaLayer = SqlReplica.layerRelay(definition, { projections: [] }).pipe(
    Layer.provide(Handlers),
    Layer.provideMerge(Base),
    Layer.orDie
  )
  const Store = DocumentStore.layer.pipe(Layer.provide(ReplicaLayer))
  const Runtime = PeerRelayClientRuntime.layerSql.pipe(Layer.provide(ReplicaLayer), Layer.orDie)
  return Layer.mergeAll(ReplicaLayer, Store, Runtime)
}

/** The recipient's options are the sender's with the two principals swapped. */
const transportOptions = (
  self: PeerAuthentication.PeerPrincipal,
  peer: PeerAuthentication.PeerPrincipal,
  senderReplicaIncarnation: Identity.ReplicaIncarnation,
  documentId: Identity.DocumentId
): RpcPeerTransport.Options => ({
  expectedLocal: { tenantId: self.tenantId, subjectId: self.subjectId, peerId: self.peerId },
  senderReplicaIncarnation,
  expectedRelayPeerId: relayPeerId,
  remote: { subjectId: peer.subjectId, peerId: peer.peerId },
  documents: [{ document: Task, documentId }],
  definition,
  receiptRetentionMillis: receiptLimits.receiptRetentionMillis,
  senderRetryHorizonMillis: Duration.toMillis(PeerRelayLimits.defaults.maximumSenderRetryHorizon),
  replayBatchSize: 16
})

const inboxKeyOf = (principal: PeerAuthentication.PeerPrincipal, crypto: Crypto.Crypto) =>
  encodeInboxKey(principal).pipe(Effect.provideService(Crypto.Crypto, crypto), Effect.orDie)

const outboxRows = (sql: SqlClient.SqlClient) =>
  sql<{ readonly relay_message_id: string }>`
    SELECT relay_message_id FROM effect_local_peer_relay_outbox
  `.pipe(Effect.orDie)

/**
 * The recipient is a clone rather than an independently created replica: a document created
 * twice for the same id is two lineages, and this exchange syncs two copies of one.
 */
const seedPair = Effect.gen(function*() {
  const senderContext = yield* Layer.build(clientStack())
  const recipientContext = yield* Layer.build(clientStack())
  const sender = Context.get(senderContext, Replica.Replica)
  const recipient = Context.get(recipientContext, Replica.Replica)

  const documentId = yield* sender.create(Task, {
    commandId: yield* Identity.makeCommandId,
    value: { title: "one", labels: [] }
  })
  const backup = yield* sender.exportBackup({ maxBytes: replicaLimits.maxBackupBytes }).pipe(Stream.runCollect)
  yield* recipient.restoreBackup({
    expectedDefinitionHash: definition.hash,
    installationId: yield* Identity.makeBackupInstallationId,
    maxBytes: replicaLimits.maxBackupBytes,
    mode: "clone",
    source: Stream.fromIterable(backup)
  })

  return {
    documentId,
    sender: {
      context: senderContext,
      replica: sender,
      store: Context.get(senderContext, DocumentStore.DocumentStore),
      sql: Context.get(senderContext, SqlClient.SqlClient),
      incarnation: (yield* Context.get(senderContext, ReplicaGate.ReplicaGate).current).incarnation
    },
    recipient: {
      context: recipientContext,
      replica: recipient,
      store: Context.get(recipientContext, DocumentStore.DocumentStore),
      sql: Context.get(recipientContext, SqlClient.SqlClient),
      incarnation: (yield* Context.get(recipientContext, ReplicaGate.ReplicaGate).current).incarnation
    }
  }
})

describe("relay custody against a real relay", () => {
  it.effect("reports exact public command custody and complete document confirmation", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const backend = yield* relayBackend
        const client = yield* backend.clientFor("local")
        const recipientClient = yield* backend.clientFor("remote")
        const { documentId, recipient, sender } = yield* seedPair
        const firstMutationCommandId = yield* Identity.makeCommandId
        const secondMutationCommandId = yield* Identity.makeCommandId
        yield* sender.replica.mutate(AddLabel, {
          commandId: firstMutationCommandId,
          documentId,
          payload: "first"
        })
        yield* sender.replica.mutate(AddLabel, {
          commandId: secondMutationCommandId,
          documentId,
          payload: "second"
        })
        yield* TestClock.adjust(5000)

        yield* Effect.scoped(Effect.gen(function*() {
          const options = transportOptions(
            localPrincipal,
            remotePrincipal,
            sender.incarnation,
            documentId
          )
          const commandHashes = yield* sender.sql<{
            readonly command_id: string
            readonly change_hash: string
          }>`SELECT command_id, change_hash
            FROM effect_local_command_delivery_changes
            WHERE command_id IN (${firstMutationCommandId}, ${secondMutationCommandId})
            ORDER BY command_id, change_hash`
          assert.strictEqual(commandHashes.length, 2)
          const expectedChangeHashes = new Set(commandHashes.map((row) => row.change_hash))
          const unavailable: PeerRpc.RpcClient = {
            ...client,
            Push: ((request) =>
              PeerSyncEnvelope.decodeSyncEnvelope(request.payload, replicaLimits).pipe(
                Effect.provideService(Crypto.Crypto, backend.crypto),
                Effect.orDie,
                Effect.flatMap((envelope) =>
                  envelope.writerProvenance.some((entry) => expectedChangeHashes.has(entry.changeHash))
                    ? Effect.fail<PeerRpcError.PeerRpcError>(new PeerRpcError.ServerUnavailable())
                    : client.Push(request)
                )
              )) as PeerRpc.RpcClient["Push"]
          }
          const attempt = yield* Effect.exit(Effect.scoped(Effect.gen(function*() {
            yield* RpcPeerTransport.makeSession(
              recipientClient,
              transportOptions(
                remotePrincipal,
                localPrincipal,
                recipient.incarnation,
                documentId
              )
            ).pipe(Effect.provideContext(recipient.context))
            const session = yield* RpcPeerTransport.makeSession(unavailable, options).pipe(
              Effect.provideContext(sender.context)
            )
            assert.isFalse(yield* session.durableConfirmation(documentId))
            yield* session.markDirty(documentId)
            yield* session.flush
            yield* TestClock.adjust(5000)
            yield* session.awaitDisconnect
          })))
          assert.strictEqual(attempt._tag, "Failure")
          assert.strictEqual((yield* outboxRows(sender.sql)).length, 1)
          const deliveryHashes = yield* sender.sql<{
            readonly relay_message_id: string
            readonly change_hash: string
          }>`SELECT relay_message_id, change_hash
            FROM effect_local_peer_relay_delivery_changes
            ORDER BY relay_message_id, change_hash`
          const admittedHashes = yield* sender.sql<{
            readonly relay_message_id: string
            readonly change_hash: string
          }>`SELECT relay_message_id, json_extract(provenance.value, '$.changeHash') AS change_hash
            FROM effect_local_peer_relay_outbox, json_each(writer_provenance) AS provenance
            ORDER BY relay_message_id, change_hash`
          assert.isAbove(admittedHashes.length, 0)
          assert.deepStrictEqual(
            new Set(deliveryHashes.map((row) => row.change_hash)),
            new Set(admittedHashes.map((row) => row.change_hash))
          )
          for (const changeHash of expectedChangeHashes) {
            assert.isTrue(
              deliveryHashes.some((row) => row.change_hash === changeHash),
              `relay message must contain command change ${changeHash}`
            )
          }

          for (
            const commandId of [
              firstMutationCommandId,
              secondMutationCommandId
            ]
          ) {
            const delivery = yield* sender.replica.lookupCommandDelivery(commandId)
            assert.strictEqual(delivery._tag, "TrackedCommand")
            if (delivery._tag !== "TrackedCommand") continue
            assert.strictEqual(delivery.localChangeCount, 1)
            assert.strictEqual(delivery.destinations.length, 1)
            const destination = delivery.destinations[0]!
            assert.strictEqual(destination.relayPeerId, relayPeerId)
            assert.strictEqual(destination.remotePeerId, remotePeerId)
            assert.strictEqual(destination.state._tag, "PendingRelayCustody")
            if (destination.state._tag === "PendingRelayCustody") {
              assert.strictEqual(destination.state.acceptedChangeCount, 0)
              assert.strictEqual(destination.state.pendingChangeCount, 1)
            }
          }

          const session = yield* RpcPeerTransport.makeSession(client, options).pipe(
            Effect.provideContext(sender.context)
          )
          assert.isTrue(yield* session.durableConfirmation(documentId))

          for (
            const commandId of [
              firstMutationCommandId,
              secondMutationCommandId
            ]
          ) {
            const delivery = yield* sender.replica.lookupCommandDelivery(commandId)
            assert.strictEqual(delivery._tag, "TrackedCommand")
            if (delivery._tag !== "TrackedCommand") continue
            assert.strictEqual(delivery.localChangeCount, 1)
            assert.strictEqual(delivery.destinations.length, 1)
            const destination = delivery.destinations[0]!
            assert.strictEqual(destination.relayPeerId, relayPeerId)
            assert.strictEqual(destination.remotePeerId, remotePeerId)
            assert.strictEqual(destination.state._tag, "RelayCustodyAccepted")
            if (destination.state._tag === "RelayCustodyAccepted") {
              assert.strictEqual(destination.state.acceptedChangeCount, 1)
            }
          }

          const heads = yield* backend.store
            .pendingHeads(yield* inboxKeyOf(remotePrincipal, backend.crypto), { limit: 10 })
            .pipe(Effect.orDie)
          const head = heads.find((candidate) =>
            candidate.envelope.writerProvenance.some((entry) => expectedChangeHashes.has(entry.changeHash))
          )
          assert.isDefined(head)
          if (head === undefined) return
          assert.strictEqual(head.envelope.sender.peerId, localPeerId)
          assert.strictEqual(head.envelope.recipient.peerId, remotePeerId)
          assert.strictEqual(head.envelope.sender.replicaIncarnation, sender.incarnation)
          assert.strictEqual(head.envelope.document.documentId, documentId)
          assert.strictEqual(head.envelope.document.documentType, Task.name)
          assert.isAbove(head.envelope.payload.byteLength, 0)

          // A count on the recipient's inbox alone would not catch a message filed back to its sender.
          const localHeads = yield* backend.store
            .pendingHeads(yield* inboxKeyOf(localPrincipal, backend.crypto), { limit: 10 })
            .pipe(Effect.orDie)
          assert.isFalse(
            localHeads.some((candidate) =>
              candidate.envelope.writerProvenance.some((entry) => expectedChangeHashes.has(entry.changeHash))
            )
          )

          assert.strictEqual((yield* outboxRows(sender.sql)).length, 0)
        }))
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("carries a real client change through the relay into the recipient's replica", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const backend = yield* relayBackend
        const senderClient = yield* backend.clientFor("local")
        const recipientClient = yield* backend.clientFor("remote")
        const { documentId, recipient, sender } = yield* seedPair

        // One label each. With a change on one side only, the recipient's head set collapses onto the
        // sender's on merge, and agreement would follow from the forward direction alone.
        for (
          const [side, label] of [
            [sender, "from-sender"],
            [recipient, "from-recipient"]
          ] as const
        ) {
          yield* side.replica.mutate(AddLabel, {
            commandId: yield* Identity.makeCommandId,
            documentId,
            payload: label
          })
        }
        assert.deepStrictEqual(
          [...(yield* recipient.store.load(Task, documentId).pipe(Effect.orDie)).encoded.labels],
          ["from-recipient"],
          "the recipient has only its own label before the exchange"
        )
        assert.deepStrictEqual(
          [...(yield* sender.store.load(Task, documentId).pipe(Effect.orDie)).encoded.labels],
          ["from-sender"],
          "the sender has only its own label before the exchange"
        )
        yield* TestClock.adjust(5000)

        yield* Effect.scoped(Effect.gen(function*() {
          // The recipient subscribes first so the relay has a session to dispatch to.
          yield* RpcPeerTransport.makeSession(
            recipientClient,
            transportOptions(remotePrincipal, localPrincipal, recipient.incarnation, documentId)
          ).pipe(Effect.provideContext(recipient.context))
          const senderSession = yield* RpcPeerTransport.makeSession(
            senderClient,
            transportOptions(localPrincipal, remotePrincipal, sender.incarnation, documentId)
          ).pipe(Effect.provideContext(sender.context))

          yield* senderSession.markDirty(documentId)
          yield* senderSession.flush
          yield* TestClock.adjust(5000)

          // Sorted because the merge order of two concurrent list insertions is Automerge's choice.
          const recipientLoaded = yield* recipient.store.load(Task, documentId).pipe(Effect.orDie)
          const senderLoaded = yield* sender.store.load(Task, documentId).pipe(Effect.orDie)
          assert.deepStrictEqual(
            [...recipientLoaded.encoded.labels].toSorted(),
            ["from-recipient", "from-sender"]
          )
          assert.deepStrictEqual(
            [...senderLoaded.encoded.labels].toSorted(),
            ["from-recipient", "from-sender"]
          )

          assert.deepStrictEqual(
            [...recipientLoaded.materializedHeads].toSorted(),
            [...senderLoaded.materializedHeads].toSorted(),
            "both replicas agree on the document after the exchange"
          )

          for (const principal of [localPrincipal, remotePrincipal]) {
            assert.deepStrictEqual(
              yield* backend.store
                .pendingHeads(yield* inboxKeyOf(principal, backend.crypto), { limit: 10 })
                .pipe(Effect.orDie),
              []
            )
          }
          assert.strictEqual((yield* outboxRows(sender.sql)).length, 0)
          assert.strictEqual((yield* outboxRows(recipient.sql)).length, 0)
        }))
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))

  it.effect("replays an outbox entry the session that admitted it never handed over", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const backend = yield* relayBackend
        const client = yield* backend.clientFor("local")
        const { documentId, sender } = yield* seedPair
        yield* sender.replica.mutate(AddLabel, {
          commandId: yield* Identity.makeCommandId,
          documentId,
          payload: "from-sender"
        })
        yield* TestClock.adjust(5000)

        const options = transportOptions(localPrincipal, remotePrincipal, sender.incarnation, documentId)
        const recipientInbox = yield* inboxKeyOf(remotePrincipal, backend.crypto)

        // The situation the outbox exists for: the change is already durably admitted, so dropping it
        // here would lose it with nobody holding a copy.
        const unavailable: PeerRpc.RpcClient = {
          ...client,
          Push: ((() => Effect.fail(new PeerRpcError.ServerUnavailable())) as PeerRpc.RpcClient["Push"])
        }
        // A failed push is fatal to the connection, so closing the scope reports it too.
        const attempt = yield* Effect.exit(Effect.scoped(Effect.gen(function*() {
          const session = yield* RpcPeerTransport.makeSession(unavailable, options).pipe(
            Effect.provideContext(sender.context)
          )
          yield* session.markDirty(documentId)
          yield* session.flush
        })))
        assert.strictEqual(attempt._tag, "Failure")

        const pending = yield* outboxRows(sender.sql)
        assert.strictEqual(pending.length, 1)

        // Nothing here marks dirty or flushes, so the entry can only arrive through connect-time replay.
        // Identified by relay message id because the new session also sends one of its own.
        yield* Effect.scoped(Effect.gen(function*() {
          yield* RpcPeerTransport.makeSession(client, options).pipe(Effect.provideContext(sender.context))

          const heads = yield* backend.store.pendingHeads(recipientInbox, { limit: 10 }).pipe(Effect.orDie)
          assert.isTrue(
            heads.some((head) => head.relayMessageId === pending[0]!.relay_message_id),
            "the entry the failed session admitted reached the relay through the replay"
          )
          assert.strictEqual((yield* outboxRows(sender.sql)).length, 0)
        }))
      }).pipe(Effect.provide(NodeCrypto.layer))
    ))
})
