import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as PrimaryKey from "effect/PrimaryKey"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import * as Entity from "effect/unstable/cluster/Entity"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as SqlMessageStorage from "effect/unstable/cluster/SqlMessageStorage"
import * as SqlRunnerStorage from "effect/unstable/cluster/SqlRunnerStorage"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as DocumentEntity from "../src/DocumentEntity.js"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
import * as PeerSync from "../src/PeerSync.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as SqlReplica from "../src/SqlReplica.js"
import { decodeJson, encodeJson } from "./helpers/json.js"

describe("DocumentEntity", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const Rename = Mutation.make("Rename", {
    document: Task,
    payload: Schema.String,
    success: Schema.String
  })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [Rename],
    projections: [],
    queries: []
  })
  const limits = {
    maxBackupBytes: 1_000_000,
    maxChunkBytes: 64_000,
    maxArchiveRecords: 1_000,
    maxJsonDepth: 32,
    maxConflictDepth: 64,
    maxConflictNodes: 100_000,
    maxConflictAlternatives: 10_000,
    maxConflictPathSegments: 128,
    maxConflictValueBytes: 16 * 1024 * 1024,
    maxConflictSourceChanges: 100_000,
    maxConflictSourceOperations: 100_000,
    maxConflictSourceBytes: 64 * 1024 * 1024,
    maxSyncMessageBytes: 64_000,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1_000,
    maxSyncOperationsPerMessage: 1_000,
    maxPendingBytesPerDocument: 1_000_000,
    maxPendingBytesPerPeer: 1_000_000,
    maxPendingBytesPerReplica: 2_000_000,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1_000,
    maxPendingChangesPerPeer: 1_000,
    maxPendingChangesPerReplica: 2_000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 20_000,
    maxSessions: 8,
    maxStreamsPerSession: 4,
    maxInFlightPerSession: 16,
    maxQueuedRpc: 32,
    maxQueuedPermits: 32,
    maxActiveRestores: 32,
    maxRestoresPerSession: 16,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  } satisfies ReplicaLimits.Values
  const TestShardingConfig = Layer.merge(
    ShardingConfig.layer({
      shardsPerGroup: 16,
      entityMailboxCapacity: limits.maxQueuedRpc,
      entityTerminationTimeout: 0,
      entityMessagePollInterval: 5_000,
      sendRetryInterval: 100,
      refreshAssignmentsInterval: 0
    }),
    NodeCrypto.layer
  )
  const syncResult = {
    reply: null,
    heads: [],
    acceptedHeads: [],
    commitSequence: Identity.CommitSequence.make(1),
    observedByPeer: false,
    durableConfirmation: false,
    duplicate: false
  }
  const peerSync = (receive: PeerSync.PeerSync["Service"]["receive"] = () => Effect.succeed(syncResult)) =>
    PeerSync.PeerSync.of({
      withDocumentInvalidation: (_documentId, effect) => effect,
      invalidateDocument: () => Effect.void,
      open: (peerId) =>
        Effect.succeed({
          peerId,
          connectionEpoch: "epoch",
          replicaIncarnation: Identity.ReplicaIncarnation.make(0)
        }),
      reset: () => Effect.void,
      generate: () => Effect.succeed({ outbound: null, observedByPeer: false, dirty: false }),
      receive,
      enqueue: (_session, reply) =>
        Effect.succeed({ ...reply, sendSequence: 0, lineage: Identity.genesisLineage, writerProvenance: [] }),
      pending: () => Effect.succeed([]),
      markSent: () => Effect.succeed(false),
      pruneRelayReceipts: Effect.succeed(0)
    })
  const throwTypeError = (): never => {
    // oxlint-disable-next-line effect/noNewError
    const error = new TypeError("Expected a primary key payload")
    // oxlint-disable-next-line effect/noThrowStatement
    throw error
  }
  const keyOf = (payload: unknown) => {
    if (PrimaryKey.isPrimaryKey(payload)) {
      return PrimaryKey.value(payload)
    }
    return throwTypeError()
  }
  const replicaGate = (permit: ReplicaGate.Permit) =>
    ReplicaGate.ReplicaGate.of({
      current: Effect.succeed(permit),
      claiming: Effect.succeed(false),
      shared: Effect.die("unused"),
      admit: Effect.die("unused"),
      claim: (use) => use(permit),
      refresh: Effect.succeed(permit),
      validate: () => Effect.void
    })

  it("uses the complete command identity as the persisted primary key", () => {
    const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000001")
    const base = {
      replicaIncarnation: Identity.ReplicaIncarnation.make(1),
      writerGeneration: Identity.WriterGeneration.make(2),
      commandId,
      documentType: "Task",
      payload: new Uint8Array([1]),
      requestHash: "hash-a"
    }
    const key = keyOf(DocumentEntity.Create.payloadSchema.make(base))
    assert.strictEqual(key, `1:${commandId}:hash-a`)
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.Create.payloadSchema.make({
        ...base,
        replicaIncarnation: Identity.ReplicaIncarnation.make(2)
      }))
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.Create.payloadSchema.make({
        ...base,
        commandId: Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000002")
      }))
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.Create.payloadSchema.make({ ...base, requestHash: "hash-b" }))
    )
    assert.strictEqual(
      key,
      keyOf(DocumentEntity.Create.payloadSchema.make({
        ...base,
        writerGeneration: Identity.WriterGeneration.make(3)
      }))
    )
  })

  it("uses peer connection sequence and message hash as the sync primary key", () => {
    const peerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
    const base = {
      replicaIncarnation: Identity.ReplicaIncarnation.make(1),
      peerId,
      connectionEpoch: "connection",
      localConnectionEpoch: "local-connection",
      receiveSequence: 2,
      documentType: Task.name,
      messageHash: "hash-a",
      message: new Uint8Array([1]),
      writerProvenance: [{
        changeHash: "a".repeat(64),
        writerSchemaVersion: Task.version,
        writerDefinitionHash: definition.hash
      }]
    }
    const key = keyOf(DocumentEntity.ApplySync.payloadSchema.make(base))
    assert.strictEqual(
      key,
      encodeJson([1, peerId, "connection", 2, "hash-a", base.writerProvenance, "", null])
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.ApplySync.payloadSchema.make({
        ...base,
        lineage: Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000001")
      }))
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.ApplySync.payloadSchema.make({
        ...base,
        receiveSequence: 3
      }))
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.ApplySync.payloadSchema.make({
        ...base,
        messageHash: "hash-b"
      }))
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.ApplySync.payloadSchema.make({
        ...base,
        writerProvenance: [{ ...base.writerProvenance[0], writerSchemaVersion: Task.version + 1 }]
      }))
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.ApplySync.payloadSchema.make({
        ...base,
        checkpointTransfer: Uint8Array.of(1, 2, 3)
      }))
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.ApplySync.payloadSchema.make({
        ...base,
        writerProvenance: [{ ...base.writerProvenance[0], writerDefinitionHash: "different-definition" }]
      }))
    )
  })

  it("keeps sync primary keys collision free for opaque wire fields", () => {
    const peerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
    const validHash = "a".repeat(64)
    const base = {
      replicaIncarnation: Identity.ReplicaIncarnation.make(1),
      peerId,
      localConnectionEpoch: "local",
      documentType: Task.name,
      message: new Uint8Array([1]),
      writerProvenance: [{
        changeHash: "a".repeat(64),
        writerSchemaVersion: Task.version,
        writerDefinitionHash: definition.hash
      }]
    }
    const first = keyOf(DocumentEntity.ApplySync.payloadSchema.make({
      ...base,
      connectionEpoch: "epoch",
      receiveSequence: 1,
      messageHash: `2:${validHash}`
    }))
    const second = keyOf(DocumentEntity.ApplySync.payloadSchema.make({
      ...base,
      connectionEpoch: "epoch:1",
      receiveSequence: 2,
      messageHash: validHash
    }))

    assert.notStrictEqual(first, second)
  })

  it("round trips a persisted ApplySync payload with and without a lineage key", () => {
    const lineage = "lin_22222222-2222-4222-9222-222222222222"
    const wire = {
      replicaIncarnation: 1,
      peerId: "peer_00000000-0000-4000-8000-000000000001",
      connectionEpoch: "connection",
      localConnectionEpoch: "local-connection",
      receiveSequence: 2,
      documentType: Task.name,
      messageHash: "hash-a",
      message: "AQID",
      writerProvenance: [{
        changeHash: "a".repeat(64),
        writerSchemaVersion: Task.version,
        writerDefinitionHash: definition.hash
      }]
    }
    // A message enqueued by a build without lineage has no such key. `Persisted` means it is
    // replayed through this schema, so decoding it must succeed rather than fail the whole payload.
    const replayed = Schema.decodeUnknownSync(DocumentEntity.ApplySync.payloadSchema)(wire)
    assert.isUndefined(replayed.lineage)
    assert.deepStrictEqual(
      Schema.encodeUnknownSync(DocumentEntity.ApplySync.payloadSchema)(replayed),
      wire
    )

    const carried = Schema.decodeUnknownSync(DocumentEntity.ApplySync.payloadSchema)({ ...wire, lineage })
    assert.strictEqual(carried.lineage, lineage)
    assert.deepStrictEqual(
      Schema.encodeUnknownSync(DocumentEntity.ApplySync.payloadSchema)(carried),
      { ...wire, lineage }
    )

    assert.throws(() =>
      Schema.decodeUnknownSync(DocumentEntity.ApplySync.payloadSchema)({
        ...wire,
        lineage: "x".repeat(257)
      })
    )
  })

  it("scopes relay sync primary keys by the complete sender identity", () => {
    const relayPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
    const senderPeerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002")
    const relayMessageId = Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000001")
    const hash = "a".repeat(64)
    const base = {
      replicaIncarnation: Identity.ReplicaIncarnation.make(1),
      peerId: relayPeerId,
      connectionEpoch: "sender-epoch",
      localConnectionEpoch: "local-epoch",
      receiveSequence: 2,
      documentType: Task.name,
      messageHash: hash,
      message: new Uint8Array([1]),
      writerProvenance: [],
      relay: {
        relayMessageId,
        relayPeerId,
        senderTenantId: "tenant",
        senderSubjectId: "sender-a",
        senderPeerId,
        senderReplicaIncarnation: Identity.ReplicaIncarnation.make(3),
        messageHash: hash,
        outerEnvelopeDigest: "b".repeat(64),
        receiptExpiresAt: "2026-08-02T00:00:00.000Z",
        encodedSize: 10
      }
    }
    const key = keyOf(DocumentEntity.ApplySync.payloadSchema.make(base))
    assert.strictEqual(
      key,
      keyOf(DocumentEntity.ApplySync.payloadSchema.make({
        ...base,
        connectionEpoch: "another-epoch",
        receiveSequence: 99
      }))
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.ApplySync.payloadSchema.make({
        ...base,
        relay: { ...base.relay, senderSubjectId: "sender-b" }
      }))
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.ApplySync.payloadSchema.make({
        ...base,
        relay: { ...base.relay, outerEnvelopeDigest: "c".repeat(64) }
      }))
    )
  })

  it.effect("replays a committed command and rolls back a failed command with its durable reply", () =>
    Effect.gen(function*() {
      const database = Layer.merge(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer
      )
      const inputs = Layer.mergeAll(
        database,
        ReplicaLimits.layer(limits),
        Rename.toLayer(({ draft, payload }) => {
          draft.title = payload
          return payload
        })
      )
      const services = SqlReplica.servicesLayerWithBindings(definition, { projections: [] })
      const peerSyncLayer = PeerSync.layer.pipe(Layer.provideMerge(services))
      const cluster = Sharding.layer.pipe(
        Layer.provideMerge(Runners.layerNoop),
        Layer.provideMerge(SqlMessageStorage.layerWith({ prefix: ClusterStorage.messagePrefix })),
        Layer.provide([
          Layer.orDie(SqlRunnerStorage.layerWith({ prefix: ClusterStorage.runnerPrefix })),
          RunnerHealth.layerNoop
        ]),
        Layer.provide(TestShardingConfig)
      )
      const live = DocumentEntity.layer(definition).pipe(
        Layer.provideMerge(peerSyncLayer),
        Layer.provideMerge(cluster),
        Layer.provideMerge(inputs)
      )

      yield* Effect.gen(function*() {
        yield* TestClock.adjust(1)
        const sql = yield* SqlClient.SqlClient
        const gate = yield* ReplicaGate.ReplicaGate
        const permit = yield* gate.current
        const documentId = yield* Identity.makeDocumentId
        const commandId = yield* Identity.makeCommandId
        const failedCommandId = yield* Identity.makeCommandId
        const makeClient = yield* DocumentEntity.DocumentEntity.client
        const client = makeClient(documentId)
        const value = { title: "first" }
        const requestHash = yield* CommandExecutor.createRequestHash({
          incarnation: permit.incarnation,
          commandId,
          document: Task,
          documentId,
          encoded: yield* Document.encode(Task, documentId, value)
        })
        const request = {
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId,
          documentType: Task.name,
          payload: new TextEncoder().encode(encodeJson(value)),
          requestHash
        }

        const first = yield* client.Create(request)
        const replayed = yield* client.Create(request)
        assert.deepStrictEqual(replayed, first)
        assert.deepStrictEqual(
          yield* Schema.decodeUnknownEffect(
            Schema.toCodecJson(CommandOutcome.schema(Identity.DocumentId, Schema.Never))
          )(decodeJson(new TextDecoder().decode(replayed))),
          CommandOutcome.durablyCommitted(commandId, documentId)
        )

        const failed = yield* Effect.flip(client.Create({
          ...request,
          commandId: failedCommandId,
          payload: new TextEncoder().encode("{"),
          requestHash: "failed-create-hash"
        }))
        if (!ReplicaError.isReplicaError(failed)) {
          assert.fail(`Expected ReplicaError, got ${failed._tag}`)
        }
        assert.strictEqual(failed.reason._tag, "ProtocolMismatch")

        const durableRows = yield* sql<{
          readonly documents: number
          readonly receipts: number
        }>`SELECT
          (SELECT COUNT(*) FROM effect_local_documents) AS documents,
          (SELECT COUNT(*) FROM effect_local_command_receipts) AS receipts`
        assert.deepStrictEqual(durableRows[0], { documents: 1, receipts: 1 })
        const clusterRows = yield* sql<{
          readonly processed: number
          readonly replies: number
        }>`SELECT m.processed AS processed,
            (SELECT COUNT(*) FROM ${sql(`${ClusterStorage.messagePrefix}_replies`)} r
              WHERE r.request_id = m.request_id) AS replies
          FROM ${sql(`${ClusterStorage.messagePrefix}_messages`)} m
          WHERE m.kind = 0
          ORDER BY m.rowid`
        assert.deepStrictEqual(clusterRows, [
          { processed: 1, replies: 1 },
          { processed: 0, replies: 0 }
        ])
      }).pipe(Effect.scoped, Effect.provide(live))
    }))

  it.effect("decodes commands and encodes their outcomes", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const commandId = yield* Identity.makeCommandId
        const documentId = yield* Identity.makeDocumentId
        const permit = {
          replicaId: (yield* Identity.makeReplicaId),
          incarnation: Identity.ReplicaIncarnation.make(1),
          writerGeneration: Identity.WriterGeneration.make(2)
        }
        const executor = CommandExecutor.CommandExecutor.of({
          create: (_document, options) =>
            Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, options.documentId)),
          mutate: (_mutation, options) =>
            Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, options.payload)),
          delete: (_document, options) => Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, undefined)),
          resolve: (_document, options) =>
            Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, undefined)),
          lookupCreate: (id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupMutation: (_mutation, id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupDelete: (id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupResolution: (_document, options) => Effect.succeed(CommandOutcome.unknown(options.commandId))
        })
        const receivedProvenance = yield* Ref.make<
          ReadonlyArray<{
            readonly changeHash: string
            readonly writerSchemaVersion: number
            readonly writerDefinitionHash: string
          }>
        >([])
        const receivedLineages = yield* Ref.make<ReadonlyArray<Identity.DocumentLineage>>([])
        const receivedRelay = yield* Ref.make<PeerSync.RelayReceipt | undefined>(undefined)
        const sync = peerSync((_document, _documentId, _session, input) =>
          Effect.all([
            Ref.set(receivedProvenance, input.writerProvenance),
            Ref.update(receivedLineages, (lineages) => [
              ...lineages,
              input.lineage ?? Identity.genesisLineage
            ]),
            Ref.set(receivedRelay, input.relay)
          ]).pipe(Effect.as(syncResult))
        )
        const makeClient = yield* Entity.makeTestClient(
          DocumentEntity.DocumentEntity,
          DocumentEntity.layer(definition).pipe(
            Layer.provide(Layer.succeed(CommandExecutor.CommandExecutor, executor)),
            Layer.provide(Layer.succeed(ReplicaGate.ReplicaGate, replicaGate(permit))),
            Layer.provide(Layer.succeed(PeerSync.PeerSync, sync)),
            Layer.provide(ReplicaLimits.layer(limits))
          )
        )
        const client = yield* makeClient(documentId)
        const bytes = yield* client.Create({
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId,
          documentType: Task.name,
          payload: new TextEncoder().encode(encodeJson({ title: "first" })),
          requestHash: "create-hash"
        })
        const outcome = yield* Schema.decodeUnknownEffect(
          Schema.toCodecJson(CommandOutcome.schema(Identity.DocumentId, Schema.Never))
        )(
          decodeJson(new TextDecoder().decode(bytes))
        )
        assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, documentId))

        const mutationCommandId = yield* Identity.makeCommandId
        const mutationBytes = yield* client.Mutate({
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId: mutationCommandId,
          documentType: Task.name,
          mutationTag: Rename.name,
          payload: new TextEncoder().encode(encodeJson("renamed")),
          requestHash: "mutation-hash"
        })
        assert.deepStrictEqual(
          yield* Schema.decodeUnknownEffect(Schema.toCodecJson(CommandOutcome.schema(Schema.String, Schema.Never)))(
            decodeJson(new TextDecoder().decode(mutationBytes))
          ),
          CommandOutcome.durablyCommitted(mutationCommandId, "renamed")
        )

        const deleteCommandId = yield* Identity.makeCommandId
        const deleteBytes = yield* client.Delete({
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId: deleteCommandId,
          documentType: Task.name,
          requestHash: "delete-hash"
        })
        assert.deepStrictEqual(
          yield* Schema.decodeUnknownEffect(Schema.toCodecJson(CommandOutcome.schema(Schema.Void, Schema.Never)))(
            decodeJson(new TextDecoder().decode(deleteBytes))
          ),
          CommandOutcome.durablyCommitted(deleteCommandId, undefined)
        )

        const resolutionCommandId = yield* Identity.makeCommandId
        const resolutionBytes = yield* client.Resolve({
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId: resolutionCommandId,
          documentType: Task.name,
          requestHash: "resolution-hash",
          resolution: {
            heads: [],
            path: {
              parents: [],
              target: { _tag: "Key", key: "title" }
            },
            choice: { _tag: "DeleteValue" }
          }
        })
        assert.deepStrictEqual(
          yield* Schema.decodeUnknownEffect(
            Schema.toCodecJson(CommandOutcome.schema(Schema.Void, Conflict.ResolutionError))
          )(decodeJson(new TextDecoder().decode(resolutionBytes))),
          CommandOutcome.durablyCommitted(resolutionCommandId, undefined)
        )

        const message = new Uint8Array([1, 2, 3])
        const writerProvenance = [{
          changeHash: "a".repeat(64),
          writerSchemaVersion: Task.version,
          writerDefinitionHash: definition.hash
        }]
        const applied = yield* client.ApplySync({
          replicaIncarnation: permit.incarnation,
          peerId: (yield* Identity.makePeerId),
          connectionEpoch: "connection",
          localConnectionEpoch: "local-connection",
          receiveSequence: 0,
          documentType: Task.name,
          messageHash: yield* Canonical.digest(message),
          message,
          writerProvenance
        })
        assert.deepStrictEqual(applied, syncResult)
        assert.deepStrictEqual(yield* Ref.get(receivedProvenance), writerProvenance)
        assert.deepStrictEqual(yield* Ref.get(receivedLineages), [Identity.genesisLineage])
        const rewrittenLineage = Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000001")
        const rewritten = yield* client.ApplySync({
          replicaIncarnation: permit.incarnation,
          peerId: (yield* Identity.makePeerId),
          connectionEpoch: "connection",
          localConnectionEpoch: "local-connection",
          receiveSequence: 1,
          documentType: Task.name,
          messageHash: yield* Canonical.digest(message),
          message,
          lineage: rewrittenLineage,
          writerProvenance
        })
        assert.deepStrictEqual(rewritten, syncResult)
        assert.deepStrictEqual(yield* Ref.get(receivedLineages), [Identity.genesisLineage, rewrittenLineage])
        const messageHash = yield* Canonical.digest(message)
        const relay = {
          relayMessageId: Identity.RelayMessageId.make("rly_00000000-0000-4000-8000-000000000001"),
          relayPeerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001"),
          senderTenantId: "tenant",
          senderSubjectId: "sender",
          senderPeerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000002"),
          senderReplicaIncarnation: Identity.ReplicaIncarnation.make(4),
          messageHash,
          outerEnvelopeDigest: "b".repeat(64),
          receiptExpiresAt: "2026-08-02T00:00:00.000Z",
          encodedSize: 128
        } satisfies PeerSync.RelayReceipt
        assert.deepStrictEqual(
          yield* client.ApplySync({
            replicaIncarnation: permit.incarnation,
            peerId: relay.relayPeerId,
            connectionEpoch: "relay-connection",
            localConnectionEpoch: "local-connection",
            receiveSequence: 1,
            documentType: Task.name,
            messageHash,
            message,
            writerProvenance,
            relay
          }),
          syncResult
        )
        assert.deepStrictEqual(yield* Ref.get(receivedRelay), relay)
        assert.deepStrictEqual(
          yield* Ref.get(receivedLineages),
          [Identity.genesisLineage, rewrittenLineage, Identity.genesisLineage]
        )
        const stale = yield* Effect.exit(client.ApplySync({
          replicaIncarnation: Identity.ReplicaIncarnation.make(permit.incarnation - 1),
          peerId: (yield* Identity.makePeerId),
          connectionEpoch: "stale-connection",
          localConnectionEpoch: "local-connection",
          receiveSequence: 0,
          documentType: Task.name,
          messageHash: yield* Canonical.digest(message),
          message,
          writerProvenance: [{
            changeHash: "a".repeat(64),
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          }]
        }))
        assert.strictEqual(stale._tag, "Failure")
      }).pipe(Effect.provide(TestShardingConfig))
    ))

  it.effect("serializes commands for one document", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const calls = yield* Ref.make(0)
        const syncCalls = yield* Ref.make(0)
        const firstStarted = yield* Deferred.make<void>()
        const releaseFirst = yield* Deferred.make<void>()
        const permit = {
          replicaId: (yield* Identity.makeReplicaId),
          incarnation: Identity.ReplicaIncarnation.make(1),
          writerGeneration: Identity.WriterGeneration.make(2)
        }
        const executor = CommandExecutor.CommandExecutor.of({
          create: (_document, options) =>
            Effect.gen(function*() {
              const call = yield* Ref.updateAndGet(calls, (count) => count + 1)
              if (call === 1) {
                yield* Deferred.succeed(firstStarted, undefined)
                yield* Deferred.await(releaseFirst)
              }
              return CommandOutcome.durablyCommitted(options.commandId, options.documentId)
            }),
          mutate: (_mutation, options) =>
            Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, options.payload)),
          delete: (_document, options) => Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, undefined)),
          resolve: (_document, options) =>
            Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, undefined)),
          lookupCreate: (id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupMutation: (_mutation, id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupDelete: (id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupResolution: (_document, options) => Effect.succeed(CommandOutcome.unknown(options.commandId))
        })
        const sync = peerSync((_document, _documentId, _session, _input) =>
          Ref.update(syncCalls, (count) => count + 1).pipe(Effect.as(syncResult))
        )
        const makeClient = yield* Entity.makeTestClient(
          DocumentEntity.DocumentEntity,
          DocumentEntity.layer(definition).pipe(
            Layer.provide(Layer.succeed(CommandExecutor.CommandExecutor, executor)),
            Layer.provide(Layer.succeed(ReplicaGate.ReplicaGate, replicaGate(permit))),
            Layer.provide(Layer.succeed(PeerSync.PeerSync, sync)),
            Layer.provide(ReplicaLimits.layer(limits))
          )
        )
        const client = yield* makeClient(yield* Identity.makeDocumentId)
        const request = (commandId: Identity.CommandId) => ({
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId,
          documentType: Task.name,
          payload: new TextEncoder().encode(encodeJson({ title: "first" })),
          requestHash: commandId
        })
        const first = yield* Effect.forkChild(client.Create(request(yield* Identity.makeCommandId)))
        yield* Deferred.await(firstStarted)
        const message = new Uint8Array([1, 2, 3])
        const second = yield* Effect.forkChild(client.ApplySync({
          replicaIncarnation: permit.incarnation,
          peerId: (yield* Identity.makePeerId),
          connectionEpoch: "connection",
          localConnectionEpoch: "local-connection",
          receiveSequence: 0,
          documentType: Task.name,
          messageHash: yield* Canonical.digest(message),
          message,
          writerProvenance: [{
            changeHash: "a".repeat(64),
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          }]
        }))
        yield* Effect.yieldNow
        assert.strictEqual(yield* Ref.get(calls), 1)
        assert.strictEqual(yield* Ref.get(syncCalls), 0)
        yield* Deferred.succeed(releaseFirst, undefined)
        yield* Fiber.join(first)
        yield* Fiber.join(second)
        assert.strictEqual(yield* Ref.get(calls), 1)
        assert.strictEqual(yield* Ref.get(syncCalls), 1)
      }).pipe(Effect.provide(TestShardingConfig))
    ))

  it.effect("rejects unregistered document types, unregistered mutations, and tampered sync hashes", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const permit = {
          replicaId: (yield* Identity.makeReplicaId),
          incarnation: Identity.ReplicaIncarnation.make(1),
          writerGeneration: Identity.WriterGeneration.make(2)
        }
        const executor = CommandExecutor.CommandExecutor.of({
          create: (_document, _options) => Effect.die("create should not run"),
          mutate: (_mutation, _options) => Effect.die("mutate should not run"),
          delete: (_document, _options) => Effect.die("delete should not run"),
          resolve: (_document, _options) => Effect.die("resolve should not run"),
          lookupCreate: (id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupMutation: (_mutation, id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupDelete: (id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupResolution: (_document, options) => Effect.succeed(CommandOutcome.unknown(options.commandId))
        })
        const makeClient = yield* Entity.makeTestClient(
          DocumentEntity.DocumentEntity,
          DocumentEntity.layer(definition).pipe(
            Layer.provide(Layer.succeed(CommandExecutor.CommandExecutor, executor)),
            Layer.provide(Layer.succeed(ReplicaGate.ReplicaGate, replicaGate(permit))),
            Layer.provide(Layer.succeed(PeerSync.PeerSync, peerSync(() => Effect.die("receive should not run")))),
            Layer.provide(ReplicaLimits.layer(limits))
          )
        )
        const client = yield* makeClient(yield* Identity.makeDocumentId)
        const unregisteredDocument = yield* Effect.flip(client.Create({
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId: yield* Identity.makeCommandId,
          documentType: "Ghost",
          payload: new TextEncoder().encode(encodeJson({ title: "x" })),
          requestHash: "hash"
        }))
        assert.strictEqual(unregisteredDocument.reason._tag, "ProtocolMismatch")
        const unregisteredMutation = yield* Effect.flip(client.Mutate({
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId: yield* Identity.makeCommandId,
          documentType: Task.name,
          mutationTag: "Ghost",
          payload: new TextEncoder().encode(encodeJson("x")),
          requestHash: "hash"
        }))
        assert.strictEqual(unregisteredMutation.reason._tag, "ProtocolMismatch")
        const message = new Uint8Array([9, 9, 9])
        const tamperedHash = yield* Effect.flip(client.ApplySync({
          replicaIncarnation: permit.incarnation,
          peerId: (yield* Identity.makePeerId),
          connectionEpoch: "connection",
          localConnectionEpoch: "local-connection",
          receiveSequence: 0,
          documentType: Task.name,
          messageHash: "not-the-real-hash",
          message,
          writerProvenance: [{
            changeHash: "a".repeat(64),
            writerSchemaVersion: Task.version,
            writerDefinitionHash: definition.hash
          }]
        }))
        assert.strictEqual(tamperedHash.reason._tag, "ProtocolMismatch")
      }).pipe(Effect.provide(TestShardingConfig))
    ))
})
