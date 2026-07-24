import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as PrimaryKey from "effect/PrimaryKey"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as ClusterSchema from "effect/unstable/cluster/ClusterSchema"
import * as Entity from "effect/unstable/cluster/Entity"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as DocumentEntity from "../src/DocumentEntity.js"
import * as PeerSync from "../src/PeerSync.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

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
    maxQueuedRpc: 32
  } satisfies ReplicaLimits.Values
  const TestShardingConfig = Layer.merge(
    ShardingConfig.layer({
      shardsPerGroup: 16,
      entityMailboxCapacity: limits.maxQueuedRpc,
      entityTerminationTimeout: 0,
      entityMessagePollInterval: 5_000,
      sendRetryInterval: 100
    }),
    NodeCrypto.layer
  )
  const syncResult = {
    reply: null,
    heads: [],
    acceptedHeads: [],
    commitSequence: Identity.CommitSequence.make(1),
    observedByPeer: false,
    durableConfirmation: false as const,
    duplicate: false
  }
  const peerSync = (receive: PeerSync.PeerSync["Service"]["receive"] = () => Effect.succeed(syncResult)) =>
    PeerSync.PeerSync.of({
      open: (peerId) =>
        Effect.succeed({
          peerId,
          connectionEpoch: "epoch",
          replicaIncarnation: Identity.ReplicaIncarnation.make(0)
        }),
      reset: () => Effect.void,
      generate: () => Effect.succeed({ outbound: null, observedByPeer: false, dirty: false }),
      receive,
      enqueue: (_session, reply) => Effect.succeed({ ...reply, sendSequence: 0, writerProvenance: [] }),
      pending: () => Effect.succeed([]),
      markSent: () => Effect.succeed(false)
    })
  const replicaGate = (permit: ReplicaGate.Permit) =>
    ReplicaGate.ReplicaGate.of({
      current: Effect.succeed(permit),
      shared: Effect.die("unused"),
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
    const keyOf = (payload: unknown) => {
      if (!PrimaryKey.isPrimaryKey(payload)) throw new TypeError("Expected a primary key payload")
      return PrimaryKey.value(payload)
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
    const keyOf = (payload: unknown) => {
      if (!PrimaryKey.isPrimaryKey(payload)) throw new TypeError("Expected a primary key payload")
      return PrimaryKey.value(payload)
    }
    const key = keyOf(DocumentEntity.ApplySync.payloadSchema.make(base))
    assert.strictEqual(key, JSON.stringify([1, peerId, "connection", 2, "hash-a", base.writerProvenance]))
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
        writerProvenance: [{ ...base.writerProvenance[0]!, writerSchemaVersion: Task.version + 1 }]
      }))
    )
    assert.notStrictEqual(
      key,
      keyOf(DocumentEntity.ApplySync.payloadSchema.make({
        ...base,
        writerProvenance: [{ ...base.writerProvenance[0]!, writerDefinitionHash: "different-definition" }]
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
    const keyOf = (payload: unknown) => {
      if (!PrimaryKey.isPrimaryKey(payload)) throw new TypeError("Expected a primary key payload")
      return PrimaryKey.value(payload)
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

  it("persists RPCs in the shared SQL transaction without server interruption", () => {
    for (const rpc of [DocumentEntity.Create, DocumentEntity.Mutate, DocumentEntity.Delete]) {
      assert.strictEqual(Context.get(rpc.annotations, ClusterSchema.Persisted), true)
      assert.strictEqual(Context.get(rpc.annotations, ClusterSchema.WithTransaction), true)
      assert.isTrue(Context.get(rpc.annotations, ClusterSchema.Uninterruptible) === "client")
    }
    assert.strictEqual(Context.get(DocumentEntity.ApplySync.annotations, ClusterSchema.Persisted), true)
    assert.strictEqual(Context.get(DocumentEntity.ApplySync.annotations, ClusterSchema.WithTransaction), true)
    assert.strictEqual(Context.get(DocumentEntity.ApplySync.annotations, ClusterSchema.Uninterruptible), true)
  })

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
          lookupCreate: (id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupMutation: (_mutation, id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupDelete: (id) => Effect.succeed(CommandOutcome.unknown(id))
        })
        const receivedProvenance = yield* Ref.make<
          ReadonlyArray<{
            readonly changeHash: string
            readonly writerSchemaVersion: number
            readonly writerDefinitionHash: string
          }>
        >([])
        const sync = peerSync((_document, _documentId, _session, input) =>
          Ref.set(receivedProvenance, input.writerProvenance).pipe(Effect.as(syncResult))
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
          payload: new TextEncoder().encode(JSON.stringify({ title: "first" })),
          requestHash: "create-hash"
        })
        const outcome = yield* Schema.decodeUnknownEffect(
          Schema.toCodecJson(CommandOutcome.schema(Identity.DocumentId, Schema.Never))
        )(
          JSON.parse(new TextDecoder().decode(bytes))
        )
        assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, documentId))

        const mutationCommandId = yield* Identity.makeCommandId
        const mutationBytes = yield* client.Mutate({
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId: mutationCommandId,
          documentType: Task.name,
          mutationTag: Rename.name,
          payload: new TextEncoder().encode(JSON.stringify("renamed")),
          requestHash: "mutation-hash"
        })
        assert.deepStrictEqual(
          yield* Schema.decodeUnknownEffect(Schema.toCodecJson(CommandOutcome.schema(Schema.String, Schema.Never)))(
            JSON.parse(new TextDecoder().decode(mutationBytes))
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
            JSON.parse(new TextDecoder().decode(deleteBytes))
          ),
          CommandOutcome.durablyCommitted(deleteCommandId, undefined)
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
          lookupCreate: (id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupMutation: (_mutation, id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupDelete: (id) => Effect.succeed(CommandOutcome.unknown(id))
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
          payload: new TextEncoder().encode(JSON.stringify({ title: "first" })),
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
          lookupCreate: (id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupMutation: (_mutation, id) => Effect.succeed(CommandOutcome.unknown(id)),
          lookupDelete: (id) => Effect.succeed(CommandOutcome.unknown(id))
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
          payload: new TextEncoder().encode(JSON.stringify({ title: "x" })),
          requestHash: "hash"
        }))
        assert.strictEqual(unregisteredDocument.reason._tag, "ProtocolMismatch")
        const unregisteredMutation = yield* Effect.flip(client.Mutate({
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId: yield* Identity.makeCommandId,
          documentType: Task.name,
          mutationTag: "Ghost",
          payload: new TextEncoder().encode(JSON.stringify("x")),
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
