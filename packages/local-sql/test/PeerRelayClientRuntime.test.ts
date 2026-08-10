import * as Automerge from "@automerge/automerge"
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as PeerRelayClientRuntime from "../src/PeerRelayClientRuntime.js"
import * as PeerRelayOutbox from "../src/PeerRelayOutbox.js"
import * as PeerRelayOutboxLimits from "../src/PeerRelayOutboxLimits.js"
import * as PeerRelayReceiptLimits from "../src/PeerRelayReceiptLimits.js"
import * as PeerSync from "../src/PeerSync.js"
import * as PeerSyncEnvelope from "../src/PeerSyncEnvelope.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

describe("PeerRelayClientRuntime", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const definition = ReplicaDefinition.make({
    name: "relay-runtime",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const replicaLimits: ReplicaLimits.Values = {
    maxBackupBytes: 1_000_000,
    maxChunkBytes: 64_000,
    maxArchiveRecords: 1_000,
    maxJsonDepth: 64,
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
    maxSyncOperationsPerMessage: 10_000,
    maxPendingBytesPerDocument: 1_000_000,
    maxPendingBytesPerPeer: 1_000_000,
    maxPendingBytesPerReplica: 1_000_000,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1_000,
    maxPendingChangesPerPeer: 1_000,
    maxPendingChangesPerReplica: 1_000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 10_000,
    maxSessions: 10,
    maxStreamsPerSession: 10,
    maxInFlightPerSession: 1,
    maxQueuedRpc: 100,
    maxQueuedPermits: 100,
    maxActiveRestores: 10,
    maxRestoresPerSession: 1,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  }
  const outboxLimits: PeerRelayOutboxLimits.Values = {
    ...PeerRelayOutboxLimits.defaults,
    maxMessagesPerRemote: 10,
    maxMessagesPerReplica: 10,
    maxEncodedBytesPerRemote: 1_000_000,
    maxEncodedBytesPerReplica: 1_000_000,
    maxRetryHorizonMillis: 60_000,
    maintenanceIntervalMillis: 1_000,
    pruneBatchSize: 10
  }
  const receiptLimits: PeerRelayReceiptLimits.Values = {
    ...PeerRelayReceiptLimits.defaults,
    maintenanceIntervalMillis: 1_000
  }
  const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000011")
  const lineage = Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000015")
  const endpoint: PeerRelayOutbox.Endpoint = {
    expectedLocal: {
      tenantId: "tenant-a",
      subjectId: "sender-a",
      peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000012")
    },
    remote: {
      tenantId: "tenant-a",
      subjectId: "recipient-a",
      peerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000013")
    },
    relayPeerId: Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000014")
  }

  const fakePeerSync = (
    pruneRelayReceipts: PeerSync.PeerSync["Service"]["pruneRelayReceipts"]
  ): PeerSync.PeerSync["Service"] => {
    const service = {
      withDocumentInvalidation: (_documentId: Identity.DocumentId, effect: Effect.Effect<unknown>) => effect,
      invalidateDocument: () => Effect.void,
      open: () => Effect.die(new Error("unused")),
      reset: () => Effect.die(new Error("unused")),
      generate: () => Effect.die(new Error("unused")),
      receive: () => Effect.die(new Error("unused")),
      enqueue: () => Effect.die(new Error("unused")),
      pending: () => Effect.die(new Error("unused")),
      markSent: () => Effect.die(new Error("unused"))
    }
    if (pruneRelayReceipts === undefined) return service
    return { ...service, pruneRelayReceipts }
  }

  const layers = (
    pruneRelayReceipts: PeerSync.PeerSync["Service"]["pruneRelayReceipts"] | null = Effect.succeed(0)
  ) => {
    const Database = Layer.merge(
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer
    )
    const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
    const Base = Layer.merge(Database, Bootstrap)
    const ReplicaLimitLayer = ReplicaLimits.layer(replicaLimits)
    const Gate = ReplicaGate.layer.pipe(
      Layer.provide(ReplicaLimitLayer),
      Layer.provide(Base)
    )
    const Dependencies = Layer.mergeAll(
      Base,
      Gate,
      ReplicaLimitLayer,
      PeerRelayOutboxLimits.layer(outboxLimits),
      PeerRelayReceiptLimits.layer(receiptLimits),
      Layer.succeed(
        PeerSync.PeerSync,
        fakePeerSync(pruneRelayReceipts ?? undefined)
      )
    )
    const Outbox = PeerRelayOutbox.layerSql.pipe(Layer.provide(Dependencies))
    const RuntimeDependencies = Layer.merge(Dependencies, Outbox)
    const Runtime = PeerRelayClientRuntime.layer.pipe(Layer.provide(RuntimeDependencies))
    return Layer.merge(RuntimeDependencies, Runtime)
  }

  const insertDocument = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* sql`INSERT INTO effect_local_documents (
      document_id, document_type, schema_version, observed_versions,
      materialized_heads, accepted_heads, tombstone, projection_status, checkpoint_hash
    ) VALUES (${documentId}, 'Task', 1, '[1]', '[]', '[]', 0, 'Ready', NULL)`
  })

  const makePayload = Effect.gen(function*() {
    let source = Automerge.from(
      { value: { title: "one" }, tombstone: false },
      { actor: "b".repeat(32) }
    )
    const remote = Automerge.init()
    const handshake = Automerge.generateSyncMessage(remote, Automerge.initSyncState())[1]!
    const received = Automerge.receiveSyncMessage(source, Automerge.initSyncState(), handshake)
    source = received[0]
    const message = Automerge.generateSyncMessage(source, received[1])[1]!
    const changes = Automerge.getAllChanges(source).map(Automerge.decodeChange)
    const payload = yield* PeerSyncEnvelope.encodeSyncEnvelope(PeerSyncEnvelope.SyncEnvelope.make({
      connectionEpoch: "epoch-runtime",
      sequence: 1,
      documentId,
      documentType: "Task",
      messageHash: yield* Canonical.digest(message),
      message,
      lineage,
      writerProvenance: changes.map((change) => ({
        changeHash: change.hash,
        writerSchemaVersion: 1,
        writerDefinitionHash: definition.hash
      }))
    }))
    Automerge.free(source)
    Automerge.free(remote)
    return payload
  })

  it.effect("propagates a real SQLite maintenance failure and fails later methods", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const runtime = yield* PeerRelayClientRuntime.PeerRelayClientRuntime
        const outbox = yield* PeerRelayOutbox.PeerRelayOutbox
        const sql = yield* SqlClient.SqlClient
        const gate = yield* ReplicaGate.ReplicaGate
        yield* insertDocument
        const payload = yield* makePayload
        yield* outbox.admit({ ...endpoint, payload, retryHorizonMillis: 1_000 })
        yield* sql`CREATE TRIGGER fail_relay_outbox_delete
        BEFORE DELETE ON effect_local_peer_relay_outbox
        BEGIN
          SELECT RAISE(FAIL, 'forced relay maintenance failure');
        END`
        yield* runtime.validateConnectionConfiguration({
          replicaIncarnation: (yield* gate.current).incarnation,
          retryHorizonMillis: 1_000,
          replayBatchSize: 1
        })
        yield* TestClock.adjust("1 second")
        const fatal = yield* Effect.exit(runtime.awaitFatal)
        assert.strictEqual(fatal._tag, "Failure")
        if (Exit.isFailure(fatal)) assert.isFalse(Cause.hasInterruptsOnly(fatal.cause))
        assert.strictEqual((yield* Effect.exit(runtime.health))._tag, "Failure")
        assert.strictEqual(
          (yield* Effect.exit(runtime.maximumPendingHorizon(endpoint)))._tag,
          "Failure"
        )
      }).pipe(Effect.provide(layers()))
    ))

  it.effect("closes both scoped maintenance fibers before scope release completes", () =>
    Effect.gen(function*() {
      const scope = yield* Scope.make()
      const context = yield* Scope.provide(Layer.build(layers()), scope)
      const runtime = Context.get(context, PeerRelayClientRuntime.PeerRelayClientRuntime)
      yield* runtime.health
      const fatalWaiter = yield* Effect.exit(runtime.awaitFatal).pipe(Effect.forkChild)
      yield* Scope.close(scope, Exit.void)
      const fatalExit = yield* Fiber.join(fatalWaiter)
      assert.strictEqual(fatalExit._tag, "Failure")
      if (Exit.isFailure(fatalExit)) assert.isTrue(Cause.hasInterruptsOnly(fatalExit.cause))
      const health = yield* Effect.exit(runtime.health)
      assert.strictEqual(health._tag, "Failure")
      if (Exit.isFailure(health)) assert.isTrue(Cause.hasInterruptsOnly(health.cause))
    }))

  it.effect("rejects a direct PeerSync before starting runtime maintenance", () =>
    Effect.gen(function*() {
      const scope = yield* Scope.make()
      const exit = yield* Effect.exit(
        Scope.provide(Layer.build(layers(null)), scope)
      )
      assert.strictEqual(exit._tag, "Failure")
      yield* Scope.close(scope, Exit.void)
    }))

  it.effect("registers exact routes, multicasts without replay, and unregisters token safely", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const runtime = yield* PeerRelayClientRuntime.PeerRelayClientRuntime
        const peerId = yield* Identity.makePeerId
        const routeDocumentId = yield* Identity.makeDocumentId
        const inbound = yield* Effect.acquireRelease(
          PubSub.sliding<PeerRelayClientRuntime.TransientMessage>(64),
          PubSub.shutdown
        )
        const sends = yield* Queue.unbounded<Uint8Array>()
        const makeSession = (target: Queue.Queue<Uint8Array>) => ({
          peerId,
          connectionEpoch: "runtime-test",
          markDirty: () => Effect.void,
          flush: Effect.void,
          observedByPeer: () => Effect.succeed(false),
          durableConfirmation: () => Effect.succeed(false),
          transient: (_documentId: Identity.DocumentId, payload: Uint8Array) =>
            Queue.offer(target, payload).pipe(Effect.asVoid),
          transients: Stream.fromPubSub(inbound)
        })
        const documents = [{ document: Task, documentId: routeDocumentId }]
        const first = yield* runtime.register(makeSession(sends), documents)
        const duplicate = yield* Effect.exit(runtime.register(makeSession(sends), documents))
        assert.strictEqual(duplicate._tag, "Failure")

        const priorScope = yield* Scope.make()
        const priorPull = yield* Stream.toPull(runtime.transients).pipe(
          Effect.provideService(Scope.Scope, priorScope)
        )
        const priorFiber = yield* priorPull.pipe(Effect.forkChild({ startImmediately: true }))
        const missed = { peerId, documentId: routeDocumentId, payload: Uint8Array.of(1) }
        yield* PubSub.publish(inbound, missed)
        assert.deepStrictEqual((yield* Fiber.join(priorFiber))[0], missed)
        yield* Scope.close(priorScope, Exit.void)

        const firstPull = yield* Stream.toPull(runtime.transients)
        const secondPull = yield* Stream.toPull(runtime.transients)
        const firstFiber = yield* firstPull.pipe(Effect.forkChild({ startImmediately: true }))
        const secondFiber = yield* secondPull.pipe(Effect.forkChild({ startImmediately: true }))
        const accepted = { peerId, documentId: routeDocumentId, payload: Uint8Array.of(2) }
        yield* PubSub.publish(inbound, accepted)
        assert.deepStrictEqual((yield* Fiber.join(firstFiber))[0], accepted)
        assert.deepStrictEqual((yield* Fiber.join(secondFiber))[0], accepted)

        yield* runtime.send(peerId, routeDocumentId, Uint8Array.of(3))
        assert.deepStrictEqual(yield* Queue.take(sends), Uint8Array.of(3))
        yield* first.unregister

        const afterUnregisterPull = yield* Stream.toPull(runtime.transients)
        const afterUnregisterFiber = yield* afterUnregisterPull.pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* PubSub.publish(inbound, { peerId, documentId: routeDocumentId, payload: Uint8Array.of(99) })

        const replacementSends = yield* Queue.unbounded<Uint8Array>()
        const replacement = yield* runtime.register(makeSession(replacementSends), documents)
        yield* first.unregister
        const replacementInbound = { peerId, documentId: routeDocumentId, payload: Uint8Array.of(100) }
        yield* PubSub.publish(inbound, replacementInbound)
        assert.deepStrictEqual((yield* Fiber.join(afterUnregisterFiber))[0], replacementInbound)
        yield* runtime.send(peerId, routeDocumentId, Uint8Array.of(4))
        assert.deepStrictEqual(yield* Queue.take(replacementSends), Uint8Array.of(4))
        yield* replacement.unregister
        const unavailable = yield* Effect.exit(runtime.send(peerId, routeDocumentId, Uint8Array.of(5)))
        assert.strictEqual(unavailable._tag, "Failure")
      }).pipe(Effect.provide(layers()))
    ))

  it.effect("lets a slow multicast subscriber converge on the newest 64 values", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const runtime = yield* PeerRelayClientRuntime.PeerRelayClientRuntime
        const peerId = yield* Identity.makePeerId
        const routeDocumentId = yield* Identity.makeDocumentId
        const inbound = yield* Effect.acquireRelease(
          PubSub.sliding<PeerRelayClientRuntime.TransientMessage>(64),
          PubSub.shutdown
        )
        const session = {
          peerId,
          connectionEpoch: "slow-runtime-test",
          markDirty: () => Effect.void,
          flush: Effect.void,
          observedByPeer: () => Effect.succeed(false),
          durableConfirmation: () => Effect.succeed(false),
          transient: () => Effect.void,
          transients: Stream.fromPubSub(inbound)
        }
        yield* runtime.register(session, [{ document: Task, documentId: routeDocumentId }])
        const slowPull = yield* Stream.toPull(runtime.transients)
        const fastPull = yield* Stream.toPull(runtime.transients)
        const slowFirst = yield* slowPull.pipe(Effect.forkChild({ startImmediately: true }))
        for (let index = 0; index < 66; index++) {
          const fast = yield* fastPull.pipe(Effect.forkChild({ startImmediately: true }))
          yield* PubSub.publish(inbound, {
            peerId,
            documentId: routeDocumentId,
            payload: Uint8Array.of(index)
          })
          assert.strictEqual((yield* Fiber.join(fast))[0]?.payload[0], index)
        }
        assert.strictEqual((yield* Fiber.join(slowFirst))[0]?.payload[0], 0)
        const newest = yield* slowPull
        assert.strictEqual(newest.length, 64)
        assert.strictEqual(newest[0]?.payload[0], 2)
        assert.strictEqual(newest[63]?.payload[0], 65)
      }).pipe(Effect.provide(layers()))
    ))
})
