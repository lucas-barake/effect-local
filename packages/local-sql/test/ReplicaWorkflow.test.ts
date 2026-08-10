import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as ClusterWorkflowEngine from "effect/unstable/cluster/ClusterWorkflowEngine"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as Compaction from "../src/Compaction.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
import * as PeerSync from "../src/PeerSync.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as ReplicaWorkflow from "../src/ReplicaWorkflow.js"
import { nativeError } from "./TestErrors.js"

describe("ReplicaWorkflow", () => {
  it.effect("derives stable execution ids from the replica incarnation and operation id", () =>
    Effect.gen(function*() {
      const payload = {
        replicaIncarnation: Identity.ReplicaIncarnation.make(2),
        operationId: ReplicaWorkflow.OperationId.make("compact-2026-07-21")
      }
      const first = yield* ReplicaWorkflow.CompactReplica.executionId(payload)
      const second = yield* ReplicaWorkflow.CompactReplica.executionId(payload)
      assert.strictEqual(first, second)
      assert.notStrictEqual(
        first,
        yield* ReplicaWorkflow.CompactReplica.executionId({
          ...payload,
          replicaIncarnation: Identity.ReplicaIncarnation.make(3)
        })
      )
      assert.notStrictEqual(
        first,
        yield* ReplicaWorkflow.CompactReplica.executionId({
          ...payload,
          operationId: ReplicaWorkflow.OperationId.make("compact-2026-07-22")
        })
      )
    }))

  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const definition = ReplicaDefinition.make({
    name: "replica-workflow-tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Limits = ReplicaLimits.layer({
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
  })
  const Gate = ReplicaGate.layer.pipe(Layer.provide(Limits), Layer.provide(Layer.merge(Database, Bootstrap)))
  const Store = DocumentStore.layer.pipe(Layer.provide(Layer.mergeAll(Database, Gate, Limits)))
  const RecoveryService = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(Database, Gate)))
  const CompactionService = Compaction.layer.pipe(
    Layer.provide(Layer.mergeAll(Database, Gate, RecoveryService, Limits))
  )
  const Projections = ProjectionStore.layer([]).pipe(Layer.provide(Database))
  const Inputs = Layer.mergeAll(
    Database,
    Bootstrap,
    Limits,
    Gate,
    Store,
    RecoveryService,
    CompactionService,
    Projections
  )
  /** The same expression `DurableRuntime` builds the workflow branch's engine from. */
  const Cluster = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(ClusterStorage.layer))

  /**
   * The production history rewrite workflow, wired to a `PeerSync` that records which documents it
   * was asked to invalidate and then delegates to the real service.
   *
   * The registration layer, the runtime layer, `Compaction`, `ReplicaGate`, `DocumentStore` and the
   * cluster workflow engine are all production code; only the observation of one cross service call
   * is added. `DurableRuntime` is deliberately not used: it hoists `PeerSync.layer` inside itself, so
   * a `PeerSync` supplied from outside it is shadowed and could never see the workflow's calls.
   */
  const recordingServices = Effect.gen(function*() {
    const invalidated = yield* Ref.make<ReadonlyArray<Identity.DocumentId>>([])
    const peerSync = Layer.effect(
      PeerSync.PeerSync,
      Effect.gen(function*() {
        const inner = yield* PeerSync.PeerSync
        return PeerSync.PeerSync.of({
          ...inner,
          withDocumentInvalidation: (documentId, effect) =>
            inner.withDocumentInvalidation(documentId, effect).pipe(
              Effect.tap(() => Ref.update(invalidated, (all) => [...all, documentId]))
            )
        })
      })
    ).pipe(Layer.provide(PeerSync.layer))
    const services = Layer.mergeAll(
      ReplicaWorkflow.layerHistoryRewriteRegistration(definition),
      ReplicaWorkflow.layerHistoryRewriteRuntime
    ).pipe(
      Layer.provide(peerSync),
      Layer.provideMerge(Cluster),
      Layer.provideMerge(Inputs)
    )
    return { invalidated, services }
  })

  const drive = Effect.gen(function*() {
    const sharding = yield* Sharding.Sharding
    for (let round = 0; round < 4; round++) {
      yield* sharding.pollStorage
      yield* TestClock.adjust(5_000)
    }
  })

  /** Overwrites `title` `count` times through the real store, leaving one change row per write. */
  const churn = (documentId: Identity.DocumentId, count: number) =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      let durable = yield* store.create(Task, documentId, { title: "write-0" })
      for (let index = 1; index <= count; index++) {
        const staged = yield* store.stage(durable, (draft) => {
          draft.title = `write-${index}`
        })
        const next = yield* store.persist(Task, documentId, durable, staged)
        InternalAutomerge.free(staged)
        InternalAutomerge.free(durable.automerge)
        durable = next
      }
      InternalAutomerge.free(durable.automerge)
    })

  it.effect(
    "invalidates the peer sync state of the rewritten document and of no other",
    () =>
      Effect.gen(function*() {
        const { invalidated, services } = yield* recordingServices
        yield* Effect.gen(function*() {
          const runtime = yield* ReplicaWorkflow.HistoryRewriteWorkflow
          const rewritten = yield* Identity.makeDocumentId
          const untouched = yield* Identity.makeDocumentId
          yield* churn(rewritten, 5)
          yield* churn(untouched, 5)
          assert.deepStrictEqual(yield* Ref.get(invalidated), [])

          const execution = yield* runtime.execute(
            rewritten,
            ReplicaWorkflow.OperationId.make("rewrite-invalidates-sync-state")
          )
          yield* drive

          const result = yield* runtime.poll(execution)
          assert.isTrue(Option.isSome(result))
          if (!Option.isSome(result)) yield* Effect.die(nativeError("unreachable"))
          assert.strictEqual(result.value._tag, "Complete")
          if (result.value._tag !== "Complete") yield* Effect.die(nativeError("unreachable"))
          assert.isTrue(Exit.isSuccess(result.value.exit))
          if (!Exit.isSuccess(result.value.exit)) yield* Effect.die(nativeError("unreachable"))
          assert.notStrictEqual(result.value.exit.value, Identity.genesisLineage)

          // Exactly the rewritten document, exactly once. Nothing else evicts the in-memory sync
          // state a live session holds: a rewrite moves neither the replica incarnation nor any
          // session generation, so a state kept across it would keep answering `generate` from the
          // heads the rewrite destroyed.
          assert.deepStrictEqual(yield* Ref.get(invalidated), [rewritten])
          assert.notStrictEqual(rewritten, untouched)
        }).pipe(Effect.provide(services))
      }),
    20_000
  )
})
