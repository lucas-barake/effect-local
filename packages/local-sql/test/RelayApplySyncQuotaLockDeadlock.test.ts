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
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as SqlMessageStorage from "effect/unstable/cluster/SqlMessageStorage"
import * as SqlRunnerStorage from "effect/unstable/cluster/SqlRunnerStorage"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as DocumentEntity from "../src/DocumentEntity.js"
import type * as InternalAutomerge from "../src/internal/automerge.js"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
import * as PeerRelayReceiptLimits from "../src/PeerRelayReceiptLimits.js"
import * as PeerSync from "../src/PeerSync.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as SqlReplica from "../src/SqlReplica.js"

/**
 * `DocumentEntity.ApplySync` is annotated `ClusterSchema.WithTransaction`, so the cluster runs the
 * whole handler inside `MessageStorage.withTransaction`, which for `SqlMessageStorage` is
 * `sql.withTransaction`. Both SQLite clients hand the transaction connection out through a
 * single-permit semaphore held until the transaction closes, so the handler owns the database for
 * its whole run and only then asks `PeerSync` for `quotaLock`.
 *
 * Every other `PeerSync` entry point takes `quotaLock` first and opens its transaction second, and
 * the relay receipt maintenance loop runs one of them every second. The two orders form a cycle:
 * the handler waits for `quotaLock` while holding the only transaction permit, and the pruner waits
 * for that permit while holding `quotaLock`.
 *
 * The test only inserts a rendezvous in front of `PeerSync.receive` so the interleaving is
 * deterministic. Both locks, the transaction, the entity, and the cluster are the production ones.
 *
 * While the cycle is open nothing in the runtime can be interrupted out of it, because a fiber
 * waiting for the transaction permit waits inside the uninterruptible mask of
 * `SqlClient.makeWithTransaction`. So today this test does not fail on its assertion: it hangs the
 * runner until the vitest timeout below. Once the lock order is fixed it passes in about a second.
 */
describe("relay ApplySync quota lock", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
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

  const ClusterConfig = Layer.merge(
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

  const rendezvous = (
    entered: Deferred.Deferred<boolean>,
    proceed: Deferred.Deferred<void>
  ) =>
    Layer.effect(
      PeerSync.PeerSync,
      Effect.gen(function*() {
        const inner = yield* PeerSync.PeerSync
        const sql = yield* SqlClient.SqlClient
        return PeerSync.PeerSync.of({
          ...inner,
          receive: (document, documentId, session, input) =>
            Effect.serviceOption(sql.transactionService).pipe(
              Effect.flatMap((transaction) => Deferred.succeed(entered, Option.isSome(transaction))),
              Effect.andThen(Deferred.await(proceed)),
              Effect.andThen(inner.receive(document, documentId, session, input))
            )
        })
      })
    )

  const live = (
    entered: Deferred.Deferred<boolean>,
    proceed: Deferred.Deferred<void>
  ) => {
    const database = Layer.merge(
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer
    )
    const inputs = Layer.mergeAll(
      database,
      ReplicaLimits.layer(limits),
      PeerRelayReceiptLimits.layer(PeerRelayReceiptLimits.defaults)
    )
    const services = SqlReplica.servicesLayerWithBindings(definition, { projections: [] })
    const peerSync = rendezvous(entered, proceed).pipe(
      Layer.provideMerge(PeerSync.layerRelay),
      Layer.provideMerge(services)
    )
    const cluster = Sharding.layer.pipe(
      Layer.provideMerge(Runners.layerNoop),
      Layer.provideMerge(SqlMessageStorage.layerWith({ prefix: ClusterStorage.messagePrefix })),
      Layer.provide([
        Layer.orDie(SqlRunnerStorage.layerWith({ prefix: ClusterStorage.runnerPrefix })),
        RunnerHealth.layerNoop
      ]),
      Layer.provide(ClusterConfig)
    )
    return DocumentEntity.layer(definition).pipe(
      Layer.provideMerge(peerSync),
      Layer.provideMerge(cluster),
      Layer.provideMerge(inputs)
    )
  }

  it.live("applies an inbound sync while relay receipt pruning holds the quota lock", () =>
    Effect.gen(function*() {
      const entered = yield* Deferred.make<boolean>()
      const proceed = yield* Deferred.make<void>()
      yield* Effect.gen(function*() {
        const gate = yield* ReplicaGate.ReplicaGate
        const sync = yield* PeerSync.PeerSync
        const permit = yield* gate.current
        const documentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
        const commandId = yield* Identity.makeCommandId
        const makeClient = yield* DocumentEntity.DocumentEntity.client
        const client = makeClient(documentId)

        const value = { title: "seed" }
        const requestHash = yield* CommandExecutor.createRequestHash({
          incarnation: permit.incarnation,
          commandId,
          document: Task,
          documentId,
          encoded: yield* Document.encode(Task, documentId, value)
        })
        yield* client.Create({
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          commandId,
          documentType: Task.name,
          payload: new TextEncoder().encode(JSON.stringify(value)),
          requestHash
        })

        const remote = Automerge.init<InternalAutomerge.Root<{ title: string }>>()
        const generated = Automerge.generateSyncMessage(remote, Automerge.initSyncState())
        const message = generated[1]!
        const messageHash = yield* Canonical.digest(message)

        // Detached on purpose: a fiber waiting for the SQL transaction permit waits inside
        // `SqlClient.makeWithTransaction`'s uninterruptible mask, so neither side of the cycle can
        // be interrupted out of it and a child fiber would hang the test instead of failing it.
        const applying = yield* Effect.forkDetach(client.ApplySync({
          replicaIncarnation: permit.incarnation,
          peerId,
          connectionEpoch: "remote-epoch",
          localConnectionEpoch: "local-epoch",
          receiveSequence: 0,
          documentType: Task.name,
          messageHash,
          message,
          lineage: Identity.genesisLineage,
          writerProvenance: []
        }))

        assert.isTrue(
          yield* Deferred.await(entered),
          "the cluster is expected to run ApplySync inside a SQL transaction"
        )

        // Stands in for the `PeerRelayClientRuntime` receipt maintenance fiber, which runs this same
        // effect every `maintenanceIntervalMillis` (1s by default).
        yield* Effect.forkDetach(sync.pruneRelayReceipts!)
        yield* Effect.sleep("200 millis")
        yield* Deferred.succeed(proceed, undefined)

        const applied = yield* Fiber.await(applying).pipe(Effect.timeoutOption("5 seconds"))
        assert.isTrue(
          applied._tag === "Some",
          "ApplySync deadlocked against PeerSync.pruneRelayReceipts"
        )
      }).pipe(
        // The layer scope is deliberately never closed. While the cycle is open the cluster storage
        // read loop is parked waiting for the same transaction permit, inside the uninterruptible
        // mask of `SqlClient.makeWithTransaction`, so closing the scope would hang instead of
        // reporting the failure.
        Effect.provide(live(entered, proceed)),
        Effect.provideServiceEffect(Scope.Scope, Scope.make())
      )
    }), 15_000)
})
