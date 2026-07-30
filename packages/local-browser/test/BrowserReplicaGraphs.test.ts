import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import * as BrowserWorkerRunner from "@effect/platform-browser/BrowserWorkerRunner"
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, it } from "@effect/vitest"
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as Worker from "effect/unstable/workers/Worker"
import * as BrowserReplica from "../src/BrowserReplica.js"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as SessionManager from "../src/SessionManager.js"
import { Task } from "./fixtures.js"

const definition = ReplicaDefinition.make({
  name: "browser-replica-graphs",
  documents: DocumentSet.make(Task),
  mutations: [],
  projections: [],
  queries: []
})

const limits = {
  maxBackupBytes: 1024,
  maxChunkBytes: 128,
  maxArchiveRecords: 100,
  maxJsonDepth: 16,
  maxSyncMessageBytes: 1024,
  maxPeerSendMillis: 1_000,
  maxSyncChangesPerMessage: 10,
  maxSyncDependencyEdgesPerMessage: 20,
  maxSyncOperationsPerMessage: 100,
  maxPendingBytesPerDocument: 1024,
  maxPendingBytesPerPeer: 2048,
  maxPendingBytesPerReplica: 4096,
  maxPendingAgeMillis: 60_000,
  maxPendingChangesPerDocument: 10,
  maxPendingChangesPerPeer: 20,
  maxPendingChangesPerReplica: 40,
  maxPendingDependencyEdgesPerDocument: 100,
  maxPendingDependencyEdgesPerPeer: 200,
  maxPendingDependencyEdgesPerReplica: 400,
  maxSessions: 8,
  maxStreamsPerSession: 2,
  maxInFlightPerSession: 2,
  maxQueuedRpc: 4,
  maxQueuedPermits: 4,
  maxActiveRestores: 4,
  maxRestoresPerSession: 2,
  maxRestoreMillis: 30_000,
  maxRestorePullMillis: 10_000,
  maxRestoreCoalesceMillis: 25,
  maxRestoreErrorBytes: 4_096
} satisfies ReplicaLimits.Values

const engineLayer = Layer.merge(
  SqlReplica.layerWithBindings(definition, { projections: [] }),
  SessionManager.layer
).pipe(
  Layer.provideMerge(Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    ReplicaLimits.layer(limits)
  )),
  Layer.orDie
)

/** One owner over one MessagePort, the wiring `OwnershipCoordinator` serves an attached tab with. */
const startOwner = Effect.gen(function*() {
  const channel = new MessageChannel()
  yield* Layer.build(
    ReplicaOwner.layerWorker(definition).pipe(
      Layer.provide(BrowserWorkerRunner.layerMessagePort(channel.port1)),
      Layer.provide(engineLayer)
    )
  )
  return channel.port2
})

const graph = (port: MessagePort) =>
  BrowserReplica.layerWith(definition, { size: 1, concurrency: 4 }).pipe(
    Layer.provide(Worker.layerSpawner(() => port)),
    Layer.provide(BrowserWorker.layerPlatform)
  )

// One `Atom.runtime` memo map is shared app wide, so two replica graphs in one application build
// under one memo map. Layer memoization is by reference, so a Layer value that republishes its own
// graph's `ReplicaClient` cannot be shared between the two constructors.
it.layer(NodeCrypto.layer)("BrowserReplica graphs", (it) => {
  it.effect("keeps two replica graphs on their own owner under one memo map", () =>
    Effect.gen(function*() {
      const portA = yield* startOwner
      const portB = yield* startOwner
      const memoMap = yield* Layer.makeMemoMap
      const scope = yield* Scope.make()
      const first = yield* Layer.buildWithMemoMap(graph(portA), memoMap, scope)
      const second = yield* Layer.buildWithMemoMap(graph(portB), memoMap, scope)

      const documentId = yield* Context.get(first, Replica.Replica).create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "only in the first replica" }
      })
      const exit = yield* Effect.exit(Context.get(second, Replica.Replica).get(Task, documentId))
      assert.isTrue(
        Exit.isFailure(exit),
        "the second replica answered for a document that only exists in the first"
      )
      assert.notStrictEqual(
        Context.get(first, PeerConnectionStatus.PeerConnectionStatus),
        Context.get(second, PeerConnectionStatus.PeerConnectionStatus)
      )

      yield* Scope.close(scope, Exit.void)
    }).pipe(Effect.scoped))
})
