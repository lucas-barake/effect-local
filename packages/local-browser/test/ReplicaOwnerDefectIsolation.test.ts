import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import * as BrowserWorkerRunner from "@effect/platform-browser/BrowserWorkerRunner"
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, it as layeredIt } from "@effect/vitest"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"
import { RpcClient } from "effect/unstable/rpc"
import * as Worker from "effect/unstable/workers/Worker"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import { definition, PeerRelayRuntime, Read, Rename, Task } from "./fixtures.js"

const limits = {
  maxBackupBytes: 1024,
  maxChunkBytes: 128,
  maxArchiveRecords: 100,
  maxJsonDepth: 16,
  maxConflictDepth: 16,
  maxConflictNodes: 10_000,
  maxConflictAlternatives: 1_000,
  maxConflictPathSegments: 16,
  maxConflictValueBytes: 1024 * 1024,
  maxConflictSourceChanges: 10_000,
  maxConflictSourceOperations: 100_000,
  maxConflictSourceBytes: 64 * 1024 * 1024,
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
  maxSessions: 2,
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

layeredIt.layer(NodeCrypto.layer)("ReplicaOwner defect isolation", (it) => {
  it.effect("answers a handler defect without tearing down the session", () =>
    Effect.gen(function*() {
      const invalidationsSubscribed = yield* Deferred.make<void>()
      const invalidationReceived = yield* Deferred.make<void>()
      let queryCalls = 0
      const Engine = Layer.merge(
        SqlReplica.layerWithBindings(definition, { projections: [] }),
        SessionManager.layer
      ).pipe(
        Layer.provideMerge(Layer.mergeAll(
          SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
          NodeCrypto.layer,
          ReplicaLimits.layer(limits),
          Rename.toLayer(({ draft, payload }) => {
            draft.title = payload.title
            return Result.succeed("renamed")
          }),
          Read.toLayer((payload) =>
            Effect.suspend(() => {
              queryCalls++
              if (queryCalls === 1) return Effect.die("poisoned query")
              return Effect.succeed([{ title: payload }])
            })
          )
        )),
        Layer.orDie
      )
      const channel = new MessageChannel()
      yield* Layer.build(
        ReplicaOwner.layerWorker(definition).pipe(
          Layer.provide(PeerRelayRuntime),
          Layer.provide(BrowserWorkerRunner.layerMessagePort(channel.port1)),
          Layer.provide(Engine)
        )
      )

      yield* Effect.gen(function*() {
        const client = yield* RpcClient.make(ReplicaRpc.group)
        const sessionId = yield* Identity.makeSessionId
        const session = yield* client.OpenSession({
          sessionId,
          protocolVersion: ReplicaRpc.protocolVersion,
          definitionHash: definition.hash
        })
        yield* client.Invalidations({
          sessionId,
          ownerEpoch: session.ownerEpoch
        }).pipe(
          Stream.tap((event) => {
            if (event._tag === "InvalidationsReady") return Deferred.succeed(invalidationsSubscribed, undefined)
            if (event._tag === "Invalidation") return Deferred.succeed(invalidationReceived, undefined)
            return Effect.void
          }),
          Stream.runDrain,
          Effect.forkChild
        )
        yield* Deferred.await(invalidationsSubscribed)

        const poisoned = yield* client.Query({ sessionId, query: Read.name, payload: "one" }).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(poisoned), "the poisoned query must not succeed")
        yield* client.Create({
          sessionId,
          document: Task.name,
          commandId: yield* Identity.makeCommandId,
          value: { title: "after defect" }
        })
        yield* Deferred.await(invalidationReceived)

        const answered = yield* client.Query({ sessionId, query: Read.name, payload: "two" })
        assert.deepStrictEqual(answered, [{ title: "two" }])
        assert.strictEqual(queryCalls, 2)
      }).pipe(
        Effect.provide(RpcClient.layerProtocolWorker({ size: 1, concurrency: 4 })),
        Effect.provide(Worker.layerSpawner(() => channel.port2)),
        Effect.provide(BrowserWorker.layerPlatform)
      )
    }).pipe(Effect.scoped))
})
