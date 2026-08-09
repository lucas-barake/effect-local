import * as BrowserWorker from "@effect/platform-browser/BrowserWorker"
import * as BrowserWorkerRunner from "@effect/platform-browser/BrowserWorkerRunner"
import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import { RpcClient } from "effect/unstable/rpc"
import * as Worker from "effect/unstable/workers/Worker"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import { definition, DeliveryPublisher, replica } from "./fixtures.js"

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

it.layer(NodeCrypto.layer)("ReplicaOwner defect isolation", (it) => {
  it.effect("answers a handler defect without tearing down the session", () =>
    Effect.gen(function*() {
      const invalidationsSubscribed = yield* Deferred.make<void>()
      let queryCalls = 0
      const rigged: Replica.Replica["Service"] = {
        ...replica,
        query: (query, ...payload) =>
          Effect.suspend(() => {
            queryCalls++
            return queryCalls === 1
              ? Effect.die(new TypeError("poisoned query"))
              : replica.query(query, ...payload)
          })
      }
      const Publisher = Layer.merge(
        Layer.succeed(
          CommitPublisher.CommitPublisher,
          CommitPublisher.CommitPublisher.of({
            publishPending: Effect.succeed(0),
            invalidate: () => Effect.void,
            subscribe: Deferred.succeed(invalidationsSubscribed, undefined).pipe(
              Effect.as({
                watermark: Identity.CommitSequence.make(0),
                refreshGeneration: 0,
                events: Stream.never
              })
            )
          })
        ),
        DeliveryPublisher
      )
      const channel = new MessageChannel()
      const Owner = ReplicaOwner.layerWorker(definition).pipe(
        Layer.provide(BrowserWorkerRunner.layerMessagePort(channel.port1)),
        Layer.provide(PeerConnectionStatus.layer),
        Layer.provide(RelayConnectionStatus.layerNotConfigured),
        Layer.provideMerge(SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(limits)))),
        Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, rigged)))
      )
      yield* Layer.build(Owner)

      yield* Effect.gen(function*() {
        const client = yield* RpcClient.make(ReplicaRpc.group)
        const sessionId = yield* Identity.makeSessionId
        const session = yield* client.OpenSession({
          sessionId,
          protocolVersion: ReplicaRpc.protocolVersion,
          definitionHash: definition.hash
        })
        const invalidations = yield* client.Invalidations({
          sessionId,
          ownerEpoch: session.ownerEpoch
        }).pipe(Stream.runDrain, Effect.forkChild)
        yield* Deferred.await(invalidationsSubscribed)

        const poisoned = yield* client.Query({ sessionId, query: "Read", payload: "one" }).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(poisoned), "the poisoned query must not succeed")
        assert.isUndefined(invalidations.pollUnsafe(), "an unrelated invalidation stream must stay active")

        const answered = yield* client.Query({ sessionId, query: "Read", payload: "two" })
        assert.deepStrictEqual(answered, [{ title: "two" }])
        assert.strictEqual(queryCalls, 2)
      }).pipe(
        Effect.provide(RpcClient.layerProtocolWorker({ size: 1, concurrency: 4 })),
        Effect.provide(Worker.layerSpawner(() => channel.port2)),
        Effect.provide(BrowserWorker.layerPlatform)
      )
    }).pipe(Effect.scoped))
})
