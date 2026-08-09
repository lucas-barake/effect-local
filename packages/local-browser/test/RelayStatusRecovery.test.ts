import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import { RpcTest } from "effect/unstable/rpc"
import * as RpcClientError from "effect/unstable/rpc/RpcClientError"
import * as WorkerError from "effect/unstable/workers/WorkerError"
import * as ReplicaClient from "../src/ReplicaClient.js"
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

const Sessions = SessionManager.layer.pipe(Layer.provide(ReplicaLimits.layer(limits)))

const Publisher = Layer.merge(
  Layer.succeed(
    CommitPublisher.CommitPublisher,
    CommitPublisher.CommitPublisher.of({
      publishPending: Effect.succeed(0),
      invalidate: () => Effect.void,
      subscribe: Effect.succeed({
        watermark: Identity.CommitSequence.make(0),
        refreshGeneration: 0,
        events: Stream.never
      })
    })
  ),
  DeliveryPublisher
)

const Owner = ReplicaOwner.layerHandlers(definition).pipe(
  Layer.provide(PeerConnectionStatus.layer),
  Layer.provide(RelayConnectionStatus.layerNotConfigured),
  Layer.provideMerge(Sessions),
  Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, replica)))
)

type RelayStatusStream = Stream.Stream<
  RelayConnectionStatus.Status,
  ReplicaError.ReplicaError | RpcClientError.RpcClientError
>

/** The exact shape an owner engine reset produces on a tab's rpc port. */
const workerReceiveError = () =>
  new RpcClientError.RpcClientError({
    reason: new WorkerError.WorkerReceiveError({
      message: "An error event was emitter",
      cause: "An error event was emitter"
    })
  })

it.layer(NodeCrypto.layer)("relay status recovery", (it) => {
  it.effect("resubscribes the relay status stream after a transient transport loss", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const transportFailed = yield* Deferred.make<void>()
      let subscriptions = 0
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "RelayConnectionStatus") return Reflect.get(target, property, receiver)
          return (payload: never) =>
            Stream.unwrap(Effect.sync((): RelayStatusStream => {
              subscriptions++
              return subscriptions === 1
                ? Stream.make(RelayConnectionStatus.connected).pipe(
                  Stream.concat(
                    Stream.fromEffect(
                      Deferred.succeed(transportFailed, undefined).pipe(
                        Effect.andThen(Effect.fail(workerReceiveError()))
                      )
                    )
                  )
                )
                : target.RelayConnectionStatus(payload)
            }))
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, reconnecting)
      const seen = yield* Queue.unbounded<RelayConnectionStatus.Status>()
      const fiber = yield* client.relayConnectionStatus.status.pipe(
        Stream.runForEach((status) => Queue.offer(seen, status)),
        Effect.forkChild({ startImmediately: true })
      )

      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.connected)
      yield* Deferred.await(transportFailed)
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      // A dead subscription leaves the reader fiber failed and the owner never asked again, which
      // is what parks the tab's relay indicator forever after an owner engine reset.
      assert.isUndefined(fiber.pollUnsafe())
      assert.strictEqual(subscriptions, 2)
      // The owner reports `NotConfigured` in this topology, so a second value proves the tab
      // reconnected to the owner rather than parking on the dead subscription.
      assert.deepStrictEqual(yield* Queue.take(seen), RelayConnectionStatus.notConfigured)
      yield* Fiber.interrupt(fiber)
    })).pipe(Effect.provide(Owner)))

  it.effect("resubscribes a peer status stream after a transient transport loss", () =>
    Effect.scoped(Effect.gen(function*() {
      const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
      const transportFailed = yield* Deferred.make<void>()
      const peerId = Identity.PeerId.make("peer_00000000-0000-4000-8000-000000000001")
      let subscriptions = 0
      const reconnecting = new Proxy(rpc, {
        get(target, property, receiver) {
          if (property !== "PeerConnectionStatus") return Reflect.get(target, property, receiver)
          return (payload: never) =>
            Stream.unwrap(Effect.sync((): Stream.Stream<
              PeerConnectionStatus.Status,
              ReplicaError.ReplicaError | RpcClientError.RpcClientError
            > => {
              subscriptions++
              return subscriptions === 1
                ? Stream.make(PeerConnectionStatus.connected).pipe(
                  Stream.concat(
                    Stream.fromEffect(
                      Deferred.succeed(transportFailed, undefined).pipe(
                        Effect.andThen(Effect.fail(workerReceiveError()))
                      )
                    )
                  )
                )
                : target.PeerConnectionStatus(payload)
            }))
        }
      })
      const client = yield* ReplicaClient.fromRpcClient(definition, reconnecting)
      const seen = yield* Queue.unbounded<PeerConnectionStatus.Status>()
      const fiber = yield* client.peerConnectionStatus.status(peerId).pipe(
        Stream.runForEach((status) => Queue.offer(seen, status)),
        Effect.forkChild({ startImmediately: true })
      )

      assert.deepStrictEqual(yield* Queue.take(seen), PeerConnectionStatus.connected)
      yield* Deferred.await(transportFailed)
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      // Same defect shape as the relay status stream: an owner engine reset must not leave the
      // page's per-peer indicator parked on a dead subscription.
      assert.isUndefined(fiber.pollUnsafe())
      assert.strictEqual(subscriptions, 2)
      assert.deepStrictEqual(yield* Queue.take(seen), PeerConnectionStatus.disconnected)
      yield* Fiber.interrupt(fiber)
    })).pipe(Effect.provide(Owner)))
})
