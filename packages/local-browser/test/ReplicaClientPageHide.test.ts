import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommitPublisher from "@lucas-barake/effect-local-sql/CommitPublisher"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import { RpcTest } from "effect/unstable/rpc"
import * as ReplicaClient from "../src/ReplicaClient.js"
import * as ReplicaOwner from "../src/ReplicaOwner.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"
import * as SessionManager from "../src/SessionManager.js"
import { definition, replica } from "./fixtures.js"

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
const Publisher = Layer.succeed(
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
)
const Owner = ReplicaOwner.layerHandlers(definition).pipe(
  Layer.provideMerge(Sessions),
  Layer.provide(Layer.merge(Publisher, Layer.succeed(Replica.Replica, replica)))
)

// The browser page boundary, replaced with a plain EventTarget so the pagehide lifecycle can be
// driven directly. Everything between the listener and the owner session is production code.
const withPageEvents = (target: EventTarget) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const previousAdd = globalThis.addEventListener
      const previousRemove = globalThis.removeEventListener
      globalThis.addEventListener = target.addEventListener.bind(target)
      globalThis.removeEventListener = target.removeEventListener.bind(target)
      return { previousAdd, previousRemove }
    }),
    ({ previousAdd, previousRemove }) =>
      Effect.sync(() => {
        globalThis.addEventListener = previousAdd
        globalThis.removeEventListener = previousRemove
      })
  )

const waitFor = (condition: Effect.Effect<boolean>) =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 1000; attempt++) {
      if (yield* condition) return
      yield* Effect.yieldNow
    }
    assert.fail("condition was not met")
  })

it.layer(NodeCrypto.layer)("ReplicaClient pagehide", (it) => {
  it.effect("closes the owner session when the page hides", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const target = new EventTarget()
      yield* withPageEvents(target)
      yield* Effect.scoped(Effect.gen(function*() {
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        yield* ReplicaClient.fromRpcClient(definition, rpc)
        assert.strictEqual(yield* sessions.activeCount, 1)
        target.dispatchEvent(new Event("pagehide"))
        yield* waitFor(sessions.activeCount.pipe(Effect.map((count) => count === 0)))
      }))
    }).pipe(Effect.provide(Owner)))

  it.effect("keeps the owner session when pagehide closing is disabled", () =>
    Effect.gen(function*() {
      const sessions = yield* SessionManager.SessionManager
      const target = new EventTarget()
      yield* withPageEvents(target)
      yield* Effect.scoped(Effect.gen(function*() {
        const rpc = yield* RpcTest.makeClient(ReplicaRpc.group)
        yield* ReplicaClient.fromRpcClient(definition, rpc, { closeSessionOnPageHide: false })
        assert.strictEqual(yield* sessions.activeCount, 1)
        target.dispatchEvent(new Event("pagehide"))
        yield* Effect.yieldNow
        yield* Effect.yieldNow
        assert.strictEqual(yield* sessions.activeCount, 1)
      }))
    }).pipe(Effect.provide(Owner)))
})
