import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as ReplicaHealth from "../src/ReplicaHealth.js"

describe("ReplicaHealth restore lifecycle", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "health-replica",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const limits: ReplicaLimits.Values = {
    maxBackupBytes: 1024 * 1024,
    maxChunkBytes: 64 * 1024,
    maxArchiveRecords: 1000,
    maxJsonDepth: 32,
    maxSyncMessageBytes: 64 * 1024,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1000,
    maxSyncOperationsPerMessage: 10_000,
    maxPendingBytesPerDocument: 1024 * 1024,
    maxPendingBytesPerPeer: 1024 * 1024,
    maxPendingBytesPerReplica: 1024 * 1024,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1000,
    maxPendingChangesPerPeer: 1000,
    maxPendingChangesPerReplica: 1000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 10_000,
    maxSessions: 8,
    maxStreamsPerSession: 8,
    maxInFlightPerSession: 32,
    maxQueuedRpc: 128,
    maxQueuedPermits: 1024,
    maxActiveRestores: 128,
    maxRestoresPerSession: 32,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  }
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provideMerge(Database))
  const Gate = ReplicaGate.layer.pipe(
    Layer.provideMerge(Layer.merge(Bootstrap, ReplicaLimits.layer(limits)))
  )
  const Live = ReplicaHealth.layer(definition).pipe(Layer.provideMerge(Gate))

  it.effect("never re-reports a restore after its installation ends", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const observed: Array<ReplicaStatus.ReplicaStatus> = []
      const subscribed = yield* Deferred.make<void>()
      // Drains until the replica reports itself usable again after the install, so the whole restore
      // lifecycle is observed instead of whichever prefix the consumer happened to be scheduled for.
      const consumer = yield* health.status.pipe(
        Stream.tap((status) =>
          Effect.sync(() => observed.push(status)).pipe(
            Effect.andThen(Deferred.succeed(subscribed, undefined))
          )
        ),
        Stream.takeUntil((status) => status._tag === "Ready" && observed.some((seen) => seen._tag === "ReadOnly")),
        Stream.runDrain,
        Effect.forkChild
      )
      // The first emission is the current status handed to a new subscriber, so awaiting it proves the
      // subscription is live before the restore starts publishing.
      yield* Deferred.await(subscribed)
      // The exact shape `BackupStore.restore` uses: the reporter is scoped to the whole restore, and the
      // install claim is wrapped in its own inner scope.
      yield* Effect.scoped(Effect.gen(function*() {
        const reporter = yield* health.restoring
        yield* reporter.progress(2048)
        yield* Effect.scoped(reporter.installing)
      }))
      yield* Fiber.join(consumer)
      // Installation is the last phase of a restore, so once it is reported the restore must never be
      // reported as ingesting again. Anything after the install is the replica going back to work.
      const afterInstall = observed.slice(observed.findIndex((status) => status._tag === "ReadOnly") + 1)
      assert.deepStrictEqual(afterInstall, [
        { _tag: "Ready", pendingCommands: 0 } satisfies ReplicaStatus.ReplicaStatus
      ])
    }).pipe(Effect.provide(Live)))
})
