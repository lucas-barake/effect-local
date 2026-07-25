import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Scheduler from "effect/Scheduler"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as ReplicaHealth from "../src/ReplicaHealth.js"

describe("ReplicaHealth", () => {
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

  it.effect("delivers the current status to the first subscriber", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      assert.deepStrictEqual(
        yield* Stream.runHead(health.status),
        Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 0 })
      )
    }).pipe(Effect.provide(Live)))

  it.effect("reports an ingesting restore, then read-only while it installs", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      yield* Effect.scoped(Effect.gen(function*() {
        const restore = yield* health.restoring
        yield* restore.progress(2048)
        assert.deepStrictEqual(
          yield* Stream.runHead(health.status),
          Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Restoring", processedBytes: 2048 })
        )
        yield* Effect.scoped(Effect.gen(function*() {
          yield* restore.installing
          assert.deepStrictEqual(
            yield* Stream.runHead(health.status),
            Option.some<ReplicaStatus.ReplicaStatus>({
              _tag: "ReadOnly",
              reason: "A backup restore is installing"
            })
          )
        }))
      }))
      assert.deepStrictEqual(
        yield* Stream.runHead(health.status),
        Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 0 })
      )
    }).pipe(Effect.provide(Live)))

  it.effect("keeps reporting a restore while a second one is still ingesting", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      yield* Effect.scoped(Effect.gen(function*() {
        const second = yield* health.restoring
        yield* Effect.scoped(Effect.gen(function*() {
          const first = yield* health.restoring
          yield* first.progress(64)
        }))
        yield* second.progress(4096)
        assert.deepStrictEqual(
          yield* Stream.runHead(health.status),
          Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Restoring", processedBytes: 4096 })
        )
      }))
    }).pipe(Effect.provide(Live)))

  it.effect("stays read-only while one restore installs and another is still ingesting", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      yield* Effect.scoped(Effect.gen(function*() {
        const installer = yield* health.restoring
        const ingester = yield* health.restoring
        yield* ingester.progress(2048)
        yield* Effect.scoped(Effect.gen(function*() {
          yield* installer.installing
          assert.deepStrictEqual(
            yield* Stream.runHead(health.status),
            Option.some<ReplicaStatus.ReplicaStatus>({
              _tag: "ReadOnly",
              reason: "A backup restore is installing"
            })
          )
        }))
      }))
    }).pipe(Effect.provide(Live)))

  it.effect("clears every restore condition when concurrent restores finish", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const restore = Effect.scoped(Effect.gen(function*() {
        const reporter = yield* health.restoring
        yield* reporter.progress(2048)
      }))
      // The runtime may preempt a fiber between any two operations; the default budget of 2048 only makes
      // that rare, and a restore ingest or a sample burns far more than 2048 operations between suspensions.
      // Shrinking the budget schedules the very same interleaving deterministically.
      for (const maxOps of [4, 8, 12, 16, 24]) {
        yield* Effect.all([restore, restore], { concurrency: "unbounded", discard: true }).pipe(
          Effect.provideService(Scheduler.MaxOpsBeforeYield, maxOps)
        )
        assert.deepStrictEqual(
          yield* Stream.runHead(health.status),
          Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 0 }),
          `a restore condition survived its scope with an operation budget of ${maxOps}`
        )
      }
    }).pipe(Effect.provide(Live)))

  it.effect("does not report a fenced replica while a gate claim publishes its permit", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const gate = yield* ReplicaGate.ReplicaGate
      // `ReplicaGate.claim` bumps `writer_generation` inside its transaction and only republishes the
      // matching permit once that transaction has committed and released the connection. Shrinking the
      // operation budget schedules a sampler into that window deterministically; a long restore install
      // reaches it on the default budget by spending far more than 2048 operations inside the claim.
      for (const maxOps of [4, 8, 16]) {
        const collected: Array<ReplicaStatus.ReplicaStatus> = []
        const observer = yield* health.status.pipe(
          Stream.tap((status) => Effect.sync(() => collected.push(status))),
          Stream.runDrain,
          Effect.forkChild
        )
        yield* Effect.yieldNow
        const holding = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const claim = yield* gate.claim(() =>
          Deferred.succeed(holding, undefined).pipe(Effect.andThen(Deferred.await(release)))
        ).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, maxOps), Effect.forkChild)
        yield* Deferred.await(holding)
        const sampler = yield* health.sample.pipe(Effect.forkChild)
        yield* Effect.yieldNow
        yield* Effect.yieldNow
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(claim)
        yield* Fiber.join(sampler)
        yield* Effect.yieldNow
        yield* Fiber.interrupt(observer)
        assert.deepStrictEqual(
          collected.filter((status) => status._tag === "ReadOnly"),
          [],
          `a finishing claim published a fenced status with an operation budget of ${maxOps}`
        )
      }
    }).pipe(Effect.provide(Live)))

  it.effect("reports a fenced replica as read-only", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1
        WHERE singleton = 1`
      assert.deepStrictEqual(
        yield* Stream.runHead(health.status),
        Option.some<ReplicaStatus.ReplicaStatus>({
          _tag: "ReadOnly",
          reason: "Another writer generation owns this replica"
        })
      )
    }).pipe(Effect.provide(Live), Effect.provide(Database)))

  it.effect("reports a missing metadata singleton as failed, and clears it on repair", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      const before = yield* sql<{ readonly replica_id: string; readonly writer_generation: number }>`SELECT
        replica_id, writer_generation FROM effect_local_metadata WHERE singleton = 1`
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`
      assert.deepStrictEqual(
        yield* Stream.runHead(health.status),
        Option.some<ReplicaStatus.ReplicaStatus>({
          _tag: "Failed",
          message: "Replica metadata is missing"
        })
      )
      yield* sql`INSERT INTO effect_local_metadata
        (singleton, storage_format_version, replica_id, replica_incarnation, writer_generation,
         definition_hash, commit_sequence)
        VALUES (1, 1, ${before[0].replica_id}, 0, ${before[0].writer_generation}, ${definition.hash}, 0)`
      assert.deepStrictEqual(
        yield* Stream.runHead(health.status),
        Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 0 })
      )
    }).pipe(Effect.provide(Live), Effect.provide(Database)))

  it.effect("ends every live subscriber when the layer scope closes", () =>
    Effect.gen(function*() {
      const ended = yield* Deferred.make<void>()
      const scope = yield* Scope.make()
      const context = yield* Scope.provide(Layer.build(Live), scope)
      const health = Context.get(context, ReplicaHealth.ReplicaHealth)
      // Forked outside the layer scope on purpose: the subscriber must be ended by the hub's shutdown,
      // not merely swept up by the same scope closing, or it would hang holding a session stream permit.
      yield* health.status.pipe(
        Stream.runDrain,
        Effect.andThen(Deferred.succeed(ended, undefined)),
        Effect.forkChild
      )
      yield* Effect.yieldNow
      assert.isTrue(Option.isNone(yield* Deferred.poll(ended)))
      yield* Scope.close(scope, Exit.void)
      yield* Deferred.await(ended)
    }).pipe(Effect.provide(Database)))

  it.effect("suppresses an emission when a sample leaves the status unchanged", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const collected: Array<ReplicaStatus.ReplicaStatus> = []
      const consumer = yield* health.status.pipe(
        Stream.tap((status) => Effect.sync(() => collected.push(status))),
        Stream.runDrain,
        Effect.forkChild
      )
      yield* Effect.yieldNow
      yield* health.sample
      yield* health.sample
      yield* Effect.yieldNow
      yield* Fiber.interrupt(consumer)
      assert.deepStrictEqual(collected, [{ _tag: "Ready", pendingCommands: 0 }])
    }).pipe(Effect.provide(Live)))
  it.effect("pushes a status change to an existing subscriber without a new subscription", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      const seen = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
      // Taking the first element is what proves the subscription is live, so no yield is load bearing here.
      yield* health.status.pipe(Stream.runForEach((status) => Queue.offer(seen, status)), Effect.forkChild)
      assert.deepStrictEqual(yield* Queue.take(seen), { _tag: "Ready", pendingCommands: 0 })
      yield* sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1
        WHERE singleton = 1`
      // Only the background poller can deliver this: the subscriber never resubscribes and never samples.
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(yield* Queue.take(seen), {
        _tag: "ReadOnly",
        reason: "Another writer generation owns this replica"
      })
    }).pipe(Effect.provide(Live), Effect.provide(Database)))

  it.effect("reports transient storage loss as degraded without failing the stream", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      yield* sql`DROP TABLE effect_local_commit_outbox`
      const exit = yield* Effect.exit(Stream.runHead(health.status))
      assert.isTrue(Exit.isSuccess(exit))
      const degraded = Option.getOrThrow(yield* Stream.runHead(health.status))
      assert.strictEqual(degraded._tag, "Degraded")
      // The reason has to name the table, not just restate the error tag, or an operator learns nothing.
      if (degraded._tag === "Degraded") {
        assert.include(degraded.reason, "no such table: effect_local_commit_outbox")
      }
    }).pipe(Effect.provide(Live), Effect.provide(Database)))

  it.effect("reports an undecodable durable row as failed", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_metadata SET writer_generation = 'not-a-number' WHERE singleton = 1`
      const failed = Option.getOrThrow(yield* Stream.runHead(health.status))
      assert.strictEqual(failed._tag, "Failed")
      if (failed._tag === "Failed") {
        assert.include(failed.message, "writer_generation")
      }
    }).pipe(Effect.provide(Live), Effect.provide(Database)))
})
