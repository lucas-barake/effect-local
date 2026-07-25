import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
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

  // A replica whose blocked documents do not all map to a projection: `Note` carries none, `Task` does.
  const Note = Document.make("Note", { schema: Schema.Struct({ body: Schema.String }), version: 1 })
  const TaskById = Projection.make("TaskById", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, title: Schema.String }),
    key: (row) => row.sourceDocumentId,
    project: (snapshot) => [{ sourceDocumentId: snapshot.documentId, title: snapshot.value.title }]
  })
  const projectedDefinition = ReplicaDefinition.make({
    name: "health-projected-replica",
    documents: DocumentSet.make(Task, Note),
    mutations: [],
    projections: [TaskById],
    queries: []
  })
  const ProjectedDatabase = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const ProjectedBootstrap = ReplicaBootstrap.layer(projectedDefinition).pipe(Layer.provideMerge(ProjectedDatabase))
  const ProjectedGate = ReplicaGate.layer.pipe(
    Layer.provideMerge(Layer.merge(ProjectedBootstrap, ReplicaLimits.layer(limits)))
  )
  const Projected = ReplicaHealth.layer(projectedDefinition).pipe(Layer.provideMerge(ProjectedGate))

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

  it.effect("does not report a fenced replica when a claim commits between the sampler's reads", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const gate = yield* ReplicaGate.ReplicaGate
      const collected: Array<ReplicaStatus.ReplicaStatus> = []
      const observer = yield* health.status.pipe(
        Stream.tap((status) => Effect.sync(() => collected.push(status))),
        Stream.runDrain,
        Effect.forkChild
      )
      yield* Effect.yieldNow
      // The sampler reads `writer_generation` before it reads the gate's owner and permit. A claim that
      // both starts and finishes inside that gap leaves the sampler holding a generation older than the
      // permit it reads next, which is this process advancing its own replica rather than a foreign
      // writer fencing it. The operation budget schedules that interleaving deterministically.
      const sampler = yield* health.sample.pipe(
        Effect.provideService(Scheduler.MaxOpsBeforeYield, 64),
        Effect.forkChild
      )
      const claim = yield* gate.claim(() => Effect.void).pipe(Effect.forkChild)
      yield* Fiber.join(sampler)
      yield* Fiber.join(claim)
      yield* Effect.yieldNow
      yield* Fiber.interrupt(observer)
      const permit = yield* gate.current
      // Nothing fenced this replica: the durable generation is exactly the one the permit carries.
      assert.strictEqual((yield* gate.refresh).writerGeneration, permit.writerGeneration)
      assert.deepStrictEqual(collected.filter((status) => status._tag === "ReadOnly"), [])
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

  it.effect("reports a blocked projection that a projectionless blocked document sorts ahead of", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      const insertBlocked = (documentId: string, documentType: string) =>
        sql`INSERT INTO effect_local_documents (
          document_id, document_type, schema_version, observed_versions,
          materialized_heads, accepted_heads, tombstone, projection_status
        ) VALUES (${documentId}, ${documentType}, 1, '[]', '[]', '[]', 0, 'Blocked')`
      // `Note` has no projection and its id sorts first, so it is the row the sampler's `LIMIT 1` sees.
      yield* insertBlocked("doc_00000000-0000-4000-8000-000000000001", "Note")
      yield* insertBlocked("doc_00000000-0000-4000-8000-000000000002", "Task")
      assert.deepStrictEqual(
        yield* Stream.runHead(health.status),
        Option.some<ReplicaStatus.ReplicaStatus>({
          _tag: "ProjectionBlocked",
          projection: "TaskById",
          reason: "A Task document is not ready for projection"
        })
      )
    }).pipe(Effect.provide(Projected), Effect.provide(ProjectedDatabase)))

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
  it.effect("does not report a fenced replica when a local claim finishes after the generation read", () =>
    Effect.gen(function*() {
      const atClaiming = yield* Deferred.make<void>()
      const releaseClaiming = yield* Deferred.make<void>()
      let armed = false
      // The sampler reads `writer_generation` from the database before it reads the gate. A local
      // `ReplicaGate.claim` that commits inside that window leaves the sampler holding a generation older
      // than the permit it reads next, which is the only seam that reproduces the read skew deterministically.
      const SkewedGate = Layer.effect(
        ReplicaGate.ReplicaGate,
        Effect.gen(function*() {
          const gate = yield* ReplicaGate.ReplicaGate
          return ReplicaGate.ReplicaGate.of({
            ...gate,
            claiming: Effect.suspend(() => {
              if (!armed) return gate.claiming
              armed = false
              return Deferred.succeed(atClaiming, undefined).pipe(
                Effect.andThen(Deferred.await(releaseClaiming)),
                Effect.andThen(gate.claiming)
              )
            })
          })
        })
      ).pipe(Layer.provideMerge(Gate))
      yield* Effect.gen(function*() {
        const health = yield* ReplicaHealth.ReplicaHealth
        const gate = yield* ReplicaGate.ReplicaGate
        const collected: Array<ReplicaStatus.ReplicaStatus> = []
        const subscribed = yield* Deferred.make<void>()
        const observer = yield* health.status.pipe(
          Stream.tap((status) =>
            Effect.sync(() => collected.push(status)).pipe(
              Effect.andThen(Deferred.succeed(subscribed, undefined))
            )
          ),
          Stream.runDrain,
          Effect.forkChild
        )
        yield* Deferred.await(subscribed)
        armed = true
        const sampler = yield* health.sample.pipe(Effect.forkChild)
        yield* Deferred.await(atClaiming)
        yield* Effect.orDie(gate.claim(() => Effect.void))
        yield* Deferred.succeed(releaseClaiming, undefined)
        yield* Fiber.join(sampler)
        yield* Effect.yieldNow
        yield* Effect.yieldNow
        yield* Effect.yieldNow
        yield* Fiber.interrupt(observer)
        assert.deepStrictEqual(
          collected.filter((status) => status._tag === "ReadOnly"),
          [],
          "a completed local claim was reported as another writer generation fencing the replica"
        )
      }).pipe(Effect.provide(ReplicaHealth.layer(definition).pipe(Layer.provideMerge(SkewedGate))))
    }))
  it.effect("reports a projection the query path already refuses to serve", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      // `QueryExecutor` refuses a query when the registry, a document projection row, or a document is not
      // `Ready`. `Rebuilding` is one of those states, so reporting `Ready` here would tell a consumer the
      // replica is usable while every query on it fails.
      yield* sql`INSERT INTO effect_local_projection_registry
        (projection_name, table_name, projection_version, schema_checksum, status)
        VALUES ('TaskTitle', 'task_title_v1', 1, 'sha', 'Rebuilding')`
      const status = Option.getOrThrow(yield* Stream.runHead(health.status))
      assert.strictEqual(status._tag, "Ready")
    }).pipe(Effect.provide(Live), Effect.provide(Database)))

  it.effect("reports a declared projection that is rebuilding as blocked", () =>
    Effect.gen(function*() {
      const TaskTitle = Projection.make("TaskTitle", {
        document: Task,
        version: 1,
        Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, title: Schema.String }),
        key: (row) => row.sourceDocumentId,
        project: () => []
      })
      const projected = ReplicaDefinition.make({
        name: "health-replica",
        documents: DocumentSet.make(Task),
        mutations: [],
        projections: [TaskTitle],
        queries: []
      })
      const ProjectedLive = ReplicaHealth.layer(projected).pipe(Layer.provideMerge(Gate))
      yield* Effect.gen(function*() {
        const health = yield* ReplicaHealth.ReplicaHealth
        const sql = yield* SqlClient.SqlClient
        yield* sql`INSERT INTO effect_local_projection_registry
          (projection_name, table_name, projection_version, schema_checksum, status)
          VALUES ('TaskTitle', 'task_title_v1', 1, 'sha', 'Rebuilding')`
        const status = Option.getOrThrow(yield* Stream.runHead(health.status))
        assert.strictEqual(status._tag, "ProjectionBlocked")
        if (status._tag === "ProjectionBlocked") {
          assert.strictEqual(status.projection, "TaskTitle")
        }
      }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database))
    }))
})
