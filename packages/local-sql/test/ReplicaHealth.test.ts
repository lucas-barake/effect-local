import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
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
import * as SqlError from "effect/unstable/sql/SqlError"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as ReplicaHealth from "../src/ReplicaHealth.js"
import { withGateLimits } from "./fixtures/limits.js"
import { nativeError } from "./TestErrors.js"

describe("ReplicaHealth", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "health-replica",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provideMerge(Database))
  const Gate = ReplicaGate.layer.pipe(withGateLimits, Layer.provideMerge(Bootstrap))
  const Live = ReplicaHealth.layer(definition, ReplicaHealth.defaultOptions).pipe(Layer.provideMerge(Gate))

  const healthWithSql = (
    transform: (
      text: string,
      statement: Effect.Effect<unknown, unknown, unknown>
    ) => Effect.Effect<unknown, unknown, unknown>
  ) => {
    const BaseSql = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
    const InstrumentedSql = Layer.effect(
      SqlClient.SqlClient,
      Effect.gen(function*() {
        const sql = yield* SqlClient.SqlClient
        const execute = (template: TemplateStringsArray, ...args: Array<any>) => {
          const statement = sql(template, ...args)
          let text = ""
          if (Array.isArray(template)) text = template.join("")
          return transform(text, statement)
        }
        return Object.assign(execute, sql) satisfies SqlClient.SqlClient
      })
    ).pipe(Layer.provideMerge(BaseSql))
    const InstrumentedDatabase = Layer.merge(InstrumentedSql, NodeCrypto.layer)
    const InstrumentedBootstrap = ReplicaBootstrap.layer(definition).pipe(
      Layer.provideMerge(InstrumentedDatabase)
    )
    const InstrumentedGate = ReplicaGate.layer.pipe(
      withGateLimits,
      Layer.provideMerge(InstrumentedBootstrap)
    )
    const Health = ReplicaHealth.layer(definition, ReplicaHealth.defaultOptions).pipe(
      Layer.provideMerge(InstrumentedGate)
    )
    return Layer.merge(Health, InstrumentedDatabase)
  }

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
  const ProjectedGate = ReplicaGate.layer.pipe(withGateLimits, Layer.provideMerge(ProjectedBootstrap))
  const Projected = ReplicaHealth.layer(projectedDefinition, ReplicaHealth.defaultOptions).pipe(
    Layer.provideMerge(ProjectedGate)
  )

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

  it.effect("keeps a repairing restore visible while it installs", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_metadata SET writer_generation = 'not-a-number' WHERE singleton = 1`
      assert.strictEqual(Option.getOrThrow(yield* Stream.runHead(health.status))._tag, "Failed")
      yield* Effect.scoped(Effect.gen(function*() {
        const restore = yield* health.restoring
        assert.deepStrictEqual(
          yield* Stream.runHead(health.status),
          Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Restoring", processedBytes: 0 })
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
    }).pipe(Effect.provide(Live), Effect.provide(Database)))

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

  it.effect("ends a status stream whose startup sample is still running", () =>
    Effect.scoped(Effect.gen(function*() {
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const ended = yield* Deferred.make<void>()
      let armed = false
      let blocked = false
      const Services = healthWithSql((text, statement) => {
        if (!armed || blocked || !text.includes("SELECT COUNT(*) AS count")) return statement
        blocked = true
        return statement.pipe(
          Effect.tap(() => Deferred.succeed(entered, undefined)),
          Effect.tap(() => Deferred.await(release))
        )
      })
      const layerScope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(layerScope, Exit.void))
      const context = yield* Scope.provide(Layer.build(Services), layerScope)
      const health = Context.get(context, ReplicaHealth.ReplicaHealth)
      armed = true
      const consumer = yield* health.status.pipe(
        Stream.runDrain,
        Effect.andThen(Deferred.succeed(ended, undefined)),
        Effect.forkChild
      )
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(release, undefined).pipe(
          Effect.andThen(Fiber.interrupt(consumer))
        )
      )
      yield* Deferred.await(entered)
      yield* Scope.close(layerScope, Exit.void)
      yield* Effect.yieldNow
      assert.isTrue(
        Option.isSome(yield* Deferred.poll(ended)),
        "closing the health layer left the started status stream blocked"
      )
    })))

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

  it.effect("delivers only the newest status to a slow consumer", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const release = yield* Deferred.make<void>()
      const seen = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
      const consumer = yield* health.status.pipe(
        Stream.runForEach((status) =>
          Effect.gen(function*() {
            yield* Queue.offer(seen, status)
            if (status._tag === "Ready") yield* Deferred.await(release)
          })
        ),
        Effect.forkChild({ startImmediately: true })
      )
      assert.deepStrictEqual(yield* Queue.take(seen), { _tag: "Ready", pendingCommands: 0 })
      yield* Effect.scoped(Effect.gen(function*() {
        const restore = yield* health.restoring
        for (let processedBytes = 1; processedBytes <= 20; processedBytes++) {
          yield* restore.progress(processedBytes)
        }
        yield* Deferred.succeed(release, undefined)
        assert.deepStrictEqual(yield* Queue.take(seen), {
          _tag: "Restoring",
          processedBytes: 20
        })
      }))
      yield* Fiber.interrupt(consumer)
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

  it.effect("uses the configured sample interval", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      const seen = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
      yield* health.status.pipe(
        Stream.runForEach((status) => Queue.offer(seen, status)),
        Effect.forkChild
      )
      assert.deepStrictEqual(yield* Queue.take(seen), { _tag: "Ready", pendingCommands: 0 })
      yield* sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1
        WHERE singleton = 1`
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(yield* Queue.poll(seen), Option.none())
      yield* TestClock.adjust("4 seconds")
      assert.deepStrictEqual(yield* Queue.take(seen), {
        _tag: "ReadOnly",
        reason: "Another writer generation owns this replica"
      })
    }).pipe(
      Effect.provide(
        ReplicaHealth.layer(definition, { sampleInterval: "5 seconds" }).pipe(
          Layer.provideMerge(Gate)
        )
      ),
      Effect.provide(Database)
    ))

  it.effect("reports permanent storage loss as failed without failing the stream", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      yield* sql`DROP TABLE effect_local_commit_outbox`
      const failed = Option.getOrThrow(yield* Stream.runHead(health.status))
      assert.strictEqual(failed._tag, "Failed")
      // The reason has to name the table, not just restate the error tag, or an operator learns nothing.
      if (failed._tag === "Failed") {
        assert.include(failed.message, "no such table: effect_local_commit_outbox")
      }
    }).pipe(Effect.provide(Live), Effect.provide(Database)))

  it.effect("reports retryable storage loss as degraded and clears it after recovery", () =>
    Effect.gen(function*() {
      let failNext = false
      const Services = healthWithSql((text, statement) => {
        if (failNext && text.includes("SELECT COUNT(*) AS count")) {
          failNext = false
          return Effect.fail(
            new SqlError.SqlError({
              reason: new SqlError.LockTimeoutError({
                cause: nativeError("database is locked"),
                message: "database is locked"
              })
            })
          )
        }
        return statement
      })
      yield* Effect.gen(function*() {
        const health = yield* ReplicaHealth.ReplicaHealth
        const seen = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
        const observer = yield* health.status.pipe(
          Stream.runForEach((status) => Queue.offer(seen, status)),
          Effect.forkChild
        )
        assert.deepStrictEqual(yield* Queue.take(seen), { _tag: "Ready", pendingCommands: 0 })
        failNext = true
        yield* health.sample
        const degraded = yield* Queue.take(seen)
        assert.strictEqual(degraded._tag, "Degraded")
        if (degraded._tag === "Degraded") assert.include(degraded.reason, "database is locked")
        yield* health.sample
        assert.deepStrictEqual(yield* Queue.take(seen), { _tag: "Ready", pendingCommands: 0 })
        yield* Fiber.interrupt(observer)
      }).pipe(Effect.provide(Services))
    }))

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

  it.effect("reports a negative durable writer generation as failed", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_metadata SET writer_generation = -1 WHERE singleton = 1`
      const failed = Option.getOrThrow(yield* Stream.runHead(health.status))
      assert.strictEqual(failed._tag, "Failed")
      if (failed._tag === "Failed") {
        assert.include(failed.message, "writer_generation")
      }
    }).pipe(Effect.provide(Live), Effect.provide(Database)))

  it.effect("does not let an older concurrent sample overwrite a newer result", () =>
    Effect.gen(function*() {
      const staleRead = yield* Deferred.make<void>()
      const releaseStale = yield* Deferred.make<void>()
      let armed = false
      let blocked = false
      const Services = healthWithSql((text, statement) => {
        if (!armed || blocked || !text.includes("SELECT COUNT(*) AS count")) return statement
        blocked = true
        return statement.pipe(
          Effect.tap(() => Deferred.succeed(staleRead, undefined)),
          Effect.tap(() => Deferred.await(releaseStale))
        )
      })
      yield* Effect.gen(function*() {
        const health = yield* ReplicaHealth.ReplicaHealth
        const sql = yield* SqlClient.SqlClient
        const seen = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
        const observer = yield* health.status.pipe(
          Stream.runForEach((status) => Queue.offer(seen, status)),
          Effect.forkChild
        )
        assert.deepStrictEqual(yield* Queue.take(seen), { _tag: "Ready", pendingCommands: 0 })
        armed = true
        const stale = yield* health.sample.pipe(Effect.forkChild)
        yield* Deferred.await(staleRead)
        yield* sql`INSERT INTO effect_local_commit_outbox
          (commit_sequence, document_id, invalidation_keys, published)
          VALUES (
            9201,
            'doc_00000000-0000-4000-8000-000000000021',
            '["Task"]',
            0
          )`
        const fresh = yield* health.sample.pipe(Effect.forkChild)
        for (let index = 0; index < 10; index++) yield* Effect.yieldNow
        yield* Deferred.succeed(releaseStale, undefined)
        yield* Fiber.join(stale)
        yield* Fiber.join(fresh)
        assert.deepStrictEqual(
          yield* Queue.take(seen),
          { _tag: "Ready", pendingCommands: 1 }
        )
        assert.deepStrictEqual(
          yield* Queue.poll(seen),
          Option.none(),
          "the stale sample published pendingCommands = 0 after the newer result"
        )
        yield* Fiber.interrupt(observer)
      }).pipe(Effect.provide(Services))
    }))

  it.effect("does not hide a terminated background sampler on subscription", () =>
    Effect.gen(function*() {
      let failNext = false
      const Services = healthWithSql((text, statement) => {
        if (failNext && text.includes("SELECT COUNT(*) AS count")) {
          failNext = false
          return Effect.die(new Error("forced sampler defect"))
        }
        return statement
      })
      yield* Effect.gen(function*() {
        const health = yield* ReplicaHealth.ReplicaHealth
        const seen = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
        yield* health.status.pipe(
          Stream.runForEach((status) => Queue.offer(seen, status)),
          Effect.forkChild
        )
        assert.deepStrictEqual(yield* Queue.take(seen), { _tag: "Ready", pendingCommands: 0 })
        failNext = true
        yield* TestClock.adjust("1 second")
        assert.deepStrictEqual(yield* Queue.take(seen), {
          _tag: "Failed",
          message: "Replica health sampling stopped"
        })
        assert.strictEqual(
          Option.getOrThrow(yield* Stream.runHead(health.status))._tag,
          "Failed",
          "a one-shot subscription sample hid the permanently stopped poller"
        )
      }).pipe(Effect.provide(Services))
    }))

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
      }).pipe(
        Effect.provide(
          ReplicaHealth.layer(definition, ReplicaHealth.defaultOptions).pipe(Layer.provideMerge(SkewedGate))
        )
      )
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
      const ProjectedLive = ReplicaHealth.layer(projected, ReplicaHealth.defaultOptions).pipe(Layer.provideMerge(Gate))
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
  it.effect("never re-reports a restore after its installation ends", () =>
    Effect.gen(function*() {
      const health = yield* ReplicaHealth.ReplicaHealth
      const observed: Array<ReplicaStatus.ReplicaStatus> = []
      const subscribed = yield* Deferred.make<void>()
      const installing = yield* Deferred.make<void>()
      // Drains until the replica reports itself usable again after the install, so the whole restore
      // lifecycle is observed instead of whichever prefix the consumer happened to be scheduled for.
      const consumer = yield* health.status.pipe(
        Stream.tap((status) =>
          Effect.gen(function*() {
            yield* Effect.sync(() => observed.push(status))
            yield* Deferred.succeed(subscribed, undefined)
            if (status._tag === "ReadOnly") yield* Deferred.succeed(installing, undefined)
          })
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
        yield* Effect.scoped(Effect.gen(function*() {
          yield* reporter.installing
          yield* Deferred.await(installing)
        }))
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
