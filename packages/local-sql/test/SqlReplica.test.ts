import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as BackupStore from "../src/BackupStore.js"
import * as CommandDeliveryPublisher from "../src/CommandDeliveryPublisher.js"
import * as CommandDeliveryStore from "../src/CommandDeliveryStore.js"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
import * as PeerRelayClientRuntime from "../src/PeerRelayClientRuntime.js"
import * as PeerRelayOutboxLimits from "../src/PeerRelayOutboxLimits.js"
import * as PeerRelayReceiptLimits from "../src/PeerRelayReceiptLimits.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as QueryExecutor from "../src/QueryExecutor.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as ReplicaHealth from "../src/ReplicaHealth.js"
import * as ReplicaWorkflow from "../src/ReplicaWorkflow.js"
import * as SqlProjection from "../src/SqlProjection.js"
import * as SqlReplica from "../src/SqlReplica.js"

describe("SqlReplica", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const Rename = Mutation.make("Rename", { document: Task, payload: Schema.String })
  const Noop = Mutation.make("Noop", { document: Task })
  const TaskTitle = Projection.make("TaskTitle", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, title: Schema.String }),
    key: (row) => row.sourceDocumentId,
    project: (snapshot) =>
      snapshot.tombstone
        ? []
        : [{ sourceDocumentId: snapshot.documentId, title: snapshot.value.title }]
  })
  const TaskTitleSql = SqlProjection.make(TaskTitle, {
    table: "task_title_v1",
    migrations: [{
      id: 1,
      name: "task_title_v1",
      run: (sql, table) =>
        sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
          source_document_id TEXT PRIMARY KEY,
          title TEXT NOT NULL
        )`.pipe(Effect.asVoid)
    }],
    deleteByDocument: (sql, table, documentId) =>
      sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
    insert: (sql, table, row) =>
      sql`INSERT INTO ${sql(table)} (source_document_id, title)
        VALUES (${row.sourceDocumentId}, ${row.title})`.pipe(Effect.asVoid)
  })
  const definition = ReplicaDefinition.make({
    name: "sql-replica",
    documents: DocumentSet.make(Task),
    mutations: [Rename, Noop],
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
    maxQueuedPermits: 128,
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
  const Handler = Layer.merge(
    Rename.toLayer(({ draft, payload }) => {
      draft.title = payload
      return undefined
    }),
    Noop.toLayer(() => undefined)
  )
  const Limits = ReplicaLimits.layer(limits)
  const Live = SqlReplica.layerWithBindings(definition, { projections: [] }).pipe(
    Layer.provide(Layer.mergeAll(Database, Handler, Limits))
  )
  const SlowHealthLive = SqlReplica.layerWithBindings(definition, {
    health: { sampleInterval: "5 seconds" },
    projections: []
  }).pipe(
    Layer.provide(Layer.mergeAll(Database, Handler, Limits))
  )
  const projectedDefinition = ReplicaDefinition.make({
    name: "projected-sql-replica",
    documents: DocumentSet.make(Task),
    mutations: [Rename, Noop],
    projections: [TaskTitle],
    queries: []
  })
  const ProjectedLive = SqlReplica.layerWithBindings(
    projectedDefinition,
    { projections: [TaskTitleSql] }
  ).pipe(
    Layer.provide(Layer.mergeAll(Database, Handler, Limits))
  )

  it("rejects duplicate bindings for one projection", () => {
    assert.throws(
      () => SqlReplica.layerWithBindings(projectedDefinition, { projections: [TaskTitleSql, TaskTitleSql] }),
      /exactly one SQL binding/
    )
  })

  it("rejects duplicate bindings for one projection through the relay constructor too", () => {
    assert.throws(
      () => SqlReplica.layerRelayWithBindings(projectedDefinition, { projections: [TaskTitleSql, TaskTitleSql] }),
      /exactly one SQL binding/
    )
  })

  // The reason `layerRelayWithBindings` exists. A deployment with projections that reaches for the
  // only bindings constructor there was got a direct `PeerSync`, and found out at the point it
  // built a service it did not think it was choosing.
  for (
    const flavour of [
      {
        name: "direct",
        build: () => SqlReplica.layerWithBindings(projectedDefinition, { projections: [TaskTitleSql] }),
        relayRuntimeBuilds: false
      },
      {
        name: "relay",
        build: () => SqlReplica.layerRelayWithBindings(projectedDefinition, { projections: [TaskTitleSql] }),
        relayRuntimeBuilds: true
      }
    ]
  ) {
    it.effect(`${flavour.name} bindings constructor: relay client runtime builds = ${flavour.relayRuntimeBuilds}`, () =>
      Effect.gen(function*() {
        const exit = yield* Effect.exit(Effect.scoped(Layer.build(
          PeerRelayClientRuntime.layerSql.pipe(
            Layer.provide(flavour.build()),
            Layer.provide(Layer.mergeAll(
              Database,
              Handler,
              Limits,
              PeerRelayReceiptLimits.layer(PeerRelayReceiptLimits.defaults),
              PeerRelayOutboxLimits.layer(PeerRelayOutboxLimits.defaults)
            ))
          )
        )))
        if (flavour.relayRuntimeBuilds) {
          assert.strictEqual(exit._tag, "Success")
          return
        }
        // Named rather than merely "it failed": the direct stack has to be rejected for being the
        // wrong `PeerSync`, not for some unrelated wiring fault that would mask a real regression.
        if (!Exit.isFailure(exit)) return assert.fail("the direct stack must not build a relay runtime")
        const failure = Cause.findErrorOption(exit.cause)
        if (Option.isNone(failure)) return assert.fail("expected a typed failure")
        const error = failure.value
        if (error._tag !== "ReplicaError") return assert.fail(`expected a ReplicaError, got ${error._tag}`)
        const reason = error.reason
        if (reason._tag !== "ProtocolMismatch") {
          return assert.fail(`expected a ProtocolMismatch, got ${reason._tag}`)
        }
        assert.strictEqual(reason.expected, "relay enabled PeerSync")
      }))
  }

  it.effect("forwards the configured health sampling interval", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const seen = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
      yield* replica.status.pipe(
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
    }).pipe(Effect.provide(SlowHealthLive), Effect.provide(Database)))

  it.effect("creates, reads, mutates, tombstones, and resolves receipts", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      assert.ok(yield* ReplicaWorkflow.CompactionWorkflow)
      const unknownCommandId = yield* Identity.makeCommandId
      assert.strictEqual(
        (yield* replica.lookupCommandDelivery(unknownCommandId))._tag,
        "UnknownCommand"
      )
      const createCommandId = yield* Identity.makeCommandId
      const created = yield* replica.create(Task, { commandId: createCommandId, value: { title: "one" } })
      const documentId = created
      const createdDelivery = yield* replica.lookupCommandDelivery(createCommandId)
      assert.strictEqual(createdDelivery._tag, "TrackedCommand")
      if (createdDelivery._tag === "TrackedCommand") {
        assert.isAbove(createdDelivery.localChangeCount, 0)
        assert.deepStrictEqual(createdDelivery.destinations, [])
      }
      assert.deepStrictEqual(
        yield* replica.create(Task, { commandId: createCommandId, value: { title: "one" } }),
        created
      )
      assert.strictEqual(
        (yield* Effect.flip(replica.create(Task, { commandId: createCommandId, value: { title: "different" } })))
          .reason._tag,
        "CommandIdConflict"
      )
      const concurrentCommandId = yield* Identity.makeCommandId
      const concurrent = yield* Effect.all([
        replica.create(Task, { commandId: concurrentCommandId, value: { title: "parallel" } }),
        replica.create(Task, { commandId: concurrentCommandId, value: { title: "parallel" } })
      ], { concurrency: "unbounded" })
      assert.deepStrictEqual(concurrent[0], concurrent[1])
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "one" })
      const mutationCommandId = yield* Identity.makeCommandId
      assert.deepStrictEqual(
        yield* replica.mutate(Rename, { commandId: mutationCommandId, documentId, payload: "two" }),
        undefined
      )
      const mutationDelivery = yield* replica.lookupCommandDelivery(mutationCommandId)
      assert.strictEqual(mutationDelivery._tag, "TrackedCommand")
      if (mutationDelivery._tag === "TrackedCommand") {
        assert.isAbove(mutationDelivery.localChangeCount, 0)
      }
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "two" })
      const noopCommandId = yield* Identity.makeCommandId
      assert.deepStrictEqual(
        yield* replica.mutate(Noop, { commandId: noopCommandId, documentId }),
        undefined
      )
      assert.strictEqual(
        (yield* replica.lookupCommandDelivery(noopCommandId))._tag,
        "NoChangesToDeliver"
      )
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "two" })
      assert.deepStrictEqual(
        yield* replica.lookupMutation(Rename, mutationCommandId),
        CommandOutcome.durablyCommitted(mutationCommandId, undefined)
      )
      const deleteCommandId = yield* Identity.makeCommandId
      yield* replica.delete(Task, { commandId: deleteCommandId, documentId })
      const deleteDelivery = yield* replica.lookupCommandDelivery(deleteCommandId)
      assert.strictEqual(deleteDelivery._tag, "TrackedCommand")
      if (deleteDelivery._tag === "TrackedCommand") {
        assert.isAbove(deleteDelivery.localChangeCount, 0)
      }
      assert.isTrue((yield* replica.get(Task, documentId)).tombstone)
      assert.deepStrictEqual(
        yield* replica.lookupDelete(Task, deleteCommandId),
        CommandOutcome.durablyCommitted(deleteCommandId, undefined)
      )
      const portableCreated = yield* replica.create(Task, {
        commandId: (yield* Identity.makeCommandId),
        value: { title: "portable" }
      })
      const exported = yield* replica.exportDocument(Task, portableCreated)
      const importCommandId = yield* Identity.makeCommandId
      const imported = yield* replica.importDocument(Task, {
        commandId: importCommandId,
        value: exported
      })
      const importedSnapshot = yield* replica.get(Task, imported)
      const sourceSnapshot = yield* replica.get(Task, portableCreated)
      assert.notStrictEqual(imported, portableCreated)
      assert.deepStrictEqual(importedSnapshot.value, { title: "portable" })
      assert.notStrictEqual(importedSnapshot.heads[0], sourceSnapshot.heads[0])
      assert.deepStrictEqual(
        yield* replica.importDocument(Task, { commandId: importCommandId, value: exported }),
        imported
      )
      const sql = yield* SqlClient.SqlClient
      const rows = yield* sql<{
        readonly changes: number
        readonly clusterMessages: number
        readonly documents: number
        readonly receipts: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM ${sql(`${ClusterStorage.messagePrefix}_messages`)}) AS clusterMessages,
        (SELECT COUNT(*) FROM effect_local_documents) AS documents,
        (SELECT COUNT(*) FROM effect_local_command_receipts) AS receipts`
      assert.deepStrictEqual(rows[0], { changes: 6, clusterMessages: 8, documents: 4, receipts: 7 })
    }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive))

  it.effect("rolls a command back when delivery source persistence fails", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const commandId = yield* Identity.makeCommandId
      yield* sql`CREATE TRIGGER fail_command_delivery_source
        BEFORE INSERT ON effect_local_command_delivery_sources
        BEGIN
          SELECT RAISE(ABORT, 'forced command delivery source failure');
        END`

      const result = yield* Effect.exit(
        replica.create(Task, { commandId, value: { title: "must roll back" } })
      )
      assert.strictEqual(result._tag, "Failure")
      assert.strictEqual(
        (yield* replica.lookupCommandDelivery(commandId))._tag,
        "UnknownCommand"
      )
      const rows = yield* sql`SELECT
        (SELECT COUNT(*) FROM effect_local_documents) AS documents,
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_command_receipts) AS receipts,
        (SELECT COUNT(*) FROM effect_local_command_delivery_sources) AS sources,
        (SELECT COUNT(*) FROM effect_local_command_delivery_changes) AS delivery_changes,
        (SELECT COUNT(*) FROM effect_local_command_delivery_events) AS delivery_events`
      assert.deepStrictEqual(rows, [{
        documents: 0,
        changes: 0,
        receipts: 0,
        sources: 0,
        delivery_changes: 0,
        delivery_events: 0
      }])
    }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive))

  it.effect("refreshes command delivery subscribers after restore clears custody evidence", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const commandId = yield* Identity.makeCommandId
        yield* replica.create(Task, { commandId, value: { title: "backed up" } })
        const backup = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        const pull = yield* replica.commandDeliveryChanges(commandId).pipe(Stream.toPull)

        const before = yield* pull
        assert.strictEqual(before[0]?._tag, "TrackedCommand")

        yield* replica.restoreBackup({
          expectedDefinitionHash: definition.hash,
          installationId: yield* Identity.makeBackupInstallationId,
          maxBytes: limits.maxBackupBytes,
          mode: "replace",
          source: Stream.fromIterable(backup)
        })

        const after = yield* pull
        assert.strictEqual(after[0]?._tag, "UnknownCommand")
      }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive)
    ))

  it.effect("provides nonempty projection bindings", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const created = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "projected" }
      })
      const sql = yield* SqlClient.SqlClient
      const rows = yield* sql<{ readonly sourceDocumentId: string; readonly title: string }>`SELECT
        source_document_id AS sourceDocumentId,
        title
      FROM task_title_v1`
      assert.deepStrictEqual(rows, [{ sourceDocumentId: created, title: "projected" }])
    }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive))

  // Deleting a document replaces its projection rows with none, driven by the
  // tombstone the commit reports. Nothing else in the suite exercises a delete
  // against a replica that actually has projection bindings.
  it.effect("clears projection rows when a projected document is deleted", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const created = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "projected" }
      })
      const documentId = created
      const projected = () =>
        sql<{ readonly sourceDocumentId: string; readonly title: string }>`SELECT
          source_document_id AS sourceDocumentId, title FROM task_title_v1`
      assert.deepStrictEqual(yield* projected(), [{ sourceDocumentId: documentId, title: "projected" }])

      yield* replica.delete(Task, { commandId: yield* Identity.makeCommandId, documentId })

      assert.deepStrictEqual(yield* projected(), [])
      assert.isTrue((yield* replica.get(Task, documentId)).tombstone)
      const keys = yield* sql<{ readonly invalidation_keys: string }>`
        SELECT invalidation_keys FROM effect_local_commit_outbox ORDER BY commit_sequence DESC LIMIT 1
      `
      assert.deepStrictEqual(JSON.parse(keys[0]!.invalidation_keys), ["Task", "TaskTitle"])
    }).pipe(Effect.provide(ProjectedLive), Effect.provide(Database), TestClock.withLive))

  it.effect("rejects importing a document whose portable definition does not match", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const created = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "source" }
      })
      const exported = yield* replica.exportDocument(Task, created)
      const wrongName = yield* Effect.flip(replica.importDocument(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { ...exported, documentName: "Other" }
      }))
      assert.strictEqual(wrongName.reason._tag, "BackupInvalid")
      const wrongVersion = yield* Effect.flip(replica.importDocument(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { ...exported, schemaVersion: 999 }
      }))
      assert.strictEqual(wrongVersion.reason._tag, "BackupInvalid")
    }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive))

  it.effect("sheds replica operations while a restore holds the writer", () =>
    Effect.gen(function*() {
      const committed = yield* Deferred.make<void>()
      const ingesting = yield* Deferred.make<void>()
      const continueIngest = yield* Deferred.make<void>()
      const release = yield* Latch.make()
      let armed = false
      const baseDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
      const instrumentedDatabase = Layer.effect(
        SqlClient.SqlClient,
        Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          return Object.assign(
            ((...args: ReadonlyArray<unknown>) => (sql as any)(...args)) as SqlClient.SqlClient,
            sql,
            {
              withTransaction: <R, E, A,>(effect: Effect.Effect<A, E, R>) =>
                Effect.serviceOption(sql.transactionService).pipe(
                  Effect.flatMap((transaction) =>
                    sql.withTransaction(effect).pipe(
                      Effect.tap(() =>
                        armed && Option.isNone(transaction)
                          ? Deferred.succeed(committed, undefined).pipe(Effect.andThen(release.await))
                          : Effect.void
                      )
                    )
                  )
                )
            }
          )
        })
      ).pipe(Layer.provideMerge(baseDatabase))
      const database = Layer.merge(instrumentedDatabase, NodeCrypto.layer)
      const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provideMerge(database))
      const infrastructure = Layer.merge(bootstrap, ReplicaLimits.layer({ ...limits, maxQueuedPermits: 1 }))
      const gate = ReplicaGate.layer.pipe(Layer.provideMerge(infrastructure))
      const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
      const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
      const projections = ProjectionStore.layer([]).pipe(Layer.provideMerge(store))
      const health = ReplicaHealth.layer(definition, ReplicaHealth.defaultOptions).pipe(Layer.provideMerge(projections))
      const commands = CommandExecutor.layer(definition).pipe(Layer.provideMerge(health))
      const queries = QueryExecutor.layer(definition).pipe(
        Layer.provideMerge(Layer.merge(commands, Reactivity.layer))
      )
      const publisher = CommitPublisher.layer.pipe(Layer.provideMerge(queries))
      const deliveryStore = CommandDeliveryStore.layer.pipe(Layer.provideMerge(gate))
      const deliveryPublisher = CommandDeliveryPublisher.layer(CommandDeliveryPublisher.defaultOptions).pipe(
        Layer.provideMerge(deliveryStore)
      )
      const backups = BackupStore.layer(definition).pipe(
        Layer.provideMerge(Layer.merge(publisher, deliveryPublisher))
      )
      const direct = SqlReplica.layerFromServices(definition).pipe(
        Layer.provideMerge(Layer.mergeAll(backups, deliveryStore, deliveryPublisher))
      )
      const services = Layer.merge(direct, Reactivity.layer).pipe(Layer.provide(Handler))

      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const created = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "before" }
        })
        const backup = Array.from(
          yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        )
        assert.isAtLeast(backup.length, 2)
        armed = true
        const restore = yield* replica.restoreBackup({
          expectedDefinitionHash: definition.hash,
          installationId: yield* Identity.makeBackupInstallationId,
          maxBytes: limits.maxBackupBytes,
          mode: "replace",
          source: Stream.fromIterable(backup).pipe(
            Stream.mapEffect((chunk, index) =>
              index === 1
                ? Deferred.succeed(ingesting, undefined).pipe(
                  Effect.andThen(Deferred.await(continueIngest)),
                  Effect.as(chunk)
                )
                : Effect.succeed(chunk)
            )
          )
        }).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(ingesting)
        assert.deepStrictEqual(
          yield* Stream.runHead(replica.status),
          Option.some<ReplicaStatus.ReplicaStatus>({
            _tag: "Restoring",
            processedBytes: backup[0]!.byteLength
          })
        )
        yield* Deferred.succeed(continueIngest, undefined)
        yield* Deferred.await(committed)
        armed = false
        assert.deepStrictEqual(
          yield* Stream.runHead(replica.status),
          Option.some<ReplicaStatus.ReplicaStatus>({
            _tag: "ReadOnly",
            reason: "A backup restore is installing"
          })
        )

        const queued = yield* Effect.forkChild(replica.get(Task, created))
        yield* Effect.yieldNow

        const shed = yield* Effect.flip(replica.get(Task, created))
        assert.strictEqual(shed.reason._tag, "QuotaExceeded")
        if (shed.reason._tag === "QuotaExceeded") {
          assert.strictEqual(shed.reason.resource, "queued permits")
          assert.strictEqual(shed.reason.limit, 1)
        }

        yield* release.open
        yield* Fiber.join(restore)
        assert.deepStrictEqual(
          yield* Stream.runHead(replica.status),
          Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 0 })
        )
        assert.strictEqual((yield* Fiber.join(queued)).value.title, "before")
      }).pipe(Effect.scoped, Effect.provide(services))
    }).pipe(TestClock.withLive))

  it.effect("invalidates reactive consumers when interruption arrives after a restore commits", () =>
    Effect.gen(function*() {
      const committed = yield* Deferred.make<void>()
      const release = yield* Latch.make()
      let armed = false
      const baseDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
      const instrumentedDatabase = Layer.effect(
        SqlClient.SqlClient,
        Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          const instrumented = Object.assign(
            ((...args: ReadonlyArray<unknown>) => (sql as any)(...args)) as SqlClient.SqlClient,
            sql,
            {
              withTransaction: <R, E, A,>(effect: Effect.Effect<A, E, R>) =>
                Effect.serviceOption(sql.transactionService).pipe(
                  Effect.flatMap((transaction) =>
                    sql.withTransaction(effect).pipe(
                      Effect.tap(() =>
                        armed && Option.isNone(transaction)
                          ? Deferred.succeed(committed, undefined).pipe(Effect.andThen(release.await))
                          : Effect.void
                      )
                    )
                  )
                )
            }
          )
          return instrumented
        })
      ).pipe(Layer.provideMerge(baseDatabase))
      const database = Layer.merge(instrumentedDatabase, NodeCrypto.layer)
      const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provideMerge(database))
      const infrastructure = Layer.merge(bootstrap, Limits)
      const gate = ReplicaGate.layer.pipe(Layer.provideMerge(infrastructure))
      const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
      const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
      const projections = ProjectionStore.layer([]).pipe(Layer.provideMerge(store))
      const health = ReplicaHealth.layer(definition, ReplicaHealth.defaultOptions).pipe(Layer.provideMerge(projections))
      const commands = CommandExecutor.layer(definition).pipe(Layer.provideMerge(health))
      const queries = QueryExecutor.layer(definition).pipe(
        Layer.provideMerge(Layer.merge(commands, Reactivity.layer))
      )
      const publisher = CommitPublisher.layer.pipe(Layer.provideMerge(queries))
      const deliveryStore = CommandDeliveryStore.layer.pipe(Layer.provideMerge(gate))
      const deliveryPublisher = CommandDeliveryPublisher.layer(CommandDeliveryPublisher.defaultOptions).pipe(
        Layer.provideMerge(deliveryStore)
      )
      const backups = BackupStore.layer(definition).pipe(
        Layer.provideMerge(Layer.merge(publisher, deliveryPublisher))
      )
      const direct = SqlReplica.layerFromServices(definition).pipe(
        Layer.provideMerge(Layer.mergeAll(backups, deliveryStore, deliveryPublisher))
      )
      const services = Layer.merge(direct, Reactivity.layer).pipe(Layer.provide(Handler))

      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const reactivity = yield* Reactivity.Reactivity
        const created = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "before" }
        })
        const backup = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        yield* replica.mutate(Rename, {
          commandId: yield* Identity.makeCommandId,
          documentId: created,
          payload: "after"
        })
        let invalidated = false
        const cancel = reactivity.registerUnsafe([Task.name], () => {
          invalidated = true
        })
        yield* Effect.addFinalizer(() => Effect.sync(cancel))
        armed = true
        const restore = yield* replica.restoreBackup({
          expectedDefinitionHash: definition.hash,
          installationId: yield* Identity.makeBackupInstallationId,
          maxBytes: limits.maxBackupBytes,
          mode: "replace",
          source: Stream.fromIterable(backup)
        }).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(committed)
        const interrupt = yield* Fiber.interrupt(restore).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Effect.yieldNow
        yield* release.open
        yield* Fiber.join(interrupt)

        assert.strictEqual((yield* replica.get(Task, created)).value.title, "before")
        assert.isTrue(invalidated)
      }).pipe(Effect.scoped, Effect.provide(services))
    }))
  const statusServices = Effect.gen(function*() {
    const database = Layer.merge(
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer
    )
    const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provideMerge(database))
    const infrastructure = Layer.merge(bootstrap, Limits)
    const gate = ReplicaGate.layer.pipe(Layer.provideMerge(infrastructure))
    const recovery = Recovery.layer.pipe(Layer.provideMerge(gate))
    const store = DocumentStore.layer.pipe(Layer.provideMerge(recovery))
    const projections = ProjectionStore.layer([]).pipe(Layer.provideMerge(store))
    const health = ReplicaHealth.layer(definition, ReplicaHealth.defaultOptions).pipe(Layer.provideMerge(projections))
    const commands = CommandExecutor.layer(definition).pipe(Layer.provideMerge(health))
    const queries = QueryExecutor.layer(definition).pipe(
      Layer.provideMerge(Layer.merge(commands, Reactivity.layer))
    )
    const publisher = CommitPublisher.layer.pipe(Layer.provideMerge(queries))
    const deliveryStore = CommandDeliveryStore.layer.pipe(Layer.provideMerge(gate))
    const deliveryPublisher = CommandDeliveryPublisher.layer(CommandDeliveryPublisher.defaultOptions).pipe(
      Layer.provideMerge(deliveryStore)
    )
    const backups = BackupStore.layer(definition).pipe(
      Layer.provideMerge(Layer.merge(publisher, deliveryPublisher))
    )
    const direct = SqlReplica.layerFromServices(definition).pipe(
      Layer.provideMerge(Layer.mergeAll(backups, deliveryStore, deliveryPublisher))
    )
    return Layer.merge(direct, Reactivity.layer).pipe(Layer.provide(Handler))
  })

  it.effect("reports unpublished commits as pending commands", () =>
    Effect.gen(function*() {
      const services = yield* statusServices
      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const sql = yield* SqlClient.SqlClient
        yield* sql`INSERT INTO effect_local_commit_outbox
          (commit_sequence, document_id, invalidation_keys, published)
          VALUES (9001, 'doc_00000000-0000-4000-8000-000000000001', '["Task"]', 0)`
        yield* sql`INSERT INTO effect_local_commit_outbox
          (commit_sequence, document_id, invalidation_keys, published)
          VALUES (9002, 'doc_00000000-0000-4000-8000-000000000002', '["Task"]', 0)`
        assert.deepStrictEqual(
          yield* Stream.runHead(replica.status),
          Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 2 })
        )
        yield* replica.flush
        assert.deepStrictEqual(
          yield* Stream.runHead(replica.status),
          Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 0 })
        )
      }).pipe(Effect.scoped, Effect.provide(services))
    }))

  it.effect("keeps the status stream open", () =>
    Effect.gen(function*() {
      const services = yield* statusServices
      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const ended = yield* Deferred.make<void>()
        const consumer = yield* replica.status.pipe(
          Stream.runDrain,
          Effect.andThen(Deferred.succeed(ended, undefined)),
          Effect.forkChild
        )
        yield* Effect.yieldNow
        assert.isTrue(Option.isNone(yield* Deferred.poll(ended)))
        yield* Fiber.interrupt(consumer)
      }).pipe(Effect.scoped, Effect.provide(services))
    }))
  it.effect("reports pending commands through the sharded replica layer", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      yield* sql`INSERT INTO effect_local_commit_outbox
        (commit_sequence, document_id, invalidation_keys, published)
        VALUES (9101, 'doc_00000000-0000-4000-8000-000000000011', '["Task"]', 0)`
      yield* sql`INSERT INTO effect_local_commit_outbox
        (commit_sequence, document_id, invalidation_keys, published)
        VALUES (9102, 'doc_00000000-0000-4000-8000-000000000012', '["Task"]', 0)`
      assert.deepStrictEqual(
        yield* Stream.runHead(replica.status),
        Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 2 })
      )
      yield* replica.flush
      assert.deepStrictEqual(
        yield* Stream.runHead(replica.status),
        Option.some<ReplicaStatus.ReplicaStatus>({ _tag: "Ready", pendingCommands: 0 })
      )
    }).pipe(Effect.provide(Live), Effect.provide(Database), TestClock.withLive))
})
