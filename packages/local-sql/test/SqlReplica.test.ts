import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as BackupStore from "../src/BackupStore.js"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
import * as PeerSession from "../src/PeerSession.js"
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
  type MetadataRow = {
    readonly commit_sequence: Identity.CommitSequence
    readonly definition_hash: string
    readonly replica_id: Identity.ReplicaId
    readonly replica_incarnation: Identity.ReplicaIncarnation
    readonly singleton: number
    readonly storage_format_version: number
    readonly writer_generation: Identity.WriterGeneration
  }
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
      const createCommandId = yield* Identity.makeCommandId
      const created = yield* replica.create(Task, { commandId: createCommandId, value: { title: "one" } })
      assert.strictEqual(created._tag, "DurablyCommittedLocal")
      if (created._tag !== "DurablyCommittedLocal") return
      const documentId = created.value
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
        CommandOutcome.durablyCommitted(mutationCommandId, undefined)
      )
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "two" })
      const noopCommandId = yield* Identity.makeCommandId
      assert.deepStrictEqual(
        yield* replica.mutate(Noop, { commandId: noopCommandId, documentId }),
        CommandOutcome.durablyCommitted(noopCommandId, undefined)
      )
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "two" })
      assert.deepStrictEqual(
        yield* replica.lookupMutation(Rename, mutationCommandId),
        CommandOutcome.durablyCommitted(mutationCommandId, undefined)
      )
      const deleteCommandId = yield* Identity.makeCommandId
      yield* replica.delete(Task, { commandId: deleteCommandId, documentId })
      assert.isTrue((yield* replica.get(Task, documentId)).tombstone)
      assert.deepStrictEqual(
        yield* replica.lookupDelete(Task, deleteCommandId),
        CommandOutcome.durablyCommitted(deleteCommandId, undefined)
      )
      const portableCreated = yield* replica.create(Task, {
        commandId: (yield* Identity.makeCommandId),
        value: { title: "portable" }
      })
      assert.strictEqual(portableCreated._tag, "DurablyCommittedLocal")
      if (portableCreated._tag !== "DurablyCommittedLocal") return
      const exported = yield* replica.exportDocument(Task, portableCreated.value)
      const importCommandId = yield* Identity.makeCommandId
      const imported = yield* replica.importDocument(Task, {
        commandId: importCommandId,
        value: exported
      })
      assert.strictEqual(imported._tag, "DurablyCommittedLocal")
      if (imported._tag !== "DurablyCommittedLocal") return
      const importedSnapshot = yield* replica.get(Task, imported.value)
      const sourceSnapshot = yield* replica.get(Task, portableCreated.value)
      assert.notStrictEqual(imported.value, portableCreated.value)
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

  it.effect("keeps missing metadata Mutate messages retryable", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const created = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "before" }
      })
      assert.strictEqual(created._tag, "DurablyCommittedLocal")
      if (created._tag !== "DurablyCommittedLocal") return
      const metadata = yield* sql<MetadataRow>`SELECT * FROM effect_local_metadata WHERE singleton = 1`
      assert.lengthOf(metadata, 1)
      const before = yield* sql<{
        readonly changes: number
        readonly commitSequence: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commitSequence`
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`
      yield* Effect.addFinalizer(() =>
        sql`INSERT OR IGNORE INTO effect_local_metadata ${sql.insert(metadata[0]!)}`.pipe(Effect.orDie)
      )

      const commandId = yield* Identity.makeCommandId
      const requestHash = yield* CommandExecutor.mutationRequestHash({
        incarnation: metadata[0]!.replica_incarnation,
        commandId,
        documentId: created.value,
        mutation: Rename,
        payload: "after"
      })
      const messageId = `EffectLocal/Document/${created.value}/Mutate/${
        metadata[0]!.replica_incarnation
      }:${commandId}:${requestHash}`
      const first = yield* replica.mutate(Rename, {
        commandId,
        documentId: created.value,
        payload: "after"
      }).pipe(
        Effect.result,
        Effect.timeoutOption("1 second"),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.addFinalizer(() =>
        Fiber.interrupt(first).pipe(
          Effect.andThen(Fiber.await(first)),
          Effect.asVoid
        )
      )
      let dispatched = false
      yield* Effect.gen(function*() {
        while (!first.pollUnsafe()) {
          const rows = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
            FROM effect_local_cluster_messages WHERE message_id = ${messageId}`
          if (rows[0]?.count === 1) {
            dispatched = true
            return
          }
          yield* sql`SELECT 1`
        }
      }).pipe(Effect.timeout("2 seconds"), TestClock.withLive)
      if (dispatched) yield* TestClock.adjust("1 second")
      const timed = yield* Fiber.join(first)
      assert.isTrue(Option.isSome(timed))
      if (!Option.isSome(timed)) return
      assert.isTrue(Result.isFailure(timed.value))
      if (!Result.isFailure(timed.value)) return
      assert.strictEqual(timed.value.failure._tag, "ReplicaError")
      if (timed.value.failure._tag !== "ReplicaError") return
      assert.strictEqual(timed.value.failure.reason._tag, "ReplicaMetadataMissing")
      assert.strictEqual(
        (yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_cluster_messages WHERE message_id = ${messageId}`)[0]?.count,
        0
      )

      yield* sql`INSERT INTO effect_local_metadata ${sql.insert(metadata[0]!)}`
      const retried = yield* replica.mutate(Rename, {
        commandId,
        documentId: created.value,
        payload: "after"
      }).pipe(Effect.timeout("5 seconds"), TestClock.withLive)
      assert.deepStrictEqual(retried, CommandOutcome.durablyCommitted(commandId, undefined))
      assert.strictEqual((yield* replica.get(Task, created.value)).value.title, "after")
      assert.strictEqual(
        (yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_command_receipts
          WHERE replica_incarnation = ${metadata[0]!.replica_incarnation}
            AND command_id = ${commandId}`)[0]?.count,
        1
      )
      const after = yield* sql<{
        readonly changes: number
        readonly commitSequence: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commitSequence`
      assert.deepStrictEqual(after[0], {
        changes: before[0]!.changes + 1,
        commitSequence: before[0]!.commitSequence + 1
      })
      const newest = yield* sql<{
        readonly commitSequence: number
        readonly documentId: Identity.DocumentId
      }>`SELECT commit_sequence AS commitSequence, document_id AS documentId
        FROM effect_local_commit_outbox ORDER BY commit_sequence DESC LIMIT 1`
      assert.deepStrictEqual(newest[0], {
        commitSequence: after[0]!.commitSequence,
        documentId: created.value
      })
    }).pipe(Effect.scoped, Effect.provide(Live), Effect.provide(Database)))

  it.effect("does not adopt a foreign writer during command preflight", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const created = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "before" }
      })
      assert.strictEqual(created._tag, "DurablyCommittedLocal")
      if (created._tag !== "DurablyCommittedLocal") return
      const local = yield* gate.current
      yield* sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1`
      yield* Effect.addFinalizer(() =>
        sql`UPDATE effect_local_metadata SET writer_generation = ${local.writerGeneration}
          WHERE singleton = 1`.pipe(Effect.orDie)
      )

      const attempt = (commandId: Identity.CommandId, payload: string) =>
        Effect.gen(function*() {
          const requestHash = yield* CommandExecutor.mutationRequestHash({
            incarnation: local.incarnation,
            commandId,
            documentId: created.value,
            mutation: Rename,
            payload
          })
          const messageId =
            `EffectLocal/Document/${created.value}/Mutate/${local.incarnation}:${commandId}:${requestHash}`
          const fiber = yield* replica.mutate(Rename, {
            commandId,
            documentId: created.value,
            payload
          }).pipe(
            Effect.result,
            Effect.timeoutOption("1 second"),
            Effect.forkChild({ startImmediately: true })
          )
          yield* Effect.addFinalizer(() =>
            Fiber.interrupt(fiber).pipe(
              Effect.andThen(Fiber.await(fiber)),
              Effect.asVoid
            )
          )
          let dispatched = false
          yield* Effect.gen(function*() {
            while (!fiber.pollUnsafe()) {
              const rows = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
                FROM effect_local_cluster_messages WHERE message_id = ${messageId}`
              if (rows[0]?.count === 1) {
                dispatched = true
                return
              }
              yield* sql`SELECT 1`
            }
          }).pipe(Effect.timeout("2 seconds"), TestClock.withLive)
          if (dispatched) yield* TestClock.adjust("1 second")
          return [yield* Fiber.join(fiber), messageId] as const
        })

      const [first, firstMessageId] = yield* attempt(yield* Identity.makeCommandId, "first")
      const [second, secondMessageId] = yield* attempt(yield* Identity.makeCommandId, "second")
      for (const result of [first, second]) {
        assert.isTrue(Option.isSome(result))
        if (!Option.isSome(result)) return
        assert.isTrue(Result.isFailure(result.value))
        if (!Result.isFailure(result.value)) return
        assert.strictEqual(result.value.failure._tag, "ReplicaError")
        if (result.value.failure._tag !== "ReplicaError") return
        assert.strictEqual(result.value.failure.reason._tag, "ReplicaFenced")
      }
      assert.deepStrictEqual(yield* gate.current, local)
      for (const messageId of [firstMessageId, secondMessageId]) {
        assert.strictEqual(
          (yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
            FROM effect_local_cluster_messages WHERE message_id = ${messageId}`)[0]?.count,
          0
        )
      }
    }).pipe(Effect.scoped, Effect.provide(Live), Effect.provide(Database)))

  it.effect("rejects missing replica metadata before dispatching inbound ApplySync", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const created = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "before" }
      })
      assert.strictEqual(created._tag, "DurablyCommittedLocal")
      if (created._tag !== "DurablyCommittedLocal") return
      const peerId = yield* Identity.makePeerId
      const inbound = yield* Queue.unbounded<Uint8Array>()
      yield* Effect.addFinalizer(() => Queue.shutdown(inbound))
      const transport = PeerTransport.PeerTransport.of({
        capabilities: { storeAndForward: false },
        connect: () =>
          Effect.succeed({
            peerId,
            capabilities: { storeAndForward: false },
            receive: Stream.fromQueue(inbound),
            send: () => Effect.void,
            close: Effect.void
          })
      })
      const session = yield* PeerSession.makeSupervised({
        peerId,
        documents: [{ document: Task, documentId: created.value }]
      }).pipe(
        Effect.provideService(PeerTransport.PeerTransport, transport),
        Effect.provideService(ReplicaLimits.ReplicaLimits, limits)
      )
      const disconnected = yield* Effect.result(session.awaitDisconnect).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.addFinalizer(() =>
        Fiber.interrupt(disconnected).pipe(
          Effect.andThen(Fiber.await(disconnected)),
          Effect.asVoid
        )
      )
      const metadata = yield* sql<MetadataRow>`SELECT * FROM effect_local_metadata WHERE singleton = 1`
      assert.lengthOf(metadata, 1)
      const before = yield* sql<{ readonly sequence: number }>`SELECT
        COALESCE(MAX(commit_sequence), 0) AS sequence FROM effect_local_commit_outbox`
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`
      yield* Effect.addFinalizer(() =>
        sql`INSERT OR IGNORE INTO effect_local_metadata ${sql.insert(metadata[0]!)}`.pipe(Effect.orDie)
      )
      const message = Uint8Array.of(1)
      const messageHash = yield* Canonical.digest(message)
      const encoded = yield* Schema.encodeEffect(
        Schema.fromJsonString(Schema.toCodecJson(PeerSession.SyncEnvelope))
      )({
        connectionEpoch: "remote-epoch",
        sequence: 0,
        documentId: created.value,
        documentType: Task.name,
        messageHash,
        message,
        lineage: Identity.genesisLineage,
        writerProvenance: []
      }).pipe(Effect.map((value) => new TextEncoder().encode(value)))
      yield* Queue.offer(inbound, encoded)

      const timed = yield* Fiber.join(disconnected).pipe(
        Effect.timeoutOption("2 seconds"),
        TestClock.withLive
      )
      assert.isTrue(Option.isSome(timed))
      if (!Option.isSome(timed)) return
      assert.isTrue(Result.isFailure(timed.value))
      if (!Result.isFailure(timed.value)) return
      assert.strictEqual(timed.value.failure._tag, "ReplicaError")
      assert.strictEqual(timed.value.failure.reason._tag, "ReplicaMetadataMissing")
      assert.strictEqual(
        (yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_cluster_messages
          WHERE message_id LIKE ${`EffectLocal/Document/${created.value}/ApplySync/%`}`)[0]?.count,
        0
      )
      assert.deepStrictEqual(
        yield* sql<{ readonly sequence: number }>`SELECT
          COALESCE(MAX(commit_sequence), 0) AS sequence FROM effect_local_commit_outbox`,
        before
      )
    }).pipe(Effect.scoped, Effect.provide(Live), Effect.provide(Database)))

  it.effect("provides nonempty projection bindings", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const created = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "projected" }
      })
      assert.strictEqual(created._tag, "DurablyCommittedLocal")
      if (created._tag !== "DurablyCommittedLocal") return
      const sql = yield* SqlClient.SqlClient
      const rows = yield* sql<{ readonly sourceDocumentId: string; readonly title: string }>`SELECT
        source_document_id AS sourceDocumentId,
        title
      FROM task_title_v1`
      assert.deepStrictEqual(rows, [{ sourceDocumentId: created.value, title: "projected" }])
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
      assert.strictEqual(created._tag, "DurablyCommittedLocal")
      if (created._tag !== "DurablyCommittedLocal") return
      const documentId = created.value
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
      assert.strictEqual(created._tag, "DurablyCommittedLocal")
      if (created._tag !== "DurablyCommittedLocal") return
      const exported = yield* replica.exportDocument(Task, created.value)
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
      const backups = BackupStore.layer(definition).pipe(Layer.provideMerge(publisher))
      const direct = SqlReplica.layerFromServices(definition).pipe(Layer.provideMerge(backups))
      const services = Layer.merge(direct, Reactivity.layer).pipe(Layer.provide(Handler))

      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const created = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "before" }
        })
        assert.strictEqual(created._tag, "DurablyCommittedLocal")
        if (created._tag !== "DurablyCommittedLocal") return
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

        const queued = yield* Effect.forkChild(replica.get(Task, created.value))
        yield* Effect.yieldNow

        const shed = yield* Effect.flip(replica.get(Task, created.value))
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
      const backups = BackupStore.layer(definition).pipe(Layer.provideMerge(publisher))
      const direct = SqlReplica.layerFromServices(definition).pipe(Layer.provideMerge(backups))
      const services = Layer.merge(direct, Reactivity.layer).pipe(Layer.provide(Handler))

      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const reactivity = yield* Reactivity.Reactivity
        const created = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "before" }
        })
        assert.strictEqual(created._tag, "DurablyCommittedLocal")
        if (created._tag !== "DurablyCommittedLocal") return
        const backup = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        yield* replica.mutate(Rename, {
          commandId: yield* Identity.makeCommandId,
          documentId: created.value,
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

        assert.strictEqual((yield* replica.get(Task, created.value)).value.title, "before")
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
    const backups = BackupStore.layer(definition).pipe(Layer.provideMerge(publisher))
    const direct = SqlReplica.layerFromServices(definition).pipe(Layer.provideMerge(backups))
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
