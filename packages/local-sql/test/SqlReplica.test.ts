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
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
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
    maxQueuedRpc: 128
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
})
