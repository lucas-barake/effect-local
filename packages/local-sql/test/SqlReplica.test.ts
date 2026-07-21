import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ClusterStorage from "../src/internal/clusterStorage.js"
import * as ReplicaWorkflow from "../src/ReplicaWorkflow.js"
import * as SqlReplica from "../src/SqlReplica.js"

describe("SqlReplica", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const Rename = Mutation.make("Rename", { document: Task, payload: Schema.String })
  const Noop = Mutation.make("Noop", { document: Task })
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
  const Database = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
  const Handler = Layer.merge(
    Mutation.layer(Rename, ({ draft, payload }) => {
      draft.title = payload
      return undefined
    }),
    Mutation.layer(Noop, () => undefined)
  )
  const Limits = ReplicaLimits.layer(limits)
  const Live = SqlReplica.layer(definition, { projections: [] }).pipe(
    Layer.provide(Layer.mergeAll(Database, Handler, Limits))
  )

  it.effect("creates, reads, mutates, tombstones, and resolves receipts", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      assert.ok(yield* ReplicaWorkflow.WorkflowRuntime)
      const createCommandId = Identity.makeCommandId()
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
      const concurrentCommandId = Identity.makeCommandId()
      const concurrent = yield* Effect.all([
        replica.create(Task, { commandId: concurrentCommandId, value: { title: "parallel" } }),
        replica.create(Task, { commandId: concurrentCommandId, value: { title: "parallel" } })
      ], { concurrency: "unbounded" })
      assert.deepStrictEqual(concurrent[0], concurrent[1])
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "one" })
      const mutationCommandId = Identity.makeCommandId()
      assert.deepStrictEqual(
        yield* replica.mutate(Rename, { commandId: mutationCommandId, documentId, payload: "two" }),
        CommandOutcome.durablyCommitted(mutationCommandId, undefined)
      )
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "two" })
      const noopCommandId = Identity.makeCommandId()
      assert.deepStrictEqual(
        yield* replica.mutate(Noop, { commandId: noopCommandId, documentId }),
        CommandOutcome.durablyCommitted(noopCommandId, undefined)
      )
      assert.deepStrictEqual((yield* replica.get(Task, documentId)).value, { title: "two" })
      assert.deepStrictEqual(
        yield* replica.lookupMutation(Rename, mutationCommandId),
        CommandOutcome.durablyCommitted(mutationCommandId, undefined)
      )
      const deleteCommandId = Identity.makeCommandId()
      yield* replica.delete(Task, { commandId: deleteCommandId, documentId })
      assert.isTrue((yield* replica.get(Task, documentId)).tombstone)
      assert.deepStrictEqual(
        yield* replica.lookupDelete(Task, deleteCommandId),
        CommandOutcome.durablyCommitted(deleteCommandId, undefined)
      )
      const portableCreated = yield* replica.create(Task, {
        commandId: Identity.makeCommandId(),
        value: { title: "portable" }
      })
      assert.strictEqual(portableCreated._tag, "DurablyCommittedLocal")
      if (portableCreated._tag !== "DurablyCommittedLocal") return
      const exported = yield* replica.exportDocument(Task, portableCreated.value)
      const importCommandId = Identity.makeCommandId()
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
    }).pipe(Effect.provide(Live), Effect.provide(Database)))
})
