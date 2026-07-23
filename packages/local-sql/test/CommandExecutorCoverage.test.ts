import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

describe("CommandExecutor coverage probes", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const Rename = Mutation.make("Rename", {
    document: Task,
    payload: Schema.String,
    success: Schema.String
  })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [Rename],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Base = Layer.merge(Database, Bootstrap)
  const Gate = ReplicaGate.layer.pipe(Layer.provide(Base))
  const Store = DocumentStore.layer.pipe(Layer.provide(Layer.merge(Base, Gate)))
  const Projections = ProjectionStore.layer([]).pipe(Layer.provide(Base))
  const Handlers = Rename.toLayer(({ draft, payload }) => {
    draft.title = payload
    return payload
  })
  const Dependencies = Layer.mergeAll(Base, Gate, Store, Projections, Handlers)
  const Executor = CommandExecutor.layer(definition).pipe(Layer.provide(Dependencies))
  const Live = Layer.mergeAll(Base, Gate, Store, Executor)

  it.effect("fences a command that presents a stale permit after an epoch claim", () =>
    Effect.scoped(Effect.gen(function*() {
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const stale = yield* gate.shared
      const documentId = yield* Identity.makeDocumentId
      const commandId = yield* Identity.makeCommandId
      const encoded = yield* Document.encode(Task, documentId, { title: "one" })
      const requestHash = yield* CommandExecutor.createRequestHash({
        incarnation: stale.incarnation,
        commandId,
        document: Task,
        documentId,
        encoded
      })
      // A failover/restore claims a fresh epoch, invalidating the captured permit.
      yield* gate.claim(() => Effect.void)
      const error = yield* Effect.flip(executor.create(Task, {
        commandId,
        documentId,
        permit: stale,
        requestHash,
        value: { title: "one" }
      }))
      assert.strictEqual(error.reason._tag, "ReplicaFenced")
      const rows = yield* sql<{
        readonly commit_sequence: number
        readonly documents: number
        readonly receipts: number
      }>`SELECT
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commit_sequence,
        (SELECT COUNT(*) FROM effect_local_documents) AS documents,
        (SELECT COUNT(*) FROM effect_local_command_receipts) AS receipts`
      assert.deepStrictEqual(rows[0], { commit_sequence: 0, documents: 0, receipts: 0 })
    })).pipe(Effect.provide(Live)))

  it.effect("serializes concurrent distinct mutations on one document without losing changes", () =>
    Effect.scoped(Effect.gen(function*() {
      const executor = yield* CommandExecutor.CommandExecutor
      const store = yield* DocumentStore.DocumentStore
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const permit = yield* gate.shared
      const documentId = yield* Identity.makeDocumentId
      const createCommandId = yield* Identity.makeCommandId
      const encoded = yield* Document.encode(Task, documentId, { title: "one" })
      const createHash = yield* CommandExecutor.createRequestHash({
        incarnation: permit.incarnation,
        commandId: createCommandId,
        document: Task,
        documentId,
        encoded
      })
      yield* executor.create(Task, {
        commandId: createCommandId,
        documentId,
        permit,
        requestHash: createHash,
        value: { title: "one" }
      })
      const mutate = (payload: string) =>
        Effect.gen(function*() {
          const commandId = yield* Identity.makeCommandId
          const requestHash = yield* CommandExecutor.mutationRequestHash({
            incarnation: permit.incarnation,
            commandId,
            documentId,
            mutation: Rename,
            payload
          })
          return yield* executor.mutate(Rename, { commandId, documentId, payload, permit, requestHash })
        })
      const outcomes = yield* Effect.all([mutate("a"), mutate("b")], { concurrency: "unbounded" })
      assert.deepStrictEqual(outcomes.map((outcome) => outcome._tag), [
        "DurablyCommittedLocal",
        "DurablyCommittedLocal"
      ])
      const counts = yield* sql<{ readonly changes: number; readonly commit_sequence: number }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes WHERE document_id = ${documentId}) AS changes,
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commit_sequence`
      assert.deepStrictEqual(counts[0], { changes: 3, commit_sequence: 3 })
      const reloaded = yield* store.load(Task, documentId)
      assert.isTrue(["a", "b"].includes(reloaded.snapshot.value.title))
      InternalAutomerge.free(reloaded.automerge)
    })).pipe(Effect.provide(Live)))

  it.effect("rolls the whole command back when a late statement fails mid-transaction", () =>
    Effect.scoped(Effect.gen(function*() {
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const permit = yield* gate.shared
      const documentId = yield* Identity.makeDocumentId
      const commandId = yield* Identity.makeCommandId
      const encoded = yield* Document.encode(Task, documentId, { title: "one" })
      const requestHash = yield* CommandExecutor.createRequestHash({
        incarnation: permit.incarnation,
        commandId,
        document: Task,
        documentId,
        encoded
      })
      yield* sql`CREATE TRIGGER fail_receipt
        AFTER INSERT ON effect_local_command_receipts
        BEGIN SELECT RAISE(ABORT, 'receipt write failed'); END`
      const error = yield* Effect.flip(executor.create(Task, {
        commandId,
        documentId,
        permit,
        requestHash,
        value: { title: "one" }
      }))
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      const rows = yield* sql<{
        readonly changes: number
        readonly commit_sequence: number
        readonly documents: number
        readonly outbox: number
      }>`SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commit_sequence,
        (SELECT COUNT(*) FROM effect_local_documents) AS documents,
        (SELECT COUNT(*) FROM effect_local_commit_outbox) AS outbox`
      assert.deepStrictEqual(rows[0], { changes: 0, commit_sequence: 0, documents: 0, outbox: 0 })
    })).pipe(Effect.provide(Live)))

  it.effect("rejects a create whose request hash does not match the canonical payload", () =>
    Effect.scoped(Effect.gen(function*() {
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const permit = yield* gate.shared
      const documentId = yield* Identity.makeDocumentId
      const commandId = yield* Identity.makeCommandId
      const error = yield* Effect.flip(executor.create(Task, {
        commandId,
        documentId,
        permit,
        requestHash: "tampered",
        value: { title: "one" }
      }))
      assert.strictEqual(error.reason._tag, "CommandIdConflict")
      const rows = yield* sql<{ readonly documents: number }>`
        SELECT COUNT(*) AS documents FROM effect_local_documents`
      assert.strictEqual(rows[0]?.documents, 0)
    })).pipe(Effect.provide(Live)))
})
