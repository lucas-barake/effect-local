import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

describe("CommandExecutor", () => {
  class CheckedRejected extends Schema.TaggedErrorClass<CheckedRejected>()("CheckedRejected", {}) {}

  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const Rename = Mutation.make("Rename", {
    document: Task,
    payload: Schema.String,
    success: Schema.String
  })
  const Checked = Mutation.make("Checked", {
    document: Task,
    payload: Schema.String,
    success: Schema.String,
    error: CheckedRejected
  })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [Rename, Checked],
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
  const Handlers = Layer.merge(
    Rename.toLayer(({ draft, payload }) => {
      draft.title = payload
      return payload
    }),
    Checked.toLayer(() => Result.fail(new CheckedRejected()))
  )
  const Dependencies = Layer.mergeAll(Base, Gate, Store, Projections, Handlers)
  const Executor = CommandExecutor.layer(definition).pipe(Layer.provide(Dependencies))
  const Live = Layer.mergeAll(Base, Gate, Executor)

  it.effect("deduplicates matching requests and rejects conflicting command reuse", () =>
    Effect.scoped(Effect.gen(function*() {
      const executor = yield* CommandExecutor.CommandExecutor
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
      const created = yield* executor.create(Task, {
        commandId: createCommandId,
        documentId,
        permit,
        requestHash: createHash,
        value: { title: "one" }
      })
      const duplicate = yield* executor.create(Task, {
        commandId: createCommandId,
        documentId,
        permit,
        requestHash: createHash,
        value: { title: "one" }
      })
      assert.deepStrictEqual(duplicate, created)

      const mutationCommandId = yield* Identity.makeCommandId
      const mutationHash = yield* CommandExecutor.mutationRequestHash({
        incarnation: permit.incarnation,
        commandId: mutationCommandId,
        documentId,
        mutation: Rename,
        payload: "two"
      })
      const mutated = yield* executor.mutate(Rename, {
        commandId: mutationCommandId,
        documentId,
        payload: "two",
        permit,
        requestHash: mutationHash
      })
      assert.deepStrictEqual(mutated, CommandOutcome.durablyCommitted(mutationCommandId, "two"))
      assert.deepStrictEqual(
        yield* executor.lookupMutation(Rename, mutationCommandId, permit),
        mutated
      )
      const conflictingHash = yield* CommandExecutor.mutationRequestHash({
        incarnation: permit.incarnation,
        commandId: mutationCommandId,
        documentId,
        mutation: Rename,
        payload: "different"
      })
      assert.strictEqual(
        (yield* Effect.exit(executor.mutate(Rename, {
          commandId: mutationCommandId,
          documentId,
          payload: "different",
          permit,
          requestHash: conflictingHash
        })))._tag,
        "Failure"
      )
      const counts = yield* sql<{ readonly changes: number; readonly receipts: number }>`
        SELECT
          (SELECT COUNT(*) FROM effect_local_changes) AS changes,
          (SELECT COUNT(*) FROM effect_local_command_receipts) AS receipts
      `
      assert.deepStrictEqual(counts[0], { changes: 2, receipts: 2 })
    })).pipe(Effect.provide(Live)))

  it.effect("stores deterministic domain rejection without changing canonical state", () =>
    Effect.scoped(Effect.gen(function*() {
      const executor = yield* CommandExecutor.CommandExecutor
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
      const commandId = yield* Identity.makeCommandId
      const requestHash = yield* CommandExecutor.mutationRequestHash({
        incarnation: permit.incarnation,
        commandId,
        documentId,
        mutation: Checked,
        payload: "no"
      })
      const outcome = yield* executor.mutate(Checked, {
        commandId,
        documentId,
        payload: "no",
        permit,
        requestHash
      })
      assert.deepStrictEqual(outcome, CommandOutcome.rejected(commandId, new CheckedRejected()))
      assert.deepStrictEqual(yield* executor.lookupMutation(Checked, commandId, permit), outcome)
      const rows = yield* sql<{ readonly commit_sequence: number }>`
        SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1
      `
      assert.strictEqual(rows[0]?.commit_sequence, 1)
    })).pipe(Effect.provide(Live)))

  it.effect("round trips void delete receipts", () =>
    Effect.scoped(Effect.gen(function*() {
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
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

      const commandId = yield* Identity.makeCommandId
      const requestHash = yield* CommandExecutor.deleteRequestHash({
        incarnation: permit.incarnation,
        commandId,
        document: Task,
        documentId
      })
      const outcome = yield* executor.delete(Task, {
        commandId,
        documentId,
        permit,
        requestHash
      })

      assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, undefined))
      assert.deepStrictEqual(yield* executor.lookupDelete(commandId, permit), outcome)
    })).pipe(Effect.provide(Live)))
})
