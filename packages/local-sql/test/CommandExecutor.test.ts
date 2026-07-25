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
import { vi } from "vitest"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import { makeProbe, probeLayer, withFault } from "./helpers/sqlProbe.js"

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

  it.effect("rejects lookups that target a receipt written by a different operation", () =>
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
      const mutationOnCreate = yield* Effect.flip(executor.lookupMutation(Rename, createCommandId, permit))
      assert.strictEqual(mutationOnCreate.reason._tag, "ReceiptOperationMismatch")
      const deleteOnCreate = yield* Effect.flip(executor.lookupDelete(createCommandId, permit))
      assert.strictEqual(deleteOnCreate.reason._tag, "ReceiptOperationMismatch")

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
      const createOnMutation = yield* Effect.flip(executor.lookupCreate(mutationCommandId, permit))
      assert.strictEqual(createOnMutation.reason._tag, "ReceiptOperationMismatch")
      const deleteOnMutation = yield* Effect.flip(executor.lookupDelete(mutationCommandId, permit))
      assert.strictEqual(deleteOnMutation.reason._tag, "ReceiptOperationMismatch")
      const otherMutation = yield* Effect.flip(executor.lookupMutation(Checked, mutationCommandId, permit))
      assert.strictEqual(otherMutation.reason._tag, "ReceiptOperationMismatch")

      assert.deepStrictEqual(yield* executor.lookupMutation(Rename, mutationCommandId, permit), mutated)
      assert.deepStrictEqual(
        yield* executor.lookupCreate(createCommandId, permit),
        CommandOutcome.durablyCommitted(createCommandId, documentId)
      )
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

  it.effect("frees the staged automerge document when a mutation is rejected", () =>
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
      const stageSpy = vi.spyOn(InternalAutomerge, "stage")
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
      const staged = stageSpy.mock.results.at(-1)?.value as InternalAutomerge.AnyDocument | undefined
      stageSpy.mockRestore()
      assert.isDefined(staged)
      // A rejected mutation must free the staged Automerge document it created.
      // A freed document throws on any access; a leaked one is still usable.
      assert.throws(() => InternalAutomerge.heads(staged!))
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

  const probe = makeProbe()
  const ProbedDatabase = Layer.merge(
    probeLayer(probe).pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))),
    NodeCrypto.layer
  )
  const ProbedBootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(ProbedDatabase))
  const ProbedBase = Layer.merge(ProbedDatabase, ProbedBootstrap)
  const ProbedGate = ReplicaGate.layer.pipe(Layer.provide(ProbedBase))
  const ProbedStore = DocumentStore.layer.pipe(Layer.provide(Layer.merge(ProbedBase, ProbedGate)))
  const ProbedProjections = ProjectionStore.layer([]).pipe(Layer.provide(ProbedBase))
  const ProbedExecutor = CommandExecutor.layer(definition).pipe(
    Layer.provide(Layer.mergeAll(ProbedBase, ProbedGate, ProbedStore, ProbedProjections, Handlers))
  )
  const Probed = Layer.mergeAll(ProbedBase, ProbedGate, ProbedExecutor)

  const seedTask = Effect.gen(function*() {
    const executor = yield* CommandExecutor.CommandExecutor
    const gate = yield* ReplicaGate.ReplicaGate
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
    yield* executor.create(Task, { commandId, documentId, permit, requestHash, value: { title: "one" } })
    return { documentId, executor, permit }
  })

  const renameTask = (
    executor: CommandExecutor.CommandExecutor["Service"],
    documentId: Identity.DocumentId,
    permit: ReplicaGate.Permit,
    title: string
  ) =>
    Effect.gen(function*() {
      const commandId = yield* Identity.makeCommandId
      const requestHash = yield* CommandExecutor.mutationRequestHash({
        incarnation: permit.incarnation,
        commandId,
        documentId,
        mutation: Rename,
        payload: title
      })
      return yield* executor.mutate(Rename, { commandId, documentId, payload: title, permit, requestHash })
    })

  it.effect("reads the retained history once per mutation regardless of its length", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, executor, permit } = yield* seedTask
      yield* renameTask(executor, documentId, permit, "two")
      yield* Effect.sync(() => probe.reset())
      yield* renameTask(executor, documentId, permit, "three")
      // `CommandExecutor.mutate` loads the document once; `DocumentStore.persist`
      // must not reconstruct it a second time to build its result.
      const shortHistory = probe.countHistoryReads()

      for (const title of ["four", "five", "six", "seven"]) {
        yield* renameTask(executor, documentId, permit, title)
      }
      yield* Effect.sync(() => probe.reset())
      yield* renameTask(executor, documentId, permit, "eight")
      const longHistory = probe.countHistoryReads()

      assert.strictEqual(shortHistory, 1)
      // Reconstruction cost must not grow with the retained history.
      assert.strictEqual(longHistory, 1)
    })).pipe(Effect.provide(Probed)))

  const durableCounts = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const rows = yield* sql<{
      readonly changes: number
      readonly outbox: number
      readonly receipts: number
      readonly commitSequence: number
    }>`
      SELECT
        (SELECT COUNT(*) FROM effect_local_changes) AS changes,
        (SELECT COUNT(*) FROM effect_local_commit_outbox) AS outbox,
        (SELECT COUNT(*) FROM effect_local_command_receipts) AS receipts,
        (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) AS commitSequence
    `
    return rows[0]!
  })

  const bumpWriterGeneration = (sql: SqlClient.SqlClient) =>
    sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1`

  const corruptChangeBytes = (text: string, params: Array<unknown>) =>
    text.includes("INSERT INTO effect_local_changes")
      ? params.map((param) => {
        if (!(param instanceof Uint8Array) || param.length === 0) return param
        const corrupted = new Uint8Array(param)
        corrupted[corrupted.length - 1] = corrupted[corrupted.length - 1]! ^ 0xff
        return corrupted
      })
      : params

  // The write path re-reads the change blobs it just wrote, so a driver that
  // mangles a BLOB on the way to storage fails the mutation instead of
  // committing it.
  it.effect("fails and rolls back a mutation whose change blob is corrupted on the way to storage", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      yield* withFault(probe, { mapParams: corruptChangeBytes })

      const error = yield* Effect.flip(renameTask(executor, documentId, permit, "corrupt"))

      assert.strictEqual(error.reason._tag, "StorageCorrupt")
      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))

  // The write path also revalidates the writer's permit after the rows are
  // written, and does so before re-reading them, so a mutation that is both
  // fenced and corrupt reports the fence. `CommandExecutor.mutate` only fences
  // before any read, so nothing else covers either behaviour.
  it.effect("reports the fence, not the corruption, when a mutation is both fenced and corrupted", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      yield* withFault(probe, {
        before: (text) =>
          text.includes("INSERT INTO effect_local_commit_outbox") ? bumpWriterGeneration(sql) : undefined,
        mapParams: corruptChangeBytes
      })

      const error = yield* Effect.flip(renameTask(executor, documentId, permit, "both"))

      assert.strictEqual(error.reason._tag, "ReplicaFenced")
      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))

  // The document row is re-read too, so a commit that publishes heads the
  // retained history cannot reproduce is rejected rather than committed.
  it.effect("fails and rolls back a mutation whose document row is written with stale heads", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      yield* withFault(probe, {
        mapParams: (text, params) =>
          text.includes("UPDATE effect_local_documents SET")
            ? params.map((param) => typeof param === "string" && param.startsWith("[\"") ? "[]" : param)
            : params
      })

      const error = yield* Effect.flip(renameTask(executor, documentId, permit, "stale"))

      assert.strictEqual(error.reason._tag, "StorageCorrupt")
      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))

  // The document row disappearing mid transaction is a typed failure, not a defect.
  it.effect("fails a mutation whose document row disappears inside the transaction", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      yield* withFault(probe, {
        before: (text) =>
          text.includes("INSERT INTO effect_local_commit_outbox")
            ? sql`DELETE FROM effect_local_documents WHERE document_id = ${documentId}`
            : undefined
      })

      const error = yield* Effect.flip(renameTask(executor, documentId, permit, "vanished"))

      assert.strictEqual(error.reason._tag, "DocumentNotFound")
      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))
})
