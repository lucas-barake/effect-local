import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import { vi } from "vitest"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as Compaction from "../src/Compaction.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as InternalConflicts from "../src/internal/conflicts.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import { gateLimits, withGateLimits } from "./fixtures/limits.js"
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
  const Gate = ReplicaGate.layer.pipe(withGateLimits, Layer.provide(Base))
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
  const RecoveryService = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(Base, Gate)))
  const CompactionService = Compaction.layer.pipe(Layer.provide(Layer.mergeAll(Base, Gate, RecoveryService)))
  const Live = Layer.mergeAll(Base, Gate, Store, Executor, CompactionService)
  const ConflictLimits = ReplicaLimits.layer({ ...gateLimits, maxConflictValueBytes: 8 })
  const ConflictLimitedGate = ReplicaGate.layer.pipe(
    Layer.provide(Layer.merge(Base, ConflictLimits))
  )
  const ConflictLimitedStore = DocumentStore.layer.pipe(
    Layer.provide(Layer.mergeAll(Base, ConflictLimits, ConflictLimitedGate))
  )
  const ConflictLimitedExecutor = CommandExecutor.layer(definition).pipe(
    Layer.provide(Layer.mergeAll(
      Base,
      ConflictLimits,
      ConflictLimitedGate,
      ConflictLimitedStore,
      Projections,
      Handlers
    ))
  )
  const ConflictLimitedLive = Layer.mergeAll(
    Base,
    ConflictLimits,
    ConflictLimitedGate,
    ConflictLimitedStore,
    ConflictLimitedExecutor
  )
  const changingLimits = { ...gateLimits }
  const ChangingLimits = Layer.succeed(ReplicaLimits.ReplicaLimits, changingLimits)
  const ChangingGate = ReplicaGate.layer.pipe(
    Layer.provide(Layer.merge(Base, ChangingLimits))
  )
  const ChangingStore = DocumentStore.layer.pipe(
    Layer.provide(Layer.mergeAll(Base, ChangingLimits, ChangingGate))
  )
  const ChangingExecutor = CommandExecutor.layer(definition).pipe(
    Layer.provide(Layer.mergeAll(
      Base,
      ChangingLimits,
      ChangingGate,
      ChangingStore,
      Projections,
      Handlers
    ))
  )
  const ChangingLive = Layer.mergeAll(Base, ChangingLimits, ChangingGate, ChangingStore, ChangingExecutor)

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

  it.effect("durably rejects invalid resolutions and replays an exact committed resolution", () =>
    Effect.scoped(Effect.gen(function*() {
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const store = yield* DocumentStore.DocumentStore
      const permit = yield* gate.shared
      const documentId = yield* Identity.makeDocumentId
      const createCommandId = yield* Identity.makeCommandId
      const encoded = yield* Document.encode(Task, documentId, { title: "base" })
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
        value: { title: "base" }
      })

      const durable = yield* store.load(Task, documentId)
      yield* Effect.addFinalizer(() => Effect.sync(() => InternalAutomerge.free(durable.automerge)))
      const initialHeads = InternalAutomerge.heads(durable.automerge)
      const left = InternalAutomerge.stage(
        durable.automerge,
        "00000000000000000000000000000002",
        (draft) => draft.title = "left"
      )
      const right = InternalAutomerge.stage(
        durable.automerge,
        "00000000000000000000000000000003",
        (draft) => draft.title = "right"
      )
      yield* Effect.addFinalizer(() => Effect.sync(() => InternalAutomerge.free(right)))
      const merged = InternalAutomerge.replay(
        left,
        InternalAutomerge.changesSince(right, initialHeads).map((change) => change.bytes)
      )
      yield* Effect.addFinalizer(() => Effect.sync(() => InternalAutomerge.free(merged)))
      const persisted = yield* store.persist(Task, documentId, durable, merged)
      yield* Effect.addFinalizer(() => Effect.sync(() => InternalAutomerge.free(persisted.automerge)))
      const [record] = yield* InternalConflicts.inspect(persisted.automerge, gateLimits)
      assert.isDefined(record)

      const hashResolution = (
        commandId: Identity.CommandId,
        resolution: Conflict.Resolution
      ) =>
        InternalConflicts.encodeResolution(resolution, gateLimits).pipe(
          Effect.flatMap((encodedResolution) =>
            CommandExecutor.resolutionRequestHash({
              incarnation: permit.incarnation,
              commandId,
              document: Task,
              documentId,
              resolution: encodedResolution
            })
          )
        )

      const invalidCommandId = yield* Identity.makeCommandId
      const invalidResolution: Conflict.Resolution = {
        heads: persisted.materializedHeads,
        path: record.path,
        choice: { _tag: "ReplaceValue", value: 42 }
      }
      const invalidOutcome = yield* executor.resolve(Task, {
        commandId: invalidCommandId,
        documentId,
        permit,
        requestHash: yield* hashResolution(invalidCommandId, invalidResolution),
        resolution: invalidResolution
      })
      assert.strictEqual(invalidOutcome._tag, "Rejected")
      if (invalidOutcome._tag === "Rejected") {
        assert.strictEqual(invalidOutcome.error._tag, "ConflictResolutionSchemaError")
      }
      assert.deepStrictEqual(
        yield* executor.lookupResolution(Task, {
          commandId: invalidCommandId,
          documentId,
          permit,
          resolution: invalidResolution
        }),
        invalidOutcome
      )

      const afterInvalid = yield* store.load(Task, documentId)
      yield* Effect.addFinalizer(() => Effect.sync(() => InternalAutomerge.free(afterInvalid.automerge)))
      assert.strictEqual((yield* InternalConflicts.inspect(afterInvalid.automerge, gateLimits)).length, 1)

      const commandId = yield* Identity.makeCommandId
      const resolution: Conflict.Resolution = {
        heads: afterInvalid.materializedHeads,
        path: record.path,
        choice: {
          _tag: "SelectAlternative",
          alternativeId: record.alternatives[0].id
        }
      }
      const requestHash = yield* hashResolution(commandId, resolution)
      const outcome = yield* executor.resolve(Task, {
        commandId,
        documentId,
        permit,
        requestHash,
        resolution
      })
      assert.deepStrictEqual(outcome, CommandOutcome.durablyCommitted(commandId, undefined))
      assert.deepStrictEqual(
        yield* executor.resolve(Task, {
          commandId,
          documentId,
          permit,
          requestHash,
          resolution
        }),
        outcome
      )
      assert.deepStrictEqual(
        yield* executor.lookupResolution(Task, {
          commandId,
          documentId,
          permit,
          resolution
        }),
        outcome
      )

      const conflictingResolution: Conflict.Resolution = {
        ...resolution,
        choice: { _tag: "DeleteValue" }
      }
      const conflicting = yield* Effect.flip(executor.lookupResolution(Task, {
        commandId,
        documentId,
        permit,
        resolution: conflictingResolution
      }))
      assert.strictEqual(conflicting.reason._tag, "CommandIdConflict")
    })).pipe(Effect.provide(Live)))

  it.effect("enforces configured conflict limits before new resolution execution", () =>
    Effect.scoped(Effect.gen(function*() {
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const permit = yield* gate.shared
      const documentId = yield* Identity.makeDocumentId
      const commandId = yield* Identity.makeCommandId
      const resolution: Conflict.Resolution = {
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "title" } },
        choice: { _tag: "ReplaceValue", value: "replacement exceeds eight bytes" }
      }
      const encoded = yield* Schema.encodeEffect(Conflict.Resolution)(resolution)
      const requestHash = yield* CommandExecutor.resolutionRequestHash({
        incarnation: permit.incarnation,
        commandId,
        document: Task,
        documentId,
        resolution: encoded
      })

      const resolveError = yield* Effect.flip(executor.resolve(Task, {
        commandId,
        documentId,
        permit,
        requestHash,
        resolution
      }))
      assert.strictEqual(resolveError.reason._tag, "QuotaExceeded")
      if (resolveError.reason._tag === "QuotaExceeded") {
        assert.strictEqual(resolveError.reason.resource, "conflict value bytes")
        assert.strictEqual(resolveError.reason.limit, 8)
      }

      assert.deepStrictEqual(
        yield* executor.lookupResolution(Task, {
          commandId,
          documentId,
          permit,
          resolution
        }),
        CommandOutcome.unknown(commandId)
      )
    })).pipe(Effect.provide(ConflictLimitedLive)))

  it.effect("replays an existing resolution receipt after conflict limits are lowered", () =>
    Effect.scoped(Effect.gen(function*() {
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const { documentId, permit } = yield* seedTask
      const commandId = yield* Identity.makeCommandId
      const resolution: Conflict.Resolution = {
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "title" } },
        choice: { _tag: "ReplaceValue", value: "replacement exceeds eight bytes" }
      }
      const encoded = yield* Schema.encodeEffect(Conflict.Resolution)(resolution)
      const requestHash = yield* CommandExecutor.resolutionRequestHash({
        incarnation: permit.incarnation,
        commandId,
        document: Task,
        documentId,
        resolution: encoded
      })
      const outcome = yield* executor.resolve(Task, {
        commandId,
        documentId,
        permit,
        requestHash,
        resolution
      })
      assert.strictEqual(outcome._tag, "Rejected")

      const previousLimit = changingLimits.maxConflictValueBytes
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          changingLimits.maxConflictValueBytes = 8
        }),
        () =>
          Effect.sync(() => {
            changingLimits.maxConflictValueBytes = previousLimit
          })
      )

      assert.deepStrictEqual(
        yield* executor.resolve(Task, {
          commandId,
          documentId,
          permit,
          requestHash,
          resolution
        }),
        outcome
      )
      assert.deepStrictEqual(
        yield* executor.lookupResolution(Task, {
          commandId,
          documentId,
          permit,
          resolution
        }),
        outcome
      )
      assert.strictEqual((yield* gate.current).incarnation, permit.incarnation)
    })).pipe(Effect.provide(ChangingLive)))

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
      const staged = stageSpy.mock.results.at(-1)?.value
      stageSpy.mockRestore()
      assert.isDefined(staged)
      // A rejected mutation must free the staged Automerge document it created.
      // A freed document throws on any access; a leaked one is still usable.
      if (staged === undefined) yield* Effect.die("expected staged document")
      assert.throws(() => InternalAutomerge.heads(staged))
    })).pipe(Effect.provide(Live)))

  it.effect("frees a staged document when an executor operation is interrupted", () =>
    Effect.scoped(Effect.gen(function*() {
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      const persisted = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const finalized = yield* Deferred.make<void>()
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          interruptBoundary.afterChangeInsert = Deferred.succeed(persisted, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.uninterruptible
          )
        }),
        () =>
          Effect.sync(() => {
            interruptBoundary.afterChangeInsert = undefined
          })
      )
      const stageSpy = vi.spyOn(InternalAutomerge, "stage")
      yield* Effect.addFinalizer(() => Effect.sync(() => stageSpy.mockRestore()))
      const commandId = yield* Identity.makeCommandId
      const requestHash = yield* CommandExecutor.mutationRequestHash({
        incarnation: permit.incarnation,
        commandId,
        documentId,
        mutation: Rename,
        payload: "two"
      })
      const operation = yield* executor.mutate(Rename, {
        commandId,
        documentId,
        payload: "two",
        permit,
        requestHash
      }).pipe(
        Effect.ensuring(Deferred.succeed(finalized, undefined)),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(persisted)
      const staged = stageSpy.mock.results.at(-1)?.value
      assert.isDefined(staged)
      const interrupt = yield* Fiber.interrupt(operation).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(interrupt)
      yield* Deferred.await(finalized)

      const exit = yield* Fiber.await(operation)
      if (!Exit.isFailure(exit)) yield* Effect.die("expected the executor operation to be interrupted")
      assert.isTrue(Cause.hasInterrupts(exit.cause))
      if (staged === undefined) yield* Effect.die("expected staged document")
      assert.throws(() => InternalAutomerge.heads(staged))
      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))

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

  it.effect("keeps replay suppression intact after pruning superseded receipts", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const changeCount = Effect.map(
        sql<{ readonly changes: number }>`SELECT COUNT(*) AS changes FROM effect_local_changes`,
        (rows) => rows[0]?.changes
      )
      const receiptKeys = sql<{ readonly command_id: string; readonly replica_incarnation: number }>`
        SELECT command_id, replica_incarnation FROM effect_local_command_receipts ORDER BY command_id
      `

      // A command committed before the epoch claim below, so its receipt becomes superseded.
      const stale = yield* gate.current
      const staleDocumentId = yield* Identity.makeDocumentId
      const staleCommandId = yield* Identity.makeCommandId
      const staleEncoded = yield* Document.encode(Task, staleDocumentId, { title: "stale" })
      yield* executor.create(Task, {
        commandId: staleCommandId,
        documentId: staleDocumentId,
        permit: stale,
        requestHash: yield* CommandExecutor.createRequestHash({
          incarnation: stale.incarnation,
          commandId: staleCommandId,
          document: Task,
          documentId: staleDocumentId,
          encoded: staleEncoded
        }),
        value: { title: "stale" }
      })

      yield* gate.claim(() => Effect.void)
      const permit = yield* gate.current
      assert.isAbove(permit.incarnation, stale.incarnation)

      const documentId = yield* Identity.makeDocumentId
      const createCommandId = yield* Identity.makeCommandId
      const encoded = yield* Document.encode(Task, documentId, { title: "one" })
      yield* executor.create(Task, {
        commandId: createCommandId,
        documentId,
        permit,
        requestHash: yield* CommandExecutor.createRequestHash({
          incarnation: permit.incarnation,
          commandId: createCommandId,
          document: Task,
          documentId,
          encoded
        }),
        value: { title: "one" }
      })
      const commandId = yield* Identity.makeCommandId
      const requestHash = yield* CommandExecutor.mutationRequestHash({
        incarnation: permit.incarnation,
        commandId,
        documentId,
        mutation: Rename,
        payload: "two"
      })
      const mutated = yield* executor.mutate(Rename, { commandId, documentId, payload: "two", permit, requestHash })
      assert.deepStrictEqual(mutated, CommandOutcome.durablyCommitted(commandId, "two"))
      const changesBeforePrune = yield* changeCount

      assert.strictEqual(yield* compaction.pruneCommandReceipts, 1)

      // Observed before any replay: re-issuing the command would reinsert the receipt and hide a
      // live row the prune had destroyed.
      assert.deepStrictEqual(
        yield* receiptKeys,
        [
          { command_id: createCommandId, replica_incarnation: permit.incarnation },
          { command_id: commandId, replica_incarnation: permit.incarnation }
        ].toSorted((left, right) => {
          if (left.command_id < right.command_id) return -1
          return 1
        })
      )
      assert.deepStrictEqual(yield* executor.lookupMutation(Rename, commandId, permit), mutated)

      const replayed = yield* executor.mutate(Rename, { commandId, documentId, payload: "two", permit, requestHash })
      assert.deepStrictEqual(replayed, mutated)
      assert.strictEqual(yield* changeCount, changesBeforePrune)
    }).pipe(Effect.provide(Live)))

  const probe = makeProbe()
  const ProbedDatabase = Layer.merge(
    probeLayer(probe).pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))),
    NodeCrypto.layer
  )
  const interruptBoundary: {
    afterChangeInsert: Effect.Effect<void> | undefined
    rollbackFailuresRemaining: number
  } = { afterChangeInsert: undefined, rollbackFailuresRemaining: 0 }
  const BoundarySql = Layer.effect(
    SqlClient.SqlClient,
    Effect.map(SqlClient.SqlClient, (sql) =>
      Object.assign(
        (...args: Array<any>) => sql(...args),
        sql,
        {
          reserve: sql.reserve.pipe(
            Effect.map((connection) =>
              Object.assign({}, connection, {
                execute: (
                  statement: string,
                  params: ReadonlyArray<unknown>,
                  transformRows: Parameters<typeof connection.execute>[2]
                ) => {
                  const executed = connection.execute(statement, params, transformRows)
                  if (!statement.includes("INSERT INTO effect_local_changes")) return executed
                  const boundary = interruptBoundary.afterChangeInsert
                  if (boundary === undefined) return executed
                  return executed.pipe(Effect.tap(() => boundary))
                },
                executeUnprepared: (
                  statement: string,
                  params: ReadonlyArray<unknown>,
                  transformRows: Parameters<typeof connection.executeUnprepared>[2]
                ) => {
                  if (statement !== "ROLLBACK" || interruptBoundary.rollbackFailuresRemaining === 0) {
                    return connection.executeUnprepared(statement, params, transformRows)
                  }
                  interruptBoundary.rollbackFailuresRemaining--
                  return Effect.fail(
                    new SqlError.SqlError({
                      reason: new SqlError.ConnectionError({
                        cause: Error("forced rollback failure"),
                        message: "forced rollback failure",
                        operation: "rollback"
                      })
                    })
                  )
                }
              })
            )
          )
        }
      ))
  ).pipe(Layer.provide(ProbedDatabase))
  const BoundaryDatabase = Layer.merge(BoundarySql, NodeCrypto.layer)
  const ProbedBootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(BoundaryDatabase))
  const ProbedBase = Layer.merge(BoundaryDatabase, ProbedBootstrap)
  const ProbedGate = ReplicaGate.layer.pipe(withGateLimits, Layer.provide(ProbedBase))
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

  const rejectStaleResolution = (
    executor: CommandExecutor.CommandExecutor["Service"],
    documentId: Identity.DocumentId,
    permit: ReplicaGate.Permit
  ) =>
    Effect.gen(function*() {
      const commandId = yield* Identity.makeCommandId
      const resolution: Conflict.Resolution = {
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "title" } },
        choice: { _tag: "DeleteValue" }
      }
      const encoded = yield* InternalConflicts.encodeResolution(resolution, gateLimits)
      const requestHash = yield* CommandExecutor.resolutionRequestHash({
        incarnation: permit.incarnation,
        commandId,
        document: Task,
        documentId,
        resolution: encoded
      })
      return yield* Effect.exit(executor.resolve(Task, {
        commandId,
        documentId,
        permit,
        requestHash,
        resolution
      }))
    })

  it.effect("recovers a resolution commit failure before releasing the connection", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      yield* sql`CREATE TABLE resolution_commit_parent (id INTEGER PRIMARY KEY)`
      yield* sql`CREATE TABLE resolution_commit_child (
        parent_id INTEGER NOT NULL,
        FOREIGN KEY (parent_id) REFERENCES resolution_commit_parent(id) DEFERRABLE INITIALLY DEFERRED
      )`
      yield* sql`CREATE TRIGGER fail_resolution_commit
        AFTER INSERT ON effect_local_command_receipts
        WHEN NEW.mutation_name = '$resolve'
        BEGIN
          INSERT INTO resolution_commit_child(parent_id) VALUES (999);
        END`

      const exit = yield* rejectStaleResolution(executor, documentId, permit)

      if (!Exit.isFailure(exit)) yield* Effect.die("expected the deferred constraint to reject the commit")
      assert.isFalse(Cause.hasDies(exit.cause))
      const failure = Cause.findErrorOption(exit.cause)
      assert.strictEqual(failure._tag, "Some")
      if (failure._tag === "Some") assert.strictEqual(failure.value.reason._tag, "StorageUnavailable")
      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))

  it.effect("does not persist a stale rejection when accepted heads are corrupt", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      yield* sql`UPDATE effect_local_documents SET accepted_heads = 'not-json'
        WHERE document_id = ${documentId}`

      const exit = yield* rejectStaleResolution(executor, documentId, permit)

      if (!Exit.isFailure(exit)) yield* Effect.die("expected corrupt accepted heads to fail")
      const failure = Cause.findErrorOption(exit.cause)
      if (Option.isNone(failure)) yield* Effect.die("expected a typed failure")
      assert.strictEqual(failure.value.reason._tag, "StorageCorrupt")
      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))

  it.effect("does not persist a stale rejection when materialized heads disagree with history", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      yield* sql`UPDATE effect_local_documents
        SET materialized_heads = '["0000000000000000000000000000000000000000000"]'
        WHERE document_id = ${documentId}`

      const exit = yield* rejectStaleResolution(executor, documentId, permit)

      if (!Exit.isFailure(exit)) yield* Effect.die("expected inconsistent materialized heads to fail")
      const failure = Cause.findErrorOption(exit.cause)
      if (Option.isNone(failure)) yield* Effect.die("expected a typed failure")
      assert.strictEqual(failure.value.reason._tag, "StorageCorrupt")
      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))

  it.effect("rolls back a failed command that is caught inside an ambient transaction", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      yield* sql`CREATE TRIGGER fail_ambient_mutation_receipt
        BEFORE INSERT ON effect_local_command_receipts
        WHEN NEW.mutation_name = 'Rename'
        BEGIN
          SELECT RAISE(ABORT, 'forced ambient receipt failure');
        END`

      yield* sql.withTransaction(
        renameTask(executor, documentId, permit, "partial").pipe(
          Effect.catchTag("ReplicaError", (error) => {
            assert.strictEqual(error.reason._tag, "StorageUnavailable")
            return Effect.void
          })
        )
      )

      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))

  it.effect("preserves typed causes when an ambient transaction is rolled back automatically", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      yield* sql`CREATE TRIGGER fail_resolution_receipt
        BEFORE INSERT ON effect_local_command_receipts
        WHEN NEW.mutation_name = '$resolve'
        BEGIN
          SELECT RAISE(ROLLBACK, 'forced resolution rollback');
        END`

      const exit = yield* sql.withTransaction(Effect.gen(function*() {
        const transactionExit = yield* rejectStaleResolution(executor, documentId, permit)
        yield* sql`BEGIN`
        return transactionExit
      }))

      if (!Exit.isFailure(exit)) yield* Effect.die("expected the receipt trigger to roll back the resolution")
      assert.isFalse(Cause.hasDies(exit.cause))
      assert.strictEqual(exit.cause.reasons.length, 2)
      for (const reason of exit.cause.reasons) {
        if (!Cause.isFailReason(reason)) yield* Effect.die(`expected a typed failure, got ${reason._tag}`)
        assert.strictEqual(reason.error.reason._tag, "StorageUnavailable")
      }
      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))

  it.effect("preserves rollback failure causes without poisoning the next transaction", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const { documentId, executor, permit } = yield* seedTask
      const before = yield* durableCounts
      yield* sql`CREATE TRIGGER fail_poisoned_mutation_receipt
        BEFORE INSERT ON effect_local_command_receipts
        WHEN NEW.mutation_name = 'Rename'
        BEGIN
          SELECT RAISE(ABORT, 'forced body failure');
        END`
      interruptBoundary.rollbackFailuresRemaining = 1

      const exit = yield* Effect.exit(renameTask(executor, documentId, permit, "poisoned"))

      if (!Exit.isFailure(exit)) yield* Effect.die("expected the body and rollback to fail")
      assert.isFalse(Cause.hasDies(exit.cause))
      assert.strictEqual(exit.cause.reasons.length, 2)
      const failures = exit.cause.reasons.flatMap((reason) => {
        if (Cause.isFailReason(reason)) return [reason.error]
        return []
      })
      assert.strictEqual(failures.length, 2)
      assert.deepStrictEqual(
        failures.map((failure) => failure.reason._tag),
        ["StorageUnavailable", "StorageUnavailable"]
      )
      assert.isTrue(failures.some((failure) =>
        failure.reason._tag === "StorageUnavailable" &&
        String(failure.reason.cause).includes("forced rollback failure")
      ))
      const commandId = yield* Identity.makeCommandId
      const requestHash = yield* CommandExecutor.mutationRequestHash({
        incarnation: permit.incarnation,
        commandId,
        documentId,
        mutation: Checked,
        payload: "next"
      })
      assert.deepStrictEqual(
        yield* executor.mutate(Checked, {
          commandId,
          documentId,
          payload: "next",
          permit,
          requestHash
        }),
        CommandOutcome.rejected(commandId, new CheckedRejected())
      )
      assert.deepStrictEqual(yield* durableCounts, {
        ...before,
        receipts: before.receipts + 1
      })
    })).pipe(Effect.provide(Probed)))

  it.effect("fails later commands immediately when transaction cleanup remains ambiguous", () =>
    Effect.scoped(Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const { documentId, executor, permit } = yield* seedTask
      yield* sql`CREATE TRIGGER fail_ambiguous_mutation_receipt
        BEFORE INSERT ON effect_local_command_receipts
        WHEN NEW.mutation_name = 'Rename'
        BEGIN
          SELECT RAISE(ABORT, 'forced ambiguous body failure');
        END`
      interruptBoundary.rollbackFailuresRemaining = 2

      const first = yield* Effect.exit(renameTask(executor, documentId, permit, "ambiguous"))
      if (!Exit.isFailure(first)) yield* Effect.die("expected ambiguous cleanup to fail")
      assert.strictEqual(first.cause.reasons.length, 2)

      const second = yield* Effect.exit(renameTask(executor, documentId, permit, "blocked"))
      if (!Exit.isFailure(second)) yield* Effect.die("expected the poisoned executor to fail")
      const failure = Cause.findErrorOption(second.cause)
      if (Option.isNone(failure)) yield* Effect.die("expected a typed failure")
      assert.strictEqual(failure.value.reason._tag, "StorageUnavailable")
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
    return rows[0]
  })

  const bumpWriterGeneration = (sql: SqlClient.SqlClient) =>
    sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1`

  const corruptChangeBytes = (text: string, params: Array<unknown>) => {
    if (!text.includes("INSERT INTO effect_local_changes")) return params
    return params.map((param) => {
      if (!(param instanceof Uint8Array) || param.length === 0) return param
      const corrupted = new Uint8Array(param)
      corrupted[corrupted.length - 1] = corrupted[corrupted.length - 1] ^ 0xff
      return corrupted
    })
  }

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
        before: (text) => {
          if (text.includes("INSERT INTO effect_local_commit_outbox")) return bumpWriterGeneration(sql)
          return undefined
        },
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
        mapParams: (text, params) => {
          if (!text.includes("UPDATE effect_local_documents SET")) return params
          return params.map((param) => {
            if (typeof param === "string" && param.startsWith("[\"")) return "[]"
            return param
          })
        }
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
        before: (text) => {
          if (text.includes("INSERT INTO effect_local_commit_outbox")) {
            return sql`DELETE FROM effect_local_documents WHERE document_id = ${documentId}`
          }
          return undefined
        }
      })

      const error = yield* Effect.flip(renameTask(executor, documentId, permit, "vanished"))

      assert.strictEqual(error.reason._tag, "DocumentNotFound")
      assert.deepStrictEqual(yield* durableCounts, before)
    })).pipe(Effect.provide(Probed)))
})
