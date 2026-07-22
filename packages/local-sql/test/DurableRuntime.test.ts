import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as MessageStorage from "effect/unstable/cluster/MessageStorage"
import * as Runners from "effect/unstable/cluster/Runners"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as DurableClock from "effect/unstable/workflow/DurableClock"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import { rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as Compaction from "../src/Compaction.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as DurableRuntime from "../src/DurableRuntime.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as ReplicaWorkflow from "../src/ReplicaWorkflow.js"

describe("DurableRuntime", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const Rename = Mutation.make("Rename", { document: Task, payload: Schema.String })
  const RestartWorkflow = Workflow.make("EffectLocal/TestRestartWorkflow", {
    payload: { operationId: Schema.String },
    idempotencyKey: ({ operationId }) => operationId
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
  const Executor = Layer.succeed(
    CommandExecutor.CommandExecutor,
    CommandExecutor.CommandExecutor.of({
      create: (_document, options) =>
        Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, options.documentId)),
      mutate: (_mutation, options) => Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, undefined)),
      delete: (_document, options) => Effect.succeed(CommandOutcome.durablyCommitted(options.commandId, undefined)),
      lookupCreate: (id) => Effect.succeed(CommandOutcome.unknown(id)),
      lookupMutation: (_mutation, id) => Effect.succeed(CommandOutcome.unknown(id)),
      lookupDelete: (id) => Effect.succeed(CommandOutcome.unknown(id))
    })
  )
  const Limits = ReplicaLimits.layer({
    maxBackupBytes: 1_000_000,
    maxChunkBytes: 64_000,
    maxArchiveRecords: 1_000,
    maxJsonDepth: 32,
    maxSyncMessageBytes: 64_000,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1_000,
    maxSyncOperationsPerMessage: 1_000,
    maxPendingBytesPerDocument: 1_000_000,
    maxPendingBytesPerPeer: 1_000_000,
    maxPendingBytesPerReplica: 2_000_000,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1_000,
    maxPendingChangesPerPeer: 1_000,
    maxPendingChangesPerReplica: 2_000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 20_000,
    maxSessions: 8,
    maxStreamsPerSession: 4,
    maxInFlightPerSession: 16,
    maxQueuedRpc: 32
  })
  const Gate = ReplicaGate.layer.pipe(Layer.provide(Layer.merge(Database, Bootstrap)))
  const Store = DocumentStore.layer.pipe(Layer.provide(Layer.merge(Database, Gate)))
  const RecoveryService = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(Database, Gate)))
  const CompactionService = Compaction.layer.pipe(Layer.provide(Layer.mergeAll(Database, Gate, RecoveryService)))
  const Inputs = Layer.mergeAll(Database, Bootstrap, Executor, Limits, Gate, Store, RecoveryService, CompactionService)
  const Live = DurableRuntime.layer(definition).pipe(Layer.provide(Inputs))
  const Services = Layer.merge(Inputs, Live)

  const servicesAtWith = <A, E, R,>(filename: string, workflowRegistrations: Layer.Layer<A, E, R>) => {
    const database = Layer.merge(SqliteClient.layer({ filename, disableWAL: true }), NodeCrypto.layer)
    const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(database))
    const gate = ReplicaGate.layer.pipe(Layer.provide(Layer.merge(database, bootstrap)))
    const store = DocumentStore.layer.pipe(Layer.provide(Layer.merge(database, gate)))
    const recovery = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(database, gate)))
    const compaction = Compaction.layer.pipe(Layer.provide(Layer.mergeAll(database, gate, recovery)))
    const inputs = Layer.mergeAll(database, bootstrap, Executor, Limits, gate, store, recovery, compaction)
    return Layer.merge(inputs, DurableRuntime.layerWith(definition, workflowRegistrations).pipe(Layer.provide(inputs)))
  }
  const servicesAt = (filename: string) => servicesAtWith(filename, Layer.empty)

  it.effect("activates the SQL runner, entity, message storage, and workflow engine", () =>
    Effect.gen(function*() {
      assert.ok(yield* Sharding.Sharding)
      assert.ok(yield* Runners.Runners)
      assert.ok(yield* MessageStorage.MessageStorage)
      assert.ok(yield* WorkflowEngine.WorkflowEngine)
    }).pipe(Effect.provide(Live)))

  it.effect("registers the replica compaction workflow", () =>
    Effect.gen(function*() {
      const result = yield* Effect.exit(ReplicaWorkflow.CompactReplica.execute({
        replicaIncarnation: Identity.ReplicaIncarnation.make(0),
        operationId: ReplicaWorkflow.OperationId.make("compact")
      }))
      assert.strictEqual(result._tag, "Success")
    }).pipe(Effect.provide(Live)))

  it.effect("executes and polls an incarnation-scoped compaction operation", () =>
    Effect.gen(function*() {
      const runtime = yield* ReplicaWorkflow.CompactionWorkflow
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const stored = yield* store.create(Task, documentId, { title: "compact me" })
      InternalAutomerge.free(stored.automerge)

      const operationId = ReplicaWorkflow.OperationId.make("compact-documents")
      const execution = yield* runtime.execute(operationId)
      const sharding = yield* Sharding.Sharding
      for (let round = 0; round < 4; round++) {
        yield* sharding.pollStorage
        yield* TestClock.adjust(5_000)
      }

      const result = yield* runtime.poll(execution)
      assert.isTrue(Option.isSome(result))
      if (Option.isSome(result)) assert.strictEqual(result.value._tag, "Complete")
      const checkpoints = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_checkpoints WHERE document_id = ${documentId}`
      assert.strictEqual(checkpoints[0]?.count, 1)
    }).pipe(Effect.provide(Services)))

  it.effect("fences workflow handles from a prior replica incarnation", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const runtime = yield* ReplicaWorkflow.CompactionWorkflow
      const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("before-restore"))
      yield* gate.claim(() => Effect.void)
      const result = yield* Effect.exit(runtime.poll(execution))
      assert.strictEqual(result._tag, "Failure")
    }).pipe(Effect.provide(Services)))

  it.effect("rejects resuming a stale incarnation without compacting documents", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const runtime = yield* ReplicaWorkflow.CompactionWorkflow
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const stored = yield* store.create(Task, documentId, { title: "stale" })
      InternalAutomerge.free(stored.automerge)
      const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("stale-resume"))
      yield* gate.claim(() => Effect.void)

      assert.strictEqual((yield* Effect.exit(runtime.resume(execution)))._tag, "Failure")
      const checkpoints = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_checkpoints WHERE document_id = ${documentId}`
      assert.strictEqual(checkpoints[0]?.count, 0)
    }).pipe(Effect.provide(Services)))

  it.effect("rejects workflow handles whose execution id does not match the operation", () =>
    Effect.gen(function*() {
      const runtime = yield* ReplicaWorkflow.CompactionWorkflow
      const first = yield* runtime.execute(ReplicaWorkflow.OperationId.make("first-operation"))
      const second = yield* runtime.execute(ReplicaWorkflow.OperationId.make("second-operation"))
      const forged = { ...first, executionId: second.executionId }
      assert.strictEqual((yield* Effect.exit(runtime.poll(forged)))._tag, "Failure")
      assert.strictEqual((yield* Effect.exit(runtime.resume(forged)))._tag, "Failure")
    }).pipe(Effect.provide(Services)))

  it.effect("polls a completed workflow after the SQL runtime restarts", () =>
    Effect.gen(function*() {
      const filename = join(tmpdir(), `effect-local-workflow-${globalThis.crypto.randomUUID()}.sqlite`)
      yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(filename, { force: true })))
      const execution = yield* Effect.scoped(
        Effect.gen(function*() {
          const runtime = yield* ReplicaWorkflow.CompactionWorkflow
          const sharding = yield* Sharding.Sharding
          const store = yield* DocumentStore.DocumentStore
          const stored = yield* store.create(Task, yield* Identity.makeDocumentId, { title: "restart" })
          InternalAutomerge.free(stored.automerge)
          const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("restart-compaction"))
          for (let round = 0; round < 4; round++) {
            yield* sharding.pollStorage
            yield* TestClock.adjust(5_000)
          }
          assert.strictEqual((yield* runtime.poll(execution))._tag, "Some")
          return execution
        }).pipe(Effect.provide(servicesAt(filename)))
      )

      yield* Effect.scoped(
        Effect.gen(function*() {
          const runtime = yield* ReplicaWorkflow.CompactionWorkflow
          const result = yield* runtime.poll(execution)
          assert.isTrue(Option.isSome(result))
          if (Option.isSome(result)) assert.strictEqual(result.value._tag, "Complete")
        }).pipe(Effect.provide(servicesAt(filename)))
      )
    }))

  it.effect("reconciles a suspended in-flight workflow after the SQL runtime restarts", () =>
    Effect.gen(function*() {
      const filename = join(tmpdir(), `effect-local-workflow-${globalThis.crypto.randomUUID()}.sqlite`)
      yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(filename, { force: true })))
      const attempts = yield* Ref.make(0)
      const registration = RestartWorkflow.toLayer(Effect.fn(function*() {
        yield* Ref.update(attempts, (value) => value + 1)
        yield* DurableClock.sleep({
          name: "RestartDelay",
          duration: "1 hour",
          inMemoryThreshold: 0
        })
      }))
      const executionId = yield* Effect.scoped(
        Effect.gen(function*() {
          const sharding = yield* Sharding.Sharding
          const executionId = yield* RestartWorkflow.execute(
            { operationId: "restart-interrupted" },
            { discard: true }
          )
          for (let round = 0; round < 4; round++) {
            yield* sharding.pollStorage
            yield* TestClock.adjust(5_000)
          }
          const suspended = yield* RestartWorkflow.poll(executionId)
          assert.isTrue(Option.isSome(suspended))
          if (Option.isSome(suspended)) assert.strictEqual(suspended.value._tag, "Suspended")
          assert.strictEqual(yield* Ref.get(attempts), 1)
          return executionId
        }).pipe(Effect.provide(servicesAtWith(filename, registration)))
      )

      yield* Effect.scoped(
        Effect.gen(function*() {
          const sharding = yield* Sharding.Sharding
          yield* RestartWorkflow.resume(executionId)
          yield* sharding.pollStorage
          yield* TestClock.adjust("1 hour")
          for (let round = 0; round < 4; round++) {
            yield* sharding.pollStorage
            yield* TestClock.adjust(5_000)
          }
          const reconciled = yield* RestartWorkflow.poll(executionId)
          assert.isTrue(Option.isSome(reconciled))
          if (Option.isSome(reconciled)) {
            assert.strictEqual(reconciled.value._tag, "Complete")
            if (reconciled.value._tag === "Complete") assert.isTrue(Exit.isSuccess(reconciled.value.exit))
          }
          assert.isAtLeast(yield* Ref.get(attempts), 2)
        }).pipe(Effect.provide(servicesAtWith(filename, registration)))
      )
    }))
})
