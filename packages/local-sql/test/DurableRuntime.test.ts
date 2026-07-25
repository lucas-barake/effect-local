import * as Automerge from "@automerge/automerge"
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Latch from "effect/Latch"
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
import * as DocumentEntity from "../src/DocumentEntity.js"
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
    maxQueuedRpc: 32,
    maxQueuedPermits: 32,
    maxActiveRestores: 32,
    maxRestoresPerSession: 16,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  })
  const Gate = ReplicaGate.layer.pipe(Layer.provide(Limits), Layer.provide(Layer.merge(Database, Bootstrap)))
  const Store = DocumentStore.layer.pipe(Layer.provide(Layer.merge(Database, Gate)))
  const RecoveryService = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(Database, Gate)))
  const CompactionService = Compaction.layer.pipe(Layer.provide(Layer.mergeAll(Database, Gate, RecoveryService)))
  const Inputs = Layer.mergeAll(Database, Bootstrap, Executor, Limits, Gate, Store, RecoveryService, CompactionService)
  const Live = DurableRuntime.layer(definition).pipe(Layer.provide(Inputs))
  const Services = Layer.merge(Inputs, Live)

  const servicesAtWith = <A, E, R,>(filename: string, workflowRegistrations: Layer.Layer<A, E, R>) => {
    const database = Layer.merge(SqliteClient.layer({ filename, disableWAL: true }), NodeCrypto.layer)
    const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(database))
    const gate = ReplicaGate.layer.pipe(Layer.provide(Limits), Layer.provide(Layer.merge(database, bootstrap)))
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
      const sharding = yield* Sharding.Sharding
      const executionId = yield* ReplicaWorkflow.CompactReplica.execute({
        replicaIncarnation: Identity.ReplicaIncarnation.make(0),
        operationId: ReplicaWorkflow.OperationId.make("compact")
      }, { discard: true })
      for (let round = 0; round < 4; round++) {
        yield* sharding.pollStorage
        yield* TestClock.adjust(5_000)
      }
      const result = yield* ReplicaWorkflow.CompactReplica.poll(executionId)
      assert.isTrue(Option.isSome(result))
      if (Option.isSome(result)) {
        assert.strictEqual(result.value._tag, "Complete")
        if (result.value._tag === "Complete") assert.isTrue(Exit.isSuccess(result.value.exit))
      }
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
      if (Option.isSome(result)) {
        assert.strictEqual(result.value._tag, "Complete")
        if (result.value._tag === "Complete") assert.isTrue(Exit.isSuccess(result.value.exit))
      }
      const checkpoints = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_checkpoints WHERE document_id = ${documentId}`
      assert.strictEqual(checkpoints[0]?.count, 1)
    }).pipe(Effect.provide(Services)))

  // Receipts are keyed `(replica_incarnation, command_id)` and are only ever read at the current
  // incarnation, so every row below it is unreachable. They are seeded directly because
  // `persistReceipt` always writes the live incarnation, which makes a superseded row impossible to
  // produce through the executor. Distinct values per column so a wrong-row deletion is visible.
  // Returns the row the insert wrote, which is what the assertions below compare against.
  const seedReceipt = (incarnation: Identity.ReplicaIncarnation, label: string, ordinal: number) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const row = {
        replica_incarnation: incarnation,
        command_id: `cmd_00000000-0000-4000-8000-00000000000${ordinal}`,
        request_hash: `hash-${label}`,
        document_id: `doc_00000000-0000-4000-8000-00000000000${ordinal}`,
        commit_sequence: ordinal
      }
      yield* sql`INSERT INTO effect_local_command_receipts (
      replica_incarnation, command_id, request_hash, mutation_name, result,
      document_id, heads, commit_sequence
    ) VALUES (
      ${row.replica_incarnation}, ${row.command_id}, ${row.request_hash}, '$create',
      ${new TextEncoder().encode(row.command_id)}, ${row.document_id}, '[]',
      ${row.commit_sequence}
    )`
      return row
    })

  const readReceipts = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    return yield* sql<{
      readonly replica_incarnation: number
      readonly command_id: string
      readonly request_hash: string
      readonly document_id: string
      readonly commit_sequence: number
    }>`SELECT replica_incarnation, command_id, request_hash, document_id, commit_sequence
      FROM effect_local_command_receipts ORDER BY command_id`
  })

  it.effect("prunes command receipts from every superseded incarnation during the compaction workflow", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const runtime = yield* ReplicaWorkflow.CompactionWorkflow

      const oldest = yield* seedReceipt((yield* gate.current).incarnation, "oldest", 1)
      yield* gate.claim(() => Effect.void)
      // A second superseded generation: this is what fails a `= current - 1` predicate.
      const middle = yield* seedReceipt((yield* gate.current).incarnation, "middle", 2)
      yield* gate.claim(() => Effect.void)
      const live = yield* seedReceipt((yield* gate.current).incarnation, "live", 3)

      assert.deepStrictEqual(yield* readReceipts, [oldest, middle, live])

      const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("prune-receipts"))
      yield* drive
      const result = yield* runtime.poll(execution)
      assert.isTrue(Option.isSome(result))
      if (Option.isSome(result)) {
        assert.strictEqual(result.value._tag, "Complete")
        if (result.value._tag === "Complete") assert.isTrue(Exit.isSuccess(result.value.exit))
      }

      // Exact set, never `every(...)`: an empty table would satisfy "only the live incarnation" and
      // would hide a `<=` predicate that destroys the live receipt and silently re-runs its command.
      assert.deepStrictEqual(yield* readReceipts, [live])
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
      // Same reason as the interrupt twin below: the handler now journals a receipt reclamation
      // activity first, so an undriven execution parks mid run holding the gate read lock and
      // shutdown waits on it.
      yield* drive
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
    }), 20_000)

  it.effect("interrupts an in-flight compaction handle for the current incarnation", () =>
    Effect.gen(function*() {
      const runtime = yield* ReplicaWorkflow.CompactionWorkflow
      const sharding = yield* Sharding.Sharding
      const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("interrupt-current"))
      yield* runtime.interrupt(execution)
      for (let round = 0; round < 4; round++) {
        yield* sharding.pollStorage
        yield* TestClock.adjust(5_000)
      }
      assert.isTrue(Option.isSome(yield* runtime.poll(execution)))
    }).pipe(Effect.provide(Services)))

  it.effect("fences interrupt handles from a prior replica incarnation", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const runtime = yield* ReplicaWorkflow.CompactionWorkflow
      const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("interrupt-fence"))
      yield* gate.claim(() => Effect.void)
      assert.strictEqual((yield* Effect.exit(runtime.interrupt(execution)))._tag, "Failure")
    }).pipe(Effect.provide(Services)))

  it.effect("rejects interrupt handles whose execution id does not match the operation", () =>
    Effect.gen(function*() {
      const runtime = yield* ReplicaWorkflow.CompactionWorkflow
      const first = yield* runtime.execute(ReplicaWorkflow.OperationId.make("interrupt-first"))
      const second = yield* runtime.execute(ReplicaWorkflow.OperationId.make("interrupt-second"))
      // Let both executions reach a stable point before tearing the layer down. The handler now
      // journals a receipt reclamation activity first, so an undriven execution parks mid run
      // holding the gate read lock and shutdown waits on it.
      yield* drive
      const forged = { ...first, executionId: second.executionId }
      assert.strictEqual((yield* Effect.exit(runtime.interrupt(forged)))._tag, "Failure")
    }).pipe(Effect.provide(Services)))

  it.effect("serves ApplySync without holding the connection across the gate", () =>
    Effect.gen(function*() {
      const atGate = yield* Deferred.make<void>()
      const releaseGate = yield* Latch.make()
      const claimRan = yield* Deferred.make<void>()
      let armed = false
      const gateLayer = Layer.effect(
        ReplicaGate.ReplicaGate,
        Effect.gen(function*() {
          const gate = yield* ReplicaGate.ReplicaGate
          return ReplicaGate.ReplicaGate.of({
            ...gate,
            current: Effect.suspend(() => {
              if (!armed) return gate.current
              armed = false
              return Deferred.succeed(atGate, undefined).pipe(
                Effect.andThen(releaseGate.await),
                Effect.andThen(gate.current)
              )
            })
          })
        })
      ).pipe(Layer.provide(Gate))
      const store = DocumentStore.layer.pipe(Layer.provide(Layer.merge(Database, gateLayer)))
      const recovery = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(Database, gateLayer)))
      const compaction = Compaction.layer.pipe(Layer.provide(Layer.mergeAll(Database, gateLayer, recovery)))
      const inputs = Layer.mergeAll(Database, Bootstrap, Executor, Limits, gateLayer, store, recovery, compaction)
      const live = Layer.merge(inputs, DurableRuntime.layer(definition).pipe(Layer.provide(inputs)))

      yield* Effect.gen(function*() {
        const gate = yield* ReplicaGate.ReplicaGate
        const documents = yield* DocumentStore.DocumentStore
        const documentId = yield* Identity.makeDocumentId
        const peerId = yield* Identity.makePeerId
        const created = yield* documents.create(Task, documentId, { title: "local" })
        const remote = Automerge.change(
          Automerge.clone(created.automerge, { actor: "1".repeat(32) }),
          (draft) => {
            ;(draft.value as { title: string }).title = "remote"
          }
        )
        InternalAutomerge.free(created.automerge)
        const generated = Automerge.generateSyncMessage(remote, Automerge.initSyncState())
        InternalAutomerge.free(remote)
        assert.isNotNull(generated[1])
        const message = generated[1]!
        const messageHash = yield* Canonical.digest(message)
        const permit = yield* gate.current
        const entity = yield* DocumentEntity.DocumentEntity.client

        armed = true
        const victim = yield* entity(documentId).ApplySync({
          replicaIncarnation: permit.incarnation,
          peerId,
          connectionEpoch: "remote-epoch",
          localConnectionEpoch: "local-epoch",
          receiveSequence: 0,
          documentType: Task.name,
          messageHash,
          message,
          writerProvenance: []
        }).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(atGate)

        const claimant = yield* gate.claim(() => Deferred.succeed(claimRan, undefined)).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        for (let index = 0; index < 200; index++) yield* Effect.yieldNow
        yield* releaseGate.open

        const applied = yield* Fiber.join(victim)
        assert.strictEqual(applied.duplicate, false)
        assert.isNotNull(applied.reply)
        yield* Fiber.join(claimant)
        assert.isTrue(Option.isSome(yield* Deferred.poll(claimRan)))
      }).pipe(Effect.scoped, Effect.provide(live), TestClock.withLive)
    }), 20_000)

  // Rebuilds the production runtime around a decorated `Recovery` service. Every other service,
  // including every `Compaction` method, stays production code.
  const servicesWithRecovery = <E,>(recoveryLayer: Layer.Layer<Recovery.Recovery, E>) => {
    const compaction = Compaction.layer.pipe(Layer.provide(Layer.mergeAll(Database, Gate, recoveryLayer)))
    const inputs = Layer.mergeAll(Database, Bootstrap, Executor, Limits, Gate, Store, recoveryLayer, compaction)
    return Layer.merge(inputs, DurableRuntime.layer(definition).pipe(Layer.provide(inputs)))
  }

  // Drives the real checkpoint-install CAS to a miss. `Compaction.prepare` reads the prepared
  // `commitSequence` through `recovery.recover`, so a real commit performed after `recover` returns
  // lands inside the prepare -> publish window and makes the CAS at Compaction.ts:144 miss.
  const supersedingServices = (options: {
    readonly injections: number
    readonly target?: () => Identity.DocumentId | undefined
  }) =>
    Effect.gen(function*() {
      const injected = yield* Ref.make(0)
      return servicesWithRecovery(
        Layer.effect(
          Recovery.Recovery,
          Effect.gen(function*() {
            const recovery = yield* Recovery.Recovery
            const store = yield* DocumentStore.DocumentStore
            const crypto = yield* Crypto.Crypto
            const freshDocumentId = Identity.makeDocumentId.pipe(
              Effect.provideService(Crypto.Crypto, crypto),
              Effect.orDie
            )
            return Recovery.Recovery.of({
              ...recovery,
              recover: <D extends Document.Any,>(document: D, documentId: Identity.DocumentId) =>
                recovery.recover(document, documentId).pipe(
                  Effect.flatMap((stored) => {
                    const targeted = options.target?.()
                    if (targeted !== undefined && targeted !== documentId) return Effect.succeed(stored)
                    return Effect.gen(function*() {
                      if ((yield* Ref.getAndUpdate(injected, (count) => count + 1)) >= options.injections) {
                        return stored
                      }
                      const other = yield* store.create(Task, yield* freshDocumentId, { title: "concurrent" })
                      InternalAutomerge.free(other.automerge)
                      return stored
                    }).pipe(Effect.orDie)
                  })
                )
            })
          })
        ).pipe(Layer.provide(Layer.mergeAll(Database, RecoveryService, Store)))
      )
    })

  // Fails every `recover` with a non-superseded error, after freeing: `prepare` only installs its
  // finalizer once `recover` returns. `recoverCalls` counts real attempts so a test can prove the
  // retry predicate did not widen and re-attempt a permanent failure.
  const failingRecoveryServices = (error: ReplicaError.ReplicaError) =>
    Effect.gen(function*() {
      const recoverCalls = yield* Ref.make(0)
      const services = servicesWithRecovery(
        Layer.effect(
          Recovery.Recovery,
          Effect.gen(function*() {
            const recovery = yield* Recovery.Recovery
            return Recovery.Recovery.of({
              ...recovery,
              recover: <D extends Document.Any,>(document: D, documentId: Identity.DocumentId) =>
                recovery.recover(document, documentId).pipe(
                  Effect.flatMap((stored) =>
                    Ref.update(recoverCalls, (count) => count + 1).pipe(
                      Effect.andThen(Effect.sync(() => InternalAutomerge.free(stored.automerge))),
                      Effect.andThen(Effect.fail(error))
                    )
                  )
                )
            })
          })
        ).pipe(Layer.provide(Layer.mergeAll(Database, RecoveryService)))
      )
      return { services, recoverCalls }
    })

  const drive = Effect.gen(function*() {
    const sharding = yield* Sharding.Sharding
    for (let round = 0; round < 4; round++) {
      yield* sharding.pollStorage
      yield* TestClock.adjust(5_000)
    }
  })

  // `injections: 8` leaves exactly one usable attempt, pinning the retry budget from below: it fails
  // if `times` is anything less than `compactionPublishAttempts - 1`.
  it.effect(
    "publishes the checkpoint on the ninth attempt when the first eight are superseded",
    () =>
      Effect.gen(function*() {
        const services = yield* supersedingServices({ injections: 8 })
        yield* Effect.gen(function*() {
          const runtime = yield* ReplicaWorkflow.CompactionWorkflow
          const store = yield* DocumentStore.DocumentStore
          const sql = yield* SqlClient.SqlClient
          const documentId = yield* Identity.makeDocumentId
          const stored = yield* store.create(Task, documentId, { title: "retried" })
          InternalAutomerge.free(stored.automerge)

          const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("retried-compaction"))
          yield* drive

          const result = yield* runtime.poll(execution)
          assert.isTrue(Option.isSome(result))
          if (!Option.isSome(result)) return
          assert.strictEqual(result.value._tag, "Complete")
          if (result.value._tag !== "Complete") return
          assert.isTrue(Exit.isSuccess(result.value.exit))

          const checkpoints = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_checkpoints WHERE document_id = ${documentId}`
          assert.strictEqual(checkpoints[0]?.count, 1)
        }).pipe(Effect.provide(services))
      }),
    20_000
  )

  it.effect("keeps compacting later documents when an earlier document is superseded", () =>
    Effect.gen(function*() {
      let blocking: Identity.DocumentId | undefined
      const services = yield* supersedingServices({
        injections: Number.MAX_SAFE_INTEGER,
        target: () => blocking
      })
      yield* Effect.gen(function*() {
        const runtime = yield* ReplicaWorkflow.CompactionWorkflow
        const store = yield* DocumentStore.DocumentStore
        const sql = yield* SqlClient.SqlClient
        // The workflow iterates ORDER BY document_id, so sort to make the blocked document deterministic.
        const sorted = [yield* Identity.makeDocumentId, yield* Identity.makeDocumentId].toSorted()
        blocking = sorted[0]
        const later = sorted[1]
        for (const documentId of sorted) {
          const stored = yield* store.create(Task, documentId, { title: "ordered" })
          InternalAutomerge.free(stored.automerge)
        }

        const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("partial-compaction"))
        yield* drive

        const result = yield* runtime.poll(execution)
        assert.isTrue(Option.isSome(result))
        if (!Option.isSome(result)) return
        assert.strictEqual(result.value._tag, "Complete")
        if (result.value._tag !== "Complete") return
        assert.isTrue(Exit.isFailure(result.value.exit))
        if (!Exit.isFailure(result.value.exit)) return
        const error = Option.getOrThrow(Cause.findErrorOption(result.value.exit.cause))
        assert.strictEqual(error.reason._tag, "CheckpointSuperseded")
        if (error.reason._tag !== "CheckpointSuperseded") return
        assert.deepStrictEqual(error.reason.documentIds, [blocking])

        const laterCheckpoints = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_checkpoints WHERE document_id = ${later}`
        assert.strictEqual(laterCheckpoints[0]?.count, 1)
      }).pipe(Effect.provide(services))
    }), 20_000)

  it.effect("aggregates every superseded document into one workflow failure", () =>
    Effect.gen(function*() {
      const services = yield* supersedingServices({ injections: Number.MAX_SAFE_INTEGER })
      yield* Effect.gen(function*() {
        const runtime = yield* ReplicaWorkflow.CompactionWorkflow
        const store = yield* DocumentStore.DocumentStore
        const sql = yield* SqlClient.SqlClient
        const sorted = [yield* Identity.makeDocumentId, yield* Identity.makeDocumentId].toSorted()
        for (const documentId of sorted) {
          const stored = yield* store.create(Task, documentId, { title: "aggregated" })
          InternalAutomerge.free(stored.automerge)
        }

        const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("aggregated-supersede"))
        yield* drive

        const result = yield* runtime.poll(execution)
        assert.isTrue(Option.isSome(result))
        if (!Option.isSome(result)) return
        assert.strictEqual(result.value._tag, "Complete")
        if (result.value._tag !== "Complete") return
        assert.isTrue(Exit.isFailure(result.value.exit))
        if (!Exit.isFailure(result.value.exit)) return
        const error = Option.getOrThrow(Cause.findErrorOption(result.value.exit.cause))
        assert.strictEqual(error.reason._tag, "CheckpointSuperseded")
        if (error.reason._tag !== "CheckpointSuperseded") return
        assert.deepStrictEqual(error.reason.documentIds, sorted)
        assert.strictEqual(error.reason.attempts, 9)

        const checkpoints = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_checkpoints`
        assert.strictEqual(checkpoints[0]?.count, 0)
      }).pipe(Effect.provide(services))
    }), 20_000)

  // The load bearing half of the ordering decision. `CheckpointSuperseded` is caught and deferred,
  // so it cannot starve the activity wherever it sits; an unrecognised document type aborts the
  // handler inside the loop, which is the failure the reclamation must run before. Without this,
  // moving the activity below the loop passes every other test.
  it.effect("prunes command receipts when the document loop aborts on an unrecognised document type", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const runtime = yield* ReplicaWorkflow.CompactionWorkflow
      const sql = yield* SqlClient.SqlClient

      const aborted = yield* seedReceipt((yield* gate.current).incarnation, "aborted", 6)
      yield* gate.claim(() => Effect.void)
      const kept = yield* seedReceipt((yield* gate.current).incarnation, "kept", 7)
      assert.deepStrictEqual(yield* readReceipts, [aborted, kept])

      const documentId = yield* Identity.makeDocumentId
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions, materialized_heads,
        accepted_heads, tombstone, projection_status, checkpoint_hash
      ) VALUES (${documentId}, 'Ghost', 1, '[]', '[]', '[]', 0, 'ready', NULL)`

      const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("aborted-document-loop"))
      yield* drive

      const result = yield* runtime.poll(execution)
      assert.isTrue(Option.isSome(result))
      if (!Option.isSome(result)) return
      assert.strictEqual(result.value._tag, "Complete")
      if (result.value._tag !== "Complete") return
      assert.isTrue(Exit.isFailure(result.value.exit))
      if (!Exit.isFailure(result.value.exit)) return
      const error = Option.getOrThrow(Cause.findErrorOption(result.value.exit.cause))
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")

      // The handler aborted before finishing the loop, and the superseded receipt is still gone.
      assert.deepStrictEqual(yield* readReceipts, [kept])
    }).pipe(Effect.provide(Services)))

  // Pins the other half: a document that can never publish must not starve reclamation either.
  it.effect(
    "prunes command receipts even when every document reports a superseded checkpoint",
    () =>
      Effect.gen(function*() {
        const services = yield* supersedingServices({ injections: Number.MAX_SAFE_INTEGER })
        yield* Effect.gen(function*() {
          const gate = yield* ReplicaGate.ReplicaGate
          const runtime = yield* ReplicaWorkflow.CompactionWorkflow
          const store = yield* DocumentStore.DocumentStore

          yield* seedReceipt((yield* gate.current).incarnation, "starved", 4)
          yield* gate.claim(() => Effect.void)
          const retained = yield* seedReceipt((yield* gate.current).incarnation, "retained", 5)

          const documentId = yield* Identity.makeDocumentId
          const stored = yield* store.create(Task, documentId, { title: "starved" })
          InternalAutomerge.free(stored.automerge)

          const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("superseded-still-prunes"))
          yield* drive

          const result = yield* runtime.poll(execution)
          assert.isTrue(Option.isSome(result))
          if (!Option.isSome(result)) return
          assert.strictEqual(result.value._tag, "Complete")
          if (result.value._tag !== "Complete") return
          assert.isTrue(Exit.isFailure(result.value.exit))
          if (!Exit.isFailure(result.value.exit)) return
          const error = Option.getOrThrow(Cause.findErrorOption(result.value.exit.cause))
          assert.strictEqual(error.reason._tag, "CheckpointSuperseded")

          // The run failed, and the superseded receipt is still gone.
          assert.deepStrictEqual(yield* readReceipts, [retained])
        }).pipe(Effect.provide(services))
      }),
    20_000
  )

  // The issue-13 regression: the workflow used to report success here. `injections: 9` also pins the
  // retry budget from above, so this subsumes an unbounded-injection variant of the same scenario.
  it.effect("fails the compaction workflow after the ninth superseded publish attempt", () =>
    Effect.gen(function*() {
      const services = yield* supersedingServices({ injections: 9 })
      yield* Effect.gen(function*() {
        const runtime = yield* ReplicaWorkflow.CompactionWorkflow
        const store = yield* DocumentStore.DocumentStore
        const sql = yield* SqlClient.SqlClient
        const documentId = yield* Identity.makeDocumentId
        const stored = yield* store.create(Task, documentId, { title: "exhausted" })
        InternalAutomerge.free(stored.automerge)

        const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("exhausted-compaction"))
        yield* drive

        const result = yield* runtime.poll(execution)
        assert.isTrue(Option.isSome(result))
        if (!Option.isSome(result)) return
        assert.strictEqual(result.value._tag, "Complete")
        if (result.value._tag !== "Complete") return
        assert.isTrue(Exit.isFailure(result.value.exit))
        if (!Exit.isFailure(result.value.exit)) return
        const error = Option.getOrThrow(Cause.findErrorOption(result.value.exit.cause))
        assert.strictEqual(error.reason._tag, "CheckpointSuperseded")
        if (error.reason._tag !== "CheckpointSuperseded") return
        assert.deepStrictEqual(error.reason.documentIds, [documentId])
        assert.strictEqual(error.reason.attempts, 9)

        const checkpoints = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_checkpoints WHERE document_id = ${documentId}`
        assert.strictEqual(checkpoints[0]?.count, 0)
      }).pipe(Effect.provide(services))
    }), 20_000)

  it.effect(
    "prunes changes dominated by the retained checkpoints after a published compaction",
    () =>
      Effect.gen(function*() {
        const runtime = yield* ReplicaWorkflow.CompactionWorkflow
        const store = yield* DocumentStore.DocumentStore
        const sql = yield* SqlClient.SqlClient
        const documentId = yield* Identity.makeDocumentId
        const created = yield* store.create(Task, documentId, { title: "one" })

        yield* runtime.execute(ReplicaWorkflow.OperationId.make("prune-first")).pipe(Effect.andThen(drive))

        const staged = yield* store.stage(created, (draft) => {
          draft.title = "two"
        })
        const persisted = yield* store.persist(Task, documentId, created, staged)
        InternalAutomerge.free(staged)
        InternalAutomerge.free(created.automerge)

        const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("prune-second"))
        yield* drive

        const result = yield* runtime.poll(execution)
        assert.isTrue(Option.isSome(result))
        if (!Option.isSome(result)) return
        assert.strictEqual(result.value._tag, "Complete")
        if (result.value._tag !== "Complete") return
        assert.isTrue(Exit.isSuccess(result.value.exit))

        // Prune deletes applied changes dominated by both retained checkpoints. Without the prune call
        // the superseded rows survive, so this pins that the published path still reclaims them.
        const changes = yield* sql<{ readonly change_hash: string }>`SELECT change_hash
        FROM effect_local_changes WHERE document_id = ${documentId} ORDER BY change_hash`
        assert.deepStrictEqual(changes.map((row) => row.change_hash), persisted.materializedHeads)
        InternalAutomerge.free(persisted.automerge)
      }).pipe(Effect.provide(Services)),
    20_000
  )

  it.effect(
    "propagates a non-superseded compaction failure without retrying or aggregating it",
    () =>
      Effect.gen(function*() {
        const { recoverCalls, services } = yield* failingRecoveryServices(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({ cause: new Error("probe") })
          })
        )
        yield* Effect.gen(function*() {
          const runtime = yield* ReplicaWorkflow.CompactionWorkflow
          const store = yield* DocumentStore.DocumentStore
          const sql = yield* SqlClient.SqlClient
          const sorted = [yield* Identity.makeDocumentId, yield* Identity.makeDocumentId].toSorted()
          for (const documentId of sorted) {
            const stored = yield* store.create(Task, documentId, { title: "corrupt" })
            InternalAutomerge.free(stored.automerge)
          }

          const execution = yield* runtime.execute(ReplicaWorkflow.OperationId.make("corrupt-compaction"))
          yield* drive

          const result = yield* runtime.poll(execution)
          assert.isTrue(Option.isSome(result))
          if (!Option.isSome(result)) return
          assert.strictEqual(result.value._tag, "Complete")
          if (result.value._tag !== "Complete") return
          assert.isTrue(Exit.isFailure(result.value.exit))
          if (!Exit.isFailure(result.value.exit)) return
          const error = Option.getOrThrow(Cause.findErrorOption(result.value.exit.cause))
          assert.strictEqual(error.reason._tag, "StorageCorrupt")

          // The retry predicate must narrow to CheckpointSuperseded. A widened predicate would burn all
          // nine attempts on a permanent failure, which the final tag alone cannot detect.
          assert.strictEqual(yield* Ref.get(recoverCalls), 1)

          const laterCheckpoints = yield* sql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_checkpoints WHERE document_id = ${sorted[1]}`
          assert.strictEqual(laterCheckpoints[0]?.count, 0)
        }).pipe(Effect.provide(services))
      }),
    20_000
  )
})
