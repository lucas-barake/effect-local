import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"
import * as ClusterWorkflowEngine from "effect/unstable/cluster/ClusterWorkflowEngine"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ReconciliationWorkflow from "../src/ReconciliationWorkflow.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const database = () =>
  Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer
  )

const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))

const definitionV2 = Definition.make({
  version: 2,
  models: Domain.definition.models,
  mutations: Domain.definition.mutations,
  queries: Domain.definition.queries
})

const migration = {
  retryDelay: "1 millis",
  maximumAttempts: 8
} satisfies { readonly retryDelay: Duration.Input; readonly maximumAttempts: number }
const clientHistory = {
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration
}
const serverHistory = {
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration
}

const serverLayer = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
  Layer.provide(runtime),
  Layer.provide(database())
)

const directSync = (server: ServerStore.Service) =>
  Layer.succeed(
    SyncEngine.SyncEngine,
    SyncEngine.SyncEngine.of({
      submit: server.submit,
      pull: server.pull,
      bootstrap: server.bootstrap,
      watch: server.watch
    })
  )

describe("reconciliation workflow", () => {
  it.effect("runs finite generation keyed reconciliation over the durable SQLite outbox", () =>
    Effect.gen(function*() {
      const serverContext = yield* Layer.build(serverLayer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const workflowEngine = WorkflowEngine.layerMemory
      const replicaLayer = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "1 millis"
      }).pipe(
        Layer.provide(Domain.handlers),
        Layer.provide(database()),
        Layer.provide(directSync(server)),
        Layer.provideMerge(workflowEngine)
      )
      const context = yield* Layer.build(replicaLayer)
      const replica = Context.get(context, Replica.Replica)
      const local = Context.get(context, LocalStore.Store)

      const first = yield* replica.mutate(Domain.PutTodo, Domain.todo("1"))
      const second = yield* replica.mutate(Domain.PutTodo, Domain.todo("2"))
      const requested = (yield* local.reconciliationGenerations).requested
      const payload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        generation: requested
      })

      const execution = yield* ReconciliationWorkflow.start(payload).pipe(Effect.provide(context))
      assert.strictEqual(execution.executionId, yield* ReconciliationWorkflow.executionId(payload))
      yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(Effect.provide(context))

      let completed = yield* local.reconciliationGenerations
      while (completed.completed < completed.requested) {
        const nextPayload = ReconciliationWorkflow.Payload.make({
          schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
          spaceId,
          clientId,
          generation: completed.requested
        })
        yield* ReconciliationWorkflow.make(nextPayload).execute(nextPayload).pipe(Effect.provide(context))
        completed = yield* local.reconciliationGenerations
      }

      assert.strictEqual(completed.completed, completed.requested)
      assert.isAtLeast(completed.completed, requested)
      assert.strictEqual(yield* local.pendingCount, 0)
      assert.strictEqual(Option.getOrThrow(yield* local.receipt(first.envelope.mutationId))._tag, "Accepted")
      assert.strictEqual(Option.getOrThrow(yield* local.receipt(second.envelope.mutationId))._tag, "Accepted")
      assert.deepStrictEqual(Option.getOrThrow(yield* replica.get(Domain.Todo, "2")), Domain.todo("2"))
      assert.deepStrictEqual(Object.keys(payload).toSorted(), [
        "clientId",
        "generation",
        "schemaIdentity",
        "spaceId"
      ])
    }))

  it.effect("rejects a workflow handle addressed to another replica", () =>
    Effect.gen(function*() {
      const serverContext = yield* Layer.build(serverLayer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const replicaLayer = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId
      }).pipe(
        Layer.provide(Domain.handlers),
        Layer.provide(database()),
        Layer.provide(directSync(server)),
        Layer.provideMerge(WorkflowEngine.layerMemory)
      )
      const context = yield* Layer.build(replicaLayer)
      const payload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId: Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002"),
        clientId,
        generation: 2
      })
      const error = yield* ReconciliationWorkflow.make({
        schemaIdentity: payload.schemaIdentity,
        spaceId,
        clientId
      }).execute(payload).pipe(Effect.flip, Effect.provide(context))
      assert.strictEqual(error._tag, "ProtocolInvalid")
    }))

  it.effect("runs with the SQL backed Cluster Workflow engine", () =>
    Effect.gen(function*() {
      const serverContext = yield* Layer.build(serverLayer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const replicaDatabase = database()
      const runnerDatabase = database()
      const runner = SingleRunner.layer({ runnerStorage: "sql" }).pipe(
        Layer.provide(runnerDatabase)
      )
      const workflowEngine = ClusterWorkflowEngine.layer.pipe(
        Layer.provideMerge(runner)
      )
      const replicaLayer = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "1 millis"
      }).pipe(
        Layer.provide(Domain.handlers),
        Layer.provide(replicaDatabase),
        Layer.provide(directSync(server)),
        Layer.provideMerge(workflowEngine)
      )
      const context = yield* Layer.build(replicaLayer)
      const replica = Context.get(context, Replica.Replica)
      const local = Context.get(context, LocalStore.Store)

      const mutation = yield* replica.mutate(Domain.PutTodo, Domain.todo("cluster"))
      const requested = (yield* local.reconciliationGenerations).requested
      const payload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        generation: requested
      })
      yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(Effect.provide(context))

      let generations = yield* local.reconciliationGenerations
      while (generations.completed < generations.requested) {
        const nextPayload = ReconciliationWorkflow.Payload.make({
          schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
          spaceId,
          clientId,
          generation: generations.requested
        })
        yield* ReconciliationWorkflow.make(nextPayload).execute(nextPayload).pipe(Effect.provide(context))
        generations = yield* local.reconciliationGenerations
      }

      assert.strictEqual(yield* local.pendingCount, 0)
      assert.strictEqual(Option.getOrThrow(yield* local.receipt(mutation.envelope.mutationId))._tag, "Accepted")
      assert.strictEqual(generations.completed, generations.requested)
    }))

  it.effect("uses the current handler after registering a new schema on one Cluster Workflow engine", () =>
    Effect.gen(function*() {
      const replicaDatabase = database()
      const runner = SingleRunner.layer({ runnerStorage: "sql" }).pipe(Layer.provide(replicaDatabase))
      const engineContext = yield* Layer.build(
        ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(runner))
      )
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)

      const register = (
        definition: Definition.Any,
        registrationEngine: WorkflowEngine.WorkflowEngine["Service"] = engine
      ) =>
        Effect.gen(function*() {
          const localContext = yield* Layer.build(
            LocalStore.layer({ ...clientHistory, definition, spaceId, clientId }).pipe(
              Layer.provide(MutationRuntime.layer(definition).pipe(Layer.provide(Domain.handlers))),
              Layer.provide(database())
            )
          )
          const local = Context.get(localContext, LocalStore.Store)
          const remote = SyncEngine.SyncEngine.of({
            submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
            pull: () => Effect.succeed({ entries: [], hasMore: false }),
            bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
            watch: () => Stream.never
          })
          const reconciliationContext = yield* Layer.build(
            Reconciler.layerOnePass({ definition, spaceId }).pipe(
              Layer.provide(Layer.succeed(LocalStore.Store, local)),
              Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
            )
          )
          const reconciliation = Context.get(reconciliationContext, Reconciler.Reconciliation)
          yield* Layer.build(
            ReconciliationWorkflow.layerRegistration({ definition, spaceId, clientId }).pipe(
              Layer.provide(Layer.succeed(LocalStore.Store, local)),
              Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
              Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, registrationEngine))
            )
          )
          return local
        })

      const legacy = yield* register(Domain.definition)
      const registrationEntered = yield* Deferred.make<void>()
      const interruptedEngine = new Proxy(engine, {
        get: (target, property, receiver) =>
          property === "register"
            ? () => Deferred.succeed(registrationEntered, undefined).pipe(Effect.andThen(Effect.never))
            : Reflect.get(target, property, receiver)
      })
      const interruptedRegistration = yield* Effect.forkChild(register(definitionV2, interruptedEngine))
      yield* Deferred.await(registrationEntered)
      yield* Fiber.interrupt(interruptedRegistration)

      const retainedGeneration = yield* legacy.requestReconciliation
      const retainedPayload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        generation: retainedGeneration
      })
      yield* ReconciliationWorkflow.make(retainedPayload).execute(retainedPayload).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
      )
      assert.deepStrictEqual(yield* legacy.reconciliationGenerations, {
        requested: retainedGeneration,
        completed: retainedGeneration
      })

      const current = yield* register(definitionV2)
      const generation = yield* current.requestReconciliation
      const payload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${definitionV2.schemaIdentity.version}:${definitionV2.schemaIdentity.hash}`,
        spaceId,
        clientId,
        generation
      })

      yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
      )
      assert.deepStrictEqual(yield* current.reconciliationGenerations, {
        requested: generation,
        completed: generation
      })

      const legacyGeneration = yield* legacy.requestReconciliation
      const legacyPayload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        generation: legacyGeneration
      })
      const error = yield* ReconciliationWorkflow.make(legacyPayload).execute(legacyPayload).pipe(
        Effect.flip,
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
      )
      assert.strictEqual(error._tag, "StaleSchema")
    }))

  it.effect("keeps registrations for distinct replicas isolated in one workflow engine", () =>
    Effect.gen(function*() {
      const firstPulls = yield* Ref.make(0)
      const secondPulls = yield* Ref.make(0)
      const firstPulled = yield* Deferred.make<void>()
      const secondPulled = yield* Deferred.make<void>()
      const secondClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
      const engineContext = yield* Layer.build(WorkflowEngine.layerMemory)
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)
      const register = (
        registeredClientId: Identity.ClientId,
        pulls: Ref.Ref<number>,
        pulled: Deferred.Deferred<void>
      ) =>
        Effect.gen(function*() {
          const localContext = yield* Layer.build(
            LocalStore.layer({
              ...clientHistory,
              definition: Domain.definition,
              spaceId,
              clientId: registeredClientId
            }).pipe(
              Layer.provide(runtime),
              Layer.provide(database())
            )
          )
          const local = Context.get(localContext, LocalStore.Store)
          const remote = SyncEngine.SyncEngine.of({
            submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
            pull: () =>
              Ref.update(pulls, (count) => count + 1).pipe(
                Effect.andThen(Deferred.succeed(pulled, undefined)),
                Effect.as({ entries: [], hasMore: false })
              ),
            bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
            watch: () => Stream.never
          })
          const reconciliationContext = yield* Layer.build(
            Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
              Layer.provide(Layer.succeed(LocalStore.Store, local)),
              Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
            )
          )
          const reconciliation = Context.get(reconciliationContext, Reconciler.Reconciliation)
          yield* Layer.build(
            ReconciliationWorkflow.layerRegistration({
              definition: Domain.definition,
              spaceId,
              clientId: registeredClientId
            }).pipe(
              Layer.provide(Layer.succeed(LocalStore.Store, local)),
              Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
              Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, engine))
            )
          )
        })

      yield* register(clientId, firstPulls, firstPulled)
      yield* register(secondClientId, secondPulls, secondPulled)
      const firstPayload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        generation: 1
      })
      const secondPayload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId: secondClientId,
        generation: 1
      })

      yield* ReconciliationWorkflow.make(firstPayload).execute(firstPayload, { discard: true }).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
      )
      yield* ReconciliationWorkflow.make(secondPayload).execute(secondPayload, { discard: true }).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
      )
      yield* Deferred.await(firstPulled)
      yield* Deferred.await(secondPulled)

      assert.isAtLeast(yield* Ref.get(firstPulls), 1)
      assert.isAtLeast(yield* Ref.get(secondPulls), 1)
    }))

  it.effect("stops a permanently failing workflow after the configured attempt bound", () =>
    Effect.gen(function*() {
      const attempts = yield* Ref.make(0)
      const localContext = yield* Layer.build(
        LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
      )
      const local = Context.get(localContext, LocalStore.Store)
      const remote = SyncEngine.SyncEngine.of({
        submit: () =>
          Ref.update(attempts, (count) => count + 1).pipe(
            Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable()))
          ),
        pull: () => Effect.succeed({ entries: [], hasMore: false }),
        bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        watch: () => Stream.never
      })
      const reconciliationContext = yield* Layer.build(
        Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
          Layer.provide(Layer.succeed(LocalStore.Store, local)),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
        )
      )
      const reconciliation = Context.get(reconciliationContext, Reconciler.Reconciliation)
      const engineContext = yield* Layer.build(WorkflowEngine.layerMemory)
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)
      yield* Layer.build(
        ReconciliationWorkflow.layerRegistration({
          definition: Domain.definition,
          spaceId,
          clientId,
          retryDelay: "1 millis",
          maximumRetryDelay: "1 millis",
          maximumAttempts: 1
        }).pipe(
          Layer.provide(Layer.succeed(LocalStore.Store, local)),
          Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
          Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, engine))
        )
      )
      yield* local.mutate(Domain.PutTodo, Domain.todo("permanent-failure"))
      const generations = yield* local.reconciliationGenerations
      const payload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        generation: generations.requested
      })

      const error = yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(
        Effect.flip,
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine)
      )

      assert.strictEqual(error._tag, "ServerUnavailable")
      assert.strictEqual(yield* Ref.get(attempts), 1)
      assert.deepStrictEqual(yield* local.reconciliationGenerations, {
        requested: generations.requested,
        completed: 0
      })
    }))
})
