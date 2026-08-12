import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as ClusterWorkflowEngine from "effect/unstable/cluster/ClusterWorkflowEngine"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ReconciliationWorkflow from "../src/ReconciliationWorkflow.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const scopeGeneration = Identity.ReplicationScopeGeneration.make(1)

const database = () =>
  Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
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
  scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration
}
const serverHistory = {
  readAuthorizationRefreshInterval: "1 second" as const,
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

const serverLayerFor = (definition: Definition.Any) =>
  ServerStore.layerTrusted({ ...serverHistory, definition }).pipe(
    Layer.provide(MutationRuntime.layer(definition).pipe(Layer.provide(Domain.handlers))),
    Layer.provide(database())
  )

const directSync = (server: ServerStore.Service) =>
  Layer.succeed(
    SyncEngine.SyncEngine,
    SyncEngine.SyncEngine.of({
      submit: server.submit,
      discard: (request) => server.discard(request, null),
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
      const space = yield* replica.space(spaceId)

      const first = yield* space.mutate(Domain.PutTodo, Domain.todo("1"))
      const second = yield* space.mutate(Domain.PutTodo, Domain.todo("2"))
      const requested = 2
      const payload = ReconciliationWorkflow.Payload.make({
        scope: clientHistory.scope,
        scopeGeneration,
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        membershipIncarnation: first.envelope.membershipIncarnation,
        generation: requested
      })

      const execution = yield* ReconciliationWorkflow.start(payload).pipe(Effect.provide(context))
      assert.strictEqual(execution.executionId, yield* ReconciliationWorkflow.executionId(payload))
      yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(Effect.provide(context))

      assert.strictEqual(Option.getOrThrow(yield* space.receipt(first.envelope.mutationId))._tag, "Accepted")
      assert.strictEqual(Option.getOrThrow(yield* space.receipt(second.envelope.mutationId))._tag, "Accepted")
      assert.deepStrictEqual(Option.getOrThrow(yield* space.get(Domain.Todo, "2")), Domain.todo("2"))
      assert.deepStrictEqual(Object.keys(payload).toSorted(), [
        "clientId",
        "generation",
        "membershipIncarnation",
        "schemaIdentity",
        "scope",
        "scopeGeneration",
        "spaceId"
      ])
    }))

  it.effect("rejects a workflow retained from a membership that was left and rejoined", () =>
    Effect.gen(function*() {
      const serverContext = yield* Layer.build(serverLayer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
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
        Layer.provideMerge(WorkflowEngine.layerMemory)
      )
      const context = yield* Layer.build(replicaLayer)
      const replica = Context.get(context, Replica.Replica)
      const original = yield* replica.space(spaceId)
      const mutation = yield* original.mutate(Domain.PutTodo, Domain.todo("retained-workflow"))
      const payload = ReconciliationWorkflow.Payload.make({
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        membershipIncarnation: mutation.envelope.membershipIncarnation,
        scope: clientHistory.scope,
        scopeGeneration,
        generation: 1_000
      })

      yield* replica.leave(spaceId)
      yield* replica.join(spaceId)

      const error = yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(
        Effect.flip,
        Effect.provide(context)
      )
      assert.strictEqual(error._tag, "SpaceUnavailable")
    }))

  it.effect("interrupts the workflow generation that is active during leave", () =>
    Effect.gen(function*() {
      const pullEntered = yield* Deferred.make<void>()
      const pullInterrupted = yield* Deferred.make<void>()
      const blockedSync = Layer.succeed(
        SyncEngine.SyncEngine,
        SyncEngine.SyncEngine.of({
          discard: () => Effect.die("unexpected discard"),
          submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
          pull: () =>
            Deferred.succeed(pullEntered, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() => Deferred.succeed(pullInterrupted, undefined))
            ),
          bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
          watch: () => Stream.never
        })
      )
      const replicaLayer = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "1 millis"
      }).pipe(
        Layer.provide(Domain.handlers),
        Layer.provide(database()),
        Layer.provide(blockedSync),
        Layer.provideMerge(WorkflowEngine.layerMemory)
      )
      const context = yield* Layer.build(replicaLayer)
      const replica = Context.get(context, Replica.Replica)
      const space = yield* replica.space(spaceId)
      yield* Deferred.await(pullEntered)
      yield* space.mutate(Domain.PutTodo, Domain.todo("next-generation"))

      yield* replica.leave(spaceId)

      assert.isTrue(yield* Deferred.isDone(pullInterrupted))
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
      const replica = Context.get(context, Replica.Replica)
      const registered = yield* replica.space(spaceId)
      const pending = yield* registered.mutate(Domain.PutTodo, Domain.todo("identity"))
      const payload = ReconciliationWorkflow.Payload.make({
        scope: clientHistory.scope,
        scopeGeneration,
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId: Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002"),
        clientId,
        membershipIncarnation: pending.envelope.membershipIncarnation,
        generation: 2
      })
      const error = yield* ReconciliationWorkflow.make({
        schemaIdentity: payload.schemaIdentity,
        spaceId,
        clientId,
        membershipIncarnation: pending.envelope.membershipIncarnation
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
      const space = yield* replica.space(spaceId)

      const mutation = yield* space.mutate(Domain.PutTodo, Domain.todo("cluster"))
      const requested = 2
      const payload = ReconciliationWorkflow.Payload.make({
        scope: clientHistory.scope,
        scopeGeneration,
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        membershipIncarnation: mutation.envelope.membershipIncarnation,
        generation: requested
      })
      yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(Effect.provide(context))

      const storedReceipt = yield* space.receipt(mutation.envelope.mutationId)
      assert.strictEqual(Option.getOrThrow(storedReceipt)._tag, "Accepted")
      yield* replica.leave(spaceId)
    }))

  it.effect("uses the current handler after registering a new schema on one Cluster Workflow engine", () =>
    Effect.gen(function*() {
      const databaseContext = yield* Layer.build(database())
      const replicaDatabase = Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, Context.get(databaseContext, SqlClient.SqlClient)),
        Layer.succeed(Crypto.Crypto, Context.get(databaseContext, Crypto.Crypto)),
        Layer.succeed(Reactivity.Reactivity, Context.get(databaseContext, Reactivity.Reactivity)),
        Layer.succeed(QueryReactivity.QueryReactivity, Context.get(databaseContext, QueryReactivity.QueryReactivity))
      )
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
            LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
              Layer.provide(runtime),
              Layer.provide(replicaDatabase)
            )
          )
          const local = Context.get(localContext, LocalStore.Store)
          const serverContext = yield* Layer.build(serverLayerFor(definition))
          const server = Context.get(serverContext, ServerStore.ServerStore)
          let remote = SyncEngine.SyncEngine.of({
            discard: (request) => server.discard(request, null),
            submit: server.submit,
            pull: server.pull,
            bootstrap: server.bootstrap,
            watch: server.watch
          })
          if (definition.hash !== Domain.definition.hash) {
            remote = SyncEngine.SyncEngine.of({
              discard: (request) => server.discard(request, null),
              submit: server.submit,
              pull: (request) => {
                if (request.cursor === null) return server.pull(request)
                const cursor = request.cursor
                return Protocol.viewChangesDigest([]).pipe(
                  Effect.map((digest) =>
                    Protocol.PullPage.make({
                      scopeGeneration: request.scopeGeneration,
                      cursor: Protocol.ReplicationCursor.make({
                        viewId: cursor.viewId,
                        revision: Identity.ReplicationViewRevision.make(cursor.revision + 1)
                      }),
                      serverSequence: Identity.ServerSequence.make(0),
                      changes: [],
                      contentBytes: Protocol.encodedBytes([]),
                      digest,
                      hasMore: false,
                      serverSchema: definition.schemaIdentity
                    })
                  ),
                  Effect.provide(NodeCrypto.layer)
                )
              },
              bootstrap: server.bootstrap,
              watch: server.watch
            })
          }
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
        get: (target, property, receiver) => {
          if (property === "register") {
            return () => Deferred.succeed(registrationEntered, undefined).pipe(Effect.andThen(Effect.never))
          }
          return Reflect.get(target, property, receiver)
        }
      })
      const interruptedRegistration = yield* Effect.forkChild(register(definitionV2, interruptedEngine))
      yield* Deferred.await(registrationEntered)
      yield* Fiber.interrupt(interruptedRegistration)

      const retainedGeneration = yield* legacy.requestReconciliation
      const retainedPayload = ReconciliationWorkflow.Payload.make({
        scope: clientHistory.scope,
        scopeGeneration,
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        membershipIncarnation: legacy.membershipIncarnation,
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
        scope: clientHistory.scope,
        scopeGeneration,
        schemaIdentity: `${definitionV2.schemaIdentity.version}:${definitionV2.schemaIdentity.hash}`,
        spaceId,
        clientId,
        membershipIncarnation: current.membershipIncarnation,
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
        scope: clientHistory.scope,
        scopeGeneration,
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        membershipIncarnation: legacy.membershipIncarnation,
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
      const serverContext = yield* Layer.build(serverLayer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
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
            discard: () => Effect.die("unexpected discard"),
            submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
            pull: (request) =>
              Ref.update(pulls, (count) => count + 1).pipe(
                Effect.andThen(Deferred.succeed(pulled, undefined)),
                Effect.andThen(server.pull(request))
              ),
            bootstrap: server.bootstrap,
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
          return local
        })

      const firstLocal = yield* register(clientId, firstPulls, firstPulled)
      const secondLocal = yield* register(secondClientId, secondPulls, secondPulled)
      const firstPayload = ReconciliationWorkflow.Payload.make({
        scope: clientHistory.scope,
        scopeGeneration,
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        membershipIncarnation: firstLocal.membershipIncarnation,
        generation: 1
      })
      const secondPayload = ReconciliationWorkflow.Payload.make({
        scope: clientHistory.scope,
        scopeGeneration,
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId: secondClientId,
        membershipIncarnation: secondLocal.membershipIncarnation,
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
      const serverContext = yield* Layer.build(serverLayer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const remote = SyncEngine.SyncEngine.of({
        discard: () => Effect.die("unexpected discard"),
        submit: () =>
          Ref.update(attempts, (count) => count + 1).pipe(
            Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable()))
          ),
        pull: server.pull,
        bootstrap: server.bootstrap,
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
        scope: clientHistory.scope,
        scopeGeneration,
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        membershipIncarnation: local.membershipIncarnation,
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

  it.effect("does not retry a stale schema workflow activity", () =>
    Effect.gen(function*() {
      const attempts = yield* Ref.make(0)
      const attempted = yield* Deferred.make<void>()
      const localContext = yield* Layer.build(
        LocalStore.layer({ ...clientHistory, definition: Domain.definition, spaceId, clientId }).pipe(
          Layer.provide(runtime),
          Layer.provide(database())
        )
      )
      const local = Context.get(localContext, LocalStore.Store)
      const stale = new ReplicaError.StaleSchema({
        expectedVersion: 2,
        expectedHash: "expected",
        actualVersion: 1,
        actualHash: "actual"
      })
      const remote = SyncEngine.SyncEngine.of({
        discard: () => Effect.die("unexpected discard"),
        submit: () => Effect.die("unexpected submit"),
        pull: () =>
          Ref.update(attempts, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(attempted, undefined)),
            Effect.andThen(Effect.fail(stale))
          ),
        bootstrap: () => Effect.fail(stale),
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
          maximumAttempts: 3
        }).pipe(
          Layer.provide(Layer.succeed(LocalStore.Store, local)),
          Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
          Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, engine))
        )
      )
      const generation = yield* local.requestReconciliation
      const payload = ReconciliationWorkflow.Payload.make({
        scope: clientHistory.scope,
        scopeGeneration,
        schemaIdentity: `${Domain.definition.schemaIdentity.version}:${Domain.definition.schemaIdentity.hash}`,
        spaceId,
        clientId,
        membershipIncarnation: local.membershipIncarnation,
        generation
      })
      const fiber = yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(
        Effect.flip,
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(attempted)
      yield* TestClock.adjust("5 millis")
      const error = yield* Fiber.join(fiber)

      assert.strictEqual(error._tag, "StaleSchema")
      assert.strictEqual(yield* Ref.get(attempts), 1)
    }))
})
