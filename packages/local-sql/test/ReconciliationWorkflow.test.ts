import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import type * as Schedule from "effect/Schedule"
import type * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as ClusterWorkflowEngine from "effect/unstable/cluster/ClusterWorkflowEngine"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as Workflow from "effect/unstable/workflow/Workflow"
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

const expectedFailure = <A, E extends { readonly _tag: string },>(exit: Exit.Exit<A, E>) => {
  assert.isTrue(Exit.isFailure(exit))
  if (Exit.isFailure(exit)) return Cause.findErrorOption(exit.cause)
  return Option.none<E>()
}

const database = () => {
  return Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
}

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

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
  defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
  scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
  retainedReceipts: 256,
  settlementCapacity: 64,
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
  maximumWatchersPerSpace: 1_024,
  readAuthorizationRefreshInterval: "30 seconds" as const,
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  migration
}

const layerServer = ServerStore.layerTrusted({ ...serverHistory, definition: Domain.definition }).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(database())
)

const serverLayerFor = (definition: Definition.Any) =>
  ServerStore.layerTrusted({ ...serverHistory, definition }).pipe(
    Layer.provide(MutationRuntime.layer(definition).pipe(Layer.provide(Domain.layerHandlers))),
    Layer.provide(database())
  )

const directSync = (server: ServerStore.Service) =>
  pipe(
    SyncEngine.SyncEngine.of({
      waitForCredentialChange: () => Effect.never,
      submit: server.submit,
      discard: (request) => server.discard(request, null),
      pull: server.pull,
      bootstrap: server.bootstrap,
      watch: server.watch
    }),
    Layer.succeed(SyncEngine.SyncEngine)
  )

describe("reconciliation workflow", () => {
  it.effect(
    "rejects a maximum retry delay below the initial delay",
    Effect.fnUntraced(function*() {
      const serverContext = yield* Layer.build(layerServer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const layerInvalid = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "2 seconds",
        maximumRetryDelay: "1 second"
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provideMerge(database()),
        Layer.provide(directSync(server)),
        Layer.provideMerge(WorkflowEngine.layerMemory)
      )
      const result = yield* Layer.build(layerInvalid).pipe(Effect.exit)
      const error = expectedFailure(result).pipe(Option.getOrThrow)
      assert.strictEqual(error._tag, "InvalidConfiguration")
      if (error._tag === "InvalidConfiguration") {
        assert.strictEqual(error.option, "maximumRetryDelay")
      }
    })
  )

  it.effect(
    "runs finite generation keyed reconciliation over the durable SQLite outbox",
    Effect.fnUntraced(function*() {
      const serverContext = yield* Layer.build(layerServer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const layerWorkflowEngine = WorkflowEngine.layerMemory
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "1 millis"
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provideMerge(database()),
        Layer.provide(directSync(server)),
        Layer.provideMerge(layerWorkflowEngine)
      )
      const context = yield* Layer.build(layerReplica)

      const space = yield* Context.get(context, Replica.Replica).space(spaceId)

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

      assert.strictEqual(
        Option.getOrThrow(yield* space.receipt(Domain.PutTodo, first.envelope.mutationId))._tag,
        "Accepted"
      )
      assert.strictEqual(
        Option.getOrThrow(yield* space.receipt(Domain.PutTodo, second.envelope.mutationId))._tag,
        "Accepted"
      )
      pipe(
        Option.getOrThrow(yield* space.get(Domain.Todo, "2")),
        (actual) => assert.deepStrictEqual(actual, Domain.todo("2"))
      )
      pipe(Object.keys(payload).toSorted(), (keys) =>
        assert.deepStrictEqual(keys, [
          "clientId",
          "generation",
          "membershipIncarnation",
          "schemaIdentity",
          "scope",
          "scopeGeneration",
          "spaceId"
        ]))
    })
  )

  it.effect(
    "rejects a workflow retained from a membership that was left and rejoined",
    Effect.fnUntraced(function*() {
      const serverContext = yield* Layer.build(layerServer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "1 millis"
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(database()),
        Layer.provide(directSync(server)),
        Layer.provideMerge(WorkflowEngine.layerMemory)
      )
      const context = yield* Layer.build(layerReplica)
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

      const result = yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(
        Effect.provide(context),
        Effect.exit
      )
      const error = expectedFailure(result).pipe(Option.getOrThrow)
      assert.strictEqual(error._tag, "SpaceUnavailable")
    })
  )

  it.effect(
    "interrupts the workflow generation that is active during leave",
    Effect.fnUntraced(function*() {
      const pullEntered = yield* Deferred.make<void>()
      const pullInterrupted = yield* Deferred.make<void>()
      const layerBlockedSync = pipe(
        SyncEngine.SyncEngine.of({
          waitForCredentialChange: () => Effect.never,
          discard: () => Effect.die("unexpected discard"),
          submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
          pull: () =>
            Deferred.succeed(pullEntered, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.onInterrupt(() => Deferred.succeed(pullInterrupted, undefined))
            ),
          bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
          watch: () => Stream.never
        }),
        Layer.succeed(SyncEngine.SyncEngine)
      )
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "1 millis"
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(database()),
        Layer.provide(layerBlockedSync),
        Layer.provideMerge(WorkflowEngine.layerMemory)
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const space = yield* replica.space(spaceId)
      yield* space.activate
      yield* Deferred.await(pullEntered)
      yield* space.mutate(Domain.PutTodo, Domain.todo("next-generation"))

      yield* replica.leave(spaceId)

      assert.isTrue(yield* Deferred.isDone(pullInterrupted))
    })
  )

  it.effect(
    "does not resubscribe a transient watch while authentication is paused",
    Effect.fnUntraced(function*() {
      const subscriptions = yield* Ref.make(0)
      const watchSubscribed = yield* Deferred.make<void>()
      const releaseWatch = yield* Deferred.make<void>()
      const credentialWaitStarted = yield* Deferred.make<void>()
      const layerRemote = pipe(
        SyncEngine.SyncEngine.of({
          waitForCredentialChange: () =>
            Deferred.succeed(credentialWaitStarted, undefined).pipe(Effect.andThen(Effect.never)),
          discard: () => Effect.die("unexpected discard"),
          submit: () => Effect.die("unexpected submit"),
          pull: () => Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 0 })),
          bootstrap: () => Effect.die("unexpected bootstrap"),
          watch: () =>
            Ref.updateAndGet(subscriptions, (count) => count + 1).pipe(
              Effect.flatMap((count) => {
                if (count === 1) {
                  return Deferred.succeed(watchSubscribed, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseWatch)),
                    Effect.as(Stream.fail(new ReplicaError.ServerUnavailable()))
                  )
                }
                return Effect.succeed(Stream.never)
              }),
              Stream.unwrap
            )
        }),
        Layer.succeed(SyncEngine.SyncEngine)
      )
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "1 second",
        maximumRetryDelay: "1 second"
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provideMerge(database()),
        Layer.provide(layerRemote),
        Layer.provideMerge(WorkflowEngine.layerMemory)
      )
      const context = yield* Layer.build(layerReplica)
      const space = yield* Context.get(context, Replica.Replica).space(spaceId)
      yield* space.activate
      yield* Deferred.await(watchSubscribed)
      yield* Deferred.await(credentialWaitStarted)
      assert.strictEqual((yield* space.status)._tag, "NeedsAuthentication")

      yield* Deferred.succeed(releaseWatch, undefined)
      yield* TestClock.adjust("1 second")

      assert.strictEqual(yield* Ref.get(subscriptions), 1)
      assert.strictEqual((yield* space.status)._tag, "NeedsAuthentication")
    })
  )

  it.effect(
    "runs a later generation after a permanent workflow failure",
    Effect.fnUntraced(function*() {
      const denied = yield* Ref.make(true)
      const recoveredPull = yield* Deferred.make<void>()
      const firstExecutionFinished = yield* Deferred.make<void>()
      const secondExecutionStarted = yield* Deferred.make<void>()
      const executions = yield* Ref.make(0)
      const serverContext = yield* Layer.build(layerServer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const layerRemote = pipe(
        SyncEngine.SyncEngine.of({
          waitForCredentialChange: () => Effect.never,
          discard: (request) => server.discard(request, null),
          submit: server.submit,
          pull: (request) =>
            Ref.get(denied).pipe(
              Effect.flatMap((isDenied) => {
                if (isDenied) return Effect.fail(new ReplicaError.AuthorizationDenied({ reason: "denied" }))
                return Deferred.succeed(recoveredPull, undefined).pipe(Effect.andThen(server.pull(request)))
              })
            ),
          bootstrap: server.bootstrap,
          watch: () => Stream.never
        }),
        Layer.succeed(SyncEngine.SyncEngine)
      )
      const engineContext = yield* Layer.build(WorkflowEngine.layerMemory)
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)
      const observedEngineService = new Proxy(engine, {
        get: (target, property, receiver) => {
          if (property !== "execute") return Reflect.get(target, property, receiver)
          return <
            Name extends string,
            Payload extends Workflow.AnyStructSchema,
            Success extends Schema.Top,
            Error extends Schema.Top & { readonly Type: { readonly _tag: string } },
            const Discard extends boolean = false,
          >(
            workflow: Workflow.Workflow<Name, Payload, Success, Error>,
            options: {
              readonly executionId: string
              readonly payload: Payload["Type"]
              readonly discard?: Discard | undefined
              readonly suspendedRetrySchedule?: Schedule.Schedule<unknown> | undefined
            }
          ) =>
            Ref.updateAndGet(executions, (count) => count + 1).pipe(
              Effect.tap((count) => {
                if (count === 2) return Deferred.succeed(secondExecutionStarted, undefined)
                return Effect.void
              }),
              Effect.flatMap((count) => {
                return target.execute(workflow, options).pipe(
                  Effect.ensuring(Effect.suspend(() => {
                    if (count === 1) return Deferred.succeed(firstExecutionFinished, undefined)
                    return Effect.void
                  }))
                )
              })
            )
        }
      })
      const layerObservedEngine = Layer.succeed(WorkflowEngine.WorkflowEngine, observedEngineService)
      const layerReplicaDatabase = database()
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerReplicaDatabase),
        Layer.provide(layerRemote),
        Layer.provideMerge(layerObservedEngine)
      )
      const context = yield* Layer.merge(layerReplica, layerReplicaDatabase).pipe(Layer.build)
      const space = yield* Context.get(context, Replica.Replica).space(spaceId)
      yield* space.activate
      const reactivity = Context.get(context, Reactivity.Reactivity)
      const online = yield* reactivity.stream([`effect-local:space:${spaceId}:status`], space.status).pipe(
        Stream.filter((status) => status._tag === "Online"),
        Stream.runHead,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(firstExecutionFinished)

      yield* Ref.set(denied, false)
      yield* space.mutate(Domain.PutTodo, Domain.todo("recovered"))
      yield* Deferred.await(secondExecutionStarted)
      yield* Deferred.await(recoveredPull)
      assert.isTrue(Option.isSome(yield* Fiber.join(online)))
    })
  )

  it.effect(
    "rejects a workflow handle addressed to another replica",
    Effect.fnUntraced(function*() {
      const serverContext = yield* Layer.build(layerServer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(database()),
        Layer.provide(directSync(server)),
        Layer.provideMerge(WorkflowEngine.layerMemory)
      )
      const context = yield* Layer.build(layerReplica)

      const registered = yield* Context.get(context, Replica.Replica).space(spaceId)
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
      const result = yield* ReconciliationWorkflow.make({
        schemaIdentity: payload.schemaIdentity,
        spaceId,
        clientId,
        membershipIncarnation: pending.envelope.membershipIncarnation
      }).execute(payload).pipe(Effect.provide(context), Effect.exit)
      const error = expectedFailure(result).pipe(Option.getOrThrow)
      assert.strictEqual(error._tag, "ProtocolInvalid")
    })
  )

  it.effect(
    "runs with the SQL backed Cluster Workflow engine",
    Effect.fnUntraced(function*() {
      const serverContext = yield* Layer.build(layerServer)
      const server = Context.get(serverContext, ServerStore.ServerStore)

      const layerRunner = SingleRunner.layer({
        runnerStorage: "sql",
        shardingConfig: { entityTerminationTimeout: 0 }
      }).pipe(
        Layer.provide(database())
      )
      const layerWorkflowEngine = ClusterWorkflowEngine.layer.pipe(
        Layer.provideMerge(layerRunner)
      )
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "1 millis"
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(database()),
        Layer.provide(directSync(server)),
        Layer.provideMerge(layerWorkflowEngine)
      )
      const context = yield* Layer.build(layerReplica)
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

      const storedReceipt = yield* space.receipt(Domain.PutTodo, mutation.envelope.mutationId)
      assert.strictEqual(Option.getOrThrow(storedReceipt)._tag, "Accepted")
      yield* replica.leave(spaceId)
    })
  )

  it.effect(
    "uses the current handler after registering a new schema on one Cluster Workflow engine",
    Effect.fnUntraced(function*() {
      const databaseContext = yield* database().pipe(Layer.build)

      const layerSql = Layer.succeed(SqlClient.SqlClient, Context.get(databaseContext, SqlClient.SqlClient))

      const layerCrypto = Layer.succeed(Crypto.Crypto, Context.get(databaseContext, Crypto.Crypto))

      const layerReactivity = Layer.succeed(Reactivity.Reactivity, Context.get(databaseContext, Reactivity.Reactivity))

      const layerQueryReactivity = Layer.succeed(
        QueryReactivity.QueryReactivity,
        Context.get(databaseContext, QueryReactivity.QueryReactivity)
      )
      const layerReplicaDatabase = Layer.mergeAll(layerSql, layerCrypto, layerReactivity, layerQueryReactivity)
      const layerRunner = SingleRunner.layer({ runnerStorage: "sql" }).pipe(Layer.provide(layerReplicaDatabase))
      const engineContext = yield* ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(layerRunner), Layer.build)
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)

      const register = Effect.fnUntraced(function*(
        definition: Definition.Any,
        registrationEngine: WorkflowEngine.WorkflowEngine["Service"] = engine
      ) {
        const localContext = yield* LocalStore.layer({
          ...clientHistory,
          definition: Domain.definition,
          spaceId,
          clientId
        }).pipe(
          Layer.provide(layerRuntime),
          Layer.provide(layerReplicaDatabase),
          Layer.build
        )
        const local = Context.get(localContext, LocalStore.Store)
        const serverContext = yield* serverLayerFor(definition).pipe(Layer.build)
        const server = Context.get(serverContext, ServerStore.ServerStore)
        let remote = SyncEngine.SyncEngine.of({
          waitForCredentialChange: () => Effect.never,
          discard: (request) => server.discard(request, null),
          submit: server.submit,
          pull: server.pull,
          bootstrap: server.bootstrap,
          watch: server.watch
        })
        if (definition.hash !== Domain.definition.hash) {
          remote = SyncEngine.SyncEngine.of({
            waitForCredentialChange: () => Effect.never,
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
        const reconciliationContext = yield* Reconciler.layerOnePass({ definition, spaceId }).pipe(
          Layer.provide(Layer.succeed(LocalStore.Store, local)),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.build
        )
        const reconciliation = Context.get(reconciliationContext, Reconciler.Reconciliation)
        yield* ReconciliationWorkflow.layerRegistration({ definition, spaceId, clientId }).pipe(
          Layer.provide(Layer.succeed(LocalStore.Store, local)),
          Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
          Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, registrationEngine)),
          Layer.build
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
      const interruptedRegistration = yield* register(definitionV2, interruptedEngine).pipe(Effect.forkChild)
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
      const result = yield* ReconciliationWorkflow.make(legacyPayload).execute(legacyPayload).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        Effect.exit
      )
      const error = expectedFailure(result).pipe(Option.getOrThrow)
      assert.strictEqual(error._tag, "StaleSchema")
    })
  )

  it.effect(
    "keeps registrations for distinct replicas isolated in one workflow engine",
    Effect.fnUntraced(function*() {
      const firstPulls = yield* Ref.make(0)
      const secondPulls = yield* Ref.make(0)
      const firstPulled = yield* Deferred.make<void>()
      const secondPulled = yield* Deferred.make<void>()
      const secondClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
      const engineContext = yield* Layer.build(WorkflowEngine.layerMemory)
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)
      const serverContext = yield* Layer.build(layerServer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const register = Effect.fnUntraced(function*(
        registeredClientId: Identity.ClientId,
        pulls: Ref.Ref<number>,
        pulled: Deferred.Deferred<void>
      ) {
        const localContext = yield* LocalStore.layer({
          ...clientHistory,
          definition: Domain.definition,
          spaceId,
          clientId: registeredClientId
        }).pipe(
          Layer.provide(layerRuntime),
          Layer.provide(database()),
          Layer.build
        )
        const local = Context.get(localContext, LocalStore.Store)
        const remote = SyncEngine.SyncEngine.of({
          waitForCredentialChange: () => Effect.never,
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
        const reconciliationContext = yield* Reconciler.layerOnePass({
          definition: Domain.definition,
          spaceId
        }).pipe(
          Layer.provide(Layer.succeed(LocalStore.Store, local)),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.build
        )
        const reconciliation = Context.get(reconciliationContext, Reconciler.Reconciliation)
        yield* ReconciliationWorkflow.layerRegistration({
          definition: Domain.definition,
          spaceId,
          clientId: registeredClientId
        }).pipe(
          Layer.provide(Layer.succeed(LocalStore.Store, local)),
          Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
          Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, engine)),
          Layer.build
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
    })
  )

  it.effect(
    "stops a permanently failing workflow after the configured attempt bound",
    Effect.fnUntraced(function*() {
      const attempts = yield* Ref.make(0)
      const localContext = yield* LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId
      }).pipe(
        Layer.provide(layerRuntime),
        Layer.provide(database()),
        Layer.build
      )
      const local = Context.get(localContext, LocalStore.Store)
      const serverContext = yield* Layer.build(layerServer)
      const server = Context.get(serverContext, ServerStore.ServerStore)
      const remote = SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        discard: () => Effect.die("unexpected discard"),
        submit: () =>
          Ref.update(attempts, (count) => count + 1).pipe(
            Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable()))
          ),
        pull: server.pull,
        bootstrap: server.bootstrap,
        watch: () => Stream.never
      })
      const reconciliationContext = yield* Reconciler.layerOnePass({
        definition: Domain.definition,
        spaceId
      }).pipe(
        Layer.provide(Layer.succeed(LocalStore.Store, local)),
        Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
        Layer.build
      )
      const reconciliation = Context.get(reconciliationContext, Reconciler.Reconciliation)
      const engineContext = yield* Layer.build(WorkflowEngine.layerMemory)
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)
      yield* ReconciliationWorkflow.layerRegistration({
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "1 millis",
        maximumRetryDelay: "1 millis",
        maximumAttempts: 1
      }).pipe(
        Layer.provide(Layer.succeed(LocalStore.Store, local)),
        Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
        Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, engine)),
        Layer.build
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

      const result = yield* ReconciliationWorkflow.make(payload).execute(payload).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        Effect.exit
      )

      const error = expectedFailure(result).pipe(Option.getOrThrow)
      assert.strictEqual(error._tag, "ServerUnavailable")
      assert.strictEqual(yield* Ref.get(attempts), 1)
      assert.deepStrictEqual(yield* local.reconciliationGenerations, {
        requested: generations.requested,
        completed: 0
      })
    })
  )

  it.effect(
    "does not retry a stale schema workflow activity",
    Effect.fnUntraced(function*() {
      const attempts = yield* Ref.make(0)
      const attempted = yield* Deferred.make<void>()
      const localContext = yield* LocalStore.layer({
        ...clientHistory,
        definition: Domain.definition,
        spaceId,
        clientId
      }).pipe(
        Layer.provide(layerRuntime),
        Layer.provide(database()),
        Layer.build
      )
      const local = Context.get(localContext, LocalStore.Store)
      const stale = new ReplicaError.StaleSchema({
        expectedVersion: 2,
        expectedHash: "expected",
        actualVersion: 1,
        actualHash: "actual"
      })
      const remote = SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
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
      const reconciliationContext = yield* Reconciler.layerOnePass({
        definition: Domain.definition,
        spaceId
      }).pipe(
        Layer.provide(Layer.succeed(LocalStore.Store, local)),
        Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
        Layer.build
      )
      const reconciliation = Context.get(reconciliationContext, Reconciler.Reconciliation)
      const engineContext = yield* Layer.build(WorkflowEngine.layerMemory)
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)
      yield* ReconciliationWorkflow.layerRegistration({
        definition: Domain.definition,
        spaceId,
        clientId,
        retryDelay: "1 millis",
        maximumRetryDelay: "1 millis",
        maximumAttempts: 3
      }).pipe(
        Layer.provide(Layer.succeed(LocalStore.Store, local)),
        Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
        Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, engine)),
        Layer.build
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
        Effect.as({ _tag: "UnexpectedStaleSchemaWorkflowSuccess" as const }),
        Effect.flip,
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(attempted)
      yield* TestClock.adjust("5 millis")
      const error = yield* Fiber.join(fiber)
      assert.strictEqual(error._tag, "StaleSchema")
      assert.strictEqual(yield* Ref.get(attempts), 1)
    })
  )
})
