import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Scheduler from "effect/Scheduler"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import type * as LocalStore from "../src/LocalStore.js"
import * as Reconciler from "../src/Reconciler.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceA = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const spaceB = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const clientHistory = {
  defaultScope: scope,
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
} satisfies Omit<SqlReplica.Options<typeof Domain.definition>, "definition" | "clientId">

const remoteService = SyncEngine.SyncEngine.of({
  waitForCredentialChange: () => Effect.never,
  transportGeneration: Effect.succeed(0),
  waitForTransportChange: () => Effect.never,
  submitBatch: () => Effect.fail(new ReplicaError.ServerUnavailable()),
  discard: () => Effect.die("unexpected discard"),
  pull: () => Effect.never,
  bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
  watch: () => Stream.never
})
const layerRemote = Layer.succeed(SyncEngine.SyncEngine, remoteService)

const layerLive = SqlReplica.layer({
  ...clientHistory,
  definition: Domain.definition,
  clientId,
  initialSpaces: [spaceB, spaceA]
}).pipe(
  Layer.provide(Domain.layerHandlers),
  Layer.provide(layerRemote),
  Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
  Layer.provide(NodeCrypto.layer),
  Layer.provide(Reactivity.layer)
)

const activeChildFibers = Metric.snapshot.pipe(
  Effect.map((snapshots) => {
    const active = snapshots.find((snapshot) => snapshot.id === "child_fibers_active")
    if (active?.type !== "Gauge") return 0
    return Number(active.state.value)
  })
)

const restartWithStalledBackgroundPull = Effect.fnUntraced(function*(constructor: "layer" | "layerWorkflow") {
  const databaseContext = yield* Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    WorkflowEngine.layerMemory
  ).pipe(Layer.build)
  const backgroundPullStarted = yield* Deferred.make<void>()
  let observePull = false
  const stalledRemote = SyncEngine.SyncEngine.of({
    ...remoteService,
    pull: () => {
      if (!observePull) return Effect.never
      return Deferred.succeed(backgroundPullStarted, undefined).pipe(Effect.andThen(Effect.never))
    }
  })
  const layerServices = Layer.mergeAll(
    Domain.layerHandlers,
    Layer.succeed(SyncEngine.SyncEngine, stalledRemote),
    Layer.succeedContext(databaseContext)
  )
  const options = { ...clientHistory, definition: Domain.definition, clientId }
  let layerReplica = SqlReplica.layer(options).pipe(Layer.provide(layerServices))
  if (constructor === "layerWorkflow") {
    layerReplica = SqlReplica.layerWorkflow(options).pipe(Layer.provide(layerServices))
  }

  const firstScope = yield* Scope.make()
  const first = Context.get(yield* Layer.buildWithScope(layerReplica, firstScope), Replica.Replica)
  yield* (yield* first.join(spaceA)).mutate(Domain.PutTodo, Domain.todo("restart", "retained"))
  yield* Scope.close(firstScope, Exit.void)

  observePull = true
  const second = Context.get(yield* Layer.build(layerReplica), Replica.Replica)
  yield* Deferred.await(backgroundPullStarted)
  return second
})

const settledReadSource = "SELECT receipt_json, settled_pending_json, settled_sequence"
const projectionDirtySource = "SELECT model, model_version, entity_key FROM effect_local_client_projection_dirty"

const probedServices = Effect.fnUntraced(function*(
  probe: (source: string, values: ReadonlyArray<unknown>) => Effect.Effect<never> | undefined
) {
  const databaseContext = yield* Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer
  ).pipe(Layer.build)
  const sql = Context.get(databaseContext, SqlClient.SqlClient)
  const probedSql = new Proxy(sql, {
    apply: (target, thisArg, args: Parameters<typeof sql>) => {
      const rawSource: unknown = args[0]
      let source: string
      if (Array.isArray(rawSource)) source = rawSource.join("")
      else source = String(rawSource)
      const replacement = probe(source, args.slice(1))
      if (replacement === undefined) return Reflect.apply(target, thisArg, args)
      return replacement
    }
  })
  const crypto = Context.get(databaseContext, Crypto.Crypto)
  const reactivity = Context.get(databaseContext, Reactivity.Reactivity)
  const layer = Layer.mergeAll(
    Layer.succeed(SqlClient.SqlClient, probedSql),
    Layer.succeed(Crypto.Crypto, crypto),
    Layer.succeed(Reactivity.Reactivity, reactivity)
  )
  return { layer, sql, crypto, reactivity }
})

const viewPage = (
  crypto: Crypto.Crypto,
  viewId: Identity.ReplicationViewId,
  request: Parameters<SyncEngine.SyncEngine["Service"]["pull"]>[0],
  changes: ReadonlyArray<Protocol.ViewChange>,
  hasMore: boolean,
  serverSequence = 1
) =>
  Protocol.viewChangesDigest(changes).pipe(
    Effect.map((digest) =>
      Protocol.PullPage.make({
        scopeGeneration: request.scopeGeneration,
        cursor: Protocol.ReplicationCursor.make({
          viewId,
          revision: Identity.ReplicationViewRevision.make((request.cursor?.revision ?? 0) + 1)
        }),
        serverSequence: Identity.ServerSequence.make(serverSequence),
        changes,
        contentBytes: Protocol.encodedBytes(changes),
        digest,
        hasMore,
        serverSchema: Domain.definition.schemaIdentity
      })
    ),
    Effect.provideService(Crypto.Crypto, crypto)
  )

const acceptSubmission: SyncEngine.SyncEngine["Service"]["submitBatch"] = (request) =>
  Effect.succeed(Protocol.SubmitBatchResult.make({
    receipts: request.envelopes.map((envelope) =>
      Protocol.AcceptedReceipt.make({
        ...envelope,
        serverSequence: Identity.ServerSequence.make(1),
        result: Domain.todo(envelope.mutationId, "accepted")
      })
    )
  }))

const awaitNoPending = Effect.fnUntraced(function*(
  replica: Replica.Replica["Service"],
  reactivity: Reactivity.Reactivity
) {
  const changes = yield* Queue.unbounded<void>()
  yield* Effect.acquireRelease(
    Effect.sync(() =>
      reactivity.registerUnsafe([ReactivityKey.aggregateStatus], () => {
        Queue.offerUnsafe(changes, undefined)
      })
    ),
    (unregister) => Effect.sync(unregister)
  )
  while ((yield* replica.status).totalPending > 0) yield* Queue.take(changes)
})

const settlementReadProbe = (reads: ReadonlyMap<Identity.SpaceId, Deferred.Deferred<void>>) =>
(
  source: string,
  values: ReadonlyArray<unknown>
) => {
  if (!source.includes(settledReadSource)) return undefined
  for (const [spaceId, read] of reads) {
    if (values.includes(spaceId)) Deferred.doneUnsafe(read, Effect.void)
  }
  return undefined
}

const makePin = Effect.all({ entered: Deferred.make<void>(), release: Deferred.make<void>() })

type Pin = Effect.Success<typeof makePin>

const pinnedRemote = (pins: ReadonlyMap<Identity.SpaceId, Pin>) => {
  const attempt = (spaceId: Identity.SpaceId) => {
    const pin = pins.get(spaceId)
    if (pin === undefined) return Effect.fail(new ReplicaError.ServerUnavailable())
    return Deferred.succeed(pin.entered, undefined).pipe(
      Effect.andThen(Deferred.await(pin.release)),
      Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable()))
    )
  }
  return SyncEngine.SyncEngine.of({
    ...remoteService,
    submitBatch: (request) => attempt(request.envelopes[0].spaceId),
    pull: (request) => attempt(request.spaceId),
    bootstrap: (request) => attempt(request.spaceId)
  })
}

const abandonActivation = Effect.fnUntraced(function*(
  begin: (space: Replica.Space) => Effect.Effect<unknown, ReplicaError.ReplicaError>
) {
  const initializing = yield* Deferred.make<void>()
  let gateInitialization = false
  const { layer: layerServices } = yield* probedServices((source, values) => {
    if (!gateInitialization || !source.includes(projectionDirtySource) || !values.includes(spaceA)) return undefined
    gateInitialization = false
    return Deferred.succeed(initializing, undefined).pipe(Effect.andThen(Effect.never))
  })
  const layerReplica = SqlReplica.layer({
    ...clientHistory,
    definition: Domain.definition,
    clientId,
    initialSpaces: [spaceA]
  }).pipe(
    Layer.provide(Domain.layerHandlers),
    Layer.provide(layerRemote),
    Layer.provide(layerServices)
  )
  const replica = Context.get(yield* Layer.build(layerReplica), Replica.Replica)
  const space = yield* replica.space(spaceA)
  gateInitialization = true
  const abandoned = yield* Effect.forkChild(begin(space), { startImmediately: true })
  yield* Deferred.await(initializing)
  const waiting = yield* Effect.forkChild(space.get(Domain.Todo, "waiting"), { startImmediately: true })
  yield* Fiber.interrupt(abandoned)

  const waited = yield* Fiber.await(waiting)
  assert.strictEqual(waited._tag, "Success")
  assert.strictEqual(yield* space.activation, "Active")
})

describe("multi space Replica", () => {
  it.effect(
    "isolates overlapping entity keys for two spaces in one database",
    Effect.fnUntraced(function*() {
      const context = yield* Layer.build(layerLive)
      const replica = Context.get(context, Replica.Replica)
      const spaces = yield* replica.spaces
      assert.deepStrictEqual(spaces.map((space) => space.spaceId), [spaceA, spaceB])
      const a = yield* replica.space(spaceA)
      const b = yield* replica.space(spaceB)

      yield* a.mutate(Domain.PutTodo, Domain.todo("same", "A"))
      yield* b.mutate(Domain.PutTodo, Domain.todo("same", "B"))

      assert.strictEqual(Option.getOrThrow(yield* a.get(Domain.Todo, "same")).title, "A")
      assert.strictEqual(Option.getOrThrow(yield* b.get(Domain.Todo, "same")).title, "B")
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.spaces, 2)
      assert.strictEqual(aggregate.totalPending, 2)
    }, Effect.scoped)
  )

  it.effect(
    "evicts one space and leaves retained handles stale across rejoin",
    Effect.fnUntraced(function*() {
      const context = yield* Layer.build(layerLive)
      const replica = Context.get(context, Replica.Replica)
      const stale = yield* replica.space(spaceA)
      const b = yield* replica.space(spaceB)
      yield* stale.mutate(Domain.PutTodo, Domain.todo("same", "A"))
      yield* b.mutate(Domain.PutTodo, Domain.todo("same", "B"))

      yield* replica.leave(spaceA)
      const missingSpace = yield* replica.space(spaceA).pipe(Effect.result)
      assert.strictEqual(missingSpace._tag, "Failure")
      if (missingSpace._tag === "Failure") assert.strictEqual(missingSpace.failure._tag, "SpaceNotJoined")
      assert.strictEqual((yield* stale.get(Domain.Todo, "same").pipe(Effect.flip))._tag, "SpaceUnavailable")
      assert.strictEqual(Option.getOrThrow(yield* b.get(Domain.Todo, "same")).title, "B")

      const rejoined = yield* replica.join(spaceA)
      const rejoinedTodo = yield* rejoined.get(Domain.Todo, "same")
      const rejoinedTodoIsNone = Option.isNone(rejoinedTodo)
      assert.isTrue(rejoinedTodoIsNone)
      const staleMutation = yield* stale.mutate(Domain.PutTodo, Domain.todo("stale")).pipe(Effect.result)
      assert.strictEqual(staleMutation._tag, "Failure")
      if (staleMutation._tag === "Failure") assert.strictEqual(staleMutation.failure._tag, "SpaceUnavailable")
    }, Effect.scoped)
  )

  it.effect(
    "restores durable memberships when the Replica runtime restarts",
    Effect.fnUntraced(function*() {
      const layerDatabase = Layer.mergeAll(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer,
        Reactivity.layer
      )
      const databaseContext = yield* Layer.build(layerDatabase)
      const layerSql = Layer.succeed(SqlClient.SqlClient, Context.get(databaseContext, SqlClient.SqlClient))
      const layerCrypto = Layer.succeed(Crypto.Crypto, Context.get(databaseContext, Crypto.Crypto))
      const layerReactivity = Layer.succeed(
        Reactivity.Reactivity,
        Context.get(databaseContext, Reactivity.Reactivity)
      )
      const layerServices = Layer.mergeAll(
        layerSql,
        layerCrypto,
        layerReactivity
      )
      const replicaLayer = (initialSpaces?: Iterable<Identity.SpaceId>) => {
        const options = {
          ...clientHistory,
          definition: Domain.definition,
          clientId
        } satisfies SqlReplica.Options<typeof Domain.definition>
        let layerReplica = SqlReplica.layer(options)
        if (initialSpaces !== undefined) layerReplica = SqlReplica.layer({ ...options, initialSpaces })
        return layerReplica.pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(layerRemote),
          Layer.provide(layerServices)
        )
      }

      const firstScope = yield* Scope.make()
      const firstContext = yield* Layer.buildWithScope(replicaLayer([spaceA, spaceB]), firstScope)
      const first = Context.get(firstContext, Replica.Replica)
      yield* (yield* first.space(spaceA)).mutate(Domain.PutTodo, Domain.todo("restart", "retained"))
      yield* Scope.close(firstScope, Exit.void)

      const secondScope = yield* Scope.make()
      const secondContext = yield* Layer.buildWithScope(replicaLayer(), secondScope)
      const second = Context.get(secondContext, Replica.Replica)
      assert.deepStrictEqual((yield* second.spaces).map((space) => space.spaceId), [spaceA, spaceB])
      assert.strictEqual(
        Option.getOrThrow(yield* (yield* second.space(spaceA)).get(Domain.Todo, "restart")).title,
        "retained"
      )
      yield* Scope.close(secondScope, Exit.void)
    }, Effect.scoped)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "serves a restored space while its background reconciliation waits on the remote with %s",
    Effect.fnUntraced(function*(constructor) {
      const replica = yield* restartWithStalledBackgroundPull(constructor)
      const restored = yield* replica.space(spaceA)
      assert.strictEqual(Option.getOrThrow(yield* restored.get(Domain.Todo, "restart")).title, "retained")
      assert.strictEqual(yield* restored.activation, "Active")
    }, Effect.scoped)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "leaves a restored space while its background reconciliation waits on the remote with %s",
    Effect.fnUntraced(function*(constructor) {
      const replica = yield* restartWithStalledBackgroundPull(constructor)
      yield* replica.leave(spaceA)
      const missing = yield* replica.space(spaceA).pipe(Effect.result)
      assert.strictEqual(missing._tag, "Failure")
      if (missing._tag === "Failure") assert.strictEqual(missing.failure._tag, "SpaceNotJoined")
    }, Effect.scoped)
  )

  it.effect(
    "releases a workflow registration reservation when its owner is interrupted",
    Effect.fnUntraced(function*() {
      const engineContext = yield* Layer.build(WorkflowEngine.layerMemory)
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)
      const firstRegistration = yield* Deferred.make<void>()
      let blockRegistration = true
      const register: typeof engine.register = (workflow, execute) => {
        if (!blockRegistration) return engine.register(workflow, execute)
        blockRegistration = false
        return Deferred.succeed(firstRegistration, undefined).pipe(Effect.andThen(Effect.never))
      }
      const interruptedEngine = new Proxy(engine, {
        get: (target, property, receiver) => {
          if (property === "register") return register
          return Reflect.get(target, property, receiver)
        }
      })
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        clientId
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer),
        Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, interruptedEngine))
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const first = yield* Effect.forkChild(replica.join(spaceA), { startImmediately: true })
      yield* Deferred.await(firstRegistration)
      yield* Fiber.interrupt(first)

      const remembered = yield* replica.join(spaceA)
      yield* remembered.activate
      assert.strictEqual(yield* remembered.activation, "Active")
    }, Effect.scoped)
  )

  it.effect(
    "returns one live remembered handle to concurrent joins",
    Effect.fnUntraced(function*() {
      const engineContext = yield* Layer.build(WorkflowEngine.layerMemory)
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)
      const firstRegistrationEntered = yield* Deferred.make<void>()
      const releaseFirstRegistration = yield* Deferred.make<void>()
      let registrations = 0
      const register: typeof engine.register = (workflow, execute) => {
        registrations += 1
        if (registrations !== 1) return engine.register(workflow, execute)
        return Deferred.succeed(firstRegistrationEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releaseFirstRegistration)),
          Effect.andThen(engine.register(workflow, execute))
        )
      }
      const gatedEngine = new Proxy(engine, {
        get: (target, property, receiver) => {
          if (property === "register") return register
          return Reflect.get(target, property, receiver)
        }
      })
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        clientId
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer),
        Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, gatedEngine))
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const firstJoin = yield* Effect.forkChild(replica.join(spaceA), { startImmediately: true })
      yield* Deferred.await(firstRegistrationEntered)
      const secondJoin = yield* Effect.forkChild(replica.join(spaceA), { startImmediately: true })
      yield* Effect.yieldNow
      yield* Deferred.succeed(releaseFirstRegistration, undefined)
      const [first, second] = yield* Effect.all([Fiber.join(firstJoin), Fiber.join(secondJoin)])

      yield* first.activate
      yield* second.activate
      assert.strictEqual((yield* replica.status).spaces, 1)
      assert.lengthOf(yield* replica.spaces, 1)
    }, Effect.scoped)
  )

  it.effect(
    "releases manager registration when join is interrupted",
    Effect.fnUntraced(function*() {
      const requestBlocked = yield* Deferred.make<void>()
      const activeWatches = yield* Ref.make(0)
      const watchStarted = yield* Deferred.make<void>()
      const countedRemoteService = SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        transportGeneration: Effect.succeed(0),
        waitForTransportChange: () => Effect.never,
        submitBatch: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        discard: () => Effect.die("unexpected discard"),
        pull: () => Effect.never,
        bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        watch: () => {
          const acquire = Ref.update(activeWatches, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(watchStarted, undefined)),
            Effect.as(Stream.never)
          )
          return Effect.acquireRelease(
            acquire,
            () => Ref.update(activeWatches, (count) => count - 1)
          ).pipe(Stream.unwrap)
        }
      })
      const layerCountedRemote = Layer.succeed(SyncEngine.SyncEngine, countedRemoteService)
      const manager = yield* Reconciler.makeManager().pipe(Effect.provide(layerCountedRemote))
      const local = {
        requestReconciliation: Deferred.succeed(requestBlocked, undefined).pipe(Effect.andThen(Effect.never)),
        reconciliationGenerations: Effect.succeed({ requested: 0, completed: 0 }),
        replicationState: Effect.succeed({
          clientId,
          scope,
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: null
        }),
        completeReconciliation: () => Effect.void
      } satisfies Pick<
        LocalStore.Service,
        "requestReconciliation" | "reconciliationGenerations" | "replicationState" | "completeReconciliation"
      >
      const reconciliation = Reconciler.Reconciliation.of({
        sync: Effect.void,
        failed: () => Effect.void,
        watchFailed: () => Effect.void,
        succeeded: Effect.void,
        status: Effect.succeed({ _tag: "Offline", pending: 0 })
      })
      const registration = yield* Effect.forkChild(
        manager.register({
          spaceId: spaceA,
          generation: 1,
          definition: Domain.definition,
          local,
          reconciliation
        }),
        { startImmediately: true }
      )
      yield* Deferred.await(watchStarted)
      yield* Deferred.await(requestBlocked)
      yield* Fiber.interrupt(registration)

      assert.strictEqual(yield* Ref.get(activeWatches), 0)
    }, Effect.scoped)
  )

  it.effect(
    "drains an admitted mutation before reconciliation teardown",
    Effect.fnUntraced(function*() {
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const definition = Definition.make({
        version: 1,
        models: [Domain.Todo],
        mutations: [Domain.PutTodo]
      })
      const layerHandlers = Domain.PutTodo.toLayer(({ payload, transaction }) =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.andThen(transaction.set(Domain.Todo, payload.id, payload)),
          Effect.as(payload)
        )
      )
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition,
        clientId,
        initialSpaces: [spaceA]
      }).pipe(
        Layer.provide(layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer)
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const space = yield* replica.space(spaceA)
      const mutate = space.mutate(Domain.PutTodo, Domain.todo("drain"))
      const mutation = yield* Effect.forkChild(
        mutate,
        { startImmediately: true }
      )
      yield* Deferred.await(entered)
      const leaving = yield* Effect.forkChild(replica.leave(spaceA), { startImmediately: true })
      yield* Effect.yieldNow
      const missingSpace = yield* replica.space(spaceA).pipe(Effect.result)
      assert.strictEqual(missingSpace._tag, "Failure")
      if (missingSpace._tag === "Failure") assert.strictEqual(missingSpace.failure._tag, "SpaceNotJoined")
      yield* Deferred.succeed(release, undefined)

      assert.strictEqual((yield* Fiber.join(mutation)).envelope.spaceId, spaceA)
      yield* Fiber.join(leaving)
    }, Effect.scoped)
  )

  it.effect(
    "releases a leaving reservation when its owner is interrupted",
    Effect.fnUntraced(function*() {
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const definition = Definition.make({
        version: 1,
        models: [Domain.Todo],
        mutations: [Domain.PutTodo]
      })
      const layerHandlers = Domain.PutTodo.toLayer(({ payload, transaction }) =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.andThen(transaction.set(Domain.Todo, payload.id, payload)),
          Effect.as(payload)
        )
      )
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition,
        clientId,
        initialSpaces: [spaceA]
      }).pipe(
        Layer.provide(layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer)
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const space = yield* replica.space(spaceA)
      const mutate = space.mutate(Domain.PutTodo, Domain.todo("interrupt-leave"))
      const mutation = yield* Effect.forkChild(
        mutate,
        { startImmediately: true }
      )
      yield* Deferred.await(entered)
      const leaving = yield* Effect.forkChild(replica.leave(spaceA), { startImmediately: true })
      yield* Effect.yieldNow
      const missingSpace = yield* replica.space(spaceA).pipe(Effect.result)
      assert.strictEqual(missingSpace._tag, "Failure")
      if (missingSpace._tag === "Failure") assert.strictEqual(missingSpace.failure._tag, "SpaceNotJoined")
      yield* Fiber.interrupt(leaving)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(mutation)

      assert.strictEqual((yield* replica.join(spaceA)).spaceId, spaceA)
    }, Effect.scoped)
  )

  it.effect(
    "retries durable eviction after a SQL failure",
    Effect.fnUntraced(function*() {
      const layerDatabase = Layer.mergeAll(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer,
        Reactivity.layer
      )
      const databaseContext = yield* Layer.build(layerDatabase)
      const sql = Context.get(databaseContext, SqlClient.SqlClient)
      let failDelete = true
      const failingSql = new Proxy(sql, {
        apply: (target, thisArg, args: Parameters<typeof sql>) => {
          const rawSource: unknown = args[0]
          let source: string
          if (Array.isArray(rawSource)) source = rawSource.join("")
          else source = String(rawSource)
          if (failDelete && source.includes("DELETE FROM effect_local_client_spaces")) {
            failDelete = false
            return Effect.fail(
              new SqlError.SqlError({
                reason: new SqlError.UnknownError({ cause: "injected leave failure" })
              })
            )
          }
          return Reflect.apply(target, thisArg, args)
        }
      })
      const services = (client: SqlClient.SqlClient) => {
        const layerSql = Layer.succeed(SqlClient.SqlClient, client)
        const layerCrypto = Layer.succeed(Crypto.Crypto, Context.get(databaseContext, Crypto.Crypto))
        const layerReactivity = Layer.succeed(
          Reactivity.Reactivity,
          Context.get(databaseContext, Reactivity.Reactivity)
        )
        return Layer.mergeAll(
          layerSql,
          layerCrypto,
          layerReactivity
        )
      }
      const replicaLayer = (client: SqlClient.SqlClient, initialSpaces?: Iterable<Identity.SpaceId>) =>
        SqlReplica.layer({
          ...clientHistory,
          definition: Domain.definition,
          clientId,
          initialSpaces: initialSpaces ?? []
        }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(layerRemote),
          Layer.provide(services(client))
        )

      const firstScope = yield* Scope.make()
      const firstContext = yield* Layer.buildWithScope(replicaLayer(failingSql, [spaceA]), firstScope)
      const first = Context.get(firstContext, Replica.Replica)
      const failedLeave = yield* first.leave(spaceA).pipe(Effect.result)
      assert.strictEqual(failedLeave._tag, "Failure")
      if (failedLeave._tag === "Failure") assert.strictEqual(failedLeave.failure._tag, "StorageUnavailable")
      yield* first.leave(spaceA)
      yield* Scope.close(firstScope, Exit.void)

      const secondScope = yield* Scope.make()
      const secondContext = yield* Layer.buildWithScope(replicaLayer(sql), secondScope)
      assert.deepStrictEqual(yield* Context.get(secondContext, Replica.Replica).spaces, [])
      yield* Scope.close(secondScope, Exit.void)
    }, Effect.scoped)
  )

  it.effect(
    "reschedules pending work when deactivation bookkeeping fails",
    Effect.fnUntraced(function*() {
      const databaseContext = yield* Layer.mergeAll(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer,
        Reactivity.layer
      ).pipe(Layer.build)
      const sql = Context.get(databaseContext, SqlClient.SqlClient)
      let failPendingCount = false
      const failingSql = new Proxy(sql, {
        apply: (target, thisArg, args: Parameters<typeof sql>) => {
          const rawSource: unknown = args[0]
          let source: string
          if (Array.isArray(rawSource)) source = rawSource.join("")
          else source = String(rawSource)
          if (
            failPendingCount &&
            source.includes("FROM effect_local_client_spaces AS s") &&
            source.includes("SELECT COUNT(p.mutation_id)")
          ) {
            failPendingCount = false
            return Effect.fail(
              new SqlError.SqlError({
                reason: new SqlError.UnknownError({ cause: "injected pending count failure" })
              })
            )
          }
          return Reflect.apply(target, thisArg, args)
        }
      })
      const backgroundAttempted = yield* Deferred.make<void>()
      let observeRemote = false
      const observeAttempt = () => {
        if (!observeRemote) return Effect.never
        return Deferred.succeed(backgroundAttempted, undefined).pipe(
          Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable()))
        )
      }
      const retryingRemote = SyncEngine.SyncEngine.of({
        ...remoteService,
        submitBatch: observeAttempt,
        pull: observeAttempt,
        bootstrap: observeAttempt
      })
      const layerSql = Layer.succeed(SqlClient.SqlClient, failingSql)
      const layerCrypto = Layer.succeed(Crypto.Crypto, Context.get(databaseContext, Crypto.Crypto))
      const layerReactivity = Layer.succeed(
        Reactivity.Reactivity,
        Context.get(databaseContext, Reactivity.Reactivity)
      )
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition: Domain.definition,
        clientId,
        initialSpaces: [spaceA],
        retryDelay: "1 hour",
        maximumRetryDelay: "1 hour"
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(Layer.succeed(SyncEngine.SyncEngine, retryingRemote)),
        Layer.provide(Layer.mergeAll(layerSql, layerCrypto, layerReactivity))
      )
      const context = yield* Layer.build(layerReplica)
      const space = yield* Context.get(context, Replica.Replica).space(spaceA)
      yield* space.mutate(Domain.PutTodo, Domain.todo("pending-bookkeeping"))
      observeRemote = true
      failPendingCount = true

      const deactivation = yield* space.deactivate.pipe(Effect.result)
      assert.strictEqual(deactivation._tag, "Failure")
      if (deactivation._tag === "Failure") {
        assert.strictEqual(deactivation.failure._tag, "StorageUnavailable")
      }
      yield* Deferred.await(backgroundAttempted)
    }, Effect.scoped)
  )

  it.effect(
    "bounds reconciliation turns across many activated spaces",
    Effect.fnUntraced(function*() {
      const spaces = Array.from({ length: 6 }, (_, index) => {
        const suffix = String(index + 1).padStart(12, "0")
        return Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${suffix}`)
      })
      const current = yield* Ref.make(0)
      const maximum = yield* Ref.make(0)
      const twoStarted = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const blockedRemoteService = SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        transportGeneration: Effect.succeed(0),
        waitForTransportChange: () => Effect.never,
        submitBatch: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        discard: () => Effect.die("unexpected discard"),
        pull: () => {
          const acquire = Ref.updateAndGet(current, (count) => count + 1).pipe(
            Effect.tap((count) => Ref.update(maximum, (seen) => Math.max(seen, count))),
            Effect.tap((count) => {
              if (count === 2) return Deferred.succeed(twoStarted, undefined)
              return Effect.void
            })
          )
          const use = () =>
            Deferred.await(release).pipe(Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable())))
          return Effect.acquireUseRelease(
            acquire,
            use,
            () => Ref.update(current, (count) => count - 1)
          )
        },
        bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        watch: () => Stream.never
      })
      const layerBlockedRemote = Layer.succeed(SyncEngine.SyncEngine, blockedRemoteService)
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition: Domain.definition,
        clientId,
        initialSpaces: spaces,
        reconciliationConcurrency: 3,
        foregroundReconciliationConcurrency: 2
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerBlockedRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer)
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const activations = yield* Effect.forEach(
        spaces,
        (spaceId) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.activate)),
        { concurrency: "unbounded" }
      ).pipe(Effect.forkScoped({ startImmediately: true }))
      yield* Deferred.await(twoStarted)

      assert.isAtMost(yield* Ref.get(maximum), 2)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(activations)
      assert.isAtMost(yield* Ref.get(maximum), 2)
    }, Effect.scoped)
  )

  it.effect(
    "bounds foreground workflow reconciliation turns",
    Effect.fnUntraced(function*() {
      const spaces = Array.from({ length: 6 }, (_, index) => {
        const suffix = String(index + 1).padStart(12, "0")
        return Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${suffix}`)
      })
      const current = yield* Ref.make(0)
      const maximum = yield* Ref.make(0)
      const twoStarted = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const blockedRemote = SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        transportGeneration: Effect.succeed(0),
        waitForTransportChange: () => Effect.never,
        submitBatch: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        discard: () => Effect.die("unexpected discard"),
        pull: () =>
          Effect.acquireUseRelease(
            Ref.updateAndGet(current, (count) => count + 1).pipe(
              Effect.tap((count) => Ref.update(maximum, (seen) => Math.max(seen, count))),
              Effect.tap((count) => {
                if (count === 2) return Deferred.succeed(twoStarted, undefined)
                return Effect.void
              })
            ),
            () => Deferred.await(release).pipe(Effect.andThen(Effect.never)),
            () => Ref.update(current, (count) => count - 1)
          ),
        bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        watch: () => Stream.never
      })
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        clientId,
        initialSpaces: spaces,
        reconciliationConcurrency: 3,
        foregroundReconciliationConcurrency: 2
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(Layer.succeed(SyncEngine.SyncEngine, blockedRemote)),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer),
        Layer.provideMerge(WorkflowEngine.layerMemory)
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      yield* Effect.forEach(
        spaces,
        (spaceId) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.activate)),
        { concurrency: "unbounded", discard: true }
      )
      yield* Deferred.await(twoStarted)
      for (let index = 0; index < 10; index += 1) yield* Effect.yieldNow

      assert.isAtMost(yield* Ref.get(maximum), 2)
      yield* Deferred.succeed(release, undefined)
    }, Effect.scoped)
  )

  it.effect(
    "admits foreground activations that reserve capacity together before any becomes active",
    Effect.fnUntraced(function*() {
      const spaces = Array.from({ length: 3 }, (_, index) => {
        const suffix = String(index + 1).padStart(12, "0")
        return Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${suffix}`)
      })
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition: Domain.definition,
        clientId,
        initialSpaces: spaces
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer)
      )
      const replica = Context.get(yield* Layer.build(layerReplica), Replica.Replica)
      const handles = yield* Effect.forEach(spaces, replica.space)
      yield* Effect.forEach(handles, (space) => space.activate, { concurrency: "unbounded", discard: true })
      const activations = yield* Effect.forEach(handles, (space) => space.activation)
      assert.strictEqual(activations.filter((activation) => activation === "Active").length, 2)
    }, (effect) => effect.pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 5), Effect.scoped))
  )

  it.effect(
    "settles a foreground mutation while the background lane is saturated",
    Effect.fnUntraced(function*() {
      const backgroundSpaces = Array.from({ length: 3 }, (_, index) => {
        const suffix = String(index + 10).padStart(12, "0")
        return Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${suffix}`)
      })
      const foregroundSpace = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000099")
      const allSpaces = [...backgroundSpaces, foregroundSpace]
      const databaseContext = yield* Layer.mergeAll(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer,
        Reactivity.layer
      ).pipe(Layer.build)
      const sqlService = Context.get(databaseContext, SqlClient.SqlClient)
      const cryptoService = Context.get(databaseContext, Crypto.Crypto)
      const reactivityService = Context.get(databaseContext, Reactivity.Reactivity)
      const layerServices = Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, sqlService),
        Layer.succeed(Crypto.Crypto, cryptoService),
        Layer.succeed(Reactivity.Reactivity, reactivityService)
      )
      let blockBackground = false
      const backgroundEntered = yield* Deferred.make<void>()
      const releaseBackground = yield* Deferred.make<void>()
      const activeBackground = yield* Ref.make(0)
      const viewId = Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000001")
      const scheduledRemote = SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        transportGeneration: Effect.succeed(0),
        waitForTransportChange: () => Effect.never,
        submitBatch: (request) => {
          if (request.envelopes[0].spaceId !== foregroundSpace) {
            if (!blockBackground) return Effect.fail(new ReplicaError.ServerUnavailable())
            return Ref.updateAndGet(activeBackground, (count) => count + 1).pipe(
              Effect.tap(() => Deferred.succeed(backgroundEntered, undefined)),
              Effect.andThen(Deferred.await(releaseBackground)),
              Effect.as(Protocol.SubmitBatchResult.make({
                receipts: request.envelopes.map((envelope) =>
                  Protocol.AcceptedReceipt.make({
                    ...envelope,
                    serverSequence: Identity.ServerSequence.make(1),
                    result: Domain.todo(envelope.spaceId, "background settled")
                  })
                )
              })),
              Effect.ensuring(Ref.update(activeBackground, (count) => count - 1))
            )
          }
          return Effect.succeed(Protocol.SubmitBatchResult.make({
            receipts: request.envelopes.map((envelope) =>
              Protocol.AcceptedReceipt.make({
                ...envelope,
                serverSequence: Identity.ServerSequence.make(1),
                result: Domain.todo("foreground", "settled")
              })
            )
          }))
        },
        discard: () => Effect.die("unexpected discard"),
        pull: (request) => {
          return Protocol.viewChangesDigest([]).pipe(
            Effect.map((digest) =>
              Protocol.PullPage.make({
                scopeGeneration: request.scopeGeneration,
                cursor: Protocol.ReplicationCursor.make({
                  viewId,
                  revision: Identity.ReplicationViewRevision.make((request.cursor?.revision ?? 0) + 1)
                }),
                serverSequence: Identity.ServerSequence.make(1),
                changes: [],
                contentBytes: Protocol.encodedBytes([]),
                digest,
                hasMore: false,
                serverSchema: Domain.definition.schemaIdentity
              })
            ),
            Effect.provideService(Crypto.Crypto, cryptoService)
          )
        },
        bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        watch: () => Stream.never
      })
      const layerReplica = () =>
        SqlReplica.layer({
          ...clientHistory,
          definition: Domain.definition,
          clientId,
          initialSpaces: allSpaces,
          maximumActiveSpaces: 4,
          foregroundActiveSpaces: 1,
          reconciliationConcurrency: 2,
          foregroundReconciliationConcurrency: 1,
          retryDelay: "1 hour",
          maximumRetryDelay: "1 hour"
        }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, scheduledRemote)),
          Layer.provide(layerServices)
        )

      const seedScope = yield* Scope.make()
      const seedContext = yield* Layer.buildWithScope(layerReplica(), seedScope)
      const seedReplica = Context.get(seedContext, Replica.Replica)
      for (const spaceId of backgroundSpaces) {
        const space = yield* seedReplica.space(spaceId)
        yield* space.mutate(Domain.PutTodo, Domain.todo(spaceId))
        yield* space.deactivate
      }
      yield* Scope.close(seedScope, Exit.void)
      yield* sqlService`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`

      blockBackground = true
      const runtimeScope = yield* Scope.make()
      const runtimeContext = yield* Layer.buildWithScope(layerReplica(), runtimeScope)
      const replica = Context.get(runtimeContext, Replica.Replica)
      yield* Deferred.await(backgroundEntered)
      for (let index = 0; index < 10; index += 1) yield* Effect.yieldNow
      assert.strictEqual(yield* Ref.get(activeBackground), 1)

      const foreground = yield* replica.space(foregroundSpace)
      const pending = yield* foreground.mutate(Domain.PutTodo, Domain.todo("foreground", "settled"))
      let receipt = yield* foreground.receipt(Domain.PutTodo, pending.envelope.mutationId)
      while (Option.isNone(receipt)) {
        yield* Effect.yieldNow
        receipt = yield* foreground.receipt(Domain.PutTodo, pending.envelope.mutationId)
      }
      assert.strictEqual(receipt.value._tag, "Accepted")

      yield* Deferred.succeed(releaseBackground, undefined)
      for (const spaceId of backgroundSpaces) {
        const space = yield* replica.space(spaceId)
        let status = yield* space.status
        while (status.pending > 0 || (yield* space.activation) !== "Inactive") {
          yield* Effect.yieldNow
          status = yield* space.status
        }
      }
      yield* Scope.close(runtimeScope, Exit.void)
    }, Effect.scoped)
  )

  it.effect(
    "keeps background workers available while failed spaces wait to retry",
    Effect.fnUntraced(function*() {
      const spaces = Array.from({ length: 6 }, (_, index) => {
        const suffix = String(index + 20).padStart(12, "0")
        return Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${suffix}`)
      })
      const databaseContext = yield* Layer.mergeAll(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer,
        Reactivity.layer
      ).pipe(Layer.build)
      const sqlService = Context.get(databaseContext, SqlClient.SqlClient)
      const cryptoService = Context.get(databaseContext, Crypto.Crypto)
      const reactivityService = Context.get(databaseContext, Reactivity.Reactivity)
      const layerServices = Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, sqlService),
        Layer.succeed(Crypto.Crypto, cryptoService),
        Layer.succeed(Reactivity.Reactivity, reactivityService)
      )
      const replicaLayer = (remote: SyncEngine.SyncEngine["Service"]) =>
        SqlReplica.layer({
          ...clientHistory,
          definition: Domain.definition,
          clientId,
          initialSpaces: spaces,
          retryDelay: "1 hour",
          maximumRetryDelay: "1 hour"
        }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.provide(layerServices)
        )

      const seedScope = yield* Scope.make()
      const seedContext = yield* Layer.buildWithScope(replicaLayer(remoteService), seedScope)
      const seedReplica = Context.get(seedContext, Replica.Replica)
      for (const spaceId of spaces) {
        const space = yield* seedReplica.space(spaceId)
        yield* space.mutate(Domain.PutTodo, Domain.todo(spaceId))
        yield* space.deactivate
      }
      yield* Scope.close(seedScope, Exit.void)

      const attempted = new Set<Identity.SpaceId>()
      const allAttempted = yield* Deferred.make<void>()
      const observeAttempt = (spaceId: Identity.SpaceId) => {
        attempted.add(spaceId)
        let observed = Effect.void
        if (attempted.size === spaces.length) observed = Deferred.succeed(allAttempted, undefined)
        return observed.pipe(Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable())))
      }
      const failingRemote = SyncEngine.SyncEngine.of({
        ...remoteService,
        submitBatch: (request) => observeAttempt(request.envelopes[0].spaceId),
        pull: (request) => observeAttempt(request.spaceId),
        bootstrap: (request) => observeAttempt(request.spaceId)
      })
      const runtimeScope = yield* Scope.make()
      const runtimeContext = yield* Layer.buildWithScope(replicaLayer(failingRemote), runtimeScope)
      assert.strictEqual((yield* Context.get(runtimeContext, Replica.Replica).status).totalPending, spaces.length)

      yield* Deferred.await(allAttempted)
      assert.strictEqual(attempted.size, spaces.length)
      yield* Scope.close(runtimeScope, Exit.void)
    }, Effect.scoped)
  )

  it.effect(
    "keeps a foreground promotion active after its background turn releases",
    Effect.fnUntraced(function*() {
      const databaseContext = yield* Layer.mergeAll(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer,
        Reactivity.layer
      ).pipe(Layer.build)
      const sqlService = Context.get(databaseContext, SqlClient.SqlClient)
      const cryptoService = Context.get(databaseContext, Crypto.Crypto)
      const reactivityService = Context.get(databaseContext, Reactivity.Reactivity)
      const layerServices = Layer.mergeAll(
        Layer.succeed(SqlClient.SqlClient, sqlService),
        Layer.succeed(Crypto.Crypto, cryptoService),
        Layer.succeed(Reactivity.Reactivity, reactivityService)
      )
      const replicaLayer = (remote: SyncEngine.SyncEngine["Service"]) =>
        SqlReplica.layer({
          ...clientHistory,
          definition: Domain.definition,
          clientId,
          initialSpaces: [spaceA],
          retryDelay: "1 hour",
          maximumRetryDelay: "1 hour"
        }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.provide(layerServices)
        )

      const seedScope = yield* Scope.make()
      const seedContext = yield* Layer.buildWithScope(replicaLayer(remoteService), seedScope)
      const seedSpace = yield* Context.get(seedContext, Replica.Replica).space(spaceA)
      yield* seedSpace.mutate(Domain.PutTodo, Domain.todo("promote"))
      yield* seedSpace.deactivate
      yield* Scope.close(seedScope, Exit.void)

      const backgroundEntered = yield* Deferred.make<void>()
      const releaseBackground = yield* Deferred.make<void>()
      const activeWatches = yield* Ref.make(0)
      const watchOpened = yield* Deferred.make<void>()
      let gateFirstAttempt = true
      const attempt = () => {
        if (!gateFirstAttempt) return Effect.fail(new ReplicaError.ServerUnavailable())
        gateFirstAttempt = false
        return Deferred.succeed(backgroundEntered, undefined).pipe(
          Effect.andThen(Deferred.await(releaseBackground)),
          Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable()))
        )
      }
      const blockedRemote = SyncEngine.SyncEngine.of({
        ...remoteService,
        submitBatch: attempt,
        pull: attempt,
        bootstrap: attempt,
        watch: () =>
          Effect.acquireRelease(
            Ref.update(activeWatches, (count) => count + 1).pipe(
              Effect.andThen(Deferred.succeed(watchOpened, undefined)),
              Effect.as(Stream.never)
            ),
            () => Ref.update(activeWatches, (count) => count - 1)
          ).pipe(Stream.unwrap)
      })
      const runtimeScope = yield* Scope.make()
      const runtimeContext = yield* Layer.buildWithScope(replicaLayer(blockedRemote), runtimeScope)
      const space = yield* Context.get(runtimeContext, Replica.Replica).space(spaceA)
      yield* Deferred.await(backgroundEntered)
      const promotion = yield* Effect.forkChild(space.activate, { startImmediately: true })
      yield* Effect.yieldNow
      yield* Deferred.succeed(releaseBackground, undefined)
      yield* Fiber.join(promotion)
      yield* Deferred.await(watchOpened)

      assert.strictEqual(yield* space.activation, "Active")
      assert.strictEqual(yield* Ref.get(activeWatches), 1)
      yield* Scope.close(runtimeScope, Exit.void)
    }, Effect.scoped)
  )

  it.effect(
    "remembers many inactive spaces without opening watch streams",
    Effect.fnUntraced(function*() {
      const spaces = Array.from({ length: 1_000 }, (_, index) => {
        const suffix = String(index + 1).padStart(12, "0")
        return Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${suffix}`)
      })
      const activeWatches = yield* Ref.make(0)
      const watchOpened = yield* Deferred.make<void>()
      const countedRemote = SyncEngine.SyncEngine.of({
        ...remoteService,
        watch: () =>
          Effect.acquireRelease(
            Ref.update(activeWatches, (count) => count + 1).pipe(
              Effect.andThen(Deferred.succeed(watchOpened, undefined)),
              Effect.as(Stream.never)
            ),
            () => Ref.update(activeWatches, (count) => count - 1)
          ).pipe(Stream.unwrap)
      })
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition: Domain.definition,
        clientId,
        maximumActiveSpaces: 8,
        foregroundActiveSpaces: 4
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(Layer.succeed(SyncEngine.SyncEngine, countedRemote)),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer)
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const baselineFibers = yield* activeChildFibers
      yield* Effect.forEach(spaces, replica.join, { discard: true })
      const remembered = yield* replica.spaces
      const activations = yield* Effect.forEach(remembered, (space) => space.activation)

      assert.lengthOf(remembered, 1_000)
      assert.strictEqual(yield* Ref.get(activeWatches), 0)
      assert.strictEqual(yield* activeChildFibers, baselineFibers)
      assert.isTrue(activations.every((activation) => activation === "Inactive"))
      yield* remembered[0].activate
      yield* Deferred.await(watchOpened)
      assert.strictEqual(yield* Ref.get(activeWatches), 1)
      yield* remembered[0].deactivate
      assert.strictEqual(yield* Ref.get(activeWatches), 0)
      assert.strictEqual(yield* activeChildFibers, baselineFibers)
    }, (effect) =>
      effect.pipe(
        Metric.enableRuntimeMetrics,
        Effect.provideService(Metric.MetricRegistry, new Map()),
        Effect.scoped
      ))
  )

  it.effect(
    "persists scope independently for each remembered space",
    Effect.fnUntraced(function*() {
      const context = yield* Layer.build(layerLive)
      const replica = Context.get(context, Replica.Replica)
      const first = yield* replica.space(spaceA)
      const second = yield* replica.space(spaceB)
      const empty = Protocol.ReplicationScope.make({ models: [] })

      yield* first.setScope(empty)
      yield* first.deactivate

      assert.deepStrictEqual(yield* first.scope, empty)
      assert.deepStrictEqual(yield* second.scope, scope)
      assert.strictEqual(yield* first.activation, "Inactive")
      yield* first.activate
      assert.deepStrictEqual(yield* first.scope, empty)
    }, Effect.scoped)
  )

  it.effect(
    "evicts the least recently used unleased foreground runtime",
    Effect.fnUntraced(function*() {
      const spaceC = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition: Domain.definition,
        clientId,
        initialSpaces: [spaceA, spaceB, spaceC],
        maximumActiveSpaces: 4,
        foregroundActiveSpaces: 2
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer)
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const first = yield* replica.space(spaceA)
      const second = yield* replica.space(spaceB)
      const third = yield* replica.space(spaceC)

      yield* first.activate
      yield* second.activate
      yield* first.get(Domain.Todo, "touch")
      yield* third.activate

      assert.strictEqual(yield* first.activation, "Active")
      assert.strictEqual(yield* second.activation, "Inactive")
      assert.strictEqual(yield* third.activation, "Active")
    }, Effect.scoped)
  )

  it.effect(
    "interrupts a get waiting for foreground capacity held by in-flight reconciliation",
    Effect.fnUntraced(function*() {
      const spaceC = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
      const firstPin = yield* makePin
      const secondPin = yield* makePin
      const pinned = pinnedRemote(new Map([[spaceA, firstPin], [spaceB, secondPin]]))
      const firstRetried = yield* Deferred.make<void>()
      let firstPulls = 0
      const remote = SyncEngine.SyncEngine.of({
        ...pinned,
        pull: (request) => {
          if (request.spaceId === spaceA && ++firstPulls === 2) Deferred.doneUnsafe(firstRetried, Effect.void)
          return pinned.pull(request)
        }
      })
      const { layer: layerServices } = yield* probedServices(() => undefined)
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        clientId,
        initialSpaces: [spaceA, spaceB, spaceC],
        foregroundReconciliationConcurrency: 2
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
        Layer.provide(layerServices),
        Layer.provide(WorkflowEngine.layerMemory)
      )
      const replica = Context.get(yield* Layer.build(layerReplica), Replica.Replica)
      const first = yield* replica.space(spaceA)
      const second = yield* replica.space(spaceB)
      const third = yield* replica.space(spaceC)
      yield* first.mutate(Domain.PutTodo, Domain.todo("pinned"))
      yield* second.mutate(Domain.PutTodo, Domain.todo("pinned"))
      yield* Deferred.await(firstPin.entered)
      yield* Deferred.await(secondPin.entered)

      const blocked = yield* Effect.forkChild(third.get(Domain.Todo, "blocked"), { startImmediately: true }).pipe(
        Effect.provideService(Scheduler.PreventSchedulerYield, true)
      )
      yield* Effect.yieldNow
      assert.strictEqual(yield* third.activation, "Inactive")
      yield* Fiber.interrupt(blocked)
      assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(blocked)))

      yield* Deferred.succeed(firstPin.release, undefined)
      assert.isTrue(Option.isNone(yield* third.get(Domain.Todo, "after")))
      yield* Deferred.await(firstRetried)
      yield* first.activation.pipe(Effect.repeat({ until: (activation) => activation === "Inactive" }))
      assert.strictEqual(yield* first.activation, "Inactive")
      assert.strictEqual(yield* second.activation, "Active")
      assert.strictEqual(yield* third.activation, "Active")
    }, Effect.scoped)
  )

  it.effect(
    "keeps a waiting get alive when the get initializing the same space is interrupted",
    Effect.fnUntraced(function*() {
      yield* abandonActivation((space) => space.get(Domain.Todo, "abandoned"))
    }, Effect.scoped)
  )

  it.effect(
    "keeps a waiting get alive when the activation initializing the same space is interrupted",
    Effect.fnUntraced(function*() {
      yield* abandonActivation((space) => space.activate)
    }, Effect.scoped)
  )

  it.effect(
    "releases the foreground reservation of an interrupted promotion from background",
    Effect.fnUntraced(function*() {
      const spaceC = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
      const { layer: layerServices } = yield* probedServices(() => undefined)
      const replicaLayer = (remote: SyncEngine.SyncEngine["Service"]) =>
        SqlReplica.layerWorkflow({
          ...clientHistory,
          definition: Domain.definition,
          clientId,
          initialSpaces: [spaceA, spaceB, spaceC],
          foregroundReconciliationConcurrency: 2,
          retryDelay: "1 hour",
          maximumRetryDelay: "1 hour"
        }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.provide(layerServices),
          Layer.provide(WorkflowEngine.layerMemory)
        )

      const seedScope = yield* Scope.make()
      const unavailableRemote = SyncEngine.SyncEngine.of({
        ...remoteService,
        pull: () => Effect.fail(new ReplicaError.ServerUnavailable())
      })
      const seedContext = yield* Layer.buildWithScope(replicaLayer(unavailableRemote), seedScope)
      const seedSpace = yield* Context.get(seedContext, Replica.Replica).space(spaceC)
      yield* seedSpace.mutate(Domain.PutTodo, Domain.todo("background"))
      yield* seedSpace.deactivate
      yield* Scope.close(seedScope, Exit.void)

      const firstPin = yield* makePin
      const secondPin = yield* makePin
      const backgroundPin = yield* makePin
      const remote = pinnedRemote(new Map([[spaceA, firstPin], [spaceB, secondPin], [spaceC, backgroundPin]]))
      const replica = Context.get(yield* Layer.build(replicaLayer(remote)), Replica.Replica)
      yield* Deferred.await(backgroundPin.entered)
      const first = yield* replica.space(spaceA)
      const second = yield* replica.space(spaceB)
      const third = yield* replica.space(spaceC)
      yield* first.mutate(Domain.PutTodo, Domain.todo("pinned"))
      yield* second.mutate(Domain.PutTodo, Domain.todo("pinned"))
      yield* Deferred.await(firstPin.entered)
      yield* Deferred.await(secondPin.entered)

      const promotion = yield* Effect.forkChild(third.activate, { startImmediately: true }).pipe(
        Effect.provideService(Scheduler.PreventSchedulerYield, true)
      )
      yield* Effect.yieldNow
      yield* Fiber.interrupt(promotion)
      assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(promotion)))

      yield* Deferred.succeed(firstPin.release, undefined)
      yield* third.get(Domain.Todo, "background")
      assert.strictEqual(yield* first.activation, "Inactive")
      assert.strictEqual(yield* second.activation, "Active")
      assert.strictEqual(yield* third.activation, "Active")
    }, Effect.scoped)
  )

  it.effect(
    "resumes background reconciliation after an interrupted promotion preempts it",
    Effect.fnUntraced(function*() {
      const services = yield* probedServices(() => undefined)
      const viewId = Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000002")
      const replicaLayer = (remote: SyncEngine.SyncEngine["Service"]) =>
        SqlReplica.layer({
          ...clientHistory,
          definition: Domain.definition,
          clientId,
          initialSpaces: [spaceA],
          retryDelay: "1 hour",
          maximumRetryDelay: "1 hour"
        }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.provide(services.layer)
        )

      const seedScope = yield* Scope.make()
      const seedContext = yield* Layer.buildWithScope(replicaLayer(remoteService), seedScope)
      const seedSpace = yield* Context.get(seedContext, Replica.Replica).space(spaceA)
      yield* seedSpace.mutate(Domain.PutTodo, Domain.todo("resumed"))
      yield* seedSpace.deactivate
      yield* Scope.close(seedScope, Exit.void)
      yield* services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`

      const pullEntered = yield* Deferred.make<void>()
      const pullInterrupted = yield* Deferred.make<void>()
      let pulls = 0
      const remote = SyncEngine.SyncEngine.of({
        ...remoteService,
        submitBatch: acceptSubmission,
        pull: (request) => {
          pulls += 1
          if (pulls > 1) return viewPage(services.crypto, viewId, request, [], false)
          return Deferred.succeed(pullEntered, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(pullInterrupted, undefined))
          )
        }
      })
      const replica = Context.get(yield* Layer.build(replicaLayer(remote)), Replica.Replica)
      yield* Deferred.await(pullEntered)
      const space = yield* replica.space(spaceA)
      const promotion = yield* Effect.forkChild(space.activate, { startImmediately: true }).pipe(
        Effect.provideService(Scheduler.PreventSchedulerYield, true)
      )
      yield* Fiber.interrupt(promotion)
      yield* Deferred.await(pullInterrupted)

      yield* awaitNoPending(replica, services.reactivity)
      const aggregate = yield* replica.status
      assert.strictEqual(aggregate.totalPending, 0)
      assert.strictEqual(aggregate.counts.connecting, 0)
    }, Effect.scoped)
  )

  it.effect(
    "invalidates entities from a partial catch up page when promotion preempts the rest",
    Effect.fnUntraced(function*() {
      const services = yield* probedServices(() => undefined)
      const viewId = Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000003")
      const replicaLayer = (remote: SyncEngine.SyncEngine["Service"]) =>
        SqlReplica.layer({
          ...clientHistory,
          definition: Domain.definition,
          clientId,
          initialSpaces: [spaceA],
          retryDelay: "1 hour",
          maximumRetryDelay: "1 hour"
        }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.provide(services.layer)
        )

      const seedScope = yield* Scope.make()
      const seedContext = yield* Layer.buildWithScope(replicaLayer(remoteService), seedScope)
      const seedSpace = yield* Context.get(seedContext, Replica.Replica).space(spaceA)
      yield* seedSpace.mutate(Domain.PutTodo, Domain.todo("local"))
      yield* seedSpace.deactivate
      yield* Scope.close(seedScope, Exit.void)
      yield* services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`

      const remoteChange = Protocol.Upsert.make({
        entity: Protocol.EntityKey.make({
          model: Domain.Todo.name,
          modelVersion: Domain.Todo.version,
          key: "remote"
        }),
        value: Domain.todo("remote", "from server")
      })
      const secondPullEntered = yield* Deferred.make<void>()
      let pulls = 0
      const remote = SyncEngine.SyncEngine.of({
        ...remoteService,
        submitBatch: acceptSubmission,
        pull: (request) => {
          pulls += 1
          if (pulls === 1) return viewPage(services.crypto, viewId, request, [remoteChange], true)
          if (pulls === 2) return Deferred.succeed(secondPullEntered, undefined).pipe(Effect.andThen(Effect.never))
          return viewPage(services.crypto, viewId, request, [], false)
        }
      })
      let invalidations = 0
      const remoteKey = ReactivityKey.entity(spaceA, Domain.Todo.name, "remote")
      yield* Effect.acquireRelease(
        Effect.sync(() =>
          services.reactivity.registerUnsafe([remoteKey], () => {
            invalidations += 1
          })
        ),
        (unregister) => Effect.sync(unregister)
      )
      const replica = Context.get(yield* Layer.build(replicaLayer(remote)), Replica.Replica)
      yield* Deferred.await(secondPullEntered)
      const space = yield* replica.space(spaceA)
      yield* space.get(Domain.Todo, "local")

      yield* awaitNoPending(replica, services.reactivity)
      const synced = yield* space.get(Domain.Todo, "remote")
      assert.strictEqual(Option.getOrThrow(synced).title, "from server")
      assert.isAbove(invalidations, 0)
    }, Effect.scoped)
  )

  it.effect(
    "serves a get on another space while settlement streams stay open on every foreground slot",
    Effect.fnUntraced(function*() {
      const spaceC = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
      const reads = new Map([
        [spaceA, yield* Deferred.make<void>()],
        [spaceB, yield* Deferred.make<void>()]
      ])
      const { layer: layerServices } = yield* probedServices(settlementReadProbe(reads))
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition: Domain.definition,
        clientId,
        initialSpaces: [spaceA, spaceB, spaceC]
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(layerServices)
      )
      const replica = Context.get(yield* Layer.build(layerReplica), Replica.Replica)
      const first = yield* replica.space(spaceA)
      const second = yield* replica.space(spaceB)
      const third = yield* replica.space(spaceC)
      const firstStream = yield* first.settlements().pipe(
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true })
      )
      const secondStream = yield* second.settlements().pipe(
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.forEach(reads.values(), Deferred.await, { discard: true })

      assert.isTrue(Option.isNone(yield* third.get(Domain.Todo, "free")))
      assert.strictEqual(yield* third.activation, "Active")
      assert.isUndefined(firstStream.pollUnsafe())
      assert.isUndefined(secondStream.pollUnsafe())
    }, Effect.scoped)
  )

  it.effect(
    "delivers settlements to one live stream across background, foreground, and re-activated runtimes",
    Effect.fnUntraced(function*() {
      const spaceC = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
      const reads = new Map([[spaceA, yield* Deferred.make<void>()]])
      const services = yield* probedServices(settlementReadProbe(reads))
      const viewId = Identity.ReplicationViewId.make("viw_00000000-0000-4000-8000-000000000004")
      const replicaLayer = (remote: SyncEngine.SyncEngine["Service"]) =>
        SqlReplica.layer({
          ...clientHistory,
          definition: Domain.definition,
          clientId,
          initialSpaces: [spaceA, spaceB, spaceC],
          retryDelay: "1 hour",
          maximumRetryDelay: "1 hour"
        }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.provide(services.layer)
        )

      const seedScope = yield* Scope.make()
      const seedContext = yield* Layer.buildWithScope(replicaLayer(remoteService), seedScope)
      const seedSpace = yield* Context.get(seedContext, Replica.Replica).space(spaceA)
      const background = yield* seedSpace.mutate(Domain.PutTodo, Domain.todo("background"))
      yield* seedSpace.deactivate
      yield* Scope.close(seedScope, Exit.void)
      yield* services.sql`UPDATE effect_local_client_spaces
        SET replication_view_id = ${viewId}, replication_view_revision = 0`

      const backgroundSubmitted = yield* Deferred.make<void>()
      const releaseBackground = yield* Deferred.make<void>()
      let serverSequence = 0
      let submissions = 0
      const remote = SyncEngine.SyncEngine.of({
        ...remoteService,
        submitBatch: (request) => {
          submissions += 1
          const accept = Effect.sync(() =>
            Protocol.SubmitBatchResult.make({
              receipts: request.envelopes.map((envelope) => {
                serverSequence += 1
                return Protocol.AcceptedReceipt.make({
                  ...envelope,
                  serverSequence: Identity.ServerSequence.make(serverSequence),
                  result: Domain.todo(envelope.mutationId, "accepted")
                })
              })
            })
          )
          if (submissions > 1) return accept
          return Deferred.succeed(backgroundSubmitted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseBackground)),
            Effect.andThen(accept)
          )
        },
        pull: (request) => viewPage(services.crypto, viewId, request, [], false, serverSequence)
      })
      const replica = Context.get(yield* Layer.build(replicaLayer(remote)), Replica.Replica)
      yield* Deferred.await(backgroundSubmitted)
      const first = yield* replica.space(spaceA)
      const second = yield* replica.space(spaceB)
      const third = yield* replica.space(spaceC)
      const received = yield* Queue.unbounded<Replica.SettledMutation>()
      yield* first.settlements().pipe(
        Stream.runForEach((settled) => Queue.offer(received, settled)),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.forEach(reads.values(), Deferred.await, { discard: true })

      yield* Deferred.succeed(releaseBackground, undefined)
      const fromBackground = yield* Queue.take(received)
      assert.strictEqual(fromBackground.settlement.pending.envelope.mutationId, background.envelope.mutationId)

      const foreground = yield* first.mutate(Domain.PutTodo, Domain.todo("foreground"))
      const fromForeground = yield* Queue.take(received)
      assert.strictEqual(fromForeground.settlement.pending.envelope.mutationId, foreground.envelope.mutationId)

      yield* second.get(Domain.Todo, "evict")
      yield* third.get(Domain.Todo, "evict")
      assert.strictEqual(yield* first.activation, "Inactive")

      const reactivated = yield* first.mutate(Domain.PutTodo, Domain.todo("reactivated"))
      const fromReactivated = yield* Queue.take(received)
      assert.strictEqual(fromReactivated.settlement.pending.envelope.mutationId, reactivated.envelope.mutationId)
      assert.deepStrictEqual(
        [fromBackground.sequence, fromForeground.sequence, fromReactivated.sequence],
        [fromBackground.sequence, fromBackground.sequence + 1, fromBackground.sequence + 2]
      )
    }, Effect.scoped)
  )

  it.effect(
    "fails a live settlement stream with SpaceUnavailable when its space is left",
    Effect.fnUntraced(function*() {
      const reads = new Map([[spaceA, yield* Deferred.make<void>()]])
      const { layer: layerServices } = yield* probedServices(settlementReadProbe(reads))
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition: Domain.definition,
        clientId,
        initialSpaces: [spaceA]
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(layerServices)
      )
      const replica = Context.get(yield* Layer.build(layerReplica), Replica.Replica)
      const space = yield* replica.space(spaceA)
      const stream = yield* space.settlements().pipe(Stream.runDrain, Effect.forkChild({ startImmediately: true }))
      yield* Effect.forEach(reads.values(), Deferred.await, { discard: true })

      yield* replica.leave(spaceA)
      const outcome = yield* Fiber.join(stream).pipe(Effect.result)
      assert.strictEqual(outcome._tag, "Failure")
      if (outcome._tag === "Failure") assert.strictEqual(outcome.failure._tag, "SpaceUnavailable")
    }, Effect.scoped)
  )
})
