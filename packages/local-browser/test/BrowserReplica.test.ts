import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Transaction from "@lucas-barake/effect-local/Transaction"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import { AtomRegistry } from "effect/unstable/reactivity"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as BrowserReplica from "../src/BrowserReplica.js"
import * as platform from "../src/internal/platform.js"
import * as ReplicaAtom from "../src/ReplicaAtom.js"
import * as testKit from "./multiTabKit.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000401")
const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000401")
const TodoSchema = Schema.Struct({ id: Schema.String, title: Schema.String })
const Todo = Model.make("Todo", { version: 1, key: Schema.String, schema: TodoSchema })
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema })
const AppendTitle = Mutation.make("AppendTitle", {
  version: 1,
  payload: { id: Schema.String, suffix: Schema.String }
})
const ListTodos = Query.make("ListTodos", { success: Schema.Array(Todo.schema) })
const RunIndex = Query.make("RunIndex", { success: Schema.Number })
const definition = Definition.make({
  version: 1,
  models: [Todo],
  mutations: [PutTodo, AppendTitle],
  queries: [ListTodos, RunIndex]
})

const TodoRow = Schema.Struct({ value: Schema.fromJsonString(TodoSchema) })
const listTodos = (query: Transaction.Query) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: TodoRow,
    execute: () => query.sql([Todo], (sql) => sql`SELECT "value" FROM "Todo" ORDER BY "key"`)
  })(undefined).pipe(
    Effect.map((rows) => rows.map((row) => row.value)),
    Effect.catchTag("SchemaError", (cause) => Effect.die(cause))
  )

type ListTodosError = Effect.Error<ReturnType<typeof listTodos>>

const layerHandlersWith = (runIndex: (query: Transaction.Query) => Effect.Effect<number, ListTodosError>) =>
  Layer.mergeAll(
    PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload)),
    AppendTitle.toLayer(({ payload, transaction }) =>
      transaction.get(Todo, payload.id).pipe(
        Effect.map(Option.match({
          onNone: () => payload.suffix,
          onSome: (todo) => `${todo.title}${payload.suffix}`
        })),
        Effect.flatMap((title) => transaction.set(Todo, payload.id, { id: payload.id, title }))
      )
    ),
    ListTodos.toLayer(({ query }) => listTodos(query)),
    RunIndex.toLayer(({ query }) => runIndex(query))
  )

const firstRunIndex = (query: Transaction.Query) => listTodos(query).pipe(Effect.as(0))

const layerHandlers = layerHandlersWith(firstRunIndex)

const layerServerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)

const layerServer = ServerStore.layerTrusted({
  definition,
  readAuthorizationRefreshInterval: "30 seconds",
  maximumWatchersPerSpace: 1_024,
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
}).pipe(
  Layer.provide(MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))),
  Layer.provide(layerServerDatabase)
)

const layerEphemeralInactive = Layer.succeed(EphemeralClient.EphemeralClient, {
  session: () => Effect.never,
  publish: () => Effect.void,
  clear: () => Effect.void,
  remove: () => Effect.void
})

const StatusProfile = Ephemeral.member({ status: Schema.String })
const member = Protocol.EphemeralMember.make({
  clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000401"),
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000401")
})

const layerEphemeralOpening = (
  opening: Effect.Effect<void, ReplicaError.ReplicaError>,
  updates: Ref.Ref<ReadonlyArray<unknown>>
) =>
  Layer.succeed(EphemeralClient.EphemeralClient, {
    session: (_profile, options) =>
      opening.pipe(Effect.as({
        spaceId: options.spaceId,
        member: options.member,
        events: () => Stream.never,
        state: () => Stream.never,
        members: Stream.never,
        updateMember: (value: unknown) => Ref.update(updates, (values) => [...values, value])
      })),
    publish: () => Effect.void,
    clear: () => Effect.void,
    remove: () => Effect.void
  })

const settle = Effect.fnUntraced(function*<A, E extends { readonly _tag: string },>(effect: Effect.Effect<A, E>) {
  const fiber = yield* Effect.forkChild(effect)
  yield* TestClock.adjust("5 seconds")
  return yield* Fiber.join(fiber)
})

const provideFileSystem = Effect.provide(NodeFileSystem.layer)

interface EnvironmentOptions {
  readonly layerEphemeral?: Layer.Layer<EphemeralClient.EphemeralClient>
  readonly submitAllowed?: () => boolean
  readonly sharding?: BrowserReplica.Options<typeof definition, never, never>["sharding"]
  readonly runIndex?: (query: Transaction.Query) => Effect.Effect<number, ListTodosError>
  readonly name?: string
  readonly kit?: testKit.MemoryPlatform
  readonly retryDelay?: BrowserReplica.Options<typeof definition, never, never>["retryDelay"]
}

const makeEnvironmentWith = Effect.fnUntraced(function*(environmentOptions: EnvironmentOptions) {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  const kit = environmentOptions.kit ?? (yield* testKit.makeMemoryPlatform)
  const store = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
  const databaseOpens = yield* Ref.make(0)
  const layerSync = Layer.merge(
    Layer.succeed(SyncEngine.SyncEngine, {
      waitForCredentialChange: () => Effect.never,
      transportGeneration: Effect.succeed(0),
      waitForTransportChange: () => Effect.never,
      submit: (request) =>
        Effect.suspend(() => {
          if (environmentOptions.submitAllowed?.() ?? true) return store.submit(request)
          return Effect.never
        }),
      discard: (request) => store.discard(request, null),
      pull: store.pull,
      bootstrap: store.bootstrap,
      watch: store.watch
    }),
    environmentOptions.layerEphemeral ?? layerEphemeralInactive
  )
  const layerDatabase = SqliteClient.layer({ filename: `${directory}/replica.sqlite` }).pipe(
    Layer.tap(() => Ref.update(databaseOpens, (count) => count + 1))
  )
  const layerReplicaWith = (visibility: platform.TabVisibilityService) =>
    BrowserReplica.layer({
      name: environmentOptions.name ?? "tabs",
      definition,
      layerDatabase,
      layerSync,
      spaces: [spaceId],
      profiles: { status: StatusProfile },
      layerPlatform: Layer.merge(kit.layerAll, Layer.succeed(platform.TabVisibility, visibility)),
      requestPersistence: false,
      retryDelay: environmentOptions.retryDelay ?? "100 millis",
      sharding: environmentOptions.sharding
    }).pipe(Layer.provide(layerHandlersWith(environmentOptions.runIndex ?? firstRunIndex)))
  const layerReplica = Layer.unwrap(
    testKit.makeMemoryVisibility(true).pipe(Effect.map((visibility) => layerReplicaWith(visibility.service)))
  )
  const openTabWith = Effect.fnUntraced(function*(visible: boolean) {
    const visibility = yield* testKit.makeMemoryVisibility(visible)
    const layerTab = layerReplicaWith(visibility.service).pipe(Layer.provide(Layer.fresh(Reactivity.layer)))
    const scope = yield* Scope.make()
    const context = yield* settle(Layer.buildWithScope(layerTab, scope))
    return { scope, context, replica: Context.get(context, Replica.Replica), visibility }
  })
  const openTab = openTabWith(true)
  return { openTab, openTabWith, databaseOpens, layerReplica, layerReplicaWith, traffic: kit.traffic }
})

const makeEnvironment = makeEnvironmentWith({})

const makeGatedRunIndex = Effect.gen(function*() {
  const started = yield* Queue.unbounded<Deferred.Deferred<void>>()
  let runs = 0
  const runIndex = Effect.fnUntraced(function*(query: Transaction.Query) {
    runs += 1
    const index = runs
    const gate = yield* Deferred.make<void>()
    yield* Queue.offer(started, gate)
    yield* Deferred.await(gate)
    yield* listTodos(query)
    return index
  })
  return { started, runIndex }
})

const openStatusSession = (context: Context.Context<EphemeralClient.EphemeralClient>) =>
  Context.get(context, EphemeralClient.EphemeralClient).session(StatusProfile, {
    spaceId,
    member,
    value: { status: "online" },
    ttl: "30 seconds"
  })

const listFrom = (replica: Replica.Service) =>
  replica.space(spaceId).pipe(Effect.flatMap((space) => space.query(ListTodos, undefined)))

describe("BrowserReplica", () => {
  it.effect(
    "serves a follower tab's mutations and queries from the leader tab's replica",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        yield* settle(space.mutate(PutTodo, { id: "1", title: "from the follower" }))
        assert.deepStrictEqual(yield* settle(listFrom(follower.replica)), [{ id: "1", title: "from the follower" }])
        assert.deepStrictEqual(yield* settle(listFrom(leader.replica)), [{ id: "1", title: "from the follower" }])
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 1)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "refreshes a follower's live query when the leader tab writes",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        const graph = ReplicaAtom.make(environment.layerReplica)
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const todos = graph.query(spaceId, ListTodos)(undefined)
        const unmount = registry.mount(todos)
        yield* Effect.addFinalizer(() => Effect.sync(unmount))
        assert.deepStrictEqual(yield* settle(AtomRegistry.getResult(registry, todos, { suspendOnWaiting: true })), [])
        const space = yield* settle(leader.replica.space(spaceId))
        yield* settle(space.mutate(PutTodo, { id: "2", title: "from the leader" }))
        assert.deepStrictEqual(
          yield* settle(AtomRegistry.getResult(registry, todos, { suspendOnWaiting: true })),
          [{ id: "2", title: "from the leader" }]
        )
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "renders a follower's own write in its live query without advancing the clock",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        yield* environment.openTab
        const graph = ReplicaAtom.make(
          environment.layerReplica.pipe(Layer.provideMerge(Layer.succeed(Clock.Clock, yield* Clock.Clock)))
        )
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const todos = graph.query(spaceId, ListTodos)(undefined)
        const put = graph.mutation(spaceId, PutTodo)
        const unmountTodos = registry.mount(todos)
        const unmountPut = registry.mount(put)
        yield* Effect.addFinalizer(() => Effect.sync(() => [unmountTodos(), unmountPut()]))
        assert.deepStrictEqual(yield* settle(AtomRegistry.getResult(registry, todos, { suspendOnWaiting: true })), [])
        const rendered = yield* Deferred.make<ReadonlyArray<typeof TodoSchema.Type>>()
        const unsubscribe = registry.subscribe(todos, (result) => {
          if (AsyncResult.isSuccess(result) && result.value.length > 0) {
            Deferred.doneUnsafe(rendered, Effect.succeed(result.value))
          }
        })
        yield* Effect.addFinalizer(() => Effect.sync(unsubscribe))
        registry.set(put, { id: "own", title: "rendered without a timer" })
        assert.deepStrictEqual(yield* Deferred.await(rendered), [{ id: "own", title: "rendered without a timer" }])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "lets a follower's in-flight live query finish when it is invalidated again, then reruns it once",
    Effect.fnUntraced(
      function*() {
        const { started, runIndex: gatedRunIndex } = yield* makeGatedRunIndex
        const environment = yield* makeEnvironmentWith({ runIndex: gatedRunIndex })
        yield* environment.openTab
        const graph = ReplicaAtom.make(environment.layerReplica)
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const runIndex = graph.query(spaceId, RunIndex)(undefined)
        const shown: Array<number> = []
        const unsubscribe = registry.subscribe(runIndex, (result) => {
          if (AsyncResult.isSuccess(result) && !result.waiting) shown.push(result.value)
        }, { immediate: true })
        yield* Effect.addFinalizer(() => Effect.sync(unsubscribe))
        const reactivityAtom = graph.runtime.atom(Effect.service(Reactivity.Reactivity))
        const reactivity = yield* settle(AtomRegistry.getResult(registry, reactivityAtom, { suspendOnWaiting: true }))
        const invalidate = reactivity.invalidate([ReactivityKey.query(spaceId, RunIndex.name, undefined)])
        yield* Deferred.succeed(yield* settle(Queue.take(started)), undefined)
        assert.strictEqual(yield* settle(AtomRegistry.getResult(registry, runIndex, { suspendOnWaiting: true })), 1)
        yield* invalidate
        const second = yield* settle(Queue.take(started))
        yield* invalidate
        yield* Deferred.succeed(second, undefined)
        yield* Deferred.succeed(yield* settle(Queue.take(started)), undefined)
        assert.strictEqual(yield* settle(AtomRegistry.getResult(registry, runIndex, { suspendOnWaiting: true })), 3)
        assert.deepStrictEqual(shown, [1, 2, 3])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "reruns an invalidated in-flight live query that is remounted before its read completes",
    Effect.fnUntraced(
      function*() {
        const { started, runIndex: gatedRunIndex } = yield* makeGatedRunIndex
        const environment = yield* makeEnvironmentWith({ runIndex: gatedRunIndex })
        yield* environment.openTab
        const graph = ReplicaAtom.make(environment.layerReplica, { idleTTL: 0 })
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const runIndex = graph.query(spaceId, RunIndex)(undefined)
        const unmount = registry.mount(runIndex)
        const reactivityAtom = graph.runtime.atom(Effect.service(Reactivity.Reactivity))
        const unmountReactivity = registry.mount(reactivityAtom)
        yield* Effect.addFinalizer(() => Effect.sync(unmountReactivity))
        const reactivity = yield* settle(AtomRegistry.getResult(registry, reactivityAtom, { suspendOnWaiting: true }))
        const invalidate = reactivity.invalidate([ReactivityKey.query(spaceId, RunIndex.name, undefined)])
        yield* Deferred.succeed(yield* settle(Queue.take(started)), undefined)
        assert.strictEqual(yield* settle(AtomRegistry.getResult(registry, runIndex, { suspendOnWaiting: true })), 1)
        yield* invalidate
        const second = yield* settle(Queue.take(started))
        yield* invalidate
        const inFlight = Array.from(registry.getNodes().values()).filter((node) => {
          if (node.atom === runIndex || node.currentState() !== "valid") return false
          const value = node.value()
          return AsyncResult.isAsyncResult(value) && AsyncResult.isSuccess(value) && value.waiting && value.value === 1
        })
        assert.strictEqual(inFlight.length, 2)
        const remounted = yield* Deferred.make<() => void>()
        registry.onNodeRemoved = (node) => {
          if (!inFlight.includes(node)) return
          registry.onNodeRemoved = undefined
          const unmountRemounted = registry.mount(runIndex)
          Deferred.doneUnsafe(remounted, Effect.succeed(unmountRemounted))
        }
        unmount()
        const unmountAgain = yield* Deferred.await(remounted)
        yield* Effect.addFinalizer(() => Effect.sync(unmountAgain))
        yield* Deferred.succeed(second, undefined)
        const outcome = yield* settle(Effect.raceFirst(
          Queue.take(started).pipe(Effect.as("reran" as const)),
          AtomRegistry.getResult(registry, runIndex, { suspendOnWaiting: true }).pipe(
            Effect.as("settled on the invalidated read" as const)
          )
        ))
        assert.strictEqual(outcome, "reran")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "keeps a replica's tabs apart from a replica whose name extends its lock namespace",
    Effect.fnUntraced(
      function*() {
        const kit = yield* testKit.makeMemoryPlatform
        const neighbour = yield* makeEnvironmentWith({ kit, name: "tabs:visible" })
        yield* neighbour.openTabWith(true)
        const environment = yield* makeEnvironmentWith({ kit, name: "tabs" })
        const tab = yield* environment.openTabWith(false)
        assert.deepStrictEqual(yield* settle(listFrom(tab.replica)), [])
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 1)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "moves the replica to a visible tab once the leader tab is hidden",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTabWith(true)
        const follower = yield* environment.openTabWith(false)
        const space = yield* settle(follower.replica.space(spaceId))
        yield* settle(space.mutate(PutTodo, { id: "1", title: "before the switch" }))
        yield* settle(leader.visibility.set(false))
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 1)
        yield* follower.visibility.set(true)
        yield* settle(space.mutate(PutTodo, { id: "2", title: "during the switch" }))
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 2)
        yield* settle(Scope.close(leader.scope, Exit.void))
        assert.deepStrictEqual(yield* settle(listFrom(follower.replica)), [
          { id: "1", title: "before the switch" },
          { id: "2", title: "during the switch" }
        ])
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 2)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "hands the replica over without waiting for shard lock refresh or entity termination timers",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironmentWith({
          sharding: { shardLockRefreshInterval: "1 hour", entityTerminationTimeout: "1 hour" }
        })
        const leader = yield* environment.openTabWith(true)
        const follower = yield* environment.openTabWith(false)
        const space = yield* settle(follower.replica.space(spaceId))
        yield* space.settlements({ from: "live" }).pipe(Stream.runDrain, Effect.forkScoped)
        yield* settle(space.mutate(PutTodo, { id: "1", title: "before the switch" }))
        yield* leader.visibility.set(false)
        yield* follower.visibility.set(true)
        yield* settle(space.mutate(PutTodo, { id: "2", title: "during the switch" }))
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 2)
        assert.deepStrictEqual(yield* settle(listFrom(follower.replica)), [
          { id: "1", title: "before the switch" },
          { id: "2", title: "during the switch" }
        ])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "resubscribes a follower's live query as soon as a handover completes",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironmentWith({ retryDelay: "1 hour" })
        const leader = yield* environment.openTabWith(true)
        const next = yield* environment.openTabWith(false)
        const hidden = yield* testKit.makeMemoryVisibility(false)
        const graph = ReplicaAtom.make(environment.layerReplicaWith(hidden.service))
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const todos = graph.query(spaceId, ListTodos)(undefined)
        const unmount = registry.mount(todos)
        yield* Effect.addFinalizer(() => Effect.sync(unmount))
        assert.deepStrictEqual(yield* settle(AtomRegistry.getResult(registry, todos, { suspendOnWaiting: true })), [])
        yield* leader.visibility.set(false)
        yield* next.visibility.set(true)
        const space = yield* settle(next.replica.space(spaceId))
        yield* settle(space.mutate(PutTodo, { id: "3", title: "written on the new leader" }))
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 2)
        assert.deepStrictEqual(
          yield* settle(AtomRegistry.getResult(registry, todos, { suspendOnWaiting: true })),
          [{ id: "3", title: "written on the new leader" }]
        )
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "serves the visible tab through rapid visibility flips without losing or repeating a mutation",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const tabs = [yield* environment.openTabWith(true), yield* environment.openTabWith(false)]
        const spaces = [
          yield* settle(tabs[0].replica.space(spaceId)),
          yield* settle(tabs[1].replica.space(spaceId))
        ]
        const suffixes = "abcdefgh"
        const writes: Array<Fiber.Fiber<Protocol.PendingMutation, ReplicaError.ReplicaError>> = []
        let visible = 0
        for (const suffix of suffixes) {
          const hiding = visible
          visible = 1 - visible
          yield* tabs[visible].visibility.set(true)
          yield* tabs[hiding].visibility.set(false)
          writes.push(yield* Effect.forkChild(spaces[visible].mutate(AppendTitle, { id: "log", suffix })))
        }
        yield* settle(Fiber.joinAll(writes))
        const [log] = yield* settle(listFrom(tabs[visible].replica))
        assert.isDefined(log)
        assert.strictEqual(Array.from(log.title).toSorted().join(""), suffixes)
        assert.deepStrictEqual(yield* settle(listFrom(tabs[1 - visible].replica)), [log])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "hands the replica to a visible tab ahead of a hidden tab that asked first",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const first = yield* environment.openTabWith(true)
        const hidden = yield* environment.openTabWith(false)
        const visible = yield* environment.openTabWith(true)
        yield* settle(Scope.close(first.scope, Exit.void))
        yield* settle(visible.replica.space(spaceId))
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 2)
        yield* settle(Scope.close(hidden.scope, Exit.void))
        yield* settle(visible.replica.space(spaceId))
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 2)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "keeps serving the same replica from the follower after the leader tab closes",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        const before = yield* settle(space.mutate(PutTodo, { id: "3", title: "before" }, { mutationId }))
        yield* settle(Scope.close(leader.scope, Exit.void))
        const replayed = yield* settle(space.mutate(PutTodo, { id: "3", title: "before" }, { mutationId }))
        assert.deepStrictEqual(replayed.envelope, before.envelope)
        yield* settle(space.mutate(PutTodo, { id: "4", title: "after" }))
        assert.deepStrictEqual(yield* settle(listFrom(follower.replica)), [
          { id: "3", title: "before" },
          { id: "4", title: "after" }
        ])
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 2)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "streams the leader's settlements to a follower",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        const pending = yield* settle(space.mutate(PutTodo, { id: "5", title: "settles" }))
        const settled = yield* settle(space.settlements({ from: 0 }).pipe(Stream.runHead))
        assert.isTrue(settled._tag === "Some")
        if (settled._tag === "Some") {
          assert.strictEqual(settled.value.settlement.pending.envelope.mutationId, pending.envelope.mutationId)
          assert.strictEqual(settled.value.settlement.receipt._tag, "Accepted")
        }
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "delivers a settlement that lands during failover to a live settlement stream opened before it",
    Effect.fnUntraced(
      function*() {
        let submitAllowed = false
        const environment = yield* makeEnvironmentWith({ submitAllowed: () => submitAllowed })
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        const received = yield* Effect.forkChild(
          space.settlements({ from: "live" }).pipe(Stream.runHead, Effect.timeoutOption("60 seconds"))
        )
        yield* TestClock.adjust("5 seconds")
        const pending = yield* settle(space.mutate(PutTodo, { id: "6", title: "settles on the new leader" }))
        submitAllowed = true
        yield* settle(Scope.close(leader.scope, Exit.void))
        yield* TestClock.adjust("60 seconds")
        const settled = Option.flatten(yield* Fiber.join(received))
        assert.isTrue(Option.isSome(settled))
        if (Option.isSome(settled)) {
          assert.strictEqual(settled.value.settlement.pending.envelope.mutationId, pending.envelope.mutationId)
        }
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "updates a follower's ephemeral member after the leader tab closes",
    Effect.fnUntraced(
      function*() {
        const opens = yield* Ref.make(0)
        const reopened = yield* Deferred.make<void>()
        const opening = Ref.getAndUpdate(opens, (count) => count + 1).pipe(
          Effect.flatMap((count) => {
            if (count === 0) return Effect.void
            return Deferred.await(reopened)
          })
        )
        const updates = yield* Ref.make<ReadonlyArray<unknown>>([])
        const environment = yield* makeEnvironmentWith({ layerEphemeral: layerEphemeralOpening(opening, updates) })
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const session = yield* settle(openStatusSession(follower.context).pipe(Scope.provide(yield* Effect.scope)))
        yield* settle(Scope.close(leader.scope, Exit.void))
        assert.strictEqual(yield* Ref.get(opens), 2)
        const updated = yield* settle(session.updateMember({ status: "away" }).pipe(Effect.exit))
        assert.isTrue(Exit.isSuccess(updated), String(updated))
        yield* settle(Deferred.succeed(reopened, undefined))
        assert.deepStrictEqual(yield* Ref.get(updates), [{ status: "away" }])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "stops opening an ephemeral session whose first open failed",
    Effect.fnUntraced(
      function*() {
        const opens = yield* Ref.make(0)
        const opening = Ref.getAndUpdate(opens, (count) => count + 1).pipe(
          Effect.flatMap((count) => {
            if (count === 0) return Effect.fail(new ReplicaError.ServerUnavailable())
            return Effect.void
          })
        )
        const environment = yield* makeEnvironmentWith({
          layerEphemeral: layerEphemeralOpening(opening, yield* Ref.make<ReadonlyArray<unknown>>([]))
        })
        const tab = yield* environment.openTab
        const outcome = yield* settle(
          openStatusSession(tab.context).pipe(Scope.provide(yield* Effect.scope), Effect.exit)
        )
        assert.isTrue(Exit.isFailure(outcome), String(outcome))
        yield* TestClock.adjust("5 seconds")
        assert.strictEqual(yield* Ref.get(opens), 1)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "delivers each frame between two tabs only to its recipient",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        yield* environment.openTab
        const follower = yield* environment.openTab
        yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        const posted = environment.traffic.posted
        const delivered = environment.traffic.delivered
        yield* settle(space.mutate(PutTodo, { id: "7", title: "one recipient" }))
        yield* settle(space.query(ListTodos, undefined))
        assert.isAbove(environment.traffic.posted - posted, 0)
        assert.strictEqual(environment.traffic.delivered - delivered, environment.traffic.posted - posted)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "keeps serving a tab while more long-lived streams are open than the entity mailbox holds",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironmentWith({ sharding: { entityMailboxCapacity: 4 } })
        yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        for (let index = 0; index < 4; index++) {
          yield* space.settlements({ from: "live" }).pipe(Stream.runDrain, Effect.forkScoped)
        }
        yield* TestClock.adjust("5 seconds")
        assert.deepStrictEqual(yield* settle(space.query(ListTodos, undefined)), [])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "answers a follower's repeated space lookups without a leader round trip",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        yield* environment.openTab
        const follower = yield* environment.openTab
        yield* settle(follower.replica.space(spaceId))
        const posted = environment.traffic.posted
        yield* settle(follower.replica.space(spaceId))
        assert.strictEqual(environment.traffic.posted, posted)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "fails a follower's space lookup after the leader tab leaves the space",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        yield* settle(follower.replica.space(spaceId))
        yield* settle(leader.replica.leave(spaceId))
        const outcome = yield* settle(
          follower.replica.space(spaceId).pipe(
            Effect.as("joined" as const),
            Effect.catchTag("SpaceNotJoined", () => Effect.succeed("not joined" as const))
          )
        )
        assert.strictEqual(outcome, "not joined")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "keeps the acknowledgement floor at the tab's acknowledgement while an acknowledged stream is open",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        yield* settle(space.mutate(PutTodo, { id: "a", title: "a" }))
        yield* settle(space.mutate(PutTodo, { id: "b", title: "b" }))
        yield* settle(space.mutate(PutTodo, { id: "c", title: "c" }))
        assert.strictEqual(yield* settle(space.resolveSettlementStart("live")), 3)
        yield* space.settlements({ from: "live" }).pipe(Stream.runDrain, Effect.forkScoped)
        yield* TestClock.adjust("5 seconds")
        yield* space.settlements({ from: "acknowledged" }).pipe(Stream.runDrain, Effect.forkScoped)
        yield* TestClock.adjust("5 seconds")
        yield* settle(space.acknowledgeSettlements(1))
        assert.strictEqual(yield* settle(space.resolveSettlementStart("acknowledged")), 1)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "delivers each settlement once to a follower's settlement stream across a leader handover",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        const delivered = yield* Queue.unbounded<number>()
        yield* space.settlements({ from: "acknowledged" }).pipe(
          Stream.runForEach((settled) => Queue.offer(delivered, settled.sequence)),
          Effect.forkScoped
        )
        yield* TestClock.adjust("5 seconds")
        yield* settle(space.mutate(PutTodo, { id: "a", title: "a" }))
        yield* settle(space.mutate(PutTodo, { id: "b", title: "b" }))
        yield* settle(Scope.close(leader.scope, Exit.void))
        yield* settle(space.mutate(PutTodo, { id: "c", title: "c" }))
        assert.deepStrictEqual(yield* settle(Queue.takeN(delivered, 3)), [1, 2, 3])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "refreshes a follower's space status for a space it could not open once a new leader joins it",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        yield* environment.openTab
        yield* settle(leader.replica.leave(spaceId))
        const graph = ReplicaAtom.make(
          environment.layerReplica.pipe(Layer.provide(Layer.succeed(Clock.Clock, yield* Clock.Clock)))
        )
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const status = graph.status(spaceId)
        const unmount = registry.mount(status)
        yield* Effect.addFinalizer(() => Effect.sync(unmount))
        const read = AtomRegistry.getResult(registry, status, { suspendOnWaiting: true }).pipe(
          Effect.as("joined" as const),
          Effect.catchTag("SpaceNotJoined", () => Effect.succeed("not joined" as const))
        )
        assert.strictEqual(yield* settle(read), "not joined")
        yield* settle(Scope.close(leader.scope, Exit.void))
        assert.strictEqual(yield* settle(read), "joined")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "rejects a corrupt durable client identity with a storage error",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const layerPlatform = Layer.mergeAll(
        Layer.succeed(platform.TabChannel, kit.tabChannel),
        Layer.succeed(platform.WebLocks, kit.webLocks),
        Layer.succeed(platform.TabVisibility, (yield* testKit.makeMemoryVisibility(true)).service),
        Layer.succeed(platform.ClientIdentityStore, {
          load: () => Effect.succeed("not-a-client-id"),
          store: () => Effect.void
        })
      )
      const outcome = yield* Layer.build(
        BrowserReplica.layer({
          name: "corrupt-identity",
          definition,
          layerDatabase: SqliteClient.layer({ filename: ":memory:" }),
          layerSync: Layer.merge(
            Layer.succeed(SyncEngine.SyncEngine, {
              waitForCredentialChange: () => Effect.never,
              transportGeneration: Effect.succeed(0),
              waitForTransportChange: () => Effect.never,
              submit: () => Effect.never,
              discard: () => Effect.never,
              pull: () => Effect.never,
              bootstrap: () => Effect.never,
              watch: () => Stream.never
            }),
            layerEphemeralInactive
          ),
          layerPlatform,
          requestPersistence: false
        }).pipe(Layer.provide(layerHandlers), Layer.provide(Reactivity.layer))
      ).pipe(
        Effect.as("built" as const),
        Effect.catchTag("BrowserStorageError", (error) => Effect.succeed(error.operation))
      )
      assert.strictEqual(outcome, "decode")
    }, Effect.scoped)
  )
})
