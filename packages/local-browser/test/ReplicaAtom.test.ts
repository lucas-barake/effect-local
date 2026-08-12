import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as FaultInjection from "@lucas-barake/effect-local-test/FaultInjection"
import * as TestReplica from "@lucas-barake/effect-local-test/TestReplica"
import * as TestServer from "@lucas-barake/effect-local-test/TestServer"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Atom, AtomRegistry } from "effect/unstable/reactivity"
import * as BrowserReplica from "../src/BrowserReplica.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const secondSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const thirdSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const TodoSchema = Schema.Struct({ id: Schema.String, title: Schema.String })
const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: TodoSchema,
  indexes: {
    byTitle: {
      version: 1,
      partition: [],
      sort: [{
        name: "title",
        affinity: "text",
        schema: Schema.String,
        extract: (todo: typeof TodoSchema.Type) => todo.title
      }]
    }
  }
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const Numbered = Model.make("Numbered", {
  version: 1,
  key: Schema.NumberFromString,
  schema: Schema.Struct({ id: Schema.Number, value: Schema.String })
})
const PutNumbered = Mutation.make("PutNumbered", {
  version: 1,
  payload: Numbered.schema,
  success: Numbered.schema
})
const ListTodos = Query.make("ListTodos", {
  success: Schema.Array(Todo.schema)
})
const RangeTodos = Query.make("RangeTodos", {
  payload: { lower: Schema.String, upper: Schema.String },
  success: Schema.Array(Todo.schema)
})
const rangeReads = new Map<string, number>()
const definition = Definition.make({
  version: 1,
  models: [Todo, Numbered],
  mutations: [PutTodo, PutNumbered],
  queries: [ListTodos, RangeTodos]
})
const handlers = Layer.mergeAll(
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))),
  PutNumbered.toLayer(({ payload, transaction }) =>
    transaction.set(Numbered, payload.id, payload).pipe(Effect.as(payload))
  ),
  ListTodos.toLayer(({ query }) =>
    query.from(Todo, "byTitle").limit(100).page().pipe(Effect.map((page) => page.items))
  ),
  RangeTodos.toLayer(({ payload, query }) => {
    const key = `${payload.lower}:${payload.upper}`
    rangeReads.set(key, (rangeReads.get(key) ?? 0) + 1)
    return query.from(Todo, "byTitle")
      .where({ title: { gte: payload.lower, lt: payload.upper } })
      .limit(20)
      .page()
      .pipe(Effect.map((page) => page.items))
  })
)
const database = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)
const mutationRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(handlers))
const migration = {
  retryDelay: "1 millis",
  maximumAttempts: 8
} satisfies { readonly retryDelay: Duration.Input; readonly maximumAttempts: number }
const clientHistory = {
  scope: Protocol.ReplicationScope.make({ models: [Todo.name, Numbered.name] }),
  settlementCapacity: 64,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration
}
const server = ServerStore.layerTrusted({
  definition,
  readAuthorizationRefreshInterval: "30 seconds" as const,
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
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration
}).pipe(
  Layer.provide(mutationRuntime),
  Layer.provide(database)
)
const sync = Layer.effect(
  SyncEngine.SyncEngine,
  ServerStore.ServerStore.pipe(
    Effect.map((store) =>
      SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        submit: store.submit,
        discard: (request) => store.discard(request, null),
        pull: store.pull,
        bootstrap: store.bootstrap,
        watch: store.watch
      })
    )
  )
).pipe(Layer.provide(server))
const replica = SqlReplica.layer({
  ...clientHistory,
  definition,
  initialSpaces: [spaceId, secondSpaceId],
  clientId,
  retryDelay: "10 millis"
}).pipe(
  Layer.provide(sync),
  Layer.provide(database),
  Layer.provide(handlers)
)

const faultedReplica = (faultsReady: Deferred.Deferred<FaultInjection.Service>) => {
  const faults = FaultInjection.layer.pipe(
    Layer.tap((context) => Deferred.succeed(faultsReady, Context.get(context, FaultInjection.FaultInjection)))
  )
  const faultedSync = TestServer.layer.pipe(
    Layer.provide(server),
    Layer.provide(faults),
    Layer.provide(NodeCrypto.layer)
  )
  return TestReplica.layer({
    ...clientHistory,
    definition,
    initialSpaces: [spaceId],
    clientId,
    retryDelay: "10 millis"
  }).pipe(
    Layer.provide(faultedSync),
    Layer.provide(database),
    Layer.provide(handlers)
  )
}

describe("Replica Atom graph", () => {
  it.effect("reacts to pending mutation submission and settlement", () =>
    Effect.scoped(Effect.gen(function*() {
      const faultsReady = yield* Deferred.make<FaultInjection.Service>()
      const graph = BrowserReplica.make(faultedReplica(faultsReady))
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const pending = graph.pendingFor(spaceId, PutTodo)
      const mutation = graph.mutation(spaceId, PutTodo)
      const unmountPending = registry.mount(pending)
      const unmountMutation = registry.mount(mutation)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmountMutation()
          unmountPending()
        })
      )
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, pending), [])
      const faults = yield* Deferred.await(faultsReady)

      const id = `pending-atom-${Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")}`
      yield* faults.holdNextReceipt(spaceId)
      const submitted = yield* AtomRegistry.toStreamResult(registry, pending).pipe(
        Stream.filter((items) =>
          items.some((item) => item.payload.id === id && item.submissionState === "Submitting" && item.attempts > 0)
        ),
        Stream.runHead,
        Effect.forkScoped({ startImmediately: true })
      )
      registry.set(mutation, { id, title: "0-pending" })
      yield* faults.awaitReceiptCommitted(spaceId)
      const pendingItems = Option.getOrThrow(yield* Fiber.join(submitted))
      const item = pendingItems.find((candidate) => candidate.payload.id === id)
      assert.isDefined(item)
      assert.deepStrictEqual(item.payload, { id, title: "0-pending" })
      assert.strictEqual(item.submissionState, "Submitting")
      assert.strictEqual(item.attempts, 1)

      const settled = yield* AtomRegistry.toStreamResult(registry, pending).pipe(
        Stream.filter((items) => !items.some((candidate) => candidate.payload.id === id)),
        Stream.runHead,
        Effect.forkScoped({ startImmediately: true })
      )
      yield* faults.releaseHeldReceipt(spaceId)
      yield* faults.awaitReceiptReturned(spaceId)
      const settledItems = Option.getOrThrow(yield* Fiber.join(settled))
      assert.isFalse(settledItems.some((candidate) => candidate.payload.id === id))
    })))

  it.live("reruns only an indexed query whose result range can change", () =>
    Effect.gen(function*() {
      rangeReads.clear()
      const graph = BrowserReplica.make(replica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const related = graph.query(spaceId, RangeTodos)({ lower: "a", upper: "m" })
      const unrelated = graph.query(spaceId, RangeTodos)({ lower: "n", upper: "z" })
      const mutation = graph.mutation(spaceId, PutTodo)
      const unmountRelated = registry.mount(related)
      const unmountUnrelated = registry.mount(unrelated)
      const unmountMutation = registry.mount(mutation)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmountMutation()
          unmountUnrelated()
          unmountRelated()
        })
      )
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, related), [])
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, unrelated), [])
      assert.strictEqual(rangeReads.get("a:m"), 1)
      assert.strictEqual(rangeReads.get("n:z"), 1)

      registry.set(mutation, { id: "range", title: "beta" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, related), [{ id: "range", title: "beta" }])
      yield* Effect.sleep("20 millis")
      assert.isAtLeast(rangeReads.get("a:m") ?? 0, 2)
      assert.strictEqual(rangeReads.get("n:z"), 1)
    }))

  it.live("refreshes an entity atom when its key has a different encoded representation", () =>
    Effect.gen(function*() {
      const graph = BrowserReplica.make(replica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const entity = graph.entity(spaceId, Numbered)(1)
      const mutation = graph.mutation(spaceId, PutNumbered)
      const unmountEntity = registry.mount(entity)
      const unmountMutation = registry.mount(mutation)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmountMutation()
          unmountEntity()
        })
      )
      assert.isTrue(Option.isNone(yield* AtomRegistry.getResult(registry, entity)))
      registry.set(mutation, { id: 1, value: "encoded-key" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      assert.deepStrictEqual(Option.getOrThrow(yield* AtomRegistry.getResult(registry, entity)), {
        id: 1,
        value: "encoded-key"
      })
    }))

  it.live("does not rerun an unrelated mounted entity atom", () =>
    Effect.gen(function*() {
      let unrelatedReads = 0
      const observed = Layer.effect(
        Replica.Replica,
        Replica.Replica.pipe(
          Effect.map((service) =>
            Replica.Replica.of({
              ...service,
              space: (address) =>
                service.space(address).pipe(Effect.map((space) => ({
                  ...space,
                  get: (model, key) => {
                    if (model.name === Todo.name && Object.is(key, "unrelated")) unrelatedReads++
                    return space.get(model, key)
                  }
                })))
            })
          )
        )
      ).pipe(Layer.provideMerge(replica))
      const graph = BrowserReplica.make(observed)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const changed = graph.entity(spaceId, Todo)("changed")
      const unrelated = graph.entity(spaceId, Todo)("unrelated")
      const mutation = graph.mutation(spaceId, PutTodo)
      const unmountChanged = registry.mount(changed)
      const unmountUnrelated = registry.mount(unrelated)
      const unmountMutation = registry.mount(mutation)
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          unmountMutation()
          unmountUnrelated()
          unmountChanged()
        })
      )
      assert.isTrue(Option.isNone(yield* AtomRegistry.getResult(registry, changed)))
      assert.isTrue(Option.isNone(yield* AtomRegistry.getResult(registry, unrelated)))
      assert.strictEqual(unrelatedReads, 1)

      registry.set(mutation, { id: "changed", title: "new" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      assert.deepStrictEqual(Option.getOrThrow(yield* AtomRegistry.getResult(registry, changed)), {
        id: "changed",
        title: "new"
      })
      yield* Effect.sleep("20 millis")
      assert.strictEqual(unrelatedReads, 1)
    }))

  it("uses the shared runtime factory by default and preserves an explicit factory", () => {
    const graph = BrowserReplica.make(replica)
    assert.strictEqual(graph.factory, Atom.runtime)

    const factory = Atom.context({ memoMap: Layer.makeMemoMapUnsafe() })
    const customGraph = BrowserReplica.make(replica, { factory })
    assert.strictEqual(customGraph.factory, factory)
  })

  it("normalizes the configured idle duration once when the graph is constructed", () => {
    let reads = 0
    const idleTTL = {
      get milliseconds() {
        reads++
        return 17
      }
    } satisfies Duration.Input
    const graph = BrowserReplica.make(replica, { idleTTL })
    const readsAfterConstruction = reads

    assert.isAbove(readsAfterConstruction, 0)
    assert.strictEqual(graph.entity(spaceId, Todo)("1").idleTTL, 17)
    assert.strictEqual(graph.query(spaceId, ListTodos)(undefined).idleTTL, 17)
    assert.strictEqual(
      graph.receipt(
        spaceId,
        PutTodo,
        Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")
      ).idleTTL,
      17
    )
    assert.strictEqual(reads, readsAfterConstruction)
  })

  it.live(
    "runs mutation, entity, query, receipt, and status state through one reactive runtime",
    () =>
      Effect.gen(function*() {
        const graph = BrowserReplica.make(replica, {
          factory: Atom.context({ memoMap: Layer.makeMemoMapUnsafe() })
        })
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const entity = graph.entity(spaceId, Todo)("1")
        const query = graph.query(spaceId, ListTodos)(undefined)
        const mutation = graph.mutation(spaceId, PutTodo)
        const unmountEntity = registry.mount(entity)
        const unmountQuery = registry.mount(query)
        const unmountMutation = registry.mount(mutation)
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            unmountMutation()
            unmountQuery()
            unmountEntity()
          })
        )
        assert.isTrue(Option.isNone(yield* AtomRegistry.getResult(registry, entity)))
        assert.deepStrictEqual(
          (yield* AtomRegistry.getResult(registry, query)).filter((todo) => todo.id === "1"),
          []
        )
        registry.set(mutation, { id: "1", title: "atom" })
        const pending = yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
        yield* Effect.yieldNow
        assert.deepStrictEqual(Option.getOrThrow(yield* AtomRegistry.getResult(registry, entity)), {
          id: "1",
          title: "atom"
        })
        assert.strictEqual(pending.envelope.name, PutTodo.name)
        const receipt = graph.receipt(spaceId, PutTodo, pending.envelope.mutationId)
        const accepted = Option.getOrThrow(Option.getOrThrow(
          yield* AtomRegistry.toStreamResult(registry, receipt).pipe(
            Stream.filter(Option.isSome),
            Stream.runHead
          )
        ))
        assert.strictEqual(accepted._tag, "Accepted")
        assert.deepStrictEqual(
          (yield* AtomRegistry.getResult(registry, query)).filter((todo) => todo.id === "1"),
          [{ id: "1", title: "atom" }]
        )
        const status = yield* AtomRegistry.getResult(registry, graph.status(spaceId))
        assert.strictEqual(status._tag, "Online")
      })
  )

  it.effect("keeps addressed atoms isolated and shares membership through one runtime", () =>
    Effect.gen(function*() {
      const graph = BrowserReplica.make(replica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const firstEntity = graph.entity(spaceId, Todo)("shared")
      const secondEntity = graph.entity(secondSpaceId, Todo)("shared")
      const firstQuery = graph.query(spaceId, ListTodos)(undefined)
      const secondQuery = graph.query(secondSpaceId, ListTodos)(undefined)
      const firstMutation = graph.mutation(spaceId, PutTodo)
      const secondMutation = graph.mutation(secondSpaceId, PutTodo)
      const mounted = [
        registry.mount(firstEntity),
        registry.mount(secondEntity),
        registry.mount(firstQuery),
        registry.mount(secondQuery),
        registry.mount(firstMutation),
        registry.mount(secondMutation),
        registry.mount(graph.spaces),
        registry.mount(graph.aggregateStatus),
        registry.mount(graph.join),
        registry.mount(graph.leave)
      ]
      yield* Effect.addFinalizer(() => Effect.sync(() => mounted.forEach((unmount) => unmount())))

      registry.set(firstMutation, { id: "shared", title: "first" })
      registry.set(secondMutation, { id: "shared", title: "second" })
      const [firstPending, secondPending] = yield* Effect.all([
        AtomRegistry.getResult(registry, firstMutation, { suspendOnWaiting: true }),
        AtomRegistry.getResult(registry, secondMutation, { suspendOnWaiting: true })
      ])
      yield* Effect.yieldNow
      assert.strictEqual(Option.getOrThrow(yield* AtomRegistry.getResult(registry, firstEntity)).title, "first")
      assert.strictEqual(Option.getOrThrow(yield* AtomRegistry.getResult(registry, secondEntity)).title, "second")
      assert.deepStrictEqual(
        (yield* AtomRegistry.getResult(registry, firstQuery)).filter((todo) => todo.id === "shared"),
        [{
          id: "shared",
          title: "first"
        }]
      )
      assert.deepStrictEqual(
        (yield* AtomRegistry.getResult(registry, secondQuery)).filter((todo) => todo.id === "shared"),
        [{
          id: "shared",
          title: "second"
        }]
      )

      const awaitReceipt = (address: Identity.SpaceId, mutationId: Identity.MutationId) =>
        AtomRegistry.toStreamResult(registry, graph.receipt(address, PutTodo, mutationId)).pipe(
          Stream.filter(Option.isSome),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
          Effect.map(Option.getOrThrow)
        )
      const [firstReceipt, secondReceipt] = yield* Effect.all([
        awaitReceipt(spaceId, firstPending.envelope.mutationId),
        awaitReceipt(secondSpaceId, secondPending.envelope.mutationId)
      ], { concurrency: "unbounded" })
      assert.strictEqual(firstReceipt.spaceId, spaceId)
      assert.strictEqual(secondReceipt.spaceId, secondSpaceId)
      assert.strictEqual((yield* AtomRegistry.getResult(registry, graph.status(spaceId))).spaceId, spaceId)
      assert.deepStrictEqual(
        (yield* AtomRegistry.getResult(registry, graph.aggregateStatus)).spaces.map((status) => status.spaceId),
        [spaceId, secondSpaceId]
      )

      registry.set(graph.join, thirdSpaceId)
      const joinedSpaces = Option.getOrThrow(
        yield* AtomRegistry.toStreamResult(registry, graph.spaces).pipe(
          Stream.filter((spaces) => spaces.some((space) => space.spaceId === thirdSpaceId)),
          Stream.runHead
        )
      )
      assert.deepStrictEqual(joinedSpaces.map((space) => space.spaceId), [spaceId, secondSpaceId, thirdSpaceId])

      registry.set(graph.leave, secondSpaceId)
      const remainingSpaces = Option.getOrThrow(
        yield* AtomRegistry.toStreamResult(registry, graph.spaces).pipe(
          Stream.filter((spaces) => !spaces.some((space) => space.spaceId === secondSpaceId)),
          Stream.runHead
        )
      )
      assert.deepStrictEqual(remainingSpaces.map((space) => space.spaceId), [spaceId, thirdSpaceId])
      const error = yield* AtomRegistry.getResult(registry, secondEntity).pipe(Effect.flip)
      assert.strictEqual(error._tag, "SpaceNotJoined")
    }))
})
