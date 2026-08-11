import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as Reconciler from "@lucas-barake/effect-local-sql/Reconciler"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { Atom, AtomRegistry } from "effect/unstable/reactivity"
import * as BrowserReplica from "../src/BrowserReplica.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const ListTodos = Query.make("ListTodos", {
  success: Schema.Array(Todo.schema),
  dependsOn: [Todo]
})
const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo], queries: [ListTodos] })
const handlers = Layer.merge(
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))),
  ListTodos.toLayer(({ query }) => query.all(Todo))
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
  scope: Protocol.ReplicationScope.make({ models: [Todo.name] }),
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
}).pipe(
  Layer.provide(mutationRuntime),
  Layer.provide(database)
)
const sync = Layer.effect(
  SyncEngine.SyncEngine,
  ServerStore.ServerStore.pipe(
    Effect.map((store) =>
      SyncEngine.SyncEngine.of({
        submit: store.submit,
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
  spaceId,
  clientId,
  retryDelay: "10 millis"
}).pipe(
  Layer.provide(sync),
  Layer.provide(database),
  Layer.provide(handlers)
)

describe("Replica Atom graph", () => {
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
    assert.strictEqual(graph.entity(Todo)("1").idleTTL, 17)
    assert.strictEqual(graph.query(ListTodos)(undefined).idleTTL, 17)
    assert.strictEqual(graph.receipt(Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")).idleTTL, 17)
    assert.strictEqual(reads, readsAfterConstruction)
  })

  it.live(
    "runs mutation, entity, query, receipt, and status state through one reactive runtime",
    () =>
      Effect.gen(function*() {
        const graph = BrowserReplica.make(replica)
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const entity = graph.entity(Todo)("1")
        const query = graph.query(ListTodos)(undefined)
        const mutation = graph.mutation(PutTodo)
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
        assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, query), [])
        registry.set(mutation, { id: "1", title: "atom" })
        const pending = yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
        assert.deepStrictEqual(Option.getOrThrow(yield* AtomRegistry.getResult(registry, entity)), {
          id: "1",
          title: "atom"
        })
        assert.strictEqual(pending.envelope.name, PutTodo.name)
        const reconciler = yield* AtomRegistry.getResult(registry, graph.runtime.atom(Reconciler.Reconciler))
        yield* reconciler.sync
        assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, query), [{
          id: "1",
          title: "atom"
        }])

        const receipt = graph.receipt(pending.envelope.mutationId)
        const accepted = Option.getOrThrow(yield* AtomRegistry.getResult(registry, receipt))
        assert.strictEqual(accepted._tag, "Accepted")
        const status = yield* AtomRegistry.getResult(registry, graph.status)
        assert.strictEqual(status._tag, "Online")
      })
  )
})
