import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import { AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity"
import * as ExpoSqlite from "../src/ExpoSqlite.js"
import * as ReactNativeCrypto from "../src/ReactNativeCrypto.js"
import * as ReactNativeReplica from "../src/ReactNativeReplica.js"
import * as ReplicaAtom from "../src/ReplicaAtom.js"
import { AddLabel, AddLabelLive, definition, LabelsSql, limits, ListLabels, ListLabelsLive, Task } from "./fixtures.js"

const Database = ExpoSqlite.layer({ databaseName: ":memory:" })
const Dependencies = Layer.mergeAll(Database, ReactNativeCrypto.layer, ReplicaLimits.layer(limits))
const DomainLive = Layer.mergeAll(AddLabelLive, ListLabelsLive.pipe(Layer.provide(Database)))

// provideMerge keeps SqlClient, Crypto, and ReplicaLimits in the output context so atoms
// and runtime functions can generate command ids and reach the client directly.
const ReplicaLive = ReactNativeReplica.layer(definition, { projections: [LabelsSql] }).pipe(
  Layer.provide(DomainLive),
  Layer.provideMerge(Dependencies)
)

// A fresh memo map per test builds an independent graph and :memory: database, so no
// state can leak between tests through the memoized layer.
const makeRuntime = () => Atom.context({ memoMap: Layer.makeMemoMapUnsafe() })(ReplicaLive)

const awaitSuccessWhere = <A,>(
  queue: Queue.Queue<AsyncResult.AsyncResult<A, unknown>>,
  predicate: (value: A) => boolean
) =>
  Effect.gen(function*() {
    for (let attempts = 0; attempts < 16; attempts++) {
      const result = yield* Queue.take(queue)
      if (AsyncResult.isSuccess(result) && predicate(result.value)) return result.value
    }
    return yield* Effect.die(new Error("atom did not reach the expected success value within 16 emissions"))
  })

describe("ReactNativeReplica", () => {
  it.effect("creates, mutates, and queries documents through the in-process replica", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const documentId = yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "groceries", labels: [] }
      })
      const created = yield* replica.get(Task, documentId)
      assert.deepStrictEqual(created.value, { title: "groceries", labels: [] })

      yield* replica.mutate(AddLabel, {
        commandId: yield* Identity.makeCommandId,
        documentId,
        payload: "errands"
      })
      const mutated = yield* replica.get(Task, documentId)
      assert.deepStrictEqual(mutated.value.labels, ["errands"])

      const rows = yield* replica.query(ListLabels, { prefix: "" })
      assert.deepStrictEqual(rows, [{ sourceDocumentId: documentId, label: "errands" }])
    }).pipe(Effect.provide(ReplicaLive)))

  it.effect("refreshes queryFamily atoms through commit reactivity", () =>
    Effect.gen(function*() {
      const registry = AtomRegistry.make()
      const runtime = makeRuntime()
      const labels = ReplicaAtom.queryFamily(runtime, ListLabels)({ prefix: "" })
      const emissions = yield* Queue.unbounded<AsyncResult.AsyncResult<unknown, unknown>>()
      const unsubscribe = registry.subscribe(labels, (result) => Queue.offerUnsafe(emissions, result), {
        immediate: true
      })

      const initial = yield* awaitSuccessWhere(emissions, (value) => Array.isArray(value))
      assert.deepStrictEqual(initial, [])

      const addLabel = ReplicaAtom.mutation(runtime, AddLabel)
      const createTask = runtime.fn<string>()(
        Effect.fnUntraced(function*(title) {
          const replica = yield* Replica.Replica
          return yield* replica.create(Task, {
            commandId: yield* Identity.makeCommandId,
            value: { title, labels: [] }
          })
        })
      )
      registry.set(createTask, "groceries")
      const documentId = yield* AtomRegistry.getResult(registry, createTask, { suspendOnWaiting: true })

      registry.set(addLabel, {
        commandId: yield* Identity.makeCommandId.pipe(Effect.provide(ReactNativeCrypto.layer)),
        documentId,
        payload: "errands"
      })
      yield* AtomRegistry.getResult(registry, addLabel, { suspendOnWaiting: true })

      const refreshed = yield* awaitSuccessWhere(emissions, (value) => Array.isArray(value) && value.length === 1)
      assert.deepStrictEqual(refreshed, [{ sourceDocumentId: documentId, label: "errands" }])
      unsubscribe()
      registry.dispose()
    }).pipe(Effect.provide(ReactNativeCrypto.layer)))

  it.effect("refreshes documentFamily atoms after a mutation", () =>
    Effect.gen(function*() {
      const registry = AtomRegistry.make()
      const runtime = makeRuntime()
      const createTask = runtime.fn<string>()(
        Effect.fnUntraced(function*(title) {
          const replica = yield* Replica.Replica
          return yield* replica.create(Task, {
            commandId: yield* Identity.makeCommandId,
            value: { title, labels: [] }
          })
        })
      )
      registry.set(createTask, "groceries")
      const documentId = yield* AtomRegistry.getResult(registry, createTask, { suspendOnWaiting: true })

      const document = ReplicaAtom.documentFamily(runtime, Task)(documentId)
      const emissions = yield* Queue.unbounded<AsyncResult.AsyncResult<unknown, unknown>>()
      const unsubscribe = registry.subscribe(document, (result) => Queue.offerUnsafe(emissions, result), {
        immediate: true
      })
      const initial = yield* awaitSuccessWhere(emissions, (value) =>
        typeof value === "object" && value !== null && "value" in value)
      assert.deepStrictEqual((initial as { value: unknown }).value, { title: "groceries", labels: [] })

      const addLabel = ReplicaAtom.mutation(runtime, AddLabel)
      registry.set(addLabel, {
        commandId: yield* Identity.makeCommandId.pipe(Effect.provide(ReactNativeCrypto.layer)),
        documentId,
        payload: "errands"
      })
      yield* AtomRegistry.getResult(registry, addLabel, { suspendOnWaiting: true })

      const refreshed = yield* awaitSuccessWhere(emissions, (value) =>
        typeof value === "object" && value !== null && "value" in value &&
        (value as { value: { labels: ReadonlyArray<string> } }).value.labels.length === 1)
      assert.deepStrictEqual((refreshed as { value: unknown }).value, { title: "groceries", labels: ["errands"] })
      unsubscribe()
      registry.dispose()
    }).pipe(Effect.provide(ReactNativeCrypto.layer)))

  it.effect("emits replica status through the status atom", () =>
    Effect.gen(function*() {
      const registry = AtomRegistry.make()
      const runtime = makeRuntime()
      const status = ReplicaAtom.status(runtime)
      const emissions = yield* Queue.unbounded<AsyncResult.AsyncResult<unknown, unknown>>()
      const unsubscribe = registry.subscribe(status, (result) => Queue.offerUnsafe(emissions, result), {
        immediate: true
      })
      const first = yield* awaitSuccessWhere(emissions, () => true)
      assert.isDefined(first)
      unsubscribe()
      registry.dispose()
    }))

  it.effect("isolates two replicas built under separate memo maps", () =>
    Effect.gen(function*() {
      const registry = AtomRegistry.make()
      // Two replicas in one app each need their own memo map; under a shared memo map
      // the second graph would alias the first graph's memoized internal services.
      const runtimeA = makeRuntime()
      const runtimeB = makeRuntime()

      const createTaskA = runtimeA.fn<string>()(
        Effect.fnUntraced(function*(title) {
          const replica = yield* Replica.Replica
          return yield* replica.create(Task, {
            commandId: yield* Identity.makeCommandId,
            value: { title, labels: ["only-in-a"] }
          })
        })
      )
      registry.set(createTaskA, "groceries")
      yield* AtomRegistry.getResult(registry, createTaskA, { suspendOnWaiting: true })

      const queryB = runtimeB.atom(
        Replica.Replica.use((replica) => replica.query(ListLabels, { prefix: "only" }))
      )
      const value = yield* AtomRegistry.getResult(registry, queryB, { suspendOnWaiting: true })
      assert.deepStrictEqual(value, [])
      registry.dispose()
    }))
})
