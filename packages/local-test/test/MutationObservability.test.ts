import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as FaultInjection from "../src/FaultInjection.js"
import * as TestReplica from "../src/TestReplica.js"
import * as TestServer from "../src/TestServer.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})

const PutTodo = Mutation.make("PutTodo", {
  version: 1,
  payload: Todo.schema,
  success: Todo.schema
})

class RejectTodoError extends Schema.TaggedErrorClass<RejectTodoError>(
  "@lucas-barake/effect-local-test/RejectTodoError"
)("RejectTodoError", { code: Schema.NumberFromString }) {}

const RejectTodo = Mutation.make("RejectTodo", {
  version: 1,
  payload: Todo.schema,
  rejection: RejectTodoError
})

const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo, RejectTodo] })
const clientHandlers = PutTodo.toLayer(({ payload, transaction }) =>
  transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))
).pipe(
  (putTodo) =>
    RejectTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload)).pipe(
      (rejectTodo) => Layer.mergeAll(putTodo, rejectTodo)
    )
)
const serverHandlers = PutTodo.toLayer(({ payload, transaction }) =>
  transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))
).pipe(
  (putTodo) =>
    RejectTodo.toLayer(({ payload, transaction }) =>
      transaction.set(Todo, payload.id, payload).pipe(
        Effect.andThen(Effect.fail(new RejectTodoError({ code: 409 })))
      )
    ).pipe(
      (rejectTodo) => Layer.mergeAll(putTodo, rejectTodo)
    )
)
const runtime = MutationRuntime.layer(definition).pipe(Layer.provide(serverHandlers))

const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const
const clientHistory = {
  scope: Protocol.ReplicationScope.make({ models: [Todo.name] }),
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  settlementCapacity: 16,
  migration
}
const serverHistory = {
  readAuthorizationRefreshInterval: "1 second" as const,
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
}

const database = () =>
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }).pipe(
    (sqlite) => Layer.mergeAll(sqlite, NodeCrypto.layer, Reactivity.layer, QueryReactivity.layer)
  )

const service = <I, S, E extends { readonly _tag: string }, R,>(
  tag: Context.Service<I, S>,
  layer: Layer.Layer<I, E, R>
) => Layer.build(layer).pipe(Effect.map(Context.get(tag)))

const makeServices = Effect.gen(function*() {
  const serverLayer = ServerStore.layerTrusted({ ...serverHistory, definition })
  const server = yield* serverLayer.pipe(
    Layer.provide(runtime),
    Layer.provide(database()),
    (layer) => service(ServerStore.ServerStore, layer)
  )
  const faults = yield* service(FaultInjection.FaultInjection, FaultInjection.layer)
  const sync = yield* TestServer.layer.pipe(
    Layer.provide(Layer.succeed(ServerStore.ServerStore, server)),
    Layer.provide(Layer.succeed(FaultInjection.FaultInjection, faults)),
    Layer.provide(NodeCrypto.layer),
    (layer) => service(SyncEngine.SyncEngine, layer)
  )
  const replicaLayer = TestReplica.layer({
    ...clientHistory,
    definition,
    clientId,
    initialSpaces: [spaceId],
    retryDelay: "1 millis"
  })
  const replica = yield* replicaLayer.pipe(
    Layer.provide(clientHandlers),
    Layer.provide(database()),
    Layer.provide(Layer.succeed(SyncEngine.SyncEngine, sync)),
    (layer) => service(Replica.Replica, layer)
  )
  return { faults, replica }
})

const subscribe = <A, E,>(stream: Stream.Stream<A, E>) =>
  stream.pipe(
    Stream.runHead,
    Effect.forkScoped({ startImmediately: true })
  )

describe("mutation observability", () => {
  it.effect("decodes a server rejection through the originating mutation schema", () =>
    Effect.gen(function*() {
      const { replica } = yield* makeServices
      const space = yield* replica.space(spaceId)
      const settlementFiber = yield* space.settlementsFor(RejectTodo).pipe(subscribe)

      const pending = yield* space.mutate(RejectTodo, { id: "typed", title: "optimistic" })
      const settlement = Option.getOrThrow(yield* Fiber.join(settlementFiber))

      assert.strictEqual(settlement.receipt._tag, "Rejected")
      if (settlement.receipt._tag !== "Rejected") return
      assert.strictEqual(settlement.receipt.origin, "Mutation")
      if (settlement.receipt.origin !== "Mutation") return
      const typed: Mutation.Rejection<typeof RejectTodo> = settlement.receipt.rejection
      assert.strictEqual(typed._tag, "RejectTodoError")
      assert.strictEqual(typed.code, 409)
      const durable = Option.getOrThrow(yield* space.receipt(RejectTodo, pending.envelope.mutationId))
      assert.strictEqual(durable._tag, "Rejected")
      if (durable._tag === "Rejected" && durable.origin === "Mutation") {
        assert.strictEqual(durable.rejection._tag, "RejectTodoError")
        assert.strictEqual(durable.rejection.code, 409)
      }
    }))

  it.effect("emits one settlement when the same receipt is persisted twice across reconnect", () =>
    Effect.gen(function*() {
      const { faults, replica } = yield* makeServices
      const space = yield* replica.space(spaceId)
      const firstCollector = yield* space.settlementsFor(PutTodo).pipe(Stream.toQueue({ capacity: "unbounded" }))
      const secondCollector = yield* space.settlementsFor(PutTodo).pipe(Stream.toQueue({ capacity: "unbounded" }))
      yield* faults.withholdPullEvidence(spaceId)
      yield* faults.dropNextReceipt(spaceId)

      const pending = yield* space.mutate(PutTodo, { id: "duplicate", title: "once" })
      const firstCommitted = yield* faults.awaitReceiptCommitted(spaceId)
      const dropped = yield* faults.awaitReceiptDropped(spaceId)
      assert.strictEqual(firstCommitted.receipt.mutationId, pending.envelope.mutationId)
      assert.strictEqual(dropped.receipt.mutationId, pending.envelope.mutationId)
      yield* faults.holdNextReceipt(spaceId)
      yield* TestClock.adjust("1 millis")
      const secondCommitted = yield* faults.awaitReceiptCommitted(spaceId)
      assert.deepStrictEqual(secondCommitted.receipt, firstCommitted.receipt)
      const barrier = yield* space.mutate(PutTodo, { id: "trigger", title: "next reconciliation" })
      yield* faults.partitionAfterNextReceipt(spaceId)
      yield* faults.releaseHeldReceipt(spaceId)
      const firstReturned = yield* faults.awaitReceiptReturned(spaceId)
      yield* faults.awaitRequestRejectedOffline(spaceId)
      yield* faults.heal(spaceId)
      yield* TestClock.adjust("2 millis")
      const duplicateReturned = yield* faults.awaitReceiptReturned(spaceId)
      assert.strictEqual(firstReturned.receipt.mutationId, pending.envelope.mutationId)
      assert.strictEqual(duplicateReturned.receipt.mutationId, pending.envelope.mutationId)
      assert.deepStrictEqual(duplicateReturned.receipt, firstReturned.receipt)
      yield* faults.releasePullEvidence(spaceId)
      yield* faults.awaitPullCompletedAfterReceipt(spaceId)

      const collectThroughBarrier = (collector: typeof firstCollector) =>
        Effect.gen(function*() {
          const settlements: Array<Replica.MutationSettlement<typeof PutTodo>> = []
          while (true) {
            const settlement = yield* Queue.take(collector)
            settlements.push(settlement)
            if (settlement.pending.envelope.mutationId === barrier.envelope.mutationId) return settlements
          }
        })
      const received = yield* Effect.all([
        collectThroughBarrier(firstCollector),
        collectThroughBarrier(secondCollector)
      ], { concurrency: "unbounded" })
      for (const settlements of received) {
        pipe(
          settlements.filter(
            (settlement) => settlement.pending.envelope.mutationId === pending.envelope.mutationId
          ),
          (matching) => assert.lengthOf(matching, 1)
        )
      }
    }))

  it.effect("publishes rejection only after rollback and later pending replay", () =>
    Effect.gen(function*() {
      const { faults, replica } = yield* makeServices
      const space = yield* replica.space(spaceId)
      yield* faults.holdNextReceipt(spaceId)
      const observed = yield* Deferred.make<{
        readonly rejected: Option.Option<typeof Todo.schema.Type>
        readonly later: Option.Option<typeof Todo.schema.Type>
      }>()
      yield* space.settlementsFor(RejectTodo).pipe(
        Stream.runForEach(() =>
          Effect.all([
            space.get(Todo, "rejected"),
            space.get(Todo, "later")
          ]).pipe(Effect.flatMap(([rejected, later]) => Deferred.succeed(observed, { rejected, later })))
        ),
        Effect.forkScoped({ startImmediately: true })
      )

      yield* space.mutate(RejectTodo, { id: "rejected", title: "rolled back" })
      yield* faults.awaitReceiptCommitted(spaceId)
      yield* space.mutate(PutTodo, { id: "later", title: "replayed" })
      yield* faults.releaseHeldReceipt(spaceId)

      const state = yield* Deferred.await(observed)
      pipe(Option.isNone(state.rejected), (isNone) => assert.isTrue(isNone))
      pipe(Option.getOrThrow(state.later), (later) => assert.deepStrictEqual(later, { id: "later", title: "replayed" }))
    }))
})
