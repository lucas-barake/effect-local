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
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
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

const RejectTodo = Mutation.make("RejectTodo", {
  version: 1,
  payload: Todo.schema,
  rejection: Schema.NumberFromString
})

const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo, RejectTodo] })
const clientHandlers = Layer.mergeAll(
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))),
  RejectTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload))
)
const serverHandlers = Layer.mergeAll(
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))),
  RejectTodo.toLayer(({ payload, transaction }) =>
    transaction.set(Todo, payload.id, payload).pipe(Effect.andThen(Effect.fail(409)))
  )
)
const runtime = MutationRuntime.layer(definition).pipe(Layer.provide(serverHandlers))

const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const
const clientHistory = {
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
  Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )

const service = <I, S, E, R,>(tag: Context.Service<I, S>, layer: Layer.Layer<I, E, R>) =>
  Layer.build(layer).pipe(Effect.map((context) => Context.get(context, tag)))

const makeServices = Effect.gen(function*() {
  const server = yield* service(
    ServerStore.ServerStore,
    ServerStore.layerTrusted({ ...serverHistory, definition }).pipe(
      Layer.provide(runtime),
      Layer.provide(database())
    )
  )
  const faults = yield* service(FaultInjection.FaultInjection, FaultInjection.layer)
  const sync = yield* service(
    SyncEngine.SyncEngine,
    TestServer.layer.pipe(
      Layer.provide(Layer.succeed(ServerStore.ServerStore, server)),
      Layer.provide(Layer.succeed(FaultInjection.FaultInjection, faults))
    )
  )
  const replica = yield* service(
    Replica.Replica,
    TestReplica.layer({
      ...clientHistory,
      definition,
      clientId,
      initialSpaces: [spaceId],
      retryDelay: "1 millis"
    }).pipe(
      Layer.provide(clientHandlers),
      Layer.provide(database()),
      Layer.provide(Layer.succeed(SyncEngine.SyncEngine, sync))
    )
  )
  return { faults, replica }
})

const subscribe = <A,>(stream: Stream.Stream<A>) =>
  stream.pipe(
    Stream.runHead,
    Effect.forkChild({ startImmediately: true })
  )

describe("mutation observability", () => {
  it.effect("decodes a server rejection through the originating mutation schema", () =>
    Effect.gen(function*() {
      const { replica } = yield* makeServices
      const space = yield* replica.space(spaceId)
      const settlementFiber = yield* subscribe(space.settlementsFor(RejectTodo))

      const pending = yield* space.mutate(RejectTodo, { id: "typed", title: "optimistic" })
      const settlement = Option.getOrThrow(yield* Fiber.join(settlementFiber))

      assert.strictEqual(settlement.receipt._tag, "Rejected")
      if (settlement.receipt._tag !== "Rejected") return
      assert.strictEqual(settlement.receipt.origin, "Mutation")
      if (settlement.receipt.origin !== "Mutation") return
      const typed: Mutation.Rejection<typeof RejectTodo> = settlement.receipt.rejection
      assert.strictEqual(typed, 409)
      const durable = Option.getOrThrow(yield* space.receipt(RejectTodo, pending.envelope.mutationId))
      assert.strictEqual(durable._tag, "Rejected")
      if (durable._tag === "Rejected" && durable.origin === "Mutation") assert.strictEqual(durable.rejection, 409)
    }))

  it.effect("emits one settlement when the same receipt is persisted twice across reconnect", () =>
    Effect.gen(function*() {
      const { faults, replica } = yield* makeServices
      const space = yield* replica.space(spaceId)
      const received = yield* Ref.make<Array<Replica.MutationSettlement<typeof PutTodo>>>([])
      let targetMutationId: Identity.MutationId | undefined
      const firstSettlement = yield* Deferred.make<void>()
      const collector = yield* space.settlementsFor(PutTodo).pipe(
        Stream.runForEach((settlement) =>
          Effect.gen(function*() {
            yield* Ref.update(received, (values) => [...values, settlement])
            if (settlement.pending.envelope.mutationId === targetMutationId) {
              yield* Deferred.succeed(firstSettlement, undefined)
            }
          })
        ),
        Effect.forkChild({ startImmediately: true })
      )
      yield* faults.withholdPullEvidence(spaceId)
      yield* faults.dropNextReceipt(spaceId)

      const pending = yield* space.mutate(PutTodo, { id: "duplicate", title: "once" })
      targetMutationId = pending.envelope.mutationId
      yield* faults.awaitReceiptDropped(spaceId)
      yield* faults.holdNextReceipt(spaceId)
      yield* TestClock.adjust("1 millis")
      yield* faults.awaitReceiptCommitted(spaceId)
      yield* space.mutate(PutTodo, { id: "trigger", title: "next reconciliation" })
      yield* faults.partitionAfterNextReceipt(spaceId)
      yield* faults.releaseHeldReceipt(spaceId)
      const firstReturned = yield* faults.awaitReceiptReturned(spaceId)
      yield* faults.awaitRequestRejectedOffline(spaceId)
      yield* faults.heal(spaceId)
      yield* TestClock.adjust("1 millis")
      const duplicateReturned = yield* faults.awaitReceiptReturned(spaceId)
      assert.strictEqual(firstReturned.receipt.mutationId, pending.envelope.mutationId)
      assert.strictEqual(duplicateReturned.receipt.mutationId, pending.envelope.mutationId)
      assert.deepStrictEqual(duplicateReturned.receipt, firstReturned.receipt)
      yield* faults.releasePullEvidence(spaceId)
      yield* faults.awaitPullCompletedAfterReceipt(spaceId)
      yield* Deferred.await(firstSettlement)

      assert.lengthOf(
        (yield* Ref.get(received)).filter(
          (settlement) => settlement.pending.envelope.mutationId === pending.envelope.mutationId
        ),
        1
      )
      yield* Fiber.interrupt(collector)
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
      const collector = yield* space.settlementsFor(RejectTodo).pipe(
        Stream.runForEach(() =>
          Effect.all([
            space.get(Todo, "rejected"),
            space.get(Todo, "later")
          ]).pipe(Effect.flatMap(([rejected, later]) => Deferred.succeed(observed, { rejected, later })))
        ),
        Effect.forkChild({ startImmediately: true })
      )

      yield* space.mutate(RejectTodo, { id: "rejected", title: "rolled back" })
      yield* faults.awaitReceiptCommitted(spaceId)
      yield* space.mutate(PutTodo, { id: "later", title: "replayed" })
      yield* faults.releaseHeldReceipt(spaceId)

      const state = yield* Deferred.await(observed)
      assert.isTrue(Option.isNone(state.rejected))
      assert.deepStrictEqual(Option.getOrThrow(state.later), { id: "later", title: "replayed" })
      yield* Fiber.interrupt(collector)
    }))
})
