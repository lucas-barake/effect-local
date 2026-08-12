import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as LocalStore from "@lucas-barake/effect-local-sql/LocalStore"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as FaultInjection from "../src/FaultInjection.js"
import * as TestServer from "../src/TestServer.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const secondSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo] })
const handlers = PutTodo.toLayer(({ payload, transaction }) =>
  transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))
)
const runtime = MutationRuntime.layer(definition).pipe(Layer.provide(handlers))
const migration = {
  retryDelay: "1 millis",
  maximumAttempts: 8
} satisfies { readonly retryDelay: Duration.Input; readonly maximumAttempts: number }
const clientHistory = {
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

const makeSyncServices = Effect.gen(function*() {
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
  return { faults, sync }
})

const makeServices = Effect.gen(function*() {
  const { faults, sync } = yield* makeSyncServices
  const local = yield* service(
    LocalStore.Store,
    LocalStore.layer({ ...clientHistory, definition, spaceId, clientId }).pipe(
      Layer.provide(runtime),
      Layer.provide(database())
    )
  )
  return { faults, local, sync }
})

describe("test synchronization faults", () => {
  it.effect("partitions and consumes one-shot faults by space", () =>
    Effect.gen(function*() {
      const { faults } = yield* makeServices
      yield* faults.partition(spaceId)
      yield* faults.dropNextReceipt(spaceId)
      yield* faults.duplicateNextPage(spaceId)

      assert.isFalse((yield* faults.state(spaceId)).online)
      assert.deepStrictEqual(yield* faults.state(secondSpaceId), {
        online: true,
        dropNextReceipt: false,
        duplicateNextPage: false
      })
      assert.isFalse(yield* faults.takeDroppedReceipt(secondSpaceId))
      assert.isFalse(yield* faults.takeDuplicatePage(secondSpaceId))
      assert.isTrue(yield* faults.takeDroppedReceipt(spaceId))
      assert.isTrue(yield* faults.takeDuplicatePage(spaceId))
      yield* faults.heal(spaceId)
      assert.isTrue((yield* faults.state(spaceId)).online)
    }))

  it.effect("lets one space settle while another space is partitioned", () =>
    Effect.gen(function*() {
      const { faults, sync } = yield* makeSyncServices
      yield* faults.partition(spaceId)
      const root = yield* service(
        Replica.Replica,
        SqlReplica.layer({
          ...clientHistory,
          definition,
          clientId,
          initialSpaces: [spaceId, secondSpaceId],
          retryDelay: "1 millis"
        }).pipe(
          Layer.provide(handlers),
          Layer.provide(database()),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, sync))
        )
      )
      const first = yield* root.space(spaceId)
      const second = yield* root.space(secondSpaceId)
      const firstPending = yield* first.mutate(PutTodo, { id: "shared", title: "first" })
      const secondPending = yield* second.mutate(PutTodo, { id: "shared", title: "second" })
      const awaitReceipt = (space: Replica.Space, mutationId: Identity.MutationId) =>
        Effect.gen(function*() {
          while (true) {
            const receipt = yield* space.receipt(PutTodo, mutationId)
            if (Option.isSome(receipt)) return receipt.value
            yield* Effect.yieldNow
          }
        })
      const awaitStatus = (space: Replica.Space, tag: "Offline" | "Online") =>
        Effect.gen(function*() {
          while (true) {
            const status = yield* space.status
            if (status._tag === tag) return status
            yield* Effect.yieldNow
          }
        })

      const secondReceipt = yield* awaitReceipt(second, secondPending.envelope.mutationId)
      assert.strictEqual(secondReceipt._tag, "Accepted")
      assert.isTrue(Option.isNone(yield* first.receipt(PutTodo, firstPending.envelope.mutationId)))
      yield* awaitStatus(first, "Offline")
      yield* awaitStatus(second, "Online")

      yield* faults.heal(spaceId)
      yield* TestClock.adjust("1 millis")
      const firstReceipt = yield* awaitReceipt(first, firstPending.envelope.mutationId)
      assert.strictEqual(firstReceipt._tag, "Accepted")
      yield* awaitStatus(first, "Online")
    }))

  it.effect("keeps optimistic state while partitioned and reconciles after healing", () =>
    Effect.gen(function*() {
      const { faults, local, sync } = yield* makeServices
      const pending = yield* local.mutate(PutTodo, { id: "1", title: "offline" })
      const request = Protocol.SubmitRequest.make({ envelope: pending.envelope, schema: definition.schemaIdentity })
      yield* faults.partition(spaceId)
      const error = yield* sync.submit(request).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ServerUnavailable")
      assert.deepStrictEqual(Option.getOrThrow(yield* local.get(Todo, "1")), { id: "1", title: "offline" })
      assert.strictEqual(yield* local.pendingCount, 1)

      yield* faults.heal(spaceId)
      const receipt = yield* sync.submit(request)
      yield* local.applyReceipt(receipt)
      const page = yield* sync.pull({
        spaceId,
        schema: definition.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
      if ("_tag" in page) assert.fail("unexpected bootstrap")
      yield* local.applyEntries(page.entries)
      assert.strictEqual(yield* local.pendingCount, 0)
      assert.strictEqual(yield* local.cursor, 1)
    }))

  it.effect("resolves a dropped receipt through an exact retry", () =>
    Effect.gen(function*() {
      const { faults, local, sync } = yield* makeServices
      const pending = yield* local.mutate(PutTodo, { id: "1", title: "ambiguous" })
      const request = Protocol.SubmitRequest.make({ envelope: pending.envelope, schema: definition.schemaIdentity })
      yield* faults.dropNextReceipt(spaceId)
      const error = yield* sync.submit(request).pipe(Effect.flip)
      assert.strictEqual(error._tag, "ServerUnavailable")
      const receipt = yield* sync.submit(request)
      assert.strictEqual(receipt._tag, "Accepted")
      if (receipt._tag === "Accepted") assert.strictEqual(receipt.serverSequence, 1)
      const page = yield* sync.pull({
        spaceId,
        schema: definition.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
      if ("_tag" in page) assert.fail("unexpected bootstrap")
      assert.strictEqual(page.entries.length, 1)
    }))

  it.effect("duplicates a catch up entry without corrupting local order", () =>
    Effect.gen(function*() {
      const { faults, local, sync } = yield* makeServices
      const pending = yield* local.mutate(PutTodo, { id: "1", title: "duplicate" })
      const receipt = yield* sync.submit({ envelope: pending.envelope, schema: definition.schemaIdentity })
      yield* local.applyReceipt(receipt)
      yield* faults.duplicateNextPage(spaceId)
      const page = yield* sync.pull({
        spaceId,
        schema: definition.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
      if ("_tag" in page) assert.fail("unexpected bootstrap")
      assert.deepStrictEqual(page.entries.map((entry) => entry.sequence), [1, 1])
      yield* local.applyEntries(page.entries)
      assert.strictEqual(yield* local.cursor, 1)
      assert.strictEqual(yield* local.pendingCount, 0)
    }))
})
