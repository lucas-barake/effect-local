import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as ConnectionLane from "@lucas-barake/effect-local-sql/ConnectionLane"
import * as LocalStore from "@lucas-barake/effect-local-sql/LocalStore"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as Reconciler from "@lucas-barake/effect-local-sql/Reconciler"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as FaultInjection from "../src/FaultInjection.js"
import * as TestServer from "../src/TestServer.js"
import * as VirtualTime from "./fixtures/VirtualTime.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const secondSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const writerClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo] })
const layerHandlers = PutTodo.toLayer(({ payload, transaction }) =>
  transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))
)
const layerRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))
const migration = {
  retryDelay: "1 millis",
  maximumAttempts: 8
} satisfies { readonly retryDelay: Duration.Input; readonly maximumAttempts: number }
const clientHistory = {
  defaultScope: Protocol.ReplicationScope.make({ models: [Todo.name] }),
  scope: Protocol.ReplicationScope.make({ models: [Todo.name] }),
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
  retainedReceipts: 256,
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
const database = () =>
  Layer.mergeAll(
    ConnectionLane.makeLayer().pipe(Layer.provideMerge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )

const service = <I, S, E extends { readonly _tag: string }, R,>(
  tag: Context.Service<I, S>,
  layer: Layer.Layer<I, E, R>
) => Layer.build(layer).pipe(Effect.map(Context.get(tag)))

const makeSyncServicesWith = Effect.fnUntraced(function*(history: typeof serverHistory) {
  const server = yield* service(
    ServerStore.ServerStore,
    ServerStore.layerTrusted({ ...history, definition }).pipe(
      Layer.provide(layerRuntime),
      Layer.provide(database())
    )
  )
  const faults = yield* service(FaultInjection.FaultInjection, FaultInjection.layer)
  const sync = yield* service(
    SyncEngine.SyncEngine,
    TestServer.layer.pipe(
      Layer.provide(Layer.succeed(ServerStore.ServerStore, server)),
      Layer.provide(Layer.succeed(FaultInjection.FaultInjection, faults)),
      Layer.provide(NodeCrypto.layer)
    )
  )
  return { faults, sync }
})

const makeSyncServices = makeSyncServicesWith(serverHistory)

const makeLocal = (localClientId: Identity.ClientId) =>
  service(
    LocalStore.Store,
    LocalStore.layer({ ...clientHistory, definition, spaceId, clientId: localClientId }).pipe(
      Layer.provide(layerRuntime),
      Layer.provide(database())
    )
  )

const makeServices = Effect.gen(function*() {
  const { faults, sync } = yield* makeSyncServices
  const local = yield* makeLocal(clientId)
  return { faults, local, sync }
})

const pullRequest = (state: LocalStore.ReplicationState, membershipIncarnation: Identity.MembershipIncarnation) =>
  Protocol.PullRequest.make({
    spaceId,
    clientId: state.clientId,
    schema: definition.schemaIdentity,
    scope: state.scope,
    membershipIncarnation,
    scopeGeneration: state.scopeGeneration,
    cursor: state.cursor,
    limit: 10
  })

const failureOf = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.result,
    Effect.map((result) => {
      if (Result.isFailure(result)) return result.failure
      return assert.fail("expected Effect failure")
    })
  )

const acceptedSequence = (receipt: Protocol.Receipt): number | string => {
  if (receipt._tag === "Accepted") return receipt.serverSequence
  return receipt._tag
}

const synchronize = (local: LocalStore.Service, sync: SyncEngine.Service) =>
  service(
    Reconciler.Reconciliation,
    Reconciler.layerOnePass({ definition, spaceId, pageSize: 10 }).pipe(
      Layer.provide(Layer.succeed(LocalStore.Store, local)),
      Layer.provide(Layer.succeed(SyncEngine.SyncEngine, sync))
    )
  ).pipe(Effect.flatMap((reconciliation) => reconciliation.sync))

describe("test synchronization faults", () => {
  it.effect(
    "routes synchronization events without stealing another space's event",
    Effect.fnUntraced(function*() {
      const { faults } = yield* makeServices
      yield* faults.emit({ _tag: "RequestRejectedOffline", spaceId: secondSpaceId })
      yield* faults.emit({ _tag: "RequestRejectedOffline", spaceId })

      assert.strictEqual((yield* faults.awaitRequestRejectedOffline(spaceId)).spaceId, spaceId)
      const second = yield* faults.awaitRequestRejectedOffline(secondSpaceId).pipe(
        Effect.timeoutOption("1 second"),
        Effect.forkChild({ startImmediately: true })
      )
      yield* TestClock.adjust("1 second")
      assert.strictEqual(Option.getOrThrow(yield* Fiber.join(second)).spaceId, secondSpaceId)
    })
  )

  it.effect(
    "partitions and consumes one-shot faults by space",
    Effect.fnUntraced(function*() {
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
    })
  )

  it.effect(
    "lets one space settle while another space is partitioned",
    Effect.fnUntraced(function*() {
      const { faults, sync } = yield* makeSyncServices
      yield* faults.partition(spaceId)
      const context = yield* Layer.build(
        SqlReplica.layer({
          ...clientHistory,
          definition,
          clientId,
          initialSpaces: [spaceId, secondSpaceId],
          retryDelay: "1 millis"
        }).pipe(
          Layer.provide(layerHandlers),
          Layer.provideMerge(database()),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, sync))
        )
      )
      const root = Context.get(context, Replica.Replica)
      const reactivity = Context.get(context, Reactivity.Reactivity)
      const first = yield* root.space(spaceId)
      const second = yield* root.space(secondSpaceId)
      const firstPending = yield* first.mutate(PutTodo, { id: "shared", title: "first" })
      const secondPending = yield* second.mutate(PutTodo, { id: "shared", title: "second" })
      const awaitReceipt = (space: Replica.Space, mutationId: Identity.MutationId) =>
        reactivity.stream([ReactivityKey.receipt(space.spaceId, mutationId)], space.receipt(PutTodo, mutationId)).pipe(
          Stream.filter(Option.isSome),
          Stream.map((receipt) => receipt.value),
          Stream.runHead,
          Effect.map(Option.getOrThrow)
        )
      const awaitStatus = (space: Replica.Space, tag: "Offline" | "Online") =>
        reactivity.stream([ReactivityKey.status(space.spaceId)], space.status).pipe(
          Stream.filter((status) => status._tag === tag),
          Stream.runHead,
          Effect.map(Option.getOrThrow)
        )

      const secondReceipt = yield* awaitReceipt(second, secondPending.envelope.mutationId)
      assert.strictEqual(secondReceipt._tag, "Accepted")
      const firstReceiptBeforeHealing = yield* first.receipt(PutTodo, firstPending.envelope.mutationId)
      assert.isTrue(Option.isNone(firstReceiptBeforeHealing))
      yield* awaitStatus(first, "Offline")
      yield* awaitStatus(second, "Online")

      yield* faults.heal(spaceId)
      yield* TestClock.adjust("1 millis")
      const firstReceipt = yield* awaitReceipt(first, firstPending.envelope.mutationId)
      assert.strictEqual(firstReceipt._tag, "Accepted")
      yield* awaitStatus(first, "Online")
    })
  )

  it.effect(
    "keeps optimistic state while partitioned and reconciles after healing",
    Effect.fnUntraced(function*() {
      const { faults, local, sync } = yield* makeServices
      yield* synchronize(local, sync)
      const pending = yield* local.mutate(PutTodo, { id: "1", title: "offline" })
      const request = Protocol.SubmitBatchRequest.make({
        envelopes: [pending.envelope],
        schema: definition.schemaIdentity
      })
      yield* faults.partition(spaceId)
      const error = yield* failureOf(sync.submitBatch(request))
      assert.strictEqual(error._tag, "ServerUnavailable")
      pipe(
        yield* local.get(Todo, "1"),
        Option.getOrThrow,
        (todo) => assert.deepStrictEqual(todo, { id: "1", title: "offline" })
      )
      assert.strictEqual(yield* local.pendingCount, 1)

      yield* faults.heal(spaceId)
      const { receipts } = yield* sync.submitBatch(request)
      yield* local.applyReceipts(receipts)
      const page = yield* sync.pull(pullRequest(yield* local.replicationState, local.membershipIncarnation))
      if ("_tag" in page) assert.fail("unexpected bootstrap")
      yield* local.applyViewPage(page)
      yield* local.settleReceipts
      assert.strictEqual(yield* local.pendingCount, 0)
      assert.strictEqual((yield* local.progress).cursor, 1)
    })
  )

  it.effect(
    "resolves a dropped receipt through an exact retry",
    Effect.fnUntraced(function*() {
      const { faults, local, sync } = yield* makeServices
      yield* synchronize(local, sync)
      const pending = yield* local.mutate(PutTodo, { id: "1", title: "ambiguous" })
      const request = Protocol.SubmitBatchRequest.make({
        envelopes: [pending.envelope],
        schema: definition.schemaIdentity
      })
      yield* faults.dropNextReceipt(spaceId)
      const error = yield* failureOf(sync.submitBatch(request))
      assert.strictEqual(error._tag, "ServerUnavailable")
      const { receipts } = yield* sync.submitBatch(request)
      assert.deepStrictEqual(receipts.map(acceptedSequence), [1])
      const page = yield* sync.pull(pullRequest(yield* local.replicationState, local.membershipIncarnation))
      if ("_tag" in page) assert.fail("unexpected bootstrap")
      assert.strictEqual(page.changes.length, 1)
    })
  )

  it.effect(
    "duplicates a catch up entry without corrupting local order",
    Effect.fnUntraced(function*() {
      const { faults, local, sync } = yield* makeServices
      yield* synchronize(local, sync)
      const pending = yield* local.mutate(PutTodo, { id: "1", title: "duplicate" })
      const { receipts } = yield* sync.submitBatch({
        envelopes: [pending.envelope],
        schema: definition.schemaIdentity
      })
      yield* local.applyReceipts(receipts)
      yield* faults.duplicateNextPage(spaceId)
      const page = yield* sync.pull(pullRequest(yield* local.replicationState, local.membershipIncarnation))
      if ("_tag" in page) assert.fail("unexpected bootstrap")
      assert.deepStrictEqual(page.changes.map((change) => change._tag), ["Upsert", "Upsert"])
      yield* local.applyViewPage(page)
      yield* local.settleReceipts
      assert.strictEqual((yield* local.progress).cursor, 1)
      assert.strictEqual(yield* local.pendingCount, 0)
    })
  )

  it.effect(
    "resubmits a batch whose response was dropped after commit without applying anything twice",
    Effect.fnUntraced(function*() {
      const { faults, local, sync } = yield* makeServices
      yield* synchronize(local, sync)
      const pending = yield* Effect.forEach(
        ["1", "2", "3"],
        (id) => local.mutate(PutTodo, { id, title: `batch ${id}` })
      )
      yield* faults.dropNextReceipt(spaceId)

      const error = yield* failureOf(synchronize(local, sync))
      assert.strictEqual(error._tag, "ServerUnavailable")
      const dropped = yield* Effect.forEach(pending, () => faults.awaitReceiptDropped(spaceId))
      assert.deepStrictEqual(
        dropped.map((event) => event.receipt.mutationId),
        pending.map((mutation) => mutation.envelope.mutationId)
      )
      assert.strictEqual(yield* local.pendingCount, 3)

      yield* synchronize(local, sync)

      const receipts = yield* Effect.forEach(pending, (mutation) => local.receipt(mutation.envelope.mutationId))
      const sequences = receipts.map(Option.map(acceptedSequence))
      assert.deepStrictEqual(sequences, [Option.some(1), Option.some(2), Option.some(3)])
      assert.strictEqual((yield* local.progress).cursor, 3)
      assert.strictEqual(yield* local.pendingCount, 0)
    })
  )

  it.effect(
    "delivers withheld pull evidence intact once released",
    Effect.fnUntraced(function*() {
      const { faults, local: reader, sync } = yield* makeServices
      const writer = yield* makeLocal(writerClientId)
      yield* synchronize(reader, sync)
      yield* synchronize(writer, sync)
      yield* writer.mutate(PutTodo, { id: "1", title: "withheld" })
      yield* synchronize(writer, sync)

      yield* faults.withholdPullEvidence(spaceId)
      const withheld = yield* synchronize(reader, sync).pipe(Effect.forkChild({ startImmediately: true }))
      yield* faults.awaitPullEvidenceWithheld(spaceId)
      yield* faults.releasePullEvidence(spaceId)
      yield* Fiber.join(withheld)
      yield* synchronize(reader, sync)

      assert.deepStrictEqual(yield* reader.get(Todo, "1"), Option.some({ id: "1", title: "withheld" }))
      assert.strictEqual((yield* reader.progress).cursor, 1)
    })
  )

  it.effect(
    "fails a watch with the server's terminal failure instead of reporting the transport unavailable",
    Effect.fnUntraced(function*() {
      const { local, sync } = yield* makeServices
      const state = yield* local.replicationState
      const error = yield* failureOf(
        sync.watch({
          spaceId,
          clientId: state.clientId,
          schema: definition.schemaIdentity,
          scope: Protocol.ReplicationScope.make({ models: ["Unknown"] }),
          scopeGeneration: state.scopeGeneration,
          cursor: state.cursor
        }).pipe(Stream.runDrain)
      )
      assert.strictEqual(error._tag, "ProtocolInvalid")
    })
  )

  it.effect(
    "marks a space failed when the server refuses its watch",
    Effect.fnUntraced(function*() {
      const { sync } = yield* makeSyncServicesWith({ ...serverHistory, maximumWatchersPerSpace: 1 })
      const local = yield* makeLocal(clientId)
      const occupied = yield* Deferred.make<void>()
      const state = yield* local.replicationState
      yield* sync.watch({
        spaceId,
        clientId: writerClientId,
        schema: definition.schemaIdentity,
        scope: state.scope,
        scopeGeneration: state.scopeGeneration,
        cursor: state.cursor
      }).pipe(
        Stream.runForEach(() => Deferred.succeed(occupied, undefined)),
        Effect.forkScoped
      )
      yield* Deferred.await(occupied)
      const statuses = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
      yield* Layer.build(
        Reconciler.layer({
          definition,
          spaceId,
          pageSize: 10,
          retryDelay: "1 millis",
          onStatusChange: (status) => Queue.offer(statuses, status).pipe(Effect.asVoid)
        }).pipe(
          Layer.provide(Layer.succeed(LocalStore.Store, local)),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, sync))
        )
      )
      const settled = yield* Stream.fromQueue(statuses).pipe(
        Stream.filter((status) => status._tag !== "Online" && status._tag !== "Connecting"),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
        VirtualTime.advanceClockUntil
      )
      assert.deepStrictEqual(settled, { _tag: "Failed", pending: 0, message: "CapacityExceeded" })
    })
  )

  it.effect(
    "refuses to open a watch across a partition",
    Effect.fnUntraced(function*() {
      const { faults, local, sync } = yield* makeServices
      const state = yield* local.replicationState
      yield* faults.partition(spaceId)
      const opened = yield* sync.watch({
        spaceId,
        clientId: state.clientId,
        schema: definition.schemaIdentity,
        scope: state.scope,
        scopeGeneration: state.scopeGeneration,
        cursor: state.cursor
      }).pipe(Stream.runDrain, Effect.timeout("1 second"), failureOf, Effect.forkChild({ startImmediately: true }))
      yield* TestClock.adjust("1 second")
      assert.strictEqual((yield* Fiber.join(opened))._tag, "ServerUnavailable")
      yield* faults.awaitRequestRejectedOffline(spaceId)
    })
  )
})
