import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as EphemeralHub from "@lucas-barake/effect-local-rpc/EphemeralHub"
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
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import { pipe } from "effect/Function"
import * as HashMap from "effect/HashMap"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Atom, AtomRegistry } from "effect/unstable/reactivity"
import * as BrowserReplica from "../src/BrowserReplica.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const secondSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const thirdSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const sameMemberForTest = (left: Protocol.EphemeralMember, right: Protocol.EphemeralMember) =>
  left.clientId === right.clientId && left.membershipIncarnation === right.membershipIncarnation
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
const layerHandlers = Layer.mergeAll(
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
const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)
const layerMutationRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))
const migration = {
  retryDelay: "1 millis",
  maximumAttempts: 8
} satisfies { readonly retryDelay: Duration.Input; readonly maximumAttempts: number }
const clientHistory = {
  defaultScope: Protocol.ReplicationScope.make({ models: [Todo.name, Numbered.name] }),
  scope: Protocol.ReplicationScope.make({ models: [Todo.name, Numbered.name] }),
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
  settlementCapacity: 64,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration
}
const layerServer = ServerStore.layerTrusted({
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
  Layer.provide(layerMutationRuntime),
  Layer.provide(layerDatabase)
)
const layerSync = Effect.gen(function*() {
  const store = yield* ServerStore.ServerStore
  return SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    submit: store.submit,
    discard: (request) => store.discard(request, null),
    pull: store.pull,
    bootstrap: store.bootstrap,
    watch: store.watch
  })
}).pipe(
  Layer.effect(SyncEngine.SyncEngine),
  Layer.provide(layerServer)
)
const layerEphemeralInactive = Layer.succeed(EphemeralClient.EphemeralClient, {
  join: () => Stream.never,
  publish: () => Effect.void,
  heartbeat: () => Effect.void
})
const layerReplica = Layer.merge(
  SqlReplica.layer({
    ...clientHistory,
    definition,
    initialSpaces: [spaceId, secondSpaceId],
    clientId,
    retryDelay: "10 millis"
  }).pipe(
    Layer.provide(layerSync),
    Layer.provide(layerDatabase),
    Layer.provide(layerHandlers)
  ),
  layerEphemeralInactive
)

const faultedReplica = (faultsReady: Deferred.Deferred<FaultInjection.Service>) => {
  const layerFaults = FaultInjection.layer.pipe(
    Layer.tap((context) => Deferred.succeed(faultsReady, Context.get(context, FaultInjection.FaultInjection)))
  )
  const layerFaultedSync = TestServer.layer.pipe(
    Layer.provide(layerServer),
    Layer.provide(layerFaults),
    Layer.provide(NodeCrypto.layer)
  )
  const layerFaultedReplica = TestReplica.layer({
    ...clientHistory,
    definition,
    initialSpaces: [spaceId],
    clientId,
    retryDelay: "10 millis"
  }).pipe(
    Layer.provide(layerFaultedSync),
    Layer.provide(layerDatabase),
    Layer.provide(layerHandlers)
  )
  return Layer.merge(layerFaultedReplica, layerEphemeralInactive)
}

describe("Replica Atom graph", () => {
  it.effect(
    "joins ephemera through atoms and applies live deltas",
    Effect.fnUntraced(function*() {
      const hub = yield* Layer.build(
        EphemeralHub.layerTrusted({ maximumWatchersPerSpace: 16 }).pipe(
          Layer.provide(NodeCrypto.layer)
        )
      ).pipe(
        Effect.map(Context.get(EphemeralHub.EphemeralHub))
      )
      const sessions = new Map<string, Identity.EphemeralSessionToken>()
      const sessionKey = (request: Protocol.EphemeralHeartbeatRequest) =>
        `${request.spaceId}:${request.member.clientId}:${request.member.membershipIncarnation}`
      const sessionToken = (request: {
        readonly spaceId: Identity.SpaceId
        readonly member: Protocol.EphemeralMember
      }): Effect.Effect<Identity.EphemeralSessionToken, ReplicaError.EphemeralSessionUnavailable> => {
        const token = sessions.get(sessionKey(request))
        if (token !== undefined) return Effect.succeed(token)
        return Effect.fail(
          new ReplicaError.EphemeralSessionUnavailable({
            spaceId: request.spaceId,
            clientId: request.member.clientId,
            membershipIncarnation: request.member.membershipIncarnation
          })
        )
      }
      const client = EphemeralClient.EphemeralClient.of({
        join: (request) =>
          hub.join(request, null).pipe(
            Stream.tap((message) => {
              if (message._tag !== "SessionStarted") return Effect.void
              sessions.set(sessionKey(request), message.sessionToken)
              return Effect.void
            }),
            Stream.filterMap((message) => {
              if (message._tag === "SessionStarted") return Result.fail(message)
              return Result.succeed(message)
            })
          ),
        publish: (request) =>
          sessionToken(request).pipe(
            Effect.flatMap((token) => hub.publish(request, token, null))
          ),
        heartbeat: (request) =>
          sessionToken(request).pipe(
            Effect.flatMap((token) => hub.heartbeat(request, token, null))
          )
      })
      const memberA = Protocol.EphemeralMember.make({
        clientId,
        membershipIncarnation: Identity.MembershipIncarnation.make(
          "inc_00000000-0000-4000-8000-000000000001"
        )
      })
      const memberB = Protocol.EphemeralMember.make({
        clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
        membershipIncarnation: Identity.MembershipIncarnation.make(
          "inc_00000000-0000-4000-8000-000000000002"
        )
      })
      const memberC = Protocol.EphemeralMember.make({
        clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000003"),
        membershipIncarnation: Identity.MembershipIncarnation.make(
          "inc_00000000-0000-4000-8000-000000000003"
        )
      })
      const memberD = Protocol.EphemeralMember.make({
        clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000004"),
        membershipIncarnation: Identity.MembershipIncarnation.make(
          "inc_00000000-0000-4000-8000-000000000004"
        )
      })
      const joinA = {
        spaceId,
        member: memberA,
        value: { status: "online" },
        ttlMillis: 10_000
      } satisfies Protocol.EphemeralJoinRequest
      const layerEphemeralClient = Layer.succeed(EphemeralClient.EphemeralClient, client)
      const graph = BrowserReplica.make(Layer.merge(layerReplica, layerEphemeralClient))
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const view = graph.ephemeral(joinA)
      const unmount = registry.mount(view)
      yield* Effect.addFinalizer(() => Effect.sync(unmount))
      const initial = yield* AtomRegistry.getResult(registry, view)
      assert.deepStrictEqual(initial.members.map((entry) => entry.member), [memberA])

      const memberVisible = yield* AtomRegistry.toStreamResult(registry, view).pipe(
        Stream.filter((current) => current.members.some((entry) => entry.member.clientId === memberB.clientId)),
        Stream.runHead,
        Effect.forkScoped({ startImmediately: true })
      )
      const secondSnapshot = yield* Deferred.make<void>()
      const secondSession = yield* Deferred.make<Protocol.EphemeralSessionStarted>()
      const second = yield* hub.join({
        spaceId,
        member: memberB,
        value: null,
        ttlMillis: 10_000
      }, null).pipe(
        Stream.tap((message) => {
          if (message._tag === "SessionStarted") return Deferred.succeed(secondSession, message)
          if (message._tag === "Snapshot") return Deferred.succeed(secondSnapshot, undefined)
          return Effect.void
        }),
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true })
      )
      const acceptedSecondSession = yield* Deferred.await(secondSession)
      yield* Deferred.await(secondSnapshot)
      yield* Fiber.join(memberVisible)

      const eventVisible = yield* AtomRegistry.toStreamResult(registry, view).pipe(
        Stream.filter((current) => current.events.some((entry) => entry.channel === "typing")),
        Stream.runHead,
        Effect.forkScoped({ startImmediately: true })
      )
      yield* hub.publish(
        {
          _tag: "Event",
          spaceId,
          member: memberB,
          channel: "typing",
          value: { active: true },
          ttlMillis: 5_000
        },
        acceptedSecondSession.sessionToken,
        null
      )
      assert.deepStrictEqual(
        Option.getOrThrow(yield* Fiber.join(eventVisible)).events.map((entry) => entry.value),
        [{ active: true }]
      )

      const stateVisible = yield* AtomRegistry.toStreamResult(registry, view).pipe(
        Stream.filter((current) =>
          Array.from(HashMap.values(current.states)).some((entry) => entry.key === "conversation-1")
        ),
        Stream.runHead,
        Effect.forkScoped({ startImmediately: true })
      )
      yield* hub.publish(
        {
          _tag: "SetState",
          spaceId,
          member: memberB,
          channel: "read",
          key: "conversation-1",
          value: { message: 42 },
          ttlMillis: 30_000
        },
        acceptedSecondSession.sessionToken,
        null
      )
      yield* Fiber.join(stateVisible)

      const lateView = graph.ephemeral({
        spaceId,
        member: memberC,
        value: null,
        ttlMillis: 10_000
      })
      const unmountLate = registry.mount(lateView)
      yield* Effect.addFinalizer(() => Effect.sync(unmountLate))
      const late = yield* AtomRegistry.getResult(registry, lateView)
      assert.deepStrictEqual(late.events, [])
      const lateStateValues = Array.from(HashMap.values(late.states), (entry) => entry.value)
      assert.deepStrictEqual(lateStateValues, [{ message: 42 }])
      const lateSettled = yield* AtomRegistry.toStreamResult(registry, lateView).pipe(
        Stream.filter((current) => HashMap.some(current.states, (entry) => entry.key === "sentinel")),
        Stream.runHead,
        Effect.forkScoped({ startImmediately: true })
      )
      yield* client.publish({
        _tag: "SetState",
        spaceId,
        member: memberA,
        channel: "read",
        key: "sentinel",
        value: true,
        ttlMillis: 30_000
      })
      assert.deepStrictEqual(Option.getOrThrow(yield* Fiber.join(lateSettled)).events, [])

      const departed = yield* AtomRegistry.toStreamResult(registry, view).pipe(
        Stream.filter((current) =>
          current.members.every((entry) => entry.member.clientId !== memberB.clientId) &&
          current.events.length === 0 && HashMap.size(current.states) === 2
        ),
        Stream.runHead,
        Effect.forkScoped({ startImmediately: true })
      )
      yield* Fiber.interrupt(second)
      yield* Fiber.join(departed)

      const publishedEventVisible = yield* AtomRegistry.toStreamResult(registry, view).pipe(
        Stream.filter((current) =>
          current.events.some((entry) => entry.channel === "typing" && sameMemberForTest(entry.member, memberA))
        ),
        Stream.runHead,
        Effect.forkScoped({ startImmediately: true })
      )
      registry.set(graph.publishEphemeral, {
        _tag: "Event",
        spaceId,
        member: memberA,
        channel: "typing",
        value: true,
        ttlMillis: 1_000
      })
      yield* AtomRegistry.getResult(registry, graph.publishEphemeral, { suspendOnWaiting: true })
      assert.deepStrictEqual(
        Option.getOrThrow(yield* Fiber.join(publishedEventVisible)).events
          .filter((entry) => sameMemberForTest(entry.member, memberA))
          .map((entry) => entry.value),
        [true]
      )

      const atomMemberLeft = yield* Deferred.make<void>()
      const observerReady = yield* Deferred.make<void>()
      const observer = yield* hub.join({
        spaceId,
        member: memberD,
        value: null,
        ttlMillis: 10_000
      }, null).pipe(
        Stream.tap((message) => {
          if (message._tag === "Snapshot") return Deferred.succeed(observerReady, undefined)
          if (message._tag === "MemberLeft" && sameMemberForTest(message.member, memberA)) {
            return Deferred.succeed(atomMemberLeft, undefined)
          }
          return Effect.void
        }),
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(observerReady)
      registry.dispose()
      yield* Deferred.await(atomMemberLeft)
      yield* Fiber.interrupt(observer)
    }, Effect.scoped)
  )

  it.effect(
    "replaces one mounted ephemeral view when its client resnapshots",
    Effect.fnUntraced(function*() {
      const memberA = Protocol.EphemeralMember.make({
        clientId,
        membershipIncarnation: Identity.MembershipIncarnation.make(
          "inc_00000000-0000-4000-8000-000000000001"
        )
      })
      const memberB = Protocol.EphemeralMember.make({
        clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002"),
        membershipIncarnation: Identity.MembershipIncarnation.make(
          "inc_00000000-0000-4000-8000-000000000002"
        )
      })
      const messages = yield* Queue.unbounded<Protocol.EphemeralMessage>()
      yield* Effect.addFinalizer(() => Queue.shutdown(messages))
      const client = EphemeralClient.EphemeralClient.of({
        join: () => Stream.fromQueue(messages),
        publish: () => Effect.void,
        heartbeat: () => Effect.void
      })
      const layerEphemeralClient = Layer.succeed(EphemeralClient.EphemeralClient, client)
      const graph = BrowserReplica.make(Layer.merge(layerReplica, layerEphemeralClient))
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const view = graph.ephemeral({
        spaceId,
        member: memberA,
        value: null,
        ttlMillis: 10_000
      })
      const unmount = registry.mount(view)
      yield* Effect.addFinalizer(() => Effect.sync(unmount))

      yield* Queue.offer(
        messages,
        Protocol.EphemeralSnapshot.make({
          spaceId,
          revision: Identity.EphemeralRevision.make(1),
          members: [{ member: memberA, value: null, expiresAtMillis: 10_000 }],
          states: [{
            member: memberA,
            channel: "read",
            key: "conversation-1",
            value: 1,
            expiresAtMillis: 10_000
          }]
        })
      )
      yield* Queue.offer(
        messages,
        Protocol.EphemeralEvent.make({
          spaceId,
          revision: Identity.EphemeralRevision.make(2),
          entry: {
            member: memberA,
            channel: "typing",
            value: true,
            expiresAtMillis: 1_000
          }
        })
      )
      const withEvent = yield* AtomRegistry.toStreamResult(registry, view).pipe(
        Stream.filter((current) => current.events.length === 1),
        Stream.runHead
      )
      assert.strictEqual(Option.getOrThrow(withEvent).events.length, 1)
      assert.strictEqual(HashMap.size(Option.getOrThrow(withEvent).states), 1)

      yield* Queue.offer(
        messages,
        Protocol.EphemeralSnapshot.make({
          spaceId,
          revision: Identity.EphemeralRevision.make(3),
          members: [{ member: memberB, value: null, expiresAtMillis: 10_000 }],
          states: []
        })
      )
      const replaced = yield* AtomRegistry.toStreamResult(registry, view).pipe(
        Stream.filter((current) => current.revision === 3),
        Stream.runHead,
        Effect.map(Option.getOrThrow)
      )
      assert.deepStrictEqual(replaced.members.map((entry) => entry.member), [memberB])
      assert.deepStrictEqual(replaced.events, [])
      assert.isTrue(HashMap.isEmpty(replaced.states))
    }, Effect.scoped)
  )

  it.effect(
    "reacts to pending mutation submission and settlement",
    Effect.fnUntraced(function*() {
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
    }, Effect.scoped)
  )

  it.effect(
    "reruns only an indexed query whose result range can change",
    Effect.fnUntraced(function*() {
      rangeReads.clear()
      const graph = BrowserReplica.make(layerReplica)
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
      assert.deepStrictEqual(
        yield* AtomRegistry.getResult(registry, unrelated, { suspendOnWaiting: true }),
        []
      )
      assert.isAtLeast(rangeReads.get("a:m") ?? 0, 2)
      assert.strictEqual(rangeReads.get("n:z"), 1)
    })
  )

  it.effect(
    "refreshes an entity atom when its key has a different encoded representation",
    Effect.fnUntraced(function*() {
      const graph = BrowserReplica.make(layerReplica)
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
      ;(yield* AtomRegistry.getResult(registry, entity)).pipe(
        Option.getOrThrow,
        (value) =>
          assert.deepStrictEqual(value, {
            id: 1,
            value: "encoded-key"
          })
      )
    })
  )

  it.effect(
    "does not rerun an unrelated mounted entity atom",
    Effect.fnUntraced(function*() {
      let unrelatedReads = 0
      const layerObserved = Effect.gen(function*() {
        const service = yield* Replica.Replica
        return Replica.Replica.of({
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
      }).pipe(
        Layer.effect(Replica.Replica),
        Layer.provideMerge(layerReplica)
      )
      const graph = BrowserReplica.make(layerObserved)
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
      ;(yield* AtomRegistry.getResult(registry, changed)).pipe(
        Option.getOrThrow,
        (value) =>
          assert.deepStrictEqual(value, {
            id: "changed",
            title: "new"
          })
      )
      assert.isTrue(Option.isNone(
        yield* AtomRegistry.getResult(registry, unrelated, { suspendOnWaiting: true })
      ))
      assert.strictEqual(unrelatedReads, 1)
    })
  )

  it.effect(
    "does not refresh membership or another space status for an addressed write",
    Effect.fnUntraced(function*() {
      let spacesReads = 0
      let secondStatusReads = 0
      const layerObserved = Effect.gen(function*() {
        const service = yield* Replica.Replica
        return Replica.Replica.of({
          ...service,
          spaces: Effect.suspend(() => {
            spacesReads++
            return service.spaces
          }),
          space: (address) =>
            service.space(address).pipe(Effect.map((space) => {
              if (address !== secondSpaceId) return space
              return {
                ...space,
                status: Effect.suspend(() => {
                  secondStatusReads++
                  return space.status
                })
              }
            }))
        })
      }).pipe(
        Layer.effect(Replica.Replica),
        Layer.provideMerge(layerReplica)
      )
      const graph = BrowserReplica.make(layerObserved)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const membership = graph.spaces
      const unrelatedStatus = graph.status(secondSpaceId)
      const mutation = graph.mutation(spaceId, PutTodo)
      const mounted = [
        registry.mount(membership),
        registry.mount(unrelatedStatus),
        registry.mount(mutation)
      ]
      yield* Effect.addFinalizer(() => Effect.sync(() => mounted.forEach((unmount) => unmount())))
      assert.lengthOf(yield* AtomRegistry.getResult(registry, membership), 2)
      assert.strictEqual((yield* AtomRegistry.getResult(registry, unrelatedStatus)).spaceId, secondSpaceId)
      const spacesBefore = spacesReads
      const secondStatusBefore = secondStatusReads

      registry.set(mutation, { id: "addressed-write", title: "first" })
      yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow

      assert.strictEqual(spacesReads, spacesBefore)
      assert.strictEqual(secondStatusReads, secondStatusBefore)
    })
  )

  it.effect(
    "reacts to per-space scope and activation commands",
    Effect.fnUntraced(function*() {
      const graph = BrowserReplica.make(layerReplica)
      const registry = AtomRegistry.make()
      yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
      const scopeAtom = graph.scope(spaceId)
      const activationAtom = graph.activation(spaceId)
      const setScope = graph.setScope(spaceId)
      const activate = graph.activate(spaceId)
      const deactivate = graph.deactivate(spaceId)
      const mounted = [
        registry.mount(scopeAtom),
        registry.mount(activationAtom),
        registry.mount(setScope),
        registry.mount(activate),
        registry.mount(deactivate)
      ]
      yield* Effect.addFinalizer(() => Effect.sync(() => mounted.forEach((unmount) => unmount())))

      registry.set(deactivate, undefined)
      yield* AtomRegistry.getResult(registry, deactivate, { suspendOnWaiting: true })
      assert.strictEqual(yield* AtomRegistry.getResult(registry, activationAtom), "Inactive")

      const empty = Protocol.ReplicationScope.make({ models: [] })
      registry.set(setScope, empty)
      yield* AtomRegistry.getResult(registry, setScope, { suspendOnWaiting: true })
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, scopeAtom), empty)
      assert.strictEqual(yield* AtomRegistry.getResult(registry, activationAtom), "Active")

      registry.set(deactivate, undefined)
      yield* AtomRegistry.getResult(registry, deactivate, { suspendOnWaiting: true })
      registry.set(activate, undefined)
      yield* AtomRegistry.getResult(registry, activate, { suspendOnWaiting: true })
      assert.strictEqual(yield* AtomRegistry.getResult(registry, activationAtom), "Active")
      assert.deepStrictEqual(yield* AtomRegistry.getResult(registry, scopeAtom), empty)
    }, Effect.scoped)
  )

  it("uses the shared runtime factory by default and preserves an explicit factory", () => {
    const graph = BrowserReplica.make(layerReplica)
    assert.strictEqual(graph.factory, Atom.runtime)

    const factory = Atom.context({ memoMap: Layer.makeMemoMapUnsafe() })
    const customGraph = BrowserReplica.make(layerReplica, { factory })
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
    const graph = BrowserReplica.make(layerReplica, { idleTTL })
    const readsAfterConstruction = reads

    assert.isAbove(readsAfterConstruction, 0)
    assert.strictEqual(graph.entity(spaceId, Todo)("1").idleTTL, 17)
    assert.strictEqual(graph.query(spaceId, ListTodos)(undefined).idleTTL, 17)
    const receipt = graph.receipt(
      spaceId,
      PutTodo,
      Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")
    )
    assert.strictEqual(receipt.idleTTL, 17)
    assert.strictEqual(reads, readsAfterConstruction)
  })

  it.live(
    "runs mutation, entity, query, receipt, and status state through one reactive runtime",
    Effect.fnUntraced(function*() {
      const graph = BrowserReplica.make(layerReplica, {
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
      pipe(
        (yield* AtomRegistry.getResult(registry, query)).filter((todo) => todo.id === "1"),
        (todos) => assert.deepStrictEqual(todos, [])
      )
      registry.set(mutation, { id: "1", title: "atom" })
      const pending = yield* AtomRegistry.getResult(registry, mutation, { suspendOnWaiting: true })
      yield* Effect.yieldNow
      ;(yield* AtomRegistry.getResult(registry, entity)).pipe(
        Option.getOrThrow,
        (value) =>
          assert.deepStrictEqual(value, {
            id: "1",
            title: "atom"
          })
      )
      assert.strictEqual(pending.envelope.name, PutTodo.name)
      const receipt = graph.receipt(spaceId, PutTodo, pending.envelope.mutationId)
      const accepted = Option.getOrThrow(Option.getOrThrow(
        yield* AtomRegistry.toStreamResult(registry, receipt).pipe(
          Stream.filter(Option.isSome),
          Stream.runHead
        )
      ))
      assert.strictEqual(accepted._tag, "Accepted")
      pipe(
        (yield* AtomRegistry.getResult(registry, query)).filter((todo) => todo.id === "1"),
        (todos) => assert.deepStrictEqual(todos, [{ id: "1", title: "atom" }])
      )
      const status = yield* AtomRegistry.getResult(registry, graph.status(spaceId))
      assert.strictEqual(status._tag, "Online")
    })
  )

  it.effect(
    "keeps addressed atoms isolated and shares membership through one runtime",
    Effect.fnUntraced(function*() {
      const graph = BrowserReplica.make(layerReplica)
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
      const firstValue = Option.getOrThrow(
        yield* AtomRegistry.getResult(registry, firstEntity, { suspendOnWaiting: true })
      )
      const secondValue = Option.getOrThrow(
        yield* AtomRegistry.getResult(registry, secondEntity, { suspendOnWaiting: true })
      )
      assert.strictEqual(firstValue.title, "first")
      assert.strictEqual(secondValue.title, "second")
      pipe(
        (yield* AtomRegistry.getResult(registry, firstQuery, { suspendOnWaiting: true })).filter((todo) =>
          todo.id === "shared"
        ),
        (todos) =>
          assert.deepStrictEqual(todos, [{
            id: "shared",
            title: "first"
          }])
      )
      pipe(
        (yield* AtomRegistry.getResult(registry, secondQuery, { suspendOnWaiting: true })).filter((todo) =>
          todo.id === "shared"
        ),
        (todos) =>
          assert.deepStrictEqual(todos, [{
            id: "shared",
            title: "second"
          }])
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
      const status = yield* AtomRegistry.getResult(registry, graph.status(spaceId))
      assert.strictEqual(status.spaceId, spaceId)
      assert.strictEqual((yield* AtomRegistry.getResult(registry, graph.aggregateStatus)).spaces, 2)

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
    })
  )
})
