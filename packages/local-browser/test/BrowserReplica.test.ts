import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as ReplicaAtom from "@lucas-barake/effect-local-rpc/ReplicaAtom"
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
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import { AtomRegistry } from "effect/reactivity"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Ref from "effect/Ref"
import * as Scheduler from "effect/Scheduler"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as SqlSchema from "effect/sql/SqlSchema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as BrowserReplica from "../src/BrowserReplica.js"
import * as LosslessQueue from "../src/internal/losslessQueue.js"
import * as platform from "../src/internal/platform.js"
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
const definitionNext = Definition.make({
  version: 2,
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

const layerSyncIdle = Layer.succeed(SyncEngine.SyncEngine, {
  waitForCredentialChange: () => Effect.never,
  transportGeneration: Effect.succeed(0),
  waitForTransportChange: () => Effect.never,
  submitBatch: () => Effect.never,
  discard: () => Effect.never,
  pull: () => Effect.never,
  bootstrap: () => Effect.never,
  watch: () => Stream.never
})

const layerOwnerIdle = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:" }),
  layerSyncIdle,
  layerEphemeralInactive
)

const StatusProfile = Ephemeral.member({ status: Schema.String })
const Reaction = Ephemeral.make("reaction", { kind: "event", payload: { emoji: Schema.String } })
const Wave = Ephemeral.make("wave", { kind: "event", payload: { hand: Schema.String } })
const Cursor = Ephemeral.make("cursor", { kind: "state", key: Schema.String, payload: { x: Schema.Number } })

interface Build {
  readonly definition: typeof definition
  readonly ephemerals: ReadonlyArray<Ephemeral.Any>
  readonly database: string
}

const currentBuild: Build = { definition, ephemerals: [Reaction], database: "replica" }
const nextVersionBuild: Build = { definition: definitionNext, ephemerals: [Reaction], database: "replica-next" }
const sameVersionRebuild: Build = { definition, ephemerals: [Reaction, Wave], database: "replica" }
const member = Protocol.EphemeralMember.make({
  clientId: Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000401"),
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000401")
})

const layerEphemeralOpening = (
  opening: Effect.Effect<void, ReplicaError.ReplicaError>,
  updates: Ref.Ref<ReadonlyArray<readonly [string, unknown]>>
) =>
  Layer.succeed(EphemeralClient.EphemeralClient, {
    session: (_profile, options) =>
      opening.pipe(Effect.as({
        spaceId: options.spaceId,
        member: options.member,
        events: () => Stream.never,
        state: () => Stream.never,
        members: Stream.never,
        updateMember: (value: unknown) =>
          Ref.update(updates, (recorded) => [...recorded, [options.member.clientId, value] as const])
      })),
    publish: () => Effect.void,
    clear: () => Effect.void,
    remove: () => Effect.void
  })

const settle = Effect.fnUntraced(function*<A, E extends { readonly _tag: string },>(effect: Effect.Effect<A, E>) {
  const fiber = yield* Effect.forkChild(effect)
  for (let step = 0; step < 100; step++) yield* TestClock.adjust("50 millis")
  return yield* Fiber.join(fiber)
})

const provideFileSystem = Effect.provide(NodeFileSystem.layer)

const statusChanges = (reactivity: Reactivity.Reactivity, space: Replica.Space) =>
  reactivity.query([ReactivityKey.status(spaceId)], space.status).pipe(
    Effect.map(LosslessQueue.stream),
    Stream.unwrap
  )

interface EnvironmentOptions {
  readonly layerEphemeral?: Layer.Layer<EphemeralClient.EphemeralClient>
  readonly submitAllowed?: () => boolean
  readonly sharding?: BrowserReplica.Options<typeof definition>["sharding"]
  readonly runIndex?: (query: Transaction.Query) => Effect.Effect<number, ListTodosError>
  readonly name?: string
  readonly kit?: testKit.MemoryPlatform
  readonly retryDelay?: BrowserReplica.Options<typeof definition>["retryDelay"]
  readonly eventCapacity?: BrowserReplica.Options<typeof definition>["eventCapacity"]
  readonly pullGate?: Effect.Effect<void>
  readonly layerOwnerProbe?: Layer.Layer<never, OwnerProbeError>
}

class OwnerProbeError extends Schema.TaggedError<OwnerProbeError>("test/OwnerProbeError")(
  "OwnerProbeError",
  {}
) {}

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
      submitBatch: (request) =>
        Effect.suspend(() => {
          if (environmentOptions.submitAllowed?.() ?? true) return store.admitBatch(request, null)
          return Effect.never
        }),
      discard: (request) => store.discard(request, null),
      pull: (request) => (environmentOptions.pullGate ?? Effect.void).pipe(Effect.andThen(store.pull(request))),
      bootstrap: store.bootstrap,
      watch: store.watch
    }),
    environmentOptions.layerEphemeral ?? layerEphemeralInactive
  )
  const databaseLog = yield* Ref.make<ReadonlyArray<string>>([])
  const layerDatabaseLifecycle = (database: string) =>
    Effect.acquireRelease(
      Ref.update(databaseLog, (log) => [...log, `open:${database}`]),
      () => Ref.update(databaseLog, (log) => [...log, `close:${database}`])
    ).pipe(Layer.effectDiscard)
  const layerDatabaseFor = (database: string) =>
    SqliteClient.layer({ filename: `${directory}/${database}.sqlite` }).pipe(
      Layer.provideMerge(layerDatabaseLifecycle(database)),
      Layer.tap(() => Ref.update(databaseOpens, (count) => count + 1))
    )
  const layerReplicaWith = (visibility: platform.TabVisibilityService, build: Build = currentBuild) => {
    const layerOwner = Layer.mergeAll(
      layerDatabaseFor(build.database),
      layerSync,
      environmentOptions.layerOwnerProbe ?? Layer.empty
    )
    const layerVisibility = Layer.succeed(platform.TabVisibility, visibility)
    return BrowserReplica.layer(layerOwner, {
      name: environmentOptions.name ?? "tabs",
      definition: build.definition,
      spaces: [spaceId],
      profiles: { status: StatusProfile },
      ephemerals: build.ephemerals,
      requestPersistence: false,
      retryDelay: environmentOptions.retryDelay ?? "100 millis",
      eventCapacity: environmentOptions.eventCapacity,
      sharding: environmentOptions.sharding
    }).pipe(
      Layer.provide(layerHandlersWith(environmentOptions.runIndex ?? firstRunIndex)),
      Layer.provide(Layer.mergeAll(kit.layerAll, layerVisibility, NodeCrypto.layer))
    )
  }
  const layerReplica = Layer.unwrap(
    testKit.makeMemoryVisibility(true).pipe(Effect.map((visibility) => layerReplicaWith(visibility.service)))
  )
  const openTabWith = Effect.fnUntraced(function*(visible: boolean, build: Build = currentBuild) {
    const visibility = yield* testKit.makeMemoryVisibility(visible)
    const layerTab = layerReplicaWith(visibility.service, build).pipe(
      Layer.provideMerge(Layer.fresh(Reactivity.layer))
    )
    const scope = yield* Scope.make()
    const context = yield* settle(Layer.buildWithScope(layerTab, scope))
    return { scope, context, replica: Context.get(context, Replica.Replica), visibility }
  })
  const openTab = openTabWith(true)
  const openBuild = (build: Build) => openTabWith(true, build)
  return {
    openTab,
    openTabWith,
    openBuild,
    databaseOpens,
    databaseLog,
    layerReplica,
    layerReplicaWith,
    traffic: kit.traffic
  }
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

const makeFenceProbe = Effect.gen(function*() {
  const holding = yield* Deferred.make<void>()
  const fenced = yield* Deferred.make<void>()
  let runs = 0
  const runIndex = (query: Transaction.Query) =>
    Effect.suspend(() => {
      runs += 1
      if (runs > 1) return listTodos(query).pipe(Effect.as(runs))
      return Deferred.succeed(holding, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.onInterrupt(() => Deferred.succeed(fenced, undefined))
      )
    })
  return { runIndex, holding: Deferred.await(holding), fenced: Deferred.await(fenced) }
})

const failureTag = <A, E extends { readonly _tag: string },>(exit: Exit.Exit<A, E>) =>
  Exit.match(exit, {
    onSuccess: () => "succeeded",
    onFailure: (cause) =>
      Option.match(Cause.findErrorOption(cause), {
        onNone: () => "interrupted",
        onSome: (error) => error._tag
      })
  })

const settledOutcome = Effect.fnUntraced(
  function*<A, E extends { readonly _tag: string },>(effect: Effect.Effect<A, E>) {
    const fiber = yield* Effect.forkChild(effect)
    for (let step = 0; step < 100; step++) yield* TestClock.adjust("50 millis")
    const exit = fiber.pollUnsafe()
    if (exit === undefined) {
      yield* Fiber.interrupt(fiber)
      return "pending"
    }
    return failureTag(exit)
  }
)

const openStatusSession = (context: Context.Context<EphemeralClient.EphemeralClient>) =>
  Context.get(context, EphemeralClient.EphemeralClient).session(StatusProfile, {
    spaceId,
    member,
    value: { status: "online" },
    ttl: "30 seconds"
  })

const sessionStreamEndsWithItsScope = Effect.fnUntraced(
  function*(
    select: (
      session: EphemeralClient.Session<typeof StatusProfile>
    ) => Stream.Stream<unknown, Ephemeral.DecodeError | ReplicaError.ReplicaError>
  ) {
    const updates = yield* Ref.make<ReadonlyArray<readonly [string, unknown]>>([])
    const environment = yield* makeEnvironmentWith({ layerEphemeral: layerEphemeralOpening(Effect.void, updates) })
    const leader = yield* environment.openTab
    const sessionScope = yield* Scope.make()
    const session = yield* settle(openStatusSession(leader.context).pipe(Scope.provide(sessionScope)))
    const consumer = yield* Effect.forkChild(select(session).pipe(Stream.runDrain))
    assert.strictEqual(yield* settledOutcome(Fiber.join(consumer)), "pending")
    yield* settle(Scope.close(sessionScope, Exit.void))
    assert.strictEqual(yield* settledOutcome(Fiber.join(consumer)), "succeeded")
  },
  Effect.scoped,
  provideFileSystem
)

const listFrom = (replica: Replica.Service) =>
  replica.space(spaceId).pipe(Effect.flatMap((space) => space.query(ListTodos, undefined)))

const liveSettlementsFailWhenTheTabCloses = Effect.fnUntraced(
  function*() {
    const environment = yield* makeEnvironment
    const leader = yield* environment.openTabWith(true)
    const space = yield* settle(leader.replica.space(spaceId))
    const streaming = yield* Effect.forkChild(space.settlements({ from: "live" }).pipe(Stream.runDrain))
    yield* TestClock.adjust("5 seconds")
    yield* settle(Scope.close(leader.scope, Exit.void))
    assert.strictEqual(failureTag(yield* settle(Fiber.await(streaming))), "OwnerUnavailable")
  },
  Effect.scoped,
  provideFileSystem
)

const followerMemberUpdatesAfterLeaderCloses = Effect.fnUntraced(
  function*() {
    const opens = yield* Ref.make(0)
    const reopened = yield* Deferred.make<void>()
    const opening = Ref.getAndUpdate(opens, (count) => count + 1).pipe(
      Effect.flatMap((count) => {
        if (count === 0) return Effect.void
        return Deferred.await(reopened)
      })
    )
    const updates = yield* Ref.make<ReadonlyArray<readonly [string, unknown]>>([])
    const environment = yield* makeEnvironmentWith({ layerEphemeral: layerEphemeralOpening(opening, updates) })
    const leader = yield* environment.openTab
    const follower = yield* environment.openTab
    const session = yield* settle(openStatusSession(follower.context).pipe(Scope.provide(yield* Effect.scope)))
    yield* settle(Scope.close(leader.scope, Exit.void))
    assert.strictEqual(yield* Ref.get(opens), 2)
    const updated = yield* settle(session.updateMember({ status: "away" }).pipe(Effect.exit))
    assert.isTrue(Exit.isSuccess(updated), String(updated))
    yield* settle(Deferred.succeed(reopened, undefined))
    assert.deepStrictEqual(yield* Ref.get(updates), [[member.clientId, { status: "away" }]])
  },
  Effect.scoped,
  provideFileSystem
)

const followerSessionUpdatesReachTheirOwnSessionsAfterHandover = Effect.fnUntraced(
  function*() {
    const updates = yield* Ref.make<ReadonlyArray<readonly [string, unknown]>>([])
    const environment = yield* makeEnvironmentWith({ layerEphemeral: layerEphemeralOpening(Effect.void, updates) })
    const leader = yield* environment.openTabWith(true)
    const follower = yield* environment.openTabWith(false)
    const ephemeral = Context.get(follower.context, EphemeralClient.EphemeralClient)
    const members = [0, 1, 2, 3].map((index) =>
      Protocol.EphemeralMember.make({
        clientId: Identity.ClientId.make(`cli_00000000-0000-4000-8000-00000000041${index}`),
        membershipIncarnation: member.membershipIncarnation
      })
    )
    const scope = yield* Effect.scope
    const sessions = yield* settle(Effect.forEach(
      members,
      (sessionMember) =>
        ephemeral.session(StatusProfile, {
          spaceId,
          member: sessionMember,
          value: { status: "online" },
          ttl: "30 seconds"
        }).pipe(Scope.provide(scope)),
      { concurrency: "unbounded" }
    ))
    yield* settle(Scope.close(leader.scope, Exit.void))
    yield* settle(Effect.forEach(
      sessions,
      (session, index) => session.updateMember({ status: `away ${index}` }),
      { discard: true }
    ))
    const recorded = (yield* Ref.get(updates)).toSorted(([left], [right]) => left.localeCompare(right))
    assert.deepStrictEqual(
      recorded,
      members.map((sessionMember, index) => [sessionMember.clientId, { status: `away ${index}` }] as const)
    )
  },
  Effect.scoped,
  provideFileSystem
)

const rapidVisibilityFlips = Effect.fnUntraced(
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

describe("BrowserReplica retryDelay", () => {
  const invalidRetryDelays: ReadonlyArray<readonly [string, Duration.Input]> = [
    ["zero millis", 0],
    ["negative millis", -1],
    ["NaN millis", Number.NaN],
    ["infinite millis", Number.POSITIVE_INFINITY],
    ["negative infinite millis", Number.NEGATIVE_INFINITY],
    ["negative nanos", -1n],
    ["the Infinity string", "Infinity"],
    ["the -Infinity string", "-Infinity"],
    ["a negative unit string", "-5 seconds"],
    ["a zero unit string", "0 millis"],
    ["a negative seconds tuple", [-1, 0]],
    ["a NaN seconds tuple", [Number.NaN, 0]],
    ["a negative duration object", { seconds: -1 }],
    ["an infinite Duration", Duration.infinity],
    ["an unparseable unit string", "1e3 seconds"]
  ]

  for (const [label, retryDelay] of invalidRetryDelays) {
    it.effect(
      `rejects ${label} with InvalidConfiguration`,
      Effect.fnUntraced(
        function*() {
          const environment = yield* makeEnvironmentWith({ retryDelay })
          const visibility = yield* testKit.makeMemoryVisibility(true)
          const layerTab = environment.layerReplicaWith(visibility.service).pipe(
            Layer.provideMerge(Layer.fresh(Reactivity.layer))
          )
          const outcome = yield* settle(
            Layer.build(layerTab).pipe(
              Effect.as("built"),
              Effect.catchTag("InvalidConfiguration", (error) => Effect.succeed(error.option)),
              Effect.scoped
            )
          )
          assert.strictEqual(outcome, "retryDelay")
        },
        Effect.scoped,
        provideFileSystem
      )
    )
  }

  it.effect(
    "waits exactly retryDelay before rebuilding a failed owner stack",
    Effect.fnUntraced(
      function*() {
        const attempts = yield* Queue.unbounded<number>()
        const sleeps = yield* Queue.unbounded<number>()
        const builds = yield* Ref.make(0)
        const clock = yield* Clock.Clock
        const recordingClock: Clock.Clock = {
          currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe(),
          currentTimeMillis: clock.currentTimeMillis,
          currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
          currentTimeNanos: clock.currentTimeNanos,
          monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
          monotonicTimeNanos: clock.monotonicTimeNanos,
          sleep: Effect.fnUntraced(function*(duration) {
            const registered = yield* Effect.forkChild(clock.sleep(duration), { startImmediately: true })
            yield* Queue.offer(sleeps, Duration.toMillis(duration))
            yield* Fiber.join(registered)
          })
        }
        const layerOwnerProbe = Layer.effectDiscard(
          Clock.currentTimeMillis.pipe(
            Effect.tap((now) => Queue.offer(attempts, now)),
            Effect.andThen(Ref.updateAndGet(builds, (count) => count + 1)),
            Effect.flatMap((count) => {
              if (count === 1) return Effect.fail(new OwnerProbeError())
              return Effect.void
            })
          )
        )
        const environment = yield* makeEnvironmentWith({ retryDelay: "7 seconds", layerOwnerProbe })
        const visibility = yield* testKit.makeMemoryVisibility(true)
        yield* environment.layerReplicaWith(visibility.service).pipe(
          Layer.provideMerge(Layer.fresh(Reactivity.layer)),
          Layer.build,
          Effect.provideService(Clock.Clock, recordingClock),
          Effect.forkScoped
        )
        const first = yield* Queue.take(attempts)
        let requested = yield* Queue.take(sleeps)
        while (requested !== 7_000) {
          requested = yield* Queue.take(sleeps)
        }
        yield* TestClock.adjust(6_999)
        assert.isTrue(Option.isNone(yield* Queue.poll(attempts)))
        yield* TestClock.adjust(1)
        const second = yield* Queue.take(attempts)
        assert.strictEqual(second - first, 7_000)
      },
      Effect.scoped,
      provideFileSystem
    )
  )
})

const ttlBuild: Build = { definition, ephemerals: [Reaction, Cursor], database: "replica" }

const layerEphemeralRecording = (received: Ref.Ref<ReadonlyArray<readonly [string, number]>>) =>
  Layer.succeed(EphemeralClient.EphemeralClient, {
    session: (_profile, options) =>
      Ref.update(received, (recorded) => [...recorded, ["session", Duration.toMillis(options.ttl)] as const]).pipe(
        Effect.as({
          spaceId: options.spaceId,
          member: options.member,
          events: () => Stream.never,
          state: () => Stream.never,
          members: Stream.never,
          updateMember: () => Effect.void
        })
      ),
    publish: (ephemeralDefinition: Ephemeral.Any, options: { readonly ttl: Duration.Input }) =>
      Ref.update(
        received,
        (recorded) => [...recorded, [ephemeralDefinition.name, Duration.toMillis(options.ttl)] as const]
      ),
    clear: () => Effect.void,
    remove: () => Effect.void
  })

type TtlFailure = ReplicaError.ReplicaError | Ephemeral.EncodeError

const describeTtlFailure = (error: TtlFailure) => {
  if (error._tag === "InvalidConfiguration") return `InvalidConfiguration ${error.option}: ${error.message}`
  return error._tag
}

const describeTtlExit = (exit: Exit.Exit<unknown, TtlFailure>) =>
  Exit.match(exit, {
    onSuccess: () => "succeeded",
    onFailure: (cause) =>
      Option.match(Cause.findErrorOption(cause), {
        onNone: () => `defect: ${String(Cause.squash(cause))}`,
        onSome: describeTtlFailure
      })
  })

const ttlOutcome = Effect.fnUntraced(function*(operation: () => Effect.Effect<unknown, TtlFailure, Scope.Scope>) {
  const fiber = yield* Effect.forkChild(Effect.suspend(operation))
  for (let step = 0; step < 100; step++) yield* TestClock.adjust("50 millis")
  const exit = fiber.pollUnsafe()
  if (exit === undefined) {
    yield* Fiber.interrupt(fiber)
    return "pending"
  }
  return describeTtlExit(exit)
})

interface TtlOperation {
  readonly label: string
  readonly wireName: string
  readonly minimum: number
  readonly maximum: number
  readonly run: (
    ephemeral: EphemeralClient.Service,
    ttl: Duration.Input
  ) => Effect.Effect<unknown, TtlFailure, Scope.Scope>
}

const ttlOperations: ReadonlyArray<TtlOperation> = [
  {
    label: "a session",
    wireName: "session",
    minimum: Protocol.minimumEphemeralMemberTtlMillis,
    maximum: Protocol.maximumEphemeralMemberTtlMillis,
    run: (ephemeral, ttl) => ephemeral.session(StatusProfile, { spaceId, member, value: { status: "here" }, ttl })
  },
  {
    label: "an event publish",
    wireName: Reaction.name,
    minimum: 1,
    maximum: Protocol.maximumEphemeralEventTtlMillis,
    run: (ephemeral, ttl) => ephemeral.publish(Reaction, { spaceId, member, payload: { emoji: "+1" }, ttl })
  },
  {
    label: "a state publish",
    wireName: Cursor.name,
    minimum: 1,
    maximum: Protocol.maximumEphemeralStateTtlMillis,
    run: (ephemeral, ttl) => ephemeral.publish(Cursor, { spaceId, member, key: "pointer", payload: { x: 1 }, ttl })
  }
]

const ttlAttempt = Effect.fnUntraced(function*(operation: TtlOperation, ttl: Duration.Input) {
  const received = yield* Ref.make<ReadonlyArray<readonly [string, number]>>([])
  const environment = yield* makeEnvironmentWith({ layerEphemeral: layerEphemeralRecording(received) })
  yield* environment.openBuild(ttlBuild)
  const follower = yield* environment.openBuild(ttlBuild)
  const ephemeral = Context.get(follower.context, EphemeralClient.EphemeralClient)
  const outcome = yield* ttlOutcome(() => operation.run(ephemeral, ttl))
  return { outcome, received: yield* Ref.get(received) }
})

describe("BrowserReplica ephemeral ttl", () => {
  const notADuration = "InvalidConfiguration ttl: ttl must be a valid positive finite duration"
  const outOfBounds = (operation: TtlOperation) =>
    `InvalidConfiguration ttl: ttl must resolve to between ${operation.minimum} and ${operation.maximum} milliseconds`

  const invalidDurations: ReadonlyArray<readonly [string, Duration.Input]> = [
    ["zero millis", 0],
    ["negative millis", -1],
    ["an unparseable unit string", "1e3 seconds"],
    ["millis beyond the safe integer range", Number.MAX_VALUE]
  ]

  interface TtlCase {
    readonly title: (operation: TtlOperation) => string
    readonly ttl: (operation: TtlOperation) => Duration.Input
    readonly outcome: (operation: TtlOperation) => string
    readonly sent: (operation: TtlOperation) => ReadonlyArray<number>
  }

  const ttlCases: ReadonlyArray<TtlCase> = [
    ...invalidDurations.map(([label, ttl]): TtlCase => ({
      title: (operation) => `rejects ${operation.label} with a ttl of ${label} before reaching the leader`,
      ttl: () => ttl,
      outcome: () => notADuration,
      sent: () => []
    })),
    {
      title: (operation) =>
        `rejects ${operation.label} with a fractional ttl that rounds above the maximum before reaching the leader`,
      ttl: (operation) => operation.maximum + 0.25,
      outcome: outOfBounds,
      sent: () => []
    },
    {
      title: (operation) => `rounds a fractional ttl up to the minimum for ${operation.label}`,
      ttl: (operation) => operation.minimum - 0.75,
      outcome: () => "succeeded",
      sent: (operation) => [operation.minimum]
    },
    {
      title: (operation) => `sends the maximum ttl for ${operation.label} given as a Duration`,
      ttl: (operation) => Duration.millis(operation.maximum),
      outcome: () => "succeeded",
      sent: (operation) => [operation.maximum]
    }
  ]

  for (const operation of ttlOperations) {
    for (const ttlCase of ttlCases) {
      it.effect(
        ttlCase.title(operation),
        Effect.fnUntraced(
          function*() {
            assert.deepStrictEqual(yield* ttlAttempt(operation, ttlCase.ttl(operation)), {
              outcome: ttlCase.outcome(operation),
              received: ttlCase.sent(operation).map((millis) => [operation.wireName, millis] as const)
            })
          },
          Effect.scoped,
          provideFileSystem
        )
      )
    }
  }

  it.effect(
    "rejects a session with a ttl below the member minimum before reaching the leader",
    Effect.fnUntraced(
      function*() {
        const [session] = ttlOperations
        assert.deepStrictEqual(yield* ttlAttempt(session, Protocol.minimumEphemeralMemberTtlMillis - 1), {
          outcome: outOfBounds(session),
          received: []
        })
      },
      Effect.scoped,
      provideFileSystem
    )
  )
})

const layerEphemeralReactions = (source: Queue.Dequeue<string>) => {
  function events<D extends Ephemeral.AnyEvent,>(
    eventMember: Protocol.EphemeralMember
  ): Stream.Stream<EphemeralClient.EventEnvelope<D>>
  function events(
    eventMember: Protocol.EphemeralMember
  ): Stream.Stream<EphemeralClient.EventEnvelope<typeof Reaction>> {
    return Stream.fromQueue(source).pipe(Stream.map((emoji) => ({ member: eventMember, payload: { emoji } })))
  }
  return Layer.succeed(EphemeralClient.EphemeralClient, {
    session: (_profile, options) =>
      Effect.succeed({
        spaceId: options.spaceId,
        member: options.member,
        events: () => events(options.member),
        state: () => Stream.never,
        members: Stream.never,
        updateMember: () => Effect.void
      }),
    publish: () => Effect.void,
    clear: () => Effect.void,
    remove: () => Effect.void
  })
}

const openFollowerReactions = Effect.fnUntraced(function*(eventCapacity: number | undefined) {
  const source = yield* Queue.unbounded<string>()
  const environment = yield* makeEnvironmentWith({ layerEphemeral: layerEphemeralReactions(source), eventCapacity })
  yield* environment.openTabWith(true)
  const follower = yield* environment.openTabWith(false)
  const session = yield* settle(openStatusSession(follower.context).pipe(Scope.provide(yield* Effect.scope)))
  const subscribe = Effect.fnUntraced(function*(gate: Effect.Effect<void>) {
    const seen = yield* Queue.unbounded<string>()
    const fiber = yield* session.events(Reaction).pipe(
      Stream.runForEach((envelope) => Queue.offer(seen, envelope.payload.emoji).pipe(Effect.andThen(gate))),
      Effect.as("ended"),
      Effect.catchTag("CapacityExceeded", (error) => Effect.succeed(`${error.resource} ${error.limit}`)),
      Effect.forkChild
    )
    yield* settle(Effect.void)
    return { seen, fiber }
  })
  const react = (emoji: string) => Queue.offer(source, emoji)
  return { subscribe, react }
})

describe("BrowserReplica ephemeral event buffer", () => {
  it.effect(
    "fails a follower subscriber that stopped reading while more events than the capacity arrived",
    Effect.fnUntraced(
      function*() {
        const follower = yield* openFollowerReactions(4)
        const resume = yield* Deferred.make<void>()
        const reading = yield* follower.subscribe(Effect.void)
        const stalled = yield* follower.subscribe(Deferred.await(resume))
        for (let index = 0; index < 12; index++) {
          yield* follower.react(`${index}`)
          assert.strictEqual(yield* Queue.take(reading.seen), `${index}`)
        }
        assert.strictEqual(yield* Queue.take(stalled.seen), "0")
        yield* settle(Deferred.succeed(resume, undefined))
        assert.isAtMost(yield* Queue.size(stalled.seen), 4)
        const outcome = stalled.fiber.pollUnsafe()
        assert.isDefined(outcome, "the stalled subscriber was never told that its buffer overflowed")
        if (outcome !== undefined) assert.strictEqual(yield* outcome, "ephemeral events 4")
        assert.isUndefined(reading.fiber.pollUnsafe())
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "delivers every event in order to a follower subscriber that keeps reading past the capacity",
    Effect.fnUntraced(
      function*() {
        const follower = yield* openFollowerReactions(4)
        const reading = yield* follower.subscribe(Effect.void)
        for (let index = 0; index < 12; index++) {
          yield* follower.react(`${index}`)
          assert.strictEqual(yield* Queue.take(reading.seen), `${index}`)
        }
        assert.isUndefined(reading.fiber.pollUnsafe())
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "neither delivers nor counts events that reached the follower before a subscription existed",
    Effect.fnUntraced(
      function*() {
        const follower = yield* openFollowerReactions(4)
        const witness = yield* follower.subscribe(Effect.void)
        for (let index = 0; index < 6; index++) {
          yield* follower.react(`early ${index}`)
          assert.strictEqual(yield* Queue.take(witness.seen), `early ${index}`)
        }
        const late = yield* follower.subscribe(Effect.void)
        yield* follower.react("late")
        assert.strictEqual(yield* Queue.take(late.seen), "late")
        assert.strictEqual(yield* Queue.size(late.seen), 0)
        assert.isUndefined(late.fiber.pollUnsafe())
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "bounds a follower subscriber at 1024 buffered events when no capacity is configured",
    Effect.fnUntraced(
      function*() {
        const follower = yield* openFollowerReactions(undefined)
        const resume = yield* Deferred.make<void>()
        const reading = yield* follower.subscribe(Effect.void)
        const stalled = yield* follower.subscribe(Deferred.await(resume))
        for (let index = 0; index < 1_040; index++) {
          yield* follower.react(`${index}`)
          assert.strictEqual(yield* Queue.take(reading.seen), `${index}`)
        }
        yield* settle(Deferred.succeed(resume, undefined))
        const outcome = stalled.fiber.pollUnsafe()
        assert.isDefined(outcome, "the stalled subscriber was never told that its buffer overflowed")
        if (outcome !== undefined) assert.strictEqual(yield* outcome, "ephemeral events 1024")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  const invalidCapacities: ReadonlyArray<readonly [string, number]> = [
    ["zero", 0],
    ["a negative integer", -1],
    ["a fraction", 1.5],
    ["NaN", Number.NaN],
    ["infinity", Number.POSITIVE_INFINITY],
    ["an unsafe integer", Number.MAX_SAFE_INTEGER + 1]
  ]

  for (const [label, eventCapacity] of invalidCapacities) {
    it.effect(
      `rejects ${label} as the event capacity with InvalidConfiguration`,
      Effect.fnUntraced(
        function*() {
          const environment = yield* makeEnvironmentWith({ eventCapacity })
          const visibility = yield* testKit.makeMemoryVisibility(true)
          const layerTab = environment.layerReplicaWith(visibility.service).pipe(
            Layer.provideMerge(Layer.fresh(Reactivity.layer))
          )
          const outcome = yield* settle(
            Layer.build(layerTab).pipe(
              Effect.as("built"),
              Effect.catchTag("InvalidConfiguration", (error) => Effect.succeed(error.option)),
              Effect.scoped
            )
          )
          assert.strictEqual(outcome, "eventCapacity")
        },
        Effect.scoped,
        provideFileSystem
      )
    )
  }
})

describe("BrowserReplica", () => {
  it.effect(
    "reports the leader replica's first sync to a follower tab through the synced status",
    Effect.fnUntraced(
      function*() {
        const pullReleased = yield* Deferred.make<void>()
        const environment = yield* makeEnvironmentWith({ pullGate: Deferred.await(pullReleased) })
        yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        yield* settle(space.activate)
        const before = yield* settle(space.status)
        assert.strictEqual(before._tag, "Connecting")
        assert.strictEqual(before.synced, false)

        const clock = yield* TestClock.adjust("100 millis").pipe(
          Effect.forever,
          Effect.forkChild({ startImmediately: true })
        )
        const reactivity = Context.get(follower.context, Reactivity.Reactivity)
        const synced = statusChanges(reactivity, space).pipe(
          Stream.filter((status) => status.synced),
          Stream.runHead
        )
        const observed = yield* synced.pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.succeed(pullReleased, undefined)
        const status = yield* Fiber.join(observed)
        yield* Fiber.interrupt(clock)
        assert.isTrue(Option.isSome(status))
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "keeps notifying the subscribers of a follower tab after one of them threw",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        yield* settle(space.activate)
        const reactivity = Context.get(follower.context, Reactivity.Reactivity)
        let throws = 0
        let notified = 0
        reactivity.registerUnsafe([ReactivityKey.pending(spaceId)], () => {
          throws += 1
          decodeURIComponent("%")
        })
        reactivity.registerUnsafe([ReactivityKey.status(spaceId)], () => {
          notified += 1
        })

        yield* settle(space.mutate(PutTodo, { id: "1", title: "first" }))
        const throwsAfterFirst = throws
        const notifiedAfterFirst = notified
        yield* settle(space.mutate(PutTodo, { id: "2", title: "second" }))

        assert.isAbove(throwsAfterFirst, 0, "the subscriber threw while the first mutation was announced")
        assert.isAbove(notifiedAfterFirst, 0, "the status subscriber was notified of the first mutation")
        assert.isAbove(throws, throwsAfterFirst, "the second mutation reached the subscriber that throws")
        assert.isAbove(notified, notifiedAfterFirst, "the status subscriber was notified of the second mutation")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "notifies the subscribers of a follower tab that was opened inside a batch",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        yield* environment.openTab
        const outer = yield* Reactivity.make
        const follower = yield* outer.withBatch(environment.openTab)
        const space = yield* settle(follower.replica.space(spaceId))
        yield* settle(space.activate)
        const reactivity = Context.get(follower.context, Reactivity.Reactivity)
        let pending = 0
        let status = 0
        reactivity.registerUnsafe([ReactivityKey.pending(spaceId)], () => {
          pending += 1
        })
        reactivity.registerUnsafe([ReactivityKey.status(spaceId)], () => {
          status += 1
        })

        yield* settle(space.mutate(PutTodo, { id: "1", title: "first" }))

        assert.isAbove(pending, 0, "the pending subscriber was notified of the mutation")
        assert.isAbove(status, 0, "the status subscriber was notified of the mutation")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

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
        const settled: Array<number> = []
        const unsubscribe = registry.subscribe(runIndex, (result) => {
          if (!AsyncResult.isSuccess(result)) return
          if (shown.at(-1) !== result.value) shown.push(result.value)
          if (!result.waiting) settled.push(result.value)
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
        assert.deepStrictEqual(settled, [1, 3])
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
    rapidVisibilityFlips
  )

  it.effect(
    "fails a caller's in-flight query with OwnerUnavailable when its tab's replica closes",
    Effect.fnUntraced(
      function*() {
        const { started, runIndex: gatedRunIndex } = yield* makeGatedRunIndex
        const environment = yield* makeEnvironmentWith({ runIndex: gatedRunIndex })
        const leader = yield* environment.openTabWith(true)
        const space = yield* settle(leader.replica.space(spaceId))
        const querying = yield* Effect.forkChild(space.query(RunIndex, undefined))
        yield* settle(Queue.take(started))
        yield* settle(Scope.close(leader.scope, Exit.void))
        assert.strictEqual(failureTag(yield* settle(Fiber.await(querying))), "OwnerUnavailable")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "fails a caller's live settlement stream with OwnerUnavailable when its tab's replica closes",
    liveSettlementsFailWhenTheTabCloses
  )

  it.effect(
    "retries a quarantine resubmission that a handover interrupts on the old leader",
    Effect.fnUntraced(
      function*() {
        const probe = yield* makeFenceProbe
        const environment = yield* makeEnvironmentWith({ runIndex: probe.runIndex })
        const leader = yield* environment.openTabWith(true)
        const follower = yield* environment.openTabWith(false)
        const space = yield* settle(follower.replica.space(spaceId))
        yield* space.query(RunIndex, undefined).pipe(Effect.forkChild)
        yield* settle(probe.holding)
        const resubmitting = yield* Effect.forkChild(
          space.resubmitQuarantined(mutationId, PutTodo, { id: "q", title: "resubmitted" })
        )
        yield* TestClock.adjust("1 second")
        yield* leader.visibility.set(false)
        yield* follower.visibility.set(true)
        yield* settle(probe.fenced)
        assert.strictEqual(failureTag(yield* settle(Fiber.await(resubmitting))), "ProtocolInvalid")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "publishes an ephemeral event once when a handover starts during its delivery",
    Effect.fnUntraced(
      function*() {
        const probe = yield* makeFenceProbe
        const publishes = yield* Ref.make(0)
        const publishing = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const environment = yield* makeEnvironmentWith({
          runIndex: probe.runIndex,
          layerEphemeral: Layer.succeed(EphemeralClient.EphemeralClient, {
            session: () => Effect.never,
            publish: () =>
              Ref.update(publishes, (count) => count + 1).pipe(
                Effect.andThen(Deferred.succeed(publishing, undefined)),
                Effect.andThen(Deferred.await(release))
              ),
            clear: () => Effect.void,
            remove: () => Effect.void
          })
        })
        const leader = yield* environment.openTabWith(true)
        const follower = yield* environment.openTabWith(false)
        const space = yield* settle(follower.replica.space(spaceId))
        yield* space.query(RunIndex, undefined).pipe(Effect.forkChild)
        yield* settle(probe.holding)
        const ephemeral = Context.get(follower.context, EphemeralClient.EphemeralClient)
        const reacting = yield* Effect.forkChild(
          ephemeral.publish(Reaction, { spaceId, member, payload: { emoji: "+1" }, ttl: "5 seconds" })
        )
        yield* settle(Deferred.await(publishing))
        yield* leader.visibility.set(false)
        yield* follower.visibility.set(true)
        yield* settle(probe.fenced)
        yield* Deferred.succeed(release, undefined)
        assert.strictEqual(failureTag(yield* settle(Fiber.await(reacting))), "succeeded")
        assert.strictEqual(yield* Ref.get(publishes), 1)
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
    "reports Connecting, not Offline, to a follower while the new leader's first sync is slow",
    Effect.fnUntraced(
      function*() {
        const pullReleased = yield* Deferred.make<void>()
        let holdPulls = false
        const environment = yield* makeEnvironmentWith({
          pullGate: Effect.suspend(() => {
            if (holdPulls) return Deferred.await(pullReleased)
            return Effect.void
          })
        })
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const space = yield* settle(follower.replica.space(spaceId))
        const reactivity = Context.get(follower.context, Reactivity.Reactivity)
        const observed: Array<string> = []
        const awaitOnline = statusChanges(reactivity, space).pipe(
          Stream.tap((status) => Effect.sync(() => observed.push(status._tag))),
          Stream.filter((status) => status._tag === "Online"),
          Stream.runHead
        )
        yield* settle(space.mutate(PutTodo, { id: "1", title: "before the failover" }))
        assert.isTrue(Option.isSome(yield* settle(awaitOnline)))

        holdPulls = true
        yield* settle(Scope.close(leader.scope, Exit.void))
        yield* settle(space.mutate(PutTodo, { id: "2", title: "after the failover" }))
        observed.length = 0
        const online = yield* awaitOnline.pipe(Effect.forkChild({ startImmediately: true }))
        assert.strictEqual((yield* settle(space.status))._tag, "Connecting")
        yield* TestClock.adjust("1 minute")
        assert.strictEqual((yield* settle(space.status))._tag, "Connecting")

        yield* Deferred.succeed(pullReleased, undefined)
        assert.isTrue(Option.isSome(yield* settle(Fiber.join(online))))
        assert.notInclude(observed, "Offline")
        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 2)
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "never reports Offline to a follower's status atom across a leader handover while the server is reachable",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        yield* environment.openTab
        const hidden = yield* testKit.makeMemoryVisibility(false)
        const graph = ReplicaAtom.make(environment.layerReplicaWith(hidden.service))
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const todos = graph.query(spaceId, ListTodos)(undefined)
        const status = graph.status(spaceId)
        const beforeHandover: Array<string> = []
        const afterHandover: Array<string> = []
        let observed = beforeHandover
        let online = yield* Deferred.make<void>()
        const unmountTodos = registry.mount(todos)
        const unsubscribe = registry.subscribe(status, (result) => {
          if (AsyncResult.isInitial(result)) return
          if (AsyncResult.isFailure(result)) {
            observed.push("Failure")
            return
          }
          observed.push(result.value._tag)
          if (result.value._tag === "Online" && !result.waiting) Deferred.doneUnsafe(online, Exit.void)
        }, { immediate: true })
        yield* Effect.addFinalizer(() => Effect.sync(() => [unsubscribe(), unmountTodos()]))
        assert.deepStrictEqual(yield* settle(AtomRegistry.getResult(registry, todos, { suspendOnWaiting: true })), [])
        yield* settle(Deferred.await(online))

        observed = afterHandover
        online = yield* Deferred.make<void>()
        yield* settle(Scope.close(leader.scope, Exit.void))
        yield* settle(Deferred.await(online))

        assert.strictEqual(yield* Ref.get(environment.databaseOpens), 2)
        assert.notInclude(afterHandover, "Offline")
        assert.notInclude(beforeHandover, "Offline")
        assert.notInclude(afterHandover, "Failure")
        assert.strictEqual(afterHandover.at(-1), "Online")
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
        const received = yield* Effect.forkChild(space.settlements({ from: "live" }).pipe(Stream.runHead))
        const pending = yield* settle(space.mutate(PutTodo, { id: "6", title: "settles on the new leader" }))
        submitAllowed = true
        yield* settle(Scope.close(leader.scope, Exit.void))
        const settled = yield* settle(Fiber.join(received))
        assert.isTrue(Option.isSome(settled))
        if (Option.isSome(settled)) {
          assert.strictEqual(settled.value.settlement.pending.envelope.mutationId, pending.envelope.mutationId)
        }
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect("updates a follower's ephemeral member after the leader tab closes", followerMemberUpdatesAfterLeaderCloses)

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
          layerEphemeral: layerEphemeralOpening(opening, yield* Ref.make<ReadonlyArray<readonly [string, unknown]>>([]))
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
        yield* settle(space.settlements({ from: 0 }).pipe(Stream.take(3), Stream.runDrain))
        const live = yield* settle(space.resolveSettlementStart("live"))
        assert.strictEqual(live, 3)
        const liveOpen = yield* Deferred.make<void>()
        yield* space.settlements({ from: live }).pipe(
          Stream.runForEach(() => Deferred.succeed(liveOpen, undefined)),
          Effect.forkScoped
        )
        yield* settle(space.mutate(PutTodo, { id: "d", title: "d" }))
        yield* settle(Deferred.await(liveOpen))
        const acknowledgedOpen = yield* Deferred.make<void>()
        yield* space.settlements({ from: "acknowledged" }).pipe(
          Stream.runForEach(() => Deferred.succeed(acknowledgedOpen, undefined)),
          Effect.forkScoped
        )
        yield* settle(Deferred.await(acknowledgedOpen))
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
        yield* settle(space.mutate(PutTodo, { id: "a", title: "a" }))
        yield* settle(space.mutate(PutTodo, { id: "b", title: "b" }))
        assert.deepStrictEqual(yield* settle(Effect.all([Queue.take(delivered), Queue.take(delivered)])), [1, 2])
        yield* settle(Scope.close(leader.scope, Exit.void))
        yield* settle(space.mutate(PutTodo, { id: "c", title: "c" }))
        assert.strictEqual(yield* settle(Queue.take(delivered)), 3)
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
        }),
        NodeCrypto.layer
      )
      const outcome = yield* Layer.build(
        BrowserReplica.layer(layerOwnerIdle, { name: "corrupt-identity", definition, requestPersistence: false }).pipe(
          Layer.provide(layerHandlers),
          Layer.provide(Reactivity.layer),
          Layer.provide(layerPlatform)
        )
      ).pipe(
        Effect.as("built" as const),
        Effect.catchTag("BrowserStorageError", (error) => Effect.succeed(error.operation))
      )
      assert.strictEqual(outcome, "decode")
    }, Effect.scoped)
  )

  it.effect(
    "generates the durable client identity with the platform's Crypto",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const nodeCrypto = Context.get(yield* Layer.build(NodeCrypto.layer), Crypto.Crypto)
      const zeroCrypto = Crypto.make({
        randomBytes: (size) => new Uint8Array(size),
        digest: (algorithm, data) => nodeCrypto.digest(algorithm, data)
      })
      const stored = yield* Deferred.make<string>()
      const layerPlatform = Layer.mergeAll(
        Layer.succeed(platform.TabChannel, kit.tabChannel),
        Layer.succeed(platform.WebLocks, kit.webLocks),
        Layer.succeed(platform.TabVisibility, (yield* testKit.makeMemoryVisibility(true)).service),
        Layer.succeed(platform.ClientIdentityStore, {
          load: () => Effect.succeed(undefined),
          store: (_key, value) => Deferred.succeed(stored, value).pipe(Effect.asVoid)
        }),
        Layer.succeed(Crypto.Crypto, zeroCrypto)
      )
      yield* Layer.build(
        BrowserReplica.layer(layerOwnerIdle, { name: "platform-crypto", definition, requestPersistence: false }).pipe(
          Layer.provide(layerHandlers),
          Layer.provide(Reactivity.layer),
          Layer.provide(layerPlatform)
        )
      ).pipe(Effect.forkScoped)
      assert.strictEqual(yield* Deferred.await(stored), "cli_00000000-0000-4000-8000-000000000000")
    }, Effect.scoped)
  )
})

describe("BrowserReplica at small scheduler budgets", () => {
  it.effect(
    "fails a caller's live settlement stream with OwnerUnavailable when its tab's replica closes at a scheduler budget of 20 operations",
    () => liveSettlementsFailWhenTheTabCloses().pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 20))
  )
  it.effect(
    "updates a follower's ephemeral member after the leader tab closes at a scheduler budget of 5 operations",
    () => followerMemberUpdatesAfterLeaderCloses().pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 5))
  )
  it.effect(
    "serves the visible tab through rapid visibility flips without losing or repeating a mutation at a scheduler budget of 31 operations",
    () => rapidVisibilityFlips().pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 31))
  )
  it.effect(
    "delivers each follower ephemeral session's member update to that session after a handover at a scheduler budget of 9 operations",
    () =>
      followerSessionUpdatesReachTheirOwnSessionsAfterHandover().pipe(
        Effect.provideService(Scheduler.MaxOpsBeforeYield, 9)
      )
  )
})

describe("BrowserReplica across builds", () => {
  it.effect(
    "hands the database to a newer build's tab and fails every older build tab with BuildSuperseded",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTabWith(true)
        const follower = yield* environment.openTabWith(false)
        const next = yield* environment.openBuild(nextVersionBuild)
        const space = yield* settle(next.replica.space(spaceId))
        yield* settle(space.mutate(PutTodo, { id: "1", title: "from the newer build" }))
        assert.deepStrictEqual(yield* settle(listFrom(next.replica)), [{ id: "1", title: "from the newer build" }])
        assert.strictEqual(yield* settledOutcome(listFrom(leader.replica)), "BuildSuperseded")
        assert.strictEqual(yield* settledOutcome(listFrom(follower.replica)), "BuildSuperseded")
        assert.deepStrictEqual(yield* Ref.get(environment.databaseLog), [
          "open:replica",
          "close:replica",
          "open:replica-next"
        ])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "keeps an older build's tab opened beside a newer leader away from the database",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const next = yield* environment.openBuild(nextVersionBuild)
        const older = yield* environment.openTab
        const failure = yield* settledOutcome(listFrom(older.replica))
        assert.strictEqual(failure, "BuildSuperseded")
        const superseded = yield* settle(
          listFrom(older.replica).pipe(
            Effect.as(undefined),
            Effect.catchTag(
              "BuildSuperseded",
              (error) => Effect.succeed({ version: error.version, supersedingVersion: error.supersedingVersion })
            )
          )
        )
        assert.deepStrictEqual(superseded, { version: 1, supersedingVersion: 2 })
        assert.deepStrictEqual(yield* settle(listFrom(next.replica)), [])
        yield* settle(Scope.close(next.scope, Exit.void))
        assert.strictEqual(yield* settledOutcome(listFrom(older.replica)), "BuildSuperseded")
        assert.deepStrictEqual(yield* Ref.get(environment.databaseLog), ["open:replica-next", "close:replica-next"])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "lets the latest started build of an equal definition version take over the shared database",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const first = yield* environment.openTab
        const firstSpace = yield* settle(first.replica.space(spaceId))
        yield* settle(firstSpace.mutate(PutTodo, { id: "1", title: "from the first build" }))
        const rebuilt = yield* environment.openBuild(sameVersionRebuild)
        const space = yield* settle(rebuilt.replica.space(spaceId))
        yield* settle(space.mutate(PutTodo, { id: "2", title: "from the rebuilt build" }))
        assert.deepStrictEqual(yield* settle(listFrom(rebuilt.replica)), [
          { id: "1", title: "from the first build" },
          { id: "2", title: "from the rebuilt build" }
        ])
        assert.strictEqual(yield* settledOutcome(listFrom(first.replica)), "BuildSuperseded")
        assert.deepStrictEqual(yield* Ref.get(environment.databaseLog), [
          "open:replica",
          "close:replica",
          "open:replica"
        ])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "fails an older build's in-flight query and live settlement stream with BuildSuperseded",
    Effect.fnUntraced(
      function*() {
        const probe = yield* makeFenceProbe
        const environment = yield* makeEnvironmentWith({ runIndex: probe.runIndex })
        yield* environment.openTabWith(true)
        const follower = yield* environment.openTabWith(false)
        const space = yield* settle(follower.replica.space(spaceId))
        const querying = yield* Effect.forkChild(space.query(RunIndex, undefined))
        const streaming = yield* Effect.forkChild(space.settlements({ from: "live" }).pipe(Stream.runDrain))
        yield* settle(probe.holding)
        yield* environment.openBuild(nextVersionBuild)
        yield* settle(probe.fenced)
        assert.strictEqual(yield* settledOutcome(Fiber.join(querying)), "BuildSuperseded")
        assert.strictEqual(yield* settledOutcome(Fiber.join(streaming)), "BuildSuperseded")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "ends a session's event stream when the session scope closes",
    () => sessionStreamEndsWithItsScope((session) => session.events(Reaction))
  )

  it.effect(
    "ends a session's member stream when the session scope closes",
    () => sessionStreamEndsWithItsScope((session) => session.members)
  )

  it.effect(
    "ends a session's state stream when the session scope closes",
    () => sessionStreamEndsWithItsScope((session) => session.state(Cursor))
  )

  it.effect(
    "fails an older build tab's ephemeral session projections with BuildSuperseded",
    Effect.fnUntraced(
      function*() {
        const updates = yield* Ref.make<ReadonlyArray<readonly [string, unknown]>>([])
        const environment = yield* makeEnvironmentWith({ layerEphemeral: layerEphemeralOpening(Effect.void, updates) })
        const leader = yield* environment.openTab
        const session = yield* settle(openStatusSession(leader.context).pipe(Scope.provide(yield* Effect.scope)))
        const members = yield* Effect.forkChild(session.members.pipe(Stream.runDrain))
        yield* environment.openBuild(nextVersionBuild)
        assert.strictEqual(yield* settledOutcome(Fiber.join(members)), "BuildSuperseded")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "fails a member update with BuildSuperseded when the tab is superseded while its session reopens",
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
        const updates = yield* Ref.make<ReadonlyArray<readonly [string, unknown]>>([])
        const environment = yield* makeEnvironmentWith({ layerEphemeral: layerEphemeralOpening(opening, updates) })
        const leader = yield* environment.openTab
        const follower = yield* environment.openTab
        const session = yield* settle(openStatusSession(follower.context).pipe(Scope.provide(yield* Effect.scope)))
        yield* settle(Scope.close(leader.scope, Exit.void))
        yield* environment.openBuild(nextVersionBuild)
        assert.strictEqual(yield* settledOutcome(listFrom(follower.replica)), "BuildSuperseded")
        yield* Deferred.succeed(reopened, undefined)
        assert.strictEqual(yield* settledOutcome(session.updateMember({ status: "away" })), "BuildSuperseded")
        assert.deepStrictEqual(yield* Ref.get(updates), [])
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "yields to a newer build whose presence lock name carries fields this build does not know",
    Effect.fnUntraced(
      function*() {
        const kit = yield* testKit.makeMemoryPlatform
        const environment = yield* makeEnvironmentWith({ kit })
        const current = yield* environment.openTab
        const base = "@lucas-barake/effect-local-browser:tabs:presence"
        yield* kit.webLocks.acquire(`${base}:9:2:0123456789abcdef:future-host:future-field`)
        const nudges = yield* kit.tabChannel.open(base)
        yield* nudges.post("future-host")
        const superseded = yield* settle(
          listFrom(current.replica).pipe(
            Effect.as(undefined),
            Effect.catchTag(
              "BuildSuperseded",
              (error) => Effect.succeed({ version: error.version, supersedingVersion: error.supersedingVersion })
            )
          )
        )
        assert.deepStrictEqual(superseded, { version: 1, supersedingVersion: 2 })
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "moves a superseded tab's status atoms to a BuildSuperseded failure",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        yield* environment.openTab
        const graph = ReplicaAtom.make(environment.layerReplica)
        const registry = AtomRegistry.make()
        yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()))
        const status = graph.status(spaceId)
        const aggregate = graph.aggregateStatus
        const unmountStatus = registry.mount(status)
        const unmountAggregate = registry.mount(aggregate)
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            unmountStatus()
            unmountAggregate()
          })
        )
        yield* settle(AtomRegistry.getResult(registry, status, { suspendOnWaiting: true }))
        yield* settle(AtomRegistry.getResult(registry, aggregate, { suspendOnWaiting: true }))
        yield* environment.openBuild(nextVersionBuild)
        assert.strictEqual(
          yield* settledOutcome(AtomRegistry.getResult(registry, status, { suspendOnWaiting: true })),
          "BuildSuperseded"
        )
        assert.strictEqual(
          yield* settledOutcome(AtomRegistry.getResult(registry, aggregate, { suspendOnWaiting: true })),
          "BuildSuperseded"
        )
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "lets a hidden tab lead when the only visible tab of its build was superseded earlier",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const superseded = yield* environment.openTab
        const firstSpace = yield* settle(superseded.replica.space(spaceId))
        yield* settle(firstSpace.mutate(PutTodo, { id: "1", title: "from the first tab" }))
        yield* environment.openBuild(sameVersionRebuild)
        assert.strictEqual(yield* settledOutcome(listFrom(superseded.replica)), "BuildSuperseded")
        const hidden = yield* environment.openTabWith(false)
        assert.strictEqual(yield* settledOutcome(listFrom(hidden.replica)), "succeeded")
        assert.strictEqual(yield* settledOutcome(listFrom(superseded.replica)), "BuildSuperseded")
      },
      Effect.scoped,
      provideFileSystem
    )
  )

  it.effect(
    "leaves tabs of the same build serving one another when another tab of that build opens",
    Effect.fnUntraced(
      function*() {
        const environment = yield* makeEnvironment
        const leader = yield* environment.openTab
        const follower = yield* environment.openTabWith(false)
        const space = yield* settle(follower.replica.space(spaceId))
        yield* settle(space.mutate(PutTodo, { id: "1", title: "before" }))
        const late = yield* environment.openTabWith(false)
        assert.deepStrictEqual(yield* settle(listFrom(late.replica)), [{ id: "1", title: "before" }])
        assert.deepStrictEqual(yield* settle(listFrom(leader.replica)), [{ id: "1", title: "before" }])
        assert.deepStrictEqual(yield* Ref.get(environment.databaseLog), ["open:replica"])
      },
      Effect.scoped,
      provideFileSystem
    )
  )
})
