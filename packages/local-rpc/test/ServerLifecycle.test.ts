import { NodeCrypto, NodeFileSystem, NodeSocket, NodeSocketServer } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import type * as Cause from "effect/Cause"
import * as SingleRunner from "effect/cluster/SingleRunner"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as MutableRef from "effect/MutableRef"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Redacted from "effect/Redacted"
import * as RpcClient from "effect/rpc/RpcClient"
import * as RpcServer from "effect/rpc/RpcServer"
import * as Schedule from "effect/Schedule"
import * as Scheduler from "effect/Scheduler"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as SocketServer from "effect/socket/SocketServer"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Authentication from "../src/Authentication.js"
import * as EphemeralClient from "../src/EphemeralClient.js"
import * as LosslessQueue from "../src/internal/losslessQueue.js"
import * as SyncClient from "../src/SyncClient.js"
import * as SyncRpc from "../src/SyncRpc.js"
import * as SyncServer from "../src/SyncServer.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const secondSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const clientA = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000000a")
const clientB = Identity.ClientId.make("cli_00000000-0000-4000-8000-00000000000b")
const memberOf = (clientId: Identity.ClientId) =>
  Protocol.EphemeralMember.make({
    clientId,
    membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
  })
const Presence = Ephemeral.member({ name: Schema.String })

const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const definition = Definition.make({ version: 1, models: [Todo], mutations: [PutTodo] })
const scope = Protocol.ReplicationScope.make({ models: [Todo.name] })
const layerHandlers = PutTodo.toLayer(({ payload, transaction }) =>
  transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))
)

const layerAuthenticator = Layer.succeed(
  Authentication.Authenticator,
  Authentication.Authenticator.of({ authenticate: () => Effect.succeed({ subject: "test" }) })
)
const layerServerAuthentication = Authentication.layerServer.pipe(Layer.provide(layerAuthenticator))
const layerServerDatabase = (filename: string) =>
  Layer.mergeAll(SqliteClient.layer({ filename }), NodeCrypto.layer, Reactivity.layer)
interface ServerBehavior {
  readonly authorizeRead?: SyncServer.LayerOptions<typeof definition>["authorizeRead"]
  readonly authorizeMutation?: SyncServer.LayerOptions<typeof definition>["authorizeMutation"]
}

const layerWebSocketServer = Layer.effect(
  SocketServer.SocketServer,
  NodeSocketServer.makeWebSocket({ port: 0 }).pipe(Effect.provideService(Scheduler.PreventSchedulerYield, true))
)

const layerServer = (filename: string, behavior: ServerBehavior) =>
  SyncServer.layer({
    definition,
    store: { acceptedSchemaVersions: 0, readAuthorizationRefreshInterval: "1 minute" },
    authorizeAccess: () => Effect.void,
    authorizeMutation: behavior.authorizeMutation ?? (() => Effect.void),
    authorizeRead: behavior.authorizeRead ?? (() => Effect.void),
    authorizeEphemeral: () => Effect.void
  }).pipe(
    Layer.provide(RpcServer.layerProtocolSocketServer),
    Layer.provide(layerServerAuthentication),
    Layer.provide(SingleRunner.layer({ runnerStorage: "memory" })),
    Layer.provide(layerHandlers),
    Layer.provide(layerServerDatabase(filename)),
    Layer.provideMerge(layerWebSocketServer),
    Layer.provide(SyncRpc.layerJson())
  )

const layerClientCredentials = Authentication.layerCredentialProviderStatic(Redacted.make("secret"))
const layerClientDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)

interface ReplicaBehavior {
  readonly initialSpaces?: ReadonlyArray<Identity.SpaceId>
  readonly retryDelay?: Duration.Input
  readonly maximumRetryDelay?: Duration.Input
}

const makeRestartHarness = Effect.fnUntraced(function*() {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  const filename = `${directory}/server.db`
  const currentUrl = MutableRef.make("")
  const start = Effect.fnUntraced(function*(behavior: ServerBehavior = {}) {
    const serverScope = yield* Scope.make()
    const context = yield* Layer.buildWithScope(layerServer(filename, behavior), serverScope).pipe(TestClock.withLive)
    const address = Context.get(context, SocketServer.SocketServer).address
    if (address._tag === "UnixPathAddress") return yield* Effect.die("Expected a TCP test server")
    MutableRef.set(currentUrl, `ws://127.0.0.1:${address.port}/sync`)
    return serverScope
  })
  const stop = (serverScope: Scope.Closeable) => Scope.close(serverScope, Exit.void)
  const resolveUrl = Effect.sync(() => MutableRef.get(currentUrl))
  const layerClient = (
    connections: Queue.Queue<void>,
    rejoinPolicy?: Schedule.Schedule<unknown, ReplicaError.ReplicaError>
  ) => {
    const hooks = RpcClient.ConnectionHooks.of({
      onConnect: Queue.offer(connections, undefined),
      onDisconnect: Effect.void
    })
    let options: SyncClient.WebSocketOptions = { url: resolveUrl }
    if (rejoinPolicy !== undefined) options = { ...options, rejoinPolicy }
    return SyncClient.layerWebSocket(options).pipe(
      Layer.provide(NodeSocket.layerWebSocketConstructor),
      Layer.provide(layerClientCredentials),
      Layer.provide(Layer.succeed(RpcClient.ConnectionHooks, hooks))
    )
  }
  const layerReplica = (
    clientId: Identity.ClientId,
    connections: Queue.Queue<void>,
    behavior: ReplicaBehavior = {}
  ) =>
    SqlReplica.layer({
      definition,
      clientId,
      initialSpaces: behavior.initialSpaces ?? [spaceId],
      retryDelay: behavior.retryDelay ?? "1 second",
      maximumRetryDelay: behavior.maximumRetryDelay ?? "1 second"
    }).pipe(
      Layer.provide(layerHandlers),
      Layer.provideMerge(layerClientDatabase),
      Layer.provideMerge(layerClient(connections))
    )
  return { start, stop, layerClient, layerReplica }
}, Effect.provide(NodeFileSystem.layer))

const awaitStatus = (
  reactivity: Reactivity.Reactivity,
  space: Replica.Space,
  predicate: (status: ReplicaStatus.SpaceStatus) => boolean
) =>
  reactivity.query([`effect-local:space:${space.spaceId}:status`], space.status).pipe(
    Effect.map(LosslessQueue.stream),
    Stream.unwrap,
    Stream.filter(predicate),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

const openReplica = Effect.fnUntraced(function*(context: Context.Context<Replica.Replica | Reactivity.Reactivity>) {
  const space = yield* Context.get(context, Replica.Replica).space(spaceId)
  yield* space.activate
  return { space, reactivity: Context.get(context, Reactivity.Reactivity) }
})

type RosterObservation =
  | { readonly _tag: "Roster"; readonly clientIds: ReadonlyArray<Identity.ClientId> }
  | { readonly _tag: "Ended"; readonly exit: Exit.Exit<void, Ephemeral.DecodeError | ReplicaError.ReplicaError> }

const nextRoster = Effect.fnUntraced(function*(
  observations: Queue.Queue<RosterObservation>,
  expected: ReadonlyArray<Identity.ClientId>
) {
  let observation = yield* LosslessQueue.take(observations)
  while (observation._tag === "Roster" && observation.clientIds.join(",") !== expected.join(",")) {
    observation = yield* LosslessQueue.take(observations)
  }
  assert.deepStrictEqual(observation, { _tag: "Roster", clientIds: expected })
})

const reasonTag = (reason: Cause.Reason<ReplicaError.ReplicaError>) => {
  if (reason._tag === "Fail") return reason.error._tag
  return reason._tag
}

describe("server lifecycle", () => {
  it.effect(
    "fails an open watch with ServerUnavailable when the server shuts down",
    Effect.fnUntraced(function*() {
      const harness = yield* makeRestartHarness()
      const server = yield* harness.start()
      const connections = yield* Queue.unbounded<void>()
      const remote = Context.get(yield* Layer.build(harness.layerClient(connections)), SyncEngine.SyncEngine)
      const subscribed = yield* Deferred.make<void>()
      const watching = yield* remote.watch({
        spaceId,
        clientId: clientA,
        schema: definition.schemaIdentity,
        scope,
        scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
        cursor: null
      }).pipe(
        Stream.runForEach(() => Deferred.succeed(subscribed, undefined)),
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(subscribed)

      yield* harness.stop(server)

      const exit = yield* Fiber.await(watching)
      assert.isTrue(Exit.isFailure(exit), "the watch must end when the server shuts down")
      if (Exit.isFailure(exit)) {
        assert.deepStrictEqual(exit.cause.reasons.map(reasonTag), ["ServerUnavailable"])
      }
    })
  )

  it.effect(
    "reports Offline during a server restart and resumes the watch afterwards",
    Effect.fnUntraced(function*() {
      const harness = yield* makeRestartHarness()
      const first = yield* harness.start()
      const connectionsA = yield* Queue.unbounded<void>()
      const connectionsB = yield* Queue.unbounded<void>()
      const a = yield* openReplica(yield* Layer.build(harness.layerReplica(clientA, connectionsA)))
      const b = yield* openReplica(yield* Layer.build(harness.layerReplica(clientB, connectionsB)))
      yield* Effect.all([
        awaitStatus(a.reactivity, a.space, (status) => status._tag === "Online"),
        awaitStatus(b.reactivity, b.space, (status) => status._tag === "Online")
      ], { concurrency: "unbounded", discard: true })
      yield* Queue.takeAll(connectionsA)
      yield* Queue.takeAll(connectionsB)

      const offline = yield* awaitStatus(a.reactivity, a.space, (status) => status._tag === "Offline").pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* harness.stop(first)
      assert.strictEqual((yield* Fiber.join(offline))._tag, "Offline")

      yield* harness.start()
      const online = yield* awaitStatus(a.reactivity, a.space, (status) => status._tag === "Online").pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* TestClock.adjust("500 millis")
      yield* LosslessQueue.take(connectionsA)
      yield* LosslessQueue.take(connectionsB)
      yield* TestClock.adjust("1 second")
      const resumed = yield* Fiber.join(online)
      assert.strictEqual(resumed._tag, "Online")
      let resumedCursor = -1
      if (resumed._tag === "Online") resumedCursor = resumed.cursor
      yield* awaitStatus(b.reactivity, b.space, (status) => status._tag === "Online")

      const delivered = yield* awaitStatus(
        a.reactivity,
        a.space,
        (status) => status._tag === "Online" && status.cursor > resumedCursor
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* b.space.mutate(PutTodo, { id: "after-restart", title: "sent by B" })
      yield* Fiber.join(delivered)
      assert.deepStrictEqual(
        yield* a.space.get(Todo, "after-restart"),
        Option.some({ id: "after-restart", title: "sent by B" })
      )
    })
  )

  it.effect(
    "clears the ephemeral roster while the server is down and rejoins after a restart",
    Effect.fnUntraced(function*() {
      const harness = yield* makeRestartHarness()
      const first = yield* harness.start()
      const connectionsA = yield* Queue.unbounded<void>()
      const connectionsB = yield* Queue.unbounded<void>()
      const ephemeralA = Context.get(
        yield* Layer.build(harness.layerClient(connectionsA)),
        EphemeralClient.EphemeralClient
      )
      const ephemeralB = Context.get(
        yield* Layer.build(harness.layerClient(connectionsB)),
        EphemeralClient.EphemeralClient
      )
      const sessionA = yield* ephemeralA.session(Presence, {
        spaceId,
        member: memberOf(clientA),
        value: { name: "A" },
        ttl: "1 minute"
      })
      yield* ephemeralB.session(Presence, { spaceId, member: memberOf(clientB), value: { name: "B" }, ttl: "1 minute" })
      const observations = yield* Queue.unbounded<RosterObservation>()
      yield* sessionA.members.pipe(
        Stream.runForEach((entries) =>
          Queue.offer(observations, {
            _tag: "Roster",
            clientIds: entries.map((entry) => entry.member.clientId).toSorted()
          })
        ),
        Effect.exit,
        Effect.flatMap((exit) => Queue.offer(observations, { _tag: "Ended", exit })),
        Effect.forkChild({ startImmediately: true })
      )
      yield* nextRoster(observations, [clientA, clientB])
      yield* Queue.takeAll(connectionsA)
      yield* Queue.takeAll(connectionsB)

      yield* harness.stop(first)
      yield* nextRoster(observations, [])

      yield* harness.start()
      yield* TestClock.adjust("500 millis")
      yield* LosslessQueue.take(connectionsA)
      yield* LosslessQueue.take(connectionsB)
      yield* TestClock.adjust("1 second")
      yield* nextRoster(observations, [clientA, clientB])
    })
  )

  it.effect(
    "reports Connecting while a reachable server is slow and Offline only once the transport fails",
    Effect.fnUntraced(function*() {
      const harness = yield* makeRestartHarness()
      const server = yield* harness.start({ authorizeRead: () => Effect.never })
      const connections = yield* Queue.unbounded<void>()
      const context = yield* Layer.build(
        harness.layerReplica(clientA, connections, { initialSpaces: [spaceId, secondSpaceId] })
      )
      const replica = Context.get(context, Replica.Replica)
      const reactivity = Context.get(context, Reactivity.Reactivity)
      const first = yield* replica.space(spaceId)
      const second = yield* replica.space(secondSpaceId)
      yield* Effect.all([first.activate, second.activate], { discard: true })
      yield* Effect.raceFirst(
        awaitStatus(reactivity, first, (status) => status._tag === "Connecting"),
        awaitStatus(reactivity, second, (status) => status._tag === "Connecting")
      )

      assert.deepStrictEqual([(yield* first.status)._tag, (yield* second.status)._tag], ["Connecting", "Connecting"])
      assert.strictEqual((yield* replica.status).state, "Connecting")

      yield* harness.stop(server)
      yield* Effect.all([
        awaitStatus(reactivity, first, (status) => status._tag === "Offline"),
        awaitStatus(reactivity, second, (status) => status._tag === "Offline")
      ], { concurrency: "unbounded", discard: true })
      assert.strictEqual((yield* replica.status).state, "Offline")
    })
  )

  it.effect(
    "resumes the watch and the pending sync as soon as the transport reconnects instead of waiting out the backoff",
    Effect.fnUntraced(function*() {
      const harness = yield* makeRestartHarness()
      const submitting = yield* Deferred.make<void>()
      const first = yield* harness.start({
        authorizeMutation: () => Deferred.succeed(submitting, undefined).pipe(Effect.andThen(Effect.never))
      })
      const connectionsA = yield* Queue.unbounded<void>()
      const connectionsB = yield* Queue.unbounded<void>()
      const longBackoff = { retryDelay: "20 seconds", maximumRetryDelay: "1 minute" } as const
      const a = yield* openReplica(yield* Layer.build(harness.layerReplica(clientA, connectionsA, longBackoff)))
      const b = yield* openReplica(yield* Layer.build(harness.layerReplica(clientB, connectionsB, longBackoff)))
      yield* Effect.all([
        awaitStatus(a.reactivity, a.space, (status) => status._tag === "Online"),
        awaitStatus(b.reactivity, b.space, (status) => status._tag === "Online")
      ], { concurrency: "unbounded", discard: true })
      yield* Queue.takeAll(connectionsA)
      yield* Queue.takeAll(connectionsB)

      yield* a.space.mutate(PutTodo, { id: "during-outage", title: "sent by A" })
      yield* Deferred.await(submitting)
      const offline = yield* awaitStatus(a.reactivity, a.space, (status) => status._tag === "Offline").pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* harness.stop(first)
      yield* Fiber.join(offline)

      yield* harness.start()
      yield* TestClock.adjust("500 millis")
      yield* LosslessQueue.take(connectionsA)
      yield* LosslessQueue.take(connectionsB)
      const resumed = yield* awaitStatus(
        a.reactivity,
        a.space,
        (status) => status._tag === "Online" && status.pending === 0
      )
      let resumedCursor = -1
      if (resumed._tag === "Online") resumedCursor = resumed.cursor

      const delivered = yield* awaitStatus(
        a.reactivity,
        a.space,
        (status) => status._tag === "Online" && status.cursor > resumedCursor
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* b.space.mutate(PutTodo, { id: "after-reconnect", title: "sent by B" })
      yield* Fiber.join(delivered)
      assert.deepStrictEqual(
        yield* a.space.get(Todo, "after-reconnect"),
        Option.some({ id: "after-reconnect", title: "sent by B" })
      )
    })
  )

  it.effect(
    "retries a background space's pending sync as soon as the transport reconnects",
    Effect.fnUntraced(function*() {
      const harness = yield* makeRestartHarness()
      const submissions = yield* Queue.unbounded<void>()
      const first = yield* harness.start({
        authorizeMutation: () => Queue.offer(submissions, undefined).pipe(Effect.andThen(Effect.never))
      })
      const connectionsA = yield* Queue.unbounded<void>()
      const connectionsB = yield* Queue.unbounded<void>()
      const longBackoff = { retryDelay: "20 seconds", maximumRetryDelay: "1 minute" } as const
      const a = yield* openReplica(yield* Layer.build(harness.layerReplica(clientA, connectionsA, longBackoff)))
      const b = yield* openReplica(yield* Layer.build(harness.layerReplica(clientB, connectionsB, longBackoff)))
      yield* Effect.all([
        awaitStatus(a.reactivity, a.space, (status) => status._tag === "Online"),
        awaitStatus(b.reactivity, b.space, (status) => status._tag === "Online")
      ], { concurrency: "unbounded", discard: true })
      yield* Queue.takeAll(connectionsA)
      yield* Queue.takeAll(connectionsB)

      yield* a.space.mutate(PutTodo, { id: "from-background", title: "sent by A in the background" })
      yield* LosslessQueue.take(submissions)
      yield* a.space.deactivate
      yield* LosslessQueue.take(submissions)
      yield* harness.stop(first)

      yield* harness.start()
      yield* TestClock.adjust("500 millis")
      yield* LosslessQueue.take(connectionsA)
      yield* LosslessQueue.take(connectionsB)
      const received = yield* b.reactivity.query(
        [`effect-local:space:${spaceId}:status`],
        b.space.get(Todo, "from-background")
      ).pipe(
        Effect.map(LosslessQueue.stream),
        Stream.unwrap,
        Stream.filter(Option.isSome),
        Stream.runHead
      )
      assert.deepStrictEqual(
        Option.flatten(received),
        Option.some({ id: "from-background", title: "sent by A in the background" })
      )
    })
  )

  it.effect(
    "rejoins ephemeral presence as soon as the transport reconnects instead of waiting out the rejoin backoff",
    Effect.fnUntraced(function*() {
      const harness = yield* makeRestartHarness()
      const first = yield* harness.start()
      const connectionsA = yield* Queue.unbounded<void>()
      const connectionsB = yield* Queue.unbounded<void>()
      const slowRejoin = Schedule.spaced("20 seconds")
      const ephemeralA = Context.get(
        yield* Layer.build(harness.layerClient(connectionsA, slowRejoin)),
        EphemeralClient.EphemeralClient
      )
      const ephemeralB = Context.get(
        yield* Layer.build(harness.layerClient(connectionsB, slowRejoin)),
        EphemeralClient.EphemeralClient
      )
      const sessionA = yield* ephemeralA.session(Presence, {
        spaceId,
        member: memberOf(clientA),
        value: { name: "A" },
        ttl: "1 minute"
      })
      yield* ephemeralB.session(Presence, { spaceId, member: memberOf(clientB), value: { name: "B" }, ttl: "1 minute" })
      const observations = yield* Queue.unbounded<RosterObservation>()
      yield* sessionA.members.pipe(
        Stream.runForEach((entries) =>
          Queue.offer(observations, {
            _tag: "Roster",
            clientIds: entries.map((entry) => entry.member.clientId).toSorted()
          })
        ),
        Effect.exit,
        Effect.flatMap((exit) => Queue.offer(observations, { _tag: "Ended", exit })),
        Effect.forkChild({ startImmediately: true })
      )
      yield* nextRoster(observations, [clientA, clientB])
      yield* Queue.takeAll(connectionsA)
      yield* Queue.takeAll(connectionsB)

      yield* harness.stop(first)
      yield* nextRoster(observations, [])

      yield* harness.start()
      yield* TestClock.adjust("250 millis")
      yield* LosslessQueue.take(connectionsA)
      yield* LosslessQueue.take(connectionsB)
      yield* nextRoster(observations, [clientA, clientB])
    })
  )
})
