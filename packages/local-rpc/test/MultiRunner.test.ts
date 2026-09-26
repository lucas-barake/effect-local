import { NodeClusterSocket, NodeCrypto, NodeHttpServer, NodeSocket, NodeSocketServer } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as TestClock from "effect/testing/TestClock"
import * as Tracer from "effect/Tracer"
import * as EntityId from "effect/unstable/cluster/EntityId"
import * as MessageStorage from "effect/unstable/cluster/MessageStorage"
import * as RunnerAddress from "effect/unstable/cluster/RunnerAddress"
import * as RunnerHealth from "effect/unstable/cluster/RunnerHealth"
import * as Runners from "effect/unstable/cluster/Runners"
import * as RunnerStorage from "effect/unstable/cluster/RunnerStorage"
import * as ShardId from "effect/unstable/cluster/ShardId"
import * as Sharding from "effect/unstable/cluster/Sharding"
import * as ShardingConfig from "effect/unstable/cluster/ShardingConfig"
import * as SocketRunner from "effect/unstable/cluster/SocketRunner"
import * as SqlMessageStorage from "effect/unstable/cluster/SqlMessageStorage"
import * as SqlRunnerStorage from "effect/unstable/cluster/SqlRunnerStorage"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServer from "effect/unstable/http/HttpServer"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as SocketServer from "effect/unstable/socket/SocketServer"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as Authentication from "../src/Authentication.js"
import * as EphemeralClient from "../src/EphemeralClient.js"
import * as SpaceEntity from "../src/SpaceEntity.js"
import * as SyncClient from "../src/SyncClient.js"
import * as SyncRpc from "../src/SyncRpc.js"
import * as SyncServer from "../src/SyncServer.js"
import { postgresDatabaseUrl } from "./fixtures/PostgresDatabase.js"

class TestAuthorizationError extends Schema.TaggedError<TestAuthorizationError, Schema.JsonObject>(
  "@lucas-barake/effect-local-rpc/test/MultiRunner/TestAuthorizationError"
)("TestAuthorizationError", { reason: Schema.String }) {}

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
const StatusProfile = Ephemeral.member({ status: Schema.String })

const replicationScope = Protocol.ReplicationScope.make({ models: [Todo.name] })
const scopeGeneration = Identity.ReplicationScopeGeneration.make(1)
const writerClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const readerClientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const memberViaA = Protocol.EphemeralMember.make({
  clientId: writerClientId,
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000001")
})
const memberViaB = Protocol.EphemeralMember.make({
  clientId: readerClientId,
  membershipIncarnation: Identity.MembershipIncarnation.make("inc_00000000-0000-4000-8000-000000000002")
})
const candidateSpaces = Array.from(
  { length: 16 },
  (_, index) => Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`)
)

const TestPrincipal = Schema.Struct({ subject: Schema.Literal("test") })
const authorizePrincipal = (principal: typeof Schema.Json.Type) => {
  if (Schema.is(TestPrincipal)(principal)) return Effect.void
  return Effect.fail(new TestAuthorizationError({ reason: "unknown principal" }))
}

const maintenanceInterval = "3 seconds"
const serverOptions = {
  definition,
  authorizeAccess: ({ principal }) => authorizePrincipal(principal),
  authorizeMutation: ({ principal }) => authorizePrincipal(principal),
  authorizeRead: ({ principal }) => authorizePrincipal(principal),
  authorizeEphemeral: () => Effect.void,
  store: { retainedHistoryEntries: 0, retainedReceipts: 0 },
  maintenance: { interval: maintenanceInterval }
} satisfies SyncServer.LayerOptions<typeof definition>

const sharedSecret = Redacted.make("multi-runner-shared-assertion-secret-0001")
const otherSecret = Redacted.make("multi-runner-other-assertion-secret-00002")
const bearer = Redacted.make("secret")

const layerAuthenticator = Layer.succeed(
  Authentication.Authenticator,
  Authentication.Authenticator.of({
    authenticate: (credential) => {
      if (Redacted.value(credential) === "secret") return Effect.succeed({ subject: "test" })
      return Effect.fail(new ReplicaError.CredentialRejected())
    }
  })
)
const layerAuthenticationServer = Authentication.layerServer.pipe(Layer.provide(layerAuthenticator))
const layerWebsocketProtocol = SyncServer.layerProtocolWebSocket({ path: "/sync" }).pipe(
  Layer.provide(HttpRouter.layer)
)
const layerGateway = (assertionSecret: Redacted.Redacted) =>
  SyncServer.layer({ ...serverOptions, assertionSecret }).pipe(
    Layer.provide(layerWebsocketProtocol),
    Layer.provide(layerAuthenticationServer),
    Layer.provide(layerHandlers),
    Layer.provide(HttpRouter.serve(layerWebsocketProtocol, { disableListenLog: true, disableLogger: true })),
    Layer.provide(SyncRpc.layerJson())
  )

const layerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
).pipe(Layer.provide(Reactivity.layer))

const loopback = "::1"
const shardsPerGroup = 4
const refreshAssignmentsInterval = "1 second"
const shardAcquisitionRetryInterval = "1 second"
const maintenanceSingleton = "@lucas-barake/effect-local-sql/ServerStore/maintenance"

type RunnerName = "A" | "B"

interface ShardView {
  readonly requested: ReadonlySet<string>
  readonly held: ReadonlySet<string>
  readonly active: ReadonlySet<string>
}

interface EndedSpan {
  readonly runner: RunnerName
  readonly name: string
  readonly spaceId: unknown
}

interface Runner {
  readonly name: RunnerName
  readonly sharding: Sharding.Sharding["Service"]
  readonly shards: SubscriptionRef.SubscriptionRef<ShardView>
  readonly url: string
}

interface Cluster {
  readonly a: Runner
  readonly b: Runner
  readonly spans: Queue.Queue<EndedSpan>
}

const makeTracer = (runner: RunnerName, spans: Queue.Queue<EndedSpan>) =>
  Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options)
      const end = span.end.bind(span)
      span.end = (...args) => {
        end(...args)
        const spaceId = span.attributes.get("space.id") ?? span.attributes.get("entity.id")
        if (spaceId !== undefined) Queue.offerUnsafe(spans, { runner, name: span.name, spaceId })
      }
      return span
    }
  })

const layerObservedRunnerStorage = (shards: SubscriptionRef.SubscriptionRef<ShardView>) =>
  Layer.effect(
    RunnerStorage.RunnerStorage,
    Effect.gen(function*() {
      const storage = yield* RunnerStorage.RunnerStorage
      return RunnerStorage.RunnerStorage.of({
        ...storage,
        acquire: (address, shardIds) => {
          const requested = new Set(Array.from(shardIds, ShardId.toString))
          return storage.acquire(address, shardIds).pipe(
            Effect.tap((acquired) =>
              SubscriptionRef.update(shards, (view) => ({
                ...view,
                requested,
                held: new Set([...view.held, ...acquired.map(ShardId.toString)])
              }))
            )
          )
        },
        release: (address, shardId) =>
          storage.release(address, shardId).pipe(
            Effect.tap(() =>
              SubscriptionRef.update(shards, (view) => ({
                ...view,
                held: new Set([...view.held].filter((held) => held !== ShardId.toString(shardId)))
              }))
            )
          )
      })
    })
  ).pipe(Layer.provide(SqlRunnerStorage.layer))

const layerObservedMessageStorage = (shards: SubscriptionRef.SubscriptionRef<ShardView>) =>
  Layer.effect(
    MessageStorage.MessageStorage,
    Effect.gen(function*() {
      const storage = yield* MessageStorage.MessageStorage
      return MessageStorage.MessageStorage.of({
        ...storage,
        unprocessedMessages: (shardIds, options) => {
          const active = new Set(Array.from(shardIds, ShardId.toString))
          return SubscriptionRef.update(shards, (view) => ({ ...view, active })).pipe(
            Effect.andThen(storage.unprocessedMessages(shardIds, options))
          )
        }
      })
    })
  ).pipe(Layer.provide(SqlMessageStorage.layer))

const layerClusterRunner = (options: {
  readonly address: RunnerAddress.RunnerAddress
  readonly socketServer: SocketServer.SocketServer["Service"]
  readonly shards: SubscriptionRef.SubscriptionRef<ShardView>
  readonly layerSerialization: Layer.Layer<RpcSerialization.RpcSerialization>
}) =>
  SocketRunner.layer.pipe(
    Layer.provide(
      RunnerHealth.layerPing.pipe(
        Layer.provide(Runners.layerRpc),
        Layer.provide(NodeClusterSocket.layerClientProtocol)
      )
    ),
    Layer.provide(Layer.succeed(SocketServer.SocketServer, options.socketServer)),
    Layer.provide(NodeClusterSocket.layerClientProtocol),
    Layer.provide([layerObservedRunnerStorage(options.shards), layerObservedMessageStorage(options.shards)]),
    Layer.provide(ShardingConfig.layer({
      runnerAddress: Option.some(options.address),
      shardsPerGroup,
      refreshAssignmentsInterval,
      entityTerminationTimeout: 0
    })),
    Layer.provide(options.layerSerialization)
  )

type Database = Context.Context<SqlClient.SqlClient | Crypto.Crypto>

const startRunner = Effect.fnUntraced(function*(options: {
  readonly name: RunnerName
  readonly clusterDatabase: Database
  readonly serverDatabase: Database
  readonly assertionSecret: Redacted.Redacted
  readonly spans: Queue.Queue<EndedSpan>
  readonly layerSerialization: Layer.Layer<RpcSerialization.RpcSerialization>
  readonly scope: Scope.Scope
}) {
  const socketServer = yield* NodeSocketServer.make({ host: loopback, port: 0 }).pipe(Scope.provide(options.scope))
  if (socketServer.address._tag === "UnixPathAddress") return yield* Effect.die("Expected a TCP runner socket")
  const shards = yield* SubscriptionRef.make<ShardView>({ requested: new Set(), held: new Set(), active: new Set() })
  const layerTracer = Layer.succeed(Tracer.Tracer, makeTracer(options.name, options.spans))
  const context = yield* Layer.buildWithScope(
    layerGateway(options.assertionSecret).pipe(
      Layer.provideMerge(
        layerClusterRunner({
          address: RunnerAddress.make(loopback, socketServer.address.port),
          socketServer,
          shards,
          layerSerialization: options.layerSerialization
        }).pipe(Layer.provide(Layer.succeedContext(options.clusterDatabase)))
      ),
      Layer.provideMerge(NodeHttpServer.layerTest),
      Layer.provide(Layer.succeedContext(options.serverDatabase)),
      Layer.provide(layerTracer)
    ),
    options.scope
  )
  const gateway = Context.get(context, HttpServer.HttpServer).address
  if (gateway._tag === "UnixPathAddress") return yield* Effect.die("Expected a TCP gateway")
  return {
    name: options.name,
    sharding: Context.get(context, Sharding.Sharding),
    shards,
    url: `http://[${loopback}]:${gateway.port}/sync`
  } satisfies Runner
})

const awaitShards = (runner: Runner, predicate: (view: ShardView) => boolean) =>
  SubscriptionRef.changes(runner.shards).pipe(
    Stream.filter(predicate),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

const allShards = Array.from({ length: shardsPerGroup }, (_, index) => ShardId.make("default", index + 1))

const makeCluster = Effect.fnUntraced(function*(options: {
  readonly secrets: { readonly a: Redacted.Redacted; readonly b: Redacted.Redacted }
  readonly layerSerialization: Layer.Layer<RpcSerialization.RpcSerialization>
  readonly serverDatabases?: { readonly a: Database; readonly b: Database } | undefined
}) {
  const database = yield* Layer.build(layerDatabase)
  const serverDatabases = options.serverDatabases ?? { a: database, b: database }
  const spans = yield* Queue.unbounded<EndedSpan>()
  const { secrets, layerSerialization } = options
  const parentScope = yield* Effect.scope
  const closesLast = yield* Scope.fork(parentScope)
  const closesFirst = yield* Scope.fork(parentScope)
  const a = yield* startRunner({
    name: "A",
    clusterDatabase: database,
    serverDatabase: serverDatabases.a,
    assertionSecret: secrets.a,
    spans,
    layerSerialization,
    scope: closesFirst
  })
  yield* awaitShards(a, (view) => view.active.size === shardsPerGroup)

  const b = yield* startRunner({
    name: "B",
    clusterDatabase: database,
    serverDatabase: serverDatabases.b,
    assertionSecret: secrets.b,
    spans,
    layerSerialization,
    scope: closesLast
  })
  const handedOver = [...(yield* awaitShards(b, (view) => view.requested.size > 0)).requested]
  yield* TestClock.adjust(refreshAssignmentsInterval)
  yield* awaitShards(a, (view) => handedOver.every((shard) => !view.held.has(shard)))
  yield* TestClock.adjust(shardAcquisitionRetryInterval)
  yield* awaitShards(b, (view) => handedOver.every((shard) => view.active.has(shard)))

  assert.deepStrictEqual(
    allShards.map((shard) => [a.sharding.hasShardId(shard), b.sharding.hasShardId(shard)].filter(Boolean).length),
    allShards.map(() => 1)
  )
  assert.strictEqual(allShards.filter((shard) => b.sharding.hasShardId(shard)).length, shardsPerGroup / 2)
  return { a, b, spans } satisfies Cluster
})

const shardOf = (runner: Runner, id: string) =>
  SpaceEntity.Space.getShardId(EntityId.make(id)).pipe(Effect.provideService(Sharding.Sharding, runner.sharding))

const spaceOwnedBy = Effect.fnUntraced(function*(owner: Runner) {
  for (const spaceId of candidateSpaces) {
    if (owner.sharding.hasShardId(yield* shardOf(owner, spaceId))) return spaceId
  }
  return assert.fail(`no candidate space hashes to a shard owned by runner ${owner.name}`)
})

const connect = Effect.fnUntraced(function*(runner: Runner) {
  const context = yield* Layer.build(
    SyncClient.layerWebSocket({ url: runner.url }).pipe(
      Layer.provide(NodeSocket.layerWebSocketConstructor),
      Layer.provide(Authentication.layerCredentialProviderStatic(bearer))
    )
  )
  return {
    sync: Context.get(context, SyncEngine.SyncEngine),
    ephemeral: Context.get(context, EphemeralClient.EphemeralClient)
  }
})

const putTodo = Effect.fnUntraced(function*(spaceId: Identity.SpaceId, localSequence: number, title: string) {
  const identity = {
    spaceId,
    clientId: writerClientId,
    mutationId: Identity.MutationId.make(`mut_00000000-0000-4000-8000-${String(localSequence).padStart(12, "0")}`),
    localSequence: Identity.LocalSequence.make(localSequence),
    basis: Identity.ServerSequence.make(0),
    name: PutTodo.name,
    payload: { id: `todo-${localSequence}`, title },
    digestVersion: 3 as const,
    membershipIncarnation: Identity.legacyMembershipIncarnation,
    sourceSchema: definition.schemaIdentity,
    mutationVersion: PutTodo.version
  }
  return {
    envelope: Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) }),
    schema: definition.schemaIdentity
  } satisfies Protocol.SubmitRequest
})

const submitOne = (engine: SyncEngine.Service, request: Protocol.SubmitRequest) =>
  engine.submitBatch({ envelopes: [request.envelope], schema: request.schema }).pipe(
    Effect.map(({ receipts }) => receipts[0])
  )

const pullRequest = (spaceId: Identity.SpaceId) =>
  Protocol.PullRequest.make({
    spaceId,
    clientId: readerClientId,
    schema: definition.schemaIdentity,
    scope: replicationScope,
    scopeGeneration,
    cursor: null,
    limit: 10
  })

const readTodos = Effect.fnUntraced(function*(remote: SyncEngine.Service, spaceId: Identity.SpaceId) {
  const pulled = yield* remote.pull(pullRequest(spaceId))
  if (!("_tag" in pulled)) return pulled.changes
  const page = yield* remote.bootstrap({
    spaceId,
    clientId: readerClientId,
    schema: definition.schemaIdentity,
    scope: replicationScope,
    scopeGeneration,
    cursor: pulled.manifest.cursor,
    snapshotId: pulled.manifest.snapshotId,
    afterOrdinal: -1,
    limit: 10
  })
  return page.entries.map((entry) => entry.change)
})

const rosterOfSize = (session: EphemeralClient.Session<typeof StatusProfile>, size: number) =>
  session.members.pipe(
    Stream.filter((entries) => entries.length === size),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed })),
    Effect.map((entries) => entries.map((entry) => entry.value.status).toSorted())
  )

const endedSpans = (cluster: Cluster, name: string, spaceId: Identity.SpaceId) =>
  Queue.clear(cluster.spans).pipe(
    Effect.map((spans) => spans.filter((span) => span.name === name && span.spaceId === spaceId))
  )

const nextEndedSpan = (cluster: Cluster, name: string, spaceId: Identity.SpaceId) =>
  Stream.fromQueue(cluster.spans).pipe(
    Stream.filter((span) => span.name === name && span.spaceId === spaceId),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

const sharedSecrets = { a: sharedSecret, b: sharedSecret }

type ServerDatabases = Effect.Effect<
  { readonly a: Database; readonly b: Database } | undefined,
  SqlError.SqlError,
  Scope.Scope
>

const sharedServerDatabase: ServerDatabases = Effect.succeed(undefined)

const postgresServerDatabases: ServerDatabases = Effect.gen(function*() {
  const { url } = yield* postgresDatabaseUrl
  const layerServerDatabase = Layer.mergeAll(PgClient.layer({ url, maxConnections: 4 }), NodeCrypto.layer).pipe(
    Layer.provide(Reactivity.layer)
  )
  return { a: yield* Layer.build(layerServerDatabase), b: yield* Layer.build(layerServerDatabase) }
})

const admitsOnOwner = (
  layerSerialization: Layer.Layer<RpcSerialization.RpcSerialization>,
  serverDatabases: ServerDatabases = sharedServerDatabase
) =>
  Effect.fnUntraced(function*() {
    const cluster = yield* makeCluster({
      secrets: sharedSecrets,
      layerSerialization,
      serverDatabases: yield* serverDatabases
    })
    const spaceId = yield* spaceOwnedBy(cluster.b)
    const viaA = yield* connect(cluster.a)
    const viaB = yield* connect(cluster.b)

    const receipt = yield* submitOne(viaA.sync, yield* putTodo(spaceId, 1, "crosses runners"))

    assert.strictEqual(receipt._tag, "Accepted")
    assert.deepStrictEqual((yield* endedSpans(cluster, "ServerStore.submit", spaceId)).map((span) => span.runner), [
      "B"
    ])
    assert.deepStrictEqual(yield* readTodos(viaB.sync, spaceId), [
      Protocol.Upsert.make({
        entity: Protocol.EntityKey.make({ model: Todo.name, modelVersion: Todo.version, key: "todo-1" }),
        value: { id: "todo-1", title: "crosses runners" }
      })
    ])
  }, Effect.provide(NodeCrypto.layer))

const wakesAcrossRunners = (
  layerSerialization: Layer.Layer<RpcSerialization.RpcSerialization>,
  serverDatabases: ServerDatabases = sharedServerDatabase
) =>
  Effect.fnUntraced(function*() {
    const cluster = yield* makeCluster({
      secrets: sharedSecrets,
      layerSerialization,
      serverDatabases: yield* serverDatabases
    })
    const spaceId = yield* spaceOwnedBy(cluster.b)
    const viaA = yield* connect(cluster.a)
    const viaB = yield* connect(cluster.b)
    const wakes = yield* Queue.unbounded<Protocol.Wake>()
    const watching = yield* viaA.sync.watch({
      spaceId,
      clientId: readerClientId,
      schema: definition.schemaIdentity,
      scope: replicationScope,
      scopeGeneration,
      cursor: null
    }).pipe(
      Stream.runForEach((wake) => Queue.offer(wakes, wake)),
      Effect.forkChild({ startImmediately: true })
    )
    yield* Queue.take(wakes)

    const receipt = yield* submitOne(viaB.sync, yield* putTodo(spaceId, 1, "written through B"))

    assert.strictEqual(receipt._tag, "Accepted")
    assert.deepStrictEqual(yield* Queue.take(wakes), { spaceId })
    yield* Fiber.interrupt(watching)
    const served = yield* nextEndedSpan(cluster, `${SpaceEntity.Space.type}(${spaceId}).Watch`, spaceId)
    assert.strictEqual(served.runner, "B")
  }, Effect.provide(NodeCrypto.layer))

const sharesPresenceAcrossRunners = (layerSerialization: Layer.Layer<RpcSerialization.RpcSerialization>) =>
  Effect.fnUntraced(function*() {
    const cluster = yield* makeCluster({ secrets: sharedSecrets, layerSerialization })
    const spaceId = yield* spaceOwnedBy(cluster.b)
    const viaA = yield* connect(cluster.a)
    const viaB = yield* connect(cluster.b)

    const joinedViaB = yield* viaB.ephemeral.session(StatusProfile, {
      spaceId,
      member: memberViaB,
      value: { status: "joined through B" },
      ttl: "30 seconds"
    })

    yield* Effect.scoped(Effect.gen(function*() {
      const joinedViaA = yield* viaA.ephemeral.session(StatusProfile, {
        spaceId,
        member: memberViaA,
        value: { status: "joined through A" },
        ttl: "30 seconds"
      })
      assert.deepStrictEqual(yield* rosterOfSize(joinedViaB, 2), ["joined through A", "joined through B"])
      assert.deepStrictEqual(yield* rosterOfSize(joinedViaA, 2), ["joined through A", "joined through B"])
    }))

    assert.deepStrictEqual(yield* rosterOfSize(joinedViaB, 1), ["joined through B"])
  }, Effect.provide(NodeCrypto.layer))

const deniesMismatchedSecrets = (layerSerialization: Layer.Layer<RpcSerialization.RpcSerialization>) =>
  Effect.fnUntraced(function*() {
    const cluster = yield* makeCluster({ secrets: { a: sharedSecret, b: otherSecret }, layerSerialization })
    const localSpace = yield* spaceOwnedBy(cluster.a)
    const remoteSpace = yield* spaceOwnedBy(cluster.b)
    const viaA = yield* connect(cluster.a)
    const viaB = yield* connect(cluster.b)

    const local = yield* submitOne(viaA.sync, yield* putTodo(localSpace, 1, "same runner"))
    const denied = yield* submitOne(viaA.sync, yield* putTodo(remoteSpace, 2, "forged across runners")).pipe(
      Effect.flip
    )

    assert.strictEqual(local._tag, "Accepted")
    assert.strictEqual(denied._tag, "AuthorizationDenied")
    if (denied._tag === "AuthorizationDenied") assert.strictEqual(denied.reason, "invalid principal assertion")
    assert.deepStrictEqual(yield* endedSpans(cluster, "ServerStore.submit", remoteSpace), [])
    assert.deepStrictEqual(yield* readTodos(viaB.sync, remoteSpace), [])
  }, Effect.provide(NodeCrypto.layer))

const maintainsOnSingletonOwner = (layerSerialization: Layer.Layer<RpcSerialization.RpcSerialization>) =>
  Effect.fnUntraced(function*() {
    const cluster = yield* makeCluster({ secrets: sharedSecrets, layerSerialization })
    const spaceId = yield* spaceOwnedBy(cluster.b)
    yield* Effect.scoped(Effect.gen(function*() {
      const viaA = yield* connect(cluster.a)
      assert.strictEqual((yield* submitOne(viaA.sync, yield* putTodo(spaceId, 1, "history to prune")))._tag, "Accepted")
    }))
    yield* Queue.clear(cluster.spans)
    const singletonShard = cluster.a.sharding.getShardId(EntityId.make(maintenanceSingleton), "default")
    const owners = [cluster.a, cluster.b]
      .filter((runner) => runner.sharding.hasShardId(singletonShard))
      .map((runner) => runner.name)

    yield* TestClock.adjust(maintenanceInterval)
    const first = yield* nextEndedSpan(cluster, "ServerStore.maintain", spaceId)
    yield* TestClock.adjust(maintenanceInterval)
    const second = yield* nextEndedSpan(cluster, "ServerStore.maintain", spaceId)

    assert.deepStrictEqual(owners, [first.runner])
    assert.strictEqual(second.runner, first.runner)
    assert.deepStrictEqual(yield* endedSpans(cluster, "ServerStore.maintain", spaceId), [])
  }, Effect.provide(NodeCrypto.layer))

describe("multi-runner cluster over NDJSON runner transport", () => {
  const layerSerialization = RpcSerialization.layerNdjson
  it.effect(
    "admits a mutation submitted through one runner on the runner that owns the space",
    admitsOnOwner(layerSerialization)
  )
  it.effect(
    "wakes a watch opened through one runner when another runner's gateway writes the space",
    wakesAcrossRunners(layerSerialization)
  )
  it.effect(
    "shows presence joined through one runner to a member joined through the other",
    sharesPresenceAcrossRunners(layerSerialization)
  )
  it.effect(
    "denies a cross-runner request when the runners sign assertions with different secrets",
    deniesMismatchedSecrets(layerSerialization)
  )
  it.effect(
    "runs the maintenance singleton on exactly the runner that owns its shard",
    maintainsOnSingletonOwner(layerSerialization)
  )
})

describe("multi-runner cluster over SchemaBinary runner transport", () => {
  const layerSerialization = RpcSerialization.layerSchemaBinary()
  it.effect(
    "admits a mutation submitted through one runner on the runner that owns the space",
    admitsOnOwner(layerSerialization)
  )
  it.effect(
    "denies a cross-runner request when the runners sign assertions with different secrets",
    deniesMismatchedSecrets(layerSerialization)
  )
  it.effect(
    "runs the maintenance singleton on exactly the runner that owns its shard",
    maintainsOnSingletonOwner(layerSerialization)
  )
})

const provideNodeCrypto = Effect.provide(NodeCrypto.layer)

const todoKey = (change: Protocol.ViewChange) => {
  if (typeof change.entity.key !== "string") return assert.fail("expected a string todo key")
  return change.entity.key
}

describe("multi-runner cluster with postgres server storage", () => {
  const layerSerialization = RpcSerialization.layerNdjson
  it.effect(
    "admits a mutation through one runner into the shared database on the owning runner",
    admitsOnOwner(layerSerialization, postgresServerDatabases)
  )
  it.effect(
    "wakes a watch through one runner when another runner writes the shared database",
    wakesAcrossRunners(layerSerialization, postgresServerDatabases)
  )
  it.effect(
    "keeps server sequences dense when both gateways write spaces owned by different runners",
    Effect.fnUntraced(function*() {
      const cluster = yield* makeCluster({
        secrets: sharedSecrets,
        layerSerialization,
        serverDatabases: yield* postgresServerDatabases
      })
      const viaA = yield* connect(cluster.a)
      const viaB = yield* connect(cluster.b)
      for (const spaceId of [yield* spaceOwnedBy(cluster.a), yield* spaceOwnedBy(cluster.b)]) {
        const sequences: Array<number> = []
        for (let localSequence = 1; localSequence <= 4; localSequence++) {
          let gateway = viaA
          if (localSequence % 2 === 0) gateway = viaB
          const receipt = yield* submitOne(
            gateway.sync,
            yield* putTodo(spaceId, localSequence, `write ${localSequence}`)
          )
          if (receipt._tag !== "Accepted") assert.fail(`expected an accepted receipt, got ${receipt._tag}`)
          sequences.push(receipt.serverSequence)
        }
        assert.deepStrictEqual(sequences, [1, 2, 3, 4])
        const throughA = (yield* readTodos(viaA.sync, spaceId)).map(todoKey).toSorted()
        const throughB = (yield* readTodos(viaB.sync, spaceId)).map(todoKey).toSorted()
        assert.deepStrictEqual(throughA, ["todo-1", "todo-2", "todo-3", "todo-4"])
        assert.deepStrictEqual(throughB, throughA)
      }
    }, provideNodeCrypto)
  )
})
