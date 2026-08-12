import { NodeCrypto, NodeHttpServer, NodeSocket } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as MutableRef from "effect/MutableRef"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as TestClock from "effect/testing/TestClock"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServer from "effect/unstable/http/HttpServer"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as Socket from "effect/unstable/socket/Socket"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as Authentication from "../src/Authentication.js"
import * as PresenceClient from "../src/PresenceClient.js"
import * as PresenceHub from "../src/PresenceHub.js"
import * as PrincipalAssertion from "../src/PrincipalAssertion.js"
import * as ProtocolSession from "../src/ProtocolSession.js"
import * as SpaceEntity from "../src/SpaceEntity.js"
import * as SyncClient from "../src/SyncClient.js"
import * as SyncRpc from "../src/SyncRpc.js"
import * as SyncServer from "../src/SyncServer.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const secondSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const forbiddenSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000003")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")

const Todo = Model.make("Todo", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { version: 1, payload: Todo.schema, success: Todo.schema })
const ReturnHugeResult = Mutation.make("ReturnHugeResult", { version: 1, success: Schema.String })
const AssignRoleV1 = Mutation.make("AssignRole", {
  version: 1,
  payload: Schema.Struct({ account: Schema.String }),
  success: Schema.String
})
const AssignRoleV2 = Mutation.make("AssignRole", {
  version: 2,
  payload: Schema.Struct({ account: Schema.String, role: Schema.Literals(["member", "admin"]) }),
  success: Schema.String
})
const definitionV1 = Definition.make({
  version: 1,
  models: [Todo],
  mutations: [PutTodo, ReturnHugeResult, AssignRoleV1]
})
const definition = Definition.make({
  version: 2,
  models: [Todo],
  mutations: [PutTodo, ReturnHugeResult, AssignRoleV2]
})
const scope = Protocol.ReplicationScope.make({ models: [Todo.name] })
const scopeGeneration = Identity.ReplicationScopeGeneration.make(1)
const pullRequest = (requestedSpaceId = spaceId): Protocol.PullRequest =>
  Protocol.PullRequest.make({
    spaceId: requestedSpaceId,
    clientId,
    schema: definition.schemaIdentity,
    scope,
    scopeGeneration,
    cursor: null,
    limit: 10
  })
const evolution = Evolution.make({
  current: definition,
  steps: [Evolution.step({
    id: "definition/1-to-2",
    from: definitionV1,
    to: definition,
    mutations: [Evolution.mutation({
      id: "assign-role/1-to-2",
      from: AssignRoleV1,
      to: AssignRoleV2,
      payload: (payload) => ({ ...payload, role: "admin" }) satisfies typeof AssignRoleV2.payloadSchema.Type
    })]
  })]
})
const handlers = Layer.mergeAll(
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))),
  ReturnHugeResult.toLayer(() => Effect.succeed("x".repeat(SyncRpc.maximumFrameBytes))),
  AssignRoleV2.toLayer(({ payload }) => Effect.succeed(payload.role))
)
const runtime = MutationRuntime.layer(definition, evolution).pipe(Layer.provide(handlers))
const readAuthorized = MutableRef.make(true)
const database = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)

const migration = {
  retryDelay: "1 millis",
  maximumAttempts: 8
} satisfies { readonly retryDelay: Duration.Input; readonly maximumAttempts: number }
const serverHistory = {
  readAuthorizationRefreshInterval: "1 second" as const,
  maximumWatchersPerSpace: 1_024,
  maximumConcurrentReadAuthorizations: 64,
  readAuthorizationCacheCapacity: 4_096,
  retainedHistoryEntries: 0,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 0,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 2_048,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration
}
const entityOptions = {
  admissionMailboxCapacity: 64,
  readMailboxCapacity: 64,
  watchMailboxCapacity: 64,
  presencePublicationMailboxCapacity: 64,
  maximumConcurrentBootstrapPagesPerSpace: 4,
  maximumConcurrentPresencePublicationsPerSpace: 16
} satisfies SpaceEntity.HandlerOptions
const clientHistory = {
  scope,
  retainedReceipts: 256,
  settlementCapacity: 64,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration
}

const store = ServerStore.layer({
  ...serverHistory,
  definition,
  evolution,
  authorizeAccess: ({ principal, spaceId: requestedSpaceId }) => {
    if (
      principal !== null && typeof principal === "object" && !Array.isArray(principal) &&
      "subject" in principal && principal.subject === "test" &&
      (requestedSpaceId === spaceId || requestedSpaceId === secondSpaceId)
    ) {
      return Effect.void
    }
    return Effect.fail({ reason: "forbidden" })
  },
  authorizeMutation: ({ mutation }) => {
    if (Schema.is(AssignRoleV2.payloadSchema)(mutation.payload) && mutation.payload.role === "admin") {
      return Effect.fail({ reason: "admin role requires elevated access" })
    }
    return Effect.void
  },
  authorizeRead: ({ principal, spaceId: requestedSpaceId }) => {
    if (
      MutableRef.get(readAuthorized) && principal !== null && typeof principal === "object" &&
      !Array.isArray(principal) &&
      "subject" in principal && principal.subject === "test" &&
      (requestedSpaceId === spaceId || requestedSpaceId === secondSpaceId)
    ) {
      return Effect.void
    }
    return Effect.fail({ reason: "forbidden" })
  }
}).pipe(Layer.provide(runtime), Layer.provide(database))

const authenticator = Layer.succeed(
  Authentication.Authenticator,
  Authentication.Authenticator.of({
    authenticate: (credential) => {
      if (Redacted.value(credential) === "secret") return Effect.succeed({ subject: "test" })
      if (Redacted.value(credential) === "revoked") return Effect.succeed({ subject: "revoked" })
      return Effect.fail(new ReplicaError.CredentialRejected())
    }
  })
)
const authenticationServer = Authentication.layerServer.pipe(Layer.provide(authenticator))
const assertionCodec = Schema.fromJsonString(Schema.Json)
const assertionIssuer = PrincipalAssertion.layerIssuer((principal) =>
  Schema.encodeUnknownEffect(assertionCodec)(principal).pipe(
    Effect.map((assertion) => PrincipalAssertion.PrincipalAssertion.make(assertion)),
    Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "could not issue principal assertion" }))
  )
)
const assertionVerifier = PrincipalAssertion.layerVerifier((assertion) =>
  Schema.decodeUnknownEffect(assertionCodec)(assertion).pipe(
    Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "invalid principal assertion" }))
  )
)
const authenticationClient = Layer.fresh(Authentication.layerClient).pipe(Layer.provide(
  Layer.succeed(
    Authentication.CredentialProvider,
    Authentication.CredentialProvider.of({
      acquire: Effect.succeed({ generation: 0, bearer: Redacted.make("secret") }),
      awaitChange: () => Effect.never
    })
  )
))
const revokedAuthenticationClient = Layer.fresh(Authentication.layerClient).pipe(Layer.provide(
  Layer.succeed(
    Authentication.CredentialProvider,
    Authentication.CredentialProvider.of({
      acquire: Effect.succeed({ generation: 0, bearer: Redacted.make("revoked") }),
      awaitChange: () => Effect.never
    })
  )
))
const presenceHub = PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })
const cluster = SpaceEntity.layer(entityOptions).pipe(
  Layer.provide(assertionVerifier),
  Layer.provide(store),
  Layer.provide(presenceHub),
  Layer.provide(SingleRunner.layer({ runnerStorage: "memory" }).pipe(Layer.provide(database)))
)

const websocketProtocol = SyncServer.layerProtocolWebSocket({ path: "/sync" }).pipe(
  Layer.provide(HttpRouter.layer)
)
const websocketServer = SyncServer.layer.pipe(
  Layer.provideMerge(websocketProtocol),
  Layer.provide(cluster),
  Layer.provide(authenticationServer),
  Layer.provide(assertionIssuer),
  Layer.provide(HttpRouter.serve(websocketProtocol, { disableListenLog: true, disableLogger: true }))
)
const webSocketConstructions = MutableRef.make(0)
const liveWebSockets = MutableRef.make(0)
const countedConstructor = Layer.effect(
  Socket.WebSocketConstructor,
  Socket.WebSocketConstructor.pipe(Effect.map((makeWebSocket) => (url: string, protocols?: string | Array<string>) => {
    MutableRef.update(webSocketConstructions, (count) => count + 1)
    MutableRef.update(liveWebSockets, (count) => count + 1)
    const webSocket = makeWebSocket(url, protocols)
    webSocket.addEventListener("close", () => {
      MutableRef.update(liveWebSockets, (count) => count - 1)
    }, { once: true })
    return webSocket
  }))
).pipe(Layer.provide(NodeSocket.layerWebSocketConstructor))
const socket = Effect.gen(function*() {
  const server = yield* HttpServer.HttpServer
  const address = server.address
  if (address._tag === "UnixAddress") return yield* Effect.die("Expected the test HTTP server to use a TCP address")
  return yield* Socket.makeWebSocket(`http://127.0.0.1:${address.port}/sync`)
}).pipe(Layer.effect(Socket.Socket), Layer.provide(countedConstructor))
const clientProtocol = SyncClient.layerProtocolSocket().pipe(Layer.provide(socket))
const client = Layer.merge(SyncClient.layer, PresenceClient.layer).pipe(
  Layer.provide(clientProtocol),
  Layer.provide(authenticationClient)
)
class RevokedSyncEngine extends Context.Service<RevokedSyncEngine, SyncEngine.Service>()(
  "@lucas-barake/effect-local-rpc/test/RevokedSyncEngine"
) {}
const revokedClient = Layer.effect(RevokedSyncEngine, SyncEngine.SyncEngine).pipe(
  Layer.provide(Layer.fresh(SyncClient.layer)),
  Layer.provide(Layer.fresh(clientProtocol)),
  Layer.provide(revokedAuthenticationClient)
)
const live = Layer.merge(client, revokedClient).pipe(
  Layer.provideMerge(websocketServer),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)
const singleClientLive = client.pipe(
  Layer.provideMerge(websocketServer),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)

const incompatibleServer = SyncServer.layerWithOptions({ supportedProtocolVersions: [2] }).pipe(
  Layer.provideMerge(websocketProtocol),
  Layer.provide(cluster),
  Layer.provide(authenticationServer),
  Layer.provide(assertionIssuer),
  Layer.provide(HttpRouter.serve(websocketProtocol, { disableListenLog: true, disableLogger: true }))
)
const incompatibleClient = SyncClient.layerWithOptions({ supportedProtocolVersions: [1] }).pipe(
  Layer.provide(clientProtocol),
  Layer.provide(authenticationClient)
)
const incompatibleLive = incompatibleClient.pipe(
  Layer.provideMerge(incompatibleServer),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)
const invalidProtocolClient = SyncClient.layerWithOptions({ supportedProtocolVersions: [] }).pipe(
  Layer.provide(clientProtocol),
  Layer.provide(authenticationClient)
)
const invalidProtocolLive = invalidProtocolClient.pipe(
  Layer.provideMerge(websocketServer),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)

type ProtocolObservation =
  | { readonly _tag: "Negotiate" }
  | { readonly _tag: "Pull"; readonly version: Protocol.ProtocolVersion }
  | { readonly _tag: "PublishPresence"; readonly version: Protocol.ProtocolVersion }

const protocolObservations = MutableRef.make<Array<ProtocolObservation>>([])
const observingAuthenticationServer = Layer.effect(
  Authentication.Authentication,
  Authentication.Authentication.pipe(
    Effect.map((authenticate) =>
      Authentication.Authentication.of((effect, options) =>
        Effect.sync(() => {
          const payload = options.payload
          if (options.rpc._tag === "Negotiate") {
            MutableRef.update(protocolObservations, (observations) => [
              ...observations,
              { _tag: "Negotiate" as const }
            ])
          } else if (options.rpc._tag === "Pull" && Schema.is(Protocol.VersionedPullRequest)(payload)) {
            MutableRef.update(protocolObservations, (observations) => [
              ...observations,
              { _tag: "Pull" as const, version: payload.protocolVersion }
            ])
          } else if (
            options.rpc._tag === "PublishPresence" && Schema.is(Protocol.VersionedPresenceUpdate)(payload)
          ) {
            MutableRef.update(protocolObservations, (observations) => [
              ...observations,
              { _tag: "PublishPresence" as const, version: payload.protocolVersion }
            ])
          }
        }).pipe(Effect.andThen(authenticate(effect, options)))
      )
    )
  )
).pipe(Layer.provide(authenticationServer))
const protocol2Server = SyncServer.layerWithOptions({ supportedProtocolVersions: [1, 2] }).pipe(
  Layer.provideMerge(websocketProtocol),
  Layer.provide(cluster),
  Layer.provide(observingAuthenticationServer),
  Layer.provide(assertionIssuer),
  Layer.provide(HttpRouter.serve(websocketProtocol, { disableListenLog: true, disableLogger: true }))
)
const configurableProtocolSession = ProtocolSession.layerWithOptions({ supportedProtocolVersions: [1, 2] })
const configurableClient = Layer.merge(SyncClient.layerFromSession(), PresenceClient.layerFromSession()).pipe(
  Layer.provide(configurableProtocolSession),
  Layer.provide(clientProtocol),
  Layer.provide(authenticationClient)
)
const configurableLive = configurableClient.pipe(
  Layer.provideMerge(protocol2Server),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)

type AuthenticatorMode = "Available" | "Rejected" | "Unavailable"

const awaitStatus = (
  reactivity: Reactivity.Reactivity["Service"],
  space: Replica.Space,
  tag: "Online" | "Offline" | "NeedsAuthentication"
) =>
  reactivity.stream([`effect-local:space:${space.spaceId}:status`], space.status).pipe(
    Stream.filter((status) => status._tag === tag),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

const makeLifecycleHarness = (options?: {
  readonly rpcTimeout?: Duration.Input
  readonly sessionAcquisitionTimeout?: Duration.Input
}) =>
  Effect.gen(function*() {
    const credentials = yield* SubscriptionRef.make<Authentication.Credential>({
      generation: 0,
      bearer: Redacted.make("secret")
    })
    const refreshWaitStarted = yield* Deferred.make<number>()
    const attempts = yield* Queue.unbounded<{
      readonly mode: AuthenticatorMode
      readonly rpc: string
    }>()
    const applications = yield* Queue.unbounded<string>()
    const watchStarted = yield* Deferred.make<void>()
    const pullEntered = yield* Deferred.make<void>()
    const pullRelease = yield* Deferred.make<void>()
    const pullInterrupted = yield* Deferred.make<void>()
    const mode = MutableRef.make<AuthenticatorMode>("Available")
    const blockPull = MutableRef.make(false)
    const lifecycleWebSocketConstructions = MutableRef.make(0)

    const provider = Authentication.CredentialProvider.of({
      acquire: SubscriptionRef.get(credentials),
      awaitChange: (generation) =>
        Deferred.succeed(refreshWaitStarted, generation).pipe(
          Effect.andThen(
            SubscriptionRef.changes(credentials).pipe(
              Stream.filter((credential) => credential.generation !== generation),
              Stream.runHead,
              Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
            )
          )
        )
    })
    const clientAuthentication = Authentication.layerClient.pipe(
      Layer.provide(Layer.succeed(Authentication.CredentialProvider, provider))
    )
    const dynamicAuthenticator = Layer.succeed(
      Authentication.Authenticator,
      Authentication.Authenticator.of({
        authenticate: (credential) => {
          const current = MutableRef.get(mode)
          if (current === "Unavailable") return Effect.fail(new ReplicaError.AuthenticatorUnavailable())
          if (current === "Rejected") return Effect.fail(new ReplicaError.CredentialRejected())
          const bearer = Redacted.value(credential)
          if (bearer === "secret" || bearer === "refreshed") return Effect.succeed({ subject: "test" })
          return Effect.fail(new ReplicaError.CredentialRejected())
        }
      })
    )
    const baseAuthenticationServer = Authentication.layerServer.pipe(Layer.provide(dynamicAuthenticator))
    const observedAuthenticationServer = Layer.effect(
      Authentication.Authentication,
      Authentication.Authentication.pipe(
        Effect.map((authenticate) =>
          Authentication.Authentication.of((effect, request) =>
            Queue.offer(attempts, {
              mode: MutableRef.get(mode),
              rpc: request.rpc._tag
            }).pipe(
              Effect.andThen(Effect.suspend(() => {
                if (request.rpc._tag === "Watch") return Deferred.succeed(watchStarted, undefined)
                return Effect.void
              })),
              Effect.andThen(authenticate(effect, request))
            )
          )
        )
      )
    ).pipe(Layer.provide(baseAuthenticationServer))

    const lifecycleHandlers = Layer.mergeAll(
      PutTodo.toLayer(({ payload, transaction }) =>
        transaction.set(Todo, payload.id, payload).pipe(
          Effect.tap(() => Queue.offer(applications, payload.id)),
          Effect.as(payload)
        )
      ),
      ReturnHugeResult.toLayer(() => Effect.succeed("x".repeat(SyncRpc.maximumFrameBytes))),
      AssignRoleV2.toLayer(({ payload }) => Effect.succeed(payload.role))
    )
    const lifecycleRuntime = MutationRuntime.layer(definition, evolution).pipe(Layer.provide(lifecycleHandlers))
    const lifecycleStore = ServerStore.layer({
      ...serverHistory,
      definition,
      evolution,
      authorizeAccess: ({ principal, spaceId: requestedSpaceId }) => {
        if (
          principal !== null && typeof principal === "object" && !Array.isArray(principal) &&
          "subject" in principal && principal.subject === "test" && requestedSpaceId === spaceId
        ) return Effect.void
        return Effect.fail({ reason: "forbidden" })
      },
      authorizeMutation: () => Effect.void,
      authorizeRead: () => {
        if (!MutableRef.get(blockPull)) return Effect.void
        return Deferred.succeed(pullEntered, undefined).pipe(
          Effect.andThen(Deferred.await(pullRelease)),
          Effect.ensuring(Deferred.succeed(pullInterrupted, undefined))
        )
      }
    }).pipe(Layer.provide(lifecycleRuntime), Layer.provide(database))
    const lifecycleCluster = SpaceEntity.layer(entityOptions).pipe(
      Layer.provide(assertionVerifier),
      Layer.provide(lifecycleStore),
      Layer.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })),
      Layer.provide(SingleRunner.layer({ runnerStorage: "memory" }).pipe(Layer.provide(database)))
    )
    const lifecycleWebSocketProtocol = SyncServer.layerProtocolWebSocket({ path: "/sync" }).pipe(
      Layer.provide(HttpRouter.layer)
    )
    const lifecycleServer = SyncServer.layer.pipe(
      Layer.provideMerge(lifecycleWebSocketProtocol),
      Layer.provide(lifecycleCluster),
      Layer.provide(observedAuthenticationServer),
      Layer.provide(assertionIssuer),
      Layer.provide(HttpRouter.serve(lifecycleWebSocketProtocol, {
        disableListenLog: true,
        disableLogger: true
      }))
    )
    const lifecycleConstructor = Layer.effect(
      Socket.WebSocketConstructor,
      Socket.WebSocketConstructor.pipe(Effect.map((makeWebSocket) =>
      (
        url: string,
        protocols?: string | Array<string>
      ) => {
        MutableRef.update(lifecycleWebSocketConstructions, (count) => count + 1)
        return makeWebSocket(url, protocols)
      }))
    ).pipe(Layer.provide(NodeSocket.layerWebSocketConstructor))
    const lifecycleSocket = Effect.gen(function*() {
      const server = yield* HttpServer.HttpServer
      const address = server.address
      if (address._tag === "UnixAddress") return yield* Effect.die("Expected a TCP test server")
      return yield* Socket.makeWebSocket(`http://127.0.0.1:${address.port}/sync`)
    }).pipe(Layer.effect(Socket.Socket), Layer.provide(lifecycleConstructor))
    const lifecycleClientProtocol = SyncClient.layerProtocolSocket().pipe(Layer.provide(lifecycleSocket))
    const lifecycleClient = SyncClient.layerWithOptions(options).pipe(
      Layer.provide(lifecycleClientProtocol),
      Layer.provide(clientAuthentication)
    )
    const lifecycleLive = lifecycleClient.pipe(
      Layer.provideMerge(lifecycleServer),
      Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
    )
    const replicaLayer = (maximumRetryDelay: Duration.Input) =>
      SqlReplica.layer({
        ...clientHistory,
        definition,
        evolution,
        clientId,
        initialSpaces: [spaceId],
        retryDelay: "1 second",
        maximumRetryDelay
      }).pipe(
        Layer.provide(lifecycleHandlers),
        Layer.provideMerge(database),
        Layer.provide(lifecycleLive)
      )

    return {
      applications,
      attempts,
      blockPull,
      credentials,
      live: lifecycleLive,
      webSocketConstructions: lifecycleWebSocketConstructions,
      mode,
      pullEntered,
      pullInterrupted,
      pullRelease,
      replicaLayer,
      refreshWaitStarted,
      watchStarted
    }
  })

describe("WebSocket synchronization", () => {
  it.effect("pauses a rejected credential generation at NeedsAuthentication", () =>
    Effect.gen(function*() {
      const harness = yield* makeLifecycleHarness()
      const replicaContext = yield* Layer.build(harness.replicaLayer("4 seconds"))
      const replica = Context.get(replicaContext, Replica.Replica)
      const reactivity = Context.get(replicaContext, Reactivity.Reactivity)
      const space = yield* replica.space(spaceId)
      yield* Effect.all([
        awaitStatus(reactivity, space, "Online"),
        Deferred.await(harness.watchStarted)
      ], { discard: true, concurrency: "unbounded" })
      yield* Queue.takeAll(harness.attempts)

      const needsAuthentication = yield* awaitStatus(reactivity, space, "NeedsAuthentication").pipe(
        Effect.forkChild({ startImmediately: true })
      )
      MutableRef.set(harness.mode, "Rejected")
      yield* space.mutate(PutTodo, { id: "expired", title: "expired credential" })
      assert.strictEqual(yield* Deferred.await(harness.refreshWaitStarted), 0)
      assert.strictEqual((yield* Fiber.join(needsAuthentication))._tag, "NeedsAuthentication")
      yield* Queue.takeAll(harness.attempts)

      yield* TestClock.adjust("1 minute")
      yield* space.mutate(PutTodo, { id: "still-expired", title: "must stay local" })
      assert.isTrue(Option.isNone(yield* Queue.poll(harness.attempts)))
    }))

  it.effect("resumes synchronization after a credential refresh without rebuilding the replica", () =>
    Effect.gen(function*() {
      const harness = yield* makeLifecycleHarness()
      const replicaContext = yield* Layer.build(harness.replicaLayer("4 seconds"))
      const replica = Context.get(replicaContext, Replica.Replica)
      const reactivity = Context.get(replicaContext, Reactivity.Reactivity)
      const space = yield* replica.space(spaceId)
      yield* Effect.all([
        awaitStatus(reactivity, space, "Online"),
        Deferred.await(harness.watchStarted)
      ], { discard: true, concurrency: "unbounded" })

      MutableRef.set(harness.mode, "Rejected")
      const needsAuthentication = yield* awaitStatus(reactivity, space, "NeedsAuthentication").pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* space.mutate(PutTodo, { id: "refresh", title: "rotated credential" })
      assert.strictEqual(yield* Deferred.await(harness.refreshWaitStarted), 0)
      yield* Fiber.join(needsAuthentication)
      const online = yield* awaitStatus(reactivity, space, "Online").pipe(
        Effect.forkChild({ startImmediately: true })
      )

      MutableRef.set(harness.mode, "Available")
      yield* SubscriptionRef.set(harness.credentials, {
        generation: 1,
        bearer: Redacted.make("refreshed")
      })

      assert.strictEqual(yield* Queue.take(harness.applications), "refresh")
      assert.strictEqual((yield* Fiber.join(online))._tag, "Online")
      assert.strictEqual(Context.get(replicaContext, Replica.Replica), replica)
      assert.strictEqual(MutableRef.get(harness.webSocketConstructions), 1)
    }))

  it.effect("backs off an unavailable authenticator and recovers", () =>
    Effect.gen(function*() {
      const harness = yield* makeLifecycleHarness()
      const replicaContext = yield* Layer.build(harness.replicaLayer("2 seconds"))
      const replica = Context.get(replicaContext, Replica.Replica)
      const reactivity = Context.get(replicaContext, Reactivity.Reactivity)
      const space = yield* replica.space(spaceId)
      yield* Effect.all([
        awaitStatus(reactivity, space, "Online"),
        Deferred.await(harness.watchStarted)
      ], { discard: true, concurrency: "unbounded" })
      yield* Queue.takeAll(harness.attempts)

      const offline = yield* awaitStatus(reactivity, space, "Offline").pipe(
        Effect.forkChild({ startImmediately: true })
      )
      MutableRef.set(harness.mode, "Unavailable")
      yield* space.mutate(PutTodo, { id: "outage", title: "backoff" })
      assert.deepInclude(yield* Queue.take(harness.attempts), { mode: "Unavailable" })
      assert.strictEqual((yield* Fiber.join(offline))._tag, "Offline")

      yield* TestClock.adjust("999 millis")
      assert.isTrue(Option.isNone(yield* Queue.poll(harness.attempts)))
      yield* TestClock.adjust("1 millis")
      assert.deepInclude(yield* Queue.take(harness.attempts), { mode: "Unavailable" })

      yield* TestClock.adjust("1999 millis")
      assert.isTrue(Option.isNone(yield* Queue.poll(harness.attempts)))
      yield* TestClock.adjust("1 millis")
      assert.deepInclude(yield* Queue.take(harness.attempts), { mode: "Unavailable" })

      const online = yield* awaitStatus(reactivity, space, "Online").pipe(
        Effect.forkChild({ startImmediately: true })
      )
      MutableRef.set(harness.mode, "Available")
      yield* TestClock.adjust("2 seconds")
      assert.strictEqual(yield* Queue.take(harness.applications), "outage")
      assert.strictEqual((yield* Fiber.join(online))._tag, "Online")
    }))

  it.effect("interrupts a hung RPC at rpcTimeout", () =>
    Effect.gen(function*() {
      const harness = yield* makeLifecycleHarness({ rpcTimeout: "1 second" })
      yield* Effect.addFinalizer(() => Deferred.succeed(harness.pullRelease, undefined))
      const context = yield* Layer.build(harness.live)
      const remote = Context.get(context, SyncEngine.SyncEngine)
      const request = pullRequest()
      yield* remote.pull(request)
      MutableRef.set(harness.blockPull, true)
      const pulling = yield* remote.pull(request).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(harness.pullEntered)

      yield* TestClock.adjust("1 second")
      const error = yield* Fiber.join(pulling).pipe(Effect.flip)
      assert.strictEqual(error._tag, "OperationTimeout")
      if (error._tag === "OperationTimeout") {
        assert.strictEqual(error.operation, "Pull")
        assert.strictEqual(error.timeoutMillis, 1_000)
      }
      yield* Deferred.await(harness.pullInterrupted)
    }))

  it.effect("keeps a healthy idle watch open past rpcTimeout", () =>
    Effect.gen(function*() {
      const harness = yield* makeLifecycleHarness({ rpcTimeout: "500 millis" })
      const replicaContext = yield* Layer.build(harness.replicaLayer("8 seconds"))
      const replica = Context.get(replicaContext, Replica.Replica)
      const reactivity = Context.get(replicaContext, Reactivity.Reactivity)
      const space = yield* replica.space(spaceId)
      yield* Effect.all([
        awaitStatus(reactivity, space, "Online"),
        Deferred.await(harness.watchStarted)
      ], { discard: true, concurrency: "unbounded" })
      yield* Queue.takeAll(harness.attempts)
      const restarted = yield* Stream.fromQueue(harness.attempts).pipe(
        Stream.filter(({ rpc }) => rpc === "Watch"),
        Stream.runHead,
        Effect.timeoutOption("3 seconds"),
        Effect.forkChild({ startImmediately: true })
      )

      yield* TestClock.adjust("3 seconds")

      assert.isTrue(Option.isNone(yield* Fiber.join(restarted)))
    }))

  it.effect("multiplexes two Replica spaces through exactly one WebSocket", () =>
    Effect.gen(function*() {
      MutableRef.set(webSocketConstructions, 0)
      MutableRef.set(liveWebSockets, 0)
      const replicaContext = yield* Layer.build(
        SqlReplica.layer({
          ...clientHistory,
          definition,
          evolution,
          clientId,
          scope,
          initialSpaces: [spaceId, secondSpaceId],
          retryDelay: "1 millis"
        }).pipe(
          Layer.provide(handlers),
          Layer.provide(database),
          Layer.provide(singleClientLive)
        )
      )
      const replica = Context.get(replicaContext, Replica.Replica)
      const first = yield* replica.space(spaceId)
      const second = yield* replica.space(secondSpaceId)
      const [firstPending, secondPending] = yield* Effect.all([
        first.mutate(PutTodo, { id: "shared", title: "first socket" }),
        second.mutate(PutTodo, { id: "shared", title: "second socket" })
      ], { concurrency: "unbounded" })
      const awaitReceipt = (space: Replica.Space, id: Identity.MutationId) =>
        Effect.gen(function*() {
          while (true) {
            const receipt = yield* space.receipt(PutTodo, id)
            if (Option.isSome(receipt)) return receipt.value
            yield* Effect.yieldNow
          }
        })
      const [firstReceipt, secondReceipt] = yield* Effect.all([
        awaitReceipt(first, firstPending.envelope.mutationId),
        awaitReceipt(second, secondPending.envelope.mutationId)
      ], { concurrency: "unbounded" })

      assert.strictEqual(firstReceipt.spaceId, spaceId)
      assert.strictEqual(secondReceipt.spaceId, secondSpaceId)
      assert.strictEqual(MutableRef.get(webSocketConstructions), 1)
      assert.strictEqual(MutableRef.get(liveWebSockets), 1)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it.effect("rejects an empty protocol version configuration at layer construction", () =>
    Effect.scoped(Effect.gen(function*() {
      const error = yield* Layer.build(invalidProtocolLive).pipe(Effect.flip)
      assert.strictEqual(error._tag, "InvalidConfiguration")
    })))

  it.effect("returns UpgradeRequired when protocol versions do not intersect", () =>
    Effect.gen(function*() {
      const remote = yield* SyncEngine.SyncEngine
      const error = yield* remote.pull(pullRequest()).pipe(Effect.flip)
      assert.strictEqual(error._tag, "UpgradeRequired")
    }).pipe(Effect.provide(incompatibleLive)))

  it.effect("shares one negotiated protocol version across configurable sync and presence clients", () =>
    Effect.gen(function*() {
      MutableRef.set(protocolObservations, [])
      const remote = yield* SyncEngine.SyncEngine
      const presence = yield* PresenceClient.PresenceClient

      yield* remote.pull(pullRequest())
      yield* presence.publish({
        spaceId,
        clientId,
        value: { cursor: 3 },
        ttlMillis: 5_000
      })

      assert.deepStrictEqual(MutableRef.get(protocolObservations), [
        { _tag: "Negotiate" },
        { _tag: "Pull", version: Protocol.ProtocolVersion.make(2) },
        { _tag: "PublishPresence", version: Protocol.ProtocolVersion.make(2) }
      ])
    }).pipe(Effect.provide(configurableLive)))

  it.effect("delivers an authorized bounded bootstrap through both RPC hops", () =>
    Effect.gen(function*() {
      const remote = yield* SyncEngine.SyncEngine
      for (let sequence = 1; sequence <= 3; sequence++) {
        const identity: Omit<Protocol.MutationEnvelope, "digest"> = {
          spaceId,
          clientId,
          mutationId: Identity.MutationId.make(
            `mut_00000000-0000-4000-8000-${String(90 + sequence).padStart(12, "0")}`
          ),
          localSequence: Identity.LocalSequence.make(sequence),
          basis: Identity.ServerSequence.make(0),
          name: PutTodo.name,
          payload: { id: `bootstrap-${sequence}`, title: "s".repeat(250) },
          digestVersion: 3,
          membershipIncarnation: Identity.legacyMembershipIncarnation,
          sourceSchema: definition.schemaIdentity,
          mutationVersion: PutTodo.version
        }
        const mutation = Protocol.MutationEnvelope.make({
          ...identity,
          digest: yield* Protocol.mutationDigest(identity)
        })
        yield* remote.submit({ envelope: mutation, schema: definition.schemaIdentity })
      }
      yield* (yield* ServerStore.ServerStore).maintain(spaceId)

      const pulled = yield* remote.pull(pullRequest())
      assert.isTrue("_tag" in pulled)
      if (!("_tag" in pulled)) assert.fail("expected bootstrap")
      const request = {
        spaceId,
        clientId,
        schema: definition.schemaIdentity,
        scope,
        scopeGeneration,
        cursor: pulled.manifest.cursor,
        snapshotId: pulled.manifest.snapshotId,
        afterOrdinal: -1,
        limit: 1
      }
      let page = yield* remote.bootstrap(request)
      let entities = 0
      let pages = 0
      while (true) {
        assert.isAbove(page.entries.length, 0)
        assert.isAtMost(Protocol.encodedBytes(page), serverHistory.maximumBootstrapPageBytes)
        entities += page.entries.length
        pages += 1
        if (!page.hasMore) break
        const last = page.entries.at(-1)
        assert.isDefined(last)
        page = yield* remote.bootstrap({
          ...request,
          afterOrdinal: last.ordinal
        })
      }
      assert.strictEqual(entities, 3)
      assert.isAbove(pages, 1)

      const denied = yield* (yield* RevokedSyncEngine).bootstrap(request).pipe(Effect.flip)
      assert.strictEqual(denied._tag, "AuthorizationDenied")
    }).pipe(
      Effect.provide(Layer.mergeAll(live, store, database)),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("reauthorizes exact retries without retaining mutation history in Cluster storage", () =>
    Effect.gen(function*() {
      const remote = yield* SyncEngine.SyncEngine
      const identity = {
        spaceId,
        clientId,
        mutationId,
        localSequence: Identity.LocalSequence.make(1),
        basis: Identity.ServerSequence.make(0),
        name: PutTodo.name,
        payload: { id: "1", title: "socket" },
        digestVersion: 3 as const,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        sourceSchema: definition.schemaIdentity,
        mutationVersion: PutTodo.version
      }
      const envelope: Protocol.MutationEnvelope = {
        ...identity,
        digest: yield* Protocol.mutationDigest(identity)
      }
      const request = { envelope, schema: definition.schemaIdentity }
      const receipt = yield* remote.submit(request)
      assert.strictEqual(receipt._tag, "Accepted")

      const revoked = yield* RevokedSyncEngine
      const revokedRetry = yield* revoked.submit(request).pipe(Effect.flip)
      assert.strictEqual(revokedRetry._tag, "AuthorizationDenied")

      const sql = yield* SqlClient.SqlClient
      const messageRows = yield* sql<{ count: number }>`SELECT COUNT(*) AS count FROM cluster_messages`
      const replyRows = yield* sql<{ count: number }>`SELECT COUNT(*) AS count FROM cluster_replies`
      assert.deepStrictEqual([messageRows[0]?.count, replyRows[0]?.count], [0, 0])

      const page = yield* remote.pull(pullRequest())
      if (!("_tag" in page)) assert.fail("expected bootstrap")
      const bootstrap = yield* remote.bootstrap({
        spaceId,
        clientId,
        schema: definition.schemaIdentity,
        scope,
        scopeGeneration,
        cursor: page.manifest.cursor,
        snapshotId: page.manifest.snapshotId,
        afterOrdinal: -1,
        limit: 10
      })
      assert.deepStrictEqual(bootstrap.entries.map((entry) => entry.change._tag), ["Upsert"])

      const denied = yield* remote.pull(pullRequest(forbiddenSpaceId)).pipe(Effect.flip)
      assert.strictEqual(denied._tag, "AuthorizationDenied")

      const forbiddenIdentity = { ...identity, spaceId: forbiddenSpaceId }
      const forbiddenEnvelope = {
        ...forbiddenIdentity,
        digest: yield* Protocol.mutationDigest(forbiddenIdentity)
      }
      const forbidden = yield* remote.submit({
        envelope: forbiddenEnvelope,
        schema: definition.schemaIdentity
      }).pipe(Effect.flip)
      assert.strictEqual(forbidden._tag, "AuthorizationDenied")
    }).pipe(
      Effect.provide(Layer.merge(live, database)),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("revokes an established watch within the configured authorization window", () =>
    Effect.gen(function*() {
      MutableRef.set(readAuthorized, true)
      const remote = yield* SyncEngine.SyncEngine
      const initialWake = yield* Deferred.make<void>()
      const watching = yield* remote.watch({
        spaceId,
        clientId,
        schema: definition.schemaIdentity,
        scope,
        scopeGeneration,
        cursor: null
      }).pipe(
        Stream.tap(() => Deferred.succeed(initialWake, undefined)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(initialWake)
      MutableRef.set(readAuthorized, false)
      yield* TestClock.adjust("500 millis")

      const denied = yield* Fiber.join(watching).pipe(Effect.flip)
      assert.strictEqual(denied._tag, "AuthorizationDenied")
    }).pipe(
      Effect.ensuring(Effect.sync(() => MutableRef.set(readAuthorized, true))),
      Effect.provide(live),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("authorizes only the migrated mutation payload", () =>
    Effect.gen(function*() {
      const remote = yield* SyncEngine.SyncEngine
      const identity = {
        spaceId,
        clientId,
        mutationId,
        localSequence: Identity.LocalSequence.make(1),
        basis: Identity.ServerSequence.make(0),
        name: AssignRoleV1.name,
        payload: { account: "victim" },
        digestVersion: 3 as const,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        sourceSchema: definitionV1.schemaIdentity,
        mutationVersion: AssignRoleV1.version
      }
      const envelope: Protocol.MutationEnvelope = {
        ...identity,
        digest: yield* Protocol.mutationDigest(identity)
      }
      const receipt = yield* remote.submit({ envelope, schema: definition.schemaIdentity })

      assert.strictEqual(receipt._tag, "Rejected")
      if (receipt._tag === "Rejected") {
        assert.strictEqual(receipt.origin, "Authorization")
        assert.deepStrictEqual(receipt.rejection, { reason: "admin role requires elevated access" })
      }
    }).pipe(
      Effect.provide(live),
      Effect.provide(NodeCrypto.layer)
    ))

  it.effect("multiplexes bounded ephemeral presence over the same WebSocket protocol", () =>
    Effect.gen(function*() {
      const presence = yield* PresenceClient.PresenceClient
      const received = yield* presence.watch(spaceId).pipe(
        Stream.runHead,
        Effect.forkChild({ startImmediately: true })
      )
      const update: Protocol.PresenceUpdate = {
        spaceId,
        clientId,
        value: { cursor: 3 },
        ttlMillis: 5_000
      }
      yield* presence.publish(update)
      const value = yield* Fiber.join(received)
      assert.deepStrictEqual(Option.getOrUndefined(value), update)
    }).pipe(TestClock.withLive, Effect.provide(live)))

  it.effect("returns and replays a bounded terminal rejection for an oversized private result", () =>
    Effect.gen(function*() {
      const remote = yield* SyncEngine.SyncEngine
      const identity = {
        spaceId,
        clientId,
        mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000002"),
        localSequence: Identity.LocalSequence.make(1),
        basis: Identity.ServerSequence.make(0),
        name: ReturnHugeResult.name,
        payload: null,
        digestVersion: 3 as const,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        sourceSchema: definition.schemaIdentity,
        mutationVersion: ReturnHugeResult.version
      }
      const submitted: Protocol.MutationEnvelope = {
        ...identity,
        digest: yield* Protocol.mutationDigest(identity)
      }
      const request = { envelope: submitted, schema: definition.schemaIdentity }
      const first = yield* remote.submit(request)
      const retry = yield* remote.submit(request)

      assert.strictEqual(first._tag, "Rejected")
      assert.deepStrictEqual(retry, first)
      assert.isAtMost(yield* Protocol.encodedBytesEffect(first), SyncRpc.maximumFrameBytes)
      if (first._tag === "Rejected") {
        assert.deepStrictEqual(first.rejection, {
          _tag: "CapacityExceeded",
          resource: "receipt bytes",
          limit: Protocol.maximumReceiptBytes
        })
      }
      const page = yield* remote.pull(pullRequest())
      if (!("_tag" in page)) assert.fail("expected bootstrap")
      assert.strictEqual(page.manifest.entityCount, 0)
    }).pipe(
      Effect.provide(live),
      Effect.provide(NodeCrypto.layer)
    ))
})
