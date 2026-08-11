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
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as MutableRef from "effect/MutableRef"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServer from "effect/unstable/http/HttpServer"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as Socket from "effect/unstable/socket/Socket"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as Authentication from "../src/Authentication.js"
import * as PresenceClient from "../src/PresenceClient.js"
import * as PresenceHub from "../src/PresenceHub.js"
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
  retainedHistoryEntries: 0,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 0,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 1_024,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration
}
const clientHistory = {
  retainedReceipts: 256,
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
      return Effect.fail(new Authentication.AuthenticationFailure())
    }
  })
)
const authenticationServer = Authentication.layerServer.pipe(Layer.provide(authenticator))
const authenticationClient = Layer.fresh(Authentication.layerClient).pipe(Layer.provide(
  Layer.succeed(Authentication.Credentials, Redacted.make("secret"))
))
const revokedAuthenticationClient = Layer.fresh(Authentication.layerClient).pipe(Layer.provide(
  Layer.succeed(Authentication.Credentials, Redacted.make("revoked"))
))
const presenceHub = PresenceHub.layerTrusted()
const cluster = SpaceEntity.layer().pipe(
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
  | { readonly _tag: "Pull"; readonly version: Protocol.ProtocolVersion | undefined }
  | { readonly _tag: "PublishPresence"; readonly version: Protocol.ProtocolVersion | undefined }

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
          } else if (options.rpc._tag === "Pull" && Schema.is(Protocol.PullRequest)(payload)) {
            MutableRef.update(protocolObservations, (observations) => [
              ...observations,
              { _tag: "Pull" as const, version: payload.protocolVersion }
            ])
          } else if (
            options.rpc._tag === "PublishPresence" && Schema.is(Protocol.PresenceUpdate)(payload)
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
  Layer.provide(HttpRouter.serve(websocketProtocol, { disableListenLog: true, disableLogger: true }))
)
const configurableProtocolSession = ProtocolSession.layerWithOptions({ supportedProtocolVersions: [1, 2] })
const configurableClient = Layer.merge(SyncClient.layerFromSession, PresenceClient.layerFromSession).pipe(
  Layer.provide(configurableProtocolSession),
  Layer.provide(clientProtocol),
  Layer.provide(authenticationClient)
)
const configurableLive = configurableClient.pipe(
  Layer.provideMerge(protocol2Server),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)

describe("WebSocket synchronization", () => {
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
            const receipt = yield* space.receipt(id)
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
      const error = yield* remote.pull({
        spaceId,
        schema: definition.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      }).pipe(Effect.flip)
      assert.strictEqual(error._tag, "UpgradeRequired")
    }).pipe(Effect.provide(incompatibleLive)))

  it.effect("shares one negotiated protocol version across configurable sync and presence clients", () =>
    Effect.gen(function*() {
      MutableRef.set(protocolObservations, [])
      const remote = yield* SyncEngine.SyncEngine
      const presence = yield* PresenceClient.PresenceClient

      yield* remote.pull({
        spaceId,
        schema: definition.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
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

      const pulled = yield* remote.pull({
        spaceId,
        schema: definition.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
      assert.isTrue("_tag" in pulled)
      if (!("_tag" in pulled)) assert.fail("expected bootstrap")
      const request = {
        spaceId,
        schema: definition.schemaIdentity,
        snapshotId: pulled.manifest.snapshotId,
        afterOrdinal: -1,
        limit: 10
      }
      let page = yield* remote.bootstrap(request)
      let entities = 0
      let pages = 0
      while (true) {
        assert.isAbove(page.entities.length, 0)
        assert.isAtMost(Protocol.encodedBytes(page), serverHistory.maximumBootstrapPageBytes)
        entities += page.entities.length
        pages += 1
        if (!page.hasMore) break
        const last = page.entities.at(-1)
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

      const page = yield* remote.pull({
        spaceId,
        schema: definition.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
      if ("_tag" in page) assert.fail("unexpected bootstrap")
      assert.deepStrictEqual(page.entries.map((entry) => entry.mutationId), [mutationId])

      const denied = yield* remote.pull({
        spaceId: forbiddenSpaceId,
        schema: definition.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      }).pipe(Effect.flip)
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

  it.effect("reauthorizes an established watch before emitting a later wake", () =>
    Effect.gen(function*() {
      MutableRef.set(readAuthorized, true)
      const remote = yield* SyncEngine.SyncEngine
      const initialWake = yield* Deferred.make<void>()
      const watching = yield* remote.watch({ spaceId, schema: definition.schemaIdentity }).pipe(
        Stream.tap((wake) => {
          if (wake.sequence === 0) return Deferred.succeed(initialWake, undefined)
          return Effect.void
        }),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(initialWake)
      MutableRef.set(readAuthorized, false)

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
      const receipt = yield* remote.submit({ envelope, schema: definition.schemaIdentity })
      assert.strictEqual(receipt._tag, "Accepted")

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
    }).pipe(Effect.provide(live)))

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
      const page = yield* remote.pull({
        spaceId,
        schema: definition.schemaIdentity,
        after: Identity.ServerSequence.make(0),
        limit: 10
      })
      if ("_tag" in page) assert.fail("unexpected bootstrap")
      assert.deepStrictEqual(page.entries, [])
    }).pipe(
      Effect.provide(live),
      Effect.provide(NodeCrypto.layer)
    ))
})
