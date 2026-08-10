import { NodeCrypto, NodeHttpServer, NodeSocket } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServer from "effect/unstable/http/HttpServer"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as Authentication from "../src/Authentication.js"
import * as PresenceClient from "../src/PresenceClient.js"
import * as PresenceHub from "../src/PresenceHub.js"
import * as SyncClient from "../src/SyncClient.js"
import * as SyncServer from "../src/SyncServer.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const forbiddenSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000001")

const Todo = Model.make("Todo", {
  key: Schema.String,
  schema: Schema.Struct({ id: Schema.String, title: Schema.String })
})
const PutTodo = Mutation.make("PutTodo", { payload: Todo.schema, success: Todo.schema })
const definition = Definition.make({ models: [Todo], mutations: [PutTodo] })
const handlers = PutTodo.toLayer(({ payload, transaction }) =>
  transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))
)
const runtime = MutationRuntime.layer(definition).pipe(Layer.provide(handlers))
const database = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)

const store = ServerStore.layer({
  definition,
  authorizeRead: ({ principal, spaceId: requestedSpaceId }) =>
    principal !== null && typeof principal === "object" && !Array.isArray(principal) &&
      "subject" in principal && principal.subject === "test" && requestedSpaceId === spaceId
      ? Effect.void
      : Effect.fail({ reason: "forbidden" })
}).pipe(Layer.provide(runtime), Layer.provide(database))

const authenticator = Layer.succeed(
  Authentication.Authenticator,
  Authentication.Authenticator.of({
    authenticate: (credential) =>
      Redacted.value(credential) === "secret"
        ? Effect.succeed({ subject: "test" })
        : Effect.fail(new Authentication.AuthenticationFailure())
  })
)
const authenticationServer = Authentication.layerServer.pipe(Layer.provide(authenticator))
const authenticationClient = Authentication.layerClient.pipe(Layer.provide(
  Layer.succeed(Authentication.Credentials, Redacted.make("secret"))
))
const presenceReady = Deferred.makeUnsafe<void>()
const presenceHub = PresenceHub.layer({
  authorize: () => Deferred.succeed(presenceReady, undefined).pipe(Effect.asVoid)
})

const websocketProtocol = SyncServer.layerProtocolWebSocket({ path: "/sync" }).pipe(
  Layer.provide(HttpRouter.layer)
)
const websocketServer = SyncServer.layer.pipe(
  Layer.provideMerge(websocketProtocol),
  Layer.provide(store),
  Layer.provide(presenceHub),
  Layer.provide(authenticationServer),
  Layer.provide(HttpRouter.serve(websocketProtocol, { disableListenLog: true, disableLogger: true }))
)
const socket = Effect.gen(function*() {
  const server = yield* HttpServer.HttpServer
  const address = server.address as HttpServer.TcpAddress
  return NodeSocket.layerWebSocket(`http://127.0.0.1:${address.port}/sync`)
}).pipe(Layer.unwrap)
const clientProtocol = SyncClient.layerProtocolSocket().pipe(Layer.provide(socket))
const client = Layer.merge(SyncClient.layer, PresenceClient.layer).pipe(
  Layer.provide(clientProtocol),
  Layer.provide(authenticationClient)
)
const live = client.pipe(
  Layer.provideMerge(websocketServer),
  Layer.provide([NodeHttpServer.layerTest, RpcSerialization.layerNdjson])
)

describe("WebSocket synchronization", () => {
  it.effect("round trips typed mutation receipts and preserves domain RPC failures", () =>
    Effect.gen(function*() {
      const remote = yield* SyncEngine.SyncEngine
      const identity = {
        spaceId,
        clientId,
        mutationId,
        localSequence: Identity.LocalSequence.make(1),
        basis: Identity.ServerSequence.make(0),
        name: PutTodo.name,
        payload: { id: "1", title: "socket" }
      }
      const envelope: Protocol.MutationEnvelope = {
        ...identity,
        digest: yield* Canonical.digest(identity)
      }
      const receipt = yield* remote.submit(envelope)
      assert.strictEqual(receipt._tag, "Accepted")
      const page = yield* remote.pull({ spaceId, after: Identity.ServerSequence.make(0), limit: 10 })
      assert.deepStrictEqual(page.entries.map((entry) => entry.envelope.mutationId), [mutationId])

      const denied = yield* remote.pull({
        spaceId: forbiddenSpaceId,
        after: Identity.ServerSequence.make(0),
        limit: 10
      }).pipe(Effect.flip)
      assert.strictEqual(denied._tag, "AuthorizationDenied")
    }).pipe(
      Effect.provide(live),
      Effect.provide(NodeCrypto.layer)
    ), { timeout: 20_000 })

  it.effect("multiplexes bounded ephemeral presence over the same WebSocket protocol", () =>
    Effect.gen(function*() {
      const presence = yield* PresenceClient.PresenceClient
      const received = yield* presence.watch(spaceId).pipe(Stream.runHead, Effect.forkChild)
      yield* Deferred.await(presenceReady)
      const update: Protocol.PresenceUpdate = {
        spaceId,
        clientId,
        value: { cursor: 3 },
        ttlMillis: 5_000
      }
      yield* presence.publish(update)
      const value = yield* Fiber.join(received)
      assert.deepStrictEqual(value._tag === "Some" ? value.value : undefined, update)
    }).pipe(Effect.provide(live)), { timeout: 20_000 })
})
