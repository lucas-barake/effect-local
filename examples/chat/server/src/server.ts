import { LoginRequest, Principal } from "@effect-local/example-chat-shared/auth"
import {
  AdvanceDelivery,
  AdvanceRead,
  Conversation,
  definition,
  Message,
  SendMessage,
  StartConversation,
  tokenFor,
  users
} from "@effect-local/example-chat-shared/domain"
import { layerMutations } from "@effect-local/example-chat-shared/handlers"
import * as NodeCrypto from "@effect/platform-node/NodeCrypto"
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer"
import * as SqliteClient from "@effect/sql-sqlite-node/SqliteClient"
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as SyncRpc from "@lucas-barake/effect-local-rpc/SyncRpc"
import * as SyncServer from "@lucas-barake/effect-local-rpc/SyncServer"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as SingleRunner from "effect/cluster/SingleRunner"
import * as Effect from "effect/Effect"
import * as HttpRouter from "effect/http/HttpRouter"
import * as HttpServerRequest from "effect/http/HttpServerRequest"
import * as HttpServerResponse from "effect/http/HttpServerResponse"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
// oxlint-disable-next-line effect/noNodeBuiltinImport -- NodeHttpServer.layer takes the platform's own http server factory; this file is the Node host boundary.
import * as Http from "node:http"

/**
 * The chat sync server: one authenticated WebSocket RPC endpoint (`/sync`)
 * backed by a SQLite ServerStore and a single-process Effect Cluster, plus a
 * plain `POST /login` route on the same router that trades a hard-coded
 * username/password pair for a bearer token.
 *
 * `makeServerLayer` is parameterized so the smoke tests boot the exact
 * production composition with an in-memory database and an ephemeral port.
 */

export interface ChatServerOptions {
  readonly port: number
  readonly databaseFile: string
}

class ChatAuthorizationError extends Schema.TaggedError<ChatAuthorizationError, Schema.JsonObject>(
  "@effect-local/example-chat/ChatAuthorizationError"
)("ChatAuthorizationError", { reason: Schema.String }) {}

const decodePrincipal = (principal: typeof Schema.Json.Type) =>
  Schema.decodeUnknownEffect(Principal)(principal).pipe(
    Effect.mapError(() => new ChatAuthorizationError({ reason: "Malformed principal" })),
    Effect.filterOrFail(
      (decoded) => users.some((user) => user.id === decoded.userId),
      () => new ChatAuthorizationError({ reason: "Unknown user" })
    )
  )

const layerAuthenticator = Layer.succeed(
  Authentication.Authenticator,
  Authentication.Authenticator.of({
    authenticate: (credential) => {
      const bearer = Redacted.value(credential)
      const user = users.find((candidate) => tokenFor(candidate.id) === bearer)
      if (user === undefined) {
        return Effect.fail(new ReplicaError.CredentialRejected())
      }
      return Effect.succeed<typeof Schema.Json.Type>({ userId: user.id, name: user.name })
    }
  })
)

const layerLoginRoute = HttpRouter.add(
  "POST",
  "/login",
  Effect.gen(function*() {
    const body = yield* HttpServerRequest.schemaBodyJson(LoginRequest)
    const user = users.find((candidate) => candidate.id === body.username)
    if (user === undefined || user.password !== body.password) {
      return HttpServerResponse.jsonUnsafe({ error: "Invalid username or password" }, { status: 401 })
    }
    return HttpServerResponse.jsonUnsafe({
      token: tokenFor(user.id),
      userId: user.id,
      name: user.name,
      color: user.color
    })
  }).pipe(
    // The response bodies are plain ASCII JSON built by this route, so the
    // non-effectful `jsonUnsafe` encoding cannot fail here.
    Effect.catchTags({
      SchemaError: () =>
        Effect.succeed(HttpServerResponse.jsonUnsafe({ error: "Invalid login request" }, { status: 400 })),
      HttpServerError: () =>
        Effect.succeed(HttpServerResponse.jsonUnsafe({ error: "Invalid login request" }, { status: 400 }))
    }),
    Effect.withSpan("chat.login")
  )
)

const makeLayerDatabase = (databaseFile: string) =>
  Layer.mergeAll(
    SqliteClient.layer({ filename: databaseFile }),
    NodeCrypto.layer
  )

const layerSync = SyncServer.layer({
  definition,
  // Any authenticated user may join the shared demo space.
  authorizeAccess: ({ principal }) => Effect.asVoid(decodePrincipal(principal)),
  // Writes are checked against the principal: you can only send as yourself,
  // start conversations you belong to, and advance your own read positions.
  authorizeMutation: Effect.fn("chat.authorizeMutation")(function*({ mutation, principal }) {
    const self = yield* decodePrincipal(principal)
    switch (mutation.name) {
      case SendMessage.name: {
        const payload = yield* Schema.decodeUnknownEffect(Message.schema)(mutation.payload).pipe(
          Effect.mapError(() => new ChatAuthorizationError({ reason: "Malformed SendMessage payload" }))
        )
        if (payload.senderId !== self.userId) {
          return yield* new ChatAuthorizationError({ reason: "Cannot send as another user" })
        }
        return yield* Effect.void
      }
      case StartConversation.name: {
        const payload = yield* Schema.decodeUnknownEffect(Conversation.schema)(mutation.payload).pipe(
          Effect.mapError(() => new ChatAuthorizationError({ reason: "Malformed StartConversation payload" }))
        )
        if (payload.createdBy !== self.userId) {
          return yield* new ChatAuthorizationError({ reason: "Cannot credit a conversation to another user" })
        }
        if (!payload.memberIds.includes(self.userId)) {
          return yield* new ChatAuthorizationError({ reason: "Cannot start a conversation you are not in" })
        }
        return yield* Effect.void
      }
      case AdvanceDelivery.name:
      case AdvanceRead.name: {
        const payload = yield* Schema.decodeUnknownEffect(AdvanceRead.payloadSchema)(mutation.payload).pipe(
          Effect.mapError(() => new ChatAuthorizationError({ reason: "Malformed read-state payload" }))
        )
        if (payload.userId !== self.userId) {
          return yield* new ChatAuthorizationError({ reason: "Cannot advance another user's read state" })
        }
        return yield* Effect.void
      }
      // Deny by default: a mutation added to the definition later must not
      // become implicitly authorized because nobody extended this switch.
      default:
        return yield* new ChatAuthorizationError({ reason: "Unknown mutation" })
    }
  }),
  // Reads are membership-gated only: every participant must observe the other
  // members' read-state rows, or delivery/read ticks could never advance.
  authorizeRead: ({ principal }) => Effect.asVoid(decodePrincipal(principal)),
  // NOTE: the authorization input carries `{ spaceId, member, principal }` but
  // not the published value, so ephemeral identity (presence/typing userId)
  // stays client-asserted in this example. Durable mutations ARE principal-
  // bound via authorizeMutation above.
  authorizeEphemeral: ({ principal }) =>
    decodePrincipal(principal).pipe(
      Effect.mapError((error) => new ReplicaError.AuthorizationDenied({ reason: error.reason })),
      Effect.asVoid
    )
})

/** The full server composition. Launch with `Layer.launch` or `Layer.unwrap`-based test harnesses. */
export const makeServerLayer = (options: ChatServerOptions) => {
  const layerRoutes = Layer.mergeAll(SyncServer.layerProtocolWebSocket({ path: "/sync" }), layerLoginRoute)
  const layerApp = layerSync.pipe(Layer.provideMerge(layerRoutes))

  return HttpRouter.serve(layerApp, { disableLogger: true }).pipe(
    Layer.provide(Authentication.layerServer.pipe(Layer.provide(layerAuthenticator))),
    Layer.provide(SingleRunner.layer({ runnerStorage: "memory" })),
    Layer.provide(layerMutations),
    Layer.provide(makeLayerDatabase(options.databaseFile)),
    // provideMerge so callers (tests) can still reach HttpServer for the bound address.
    Layer.provideMerge([NodeHttpServer.layer(() => Http.createServer(), { port: options.port }), SyncRpc.layerJson()])
  )
}
