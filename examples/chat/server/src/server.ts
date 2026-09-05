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
import * as EphemeralHub from "@lucas-barake/effect-local-rpc/EphemeralHub"
import * as PrincipalAssertion from "@lucas-barake/effect-local-rpc/PrincipalAssertion"
import * as SpaceEntity from "@lucas-barake/effect-local-rpc/SpaceEntity"
import * as SyncRpc from "@lucas-barake/effect-local-rpc/SyncRpc"
import * as SyncServer from "@lucas-barake/effect-local-rpc/SyncServer"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest"
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
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

class ChatAuthorizationError extends Schema.TaggedErrorClass<ChatAuthorizationError, Schema.JsonObject>(
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

const migration = { retryDelay: "100 millis", maximumAttempts: 8 } as const

const layerRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(layerMutations))

const makeLayerDatabase = (databaseFile: string) =>
  Layer.mergeAll(
    SqliteClient.layer({ filename: databaseFile }),
    NodeCrypto.layer,
    Reactivity.layer
  )

const makeLayerStore = (layerDatabase: ReturnType<typeof makeLayerDatabase>) =>
  ServerStore.layer({
    definition,
    migration,
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
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    pruneBatchSize: 1_000,
    retainedSnapshots: 2,
    maintenanceConcurrency: 1,
    maintenanceSpaceBatchSize: 128,
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
    authorizeRead: ({ principal }) => Effect.asVoid(decodePrincipal(principal))
  }).pipe(
    Layer.provide(layerRuntime),
    Layer.provide(layerDatabase)
  )

const layerEphemeralHub = EphemeralHub.layer({
  maximumWatchersPerSpace: 1_024,
  // NOTE: the authorization input carries `{ spaceId, member, principal }` but
  // not the published value, so ephemeral identity (presence/typing userId)
  // stays client-asserted in this example. Durable mutations ARE principal-
  // bound via authorizeMutation above.
  authorize: ({ principal }) =>
    decodePrincipal(principal).pipe(
      Effect.mapError((error) => new ReplicaError.AuthorizationDenied({ reason: error.reason })),
      Effect.asVoid
    )
}).pipe(Layer.provide(NodeCrypto.layer))

/** The full server composition. Launch with `Layer.launch` or `Layer.unwrap`-based test harnesses. */
export const makeServerLayer = (options: ChatServerOptions) => {
  const layerDatabase = makeLayerDatabase(options.databaseFile)
  const layerStore = makeLayerStore(layerDatabase)

  const layerCluster = SpaceEntity.layer({
    admissionMailboxCapacity: 64,
    readMailboxCapacity: 64,
    watchMailboxCapacity: 64,
    ephemeralJoinMailboxCapacity: 64,
    ephemeralCommandMailboxCapacity: 64,
    maximumConcurrentBootstrapAuthorizations: 16,
    maximumConcurrentBootstrapPagesPerSpace: 4,
    maximumConcurrentEphemeralJoinVerificationsPerSpace: 16,
    maximumConcurrentEphemeralRequestsPerSpace: 16
  }).pipe(
    // Single process: the facade and the entities share one trusted runtime.
    Layer.provide(PrincipalAssertion.layerJson),
    Layer.provide(layerStore),
    Layer.provide(layerEphemeralHub),
    Layer.provide(SingleRunner.layer({ runnerStorage: "memory" }).pipe(Layer.provide(layerDatabase)))
  )

  const layerMaintenance = ServerStore.layerMaintenance({ interval: "1 hour", runOnStart: true }).pipe(
    Layer.provide(layerStore)
  )

  // The login route and the RPC websocket upgrade share ONE router instance:
  // a separately built router would 404 /login.
  const layerApp = Layer.mergeAll(
    SyncServer.layerProtocolWebSocket({ path: "/sync" }),
    layerLoginRoute
  ).pipe(Layer.provide(HttpRouter.layer))

  return Layer.mergeAll(
    SyncServer.layer.pipe(
      Layer.provideMerge(layerApp),
      Layer.provide(layerCluster),
      Layer.provide(Authentication.layerServer.pipe(Layer.provide(layerAuthenticator))),
      Layer.provide(PrincipalAssertion.layerJson),
      Layer.provide(HttpRouter.serve(layerApp, { disableLogger: true }))
    ),
    layerMaintenance
  ).pipe(
    // provideMerge so callers (tests) can still reach HttpServer for the bound address.
    Layer.provideMerge([NodeHttpServer.layer(() => Http.createServer(), { port: options.port }), SyncRpc.layerJson()])
  )
}
