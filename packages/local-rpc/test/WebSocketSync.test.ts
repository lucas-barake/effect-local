import { NodeCrypto, NodeHttpServer, NodeSocket } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as AttachmentServer from "@lucas-barake/effect-local-sql/AttachmentServer"
import * as AttachmentTransferService from "@lucas-barake/effect-local-sql/AttachmentTransfer"
import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as AttachmentTransfer from "@lucas-barake/effect-local/AttachmentTransfer"
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
import * as Encoding from "effect/Encoding"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as MutableRef from "effect/MutableRef"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Redacted from "effect/Redacted"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as TestClock from "effect/testing/TestClock"
import * as SingleRunner from "effect/unstable/cluster/SingleRunner"
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServer from "effect/unstable/http/HttpServer"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as Socket from "effect/unstable/socket/Socket"

const failureOf = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.result,
    Effect.map((result) => {
      if (Result.isFailure(result)) return result.failure
      return assert.fail("expected Effect failure")
    })
  )

class TestAuthorizationError extends Schema.TaggedErrorClass<TestAuthorizationError, Schema.JsonObject>(
  "@lucas-barake/effect-local-rpc/test/WebSocketSync/TestAuthorizationError"
)("TestAuthorizationError", { reason: Schema.String }) {}
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as AttachmentDirectHttpClient from "../src/AttachmentDirectHttpClient.js"
import * as AttachmentRpcClient from "../src/AttachmentRpcClient.js"
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
const layerHandlers = Layer.mergeAll(
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload))),
  ReturnHugeResult.toLayer(() => Effect.succeed("x".repeat(SyncRpc.maximumFrameBytes))),
  AssignRoleV2.toLayer(({ payload }) => Effect.succeed(payload.role))
)
const layerRuntime = MutationRuntime.layer(definition, evolution).pipe(Layer.provide(layerHandlers))
const readAuthorized = MutableRef.make(true)
const layerDatabase = Layer.mergeAll(
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
  maximumPendingReadAuthorizations: 4_096,
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
  attachmentControlMailboxCapacity: 1,
  maximumConcurrentBootstrapAuthorizations: 16,
  maximumConcurrentBootstrapPagesPerSpace: 4,
  maximumConcurrentPresencePublicationsPerSpace: 16,
  maximumConcurrentAttachmentControlsPerSpace: 1
} satisfies SpaceEntity.HandlerOptions
const clientHistory = {
  defaultScope: scope,
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
  retainedReceipts: 256,
  settlementCapacity: 64,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration
}

const layerStore = ServerStore.layer({
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
    return Effect.fail(new TestAuthorizationError({ reason: "forbidden" }))
  },
  authorizeMutation: ({ mutation }) => {
    if (Schema.is(AssignRoleV2.payloadSchema)(mutation.payload) && mutation.payload.role === "admin") {
      return Effect.fail(new TestAuthorizationError({ reason: "admin role requires elevated access" }))
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
    return Effect.fail(new TestAuthorizationError({ reason: "forbidden" }))
  }
}).pipe(Layer.provide(layerRuntime), Layer.provide(layerDatabase))

const layerAuthenticator = Layer.succeed(
  Authentication.Authenticator,
  Authentication.Authenticator.of({
    authenticate: (credential) => {
      if (Redacted.value(credential) === "secret") return Effect.succeed({ subject: "test" })
      if (Redacted.value(credential) === "revoked") return Effect.succeed({ subject: "revoked" })
      return Effect.fail(new ReplicaError.CredentialRejected())
    }
  })
)
const layerAuthenticationServer = Authentication.layerServer.pipe(Layer.provide(layerAuthenticator))
const assertionCodec = Schema.fromJsonString(Schema.Json)
const layerAssertionIssuer = PrincipalAssertion.layerIssuer((principal) =>
  Schema.encodeUnknownEffect(assertionCodec)(principal).pipe(
    Effect.map((assertion) => PrincipalAssertion.PrincipalAssertion.make(assertion)),
    Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "could not issue principal assertion" }))
  )
)
const layerAssertionVerifier = PrincipalAssertion.layerVerifier((assertion) =>
  Schema.decodeUnknownEffect(assertionCodec)(assertion).pipe(
    Effect.mapError(() => new ReplicaError.AuthorizationDenied({ reason: "invalid principal assertion" }))
  )
)
const authenticationClientProvider = Authentication.CredentialProvider.of({
  acquire: Effect.succeed({ generation: 0, bearer: Redacted.make("secret") }),
  awaitChange: () => Effect.never
})
const layerAuthenticationClient = Layer.fresh(Authentication.layerClient).pipe(Layer.provide(
  Layer.succeed(
    Authentication.CredentialProvider,
    authenticationClientProvider
  )
))
const attachmentPrincipals = MutableRef.make<Array<Schema.Json>>([])
const attachmentUploaded = MutableRef.make(false)
const attachmentFinalized = MutableRef.make(false)
const attachmentProviderBytes = MutableRef.make<Array<number>>([])
const attachmentFailAfterUpload = MutableRef.make(false)
const attachmentExpireUploadGrant = MutableRef.make(false)
const attachmentDownloadStarts = MutableRef.make(0)
const attachmentPrepareEntries = MutableRef.make(0)
const attachmentPrepareEntered = MutableRef.make<Deferred.Deferred<void> | undefined>(undefined)
const attachmentPrepareGate = MutableRef.make<Deferred.Deferred<void> | undefined>(undefined)
const attachmentGrantOffset = MutableRef.make(0)
const attachmentGrantBytes = MutableRef.make<number | undefined>(undefined)
const attachmentAttemptId = AttachmentTransfer.AttemptId.make("test-attempt")
const authorizeAttachment = Effect.fnUntraced(function*(principal: Schema.Json) {
  MutableRef.update(attachmentPrincipals, (principals) => [...principals, principal])
  if (
    principal === null || typeof principal !== "object" || Array.isArray(principal) ||
    !("subject" in principal) || principal.subject !== "test"
  ) yield* new ReplicaError.AuthorizationDenied({ reason: { _tag: "Forbidden" } })
})
const layerAttachmentServer = Layer.succeed(
  AttachmentServer.AttachmentServer,
  AttachmentServer.AttachmentServer.of({
    prepareUpload: (input) =>
      authorizeAttachment(input.principal).pipe(
        Effect.tap(() =>
          Effect.suspend(() => {
            MutableRef.update(attachmentPrepareEntries, (count) => count + 1)
            const entered = MutableRef.get(attachmentPrepareEntered)
            const gate = MutableRef.get(attachmentPrepareGate)
            let wait = Effect.void
            if (gate !== undefined) wait = Deferred.await(gate)
            if (entered === undefined) return wait
            return Deferred.succeed(entered, undefined).pipe(Effect.andThen(wait))
          })
        ),
        Effect.map(() => {
          if (MutableRef.get(attachmentFinalized)) return AttachmentTransfer.UploadComplete.make({})
          if (MutableRef.get(attachmentUploaded)) {
            return AttachmentTransfer.UploadReady.make({ attemptId: attachmentAttemptId })
          }
          let expiresAt = Number.MAX_SAFE_INTEGER
          if (MutableRef.get(attachmentExpireUploadGrant)) {
            MutableRef.set(attachmentExpireUploadGrant, false)
            expiresAt = 0
          }
          return AttachmentTransfer.UploadPart.make({
            attemptId: attachmentAttemptId,
            partNumber: 1,
            offset: MutableRef.get(attachmentGrantOffset),
            bytes: MutableRef.get(attachmentGrantBytes) ?? input.reference.bytes,
            expiresAt,
            request: AttachmentTransfer.DirectUploadRequest.make({
              method: "PUT",
              url: AttachmentTransfer.GrantUrl.make("https://provider.example/upload?secret=sentinel"),
              headers: []
            })
          })
        })
      ),
    finalizeUpload: (input) =>
      authorizeAttachment(input.principal).pipe(Effect.flatMap(() => {
        if (input.attemptId !== attachmentAttemptId || !MutableRef.get(attachmentUploaded)) {
          return Effect.die("invalid test upload state")
        }
        MutableRef.set(attachmentFinalized, true)
        return Effect.succeed(AttachmentTransfer.UploadComplete.make({}))
      })),
    prepareDownload: (input) =>
      authorizeAttachment(input.principal).pipe(Effect.as(
        AttachmentTransfer.DownloadGrant.make({
          grantId: AttachmentTransfer.GrantId.make("test-download-grant"),
          objectVersion: AttachmentTransfer.ObjectVersion.make("test-object-version"),
          objectBytes: input.reference.bytes,
          expiresAt: Number.MAX_SAFE_INTEGER,
          chunk: AttachmentTransfer.VerifiedChunk.make({
            index: 0,
            offset: 0,
            bytes: input.reference.bytes,
            digest: input.reference.digest
          }),
          slice: { offset: input.range?.offset ?? 0, length: input.range?.length ?? input.reference.bytes },
          request: AttachmentTransfer.DirectDownloadRequest.make({
            method: "GET",
            url: AttachmentTransfer.GrantUrl.make("https://provider.example/download?secret=sentinel"),
            headers: []
          })
        })
      )),
    replaceEntityReferences: () => Effect.void,
    activateGeneration: () => Effect.void,
    expireGrants: Effect.void,
    sweepSpace: () => Effect.succeed(0),
    drainOutbox: Effect.succeed(0),
    maintain: () => Effect.succeed(0)
  })
)
const providerFetch: typeof globalThis.fetch = (input, init) => {
  const request = new Request(input, init)
  if (request.method === "PUT") {
    return request.arrayBuffer().then((buffer) => {
      MutableRef.set(attachmentProviderBytes, Array.from(new Uint8Array(buffer)))
      MutableRef.set(attachmentUploaded, true)
      if (MutableRef.get(attachmentFailAfterUpload)) {
        MutableRef.set(attachmentFailAfterUpload, false)
        // oxlint-disable-next-line effect/noNewPromise -- Fetch is the native provider boundary and must reject with a Promise.
        return Promise.reject("provider upload interrupted")
      }
      return new Response(null, { status: 200 })
    })
  }
  MutableRef.update(attachmentDownloadStarts, (count) => count + 1)
  const bytes = Uint8Array.from(MutableRef.get(attachmentProviderBytes))
  // oxlint-disable-next-line effect/noNewPromise -- Fetch is the native provider boundary and must return a Promise.
  return Promise.resolve(
    new Response(bytes, {
      status: 200,
      headers: { "content-length": String(bytes.byteLength) }
    })
  )
}
const layerDirectAttachmentClient = AttachmentDirectHttpClient.layer({
  uploadOrigins: ["https://provider.example"],
  downloadOrigins: ["https://provider.example"],
  insecureDevelopmentOrigins: []
}).pipe(
  Layer.provide(FetchHttpClient.layer),
  Layer.provide(Layer.succeed(FetchHttpClient.Fetch, providerFetch))
)
const revokedAuthenticationClientProvider = Authentication.CredentialProvider.of({
  acquire: Effect.succeed({ generation: 0, bearer: Redacted.make("revoked") }),
  awaitChange: () => Effect.never
})
const layerRevokedAuthenticationClient = Layer.fresh(Authentication.layerClient).pipe(Layer.provide(
  Layer.succeed(
    Authentication.CredentialProvider,
    revokedAuthenticationClientProvider
  )
))
const rejectedAuthenticationClientProvider = Authentication.CredentialProvider.of({
  acquire: Effect.succeed({ generation: 4, bearer: Redacted.make("rejected") }),
  awaitChange: () => Effect.never
})
const layerRejectedAuthenticationClient = Layer.fresh(Authentication.layerClient).pipe(Layer.provide(
  Layer.succeed(Authentication.CredentialProvider, rejectedAuthenticationClientProvider)
))
const layerCluster = SpaceEntity.layer(entityOptions).pipe(
  Layer.provideMerge(layerAttachmentServer),
  Layer.provide(layerAssertionVerifier),
  Layer.provide(layerStore),
  Layer.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })),
  Layer.provide(SingleRunner.layer({ runnerStorage: "memory" }).pipe(Layer.provide(layerDatabase)))
)

const layerWebsocketProtocol = SyncServer.layerProtocolWebSocket({ path: "/sync" }).pipe(
  Layer.provide(HttpRouter.layer)
)
const layerWebsocketServer = SyncServer.layer.pipe(
  Layer.provideMerge(layerWebsocketProtocol),
  Layer.provide(layerCluster),
  Layer.provide(layerAuthenticationServer),
  Layer.provide(layerAssertionIssuer),
  Layer.provide(HttpRouter.serve(layerWebsocketProtocol, { disableListenLog: true, disableLogger: true }))
)
const webSocketConstructions = MutableRef.make(0)
const liveWebSockets = MutableRef.make(0)
interface ObservedWebSocketFrame {
  readonly bytes: Uint8Array | undefined
  readonly byteLength: number
}
const clientWebSocketFrames = MutableRef.make<Array<ObservedWebSocketFrame>>([])
const serverWebSocketFrames = MutableRef.make<Array<ObservedWebSocketFrame>>([])
const observeWebSocketFrame = (frames: MutableRef.MutableRef<Array<ObservedWebSocketFrame>>, data: unknown) => {
  if (typeof data === "string") {
    const bytes = new TextEncoder().encode(data)
    MutableRef.update(frames, (observed) => [...observed, { bytes, byteLength: bytes.byteLength }])
    return
  }
  if (data instanceof ArrayBuffer) {
    const bytes = Uint8Array.from(new Uint8Array(data))
    MutableRef.update(frames, (observed) => [...observed, { bytes, byteLength: bytes.byteLength }])
    return
  }
  if (ArrayBuffer.isView(data)) {
    const bytes = Uint8Array.from(new Uint8Array(data.buffer, data.byteOffset, data.byteLength))
    MutableRef.update(frames, (observed) => [...observed, { bytes, byteLength: bytes.byteLength }])
    return
  }
  if (data instanceof Blob) {
    MutableRef.update(frames, (observed) => [...observed, { bytes: undefined, byteLength: data.size }])
    return
  }
  MutableRef.update(frames, (observed) => [...observed, { bytes: undefined, byteLength: 0 }])
}
const countedWebSocketConstructor = Effect.gen(function*() {
  const makeWebSocket = yield* Socket.WebSocketConstructor
  return (url: string, protocols?: string | Array<string>) => {
    MutableRef.update(webSocketConstructions, (count) => count + 1)
    MutableRef.update(liveWebSockets, (count) => count + 1)
    const webSocket = makeWebSocket(url, protocols)
    const send = webSocket.send.bind(webSocket)
    webSocket.send = (data) => {
      observeWebSocketFrame(clientWebSocketFrames, data)
      send(data)
    }
    webSocket.binaryType = "arraybuffer"
    webSocket.addEventListener("message", (event) => {
      observeWebSocketFrame(serverWebSocketFrames, event.data)
    })
    webSocket.addEventListener("close", () => {
      MutableRef.update(liveWebSockets, (count) => count - 1)
    }, { once: true })
    return webSocket
  }
})
const layerCountedConstructor = Layer.effect(
  Socket.WebSocketConstructor,
  countedWebSocketConstructor
).pipe(
  Layer.provide(NodeSocket.layerWebSocketConstructor)
)
const layerSocket = Effect.gen(function*() {
  const server = yield* HttpServer.HttpServer
  const address = server.address
  if (address._tag === "UnixAddress") return yield* Effect.die("Expected the test HTTP server to use a TCP address")
  return yield* Socket.makeWebSocket(`http://127.0.0.1:${address.port}/sync`)
}).pipe(Layer.effect(Socket.Socket), Layer.provide(layerCountedConstructor))
const layerClientProtocol = SyncClient.layerProtocolSocket().pipe(Layer.provide(layerSocket))
const layerAttachmentRpcClient = AttachmentRpcClient.layerFromSession().pipe(
  Layer.provide(ProtocolSession.layer),
  Layer.provide(layerDirectAttachmentClient)
)
const layerClient = Layer.mergeAll(
  SyncClient.layer,
  PresenceClient.layer,
  ProtocolSession.layer,
  layerAttachmentRpcClient
).pipe(
  Layer.provide(layerClientProtocol),
  Layer.provide(layerAuthenticationClient)
)
class RevokedSyncEngine extends Context.Service<RevokedSyncEngine, SyncEngine.Service>()(
  "@lucas-barake/effect-local-rpc/test/RevokedSyncEngine"
) {}
class RevokedAttachmentTransfer extends Context.Service<
  RevokedAttachmentTransfer,
  AttachmentTransferService.Service
>()("@lucas-barake/effect-local-rpc/test/RevokedAttachmentTransfer") {}
class RejectedAttachmentTransfer extends Context.Service<
  RejectedAttachmentTransfer,
  AttachmentTransferService.Service
>()("@lucas-barake/effect-local-rpc/test/RejectedAttachmentTransfer") {}
const layerRevokedClient = Layer.effect(RevokedSyncEngine, SyncEngine.SyncEngine).pipe(
  Layer.provide(Layer.fresh(SyncClient.layer)),
  Layer.provide(Layer.fresh(layerClientProtocol)),
  Layer.provide(layerRevokedAuthenticationClient)
)
const attachmentTransferForAuthentication = (
  authentication: typeof layerRevokedAuthenticationClient
) =>
  AttachmentRpcClient.layerFromSession().pipe(
    Layer.provide(Layer.fresh(ProtocolSession.layer)),
    Layer.provide(layerDirectAttachmentClient),
    Layer.provide(Layer.fresh(layerClientProtocol)),
    Layer.provide(authentication)
  )
const layerRevokedAttachmentTransfer = Layer.effect(
  RevokedAttachmentTransfer,
  AttachmentTransferService.AttachmentTransfer
).pipe(Layer.provide(attachmentTransferForAuthentication(layerRevokedAuthenticationClient)))
const layerRejectedAttachmentTransfer = Layer.effect(
  RejectedAttachmentTransfer,
  AttachmentTransferService.AttachmentTransfer
).pipe(Layer.provide(attachmentTransferForAuthentication(layerRejectedAuthenticationClient)))
const layerLive = Layer.mergeAll(
  layerClient,
  layerRevokedClient,
  layerRevokedAttachmentTransfer,
  layerRejectedAttachmentTransfer
).pipe(
  Layer.provideMerge(layerWebsocketServer),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)
const layerSingleClientLive = layerClient.pipe(
  Layer.provideMerge(layerWebsocketServer),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)

const layerIncompatibleServer = SyncServer.layerWithOptions({ supportedProtocolVersions: [2] }).pipe(
  Layer.provideMerge(layerWebsocketProtocol),
  Layer.provide(layerCluster),
  Layer.provide(layerAuthenticationServer),
  Layer.provide(layerAssertionIssuer),
  Layer.provide(HttpRouter.serve(layerWebsocketProtocol, { disableListenLog: true, disableLogger: true }))
)
const layerIncompatibleClient = SyncClient.layerWithOptions({ supportedProtocolVersions: [1] }).pipe(
  Layer.provide(layerClientProtocol),
  Layer.provide(layerAuthenticationClient)
)
const layerIncompatibleLive = layerIncompatibleClient.pipe(
  Layer.provideMerge(layerIncompatibleServer),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)
const layerInvalidProtocolClient = SyncClient.layerWithOptions({ supportedProtocolVersions: [] }).pipe(
  Layer.provide(layerClientProtocol),
  Layer.provide(layerAuthenticationClient)
)
const layerInvalidProtocolLive = layerInvalidProtocolClient.pipe(
  Layer.provideMerge(layerWebsocketServer),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)

type ProtocolObservation =
  | { readonly _tag: "Negotiate" }
  | { readonly _tag: "Pull"; readonly version: Protocol.ProtocolVersion }
  | { readonly _tag: "PublishPresence"; readonly version: Protocol.ProtocolVersion }

const protocolObservations = MutableRef.make<Array<ProtocolObservation>>([])
const observingAuthentication = Effect.gen(function*() {
  const authenticate = yield* Authentication.Authentication
  return Authentication.Authentication.of((effect, options) =>
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
})
const layerObservingAuthenticationServer = Layer.effect(
  Authentication.Authentication,
  observingAuthentication
).pipe(
  Layer.provide(layerAuthenticationServer)
)
const layerProtocol2Server = SyncServer.layerWithOptions({ supportedProtocolVersions: [1, 2] }).pipe(
  Layer.provideMerge(layerWebsocketProtocol),
  Layer.provide(layerCluster),
  Layer.provide(layerObservingAuthenticationServer),
  Layer.provide(layerAssertionIssuer),
  Layer.provide(HttpRouter.serve(layerWebsocketProtocol, { disableListenLog: true, disableLogger: true }))
)
const layerConfigurableProtocolSession = ProtocolSession.layerWithOptions({ supportedProtocolVersions: [1, 2] })
const layerConfigurableClient = Layer.merge(SyncClient.layerFromSession(), PresenceClient.layerFromSession()).pipe(
  Layer.provide(layerConfigurableProtocolSession),
  Layer.provide(layerClientProtocol),
  Layer.provide(layerAuthenticationClient)
)
const layerConfigurableLive = layerConfigurableClient.pipe(
  Layer.provideMerge(layerProtocol2Server),
  Layer.provide([NodeHttpServer.layerTest, SyncRpc.layerJson()])
)
const layerBootstrapDependencies = Layer.mergeAll(layerLive, layerStore, layerDatabase)
const layerRetryDependencies = Layer.merge(layerLive, layerDatabase)
const provideBootstrapDependencies = Effect.provide(layerBootstrapDependencies)
const provideConfigurableLive = Effect.provide(layerConfigurableLive)
const provideIncompatibleLive = Effect.provide(layerIncompatibleLive)
const provideLive = Effect.provide(layerLive)
const provideNodeCrypto = Effect.provide(NodeCrypto.layer)
const provideRetryDependencies = Effect.provide(layerRetryDependencies)
const restoreReadAuthorization = Effect.ensuring(Effect.sync(() => MutableRef.set(readAuthorized, true)))

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

const makeLifecycleHarness = Effect.fnUntraced(function*(options?: {
  readonly rpcTimeout?: Duration.Input
  readonly sessionAcquisitionTimeout?: Duration.Input
}) {
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
  const layerClientAuthentication = Authentication.layerClient.pipe(
    Layer.provide(Layer.succeed(Authentication.CredentialProvider, provider))
  )
  const layerDynamicAuthenticator = Layer.succeed(
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
  const layerBaseAuthenticationServer = Authentication.layerServer.pipe(Layer.provide(layerDynamicAuthenticator))
  const observedAuthentication = Effect.gen(function*() {
    const authenticate = yield* Authentication.Authentication
    return Authentication.Authentication.of((effect, request) =>
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
  })
  const layerObservedAuthenticationServer = Layer.effect(
    Authentication.Authentication,
    observedAuthentication
  ).pipe(
    Layer.provide(layerBaseAuthenticationServer)
  )

  const layerLifecycleHandlers = Layer.mergeAll(
    PutTodo.toLayer(({ payload, transaction }) =>
      transaction.set(Todo, payload.id, payload).pipe(
        Effect.tap(() => Queue.offer(applications, payload.id)),
        Effect.as(payload)
      )
    ),
    ReturnHugeResult.toLayer(() => Effect.succeed("x".repeat(SyncRpc.maximumFrameBytes))),
    AssignRoleV2.toLayer(({ payload }) => Effect.succeed(payload.role))
  )
  const layerLifecycleRuntime = MutationRuntime.layer(definition, evolution).pipe(Layer.provide(layerLifecycleHandlers))
  const layerLifecycleStore = ServerStore.layer({
    ...serverHistory,
    definition,
    evolution,
    authorizeAccess: ({ principal, spaceId: requestedSpaceId }) => {
      if (
        principal !== null && typeof principal === "object" && !Array.isArray(principal) &&
        "subject" in principal && principal.subject === "test" && requestedSpaceId === spaceId
      ) return Effect.void
      return Effect.fail(new TestAuthorizationError({ reason: "forbidden" }))
    },
    authorizeMutation: () => Effect.void,
    authorizeRead: () => {
      if (!MutableRef.get(blockPull)) return Effect.void
      return Deferred.succeed(pullEntered, undefined).pipe(
        Effect.andThen(Deferred.await(pullRelease)),
        Effect.ensuring(Deferred.succeed(pullInterrupted, undefined))
      )
    }
  }).pipe(Layer.provide(layerLifecycleRuntime), Layer.provide(layerDatabase))
  const layerLifecycleCluster = SpaceEntity.layer(entityOptions).pipe(
    Layer.provide(layerAssertionVerifier),
    Layer.provide(layerLifecycleStore),
    Layer.provide(PresenceHub.layerTrusted({ maximumWatchersPerSpace: 1_024 })),
    Layer.provide(SingleRunner.layer({ runnerStorage: "memory" }).pipe(Layer.provide(layerDatabase)))
  )
  const layerLifecycleWebSocketProtocol = SyncServer.layerProtocolWebSocket({ path: "/sync" }).pipe(
    Layer.provide(HttpRouter.layer)
  )
  const layerLifecycleServer = SyncServer.layer.pipe(
    Layer.provideMerge(layerLifecycleWebSocketProtocol),
    Layer.provide(layerLifecycleCluster),
    Layer.provide(layerObservedAuthenticationServer),
    Layer.provide(layerAssertionIssuer),
    Layer.provide(HttpRouter.serve(layerLifecycleWebSocketProtocol, {
      disableListenLog: true,
      disableLogger: true
    }))
  )
  const lifecycleWebSocketConstructor = Effect.gen(function*() {
    const makeWebSocket = yield* Socket.WebSocketConstructor
    return (
      url: string,
      protocols?: string | Array<string>
    ) => {
      MutableRef.update(lifecycleWebSocketConstructions, (count) => count + 1)
      return makeWebSocket(url, protocols)
    }
  })
  const layerLifecycleConstructor = Layer.effect(
    Socket.WebSocketConstructor,
    lifecycleWebSocketConstructor
  ).pipe(
    Layer.provide(NodeSocket.layerWebSocketConstructor)
  )
  const layerLifecycleSocket = Effect.gen(function*() {
    const server = yield* HttpServer.HttpServer
    const address = server.address
    if (address._tag === "UnixAddress") return yield* Effect.die("Expected a TCP test server")
    return yield* Socket.makeWebSocket(`http://127.0.0.1:${address.port}/sync`)
  }).pipe(Layer.effect(Socket.Socket), Layer.provide(layerLifecycleConstructor))
  const layerLifecycleClientProtocol = SyncClient.layerProtocolSocket().pipe(Layer.provide(layerLifecycleSocket))
  const layerLifecycleClient = SyncClient.layerWithOptions(options).pipe(
    Layer.provide(layerLifecycleClientProtocol),
    Layer.provide(layerClientAuthentication)
  )
  const layerLifecycleLive = layerLifecycleClient.pipe(
    Layer.provideMerge(layerLifecycleServer),
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
      Layer.provide(layerLifecycleHandlers),
      Layer.provideMerge(layerDatabase),
      Layer.provide(layerLifecycleLive)
    )

  return {
    applications,
    attempts,
    blockPull,
    credentials,
    layerLive: layerLifecycleLive,
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
  it.effect(
    "keeps upload and lazy download media bytes off actual application WebSocket frames",
    Effect.fnUntraced(function*() {
      MutableRef.set(attachmentPrincipals, [])
      MutableRef.set(attachmentUploaded, false)
      MutableRef.set(attachmentFinalized, false)
      MutableRef.set(attachmentProviderBytes, [])
      MutableRef.set(attachmentFailAfterUpload, false)
      MutableRef.set(attachmentExpireUploadGrant, false)
      MutableRef.set(attachmentGrantOffset, 0)
      MutableRef.set(attachmentGrantBytes, undefined)
      MutableRef.set(attachmentDownloadStarts, 0)
      MutableRef.set(clientWebSocketFrames, [])
      MutableRef.set(serverWebSocketFrames, [])
      const transfer = yield* AttachmentTransferService.AttachmentTransfer
      const mediaText = "upload-and-download-media-byte-sentinel"
      const mediaPrefix = new TextEncoder().encode(mediaText)
      const bytes = new Uint8Array(AttachmentTransfer.maximumControlBytes + 1_024)
      bytes.set(mediaPrefix)
      for (let index = mediaPrefix.byteLength; index < bytes.byteLength; index++) {
        bytes[index] = index % 251
      }
      const representativePrefix = bytes.subarray(0, 24)
      const byteArray = Schema.Array(Schema.Int)
      const byteArrayJson = Schema.fromJsonString(byteArray)
      const jsonNumericPrefix = yield* Schema.encodeEffect(byteArrayJson)(Array.from(representativePrefix))
      const base64Prefix = Encoding.encodeBase64(representativePrefix)
      const reference = Attachment.Reference.make({
        _tag: "Attachment",
        digest: Attachment.Digest.make(`sha256:${"4".repeat(64)}`),
        bytes: bytes.byteLength
      })

      yield* transfer.upload({
        spaceId,
        clientId,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        reference,
        bytes: (offset) => Stream.make(bytes.subarray(offset))
      })
      const prepared = yield* transfer.download({
        spaceId,
        clientId,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        reference
      })
      assert.strictEqual(MutableRef.get(attachmentDownloadStarts), 0)
      const downloaded = yield* Stream.runCollect(prepared.bytes)

      assert.deepStrictEqual(MutableRef.get(attachmentProviderBytes), Array.from(bytes))
      assert.deepStrictEqual(Array.from(downloaded).flatMap((chunk) => Array.from(chunk)), Array.from(bytes))
      assert.isAbove(bytes.byteLength, AttachmentTransfer.maximumControlBytes)
      assert.strictEqual(MutableRef.get(attachmentDownloadStarts), 1)
      assert.isTrue(MutableRef.get(attachmentFinalized))
      assert.deepStrictEqual(MutableRef.get(attachmentPrincipals), [
        { subject: "test" },
        { subject: "test" },
        { subject: "test" },
        { subject: "test" }
      ])
      const clientFrames = MutableRef.get(clientWebSocketFrames)
      const serverFrames = MutableRef.get(serverWebSocketFrames)
      assert.isNotEmpty(clientFrames)
      assert.isNotEmpty(serverFrames)
      for (const frame of [...clientFrames, ...serverFrames]) {
        assert.isAtMost(frame.byteLength, AttachmentTransfer.maximumControlBytes)
        if (frame.bytes === undefined) assert.fail("expected the JSON RPC transport to use text frames")
        const encodedFrame = new TextDecoder().decode(frame.bytes)
        assert.notInclude(encodedFrame, mediaText)
        assert.notInclude(encodedFrame, jsonNumericPrefix)
        assert.notInclude(encodedFrame, base64Prefix)
      }
    }, provideLive)
  )

  it.effect(
    "reads exactly the provider granted local byte range",
    Effect.fnUntraced(function*() {
      MutableRef.set(attachmentUploaded, false)
      MutableRef.set(attachmentFinalized, false)
      MutableRef.set(attachmentProviderBytes, [])
      MutableRef.set(attachmentFailAfterUpload, false)
      MutableRef.set(attachmentExpireUploadGrant, false)
      MutableRef.set(attachmentGrantOffset, 1)
      MutableRef.set(attachmentGrantBytes, 2)
      const transfer = yield* AttachmentTransferService.AttachmentTransfer
      const bytes = Uint8Array.from([31, 32, 33, 34])
      const reference = Attachment.Reference.make({
        _tag: "Attachment",
        digest: Attachment.Digest.make(`sha256:${"c".repeat(64)}`),
        bytes: bytes.byteLength
      })

      yield* transfer.upload({
        spaceId,
        clientId,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        reference,
        bytes: (offset) => Stream.make(bytes.subarray(offset))
      })

      assert.deepStrictEqual(MutableRef.get(attachmentProviderBytes), [32, 33])
      MutableRef.set(attachmentGrantOffset, 0)
      MutableRef.set(attachmentGrantBytes, undefined)
    }, provideLive)
  )

  it.effect(
    "bounds attachment control concurrency and rejects mailbox overflow per space",
    Effect.fnUntraced(function*() {
      MutableRef.set(attachmentPrepareEntries, 0)
      const entered = yield* Deferred.make<void>()
      const gate = yield* Deferred.make<void>()
      MutableRef.set(attachmentPrepareEntered, entered)
      MutableRef.set(attachmentPrepareGate, gate)
      const results = yield* Queue.unbounded<Result.Result<AttachmentTransfer.PrepareUploadResult, unknown>>()
      const session = yield* ProtocolSession.ProtocolSession
      const protocolVersion = yield* session.version
      const reference = Attachment.Reference.make({
        _tag: "Attachment",
        digest: Attachment.Digest.make(`sha256:${"a".repeat(64)}`),
        bytes: 3
      })
      const request = {
        spaceId,
        clientId,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        reference,
        protocolVersion
      }
      const calls = yield* Effect.forEach(
        [1, 2, 3],
        () =>
          session.client.PrepareAttachmentUpload(request).pipe(
            Effect.result,
            Effect.flatMap((result) => Queue.offer(results, result))
          ),
        { concurrency: "unbounded", discard: true }
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(entered)
      const overflow = yield* Queue.take(results)
      const enteredCount = MutableRef.get(attachmentPrepareEntries)
      yield* Deferred.succeed(gate, undefined)
      MutableRef.set(attachmentPrepareEntered, undefined)
      MutableRef.set(attachmentPrepareGate, undefined)
      yield* Fiber.join(calls)

      assert.strictEqual(enteredCount, 1)
      assert.isTrue(Result.isFailure(overflow))
      if (Result.isFailure(overflow)) {
        assert.deepInclude(overflow.failure, { _tag: "ServerUnavailable" })
      }
    }, provideLive)
  )

  it.effect(
    "preserves a typed local source failure when provider upload consumption stops",
    Effect.fnUntraced(function*() {
      MutableRef.set(attachmentUploaded, false)
      MutableRef.set(attachmentFinalized, false)
      MutableRef.set(attachmentFailAfterUpload, false)
      MutableRef.set(attachmentExpireUploadGrant, false)
      const transfer = yield* AttachmentTransferService.AttachmentTransfer
      const reference = Attachment.Reference.make({
        _tag: "Attachment",
        digest: Attachment.Digest.make(`sha256:${"b".repeat(64)}`),
        bytes: 3
      })
      const localFailure = new Attachment.AttachmentStorageError({
        operation: "read",
        cause: "local source failed"
      })

      const failure = yield* transfer.upload({
        spaceId,
        clientId,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        reference,
        bytes: () => Stream.fail(localFailure)
      }).pipe(failureOf)

      assert.strictEqual(failure._tag, "AttachmentStorageError")
    }, provideLive)
  )

  it.effect(
    "resumes from provider authoritative parts after an interrupted upload client",
    Effect.fnUntraced(function*() {
      MutableRef.set(attachmentUploaded, false)
      MutableRef.set(attachmentFinalized, false)
      MutableRef.set(attachmentProviderBytes, [])
      MutableRef.set(attachmentFailAfterUpload, true)
      MutableRef.set(attachmentExpireUploadGrant, false)
      const transfer = yield* AttachmentTransferService.AttachmentTransfer
      const bytes = Uint8Array.from([10, 11, 12])
      const reference = Attachment.Reference.make({
        _tag: "Attachment",
        digest: Attachment.Digest.make(`sha256:${"6".repeat(64)}`),
        bytes: bytes.byteLength
      })
      const request: AttachmentTransferService.UploadRequest = {
        spaceId,
        clientId,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        reference,
        bytes: (offset) => Stream.make(bytes.subarray(offset))
      }

      const interrupted = yield* transfer.upload(request).pipe(failureOf)
      assert.strictEqual(interrupted._tag, "ServerUnavailable")
      yield* transfer.upload(request)

      assert.deepStrictEqual(MutableRef.get(attachmentProviderBytes), [...bytes])
      assert.isTrue(MutableRef.get(attachmentFinalized))
    }, provideLive)
  )

  it.effect(
    "refreshes an expired upload grant before sending provider bytes",
    Effect.fnUntraced(function*() {
      MutableRef.set(attachmentUploaded, false)
      MutableRef.set(attachmentFinalized, false)
      MutableRef.set(attachmentProviderBytes, [])
      MutableRef.set(attachmentFailAfterUpload, false)
      MutableRef.set(attachmentExpireUploadGrant, true)
      const transfer = yield* AttachmentTransferService.AttachmentTransfer
      const bytes = Uint8Array.from([13, 14, 15])
      const reference = Attachment.Reference.make({
        _tag: "Attachment",
        digest: Attachment.Digest.make(`sha256:${"7".repeat(64)}`),
        bytes: bytes.byteLength
      })

      yield* transfer.upload({
        spaceId,
        clientId,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        reference,
        bytes: (offset) => Stream.make(bytes.subarray(offset))
      })

      assert.deepStrictEqual(MutableRef.get(attachmentProviderBytes), [...bytes])
      assert.isFalse(MutableRef.get(attachmentExpireUploadGrant))
    }, provideLive)
  )

  it.effect(
    "authorizes recipient metadata before lazily fetching provider bytes",
    Effect.fnUntraced(function*() {
      MutableRef.set(attachmentDownloadStarts, 0)
      MutableRef.set(attachmentProviderBytes, [21, 22, 23])
      const transfer = yield* AttachmentTransferService.AttachmentTransfer
      const reference = Attachment.Reference.make({
        _tag: "Attachment",
        digest: Attachment.Digest.make(`sha256:${"8".repeat(64)}`),
        bytes: 3
      })

      const prepared = yield* transfer.download({
        spaceId,
        clientId,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        reference
      })
      assert.strictEqual(MutableRef.get(attachmentDownloadStarts), 0)
      const downloaded = yield* Stream.runCollect(prepared.bytes)

      assert.deepStrictEqual(Array.from(downloaded).flatMap((chunk) => Array.from(chunk)), [21, 22, 23])
      assert.strictEqual(MutableRef.get(attachmentDownloadStarts), 1)
      const restarted = yield* Stream.runCollect(prepared.bytes)
      assert.deepStrictEqual(Array.from(restarted).flatMap((chunk) => Array.from(chunk)), [21, 22, 23])
      assert.strictEqual(MutableRef.get(attachmentDownloadStarts), 2)
    }, provideLive)
  )

  it.effect(
    "preserves attachment authorization and credential rejection across control RPC",
    Effect.fnUntraced(function*() {
      const reference = Attachment.Reference.make({
        _tag: "Attachment",
        digest: Attachment.Digest.make(`sha256:${"9".repeat(64)}`),
        bytes: 3
      })
      const request: AttachmentTransferService.UploadRequest = {
        spaceId,
        clientId,
        membershipIncarnation: Identity.legacyMembershipIncarnation,
        reference,
        bytes: () => Stream.make(Uint8Array.from([1, 2, 3]))
      }

      const denied = yield* (yield* RevokedAttachmentTransfer).upload(request).pipe(failureOf)
      assert.strictEqual(denied._tag, "AuthorizationDenied")
      const rejected = yield* (yield* RejectedAttachmentTransfer).upload(request).pipe(failureOf)
      assert.strictEqual(rejected._tag, "CredentialRejected")
      if (rejected._tag === "CredentialRejected") assert.strictEqual(rejected.credentialGeneration, 4)
    }, provideLive)
  )

  it.effect(
    "pauses a rejected credential generation at NeedsAuthentication",
    Effect.fnUntraced(function*() {
      const harness = yield* makeLifecycleHarness()
      const replicaContext = yield* Layer.build(harness.replicaLayer("4 seconds"))
      const replica = Context.get(replicaContext, Replica.Replica)
      const reactivity = Context.get(replicaContext, Reactivity.Reactivity)
      const space = yield* replica.space(spaceId)
      yield* space.activate
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
    })
  )

  it.effect(
    "resumes synchronization after a credential refresh without rebuilding the replica",
    Effect.fnUntraced(function*() {
      const harness = yield* makeLifecycleHarness()
      const replicaContext = yield* Layer.build(harness.replicaLayer("4 seconds"))
      const replica = Context.get(replicaContext, Replica.Replica)
      const reactivity = Context.get(replicaContext, Reactivity.Reactivity)
      const space = yield* replica.space(spaceId)
      yield* space.activate
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
    })
  )

  it.effect(
    "backs off an unavailable authenticator and recovers",
    Effect.fnUntraced(function*() {
      const harness = yield* makeLifecycleHarness()
      const replicaContext = yield* Layer.build(harness.replicaLayer("2 seconds"))
      const replica = Context.get(replicaContext, Replica.Replica)
      const reactivity = Context.get(replicaContext, Reactivity.Reactivity)
      const space = yield* replica.space(spaceId)
      yield* space.activate
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
    })
  )

  it.effect(
    "interrupts a hung RPC at rpcTimeout",
    Effect.fnUntraced(function*() {
      const harness = yield* makeLifecycleHarness({ rpcTimeout: "1 second" })
      yield* Effect.addFinalizer(() => Deferred.succeed(harness.pullRelease, undefined))
      const context = yield* Layer.build(harness.layerLive)
      const remote = Context.get(context, SyncEngine.SyncEngine)
      const request = pullRequest()
      yield* remote.pull(request)
      MutableRef.set(harness.blockPull, true)
      const pulling = yield* remote.pull(request).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.await(harness.pullEntered)

      yield* TestClock.adjust("1 second")
      const error = yield* Fiber.join(pulling).pipe(failureOf)
      assert.strictEqual(error._tag, "OperationTimeout")
      if (error._tag === "OperationTimeout") {
        assert.strictEqual(error.operation, "Pull")
        assert.strictEqual(error.timeoutMillis, 1_000)
      }
      yield* Deferred.await(harness.pullInterrupted)
    })
  )

  it.effect(
    "keeps a healthy idle watch open past rpcTimeout",
    Effect.fnUntraced(function*() {
      const harness = yield* makeLifecycleHarness({ rpcTimeout: "500 millis" })
      const replicaContext = yield* Layer.build(harness.replicaLayer("8 seconds"))
      const replica = Context.get(replicaContext, Replica.Replica)
      const reactivity = Context.get(replicaContext, Reactivity.Reactivity)
      const space = yield* replica.space(spaceId)
      yield* space.activate
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
    })
  )

  it.effect(
    "multiplexes two Replica spaces through exactly one WebSocket",
    Effect.fnUntraced(function*() {
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
          Layer.provide(layerHandlers),
          Layer.provide(layerDatabase),
          Layer.provide(layerSingleClientLive)
        )
      )
      const replica = Context.get(replicaContext, Replica.Replica)
      const first = yield* replica.space(spaceId)
      const second = yield* replica.space(secondSpaceId)
      const [firstPending, secondPending] = yield* Effect.all([
        first.mutate(PutTodo, { id: "shared", title: "first socket" }),
        second.mutate(PutTodo, { id: "shared", title: "second socket" })
      ], { concurrency: "unbounded" })
      const awaitReceipt = Effect.fnUntraced(function*(space: Replica.Space, id: Identity.MutationId) {
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
    }, provideNodeCrypto)
  )

  it.effect("rejects an empty protocol version configuration at layer construction", () =>
    Effect.scoped(Effect.gen(function*() {
      const error = yield* Layer.build(layerInvalidProtocolLive).pipe(failureOf)
      assert.strictEqual(error._tag, "InvalidConfiguration")
    })))

  it.effect(
    "returns UpgradeRequired when protocol versions do not intersect",
    Effect.fnUntraced(function*() {
      const remote = yield* SyncEngine.SyncEngine
      const error = yield* remote.pull(pullRequest()).pipe(failureOf)
      assert.strictEqual(error._tag, "UpgradeRequired")
    }, provideIncompatibleLive)
  )

  it.effect(
    "shares one negotiated protocol version across configurable sync and presence clients",
    Effect.fnUntraced(function*() {
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
    }, provideConfigurableLive)
  )

  it.effect(
    "delivers an authorized bounded bootstrap through both RPC hops",
    Effect.fnUntraced(
      function*() {
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

        const denied = yield* (yield* RevokedSyncEngine).bootstrap(request).pipe(failureOf)
        assert.strictEqual(denied._tag, "AuthorizationDenied")
      },
      provideBootstrapDependencies,
      provideNodeCrypto
    )
  )

  it.effect(
    "reauthorizes exact retries without retaining mutation history in Cluster storage",
    Effect.fnUntraced(
      function*() {
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

        const denied = yield* remote.pull(pullRequest(forbiddenSpaceId)).pipe(failureOf)
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
      },
      provideRetryDependencies,
      provideNodeCrypto
    )
  )

  it.effect(
    "revokes an established watch within the configured authorization window",
    Effect.fnUntraced(
      function*() {
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

        const denied = yield* Fiber.join(watching).pipe(failureOf)
        assert.strictEqual(denied._tag, "AuthorizationDenied")
      },
      restoreReadAuthorization,
      provideLive,
      provideNodeCrypto
    )
  )

  it.effect(
    "authorizes only the migrated mutation payload",
    Effect.fnUntraced(
      function*() {
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
          assert.deepStrictEqual(receipt.rejection, {
            _tag: "TestAuthorizationError",
            reason: "admin role requires elevated access"
          })
        }
      },
      provideLive,
      provideNodeCrypto
    )
  )

  it.effect(
    "multiplexes bounded ephemeral presence over the same WebSocket protocol",
    Effect.fnUntraced(
      function*() {
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
      },
      TestClock.withLive,
      provideLive
    )
  )

  it.effect(
    "returns and replays a bounded terminal rejection for an oversized private result",
    Effect.fnUntraced(
      function*() {
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
      },
      provideLive,
      provideNodeCrypto
    )
  )
})
