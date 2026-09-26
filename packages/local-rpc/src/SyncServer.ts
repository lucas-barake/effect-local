import * as MutationRuntime from "@lucas-barake/effect-local-sql/MutationRuntime"
import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Encoding from "effect/Encoding"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import type * as HttpRouter from "effect/unstable/http/HttpRouter"
import type * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as Authentication from "./Authentication.js"
import * as EphemeralHub from "./EphemeralHub.js"
import { invalidConfiguration } from "./internal/errors.js"
import * as PrincipalAssertion from "./PrincipalAssertion.js"
import * as SpaceEntity from "./SpaceEntity.js"
import * as SyncRpc from "./SyncRpc.js"

const makeHandlers = Effect.fnUntraced(function*(options: {
  readonly supportedProtocolVersions?: ReadonlyArray<number> | undefined
}) {
  const configured = options.supportedProtocolVersions ?? Protocol.supportedProtocolVersions
  const decoded = yield* Schema.decodeUnknownEffect(Protocol.NegotiateRequest)({
    supportedVersions: configured
  }).pipe(
    Effect.mapError(() =>
      invalidConfiguration(
        "supportedProtocolVersions",
        "supportedProtocolVersions must be a nonempty list of positive safe integers"
      )
    )
  )
  const supportedVersions = [...decoded.supportedVersions].toSorted((left, right) => right - left)
  const requireVersion = (version: Protocol.ProtocolVersion) => {
    if (supportedVersions.includes(version)) return Effect.void
    return Effect.fail(new ReplicaError.ProtocolVersionRejected({ version, serverVersions: supportedVersions }))
  }
  const client = yield* SpaceEntity.Client
  const issuer = yield* PrincipalAssertion.Issuer
  const issueAssertion = Authentication.Principal.pipe(Effect.flatMap(issuer.issue))
  return SyncRpc.Rpcs.of({
    Negotiate: ({ supportedVersions: clientVersions }) => {
      const version = supportedVersions.find((candidate) => clientVersions.includes(candidate))
      if (version !== undefined) return Effect.succeed({ version })
      return Effect.fail(new ReplicaError.UpgradeRequired({ clientVersions, serverVersions: supportedVersions }))
    },
    Submit: (request) =>
      requireVersion(request.protocolVersion).pipe(
        Effect.andThen(issueAssertion),
        Effect.flatMap((assertion) => client.submit(request.envelope.spaceId, request, assertion))
      ),
    SubmitBatch: ({ protocolVersion, ...request }) => {
      if (protocolVersion < Protocol.submitBatchProtocolVersion) {
        return Effect.fail(
          new ReplicaError.ProtocolInvalid({
            message: `SubmitBatch requires protocol version ${Protocol.submitBatchProtocolVersion} or later`
          })
        )
      }
      return requireVersion(protocolVersion).pipe(
        Effect.andThen(issueAssertion),
        Effect.flatMap((assertion) => client.submitBatch(request.envelopes[0].spaceId, request, assertion))
      )
    },
    Discard: (request) =>
      requireVersion(request.protocolVersion).pipe(
        Effect.andThen(issueAssertion),
        Effect.flatMap((assertion) => client.discard(request.envelope.spaceId, request, assertion))
      ),
    Pull: (request) =>
      requireVersion(request.protocolVersion).pipe(
        Effect.andThen(issueAssertion),
        Effect.flatMap((assertion) => client.pull(request.spaceId, request, assertion))
      ),
    Bootstrap: (request) =>
      requireVersion(request.protocolVersion).pipe(
        Effect.andThen(issueAssertion),
        Effect.flatMap((assertion) => client.bootstrap(request.spaceId, request, assertion))
      ),
    Watch: (request) =>
      Stream.fromEffect(requireVersion(request.protocolVersion)).pipe(
        Stream.flatMap(() =>
          Stream.unwrap(issueAssertion.pipe(
            Effect.map((assertion) => client.watch(request.spaceId, request, assertion))
          ))
        )
      ),
    JoinEphemeral: (request) => {
      const { protocolVersion, ...join } = request
      return Stream.fromEffect(requireVersion(protocolVersion)).pipe(
        Stream.flatMap(() =>
          Stream.unwrap(issueAssertion.pipe(
            Effect.map((assertion) => client.joinEphemeral(request.spaceId, join, assertion))
          ))
        )
      )
    },
    PublishEphemeral: ({ request, sessionToken, protocolVersion }) => {
      return requireVersion(protocolVersion).pipe(
        Effect.andThen(issueAssertion),
        Effect.flatMap((assertion) =>
          client.publishEphemeral(
            request.spaceId,
            request,
            sessionToken,
            assertion
          )
        ),
        Effect.as(null)
      )
    },
    HeartbeatEphemeral: (request) => {
      const { protocolVersion, sessionToken, ...heartbeat } = request
      return requireVersion(protocolVersion).pipe(
        Effect.andThen(issueAssertion),
        Effect.flatMap((assertion) =>
          client.heartbeatEphemeral(
            request.spaceId,
            heartbeat,
            sessionToken,
            assertion
          )
        ),
        Effect.as(null)
      )
    }
  })
})

export interface LayerOptions<D extends Definition.Any, R = never,> {
  readonly definition: D
  readonly authorizeAccess: ServerStore.Options<R>["authorizeAccess"]
  readonly authorizeMutation: ServerStore.Options<R>["authorizeMutation"]
  readonly authorizeRead: ServerStore.Options<R>["authorizeRead"]
  readonly authorizeEphemeral: (
    input: EphemeralHub.AuthorizationInput
  ) => Effect.Effect<void, ReplicaError.AuthorizationDenied, R>
  readonly assertionSecret?: Redacted.Redacted | undefined
  readonly store?:
    | Omit<ServerStore.Options<R>, "definition" | "authorizeAccess" | "authorizeMutation" | "authorizeRead">
    | undefined
  readonly ephemeral?: EphemeralHub.Options | undefined
  readonly spaces?: SpaceEntity.HandlerOptions | undefined
  readonly maintenance?: ServerStore.MaintenanceOptions | undefined
  readonly supportedProtocolVersions?: ReadonlyArray<number> | undefined
}

const assertionSecretBytes = 32

const randomAssertionSecret = Crypto.Crypto.use((crypto) => crypto.randomBytes(assertionSecretBytes)).pipe(
  Effect.map((bytes) => Redacted.make(Encoding.encodeBase64Url(bytes))),
  Effect.catchTag("PlatformError", (error) => Effect.die(error))
)

export const layer = <D extends Definition.Any, R = never,>(
  options: LayerOptions<D, R>
): Layer.Layer<
  ServerStore.ServerStore,
  ReplicaError.ReplicaError,
  | Sharding.Sharding
  | SqlClient.SqlClient
  | Crypto.Crypto
  | Authentication.Authentication
  | RpcServer.Protocol
  | MutationRuntime.Handlers<D>
  | R
> => {
  const layerAssertions = Layer.unwrap(Effect.gen(function*() {
    const secret = options.assertionSecret ?? (yield* randomAssertionSecret)
    return PrincipalAssertion.layerHmac({ secret })
  }))
  const layerStore = ServerStore.layer({
    ...options.store,
    definition: options.definition,
    authorizeAccess: options.authorizeAccess,
    authorizeMutation: options.authorizeMutation,
    authorizeRead: options.authorizeRead
  }).pipe(Layer.provide(MutationRuntime.layer(options.definition, options.store?.evolution)))
  const layerHub = EphemeralHub.layer({ ...options.ephemeral, authorize: options.authorizeEphemeral })
  const layerEntities = SpaceEntity.layer(options.spaces).pipe(
    Layer.provide(layerAssertions),
    Layer.provide(layerStore),
    Layer.provide(layerHub)
  )
  const layerGatewayHandlers = SyncRpc.Rpcs.toLayer(makeHandlers(options))
  const layerGateway = RpcServer.layer(SyncRpc.Rpcs, { disableFatalDefects: true }).pipe(
    Layer.provide(layerGatewayHandlers),
    Layer.provide(layerEntities),
    Layer.provide(layerAssertions)
  )
  return Layer.mergeAll(
    layerGateway,
    ServerStore.layerMaintenance(options.maintenance).pipe(Layer.provide(layerStore)),
    layerStore
  )
}

export const layerProtocolWebSocket = (options: {
  readonly path: HttpRouter.PathInput
}): Layer.Layer<RpcServer.Protocol, never, RpcSerialization.RpcSerialization | HttpRouter.HttpRouter> =>
  RpcServer.layerProtocolWebsocket(options)
