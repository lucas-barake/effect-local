import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import type * as HttpRouter from "effect/unstable/http/HttpRouter"
import type * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as Authentication from "./Authentication.js"
import * as PrincipalAssertion from "./PrincipalAssertion.js"
import * as SpaceEntity from "./SpaceEntity.js"
import * as SyncRpc from "./SyncRpc.js"

export interface Options {
  readonly supportedProtocolVersions?: ReadonlyArray<number>
}

const makeLayerHandlers = (options?: Options) => {
  return SyncRpc.Rpcs.toLayer(Effect.gen(function*() {
    const configured = options?.supportedProtocolVersions ?? [Protocol.currentProtocolVersion]
    const decoded = yield* Schema.decodeUnknownEffect(Protocol.NegotiateRequest)({
      supportedVersions: configured
    }).pipe(
      Effect.mapError(() =>
        new ReplicaError.InvalidConfiguration({
          option: "supportedProtocolVersions",
          message: "supportedProtocolVersions must be a nonempty list of positive safe integers"
        })
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
      PublishPresence: (update) => {
        const { protocolVersion, ...presence } = update
        return requireVersion(protocolVersion).pipe(
          Effect.andThen(issueAssertion),
          Effect.flatMap((assertion) => client.publishPresence(update.spaceId, presence, assertion)),
          Effect.as(null)
        )
      },
      WatchPresence: ({ spaceId, protocolVersion }) =>
        Stream.fromEffect(requireVersion(protocolVersion)).pipe(
          Stream.flatMap(() =>
            Stream.unwrap(issueAssertion.pipe(
              Effect.map((assertion) => client.watchPresence(spaceId, assertion))
            ))
          )
        )
    })
  }))
}

export const layerHandlers = makeLayerHandlers()
export const layerHandlersWithOptions = (options: Options) => makeLayerHandlers(options)

export const layer = RpcServer.layer(SyncRpc.Rpcs, { disableFatalDefects: true }).pipe(
  Layer.provide(layerHandlers)
)

export const layerWithOptions = (options: Options) =>
  RpcServer.layer(SyncRpc.Rpcs, { disableFatalDefects: true }).pipe(
    Layer.provide(layerHandlersWithOptions(options))
  )

export const layerProtocolWebSocket = (options: {
  readonly path: HttpRouter.PathInput
}): Layer.Layer<RpcServer.Protocol, never, RpcSerialization.RpcSerialization | HttpRouter.HttpRouter> =>
  RpcServer.layerProtocolWebsocket(options)
