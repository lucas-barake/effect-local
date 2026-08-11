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
    const requireVersion = (protocolVersion: Protocol.ProtocolVersion | undefined) => {
      const version = protocolVersion ?? Protocol.legacyProtocolVersion
      if (supportedVersions.includes(version)) return Effect.void
      return Effect.fail(new ReplicaError.ProtocolVersionRejected({ version, serverVersions: supportedVersions }))
    }
    const client = yield* SpaceEntity.Client
    return SyncRpc.Rpcs.of({
      Negotiate: ({ supportedVersions: clientVersions }) => {
        const version = supportedVersions.find((candidate) => clientVersions.includes(candidate))
        if (version !== undefined) return Effect.succeed({ version })
        return Effect.fail(new ReplicaError.UpgradeRequired({ clientVersions, serverVersions: supportedVersions }))
      },
      Submit: (request) =>
        requireVersion(request.protocolVersion).pipe(
          Effect.andThen(Authentication.Principal),
          Effect.flatMap((principal) => client.submit(request.envelope.spaceId, request, principal))
        ),
      Discard: (request) =>
        requireVersion(request.protocolVersion).pipe(
          Effect.andThen(Authentication.Principal),
          Effect.flatMap((principal) => client.discard(request.envelope.spaceId, request, principal))
        ),
      Pull: (request) =>
        requireVersion(request.protocolVersion).pipe(
          Effect.andThen(Authentication.Principal),
          Effect.flatMap((principal) => client.pull(request.spaceId, request, principal))
        ),
      Bootstrap: (request) =>
        requireVersion(request.protocolVersion).pipe(
          Effect.andThen(Authentication.Principal),
          Effect.flatMap((principal) => client.bootstrap(request.spaceId, request, principal))
        ),
      Watch: (request) =>
        Stream.fromEffect(requireVersion(request.protocolVersion)).pipe(
          Stream.flatMap(() =>
            Stream.unwrap(Authentication.Principal.pipe(
              Effect.map((principal) => client.watch(request.spaceId, request, principal))
            ))
          )
        ),
      PublishPresence: (update) => {
        const { protocolVersion, ...presence } = update
        return requireVersion(protocolVersion).pipe(
          Effect.andThen(Authentication.Principal),
          Effect.flatMap((principal) => client.publishPresence(update.spaceId, presence, principal)),
          Effect.as(null)
        )
      },
      WatchPresence: ({ spaceId, protocolVersion }) =>
        Stream.fromEffect(requireVersion(protocolVersion)).pipe(
          Stream.flatMap(() =>
            Stream.unwrap(Authentication.Principal.pipe(
              Effect.map((principal) => client.watchPresence(spaceId, principal))
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
