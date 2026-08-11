import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import type * as HttpRouter from "effect/unstable/http/HttpRouter"
import type * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as Authentication from "./Authentication.js"
import * as SpaceEntity from "./SpaceEntity.js"
import * as SyncRpc from "./SyncRpc.js"

export const layerHandlers = SyncRpc.Rpcs.toLayer(Effect.gen(function*() {
  const client = yield* SpaceEntity.Client
  return SyncRpc.Rpcs.of({
    Submit: (request) =>
      Authentication.Principal.pipe(
        Effect.flatMap((principal) => client.submit(request.envelope.spaceId, request, principal))
      ),
    Pull: (request) =>
      Authentication.Principal.pipe(
        Effect.flatMap((principal) => client.pull(request.spaceId, request, principal))
      ),
    Watch: (request) =>
      Stream.unwrap(Authentication.Principal.pipe(
        Effect.map((principal) => client.watch(request.spaceId, request, principal))
      )),
    PublishPresence: (update) =>
      Authentication.Principal.pipe(
        Effect.flatMap((principal) => client.publishPresence(update.spaceId, update, principal)),
        Effect.as(null)
      ),
    WatchPresence: ({ spaceId }) =>
      Stream.unwrap(Authentication.Principal.pipe(
        Effect.map((principal) => client.watchPresence(spaceId, principal))
      ))
  })
}))

export const layer = RpcServer.layer(SyncRpc.Rpcs, { disableFatalDefects: true }).pipe(
  Layer.provide(layerHandlers)
)

export const layerProtocolWebSocket = (options: {
  readonly path: HttpRouter.PathInput
}): Layer.Layer<RpcServer.Protocol, never, RpcSerialization.RpcSerialization | HttpRouter.HttpRouter> =>
  RpcServer.layerProtocolWebsocket(options)
