import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import type * as HttpRouter from "effect/unstable/http/HttpRouter"
import type * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as Authentication from "./Authentication.js"
import * as PresenceHub from "./PresenceHub.js"
import * as SyncRpc from "./SyncRpc.js"

export const layerHandlers = SyncRpc.Rpcs.toLayer(Effect.gen(function*() {
  const store = yield* ServerStore.ServerStore
  const presence = yield* PresenceHub.PresenceHub
  return SyncRpc.Rpcs.of({
    Submit: (envelope) =>
      Authentication.Principal.pipe(
        Effect.flatMap((principal) => store.admit(envelope, principal))
      ),
    Pull: (request) =>
      Authentication.Principal.pipe(
        Effect.flatMap((principal) => store.pullAuthorized(request, principal))
      ),
    Watch: ({ spaceId }) =>
      Stream.unwrap(Authentication.Principal.pipe(
        Effect.flatMap((principal) => store.watchAuthorized(spaceId, principal))
      )),
    PublishPresence: (update) =>
      Authentication.Principal.pipe(
        Effect.flatMap((principal) => presence.publish(update, principal)),
        Effect.as(null)
      ),
    WatchPresence: ({ spaceId }) =>
      Stream.unwrap(Authentication.Principal.pipe(
        Effect.map((principal) => presence.watch(spaceId, principal))
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
