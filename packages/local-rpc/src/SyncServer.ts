import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import type * as HttpRouter from "effect/unstable/http/HttpRouter"
import type * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as RpcServer from "effect/unstable/rpc/RpcServer"
import * as Authentication from "./Authentication.js"
import * as PrincipalAssertion from "./PrincipalAssertion.js"
import * as SpaceEntity from "./SpaceEntity.js"
import * as SyncRpc from "./SyncRpc.js"

export const layerHandlers = SyncRpc.Rpcs.toLayer(Effect.gen(function*() {
  const client = yield* SpaceEntity.Client
  const issuer = yield* PrincipalAssertion.Issuer
  const issueAssertion = Authentication.Principal.pipe(Effect.flatMap(issuer.issue))
  return SyncRpc.Rpcs.of({
    Submit: (request) =>
      issueAssertion.pipe(
        Effect.flatMap((assertion) => client.submit(request.envelope.spaceId, request, assertion))
      ),
    Pull: (request) =>
      issueAssertion.pipe(
        Effect.flatMap((assertion) => client.pull(request.spaceId, request, assertion))
      ),
    Bootstrap: (request) =>
      issueAssertion.pipe(
        Effect.flatMap((assertion) => client.bootstrap(request.spaceId, request, assertion))
      ),
    Watch: (request) =>
      Stream.unwrap(issueAssertion.pipe(
        Effect.map((assertion) => client.watch(request.spaceId, request, assertion))
      )),
    PublishPresence: (update) =>
      issueAssertion.pipe(
        Effect.flatMap((assertion) => client.publishPresence(update.spaceId, update, assertion)),
        Effect.as(null)
      ),
    WatchPresence: ({ spaceId }) =>
      Stream.unwrap(issueAssertion.pipe(
        Effect.map((assertion) => client.watchPresence(spaceId, assertion))
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
