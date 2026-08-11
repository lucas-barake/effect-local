import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import type * as Socket from "effect/unstable/socket/Socket"
import * as SyncRpc from "./SyncRpc.js"

import type * as Authentication from "./Authentication.js"

const transportError = (cause: unknown): ReplicaError.ReplicaError => {
  if (Schema.is(ReplicaError.ReplicaError)(cause)) return cause
  return new ReplicaError.ProtocolInvalid({
    message: "The synchronization transport failed",
    cause
  })
}

export const layer: Layer.Layer<
  SyncEngine.SyncEngine,
  never,
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication>
> = Layer.effect(
  SyncEngine.SyncEngine,
  Effect.gen(function*() {
    const client = yield* SyncRpc.makeRpcClient
    return SyncEngine.SyncEngine.of({
      submit: (request) =>
        client.Submit(request).pipe(
          Effect.mapError(transportError),
          Effect.withSpan("SyncClient.submit", {
            attributes: {
              "space.id": request.envelope.spaceId,
              "mutation.id": request.envelope.mutationId
            }
          })
        ),
      pull: (request) =>
        client.Pull(request).pipe(
          Effect.mapError(transportError),
          Effect.withSpan("SyncClient.pull", {
            attributes: { "space.id": request.spaceId, "server.after": request.after }
          })
        ),
      bootstrap: (request) =>
        client.Bootstrap(request).pipe(
          Effect.mapError(transportError),
          Effect.withSpan("SyncClient.bootstrap", {
            attributes: { "space.id": request.spaceId, "snapshot.id": request.snapshotId }
          })
        ),
      watch: (request) =>
        client.Watch(request).pipe(
          Stream.mapError(transportError),
          Stream.withSpan("SyncClient.watch", {
            attributes: { "space.id": request.spaceId }
          })
        )
    })
  })
)

export const layerProtocolSocket = (options?: {
  readonly retryTransientErrors?: boolean
}): Layer.Layer<RpcClient.Protocol, never, Socket.Socket | RpcSerialization.RpcSerialization> =>
  RpcClient.layerProtocolSocket(options)
