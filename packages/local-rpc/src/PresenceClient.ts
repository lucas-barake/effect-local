import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type * as Authentication from "./Authentication.js"
import { mapRpcError } from "./SyncClient.js"
import * as SyncRpc from "./SyncRpc.js"

export interface Service {
  readonly publish: (update: Protocol.PresenceUpdate) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly watch: (spaceId: Identity.SpaceId) => Stream.Stream<Protocol.PresenceUpdate, ReplicaError.ReplicaError>
}

export class PresenceClient extends Context.Service<PresenceClient, Service>()(
  "@lucas-barake/effect-local-rpc/PresenceClient"
) {}

export const layer: Layer.Layer<
  PresenceClient,
  never,
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication>
> = Layer.effect(
  PresenceClient,
  SyncRpc.makeRpcClient.pipe(
    Effect.map((client) =>
      PresenceClient.of({
        publish: (update) =>
          client.PublishPresence(update).pipe(
            Effect.mapError(mapRpcError),
            Effect.asVoid
          ),
        watch: (spaceId) => client.WatchPresence({ spaceId }).pipe(Stream.mapError(mapRpcError))
      })
    )
  )
)
