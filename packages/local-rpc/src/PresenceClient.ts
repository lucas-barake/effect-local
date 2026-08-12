import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type * as Authentication from "./Authentication.js"
import * as ProtocolSessionRetry from "./internal/protocolSession.js"
import * as ProtocolSession from "./ProtocolSession.js"

export interface Service {
  readonly publish: (update: Protocol.PresenceUpdate) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly watch: (spaceId: Identity.SpaceId) => Stream.Stream<Protocol.PresenceUpdate, ReplicaError.ReplicaError>
}

export class PresenceClient extends Context.Service<PresenceClient, Service>()(
  "@lucas-barake/effect-local-rpc/PresenceClient"
) {}

export interface Options extends ProtocolSession.Options {}

export const layerFromSession: Layer.Layer<
  PresenceClient,
  never,
  ProtocolSession.ProtocolSession
> = Layer.effect(
  PresenceClient,
  Effect.gen(function*() {
    const session = yield* ProtocolSession.ProtocolSession
    const client = session.client
    return PresenceClient.of({
      publish: (update) =>
        ProtocolSessionRetry.run(
          session,
          (version) =>
            client.PublishPresence({ ...update, protocolVersion: version }).pipe(
              Effect.mapError((cause) => {
                if (Schema.is(ReplicaError.ReplicaError)(cause)) return cause
                return new ReplicaError.ProtocolInvalid({ message: "The presence transport failed", cause })
              })
            )
        ).pipe(
          Effect.asVoid,
          Effect.withSpan("PresenceClient.publish", {
            attributes: { "space.id": update.spaceId, "client.id": update.clientId }
          })
        ),
      watch: (spaceId) =>
        ProtocolSessionRetry.runStream(
          session,
          (version) =>
            client.WatchPresence({ spaceId, protocolVersion: version }).pipe(
              Stream.mapError((cause) => {
                if (Schema.is(ReplicaError.ReplicaError)(cause)) return cause
                return new ReplicaError.ProtocolInvalid({ message: "The presence transport failed", cause })
              })
            )
        ).pipe(
          Stream.withSpan("PresenceClient.watch", {
            attributes: { "space.id": spaceId }
          })
        )
    })
  })
)

export const layerWithOptions = (options?: Options): Layer.Layer<
  PresenceClient,
  ReplicaError.InvalidConfiguration,
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication>
> => layerFromSession.pipe(Layer.provide(ProtocolSession.layerWithOptions(options)))

export const layer = layerFromSession.pipe(Layer.provide(ProtocolSession.layer))
