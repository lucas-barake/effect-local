import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type * as Authentication from "./Authentication.js"
import * as SyncRpc from "./SyncRpc.js"

type Client = Effect.Success<typeof SyncRpc.makeRpcClient>

export interface Service {
  readonly client: Client
  readonly version: Effect.Effect<Protocol.ProtocolVersion, ReplicaError.ReplicaError>
  readonly rejected: (
    version: Protocol.ProtocolVersion
  ) => Effect.Effect<Protocol.ProtocolVersion, ReplicaError.ReplicaError>
}

export class ProtocolSession extends Context.Service<ProtocolSession, Service>()(
  "@lucas-barake/effect-local-rpc/ProtocolSession"
) {}

export interface Options {
  readonly supportedProtocolVersions?: ReadonlyArray<number>
}

export const layerWithOptions = (options?: Options): Layer.Layer<
  ProtocolSession,
  ReplicaError.InvalidConfiguration,
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication>
> =>
  Layer.effect(
    ProtocolSession,
    Effect.gen(function*() {
      const configured = options?.supportedProtocolVersions ?? [Protocol.currentProtocolVersion]
      const request = yield* Schema.decodeUnknownEffect(Protocol.NegotiateRequest)({
        supportedVersions: configured
      }).pipe(
        Effect.mapError(() =>
          new ReplicaError.InvalidConfiguration({
            option: "supportedProtocolVersions",
            message: "supportedProtocolVersions must be a nonempty list of positive safe integers"
          })
        )
      )
      const client = yield* SyncRpc.makeRpcClient
      const selected = yield* Ref.make<Protocol.ProtocolVersion | undefined>(undefined)
      const gate = yield* Semaphore.make(1)
      const negotiateUnlocked = Effect.gen(function*() {
        const cached = yield* Ref.get(selected)
        if (cached !== undefined) return cached
        const negotiated = yield* client.Negotiate(request).pipe(
          Effect.mapError((cause) => {
            if (Schema.is(ReplicaError.ReplicaError)(cause)) return cause
            return new ReplicaError.ProtocolInvalid({
              message: "The protocol negotiation failed",
              cause
            })
          }),
          Effect.withSpan("ProtocolSession.negotiate")
        )
        yield* Ref.set(selected, negotiated.version)
        return negotiated.version
      })
      const version = gate.withPermit(negotiateUnlocked)
      const rejected = (rejectedVersion: Protocol.ProtocolVersion) =>
        gate.withPermit(Effect.gen(function*() {
          const cached = yield* Ref.get(selected)
          if (cached === rejectedVersion) yield* Ref.set(selected, undefined)
          return yield* negotiateUnlocked
        }))
      return ProtocolSession.of({ client, version, rejected })
    })
  )

export const layer = layerWithOptions()
