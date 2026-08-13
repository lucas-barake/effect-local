import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type * as Authentication from "./Authentication.js"
import { positiveFiniteDurationMillis } from "./internal/configuration.js"
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
  readonly sessionAcquisitionTimeout?: Duration.Input
}

export const layerWithOptions = (options?: Options): Layer.Layer<
  ProtocolSession,
  ReplicaError.InvalidConfiguration,
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication>
> =>
  Layer.effect(
    ProtocolSession,
    Effect.gen(function*() {
      const sessionAcquisitionTimeoutMillis = yield* positiveFiniteDurationMillis(
        "sessionAcquisitionTimeout",
        options?.sessionAcquisitionTimeout ?? "10 seconds"
      )
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
          Effect.catchReasons(
            "RpcClientError",
            {
              WorkerSpawnError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
              WorkerSendError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
              WorkerReceiveError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
              WorkerUnknownError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
              SocketReadError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
              SocketWriteError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
              SocketOpenError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
              SocketCloseError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
              HttpError: (reason, error) => {
                if (reason.kind === "TransportError") {
                  return Effect.fail(new ReplicaError.ServerUnavailable())
                }
                return Effect.fail(
                  new ReplicaError.ProtocolInvalid({
                    message: "The protocol negotiation failed",
                    cause: error
                  })
                )
              },
              RpcClientDefect: (_, error) =>
                Effect.fail(
                  new ReplicaError.ProtocolInvalid({
                    message: "The protocol negotiation failed",
                    cause: error
                  })
                )
            },
            (_, error) => Effect.die(error)
          ),
          Effect.withSpan("ProtocolSession.negotiate")
        )
        yield* Ref.set(selected, negotiated.version)
        return negotiated.version
      })
      const version = gate.withPermit(negotiateUnlocked).pipe(
        Effect.timeoutOrElse({
          duration: sessionAcquisitionTimeoutMillis,
          orElse: () =>
            Effect.fail(
              new ReplicaError.OperationTimeout({
                operation: "Negotiate",
                timeoutMillis: sessionAcquisitionTimeoutMillis
              })
            )
        })
      )
      const rejected = (rejectedVersion: Protocol.ProtocolVersion) =>
        gate.withPermit(Effect.gen(function*() {
          const cached = yield* Ref.get(selected)
          if (cached === rejectedVersion) yield* Ref.set(selected, undefined)
          return yield* negotiateUnlocked
        })).pipe(
          Effect.timeoutOrElse({
            duration: sessionAcquisitionTimeoutMillis,
            orElse: () =>
              Effect.fail(
                new ReplicaError.OperationTimeout({
                  operation: "Negotiate",
                  timeoutMillis: sessionAcquisitionTimeoutMillis
                })
              )
          })
        )
      return ProtocolSession.of({ client, version, rejected })
    })
  )

export const Default = layerWithOptions()
