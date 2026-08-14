import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schedule from "effect/Schedule"
import * as Stream from "effect/Stream"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type * as Authentication from "./Authentication.js"
import { positiveFiniteDurationMillis } from "./internal/configuration.js"
import * as ProtocolSessionRetry from "./internal/protocolSession.js"
import * as ProtocolSession from "./ProtocolSession.js"

export interface Service {
  readonly join: (
    request: Protocol.EphemeralJoinRequest
  ) => Stream.Stream<Protocol.EphemeralMessage, ReplicaError.ReplicaError>
  readonly publish: (
    request: Protocol.EphemeralPublishRequest
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly heartbeat: (
    request: Protocol.EphemeralHeartbeatRequest
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
}

export class EphemeralClient extends Context.Service<EphemeralClient, Service>()(
  "@lucas-barake/effect-local-rpc/EphemeralClient"
) {}

export interface Options extends ProtocolSession.Options {
  readonly rpcTimeout?: Duration.Input
  readonly heartbeatInterval?: Duration.Input
}

export const layerFromSession = (
  options?: Pick<Options, "rpcTimeout" | "heartbeatInterval">
): Layer.Layer<EphemeralClient, ReplicaError.InvalidConfiguration, ProtocolSession.ProtocolSession> =>
  Layer.effect(
    EphemeralClient,
    Effect.gen(function*() {
      const rpcTimeoutMillis = yield* positiveFiniteDurationMillis(
        "rpcTimeout",
        options?.rpcTimeout ?? "10 seconds"
      )
      const heartbeatIntervalMillis = yield* positiveFiniteDurationMillis(
        "heartbeatInterval",
        options?.heartbeatInterval ?? "20 seconds"
      )
      const session = yield* ProtocolSession.ProtocolSession
      const client = session.client

      const publish = (request: Protocol.EphemeralPublishRequest) =>
        ProtocolSessionRetry.run(
          session,
          (version) =>
            client.PublishEphemeral({ request, protocolVersion: version }).pipe(
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
                        message: "The PublishEphemeral RPC failed",
                        cause: error
                      })
                    )
                  },
                  RpcClientDefect: (_, error) =>
                    Effect.fail(
                      new ReplicaError.ProtocolInvalid({
                        message: "The PublishEphemeral RPC failed",
                        cause: error
                      })
                    )
                },
                (_, error) => Effect.die(error)
              ),
              Effect.timeoutOrElse({
                duration: rpcTimeoutMillis,
                orElse: () =>
                  Effect.fail(
                    new ReplicaError.OperationTimeout({
                      operation: "PublishEphemeral",
                      timeoutMillis: rpcTimeoutMillis
                    })
                  )
              })
            )
        ).pipe(Effect.asVoid)

      const heartbeat = (request: Protocol.EphemeralHeartbeatRequest) =>
        ProtocolSessionRetry.run(
          session,
          (version) =>
            client.HeartbeatEphemeral({ ...request, protocolVersion: version }).pipe(
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
                        message: "The HeartbeatEphemeral RPC failed",
                        cause: error
                      })
                    )
                  },
                  RpcClientDefect: (_, error) =>
                    Effect.fail(
                      new ReplicaError.ProtocolInvalid({
                        message: "The HeartbeatEphemeral RPC failed",
                        cause: error
                      })
                    )
                },
                (_, error) => Effect.die(error)
              ),
              Effect.timeoutOrElse({
                duration: rpcTimeoutMillis,
                orElse: () =>
                  Effect.fail(
                    new ReplicaError.OperationTimeout({
                      operation: "HeartbeatEphemeral",
                      timeoutMillis: rpcTimeoutMillis
                    })
                  )
              })
            )
        ).pipe(Effect.asVoid)

      return EphemeralClient.of({
        publish,
        heartbeat,
        join: (request) =>
          ProtocolSessionRetry.runStream(
            session,
            (version) =>
              Stream.unwrap(Effect.gen(function*() {
                const acquisition = yield* client.JoinEphemeral(
                  { ...request, protocolVersion: version },
                  { asQueue: true }
                ).pipe(Effect.forkScoped({ startImmediately: true }))
                const queue = yield* Fiber.join(acquisition).pipe(
                  Effect.timeoutOrElse({
                    duration: rpcTimeoutMillis,
                    orElse: () =>
                      Effect.fail(
                        new ReplicaError.OperationTimeout({
                          operation: "JoinEphemeral",
                          timeoutMillis: rpcTimeoutMillis
                        })
                      )
                  }),
                  Effect.ensuring(Fiber.interrupt(acquisition))
                )
                const messages = Stream.fromQueue(queue).pipe(
                  Stream.catchReasons(
                    "RpcClientError",
                    {
                      WorkerSpawnError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                      WorkerSendError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                      WorkerReceiveError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                      WorkerUnknownError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                      SocketReadError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                      SocketWriteError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                      SocketOpenError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                      SocketCloseError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                      HttpError: (reason, error) => {
                        if (reason.kind === "TransportError") {
                          return Stream.fail(new ReplicaError.ServerUnavailable())
                        }
                        return Stream.fail(
                          new ReplicaError.ProtocolInvalid({
                            message: "The JoinEphemeral RPC failed",
                            cause: error
                          })
                        )
                      },
                      RpcClientDefect: (_, error) =>
                        Stream.fail(
                          new ReplicaError.ProtocolInvalid({
                            message: "The JoinEphemeral RPC failed",
                            cause: error
                          })
                        )
                    },
                    (_, error) => Stream.die(error)
                  )
                )
                const requestedInterval = Math.floor(request.ttlMillis / 2)
                const interval = Math.max(1, Math.min(heartbeatIntervalMillis, requestedInterval))
                const heartbeatLoop = Effect.sleep(interval).pipe(
                  Effect.andThen(heartbeat({ spaceId: request.spaceId, member: request.member })),
                  Effect.forever
                )
                return messages.pipe(Stream.mergeEffect(heartbeatLoop))
              })).pipe(Stream.repeat(Schedule.forever))
          ).pipe(
            Stream.withSpan("EphemeralClient.join", {
              attributes: {
                "space.id": request.spaceId,
                "client.id": request.member.clientId
              }
            })
          )
      })
    })
  )

export const layerWithOptions = (options?: Options): Layer.Layer<
  EphemeralClient,
  ReplicaError.InvalidConfiguration,
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication>
> => layerFromSession(options).pipe(Layer.provide(ProtocolSession.layerWithOptions(options)))

export const layer = layerFromSession().pipe(Layer.provide(ProtocolSession.layer))
