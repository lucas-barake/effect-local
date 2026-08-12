import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type * as Authentication from "./Authentication.js"
import { positiveFiniteDurationMillis } from "./internal/configuration.js"
import * as ProtocolSessionRetry from "./internal/protocolSession.js"
import * as ProtocolSession from "./ProtocolSession.js"

export interface Service {
  readonly publish: (update: Protocol.PresenceUpdate) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly watch: (spaceId: Identity.SpaceId) => Stream.Stream<Protocol.PresenceUpdate, ReplicaError.ReplicaError>
}

export class PresenceClient extends Context.Service<PresenceClient, Service>()(
  "@lucas-barake/effect-local-rpc/PresenceClient"
) {}

export interface Options extends ProtocolSession.Options {
  readonly rpcTimeout?: Duration.Input
}

export const layerFromSession = (options?: Pick<Options, "rpcTimeout">): Layer.Layer<
  PresenceClient,
  ReplicaError.InvalidConfiguration,
  ProtocolSession.ProtocolSession
> =>
  Layer.effect(
    PresenceClient,
    Effect.gen(function*() {
      const rpcTimeoutMillis = yield* positiveFiniteDurationMillis(
        "rpcTimeout",
        options?.rpcTimeout ?? "10 seconds"
      )
      const session = yield* ProtocolSession.ProtocolSession
      const client = session.client
      return PresenceClient.of({
        publish: (update) =>
          ProtocolSessionRetry.run(
            session,
            (version) =>
              client.PublishPresence({ ...update, protocolVersion: version }).pipe(
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
                          message: "The PublishPresence RPC failed",
                          cause: error
                        })
                      )
                    },
                    RpcClientDefect: (_, error) =>
                      Effect.fail(
                        new ReplicaError.ProtocolInvalid({
                          message: "The PublishPresence RPC failed",
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
                        operation: "PublishPresence",
                        timeoutMillis: rpcTimeoutMillis
                      })
                    )
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
              Stream.unwrap(
                Effect.gen(function*() {
                  // Acquire eagerly so the first event cannot race the subscription.
                  const acquisition = yield* client.WatchPresence(
                    { spaceId, protocolVersion: version },
                    { asQueue: true }
                  ).pipe(Effect.forkScoped({ startImmediately: true }))
                  const queue = yield* Fiber.join(acquisition).pipe(
                    Effect.timeoutOrElse({
                      duration: rpcTimeoutMillis,
                      orElse: () =>
                        Effect.fail(
                          new ReplicaError.OperationTimeout({
                            operation: "WatchPresence",
                            timeoutMillis: rpcTimeoutMillis
                          })
                        )
                    }),
                    Effect.ensuring(Fiber.interrupt(acquisition))
                  )
                  return Stream.fromQueue(queue).pipe(
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
                              message: "The WatchPresence RPC failed",
                              cause: error
                            })
                          )
                        },
                        RpcClientDefect: (_, error) =>
                          Stream.fail(
                            new ReplicaError.ProtocolInvalid({
                              message: "The WatchPresence RPC failed",
                              cause: error
                            })
                          )
                      },
                      (_, error) => Stream.die(error)
                    )
                  )
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
> => layerFromSession(options).pipe(Layer.provide(ProtocolSession.layerWithOptions(options)))

export const layer = layerFromSession().pipe(Layer.provide(ProtocolSession.layer))
