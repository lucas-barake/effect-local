import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import type * as Schedule from "effect/Schedule"
import * as Stream from "effect/Stream"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import * as Socket from "effect/unstable/socket/Socket"
import * as Authentication from "./Authentication.js"
import * as EphemeralClient from "./EphemeralClient.js"
import { positiveFiniteDurationMillis } from "./internal/configuration.js"
import * as ProtocolSessionRetry from "./internal/protocolSession.js"
import * as ProtocolSocket from "./internal/protocolSocket.js"
import * as ProtocolSession from "./ProtocolSession.js"
import * as SyncRpc from "./SyncRpc.js"

export interface Options extends ProtocolSession.Options {
  readonly rpcTimeout?: Duration.Input
}

export const layerFromSession = (options?: Pick<Options, "rpcTimeout">): Layer.Layer<
  SyncEngine.SyncEngine,
  ReplicaError.InvalidConfiguration,
  Authentication.CredentialProvider | ProtocolSession.ProtocolSession
> =>
  Layer.effect(
    SyncEngine.SyncEngine,
    Effect.gen(function*() {
      const rpcTimeoutMillis = yield* positiveFiniteDurationMillis(
        "rpcTimeout",
        options?.rpcTimeout ?? "10 seconds"
      )
      const session = yield* ProtocolSession.ProtocolSession
      const credentialProvider = yield* Authentication.CredentialProvider
      const client = session.client
      return SyncEngine.SyncEngine.of({
        waitForCredentialChange: (rejectedGeneration) =>
          credentialProvider.awaitChange(rejectedGeneration).pipe(Effect.asVoid),
        submit: (request) =>
          ProtocolSessionRetry.run(session, (version) =>
            client.Submit({ ...request, protocolVersion: version }).pipe(
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
                        message: "The Submit RPC failed",
                        cause: error
                      })
                    )
                  },
                  RpcClientDefect: (_, error) =>
                    Effect.fail(
                      new ReplicaError.ProtocolInvalid({
                        message: "The Submit RPC failed",
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
                      operation: "Submit",
                      timeoutMillis: rpcTimeoutMillis
                    })
                  )
              })
            )).pipe(
              Effect.withSpan("SyncClient.submit", {
                attributes: {
                  "space.id": request.envelope.spaceId,
                  "mutation.id": request.envelope.mutationId
                }
              })
            ),
        discard: (request) =>
          ProtocolSessionRetry.run(session, (version) =>
            client.Discard({ ...request, protocolVersion: version }).pipe(
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
                        message: "The Discard RPC failed",
                        cause: error
                      })
                    )
                  },
                  RpcClientDefect: (_, error) =>
                    Effect.fail(
                      new ReplicaError.ProtocolInvalid({
                        message: "The Discard RPC failed",
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
                      operation: "Discard",
                      timeoutMillis: rpcTimeoutMillis
                    })
                  )
              })
            )).pipe(
              Effect.withSpan("SyncClient.discard", {
                attributes: {
                  "space.id": request.envelope.spaceId,
                  "mutation.id": request.envelope.mutationId
                }
              })
            ),
        pull: (request) =>
          ProtocolSessionRetry.run(session, (version) =>
            client.Pull({ ...request, protocolVersion: version }).pipe(
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
                        message: "The Pull RPC failed",
                        cause: error
                      })
                    )
                  },
                  RpcClientDefect: (_, error) =>
                    Effect.fail(
                      new ReplicaError.ProtocolInvalid({
                        message: "The Pull RPC failed",
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
                      operation: "Pull",
                      timeoutMillis: rpcTimeoutMillis
                    })
                  )
              })
            ))
            .pipe(
              Effect.withSpan("SyncClient.pull", {
                attributes: {
                  "space.id": request.spaceId,
                  "view.revision": request.cursor?.revision ?? 0,
                  "scope.generation": request.scopeGeneration
                }
              })
            ),
        bootstrap: (request) =>
          ProtocolSessionRetry.run(session, (version) =>
            client.Bootstrap({ ...request, protocolVersion: version }).pipe(
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
                        message: "The Bootstrap RPC failed",
                        cause: error
                      })
                    )
                  },
                  RpcClientDefect: (_, error) =>
                    Effect.fail(
                      new ReplicaError.ProtocolInvalid({
                        message: "The Bootstrap RPC failed",
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
                      operation: "Bootstrap",
                      timeoutMillis: rpcTimeoutMillis
                    })
                  )
              })
            )).pipe(
              Effect.withSpan("SyncClient.bootstrap", {
                attributes: { "space.id": request.spaceId, "snapshot.id": request.snapshotId }
              })
            ),
        watch: (request) =>
          ProtocolSessionRetry.runStream(
            session,
            (version) =>
              Stream.unwrap(
                Effect.gen(function*() {
                  // Acquire eagerly so the first event cannot race the subscription.
                  const acquisition = yield* client.Watch(
                    { ...request, protocolVersion: version },
                    { asQueue: true }
                  ).pipe(Effect.forkScoped({ startImmediately: true }))
                  const queue = yield* Fiber.join(acquisition).pipe(
                    Effect.timeoutOrElse({
                      duration: rpcTimeoutMillis,
                      orElse: () =>
                        Effect.fail(
                          new ReplicaError.OperationTimeout({
                            operation: "Watch",
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
                              message: "The Watch RPC failed",
                              cause: error
                            })
                          )
                        },
                        RpcClientDefect: (_, error) =>
                          Stream.fail(
                            new ReplicaError.ProtocolInvalid({
                              message: "The Watch RPC failed",
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
            Stream.withSpan("SyncClient.watch", {
              attributes: { "space.id": request.spaceId }
            })
          )
      })
    })
  )

export const layerWithOptions = (options?: Options): Layer.Layer<
  SyncEngine.SyncEngine,
  ReplicaError.InvalidConfiguration,
  Authentication.CredentialProvider | RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication>
> => layerFromSession(options).pipe(Layer.provide(ProtocolSession.layerWithOptions(options)))

export const layer = layerFromSession().pipe(Layer.provide(ProtocolSession.layer))

export const layerProtocolSocket = (options?: {
  readonly retryTransientErrors?: boolean
  readonly retryPolicy?: Schedule.Schedule<any, Socket.SocketError>
}): Layer.Layer<RpcClient.Protocol, never, Socket.Socket | RpcSerialization.RpcSerialization> =>
  Layer.effect(RpcClient.Protocol, ProtocolSocket.make(options))

export interface WebSocketOptions<R = never,> extends Options, Pick<EphemeralClient.Options, "heartbeatInterval"> {
  readonly url: string | Effect.Effect<string, never, R>
  readonly maximumFrameBytes?: number
  readonly retryTransientErrors?: boolean
  readonly retryPolicy?: Schedule.Schedule<any, Socket.SocketError>
  readonly socket?: {
    readonly closeCodeIsError?: ((code: number) => boolean) | undefined
    readonly openTimeout?: Duration.Input | undefined
    readonly protocols?: string | Array<string> | undefined
  }
}

/**
 * One WebSocket carrying both the sync engine and the ephemeral client over a
 * shared protocol session. The credential middleware is built fresh per call
 * so two clients with different providers under one memo map never share it.
 */
export const layerWebSocket = <R = never,>(options: WebSocketOptions<R>): Layer.Layer<
  SyncEngine.SyncEngine | EphemeralClient.EphemeralClient,
  ReplicaError.InvalidConfiguration,
  Authentication.CredentialProvider | Socket.WebSocketConstructor | R
> => {
  const layerSocket = Layer.effect(
    Socket.Socket,
    Effect.suspend(() => {
      const url = options.url
      if (typeof url === "string") return Socket.makeWebSocket(url, options.socket)
      return Effect.flatMap(url, (resolved) => Socket.makeWebSocket(resolved, options.socket))
    })
  )
  const layerProtocol = layerProtocolSocket(options).pipe(
    Layer.provide(layerSocket),
    Layer.provide(SyncRpc.layerJson(options))
  )
  const layerSession = ProtocolSession.layerWithOptions(options).pipe(
    Layer.provide(layerProtocol),
    Layer.provide(Layer.fresh(Authentication.layerClient))
  )
  return Layer.merge(layerFromSession(options), EphemeralClient.layerFromSession(options)).pipe(
    Layer.provide(layerSession)
  )
}
