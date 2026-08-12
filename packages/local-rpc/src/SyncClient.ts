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
import type * as Socket from "effect/unstable/socket/Socket"
import * as Authentication from "./Authentication.js"
import { positiveFiniteDurationMillis } from "./internal/configuration.js"
import * as ProtocolSessionRetry from "./internal/protocolSession.js"
import * as ProtocolSession from "./ProtocolSession.js"

export interface Options extends ProtocolSession.Options {
  readonly rpcTimeout?: Duration.Input
}

export const layerFromSession = (options?: Pick<Options, "rpcTimeout">): Layer.Layer<
  SyncEngine.SyncEngine,
  ReplicaError.InvalidConfiguration,
  Authentication.CredentialLifecycle | ProtocolSession.ProtocolSession
> =>
  Layer.effect(
    SyncEngine.SyncEngine,
    Effect.gen(function*() {
      const rpcTimeoutMillis = yield* positiveFiniteDurationMillis(
        "rpcTimeout",
        options?.rpcTimeout ?? "10 seconds"
      )
      const session = yield* ProtocolSession.ProtocolSession
      const credentialLifecycle = yield* Authentication.CredentialLifecycle
      const client = session.client
      return SyncEngine.SyncEngine.of({
        waitForCredentialChange: (rejectedGeneration) => credentialLifecycle.awaitChange(rejectedGeneration),
        submit: (request) =>
          ProtocolSessionRetry.run(session, (version) =>
            client.Submit({ ...request, protocolVersion: version }).pipe(
              Effect.catchTag(
                "RpcClientError",
                (error): Effect.Effect<never, ReplicaError.ServerUnavailable | ReplicaError.ProtocolInvalid> => {
                  switch (error.reason._tag) {
                    case "WorkerSpawnError":
                    case "WorkerSendError":
                    case "WorkerReceiveError":
                    case "WorkerUnknownError":
                    case "SocketReadError":
                    case "SocketWriteError":
                    case "SocketOpenError":
                    case "SocketCloseError":
                      return Effect.fail(new ReplicaError.ServerUnavailable())
                    case "HttpError":
                      if (error.reason.kind === "TransportError") {
                        return Effect.fail(new ReplicaError.ServerUnavailable())
                      }
                      return Effect.fail(
                        new ReplicaError.ProtocolInvalid({
                          message: "The Submit RPC failed",
                          cause: error
                        })
                      )
                    case "RpcClientDefect":
                      return Effect.fail(
                        new ReplicaError.ProtocolInvalid({
                          message: "The Submit RPC failed",
                          cause: error
                        })
                      )
                  }
                  return Effect.die(error)
                }
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
              Effect.catchTag(
                "RpcClientError",
                (error): Effect.Effect<never, ReplicaError.ServerUnavailable | ReplicaError.ProtocolInvalid> => {
                  switch (error.reason._tag) {
                    case "WorkerSpawnError":
                    case "WorkerSendError":
                    case "WorkerReceiveError":
                    case "WorkerUnknownError":
                    case "SocketReadError":
                    case "SocketWriteError":
                    case "SocketOpenError":
                    case "SocketCloseError":
                      return Effect.fail(new ReplicaError.ServerUnavailable())
                    case "HttpError":
                      if (error.reason.kind === "TransportError") {
                        return Effect.fail(new ReplicaError.ServerUnavailable())
                      }
                      return Effect.fail(
                        new ReplicaError.ProtocolInvalid({
                          message: "The Discard RPC failed",
                          cause: error
                        })
                      )
                    case "RpcClientDefect":
                      return Effect.fail(
                        new ReplicaError.ProtocolInvalid({
                          message: "The Discard RPC failed",
                          cause: error
                        })
                      )
                  }
                  return Effect.die(error)
                }
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
              Effect.catchTag(
                "RpcClientError",
                (error): Effect.Effect<never, ReplicaError.ServerUnavailable | ReplicaError.ProtocolInvalid> => {
                  switch (error.reason._tag) {
                    case "WorkerSpawnError":
                    case "WorkerSendError":
                    case "WorkerReceiveError":
                    case "WorkerUnknownError":
                    case "SocketReadError":
                    case "SocketWriteError":
                    case "SocketOpenError":
                    case "SocketCloseError":
                      return Effect.fail(new ReplicaError.ServerUnavailable())
                    case "HttpError":
                      if (error.reason.kind === "TransportError") {
                        return Effect.fail(new ReplicaError.ServerUnavailable())
                      }
                      return Effect.fail(
                        new ReplicaError.ProtocolInvalid({
                          message: "The Pull RPC failed",
                          cause: error
                        })
                      )
                    case "RpcClientDefect":
                      return Effect.fail(
                        new ReplicaError.ProtocolInvalid({
                          message: "The Pull RPC failed",
                          cause: error
                        })
                      )
                  }
                  return Effect.die(error)
                }
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
              Effect.catchTag(
                "RpcClientError",
                (error): Effect.Effect<never, ReplicaError.ServerUnavailable | ReplicaError.ProtocolInvalid> => {
                  switch (error.reason._tag) {
                    case "WorkerSpawnError":
                    case "WorkerSendError":
                    case "WorkerReceiveError":
                    case "WorkerUnknownError":
                    case "SocketReadError":
                    case "SocketWriteError":
                    case "SocketOpenError":
                    case "SocketCloseError":
                      return Effect.fail(new ReplicaError.ServerUnavailable())
                    case "HttpError":
                      if (error.reason.kind === "TransportError") {
                        return Effect.fail(new ReplicaError.ServerUnavailable())
                      }
                      return Effect.fail(
                        new ReplicaError.ProtocolInvalid({
                          message: "The Bootstrap RPC failed",
                          cause: error
                        })
                      )
                    case "RpcClientDefect":
                      return Effect.fail(
                        new ReplicaError.ProtocolInvalid({
                          message: "The Bootstrap RPC failed",
                          cause: error
                        })
                      )
                  }
                  return Effect.die(error)
                }
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
                  // Acquire before installing the per-pull timeout so the first event cannot race the subscription.
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
                    Stream.catchTag(
                      "RpcClientError",
                      (
                        error
                      ): Stream.Stream<never, ReplicaError.ServerUnavailable | ReplicaError.ProtocolInvalid> => {
                        switch (error.reason._tag) {
                          case "WorkerSpawnError":
                          case "WorkerSendError":
                          case "WorkerReceiveError":
                          case "WorkerUnknownError":
                          case "SocketReadError":
                          case "SocketWriteError":
                          case "SocketOpenError":
                          case "SocketCloseError":
                            return Stream.fail(new ReplicaError.ServerUnavailable())
                          case "HttpError":
                            if (error.reason.kind === "TransportError") {
                              return Stream.fail(new ReplicaError.ServerUnavailable())
                            }
                            return Stream.fail(
                              new ReplicaError.ProtocolInvalid({
                                message: "The Watch RPC failed",
                                cause: error
                              })
                            )
                          case "RpcClientDefect":
                            return Stream.fail(
                              new ReplicaError.ProtocolInvalid({
                                message: "The Watch RPC failed",
                                cause: error
                              })
                            )
                        }
                        return Stream.die(error)
                      }
                    ),
                    Stream.timeoutOrElse({
                      duration: rpcTimeoutMillis,
                      orElse: () =>
                        Stream.fail(
                          new ReplicaError.OperationTimeout({
                            operation: "Watch",
                            timeoutMillis: rpcTimeoutMillis
                          })
                        )
                    })
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
  Authentication.CredentialLifecycle | RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication>
> => layerFromSession(options).pipe(Layer.provide(ProtocolSession.layerWithOptions(options)))

export const layer = layerFromSession().pipe(Layer.provide(ProtocolSession.layer))

export const layerProtocolSocket = (options?: {
  readonly retryTransientErrors?: boolean
  readonly retryPolicy?: Schedule.Schedule<any, Socket.SocketError>
}): Layer.Layer<RpcClient.Protocol, never, Socket.Socket | RpcSerialization.RpcSerialization> =>
  Layer.effect(RpcClient.Protocol, RpcClient.makeProtocolSocket(options))
