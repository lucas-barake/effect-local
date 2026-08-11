import * as SyncEngine from "@lucas-barake/effect-local-sql/SyncEngine"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type * as RpcSerialization from "effect/unstable/rpc/RpcSerialization"
import type * as Socket from "effect/unstable/socket/Socket"
import type * as Authentication from "./Authentication.js"
import * as ProtocolSession from "./ProtocolSession.js"

export interface Options extends ProtocolSession.Options {}

export const layerFromSession: Layer.Layer<
  SyncEngine.SyncEngine,
  never,
  ProtocolSession.ProtocolSession
> = Layer.effect(
  SyncEngine.SyncEngine,
  Effect.gen(function*() {
    const session = yield* ProtocolSession.ProtocolSession
    const client = session.client
    const run = <A,>(
      execute: (version: Protocol.ProtocolVersion) => Effect.Effect<A, ReplicaError.ReplicaError>
    ) =>
      Effect.gen(function*() {
        const version = yield* session.version
        const first = yield* execute(version).pipe(Effect.result)
        if (first._tag === "Success") return first.success
        if (first.failure._tag !== "ProtocolVersionRejected") return yield* first.failure
        return yield* execute(yield* session.rejected(version))
      })
    const watch = (request: Parameters<SyncEngine.Service["watch"]>[0]) =>
      Stream.unwrap(session.version.pipe(
        Effect.map((version) =>
          client.Watch({ ...request, protocolVersion: version }).pipe(
            Stream.mapError((cause) => {
              if (Schema.is(ReplicaError.ReplicaError)(cause)) return cause
              return new ReplicaError.ProtocolInvalid({
                message: "The synchronization transport failed",
                cause
              })
            }),
            Stream.catchTag("ProtocolVersionRejected", () =>
              Stream.unwrap(
                session.rejected(version).pipe(
                  Effect.map((replacement) =>
                    client.Watch({ ...request, protocolVersion: replacement }).pipe(
                      Stream.mapError((cause) => {
                        if (Schema.is(ReplicaError.ReplicaError)(cause)) return cause
                        return new ReplicaError.ProtocolInvalid({
                          message: "The synchronization transport failed",
                          cause
                        })
                      })
                    )
                  )
                )
              ))
          )
        )
      ))
    return SyncEngine.SyncEngine.of({
      submit: (request) =>
        run((version) =>
          client.Submit({ ...request, protocolVersion: version }).pipe(
            Effect.mapError((cause) => {
              if (Schema.is(ReplicaError.ReplicaError)(cause)) return cause
              return new ReplicaError.ProtocolInvalid({
                message: "The synchronization transport failed",
                cause
              })
            })
          )
        ).pipe(
          Effect.withSpan("SyncClient.submit", {
            attributes: {
              "space.id": request.envelope.spaceId,
              "mutation.id": request.envelope.mutationId
            }
          })
        ),
      discard: (request) =>
        run((version) =>
          client.Discard({ ...request, protocolVersion: version }).pipe(
            Effect.mapError((cause) => {
              if (Schema.is(ReplicaError.ReplicaError)(cause)) return cause
              return new ReplicaError.ProtocolInvalid({
                message: "The synchronization transport failed",
                cause
              })
            })
          )
        ).pipe(
          Effect.withSpan("SyncClient.discard", {
            attributes: {
              "space.id": request.envelope.spaceId,
              "mutation.id": request.envelope.mutationId
            }
          })
        ),
      pull: (request) =>
        run((version) =>
          client.Pull({ ...request, protocolVersion: version }).pipe(
            Effect.mapError((cause) => {
              if (Schema.is(ReplicaError.ReplicaError)(cause)) return cause
              return new ReplicaError.ProtocolInvalid({
                message: "The synchronization transport failed",
                cause
              })
            })
          )
        )
          .pipe(
            Effect.withSpan("SyncClient.pull", {
              attributes: { "space.id": request.spaceId, "server.after": request.after }
            })
          ),
      bootstrap: (request) =>
        run((version) =>
          client.Bootstrap({ ...request, protocolVersion: version }).pipe(
            Effect.mapError((cause) => {
              if (Schema.is(ReplicaError.ReplicaError)(cause)) return cause
              return new ReplicaError.ProtocolInvalid({
                message: "The synchronization transport failed",
                cause
              })
            })
          )
        ).pipe(
          Effect.withSpan("SyncClient.bootstrap", {
            attributes: { "space.id": request.spaceId, "snapshot.id": request.snapshotId }
          })
        ),
      watch: (request) =>
        watch(request).pipe(
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
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication>
> => layerFromSession.pipe(Layer.provide(ProtocolSession.layerWithOptions(options)))

export const layer = layerFromSession.pipe(Layer.provide(ProtocolSession.layer))

export const layerProtocolSocket = (options?: {
  readonly retryTransientErrors?: boolean
}): Layer.Layer<RpcClient.Protocol, never, Socket.Socket | RpcSerialization.RpcSerialization> =>
  RpcClient.layerProtocolSocket(options)
