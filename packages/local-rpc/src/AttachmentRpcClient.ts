import * as AttachmentTransfer from "@lucas-barake/effect-local-sql/AttachmentTransfer"
import type * as AttachmentTransferProtocol from "@lucas-barake/effect-local/AttachmentTransfer"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import * as AttachmentDirectHttpClient from "./AttachmentDirectHttpClient.js"
import { positiveFiniteDurationMillis } from "./internal/configuration.js"
import * as ProtocolSessionRetry from "./internal/protocolSession.js"
import * as ProtocolSession from "./ProtocolSession.js"

export interface Options {
  readonly rpcTimeout?: Duration.Input
}

const exactBytes = <E, R,>(source: Stream.Stream<Uint8Array, E, R>, bytes: number) =>
  source.pipe(
    Stream.mapAccum(() => bytes, (remaining, chunk: Uint8Array) => {
      const length = Math.min(remaining, chunk.byteLength)
      return [remaining - length, [{ chunk: chunk.subarray(0, length), done: length === remaining }]] as const
    }),
    Stream.takeUntil(({ done }) => done),
    Stream.map(({ chunk }) => chunk)
  )

export const layerFromSession = (options?: Options): Layer.Layer<
  AttachmentTransfer.AttachmentTransfer,
  ReplicaError.InvalidConfiguration,
  AttachmentDirectHttpClient.AttachmentDirectHttpClient | ProtocolSession.ProtocolSession
> =>
  Layer.effect(
    AttachmentTransfer.AttachmentTransfer,
    Effect.gen(function*() {
      const rpcTimeoutMillis = yield* positiveFiniteDurationMillis(
        "attachmentRpcTimeout",
        options?.rpcTimeout ?? "30 seconds"
      )
      const session = yield* ProtocolSession.ProtocolSession
      const direct = yield* AttachmentDirectHttpClient.AttachmentDirectHttpClient
      const client = session.client

      const prepareUpload = (request: AttachmentTransfer.UploadRequest) =>
        ProtocolSessionRetry.run(session, (protocolVersion) =>
          client.PrepareAttachmentUpload({
            spaceId: request.spaceId,
            clientId: request.clientId,
            membershipIncarnation: request.membershipIncarnation,
            reference: request.reference,
            protocolVersion
          }).pipe(
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
                HttpError: (reason) => {
                  if (reason.kind === "TransportError") return Effect.fail(new ReplicaError.ServerUnavailable())
                  return Effect.fail(
                    new ReplicaError.ProtocolInvalid({ message: "The PrepareAttachmentUpload RPC failed" })
                  )
                },
                RpcClientDefect: () =>
                  Effect.fail(
                    new ReplicaError.ProtocolInvalid({ message: "The PrepareAttachmentUpload RPC failed" })
                  )
              },
              (_, error) => Effect.die(error)
            ),
            Effect.timeoutOrElse({
              duration: rpcTimeoutMillis,
              orElse: () =>
                Effect.fail(
                  new ReplicaError.OperationTimeout({
                    operation: "PrepareAttachmentUpload",
                    timeoutMillis: rpcTimeoutMillis
                  })
                )
            })
          ))

      const finalizeUpload = (
        request: AttachmentTransfer.UploadRequest,
        attemptId: AttachmentTransferProtocol.AttemptId
      ) =>
        ProtocolSessionRetry.run(session, (protocolVersion) =>
          client.FinalizeAttachmentUpload({
            spaceId: request.spaceId,
            clientId: request.clientId,
            membershipIncarnation: request.membershipIncarnation,
            reference: request.reference,
            attemptId,
            protocolVersion
          }).pipe(
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
                HttpError: (reason) => {
                  if (reason.kind === "TransportError") return Effect.fail(new ReplicaError.ServerUnavailable())
                  return Effect.fail(
                    new ReplicaError.ProtocolInvalid({ message: "The FinalizeAttachmentUpload RPC failed" })
                  )
                },
                RpcClientDefect: () =>
                  Effect.fail(
                    new ReplicaError.ProtocolInvalid({ message: "The FinalizeAttachmentUpload RPC failed" })
                  )
              },
              (_, error) => Effect.die(error)
            ),
            Effect.timeoutOrElse({
              duration: rpcTimeoutMillis,
              orElse: () =>
                Effect.fail(
                  new ReplicaError.OperationTimeout({
                    operation: "FinalizeAttachmentUpload",
                    timeoutMillis: rpcTimeoutMillis
                  })
                )
            })
          ))

      const upload: AttachmentTransfer.Service["upload"] = Effect.fnUntraced(function*(request) {
        const loop: Effect.Effect<void, ReplicaError.ReplicaError> = Effect.suspend(() =>
          prepareUpload(request).pipe(
            Effect.flatMap((prepared) => {
              if (prepared._tag === "UploadComplete") return Effect.void
              if (prepared._tag === "UploadReady") {
                return finalizeUpload(request, prepared.attemptId).pipe(Effect.asVoid)
              }
              let sourceFailure: ReplicaError.ReplicaError | undefined
              const source = exactBytes(request.bytes(prepared.offset), prepared.bytes).pipe(
                Stream.tapError((error) =>
                  Effect.sync(() => {
                    sourceFailure = error
                  })
                )
              )
              return direct.upload(prepared, source).pipe(
                Effect.as(true),
                Effect.catchTag("AttachmentGrantExpired", () => Effect.succeed(false)),
                Effect.catchTags({
                  AttachmentGrantRejected: () =>
                    Effect.fail(
                      new ReplicaError.ProtocolInvalid({ message: "The attachment upload grant is invalid" })
                    ),
                  AttachmentTransferUnavailable: () =>
                    Effect.fail(sourceFailure ?? new ReplicaError.ServerUnavailable()),
                  AttachmentTransferStatusError: (error) => {
                    if (
                      error.status === 408 || error.status === 425 || error.status === 429 || error.status >= 500
                    ) return Effect.fail(new ReplicaError.ServerUnavailable())
                    return Effect.fail(
                      new ReplicaError.ProtocolInvalid({ message: "The direct attachment upload was rejected" })
                    )
                  },
                  AttachmentTransferLengthMismatch: () =>
                    Effect.fail(
                      new ReplicaError.ProtocolInvalid({ message: "The attachment upload length is invalid" })
                    ),
                  AttachmentTransferRangeMismatch: () =>
                    Effect.fail(new ReplicaError.ProtocolInvalid({ message: "The attachment upload range is invalid" }))
                }),
                Effect.flatMap(() => loop)
              )
            })
          )
        )
        yield* loop
      })

      const prepareDownload = (request: AttachmentTransfer.DownloadRequest) =>
        ProtocolSessionRetry.run(session, (protocolVersion) => {
          let range: { readonly range?: NonNullable<typeof request.range> } = {}
          if (request.range !== undefined) range = { range: request.range }
          const payload = {
            spaceId: request.spaceId,
            clientId: request.clientId,
            membershipIncarnation: request.membershipIncarnation,
            reference: request.reference,
            protocolVersion,
            ...range
          }
          return client.PrepareAttachmentDownload(payload).pipe(
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
                HttpError: (reason) => {
                  if (reason.kind === "TransportError") return Effect.fail(new ReplicaError.ServerUnavailable())
                  return Effect.fail(
                    new ReplicaError.ProtocolInvalid({ message: "The PrepareAttachmentDownload RPC failed" })
                  )
                },
                RpcClientDefect: () =>
                  Effect.fail(
                    new ReplicaError.ProtocolInvalid({ message: "The PrepareAttachmentDownload RPC failed" })
                  )
              },
              (_, error) => Effect.die(error)
            ),
            Effect.timeoutOrElse({
              duration: rpcTimeoutMillis,
              orElse: () =>
                Effect.fail(
                  new ReplicaError.OperationTimeout({
                    operation: "PrepareAttachmentDownload",
                    timeoutMillis: rpcTimeoutMillis
                  })
                )
            })
          )
        })

      const downloadBytes = (
        request: AttachmentTransfer.DownloadRequest,
        expected: AttachmentTransferProtocol.DownloadGrant
      ): Stream.Stream<Uint8Array, ReplicaError.ReplicaError> =>
        Stream.suspend(() => direct.download(expected)).pipe(
          Stream.catchTag(
            "AttachmentGrantExpired",
            () =>
              Stream.unwrap(
                prepareDownload(request).pipe(Effect.map((refreshed) => {
                  if (
                    refreshed.objectVersion !== expected.objectVersion ||
                    refreshed.objectBytes !== expected.objectBytes ||
                    refreshed.chunk.index !== expected.chunk.index ||
                    refreshed.chunk.offset !== expected.chunk.offset ||
                    refreshed.chunk.bytes !== expected.chunk.bytes ||
                    refreshed.chunk.digest !== expected.chunk.digest
                  ) {
                    return Stream.fail(
                      new ReplicaError.ProtocolInvalid({ message: "The refreshed attachment grant changed its object" })
                    )
                  }
                  return downloadBytes(request, refreshed)
                }))
              )
          ),
          Stream.catchTags({
            AttachmentGrantRejected: () =>
              Stream.fail(new ReplicaError.ProtocolInvalid({ message: "The attachment download grant is invalid" })),
            AttachmentTransferUnavailable: () => Stream.fail(new ReplicaError.ServerUnavailable()),
            AttachmentTransferStatusError: (error) => {
              if (error.status === 408 || error.status === 425 || error.status === 429 || error.status >= 500) {
                return Stream.fail(new ReplicaError.ServerUnavailable())
              }
              return Stream.fail(
                new ReplicaError.ProtocolInvalid({ message: "The direct attachment download was rejected" })
              )
            },
            AttachmentTransferLengthMismatch: () =>
              Stream.fail(new ReplicaError.ProtocolInvalid({ message: "The attachment download length is invalid" })),
            AttachmentTransferRangeMismatch: () =>
              Stream.fail(new ReplicaError.ProtocolInvalid({ message: "The attachment download range is invalid" }))
          })
        )

      const download: AttachmentTransfer.Service["download"] = (request) =>
        prepareDownload(request).pipe(
          Effect.map((grant) => ({
            objectVersion: grant.objectVersion,
            objectBytes: grant.objectBytes,
            chunk: grant.chunk,
            bytes: downloadBytes(request, grant)
          }))
        )

      return AttachmentTransfer.AttachmentTransfer.of({ upload, download })
    })
  )
