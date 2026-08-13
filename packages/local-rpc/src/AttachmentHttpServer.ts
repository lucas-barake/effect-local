import * as AttachmentServer from "@lucas-barake/effect-local-sql/AttachmentServer"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import type * as HttpServerRequest from "effect/unstable/http/HttpServerRequest"
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse"
import * as Authentication from "./Authentication.js"

export interface Options {
  readonly path?: HttpRouter.PathInput
}

const clientHeader = "x-effect-local-client-id"
const incarnationHeader = "x-effect-local-membership-incarnation"
const bytesHeader = "x-effect-local-attachment-bytes"
const errorHeader = "x-effect-local-error"
const offsetHeader = "upload-offset"
const completeHeader = "upload-complete"

const parseInteger = (value: string | undefined, name: string) => {
  if (value === undefined || !/^\d+$/.test(value)) {
    return Effect.fail(new ReplicaError.ProtocolInvalid({ message: `Missing or invalid ${name}` }))
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed)) {
    return Effect.fail(new ReplicaError.ProtocolInvalid({ message: `Missing or invalid ${name}` }))
  }
  return Effect.succeed(parsed)
}

const parseRange = (
  value: string | undefined,
  bytes: number
): Effect.Effect<Attachment.Range | undefined, ReplicaError.ProtocolInvalid | Attachment.InvalidAttachmentRange> => {
  if (value === undefined) return Effect.succeed(undefined)
  const match = /^bytes=(\d+)-(\d*)$/.exec(value)
  if (match === null) {
    return Effect.fail(
      new ReplicaError.ProtocolInvalid({
        message: "Attachment Range must contain one byte range"
      })
    )
  }
  const offset = Number(match[1])
  let end = bytes - 1
  if (match[2] !== "") end = Number(match[2])
  if (!Number.isSafeInteger(offset) || !Number.isSafeInteger(end) || offset < 0 || end < offset || end >= bytes) {
    return Effect.fail(new Attachment.InvalidAttachmentRange({ bytes, offset, length: end - offset + 1 }))
  }
  return Effect.succeed({ offset, length: end - offset + 1 })
}

const statusFor = (error: ReplicaError.ReplicaError): number => {
  switch (error._tag) {
    case "CredentialRejected":
      return 401
    case "AuthorizationDenied":
      return 403
    case "AttachmentUnavailable":
    case "AttachmentNotFound":
      return 404
    case "AttachmentOffsetConflict":
    case "AttachmentUploadBusy":
      return 409
    case "AttachmentTooLarge":
      return 413
    case "CapacityExceeded":
      return 429
    case "StorageUnavailable":
    case "AttachmentStorageError":
    case "AuthenticatorUnavailable":
    case "ServerUnavailable":
      return 503
    default:
      return 422
  }
}

const failureResponse = (error: ReplicaError.ReplicaError) => {
  const headers: Record<string, string> = { [errorHeader]: error._tag }
  if (error._tag === "AttachmentOffsetConflict") headers[offsetHeader] = String(error.actual)
  return HttpServerResponse.empty({ status: statusFor(error), headers })
}

export const layer = (options?: Options): Layer.Layer<
  never,
  never,
  HttpRouter.HttpRouter | AttachmentServer.AttachmentServer | Authentication.Authenticator
> =>
  Layer.effectDiscard(Effect.gen(function*() {
    const router = yield* HttpRouter.HttpRouter
    const attachments = yield* AttachmentServer.AttachmentServer
    const authenticator = yield* Authentication.Authenticator
    const path = options?.path ?? "/effect-local/attachments/:spaceId/:digest"

    const input = Effect.fn("AttachmentHttpServer.input")(function*(request: HttpServerRequest.HttpServerRequest) {
      yield* Effect.annotateCurrentSpan("http.request.method", request.method)
      const params = yield* HttpRouter.params
      const authorization = request.headers.authorization
      if (authorization === undefined || !authorization.startsWith("Bearer ")) {
        return yield* new ReplicaError.CredentialRejected()
      }
      const credential = Redacted.make(authorization.slice("Bearer ".length))
      const principal = yield* authenticator.authenticate(credential)
      const spaceId = yield* Schema.decodeUnknownEffect(Identity.SpaceId)(params.spaceId).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new ReplicaError.ProtocolInvalid({
              message: "Invalid attachment space id",
              cause
            })
          ))
      )
      const digest = yield* Schema.decodeUnknownEffect(Attachment.Digest)(params.digest).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new ReplicaError.ProtocolInvalid({
              message: "Invalid attachment digest",
              cause
            })
          ))
      )
      const bytes = yield* parseInteger(request.headers[bytesHeader], bytesHeader)
      yield* Effect.annotateCurrentSpan("attachment.bytes", bytes)
      const reference = yield* Schema.decodeUnknownEffect(Attachment.Reference)({
        _tag: "Attachment",
        digest,
        bytes
      }).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new ReplicaError.ProtocolInvalid({
              message: "Invalid attachment reference",
              cause
            })
          ))
      )
      const clientId = yield* Schema.decodeUnknownEffect(Identity.ClientId)(request.headers[clientHeader]).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new ReplicaError.ProtocolInvalid({
              message: "Invalid attachment client id",
              cause
            })
          ))
      )
      const membershipIncarnation = yield* Schema.decodeUnknownEffect(Identity.MembershipIncarnation)(
        request.headers[incarnationHeader]
      ).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new ReplicaError.ProtocolInvalid({
              message: "Invalid attachment membership incarnation",
              cause
            })
          ))
      )
      return { spaceId, clientId, membershipIncarnation, reference, principal }
    })

    const handle = <A,>(effect: Effect.Effect<HttpServerResponse.HttpServerResponse, ReplicaError.ReplicaError, A>) =>
      effect.pipe(Effect.catch((error) => Effect.succeed(failureResponse(error))))

    yield* router.addAll([
      HttpRouter.route("HEAD", path, (request) =>
        handle(
          Effect.gen(function*() {
            const prepared = yield* attachments.prepareUpload(yield* input(request))
            return HttpServerResponse.empty({
              headers: {
                [offsetHeader]: String(prepared.offset),
                [completeHeader]: String(prepared.complete)
              }
            })
          }).pipe(Effect.withSpan("AttachmentHttpServer.head"))
        )),
      HttpRouter.route("PATCH", path, (request) =>
        handle(
          Effect.gen(function*() {
            const decoded = yield* input(request)
            const expectedOffset = yield* parseInteger(request.headers[offsetHeader], offsetHeader)
            let declaredLength: number | undefined
            if (request.headers["content-length"] !== undefined) {
              declaredLength = yield* parseInteger(request.headers["content-length"], "content-length")
            }
            if (declaredLength !== undefined && declaredLength > decoded.reference.bytes - expectedOffset) {
              return yield* new Attachment.AttachmentLengthMismatch({
                expected: decoded.reference.bytes - expectedOffset,
                actual: declaredLength
              })
            }
            const prepared = yield* attachments.appendUpload({
              ...decoded,
              expectedOffset,
              bytes: request.stream.pipe(
                Stream.catchTag(
                  "HttpServerError",
                  (cause) => Stream.fail(new Attachment.AttachmentStorageError({ operation: "upload.httpBody", cause }))
                )
              )
            }).pipe(Effect.withSpan("AttachmentHttpServer.upload", {
              attributes: {
                "attachment.bytes": decoded.reference.bytes,
                "attachment.offset": expectedOffset
              }
            }))
            if (declaredLength !== undefined && prepared.offset !== expectedOffset + declaredLength) {
              return yield* new Attachment.AttachmentLengthMismatch({
                expected: declaredLength,
                actual: prepared.offset - expectedOffset
              })
            }
            return HttpServerResponse.empty({
              headers: {
                [offsetHeader]: String(prepared.offset),
                [completeHeader]: String(prepared.complete)
              }
            })
          }).pipe(Effect.withSpan("AttachmentHttpServer.patch"))
        )),
      HttpRouter.route("GET", path, (request) =>
        handle(
          Effect.gen(function*() {
            const decoded = yield* input(request)
            const range = yield* parseRange(request.headers.range, decoded.reference.bytes)
            let readInput: Parameters<AttachmentServer.Service["prepareRead"]>[0] = decoded
            if (range !== undefined) readInput = { ...decoded, range }
            const body = yield* attachments.prepareRead(readInput).pipe(
              Effect.withSpan("AttachmentHttpServer.read", {
                attributes: {
                  "attachment.bytes": decoded.reference.bytes,
                  "attachment.range": range !== undefined
                }
              })
            )
            const offset = range?.offset ?? 0
            const length = range?.length ?? decoded.reference.bytes
            let status = 200
            const headers: Record<string, string> = {
              "Accept-Ranges": "bytes",
              "Cache-Control": "private, no-store"
            }
            if (range !== undefined) {
              status = 206
              headers["Content-Range"] = `bytes ${offset}-${offset + length - 1}/${decoded.reference.bytes}`
            }
            return HttpServerResponse.stream(body, {
              status,
              contentLength: length,
              headers
            })
          }).pipe(Effect.withSpan("AttachmentHttpServer.get"))
        ))
    ])
  }))
