import * as AttachmentTransfer from "@lucas-barake/effect-local-sql/AttachmentTransfer"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Stream from "effect/Stream"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import * as Authentication from "./Authentication.js"

export interface Options {
  readonly baseUrl: string
  readonly path?: string
}

const clientHeader = "x-effect-local-client-id"
const incarnationHeader = "x-effect-local-membership-incarnation"
const bytesHeader = "x-effect-local-attachment-bytes"
const errorHeader = "x-effect-local-error"
const offsetHeader = "upload-offset"
const completeHeader = "upload-complete"

export const layer = (options: Options): Layer.Layer<
  AttachmentTransfer.AttachmentTransfer,
  never,
  HttpClient.HttpClient | Authentication.CredentialProvider
> =>
  Layer.effect(
    AttachmentTransfer.AttachmentTransfer,
    Effect.gen(function*() {
      const client = yield* HttpClient.HttpClient
      const credentials = yield* Authentication.CredentialProvider
      const baseUrl = options.baseUrl.replace(/\/$/, "")
      const path = (options.path ?? "/effect-local/attachments").replace(/^\/?/, "/").replace(/\/$/, "")

      const requestFor = Effect.fn("AttachmentHttpClient.request")(function*(
        method: "GET" | "HEAD" | "PATCH",
        request: AttachmentTransfer.UploadRequest | AttachmentTransfer.DownloadRequest
      ) {
        const credential = yield* credentials.acquire
        const url = `${baseUrl}${path}/${encodeURIComponent(request.spaceId)}/${
          encodeURIComponent(request.reference.digest)
        }`
        return HttpClientRequest.make(method)(url, {
          headers: {
            authorization: `Bearer ${Redacted.value(credential.bearer)}`,
            [clientHeader]: request.clientId,
            [incarnationHeader]: request.membershipIncarnation,
            [bytesHeader]: String(request.reference.bytes)
          }
        })
      })

      const upload: AttachmentTransfer.Service["upload"] = Effect.fn("AttachmentHttpClient.upload")(function*(request) {
        yield* Effect.annotateCurrentSpan("attachment.bytes", request.reference.bytes)
        const headRequest = yield* requestFor("HEAD", request)
        const head = yield* client.execute(headRequest).pipe(
          Effect.catchTag("HttpClientError", () => Effect.fail(new ReplicaError.ServerUnavailable())),
          Effect.withSpan("AttachmentHttpClient.head", {
            attributes: { "attachment.bytes": request.reference.bytes }
          })
        )
        if (head.status < 200 || head.status >= 300) {
          switch (head.headers[errorHeader]) {
            case "CredentialRejected":
              return yield* new ReplicaError.CredentialRejected()
            case "AuthorizationDenied":
              return yield* new ReplicaError.AuthorizationDenied({ reason: { _tag: "AttachmentDenied" } })
            case "AttachmentUnavailable":
            case "AttachmentNotFound":
              return yield* new Attachment.AttachmentUnavailable({ digest: request.reference.digest })
            case "AttachmentUploadBusy":
              return yield* new Attachment.AttachmentUploadBusy({ digest: request.reference.digest })
            case "AttachmentOffsetConflict": {
              const actual = Number(head.headers[offsetHeader])
              let validActual = 0
              if (Number.isSafeInteger(actual) && actual >= 0) validActual = actual
              return yield* new Attachment.AttachmentOffsetConflict({
                expected: 0,
                actual: validActual
              })
            }
            case "AttachmentTooLarge":
              return yield* new Attachment.AttachmentTooLarge({ limit: request.reference.bytes })
            case "CapacityExceeded":
              return yield* new ReplicaError.CapacityExceeded({ resource: "attachment storage", limit: 0 })
            default:
              if (head.status >= 500) return yield* new ReplicaError.ServerUnavailable()
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Attachment HTTP request failed with status ${head.status}`
              })
          }
        }
        const offsetValue = head.headers[offsetHeader]
        let offset = Number.NaN
        if (offsetValue !== undefined) offset = Number(offsetValue)
        if (!Number.isSafeInteger(offset) || offset < 0 || offset > request.reference.bytes) {
          return yield* new ReplicaError.ProtocolInvalid({
            message: "Attachment response has an invalid upload offset"
          })
        }
        if (head.headers[completeHeader] === "true") return yield* Effect.void
        const patchBase = yield* requestFor("PATCH", request)
        const offsetRequest = HttpClientRequest.setHeader(patchBase, offsetHeader, String(offset))
        const patchRequest = HttpClientRequest.bodyStream(
          offsetRequest,
          request.bytes(offset),
          { contentLength: request.reference.bytes - offset }
        )
        const patched = yield* client.execute(patchRequest).pipe(
          Effect.catchTag("HttpClientError", () => Effect.fail(new ReplicaError.ServerUnavailable())),
          Effect.withSpan("AttachmentHttpClient.patch", {
            attributes: {
              "attachment.bytes": request.reference.bytes,
              "attachment.offset": offset
            }
          })
        )
        if (patched.status < 200 || patched.status >= 300) {
          switch (patched.headers[errorHeader]) {
            case "CredentialRejected":
              return yield* new ReplicaError.CredentialRejected()
            case "AuthorizationDenied":
              return yield* new ReplicaError.AuthorizationDenied({ reason: { _tag: "AttachmentDenied" } })
            case "AttachmentUnavailable":
            case "AttachmentNotFound":
              return yield* new Attachment.AttachmentUnavailable({ digest: request.reference.digest })
            case "AttachmentUploadBusy":
              return yield* new Attachment.AttachmentUploadBusy({ digest: request.reference.digest })
            case "AttachmentOffsetConflict": {
              const actual = Number(patched.headers[offsetHeader])
              let validActual = 0
              if (Number.isSafeInteger(actual) && actual >= 0) validActual = actual
              return yield* new Attachment.AttachmentOffsetConflict({
                expected: offset,
                actual: validActual
              })
            }
            case "AttachmentTooLarge":
              return yield* new Attachment.AttachmentTooLarge({ limit: request.reference.bytes })
            case "CapacityExceeded":
              return yield* new ReplicaError.CapacityExceeded({ resource: "attachment storage", limit: 0 })
            default:
              if (patched.status >= 500) return yield* new ReplicaError.ServerUnavailable()
              return yield* new ReplicaError.ProtocolInvalid({
                message: `Attachment HTTP request failed with status ${patched.status}`
              })
          }
        }
        const uploadedValue = patched.headers[offsetHeader]
        let uploaded = Number.NaN
        if (uploadedValue !== undefined) uploaded = Number(uploadedValue)
        if (!Number.isSafeInteger(uploaded) || uploaded < 0 || uploaded > request.reference.bytes) {
          return yield* new ReplicaError.ProtocolInvalid({
            message: "Attachment response has an invalid upload offset"
          })
        }
        if (uploaded !== request.reference.bytes || patched.headers[completeHeader] !== "true") {
          return yield* new Attachment.AttachmentUnavailable({ digest: request.reference.digest })
        }
        return yield* Effect.void
      })

      const download: AttachmentTransfer.Service["download"] = (request) =>
        Stream.unwrap(Effect.gen(function*() {
          let httpRequest = yield* requestFor("GET", request)
          if (request.range !== undefined) {
            let end = ""
            if (request.range.length !== undefined) {
              end = String(request.range.offset + request.range.length - 1)
            }
            httpRequest = HttpClientRequest.setHeader(httpRequest, "range", `bytes=${request.range.offset}-${end}`)
          }
          const response = yield* client.execute(httpRequest).pipe(
            Effect.catchTag("HttpClientError", () => Effect.fail(new ReplicaError.ServerUnavailable())),
            Effect.withSpan("AttachmentHttpClient.get", {
              attributes: {
                "attachment.bytes": request.reference.bytes,
                "attachment.range": request.range !== undefined
              }
            })
          )
          if (response.status < 200 || response.status >= 300) {
            switch (response.headers[errorHeader]) {
              case "CredentialRejected":
                return yield* new ReplicaError.CredentialRejected()
              case "AuthorizationDenied":
                return yield* new ReplicaError.AuthorizationDenied({ reason: { _tag: "AttachmentDenied" } })
              case "AttachmentUnavailable":
              case "AttachmentNotFound":
                return yield* new Attachment.AttachmentUnavailable({ digest: request.reference.digest })
              case "AttachmentUploadBusy":
                return yield* new Attachment.AttachmentUploadBusy({ digest: request.reference.digest })
              case "AttachmentOffsetConflict": {
                const actual = Number(response.headers[offsetHeader])
                let validActual = 0
                if (Number.isSafeInteger(actual) && actual >= 0) validActual = actual
                return yield* new Attachment.AttachmentOffsetConflict({
                  expected: 0,
                  actual: validActual
                })
              }
              case "AttachmentTooLarge":
                return yield* new Attachment.AttachmentTooLarge({ limit: request.reference.bytes })
              case "CapacityExceeded":
                return yield* new ReplicaError.CapacityExceeded({ resource: "attachment storage", limit: 0 })
              default:
                if (response.status >= 500) return yield* new ReplicaError.ServerUnavailable()
                return yield* new ReplicaError.ProtocolInvalid({
                  message: `Attachment HTTP request failed with status ${response.status}`
                })
            }
          }
          return response.stream.pipe(
            Stream.catchTag("HttpClientError", () => Stream.fail(new ReplicaError.ServerUnavailable()))
          )
        })).pipe(Stream.withSpan("AttachmentHttpClient.download", {
          attributes: {
            "attachment.bytes": request.reference.bytes,
            "attachment.range": request.range !== undefined
          }
        }))

      return AttachmentTransfer.AttachmentTransfer.of({ upload, download })
    })
  )
