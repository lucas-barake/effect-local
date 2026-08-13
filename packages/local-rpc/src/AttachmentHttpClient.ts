import * as AttachmentTransfer from "@lucas-barake/effect-local-sql/AttachmentTransfer"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as Stream from "effect/Stream"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import type * as HttpClientResponse from "effect/unstable/http/HttpClientResponse"
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

const parseOffset = (response: HttpClientResponse.HttpClientResponse, reference: Attachment.Reference) => {
  const value = response.headers[offsetHeader]
  let offset = Number.NaN
  if (value !== undefined) offset = Number(value)
  if (Number.isSafeInteger(offset) && offset >= 0 && offset <= reference.bytes) return Effect.succeed(offset)
  return Effect.fail(new ReplicaError.ProtocolInvalid({ message: "Attachment response has an invalid upload offset" }))
}

const remoteFailure = (
  response: HttpClientResponse.HttpClientResponse,
  reference: Attachment.Reference
): ReplicaError.ReplicaError => {
  switch (response.headers[errorHeader]) {
    case "CredentialRejected":
      return new ReplicaError.CredentialRejected()
    case "AuthorizationDenied":
      return new ReplicaError.AuthorizationDenied({ reason: { _tag: "AttachmentDenied" } })
    case "AttachmentUnavailable":
    case "AttachmentNotFound":
      return new Attachment.AttachmentUnavailable({ digest: reference.digest })
    case "AttachmentUploadBusy":
      return new Attachment.AttachmentUploadBusy({ digest: reference.digest })
    case "AttachmentOffsetConflict": {
      const actual = Number(response.headers[offsetHeader])
      let validActual = 0
      if (Number.isSafeInteger(actual) && actual >= 0) validActual = actual
      return new Attachment.AttachmentOffsetConflict({
        expected: 0,
        actual: validActual
      })
    }
    case "AttachmentTooLarge":
      return new Attachment.AttachmentTooLarge({ limit: reference.bytes })
    case "CapacityExceeded":
      return new ReplicaError.CapacityExceeded({ resource: "attachment storage", limit: 0 })
    default:
      if (response.status >= 500) return new ReplicaError.ServerUnavailable()
      return new ReplicaError.ProtocolInvalid({
        message: `Attachment HTTP request failed with status ${response.status}`
      })
  }
}

const accepted = (
  response: HttpClientResponse.HttpClientResponse,
  reference: Attachment.Reference
) => {
  if (response.status >= 200 && response.status < 300) return Effect.succeed(response)
  return Effect.fail(remoteFailure(response, reference))
}

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

      const requestFor = Effect.fnUntraced(function*(
        method: "GET" | "HEAD" | "PATCH",
        request: AttachmentTransfer.UploadRequest | AttachmentTransfer.DownloadRequest
      ) {
        const credential = yield* credentials.acquire
        const url = `${baseUrl}${path}/${encodeURIComponent(request.spaceId)}/${
          encodeURIComponent(request.reference.digest)
        }`
        let initial: HttpClientRequest.HttpClientRequest
        switch (method) {
          case "GET":
            initial = HttpClientRequest.get(url)
            break
          case "HEAD":
            initial = HttpClientRequest.head(url)
            break
          case "PATCH":
            initial = HttpClientRequest.patch(url)
            break
        }
        return HttpClientRequest.setHeaders(initial, {
          authorization: `Bearer ${Redacted.value(credential.bearer)}`,
          [clientHeader]: request.clientId,
          [incarnationHeader]: request.membershipIncarnation,
          [bytesHeader]: String(request.reference.bytes)
        })
      })

      const execute = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(
          Effect.mapError(() => new ReplicaError.ServerUnavailable())
        )

      const upload: AttachmentTransfer.Service["upload"] = Effect.fnUntraced(function*(request) {
        const headRequest = yield* requestFor("HEAD", request)
        const head = yield* execute(client.execute(headRequest))
        yield* accepted(head, request.reference)
        const offset = yield* parseOffset(head, request.reference)
        if (head.headers[completeHeader] === "true") return yield* Effect.void
        const patchBase = yield* requestFor("PATCH", request)
        const offsetRequest = HttpClientRequest.setHeader(patchBase, offsetHeader, String(offset))
        const patchRequest = HttpClientRequest.bodyStream(
          offsetRequest,
          request.bytes(offset),
          { contentLength: request.reference.bytes - offset }
        )
        const patched = yield* execute(client.execute(patchRequest))
        yield* accepted(patched, request.reference)
        const uploaded = yield* parseOffset(patched, request.reference)
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
          const response = yield* execute(client.execute(httpRequest))
          yield* accepted(response, request.reference)
          return response.stream.pipe(Stream.mapError(() => new ReplicaError.ServerUnavailable()))
        }))

      return AttachmentTransfer.AttachmentTransfer.of({ upload, download })
    })
  )
