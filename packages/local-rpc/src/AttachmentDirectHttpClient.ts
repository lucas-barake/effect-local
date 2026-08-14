import * as AttachmentTransfer from "@lucas-barake/effect-local/AttachmentTransfer"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"

const Operation = Schema.Literals(["Upload", "Download"])
export type Operation = typeof Operation.Type

const GrantRejectionReason = Schema.Literals([
  "InvalidUrl",
  "InsecureUrl",
  "PrivateAddress",
  "OriginNotAllowed",
  "UserInfo",
  "Fragment",
  "UnsafeHeader",
  "InvalidRange"
])
export type GrantRejectionReason = typeof GrantRejectionReason.Type

export class AttachmentDirectHttpConfigurationError
  extends Schema.TaggedErrorClass<AttachmentDirectHttpConfigurationError>(
    "@lucas-barake/effect-local-rpc/AttachmentDirectHttpConfigurationError"
  )("AttachmentDirectHttpConfigurationError", {
    reason: Schema.Literals(["EmptyAllowlist", "InvalidOrigin", "InsecureOrigin", "PrivateOrigin"])
  })
{}

export class AttachmentGrantRejected extends Schema.TaggedErrorClass<AttachmentGrantRejected>(
  "@lucas-barake/effect-local-rpc/AttachmentGrantRejected"
)("AttachmentGrantRejected", { operation: Operation, reason: GrantRejectionReason }) {}

export class AttachmentGrantExpired extends Schema.TaggedErrorClass<AttachmentGrantExpired>(
  "@lucas-barake/effect-local-rpc/AttachmentGrantExpired"
)("AttachmentGrantExpired", { operation: Operation }) {}

export class AttachmentTransferUnavailable extends Schema.TaggedErrorClass<AttachmentTransferUnavailable>(
  "@lucas-barake/effect-local-rpc/AttachmentTransferUnavailable"
)("AttachmentTransferUnavailable", { operation: Operation }) {}

export class AttachmentTransferStatusError extends Schema.TaggedErrorClass<AttachmentTransferStatusError>(
  "@lucas-barake/effect-local-rpc/AttachmentTransferStatusError"
)("AttachmentTransferStatusError", { operation: Operation, status: Schema.Int }) {}

export class AttachmentTransferLengthMismatch extends Schema.TaggedErrorClass<AttachmentTransferLengthMismatch>(
  "@lucas-barake/effect-local-rpc/AttachmentTransferLengthMismatch"
)("AttachmentTransferLengthMismatch", {
  operation: Operation,
  expected: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  actual: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
}) {}

export class AttachmentTransferRangeMismatch extends Schema.TaggedErrorClass<AttachmentTransferRangeMismatch>(
  "@lucas-barake/effect-local-rpc/AttachmentTransferRangeMismatch"
)("AttachmentTransferRangeMismatch", {
  expectedOffset: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  expectedBytes: Schema.Int.check(Schema.isGreaterThan(0))
}) {}

export type AttachmentDirectHttpError =
  | AttachmentGrantRejected
  | AttachmentGrantExpired
  | AttachmentTransferUnavailable
  | AttachmentTransferStatusError
  | AttachmentTransferLengthMismatch
  | AttachmentTransferRangeMismatch

export interface Options {
  readonly uploadOrigins: ReadonlyArray<string>
  readonly downloadOrigins: ReadonlyArray<string>
  readonly insecureDevelopmentOrigins: ReadonlyArray<string>
}

export interface Service {
  readonly upload: (
    grant: AttachmentTransfer.UploadPart,
    bytes: Stream.Stream<Uint8Array, ReplicaError.ReplicaError>
  ) => Effect.Effect<void, AttachmentDirectHttpError>
  readonly download: (
    grant: AttachmentTransfer.DownloadGrant
  ) => Stream.Stream<Uint8Array, AttachmentDirectHttpError>
}

export class AttachmentDirectHttpClient extends Context.Service<AttachmentDirectHttpClient, Service>()(
  "@lucas-barake/effect-local-rpc/AttachmentDirectHttpClient"
) {}

const unsafeHeaders = new Set([
  "authorization",
  "baggage",
  "connection",
  "content-length",
  "cookie",
  "cookie2",
  "date",
  "expect",
  "forwarded",
  "host",
  "keep-alive",
  "origin",
  "proxy-authorization",
  "referer",
  "te",
  "traceparent",
  "tracestate",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "via"
])

const isUnsafeHeader = (name: string) =>
  unsafeHeaders.has(name) ||
  name.startsWith("proxy-") ||
  name.startsWith("sec-") ||
  name.startsWith("x-forwarded-") ||
  name.startsWith("x-http-method-")

const ipv4 = (hostname: string): ReadonlyArray<number> | undefined => {
  const parts = hostname.split(".")
  if (parts.length !== 4) return undefined
  const bytes = parts.map(Number)
  if (bytes.every((byte) => Number.isInteger(byte) && byte >= 0 && byte <= 255)) return bytes
  return undefined
}

const isPrivateIpv4 = (bytes: ReadonlyArray<number>) => {
  const [a, b] = bytes
  return a === 0 ||
    a === 10 ||
    a === 127 ||
    (a === 100 && b >= 64 && b <= 127) ||
    (a === 169 && b === 254) ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && (b === 0 || b === 168)) ||
    (a === 198 && (b === 18 || b === 19)) ||
    a >= 224
}

const isPrivateHost = (hostname: string) => {
  const normalized = hostname.toLowerCase()
  if (normalized === "localhost" || normalized.endsWith(".localhost")) return true
  const v4 = ipv4(normalized)
  if (v4 !== undefined) return isPrivateIpv4(v4)
  if (!normalized.startsWith("[") || !normalized.endsWith("]")) return false
  const v6 = normalized.slice(1, -1)
  if (v6 === "::" || v6 === "::1") return true
  const first = Number.parseInt(v6.split(":", 1)[0] || "0", 16)
  if ((first & 0xfe00) === 0xfc00 || (first & 0xffc0) === 0xfe80 || (first & 0xff00) === 0xff00) return true
  const mapped = /^::(?:ffff:)?([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(v6)
  if (mapped === null) return false
  const high = Number.parseInt(mapped[1], 16)
  const low = Number.parseInt(mapped[2], 16)
  return isPrivateIpv4([high >>> 8, high & 255, low >>> 8, low & 255])
}

const parseUrl = (input: string) => {
  const parsed = Result.try(() => new URL(input))
  if (Result.isSuccess(parsed)) return parsed.success
  return undefined
}

const isOriginUrl = (url: URL) =>
  url.username === "" &&
  url.password === "" &&
  url.hash === "" &&
  url.search === "" &&
  (url.pathname === "" || url.pathname === "/")

const parseDevelopmentOrigins = (origins: ReadonlyArray<string>) => {
  const allowed = new Set<string>()
  for (const input of origins) {
    const url = parseUrl(input)
    if (url === undefined || !isOriginUrl(url)) {
      return Effect.fail(new AttachmentDirectHttpConfigurationError({ reason: "InvalidOrigin" }))
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return Effect.fail(new AttachmentDirectHttpConfigurationError({ reason: "InsecureOrigin" }))
    }
    allowed.add(url.origin)
  }
  return Effect.succeed(allowed)
}

const parseAllowedOrigins = (
  origins: ReadonlyArray<string>,
  insecureDevelopmentOrigins: ReadonlySet<string>
) => {
  if (origins.length === 0) {
    return Effect.fail(new AttachmentDirectHttpConfigurationError({ reason: "EmptyAllowlist" }))
  }
  const allowed = new Set<string>()
  for (const input of origins) {
    const url = parseUrl(input)
    if (url === undefined || !isOriginUrl(url)) {
      return Effect.fail(new AttachmentDirectHttpConfigurationError({ reason: "InvalidOrigin" }))
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return Effect.fail(new AttachmentDirectHttpConfigurationError({ reason: "InsecureOrigin" }))
    }
    if (url.protocol === "http:" && !insecureDevelopmentOrigins.has(url.origin)) {
      return Effect.fail(new AttachmentDirectHttpConfigurationError({ reason: "InsecureOrigin" }))
    }
    if (isPrivateHost(url.hostname) && !insecureDevelopmentOrigins.has(url.origin)) {
      return Effect.fail(new AttachmentDirectHttpConfigurationError({ reason: "PrivateOrigin" }))
    }
    allowed.add(url.origin)
  }
  return Effect.succeed(allowed)
}

const validateUrl = (
  input: string,
  operation: Operation,
  allowedOrigins: ReadonlySet<string>,
  insecureDevelopmentOrigins: ReadonlySet<string>
) => {
  const url = parseUrl(input)
  if (url === undefined) return Effect.fail(new AttachmentGrantRejected({ operation, reason: "InvalidUrl" }))
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    return Effect.fail(new AttachmentGrantRejected({ operation, reason: "InsecureUrl" }))
  }
  if (url.username !== "" || url.password !== "") {
    return Effect.fail(new AttachmentGrantRejected({ operation, reason: "UserInfo" }))
  }
  if (url.hash !== "") return Effect.fail(new AttachmentGrantRejected({ operation, reason: "Fragment" }))
  if (url.protocol === "http:" && !insecureDevelopmentOrigins.has(url.origin)) {
    return Effect.fail(new AttachmentGrantRejected({ operation, reason: "InsecureUrl" }))
  }
  if (isPrivateHost(url.hostname) && !insecureDevelopmentOrigins.has(url.origin)) {
    return Effect.fail(new AttachmentGrantRejected({ operation, reason: "PrivateAddress" }))
  }
  if (!allowedOrigins.has(url.origin)) {
    return Effect.fail(new AttachmentGrantRejected({ operation, reason: "OriginNotAllowed" }))
  }
  return Effect.succeed(url)
}

const validateHeaders = (
  headers: AttachmentTransfer.GrantHeaders,
  operation: Operation
) => {
  const result: Record<string, string> = Object.create(null)
  for (const header of headers) {
    if (isUnsafeHeader(header.name)) {
      return Effect.fail(new AttachmentGrantRejected({ operation, reason: "UnsafeHeader" }))
    }
    result[header.name] = header.value
  }
  return Effect.succeed(result)
}

const requestInit: RequestInit = {
  cache: "no-store",
  credentials: "omit",
  redirect: "manual",
  referrerPolicy: "no-referrer"
}

const execute = <E extends { readonly _tag: string }, R,>(
  client: HttpClient.HttpClient.With<E, R>,
  request: HttpClientRequest.HttpClientRequest
) =>
  client.execute(request).pipe(
    Effect.provideService(FetchHttpClient.RequestInit, requestInit),
    Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
    Effect.provideService(HttpClient.TracerPropagationEnabled, false)
  )

const makeService = Effect.fnUntraced(function*(options: Options) {
  const insecureDevelopmentOrigins = yield* parseDevelopmentOrigins(options.insecureDevelopmentOrigins)
  const uploadOrigins = yield* parseAllowedOrigins(options.uploadOrigins, insecureDevelopmentOrigins)
  const downloadOrigins = yield* parseAllowedOrigins(options.downloadOrigins, insecureDevelopmentOrigins)
  const client = yield* HttpClient.HttpClient

  const upload: Service["upload"] = Effect.fn("AttachmentDirectHttpClient.upload", { kind: "client" })(
    function*(input, source) {
      yield* Effect.annotateCurrentSpan({ operation: "upload", bytes: input.bytes })
      const grant = yield* Schema.decodeUnknownEffect(AttachmentTransfer.UploadPart)(input).pipe(
        Effect.catchTag(
          "SchemaError",
          () => Effect.fail(new AttachmentGrantRejected({ operation: "Upload", reason: "InvalidUrl" }))
        )
      )
      const now = yield* Clock.currentTimeMillis
      if (now >= grant.expiresAt) return yield* new AttachmentGrantExpired({ operation: "Upload" })
      const url = yield* validateUrl(grant.request.url, "Upload", uploadOrigins, insecureDevelopmentOrigins)
      const headers = yield* validateHeaders(grant.request.headers, "Upload")
      let actual = 0
      const body = source.pipe(Stream.tap((chunk) => {
        actual += chunk.byteLength
        return Effect.void
      }))
      const request = HttpClientRequest.put(url, { headers }).pipe(
        HttpClientRequest.bodyStream(body, { contentLength: grant.bytes })
      )
      const response = yield* execute(client.pipe(HttpClient.withScope), request).pipe(
        Effect.catchTag(
          "HttpClientError",
          () => Effect.fail(new AttachmentTransferUnavailable({ operation: "Upload" }))
        ),
        Effect.scoped
      )
      if (response.status === 401 || response.status === 403) {
        return yield* new AttachmentGrantExpired({ operation: "Upload" })
      }
      if (response.status !== 200) {
        return yield* new AttachmentTransferStatusError({ operation: "Upload", status: response.status })
      }
      if (actual !== grant.bytes) {
        return yield* new AttachmentTransferLengthMismatch({
          operation: "Upload",
          expected: grant.bytes,
          actual
        })
      }
      return undefined
    }
  )

  const download: Service["download"] = (input) => {
    let actual = 0
    return Stream.unwrap(Effect.gen(function*() {
      const grant = yield* Schema.decodeUnknownEffect(AttachmentTransfer.DownloadGrant)(input).pipe(
        Effect.catchTag(
          "SchemaError",
          () => Effect.fail(new AttachmentGrantRejected({ operation: "Download", reason: "InvalidUrl" }))
        )
      )
      const now = yield* Clock.currentTimeMillis
      if (now >= grant.expiresAt) return yield* new AttachmentGrantExpired({ operation: "Download" })
      const url = yield* validateUrl(grant.request.url, "Download", downloadOrigins, insecureDevelopmentOrigins)
      const headers = yield* validateHeaders(grant.request.headers, "Download")
      const range = headers.range
      const expectedRange = `bytes=${grant.chunk.offset}-${grant.chunk.offset + grant.chunk.bytes - 1}`
      if (range !== undefined && range !== expectedRange) {
        return yield* new AttachmentGrantRejected({ operation: "Download", reason: "InvalidRange" })
      }
      const request = HttpClientRequest.get(url, { headers })
      const response = yield* execute(client.pipe(HttpClient.withScope), request).pipe(
        Effect.catchTag(
          "HttpClientError",
          () => Effect.fail(new AttachmentTransferUnavailable({ operation: "Download" }))
        )
      )
      let expectedStatus = 200
      if (range !== undefined) expectedStatus = 206
      if (response.status === 401 || response.status === 403) {
        return yield* new AttachmentGrantExpired({ operation: "Download" })
      }
      if (response.status !== expectedStatus) {
        return yield* new AttachmentTransferStatusError({ operation: "Download", status: response.status })
      }
      const contentLength = response.headers["content-length"]
      if (contentLength !== String(grant.chunk.bytes)) {
        let actualLength = 0
        if (contentLength !== undefined && /^\d+$/.test(contentLength)) actualLength = Number(contentLength)
        return yield* new AttachmentTransferLengthMismatch({
          operation: "Download",
          expected: grant.chunk.bytes,
          actual: actualLength
        })
      }
      if (range !== undefined) {
        const contentRange = response.headers["content-range"]
        let match: RegExpExecArray | null = null
        if (contentRange !== undefined) match = /^bytes (\d+)-(\d+)\/(\d+|\*)$/.exec(contentRange)
        const expectedEnd = grant.chunk.offset + grant.chunk.bytes - 1
        if (
          match === null || Number(match[1]) !== grant.chunk.offset || Number(match[2]) !== expectedEnd ||
          Number(match[3]) !== grant.objectBytes
        ) {
          return yield* new AttachmentTransferRangeMismatch({
            expectedOffset: grant.chunk.offset,
            expectedBytes: grant.chunk.bytes
          })
        }
      } else if (grant.chunk.offset !== 0 || response.headers["content-range"] !== undefined) {
        return yield* new AttachmentTransferRangeMismatch({
          expectedOffset: grant.chunk.offset,
          expectedBytes: grant.chunk.bytes
        })
      }
      return response.stream.pipe(
        Stream.catchTag(
          "HttpClientError",
          () => Stream.fail(new AttachmentTransferUnavailable({ operation: "Download" }))
        ),
        Stream.mapEffect((chunk) => {
          actual += chunk.byteLength
          if (actual <= grant.chunk.bytes) return Effect.succeed(chunk)
          return Effect.fail(
            new AttachmentTransferLengthMismatch({
              operation: "Download",
              expected: grant.chunk.bytes,
              actual
            })
          )
        }),
        Stream.concat(
          Stream.fromEffect(Effect.suspend(() => {
            if (actual === grant.chunk.bytes) return Effect.void
            return Effect.fail(
              new AttachmentTransferLengthMismatch({
                operation: "Download",
                expected: grant.chunk.bytes,
                actual
              })
            )
          })).pipe(Stream.drain)
        )
      )
    })).pipe(Stream.withSpan("AttachmentDirectHttpClient.download", {
      kind: "client",
      attributes: { operation: "download", bytes: input.chunk.bytes }
    }))
  }

  return AttachmentDirectHttpClient.of({ upload, download })
})

export const layer = (
  options: Options
): Layer.Layer<AttachmentDirectHttpClient, AttachmentDirectHttpConfigurationError, HttpClient.HttpClient> =>
  Layer.effect(AttachmentDirectHttpClient, makeService(options))
