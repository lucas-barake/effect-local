import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Attachment from "./Attachment.js"
import * as Canonical from "./Canonical.js"

export const maximumControlBytes = 64 * 1024
export const maximumGrantUrlLength = 8 * 1024
export const maximumGrantHeaders = 32
export const maximumGrantHeaderNameLength = 128
export const maximumGrantHeaderValueLength = 8 * 1024

const OpaqueId = Schema.NonEmptyString.check(Schema.isMaxLength(512))

export const AttemptId = OpaqueId.pipe(
  Schema.brand("@lucas-barake/effect-local/AttachmentUploadAttemptId")
)
export type AttemptId = typeof AttemptId.Type

export const GrantId = OpaqueId.pipe(
  Schema.brand("@lucas-barake/effect-local/AttachmentGrantId")
)
export type GrantId = typeof GrantId.Type

export const GrantUrl = Schema.NonEmptyString.check(Schema.isMaxLength(maximumGrantUrlLength)).pipe(
  Schema.brand("@lucas-barake/effect-local/AttachmentGrantUrl")
)
export type GrantUrl = typeof GrantUrl.Type

export const GrantHeaderName = Schema.String.check(
  Schema.isMaxLength(maximumGrantHeaderNameLength),
  Schema.isPattern(/^[!#$%&'*+.^_`|~0-9a-z-]+$/)
).pipe(Schema.brand("@lucas-barake/effect-local/AttachmentGrantHeaderName"))
export type GrantHeaderName = typeof GrantHeaderName.Type

export const GrantHeaderValue = Schema.String.check(
  Schema.isMaxLength(maximumGrantHeaderValueLength),
  Schema.isPattern(/^[^\r\n]*$/)
).pipe(Schema.brand("@lucas-barake/effect-local/AttachmentGrantHeaderValue"))
export type GrantHeaderValue = typeof GrantHeaderValue.Type

export const GrantHeader = Schema.Struct({
  name: GrantHeaderName,
  value: GrantHeaderValue
})
export type GrantHeader = typeof GrantHeader.Type

const distinctHeaderNames = Schema.makeFilter<ReadonlyArray<GrantHeader>>(
  (headers) => {
    if (new Set(headers.map((header) => header.name)).size === headers.length) return undefined
    return "attachment grant header names must be unique"
  }
)

export const GrantHeaders = Schema.Array(GrantHeader).check(
  Schema.isMaxLength(maximumGrantHeaders),
  distinctHeaderNames
)
export type GrantHeaders = typeof GrantHeaders.Type

export const DirectUploadRequest = Schema.Struct({
  method: Schema.Literal("PUT"),
  url: GrantUrl,
  headers: GrantHeaders
})
export type DirectUploadRequest = typeof DirectUploadRequest.Type

export const DirectDownloadRequest = Schema.Struct({
  method: Schema.Literal("GET"),
  url: GrantUrl,
  headers: GrantHeaders
})
export type DirectDownloadRequest = typeof DirectDownloadRequest.Type

const ExpiresAt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PartNumber = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(10_000)
)
const ChunkIndex = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveBytes = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)
)

export const UploadComplete = Schema.Struct({
  _tag: Schema.tag("UploadComplete")
})
export type UploadComplete = typeof UploadComplete.Type

export const UploadReady = Schema.Struct({
  _tag: Schema.tag("UploadReady"),
  attemptId: AttemptId
})
export type UploadReady = typeof UploadReady.Type

export const UploadPart = Schema.Struct({
  _tag: Schema.tag("UploadPart"),
  attemptId: AttemptId,
  partNumber: PartNumber,
  offset: Attachment.ByteLength,
  bytes: PositiveBytes,
  expiresAt: ExpiresAt,
  request: DirectUploadRequest
}).check(Schema.makeFilter((part) => {
  if (part.offset + part.bytes <= Number.MAX_SAFE_INTEGER) return undefined
  return "attachment upload part range exceeds the safe integer limit"
}))
export type UploadPart = typeof UploadPart.Type

export const PrepareUploadResult = Schema.Union([
  UploadComplete,
  UploadReady,
  UploadPart
])
export type PrepareUploadResult = typeof PrepareUploadResult.Type

export const VerifiedChunk = Schema.Struct({
  index: ChunkIndex,
  offset: Attachment.ByteLength,
  bytes: PositiveBytes,
  digest: Attachment.Digest
}).check(Schema.makeFilter((chunk) => {
  if (chunk.offset + chunk.bytes <= Number.MAX_SAFE_INTEGER) return undefined
  return "attachment verification chunk range exceeds the safe integer limit"
}))
export type VerifiedChunk = typeof VerifiedChunk.Type

const DownloadGrantFields = Schema.Struct({
  _tag: Schema.tag("DownloadGrant"),
  grantId: GrantId,
  expiresAt: ExpiresAt,
  chunk: VerifiedChunk,
  slice: Schema.Struct({
    offset: Attachment.ByteLength,
    length: PositiveBytes
  }),
  request: DirectDownloadRequest
})

export const DownloadGrant = DownloadGrantFields.check(Schema.makeFilter((grant) => {
  if (grant.slice.offset + grant.slice.length <= grant.chunk.bytes) return undefined
  return "attachment download slice exceeds its verification chunk"
}))
export type DownloadGrant = typeof DownloadGrant.Type

export class AttachmentControlTooLarge extends Schema.TaggedErrorClass<AttachmentControlTooLarge>(
  "@lucas-barake/effect-local/AttachmentControlTooLarge"
)("AttachmentControlTooLarge", {
  limit: Schema.Int.check(Schema.isGreaterThan(0)),
  actual: Schema.Int.check(Schema.isGreaterThan(0))
}) {}

export const encodeControl = <S extends Schema.Top,>(
  schema: S,
  value: S["Type"]
) =>
  Schema.encodeEffect(schema)(value).pipe(
    Effect.flatMap(Canonical.stringifyEffect),
    Effect.flatMap((encoded) => {
      const actual = new TextEncoder().encode(encoded).byteLength
      if (actual <= maximumControlBytes) return Effect.succeed(encoded)
      return Effect.fail(new AttachmentControlTooLarge({ limit: maximumControlBytes, actual }))
    })
  )
