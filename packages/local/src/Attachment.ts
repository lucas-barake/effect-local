import { sha256 } from "@noble/hashes/sha2.js"
import * as Effect from "effect/Effect"
import * as Encoding from "effect/Encoding"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"

const digestIdentifier = "EffectLocalAttachmentDigestV1"
const byteLengthIdentifier = "EffectLocalAttachmentByteLengthV1"
const referenceIdentifier = "EffectLocalAttachmentReferenceV1"

export const Digest = Schema.String.annotate({ identifier: digestIdentifier }).check(
  Schema.isPattern(/^sha256:[0-9a-f]{64}$/)
).pipe(Schema.brand("@lucas-barake/effect-local/AttachmentDigest"))
export type Digest = typeof Digest.Type

export const ByteLength = Schema.Number.annotate({ identifier: byteLengthIdentifier }).check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)
)

const PositiveByteLength = Schema.Number.check(
  Schema.isInt(),
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)
)

export const Range = Schema.Struct({
  offset: ByteLength,
  length: Schema.optionalKey(PositiveByteLength)
})
export type Range = typeof Range.Type

export const Reference = Schema.Struct({
  _tag: Schema.tag("Attachment"),
  digest: Digest,
  bytes: ByteLength
}).annotate({ identifier: referenceIdentifier }).pipe(
  Schema.brand("@lucas-barake/effect-local/AttachmentReference")
)
export type Reference = typeof Reference.Type

export const HashResult = Schema.Struct({ digest: Digest, bytes: ByteLength })
export type HashResult = typeof HashResult.Type

const Path = Schema.Array(Schema.Union([Schema.String, Schema.Number]))

export class InvalidAttachmentReference extends Schema.TaggedErrorClass<InvalidAttachmentReference>(
  "@lucas-barake/effect-local/InvalidAttachmentReference"
)("InvalidAttachmentReference", {
  path: Path,
  reason: Schema.Literals(["InvalidShape", "ConflictingLength"]),
  cause: Schema.optionalKey(Schema.Defect())
}) {}

export class AttachmentTooLarge extends Schema.TaggedErrorClass<AttachmentTooLarge>(
  "@lucas-barake/effect-local/AttachmentTooLarge"
)("AttachmentTooLarge", { limit: ByteLength }) {}

export class AttachmentNotFound extends Schema.TaggedErrorClass<AttachmentNotFound>(
  "@lucas-barake/effect-local/AttachmentNotFound"
)("AttachmentNotFound", { key: Schema.String }) {}

export class AttachmentStorageError extends Schema.TaggedErrorClass<AttachmentStorageError>(
  "@lucas-barake/effect-local/AttachmentStorageError"
)("AttachmentStorageError", { operation: Schema.String, cause: Schema.Defect() }) {}

export class AttachmentOffsetConflict extends Schema.TaggedErrorClass<AttachmentOffsetConflict>(
  "@lucas-barake/effect-local/AttachmentOffsetConflict"
)("AttachmentOffsetConflict", { expected: ByteLength, actual: ByteLength }) {}

export class AttachmentLengthMismatch extends Schema.TaggedErrorClass<AttachmentLengthMismatch>(
  "@lucas-barake/effect-local/AttachmentLengthMismatch"
)("AttachmentLengthMismatch", { expected: ByteLength, actual: ByteLength }) {}

export class AttachmentDigestMismatch extends Schema.TaggedErrorClass<AttachmentDigestMismatch>(
  "@lucas-barake/effect-local/AttachmentDigestMismatch"
)("AttachmentDigestMismatch", { expected: Digest, actual: Digest }) {}

export class InvalidAttachmentRange extends Schema.TaggedErrorClass<InvalidAttachmentRange>(
  "@lucas-barake/effect-local/InvalidAttachmentRange"
)("InvalidAttachmentRange", {
  bytes: ByteLength,
  offset: ByteLength,
  length: Schema.optionalKey(ByteLength)
}) {}

const isJsonArray = (
  value: Schema.JsonArray | Schema.JsonObject
): value is Schema.JsonArray => Array.isArray(value)

export const hash = Effect.fnUntraced(function*<E extends { readonly _tag: string }, R,>(
  bytes: Stream.Stream<Uint8Array, E, R>,
  options?: { readonly maximumBytes?: number }
): Effect.fn.Return<HashResult, E | AttachmentTooLarge, R> {
  return yield* Effect.acquireUseRelease(
    Effect.sync(() => sha256.create()),
    Effect.fnUntraced(function*(hasher) {
      let length = 0
      yield* Stream.runForEach(bytes, (chunk) => {
        length += chunk.length
        if (options?.maximumBytes !== undefined && length > options.maximumBytes) {
          return Effect.fail(new AttachmentTooLarge({ limit: options.maximumBytes }))
        }
        hasher.update(chunk)
        return Effect.void
      })
      return HashResult.make({
        digest: Digest.make(`sha256:${Encoding.encodeHex(hasher.digest())}`),
        bytes: length
      })
    }),
    (hasher) => Effect.sync(() => hasher.destroy())
  )
})

export const collect = Effect.fnUntraced(function*(
  value: typeof Schema.Json.Type
): Effect.fn.Return<ReadonlyArray<Reference>, InvalidAttachmentReference> {
  const references = new Map<Digest, Reference>()
  const visit: (
    current: typeof Schema.Json.Type,
    path: ReadonlyArray<string | number>
  ) => Effect.Effect<void, InvalidAttachmentReference> = Effect.fnUntraced(
    function*(current, path) {
      if (current === null || typeof current !== "object") return
      if (isJsonArray(current)) {
        for (let index = 0; index < current.length; index++) yield* visit(current[index], [...path, index])
        return
      }
      if (current._tag === "Attachment") {
        const reference = yield* Schema.decodeUnknownEffect(Reference)(current).pipe(
          Effect.mapError((cause) => new InvalidAttachmentReference({ path, reason: "InvalidShape", cause }))
        )
        const existing = references.get(reference.digest)
        if (existing !== undefined && existing.bytes !== reference.bytes) {
          yield* new InvalidAttachmentReference({
            path,
            reason: "ConflictingLength"
          })
          return
        }
        references.set(reference.digest, reference)
        return
      }
      for (const [key, nested] of Object.entries(current)) yield* visit(nested, [...path, key])
    }
  )
  yield* visit(value, [])
  return [...references.values()]
})
