import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Attachment from "../src/Attachment.js"
import * as AttachmentTransfer from "../src/AttachmentTransfer.js"

const digest = Attachment.Digest.make(
  "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
)

const decodePart = (headers: ReadonlyArray<{ readonly name: string; readonly value: string }>) =>
  Schema.decodeUnknownEffect(AttachmentTransfer.PrepareUploadResult)({
    _tag: "UploadPart",
    attemptId: "attempt-1",
    partNumber: 1,
    offset: 0,
    bytes: 5,
    expiresAt: 1_900_000_000_000,
    request: {
      method: "PUT",
      url: "https://objects.example/upload?signature=secret",
      headers
    }
  })

describe("attachment transfer protocol", () => {
  it.effect(
    "decodes one bounded direct upload grant",
    Effect.fnUntraced(function*() {
      const decoded = yield* decodePart([
        { name: "content-type", value: "application/octet-stream" },
        { name: "x-provider-checksum", value: "checksum" }
      ])
      assert.strictEqual(decoded._tag, "UploadPart")
      if (decoded._tag === "UploadPart") {
        assert.strictEqual(decoded.partNumber, 1)
        assert.strictEqual(decoded.request.url, "https://objects.example/upload?signature=secret")
      }
    })
  )

  it.effect(
    "rejects unbounded, duplicate, or unsafe grant headers",
    Effect.fnUntraced(function*() {
      const tooMany = Array.from({ length: AttachmentTransfer.maximumGrantHeaders + 1 }, (_, index) => ({
        name: `x-header-${index}`,
        value: "value"
      }))
      for (
        const headers of [
          tooMany,
          [{ name: "x-token", value: "safe\r\ninjected: value" }],
          [{ name: "X-Uppercase", value: "value" }],
          [{ name: "x-duplicate", value: "one" }, { name: "x-duplicate", value: "two" }]
        ]
      ) {
        const result = yield* decodePart(headers).pipe(Effect.result)
        assert.isTrue(Result.isFailure(result))
      }
    })
  )

  it.effect(
    "bounds identifiers, URLs, values, and the encoded control frame",
    Effect.fnUntraced(function*() {
      const oversized = yield* decodePart([{
        name: "x-value",
        value: "x".repeat(AttachmentTransfer.maximumGrantHeaderValueLength + 1)
      }]).pipe(Effect.result)
      assert.isTrue(Result.isFailure(oversized))

      const oversizedUrl = yield* Schema.decodeUnknownEffect(AttachmentTransfer.DirectUploadRequest)({
        method: "PUT",
        url: `https://objects.example/${"x".repeat(AttachmentTransfer.maximumGrantUrlLength)}`,
        headers: []
      }).pipe(Effect.result)
      assert.isTrue(Result.isFailure(oversizedUrl))

      const encoded = yield* AttachmentTransfer.encodeControl(AttachmentTransfer.PrepareUploadResult, {
        _tag: "UploadReady",
        attemptId: AttachmentTransfer.AttemptId.make("attempt-1")
      })
      assert.isBelow(new TextEncoder().encode(encoded).byteLength, AttachmentTransfer.maximumControlBytes)
    })
  )

  it.effect(
    "describes one verified download chunk and caller slice",
    Effect.fnUntraced(function*() {
      const decoded = yield* Schema.decodeUnknownEffect(AttachmentTransfer.DownloadGrant)({
        _tag: "DownloadGrant",
        grantId: "grant-1",
        expiresAt: 1_900_000_000_000,
        chunk: {
          index: 2,
          offset: 10,
          bytes: 5,
          digest
        },
        slice: { offset: 1, length: 3 },
        request: {
          method: "GET",
          url: "https://cdn.example/object?signature=secret",
          headers: [{ name: "range", value: "bytes=10-14" }]
        }
      })
      assert.strictEqual(decoded.chunk.digest, digest)
      assert.deepStrictEqual(decoded.slice, { offset: 1, length: 3 })
    })
  )
})
