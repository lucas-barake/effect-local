import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Attachment from "../src/Attachment.js"
import * as Definition from "../src/Definition.js"
import * as Model from "../src/Model.js"
import { maximumMutationBytes } from "../src/Protocol.js"

const digest = Attachment.Digest.make(
  "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
)
const reference = Attachment.Reference.make({ _tag: "Attachment", digest, bytes: 5 })

describe("attachment contract", () => {
  it.effect(
    "keeps references as compact branded JSON in model schemas",
    Effect.fnUntraced(function*() {
      const Message = Model.make("Message", {
        version: 1,
        key: Schema.String,
        schema: Schema.Struct({ id: Schema.String, attachment: Attachment.Reference })
      })
      const definition = Definition.make({ version: 1, models: [Message], mutations: [] })
      const encoded = yield* Schema.encodeEffect(Message.schema)({ id: "message-1", attachment: reference })
      assert.deepStrictEqual(encoded, {
        id: "message-1",
        attachment: { _tag: "Attachment", digest, bytes: 5 }
      })
      assert.strictEqual(definition.modelByName.get("Message"), Message)
    })
  )

  it.effect(
    "hashes raw chunks without canonical JSON framing",
    Effect.fnUntraced(function*() {
      const result = yield* Attachment.hash(
        Stream.fromIterable([
          Uint8Array.from([104, 101, 108]),
          Uint8Array.from([108, 111])
        ])
      )
      assert.deepStrictEqual(result, { digest, bytes: 5 })
    })
  )

  it.effect(
    "stops hashing as soon as the configured byte bound is crossed",
    Effect.fnUntraced(function*() {
      const result = yield* Attachment.hash(
        Stream.fromIterable([
          Uint8Array.from([104, 101, 108]),
          Uint8Array.from([108, 111])
        ]),
        { maximumBytes: 4 }
      ).pipe(Effect.result)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure._tag, "AttachmentTooLarge")
        assert.strictEqual(result.failure.limit, 4)
      }
    })
  )

  it.effect(
    "discovers nested references once and rejects malformed reserved markers",
    Effect.fnUntraced(function*() {
      const Payload = Schema.Struct({
        message: Schema.String,
        primary: Attachment.Reference,
        nested: Schema.Array(Schema.Union([
          Attachment.Reference,
          Schema.Struct({ ordinaryDigest: Schema.String })
        ]))
      })
      const payload = yield* Schema.encodeEffect(Payload)({
        message: "hello",
        primary: reference,
        nested: [reference, { ordinaryDigest: digest }]
      })
      const found = yield* Attachment.collect(payload)
      assert.deepStrictEqual(found, [reference])

      const result = yield* Attachment.collect({
        nested: [{ _tag: "Attachment", digest: "sha256:not-a-digest", bytes: 5 }]
      }).pipe(Effect.result)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure._tag, "InvalidAttachmentReference")
        assert.deepStrictEqual(result.failure.path, ["nested", 0])
      }

      const conflict = yield* Attachment.collect({
        first: { _tag: "Attachment", digest, bytes: 5 },
        second: { _tag: "Attachment", digest, bytes: 6 }
      }).pipe(Effect.result)
      assert.isTrue(Result.isFailure(conflict))
      if (Result.isFailure(conflict)) {
        assert.strictEqual(conflict.failure.reason, "ConflictingLength")
        assert.deepStrictEqual(conflict.failure.path, ["second"])
      }
    })
  )

  it.effect(
    "collects references from deeply nested values within the mutation byte limit",
    Effect.fnUntraced(function*() {
      const depth = 10_000
      const encodedReference = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(reference)
      let payload: typeof Schema.Json.Type = reference
      for (let index = 0; index < depth; index++) payload = { nested: payload }

      assert.isBelow(depth * "{\"nested\":}".length + encodedReference.length, maximumMutationBytes)
      assert.deepStrictEqual(yield* Attachment.collect(payload), [reference])
    })
  )
})
