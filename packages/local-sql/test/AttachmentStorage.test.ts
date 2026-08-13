import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as AttachmentStorage from "../src/AttachmentStorage.js"
import * as FileSystemAttachmentStorage from "../src/FileSystemAttachmentStorage.js"

const hello = Uint8Array.from([104, 101, 108, 108, 111])
const digest = Attachment.Digest.make(
  "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
)
const reference = Attachment.Reference.make({ _tag: "Attachment", digest, bytes: hello.length })

class Interrupted extends Schema.TaggedErrorClass<Interrupted>("test/Interrupted")("Interrupted", {}) {}

const layerNodeServices = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer, NodePath.layer)
const provideNodeServices = Effect.provide(layerNodeServices)

const makeStorage = Effect.fnUntraced(function*() {
  const fs = yield* FileSystem.FileSystem
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "effect-local-attachments-" })
  return yield* Layer.build(FileSystemAttachmentStorage.layer({
    directory: `${root}/objects`,
    maximumBytes: 8
  })).pipe(Effect.map(Context.get(AttachmentStorage.AttachmentStorage)))
})

const collectBytes = <E extends { readonly _tag: string }, R,>(stream: Stream.Stream<Uint8Array, E, R>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))))
  )

describe("attachment file storage", () => {
  it.effect(
    "stages bounded bytes and serves an exact range",
    Effect.fnUntraced(
      function*() {
        const storage = yield* makeStorage()
        const staged = yield* storage.stage(Stream.make(hello))
        assert.deepStrictEqual(staged.reference, reference)
        assert.strictEqual(yield* storage.offset(staged.key), hello.length)
        const range = yield* collectBytes(storage.read(staged.key, reference, { offset: 1, length: 3 }))
        assert.deepStrictEqual(range, Uint8Array.from([101, 108, 108]))

        const tooLarge = yield* storage.stage(Stream.make(new Uint8Array(9))).pipe(Effect.result)
        assert.isTrue(Result.isFailure(tooLarge))
        if (Result.isFailure(tooLarge)) assert.strictEqual(tooLarge.failure._tag, "AttachmentTooLarge")
      },
      Effect.scoped,
      provideNodeServices
    )
  )

  it.effect(
    "resumes from the durable file offset after interruption",
    Effect.fnUntraced(
      function*() {
        const storage = yield* makeStorage()
        const key = yield* storage.create()
        const first = hello.subarray(0, 3)
        const partial = Stream.concat(Stream.make(first), Stream.fail(new Interrupted()))
        const interrupted = yield* storage.append(
          key,
          reference,
          0,
          partial
        ).pipe(Effect.result)
        assert.isTrue(Result.isFailure(interrupted))
        assert.strictEqual(yield* storage.offset(key), 3)

        const conflict = yield* storage.append(key, reference, 0, Stream.empty).pipe(Effect.result)
        assert.isTrue(Result.isFailure(conflict))
        if (Result.isFailure(conflict)) {
          assert.strictEqual(conflict.failure._tag, "AttachmentOffsetConflict")
          if (conflict.failure._tag === "AttachmentOffsetConflict") assert.strictEqual(conflict.failure.actual, 3)
        }

        const remainder = Stream.make(hello.subarray(3))
        assert.strictEqual(yield* storage.append(key, reference, 3, remainder), 5)
        yield* storage.verify(key, reference)
        assert.deepStrictEqual(yield* collectBytes(storage.read(key, reference)), hello)
      },
      Effect.scoped,
      provideNodeServices
    )
  )

  it.effect(
    "rejects mismatched content and removes an incarnation idempotently",
    Effect.fnUntraced(
      function*() {
        const storage = yield* makeStorage()
        const key = yield* storage.create()
        const wrong = Uint8Array.from([119, 114, 111, 110, 103])
        yield* storage.append(key, reference, 0, Stream.make(wrong))
        const mismatch = yield* storage.verify(key, reference).pipe(Effect.result)
        assert.isTrue(Result.isFailure(mismatch))
        if (Result.isFailure(mismatch)) assert.strictEqual(mismatch.failure._tag, "AttachmentDigestMismatch")
        yield* storage.remove(key)
        yield* storage.remove(key)
        assert.strictEqual(yield* storage.exists(key), false)
      },
      Effect.scoped,
      provideNodeServices
    )
  )
})
