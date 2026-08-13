import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as AttachmentStorage from "@lucas-barake/effect-local-sql/AttachmentStorage"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as BrowserAttachmentStorage from "../src/BrowserAttachmentStorage.js"
import * as AttachmentDirectory from "../src/internal/AttachmentDirectory.js"
import * as AttachmentWorkerProtocol from "../src/internal/AttachmentWorkerProtocol.js"

const collectBytes = <E extends { readonly _tag: string }, R,>(stream: Stream.Stream<Uint8Array, E, R>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))))
  )

const makeDirectory = () => {
  const files = new Map<AttachmentStorage.ObjectKey, Uint8Array>()
  const notFound = (key: AttachmentStorage.ObjectKey) => new Attachment.AttachmentNotFound({ key })
  const service = AttachmentDirectory.AttachmentDirectory.of({
    create: (key) => Effect.sync(() => files.set(key, new Uint8Array(0))),
    offset: (key) => {
      const bytes = files.get(key)
      if (bytes === undefined) return Effect.fail(notFound(key))
      return Effect.succeed(bytes.length)
    },
    write: (key, expectedOffset, bytes) => {
      const current = files.get(key)
      if (current === undefined) return Effect.fail(notFound(key))
      if (current.length !== expectedOffset) {
        return Effect.fail(
          new Attachment.AttachmentOffsetConflict({ expected: expectedOffset, actual: current.length })
        )
      }
      return Effect.sync(() => {
        const next = new Uint8Array(current.length + bytes.length)
        next.set(current)
        next.set(bytes, current.length)
        files.set(key, next)
        return next.length
      })
    },
    read: (key, offset, length) => {
      const bytes = files.get(key)
      if (bytes === undefined) return Effect.fail(notFound(key))
      return Effect.succeed(bytes.slice(offset, offset + length))
    },
    exists: (key) => Effect.succeed(files.has(key)),
    remove: (key) => Effect.sync(() => void files.delete(key))
  })
  return { files, service }
}

describe("browser attachment storage", () => {
  it.effect(
    "streams bounded bytes through an application owned port and reads them after a client restart",
    Effect.fnUntraced(function*() {
      const directory = makeDirectory()
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          channel.port1.close()
          channel.port2.close()
        })
      )
      yield* AttachmentWorkerProtocol.serve(channel.port1, { maximumBytes: 5 }).pipe(
        Effect.provideService(AttachmentDirectory.AttachmentDirectory, directory.service),
        Effect.forkScoped({ startImmediately: true })
      )
      const layer = BrowserAttachmentStorage.layerMessagePort(channel.port2, {
        maximumBytes: 5,
        readChunkBytes: 2
      }).pipe(Layer.provide(NodeCrypto.layer))

      const firstScope = yield* Scope.make()
      const first = yield* Layer.buildWithScope(layer, firstScope)
      const storage = Context.get(first, AttachmentStorage.AttachmentStorage)
      const firstChunk = Uint8Array.from([1, 2])
      const secondChunk = Uint8Array.from([3, 4, 5])
      const staged = yield* storage.stage(Stream.make(firstChunk, secondChunk))
      yield* Scope.close(firstScope, Exit.void)

      const secondScope = yield* Scope.make()
      const second = yield* Layer.buildWithScope(layer, secondScope)
      const restarted = Context.get(second, AttachmentStorage.AttachmentStorage)
      assert.deepStrictEqual(
        yield* collectBytes(restarted.read(staged.key, staged.reference)),
        Uint8Array.from([1, 2, 3, 4, 5])
      )
      assert.deepStrictEqual(
        yield* collectBytes(restarted.read(staged.key, staged.reference, { offset: 1, length: 3 })),
        Uint8Array.from([2, 3, 4])
      )
      yield* restarted.verify(staged.key, staged.reference)

      const empty = yield* restarted.stage(Stream.empty)
      assert.strictEqual(empty.reference.bytes, 0)
      assert.deepStrictEqual(yield* collectBytes(restarted.read(empty.key, empty.reference)), new Uint8Array(0))
      yield* restarted.verify(empty.key, empty.reference)

      const oversized = Uint8Array.from([1, 2, 3, 4, 5, 6])
      const oversizedStream = Stream.make(oversized)
      const tooLarge = yield* Effect.exit(restarted.stage(oversizedStream))
      assert.isTrue(Exit.isFailure(tooLarge))
      if (Exit.isFailure(tooLarge)) {
        const failure = Option.getOrThrow(Cause.findErrorOption(tooLarge.cause))
        assert.strictEqual(failure._tag, "AttachmentTooLarge")
      }
      assert.strictEqual(directory.files.size, 2)
      yield* restarted.remove(staged.key)
      yield* restarted.remove(empty.key)
      assert.isFalse(yield* restarted.exists(staged.key))
      yield* Scope.close(secondScope, Exit.void)
    }, Effect.scoped)
  )
})
