/* oxlint-disable effect/noAs, effect/noNewPromise, effect/noNewError, effect/noTernary, effect/noThrowStatement, effect-local/noFunctionEffectGen, effect-local/noNestedCalls, typescript/no-unsafe-type-assertion -- These tests model native MessagePort and OPFS host boundaries. */
import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as AttachmentStorage from "@lucas-barake/effect-local-sql/AttachmentStorage"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as BrowserAttachmentStorage from "../src/BrowserAttachmentStorage.js"
import * as AttachmentDirectory from "../src/internal/AttachmentDirectory.js"
import * as AttachmentWorkerProtocol from "../src/internal/AttachmentWorkerProtocol.js"
import * as OpfsAttachmentDirectory from "../src/internal/OpfsAttachmentDirectory.js"

const collectBytes = <E extends { readonly _tag: string }, R,>(stream: Stream.Stream<Uint8Array, E, R>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Uint8Array.from(chunks.flatMap((chunk) => Array.from(chunk))))
  )

const makeDirectory = () => {
  const files = new Map<AttachmentStorage.ObjectKey, Uint8Array>()
  let writes = 0
  const service = AttachmentDirectory.AttachmentDirectory.of({
    create: (key) => Effect.sync(() => files.set(key, new Uint8Array(0))),
    offset: (key) => {
      const bytes = files.get(key)
      if (bytes === undefined) return Effect.fail(new Attachment.AttachmentNotFound({ key }))
      return Effect.succeed(bytes.length)
    },
    write: (key, expectedOffset, bytes) => {
      writes++
      const current = files.get(key)
      if (current === undefined) return Effect.fail(new Attachment.AttachmentNotFound({ key }))
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
      if (bytes === undefined) return Effect.fail(new Attachment.AttachmentNotFound({ key }))
      return Effect.succeed(bytes.slice(offset, offset + length))
    },
    exists: (key) => Effect.succeed(files.has(key)),
    remove: (key) => Effect.sync(() => void files.delete(key))
  })
  return {
    files,
    get writes() {
      return writes
    },
    service
  }
}

const makeLayer = (port: MessagePort, maximumBytes: number) =>
  BrowserAttachmentStorage.layerMessagePort(port, {
    maximumBytes,
    readChunkBytes: 2
  }).pipe(Layer.provide(NodeCrypto.layer))

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
      const layer = makeLayer(channel.port2, 5)

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

  it.effect(
    "coalesces one byte fragments and durably flushes the final prefix on failure",
    Effect.fnUntraced(function*() {
      const directory = makeDirectory()
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          channel.port1.close()
          channel.port2.close()
        })
      )
      yield* AttachmentWorkerProtocol.serve(channel.port1, { maximumBytes: 1_024 }).pipe(
        Effect.provideService(AttachmentDirectory.AttachmentDirectory, directory.service),
        Effect.forkScoped({ startImmediately: true })
      )
      const context = yield* Layer.build(makeLayer(channel.port2, 1_024))
      const storage = Context.get(context, AttachmentStorage.AttachmentStorage)
      const bytes = Array.from({ length: 1_024 }, (_, index) => Uint8Array.of(index % 251))
      const staged = yield* storage.stage(Stream.fromIterable(bytes))
      assert.strictEqual(staged.reference.bytes, 1_024)
      assert.strictEqual(directory.writes, 1)

      const key = yield* storage.create()
      const interrupted = Stream.concat(
        Stream.fromIterable([Uint8Array.of(1), Uint8Array.of(2), Uint8Array.of(3)]),
        Stream.fail(new Attachment.AttachmentStorageError({ operation: "test.interrupted", cause: "interrupted" }))
      )
      const append = yield* storage.append(
        key,
        Attachment.Reference.make({ _tag: "Attachment", digest: staged.reference.digest, bytes: 5 }),
        0,
        interrupted
      ).pipe(Effect.result)
      assert.isTrue(Result.isFailure(append))
      assert.strictEqual(yield* storage.offset(key), 3)
      assert.strictEqual(directory.writes, 2)
    }, Effect.scoped)
  )

  it.effect(
    "removes an object when staging is interrupted while Create is in flight",
    Effect.fnUntraced(function*() {
      const createStarted = yield* Deferred.make<void>()
      const allowCreate = yield* Deferred.make<void>()
      const createCompleted = yield* Deferred.make<void>()
      const files = new Set<AttachmentStorage.ObjectKey>()
      const directory = AttachmentDirectory.AttachmentDirectory.of({
        create: (key) =>
          Effect.gen(function*() {
            files.add(key)
            yield* Deferred.succeed(createStarted, undefined)
            yield* Deferred.await(allowCreate)
            yield* Deferred.succeed(createCompleted, undefined)
          }),
        offset: (key) => files.has(key) ? Effect.succeed(0) : Effect.fail(new Attachment.AttachmentNotFound({ key })),
        write: (key) => files.has(key) ? Effect.succeed(0) : Effect.fail(new Attachment.AttachmentNotFound({ key })),
        read: (key) =>
          files.has(key)
            ? Effect.succeed(new Uint8Array(0))
            : Effect.fail(new Attachment.AttachmentNotFound({ key })),
        exists: (key) => Effect.succeed(files.has(key)),
        remove: (key) => Effect.sync(() => void files.delete(key))
      })
      const channel = new MessageChannel()
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          channel.port1.close()
          channel.port2.close()
        })
      )
      yield* AttachmentWorkerProtocol.serve(channel.port1, { maximumBytes: 8 }).pipe(
        Effect.provideService(AttachmentDirectory.AttachmentDirectory, directory),
        Effect.forkScoped({ startImmediately: true })
      )
      const context = yield* Layer.build(makeLayer(channel.port2, 8))
      const storage = Context.get(context, AttachmentStorage.AttachmentStorage)
      const fiber = yield* storage.stage(Stream.make(Uint8Array.of(1))).pipe(
        Effect.forkScoped({ startImmediately: true })
      )
      yield* Deferred.await(createStarted)
      const interruption = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Effect.yieldNow
      yield* Deferred.succeed(allowCreate, undefined)
      yield* Deferred.await(createCompleted)
      yield* Fiber.join(interruption)
      assert.strictEqual(files.size, 0)
    }, Effect.scoped)
  )

  it.effect(
    "forgets a request callback when postMessage throws",
    Effect.fnUntraced(function*() {
      let messageError: ((event: MessageEvent<unknown>) => void) | undefined
      const port = {
        addEventListener: (type: string, listener: EventListenerOrEventListenerObject) => {
          if (type === "messageerror") messageError = listener as (event: MessageEvent<unknown>) => void
        },
        removeEventListener: () => undefined,
        start: () => undefined,
        postMessage: () => {
          throw new Error("send failed")
        }
      } as unknown as MessagePort
      const context = yield* Layer.build(makeLayer(port, 8))
      const storage = Context.get(context, AttachmentStorage.AttachmentStorage)
      const result = yield* storage.create().pipe(Effect.result)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "AttachmentStorageError")

      let dataReads = 0
      messageError?.({
        get data() {
          dataReads++
          return undefined
        }
      } as MessageEvent<unknown>)
      assert.strictEqual(dataReads, 0)
    }, Effect.scoped)
  )

  it.effect("removes the exact OPFS entry when access or initialization fails after creation", () => {
    const originalNavigator = globalThis.navigator
    const removed: Array<string> = []
    let creations = 0
    const directory = {
      getFileHandle: (name: string, options?: { readonly create?: boolean }) => {
        if (options?.create === true) {
          creations++
          return Promise.resolve({
            createSyncAccessHandle: () =>
              creations === 1
                ? Promise.reject(new Error("access denied"))
                : Promise.resolve({
                  truncate: () => {
                    throw new Error("truncate failed")
                  },
                  flush: () => undefined,
                  close: () => undefined
                })
          })
        }
        return Promise.reject(new DOMException("missing", "NotFoundError"))
      },
      removeEntry: (name: string) => {
        removed.push(name)
        return Promise.resolve()
      }
    }
    return Effect.acquireUseRelease(
      Effect.sync(() => {
        Object.defineProperty(globalThis, "navigator", {
          configurable: true,
          value: {
            storage: { getDirectory: () => Promise.resolve({ getDirectoryHandle: () => Promise.resolve(directory) }) }
          }
        })
      }),
      () =>
        Effect.gen(function*() {
          const context = yield* Layer.build(OpfsAttachmentDirectory.layer({ directory: "test-attachments" }))
          const service = Context.get(context, AttachmentDirectory.AttachmentDirectory)
          const accessKey = AttachmentStorage.ObjectKey.make("0123456789abcdef0123456789abcdef")
          const initializeKey = AttachmentStorage.ObjectKey.make("fedcba9876543210fedcba9876543210")
          assert.isTrue(Result.isFailure(yield* service.create(accessKey).pipe(Effect.result)))
          assert.isTrue(Result.isFailure(yield* service.create(initializeKey).pipe(Effect.result)))
          assert.deepStrictEqual(removed, [`${accessKey}.blob`, `${initializeKey}.blob`])
        }),
      () =>
        Effect.sync(() =>
          Object.defineProperty(globalThis, "navigator", {
            configurable: true,
            value: originalNavigator
          })
        )
    )
  })
})
