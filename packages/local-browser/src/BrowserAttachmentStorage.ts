import * as AttachmentStorage from "@lucas-barake/effect-local-sql/AttachmentStorage"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as AttachmentWorkerProtocol from "./internal/AttachmentWorkerProtocol.js"

export interface Options {
  readonly maximumBytes: number
  readonly readChunkBytes: number
}

export const layerMessagePort = (port: MessagePort, options: Options) =>
  Layer.effect(
    AttachmentStorage.AttachmentStorage,
    Effect.gen(function*() {
      const crypto = yield* Crypto.Crypto
      if (!Number.isSafeInteger(options.maximumBytes) || options.maximumBytes <= 0) {
        return yield* new Attachment.AttachmentStorageError({
          operation: "configure.maximumBytes",
          cause: options.maximumBytes
        })
      }
      if (!Number.isSafeInteger(options.readChunkBytes) || options.readChunkBytes <= 0) {
        return yield* new Attachment.AttachmentStorageError({
          operation: "configure.readChunkBytes",
          cause: options.readChunkBytes
        })
      }
      const maximumBytes = options.maximumBytes
      const readChunkBytes = options.readChunkBytes
      const locks = yield* RcMap.make({ lookup: () => Semaphore.make(1) })
      const pending = new Map<number, Deferred.Deferred<unknown>>()
      let nextRequestId = 0
      const receive = (event: MessageEvent<unknown>) => {
        const data = event.data
        if (typeof data !== "object" || data === null || !("id" in data) || typeof data.id !== "number") return
        const deferred = pending.get(data.id)
        if (deferred !== undefined) Deferred.doneUnsafe(deferred, Exit.succeed(data))
      }
      const messageError = (event: MessageEvent<unknown>) => {
        for (const deferred of pending.values()) Deferred.doneUnsafe(deferred, Exit.succeed(event.data))
      }
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          port.addEventListener("message", receive)
          port.addEventListener("messageerror", messageError)
          port.start()
        }),
        () =>
          Effect.sync(() => {
            port.removeEventListener("message", receive)
            port.removeEventListener("messageerror", messageError)
          })
      )

      const request = Effect.fnUntraced(function*(
        message: AttachmentWorkerProtocol.RequestWithoutId,
        transfer?: ReadonlyArray<Transferable>
      ): Effect.fn.Return<AttachmentWorkerProtocol.Response, Attachment.AttachmentStorageError> {
        const id = nextRequestId++
        const completion = yield* Deferred.make<unknown>()
        return yield* Effect.acquireUseRelease(
          Effect.sync(() => pending.set(id, completion)),
          () => {
            const transfers: Array<Transferable> = []
            if (transfer !== undefined) transfers.push(...transfer)
            return Effect.try({
              try: () => port.postMessage({ ...message, id }, transfers),
              catch: (cause) => new Attachment.AttachmentStorageError({ operation: "request.send", cause })
            }).pipe(
              Effect.andThen(Deferred.await(completion)),
              Effect.flatMap(Schema.decodeUnknownEffect(AttachmentWorkerProtocol.Response)),
              Effect.mapError((cause) => {
                if (cause._tag === "AttachmentStorageError") return cause
                return new Attachment.AttachmentStorageError({ operation: "response.decode", cause })
              })
            )
          },
          () => Effect.sync(() => pending.delete(id))
        )
      })

      const withLock = <A, E extends { readonly _tag: string }, R,>(
        key: AttachmentStorage.ObjectKey,
        effect: Effect.Effect<A, E, R>
      ): Effect.Effect<A, E, R> =>
        RcMap.get(locks, key).pipe(
          Effect.flatMap((lock) => lock.withPermit(effect)),
          Effect.scoped
        )

      const create: AttachmentStorage.Service["create"] = Effect.fnUntraced(function*() {
        const uuid = yield* crypto.randomUUIDv4.pipe(
          Effect.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "create.key", cause }))
        )
        const key = AttachmentStorage.ObjectKey.make(uuid.replaceAll("-", ""))
        const response = yield* request({ _tag: "Create", key })
        if (response._tag !== "Created") {
          let operation: string = response._tag
          if (response._tag === "StorageError") operation = response.operation
          return yield* new Attachment.AttachmentStorageError({ operation: `worker.${operation}`, cause: operation })
        }
        return key
      })

      const offset: AttachmentStorage.Service["offset"] = Effect.fnUntraced(function*(key) {
        const response = yield* request({ _tag: "Offset", key })
        if (response._tag === "NotFound") return yield* new Attachment.AttachmentNotFound({ key: response.key })
        if (response._tag !== "Offset") {
          let operation: string = response._tag
          if (response._tag === "StorageError") operation = response.operation
          return yield* new Attachment.AttachmentStorageError({ operation: `worker.${operation}`, cause: operation })
        }
        return response.offset
      })

      const remove: AttachmentStorage.Service["remove"] = (key) =>
        withLock(
          key,
          Effect.gen(function*() {
            const response = yield* request({ _tag: "Remove", key })
            if (response._tag !== "Removed") {
              let operation: string = response._tag
              if (response._tag === "StorageError") operation = response.operation
              yield* new Attachment.AttachmentStorageError({
                operation: `worker.${operation}`,
                cause: operation
              })
            }
          })
        )

      const append = <E extends { readonly _tag: string }, R,>(
        key: AttachmentStorage.ObjectKey,
        reference: Attachment.Reference,
        expectedOffset: number,
        bytes: Stream.Stream<Uint8Array, E, R>
      ): Effect.Effect<
        number,
        E | AttachmentStorage.StorageFailure | Attachment.AttachmentOffsetConflict | Attachment.AttachmentTooLarge,
        R
      > =>
        withLock(
          key,
          Effect.gen(function*() {
            const actual = yield* offset(key)
            if (actual !== expectedOffset) {
              return yield* new Attachment.AttachmentOffsetConflict({ expected: expectedOffset, actual })
            }
            const limit = Math.min(reference.bytes, maximumBytes)
            if (actual > limit) return yield* new Attachment.AttachmentTooLarge({ limit })
            let written = actual
            yield* Stream.runForEach(
              bytes,
              Effect.fnUntraced(function*(chunk): Effect.fn.Return<
                void,
                AttachmentStorage.StorageFailure | Attachment.AttachmentOffsetConflict | Attachment.AttachmentTooLarge
              > {
                if (written + chunk.length > limit) {
                  yield* new Attachment.AttachmentTooLarge({ limit })
                }
                const outgoing = Uint8Array.from(chunk)
                const response = yield* request(
                  { _tag: "Write", key, expectedOffset: written, bytes: outgoing },
                  [outgoing.buffer]
                )
                if (response._tag === "Written") {
                  written = response.offset
                } else if (response._tag === "NotFound") {
                  yield* new Attachment.AttachmentNotFound({ key: response.key })
                }
                if (response._tag === "OffsetConflict") {
                  yield* new Attachment.AttachmentOffsetConflict({
                    expected: response.expected,
                    actual: response.actual
                  })
                }
                if (response._tag === "TooLarge") {
                  yield* new Attachment.AttachmentTooLarge({ limit: response.limit })
                }
                if (response._tag !== "Written") {
                  let operation: string = response._tag
                  if (response._tag === "StorageError") operation = response.operation
                  yield* new Attachment.AttachmentStorageError({
                    operation: `worker.${operation}`,
                    cause: operation
                  })
                }
              })
            )
            return written
          })
        )

      const stage: AttachmentStorage.Service["stage"] = Effect.fnUntraced(function*<
        E extends { readonly _tag: string },
        R,
      >(bytes: Stream.Stream<Uint8Array, E, R>) {
        const key = yield* create()
        const write = Effect.gen(function*() {
          let written = 0
          const stored = Stream.mapEffect(
            bytes,
            Effect.fnUntraced(function*(chunk): Effect.fn.Return<
              Uint8Array,
              Attachment.AttachmentStorageError | Attachment.AttachmentTooLarge
            > {
              if (written + chunk.length > maximumBytes) {
                return yield* new Attachment.AttachmentTooLarge({ limit: maximumBytes })
              }
              const outgoing = Uint8Array.from(chunk)
              const response = yield* request(
                { _tag: "Write", key, expectedOffset: written, bytes: outgoing },
                [outgoing.buffer]
              )
              if (response._tag === "TooLarge") {
                return yield* new Attachment.AttachmentTooLarge({ limit: response.limit })
              }
              if (response._tag !== "Written") {
                let operation: string = response._tag
                if (response._tag === "StorageError") operation = response.operation
                return yield* new Attachment.AttachmentStorageError({
                  operation: `worker.${operation}`,
                  cause: operation
                })
              }
              written = response.offset
              return chunk
            })
          )
          const hashed = yield* Attachment.hash(stored, { maximumBytes })
          return {
            key,
            reference: Attachment.Reference.make({ _tag: "Attachment", ...hashed })
          }
        })
        return yield* write.pipe(
          Effect.onExit((exit) => {
            if (Exit.isFailure(exit)) return remove(key)
            return Effect.void
          })
        )
      })

      const read: AttachmentStorage.Service["read"] = (key, reference, range) => {
        const prepare: Effect.Effect<
          { readonly start: number; readonly end: number },
          AttachmentStorage.ReadFailure
        > = Effect.gen(function*() {
          const actual = yield* offset(key)
          if (actual !== reference.bytes) {
            return yield* new Attachment.AttachmentLengthMismatch({ expected: reference.bytes, actual })
          }
          if (range === undefined) return { start: 0, end: reference.bytes }
          const start = range.offset
          let length = reference.bytes - start
          if (range.length !== undefined) length = range.length
          if (
            !Number.isSafeInteger(start) || start < 0 ||
            !Number.isSafeInteger(length) || length <= 0 ||
            start >= reference.bytes || start + length > reference.bytes
          ) {
            return yield* new Attachment.InvalidAttachmentRange({
              bytes: reference.bytes,
              offset: start,
              length
            })
          }
          return { start, end: start + length }
        })
        return Stream.unwrap(prepare.pipe(Effect.map(({ end, start }) =>
          Stream.unfold(
            start,
            Effect.fnUntraced(function*(position): Effect.fn.Return<
              readonly [Uint8Array, number] | undefined,
              AttachmentStorage.StorageFailure
            > {
              if (position >= end) return undefined
              const length = Math.min(readChunkBytes, end - position)
              const response = yield* request({ _tag: "Read", key, offset: position, length })
              if (response._tag === "NotFound") {
                return yield* new Attachment.AttachmentNotFound({ key: response.key })
              }
              if (response._tag !== "Bytes") {
                let operation: string = response._tag
                if (response._tag === "StorageError") {
                  operation = response.operation
                }
                return yield* new Attachment.AttachmentStorageError({
                  operation: `worker.${operation}`,
                  cause: operation
                })
              }
              if (response.bytes.length === 0 || response.bytes.length > length) {
                return yield* new Attachment.AttachmentStorageError({
                  operation: "read.chunk",
                  cause: response.bytes.length
                })
              }
              return [response.bytes, position + response.bytes.length] as const
            })
          )
        )))
      }

      const verify: AttachmentStorage.Service["verify"] = Effect.fnUntraced(function*(key, reference) {
        const actual = yield* offset(key)
        if (actual !== reference.bytes) {
          yield* new Attachment.AttachmentLengthMismatch({ expected: reference.bytes, actual })
        }
        const hashed = yield* Attachment.hash(read(key, reference), { maximumBytes }).pipe(
          Effect.catchTag(
            "InvalidAttachmentRange",
            (error) => Effect.fail(new Attachment.AttachmentStorageError({ operation: "verify.range", cause: error }))
          )
        )
        if (hashed.digest !== reference.digest) {
          yield* new Attachment.AttachmentDigestMismatch({
            expected: reference.digest,
            actual: hashed.digest
          })
        }
      })

      const exists: AttachmentStorage.Service["exists"] = Effect.fnUntraced(function*(key) {
        const response = yield* request({ _tag: "Exists", key })
        if (response._tag !== "Exists") {
          let operation: string = response._tag
          if (response._tag === "StorageError") operation = response.operation
          return yield* new Attachment.AttachmentStorageError({ operation: `worker.${operation}`, cause: operation })
        }
        return response.exists
      })

      return AttachmentStorage.AttachmentStorage.of({
        create,
        stage,
        append,
        offset,
        verify,
        read,
        exists,
        remove
      })
    })
  )
