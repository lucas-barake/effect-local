import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as FileSystem from "effect/FileSystem"
import * as Layer from "effect/Layer"
import * as Path from "effect/Path"
import type * as PlatformError from "effect/PlatformError"
import * as RcMap from "effect/RcMap"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as AttachmentStorage from "./AttachmentStorage.js"
import * as Configuration from "./internal/configuration.js"

export interface Options {
  readonly directory: string
  readonly maximumBytes: number
}

const writeChunkBytes = 256 * 1_024

const isNotFound = (cause: PlatformError.PlatformError) => cause.reason._tag === "NotFound"

export const layer = (options: Options) =>
  Layer.effect(
    AttachmentStorage.AttachmentStorage,
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const crypto = yield* Crypto.Crypto
      const maximumBytes = yield* Configuration.positiveSafeInteger(
        "attachments.maximumBytes",
        options.maximumBytes
      )
      yield* fs.makeDirectory(options.directory, { recursive: true }).pipe(
        Effect.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "initialize", cause })),
        Effect.withSpan("FileSystemAttachmentStorage.initialize")
      )
      const locks = yield* RcMap.make({ lookup: () => Semaphore.make(1) })

      const objectPath = (key: AttachmentStorage.ObjectKey) => path.join(options.directory, `${key}.blob`)
      const withLock = <A, E extends { readonly _tag: string }, R,>(
        key: AttachmentStorage.ObjectKey,
        effect: Effect.Effect<A, E, R>
      ): Effect.Effect<A, E, R> =>
        RcMap.get(locks, key).pipe(
          Effect.flatMap((lock) => lock.withPermit(effect)),
          Effect.scoped
        )

      const writeCoalesced = <E extends { readonly _tag: string }, R, E2 extends { readonly _tag: string },>(
        bytes: Stream.Stream<Uint8Array, E, R>,
        limit: number,
        reportedLimit: number,
        write: (bytes: Uint8Array) => Effect.Effect<void, E2>
      ): Effect.Effect<number, E | E2 | Attachment.AttachmentTooLarge, R> => {
        const buffer = new Uint8Array(Math.min(writeChunkBytes, Math.max(1, limit)))
        let buffered = 0
        let observed = 0
        const flush = Effect.suspend(() => {
          if (buffered === 0) return Effect.void
          const outgoing = buffer.slice(0, buffered)
          buffered = 0
          return write(outgoing)
        })
        const consume = Effect.fnUntraced(function*(chunk: Uint8Array) {
          if (observed + chunk.length > limit) {
            return yield* Effect.fail<E2 | Attachment.AttachmentTooLarge>(
              new Attachment.AttachmentTooLarge({ limit: reportedLimit })
            )
          }
          observed += chunk.length
          let sourceOffset = 0
          while (sourceOffset < chunk.length) {
            const copied = Math.min(buffer.length - buffered, chunk.length - sourceOffset)
            buffer.set(chunk.subarray(sourceOffset, sourceOffset + copied), buffered)
            buffered += copied
            sourceOffset += copied
            if (buffered === buffer.length) yield* flush
          }
          return undefined
        })
        return Stream.runForEach(bytes, consume).pipe(
          Effect.onExit(() => flush),
          Effect.as(observed)
        )
      }

      const hashCoalesced = <E extends { readonly _tag: string }, R, E2 extends { readonly _tag: string },>(
        bytes: Stream.Stream<Uint8Array, E, R>,
        write: (bytes: Uint8Array) => Effect.Effect<void, E2>
      ): Effect.Effect<Attachment.HashResult, E | E2 | Attachment.AttachmentTooLarge, R> => {
        const buffer = new Uint8Array(Math.min(writeChunkBytes, maximumBytes))
        let buffered = 0
        let observed = 0
        const flush = Effect.suspend(() => {
          if (buffered === 0) return Effect.void
          const outgoing = buffer.slice(0, buffered)
          buffered = 0
          return write(outgoing)
        })
        const stored = Stream.mapEffect(
          bytes,
          Effect.fnUntraced(function*(chunk) {
            if (observed + chunk.length > maximumBytes) {
              return yield* Effect.fail<E2 | Attachment.AttachmentTooLarge>(
                new Attachment.AttachmentTooLarge({ limit: maximumBytes })
              )
            }
            observed += chunk.length
            let sourceOffset = 0
            while (sourceOffset < chunk.length) {
              const copied = Math.min(buffer.length - buffered, chunk.length - sourceOffset)
              buffer.set(chunk.subarray(sourceOffset, sourceOffset + copied), buffered)
              buffered += copied
              sourceOffset += copied
              if (buffered === buffer.length) yield* flush
            }
            return chunk
          })
        )
        return Attachment.hash(stored, { maximumBytes }).pipe(Effect.onExit(() => flush))
      }

      const create: AttachmentStorage.Service["create"] = Effect.fn("FileSystemAttachmentStorage.create")(function*() {
        const uuid = yield* crypto.randomUUIDv4.pipe(
          Effect.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "create.key", cause }))
        )
        const key = AttachmentStorage.ObjectKey.make(uuid.replaceAll("-", ""))
        const destination = objectPath(key)
        const opened = fs.open(destination, { flag: "wx" }).pipe(Effect.scoped)
        yield* opened.pipe(
          Effect.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "create", cause }))
        )
        return key
      })

      const offset: AttachmentStorage.Service["offset"] = (key) =>
        fs.stat(objectPath(key)).pipe(
          Effect.mapError((cause): AttachmentStorage.StorageFailure => {
            if (isNotFound(cause)) return new Attachment.AttachmentNotFound({ key })
            return new Attachment.AttachmentStorageError({ operation: "offset", cause })
          }),
          Effect.flatMap((info): Effect.Effect<number, Attachment.AttachmentStorageError> => {
            const size = Number(info.size)
            if (Number.isSafeInteger(size)) return Effect.succeed(size)
            return Effect.fail(new Attachment.AttachmentStorageError({ operation: "offset", cause: info.size }))
          }),
          Effect.withSpan("FileSystemAttachmentStorage.offset")
        )

      const remove: AttachmentStorage.Service["remove"] = (key) => {
        const destination = objectPath(key)
        return withLock(
          key,
          fs.remove(destination, { force: true }).pipe(
            Effect.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "remove", cause })),
            Effect.withSpan("FileSystemAttachmentStorage.remove")
          )
        )
      }

      const stage: AttachmentStorage.Service["stage"] = Effect.fnUntraced(function*<
        E extends { readonly _tag: string },
        R,
      >(
        bytes: Stream.Stream<Uint8Array, E, R>
      ): Effect.fn.Return<
        AttachmentStorage.Staged,
        E | Attachment.AttachmentTooLarge | Attachment.AttachmentStorageError,
        R
      > {
        return yield* Effect.acquireUseRelease(
          create(),
          (key) =>
            Effect.scoped(Effect.gen(function*() {
              const destination = objectPath(key)
              const file = yield* fs.open(destination, { flag: "w" }).pipe(
                Effect.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "stage.open", cause }))
              )
              const hashed = yield* hashCoalesced(
                bytes,
                (chunk) =>
                  file.writeAll(chunk).pipe(
                    Effect.mapError((cause) =>
                      new Attachment.AttachmentStorageError({ operation: "stage.write", cause })
                    )
                  )
              )
              yield* file.sync.pipe(
                Effect.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "stage.sync", cause }))
              )
              return {
                key,
                reference: Attachment.Reference.make({ _tag: "Attachment", ...hashed })
              }
            })),
          Effect.fnUntraced(function*(key, exit) {
            if (Exit.isFailure(exit)) {
              yield* remove(key).pipe(
                Effect.catchTag(
                  "AttachmentStorageError",
                  (error) =>
                    Effect.logWarning("Failed to remove an interrupted staged attachment").pipe(
                      Effect.annotateLogs("operation", error.operation)
                    )
                )
              )
            }
          })
        ).pipe(Effect.withSpan("FileSystemAttachmentStorage.stage"))
      })

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
            return yield* Effect.scoped(Effect.gen(function*() {
              const file = yield* fs.open(objectPath(key), { flag: "r+" }).pipe(
                Effect.mapError((cause): AttachmentStorage.StorageFailure => {
                  if (isNotFound(cause)) return new Attachment.AttachmentNotFound({ key })
                  return new Attachment.AttachmentStorageError({ operation: "append.open", cause })
                })
              )
              yield* file.seek(actual, "start")
              let written = 0
              yield* writeCoalesced(
                bytes,
                limit - actual,
                limit,
                (chunk) =>
                  Effect.uninterruptible(
                    file.writeAll(chunk).pipe(
                      Effect.mapError((cause) =>
                        new Attachment.AttachmentStorageError({ operation: "append.write", cause })
                      ),
                      Effect.andThen(file.sync.pipe(
                        Effect.mapError((cause) =>
                          new Attachment.AttachmentStorageError({ operation: "append.sync", cause })
                        )
                      )),
                      Effect.tap(() => Effect.sync(() => written += chunk.length))
                    )
                  )
              )
              return actual + written
            }))
          })
        ).pipe(Effect.withSpan("FileSystemAttachmentStorage.append"))

      const verify: AttachmentStorage.Service["verify"] = Effect.fn("FileSystemAttachmentStorage.verify")(function*(
        key,
        reference
      ) {
        const actualLength = yield* offset(key)
        if (actualLength !== reference.bytes) {
          return yield* new Attachment.AttachmentLengthMismatch({
            expected: reference.bytes,
            actual: actualLength
          })
        }
        const source = fs.stream(objectPath(key)).pipe(
          Stream.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "verify.read", cause }))
        )
        const actual = yield* Attachment.hash(source)
        if (actual.digest !== reference.digest) {
          return yield* new Attachment.AttachmentDigestMismatch({
            expected: reference.digest,
            actual: actual.digest
          })
        }
        return undefined
      })

      const read: AttachmentStorage.Service["read"] = (key, reference, range) => {
        const validate: Effect.Effect<
          { readonly offset: number; readonly bytesToRead: number } | undefined,
          AttachmentStorage.ReadFailure
        > = Effect.gen(function*() {
          const actual = yield* offset(key)
          if (actual !== reference.bytes) {
            return yield* new Attachment.AttachmentLengthMismatch({ expected: reference.bytes, actual })
          }
          if (range === undefined) return undefined
          let length = reference.bytes - range.offset
          if (range.length !== undefined) length = range.length
          if (
            !Number.isSafeInteger(range.offset) || range.offset < 0 ||
            !Number.isSafeInteger(length) || length <= 0 ||
            range.offset >= reference.bytes || range.offset + length > reference.bytes
          ) {
            return yield* new Attachment.InvalidAttachmentRange({
              bytes: reference.bytes,
              offset: range.offset,
              length
            })
          }
          return { offset: range.offset, bytesToRead: length }
        })
        return Stream.unwrap(validate.pipe(Effect.map((validated) =>
          fs.stream(objectPath(key), validated).pipe(
            Stream.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "read", cause }))
          )
        ))).pipe(Stream.withSpan("FileSystemAttachmentStorage.read"))
      }

      const exists: AttachmentStorage.Service["exists"] = (key) => {
        const destination = objectPath(key)
        return fs.exists(destination).pipe(
          Effect.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "exists", cause })),
          Effect.withSpan("FileSystemAttachmentStorage.exists")
        )
      }

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
