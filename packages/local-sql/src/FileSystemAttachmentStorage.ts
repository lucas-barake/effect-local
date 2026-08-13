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

const storageError = (operation: string, cause: unknown) => new Attachment.AttachmentStorageError({ operation, cause })

const platform = <A, R,>(
  operation: string,
  self: Effect.Effect<A, PlatformError.PlatformError, R>
): Effect.Effect<A, Attachment.AttachmentStorageError, R> =>
  Effect.mapError(self, (cause) => storageError(operation, cause))

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
      yield* platform("initialize", fs.makeDirectory(options.directory, { recursive: true }))
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

      const create: AttachmentStorage.Service["create"] = Effect.fnUntraced(function*() {
        const uuid = yield* platform("create.key", crypto.randomUUIDv4)
        const key = AttachmentStorage.ObjectKey.make(uuid.replaceAll("-", ""))
        const destination = objectPath(key)
        const opened = fs.open(destination, { flag: "wx" }).pipe(Effect.scoped)
        yield* platform("create", opened)
        return key
      })

      const notFound = (key: AttachmentStorage.ObjectKey) => new Attachment.AttachmentNotFound({ key })

      const offset: AttachmentStorage.Service["offset"] = (key) =>
        fs.stat(objectPath(key)).pipe(
          Effect.mapError((cause): AttachmentStorage.StorageFailure => {
            if (isNotFound(cause)) return notFound(key)
            return storageError("offset", cause)
          }),
          Effect.flatMap((info): Effect.Effect<number, Attachment.AttachmentStorageError> => {
            const size = Number(info.size)
            if (Number.isSafeInteger(size)) return Effect.succeed(size)
            return Effect.fail(storageError("offset", info.size))
          })
        )

      const remove: AttachmentStorage.Service["remove"] = (key) => {
        const destination = objectPath(key)
        const removed = platform("remove", fs.remove(destination, { force: true }))
        return withLock(key, removed)
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
        const key = yield* create()
        const write = Effect.scoped(Effect.gen(function*() {
          const destination = objectPath(key)
          const file = yield* platform("stage.open", fs.open(destination, { flag: "w" }))
          let observed = 0
          const stored = Stream.mapEffect(
            bytes,
            (chunk): Effect.Effect<Uint8Array, Attachment.AttachmentTooLarge | Attachment.AttachmentStorageError> => {
              observed += chunk.length
              if (observed > maximumBytes) {
                return Effect.fail(new Attachment.AttachmentTooLarge({ limit: maximumBytes }))
              }
              return platform("stage.write", file.writeAll(chunk)).pipe(Effect.as(chunk))
            }
          )
          const hashed = yield* Attachment.hash(stored)
          yield* platform("stage.sync", file.sync)
          return {
            key,
            reference: Attachment.Reference.make({ _tag: "Attachment", ...hashed })
          }
        }))
        return yield* write.pipe(
          Effect.onExit((exit) => {
            if (Exit.isFailure(exit)) return remove(key)
            return Effect.void
          })
        )
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
                  if (isNotFound(cause)) return notFound(key)
                  return storageError("append.open", cause)
                })
              )
              yield* file.seek(actual, "start")
              let written = 0
              yield* Stream.runForEach(
                bytes,
                (chunk): Effect.Effect<void, Attachment.AttachmentTooLarge | Attachment.AttachmentStorageError> => {
                  if (actual + written + chunk.length > limit) {
                    return Effect.fail(new Attachment.AttachmentTooLarge({ limit }))
                  }
                  return Effect.uninterruptible(
                    platform("append.write", file.writeAll(chunk)).pipe(
                      Effect.andThen(platform("append.sync", file.sync)),
                      Effect.tap(() => Effect.sync(() => written += chunk.length))
                    )
                  )
                }
              )
              return actual + written
            }))
          })
        )

      const verify: AttachmentStorage.Service["verify"] = Effect.fnUntraced(function*(key, reference) {
        const actualLength = yield* offset(key)
        if (actualLength !== reference.bytes) {
          return yield* new Attachment.AttachmentLengthMismatch({
            expected: reference.bytes,
            actual: actualLength
          })
        }
        const source = fs.stream(objectPath(key)).pipe(
          Stream.mapError((cause) => storageError("verify.read", cause))
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
            Stream.mapError((cause) => storageError("read", cause))
          )
        )))
      }

      const exists: AttachmentStorage.Service["exists"] = (key) => {
        const destination = objectPath(key)
        return platform("exists", fs.exists(destination))
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
