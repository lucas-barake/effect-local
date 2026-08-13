import type * as AttachmentStorage from "@lucas-barake/effect-local-sql/AttachmentStorage"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as AttachmentDirectory from "./AttachmentDirectory.js"

export interface Options {
  readonly directory: string
}

const fileName = (key: AttachmentStorage.ObjectKey) => `${key}.blob`

export const layer = (options: Options) =>
  Layer.effect(
    AttachmentDirectory.AttachmentDirectory,
    Effect.gen(function*() {
      if (options.directory.length === 0 || options.directory.includes("/") || options.directory.includes("\\")) {
        return yield* new Attachment.AttachmentStorageError({
          operation: "opfs.configure.directory",
          cause: options.directory
        })
      }
      const root = yield* Effect.tryPromise({
        try: () => navigator.storage.getDirectory(),
        catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.root", cause })
      })
      const directory = yield* Effect.tryPromise({
        try: () => root.getDirectoryHandle(options.directory, { create: true }),
        catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.initialize", cause })
      })

      const open = (key: AttachmentStorage.ObjectKey) =>
        Effect.tryPromise({
          try: () => directory.getFileHandle(fileName(key)),
          catch: (cause): AttachmentDirectory.Failure => {
            if (cause instanceof DOMException && cause.name === "NotFoundError") {
              return new Attachment.AttachmentNotFound({ key })
            }
            return new Attachment.AttachmentStorageError({ operation: "opfs.open", cause })
          }
        })

      const useHandle = <A, E extends { readonly _tag: string },>(
        key: AttachmentStorage.ObjectKey,
        use: (handle: FileSystemSyncAccessHandle) => Effect.Effect<A, E>
      ): Effect.Effect<A, E | AttachmentDirectory.Failure> =>
        Effect.acquireUseRelease(
          open(key).pipe(
            Effect.flatMap((file) =>
              Effect.tryPromise({
                try: () => file.createSyncAccessHandle(),
                catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.access", cause })
              })
            )
          ),
          use,
          (handle) =>
            Effect.try({
              try: () => handle.close(),
              catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.close", cause })
            }).pipe(
              Effect.catchTag("AttachmentStorageError", (error) =>
                Effect.logWarning("Failed to close an OPFS attachment handle").pipe(
                  Effect.annotateLogs("operation", error.operation)
                ))
            )
        )

      const create: AttachmentDirectory.Service["create"] = Effect.fnUntraced(function*(key) {
        const existing = yield* Effect.tryPromise({
          try: () => directory.getFileHandle(fileName(key)),
          catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.create.inspect", cause })
        }).pipe(
          Effect.as(true),
          Effect.catchTag("AttachmentStorageError", (error) => {
            if (error.cause instanceof DOMException && error.cause.name === "NotFoundError") {
              return Effect.succeed(false)
            }
            return Effect.fail(error)
          })
        )
        if (existing) {
          yield* new Attachment.AttachmentStorageError({ operation: "opfs.create.collision", cause: key })
        }
        const file = yield* Effect.tryPromise({
          try: () => directory.getFileHandle(fileName(key), { create: true }),
          catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.create", cause })
        })
        yield* Effect.acquireUseRelease(
          Effect.tryPromise({
            try: () => file.createSyncAccessHandle(),
            catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.create.access", cause })
          }),
          (handle) =>
            Effect.try({
              try: () => {
                handle.truncate(0)
                handle.flush()
              },
              catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.create.initialize", cause })
            }),
          (handle) =>
            Effect.try({
              try: () => handle.close(),
              catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.create.close", cause })
            }).pipe(
              Effect.catchTag("AttachmentStorageError", (error) =>
                Effect.logWarning("Failed to close a new OPFS attachment handle").pipe(
                  Effect.annotateLogs("operation", error.operation)
                ))
            )
        )
      })

      const offset: AttachmentDirectory.Service["offset"] = (key) =>
        useHandle(key, (handle) =>
          Effect.try({
            try: () => handle.getSize(),
            catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.offset", cause })
          }))

      const write: AttachmentDirectory.Service["write"] = (key, expectedOffset, bytes) =>
        useHandle(
          key,
          Effect.fnUntraced(function*(handle) {
            const actual = yield* Effect.try({
              try: () => handle.getSize(),
              catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.write.offset", cause })
            })
            if (actual !== expectedOffset) {
              return yield* new Attachment.AttachmentOffsetConflict({ expected: expectedOffset, actual })
            }
            let written = 0
            while (written < bytes.length) {
              const count = yield* Effect.try({
                try: () => handle.write(bytes.subarray(written), { at: expectedOffset + written }),
                catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.write", cause })
              })
              if (count <= 0) {
                return yield* new Attachment.AttachmentStorageError({
                  operation: "opfs.write.progress",
                  cause: count
                })
              }
              written += count
            }
            yield* Effect.try({
              try: () => handle.flush(),
              catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.write", cause })
            })
            return expectedOffset + written
          })
        )

      const read: AttachmentDirectory.Service["read"] = (key, fileOffset, length) =>
        useHandle(key, (handle) =>
          Effect.try({
            try: () => {
              const bytes = new Uint8Array(length)
              const bytesRead = handle.read(bytes, { at: fileOffset })
              if (bytesRead === bytes.length) return bytes
              return bytes.slice(0, bytesRead)
            },
            catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.read", cause })
          }))

      const exists: AttachmentDirectory.Service["exists"] = (key) =>
        Effect.tryPromise({
          try: () => directory.getFileHandle(fileName(key)),
          catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.exists", cause })
        }).pipe(
          Effect.as(true),
          Effect.catchTag("AttachmentStorageError", (error) => {
            if (error.cause instanceof DOMException && error.cause.name === "NotFoundError") {
              return Effect.succeed(false)
            }
            return Effect.fail(error)
          })
        )

      const remove: AttachmentDirectory.Service["remove"] = (key) =>
        Effect.tryPromise({
          try: () => directory.removeEntry(fileName(key)),
          catch: (cause) => new Attachment.AttachmentStorageError({ operation: "opfs.remove", cause })
        }).pipe(
          Effect.catchTag("AttachmentStorageError", (error) => {
            if (error.cause instanceof DOMException && error.cause.name === "NotFoundError") return Effect.void
            return Effect.fail(error)
          })
        )

      return AttachmentDirectory.AttachmentDirectory.of({ create, offset, write, read, exists, remove })
    })
  )
