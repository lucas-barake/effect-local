import * as AttachmentStorage from "@lucas-barake/effect-local-sql/AttachmentStorage"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as AttachmentWorkerProtocol from "./internal/AttachmentWorkerProtocol.js"

export interface Options {
  readonly maximumBytes: number
  readonly readChunkBytes: number
  readonly maximumPendingRequests: number
  readonly cleanupRequestTimeout: Duration.Input
}

const writeChunkBytes = 256 * 1_024

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
      if (
        !Number.isSafeInteger(options.maximumPendingRequests) || options.maximumPendingRequests <= 0 ||
        options.maximumPendingRequests > AttachmentWorkerProtocol.maximumRequestsPerLane
      ) {
        return yield* new Attachment.AttachmentStorageError({
          operation: "configure.maximumPendingRequests",
          cause: options.maximumPendingRequests
        })
      }
      const cleanupRequestTimeout = yield* Option.match(Duration.fromInput(options.cleanupRequestTimeout), {
        onNone: () =>
          Effect.fail(
            new Attachment.AttachmentStorageError({
              operation: "configure.cleanupRequestTimeout",
              cause: options.cleanupRequestTimeout
            })
          ),
        onSome: (duration) => {
          if (Duration.isPositive(duration) && Duration.isFinite(duration)) return Effect.succeed(duration)
          return Effect.fail(
            new Attachment.AttachmentStorageError({
              operation: "configure.cleanupRequestTimeout",
              cause: options.cleanupRequestTimeout
            })
          )
        }
      })
      const maximumBytes = options.maximumBytes
      const readChunkBytes = options.readChunkBytes
      const locks = yield* RcMap.make({ lookup: () => Semaphore.make(1) })
      const requestPermits = yield* Semaphore.make(options.maximumPendingRequests)
      const cleanupPermit = yield* Semaphore.make(options.maximumPendingRequests)
      const pending = new Map<number, (value: unknown) => void>()
      let nextRequestId = 0
      const receive = (event: MessageEvent<unknown>) => {
        const data = event.data
        if (typeof data !== "object" || data === null || !("id" in data) || typeof data.id !== "number") return
        pending.get(data.id)?.(data)
      }
      const messageError = (event: MessageEvent<unknown>) => {
        for (const complete of pending.values()) complete(event.data)
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

      const requestRaw = Effect.fn("BrowserAttachmentStorage.request")(function*(
        message: AttachmentWorkerProtocol.RequestWithoutId,
        transfer?: ReadonlyArray<Transferable>
      ): Effect.fn.Return<AttachmentWorkerProtocol.Response, Attachment.AttachmentStorageError> {
        const id = nextRequestId++
        const response = yield* Effect.callback<unknown, Attachment.AttachmentStorageError>((resume) => {
          pending.set(id, (value) => resume(Effect.succeed(value)))
          // oxlint-disable-next-line effect/noTryCatch -- MessagePort postMessage is the synchronous host boundary inside callback registration.
          try {
            const transfers: Array<Transferable> = []
            if (transfer !== undefined) transfers.push(...transfer)
            port.postMessage({ ...message, id }, transfers)
          } catch (cause) {
            pending.delete(id)
            resume(Effect.fail(new Attachment.AttachmentStorageError({ operation: "request.send", cause })))
          }
          return Effect.sync(() => pending.delete(id))
        })
        pending.delete(id)
        return yield* Schema.decodeUnknownEffect(AttachmentWorkerProtocol.Response)(response).pipe(
          Effect.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "response.decode", cause }))
        )
      })
      const request = (
        message: AttachmentWorkerProtocol.RequestWithoutId,
        transfer?: ReadonlyArray<Transferable>
      ) => requestPermits.withPermit(requestRaw(message, transfer))

      const cleanupRemove = Effect.fn("BrowserAttachmentStorage.cleanupRemove")(function*(key) {
        const response = yield* requestRaw({ _tag: "CleanupRemove", key }).pipe(
          Effect.timeoutOption(cleanupRequestTimeout)
        )
        if (Option.isNone(response)) {
          yield* Effect.logWarning("Timed out removing an interrupted staged attachment")
          return
        }
        if (response.value._tag !== "Removed") {
          let operation: string = response.value._tag
          if (response.value._tag === "StorageError") operation = response.value.operation
          yield* new Attachment.AttachmentStorageError({
            operation: `worker.${operation}`,
            cause: operation
          })
        }
      }, (effect) => cleanupPermit.withPermit(effect))

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

      const create: AttachmentStorage.Service["create"] = Effect.fn("BrowserAttachmentStorage.create")(function*() {
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

      const offset: AttachmentStorage.Service["offset"] = Effect.fn("BrowserAttachmentStorage.offset")(function*(key) {
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
        ).pipe(Effect.withSpan("BrowserAttachmentStorage.remove"))

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
            yield* writeCoalesced(
              bytes,
              limit - actual,
              limit,
              Effect.fnUntraced(function*(chunk): Effect.fn.Return<
                void,
                AttachmentStorage.StorageFailure | Attachment.AttachmentOffsetConflict | Attachment.AttachmentTooLarge
              > {
                const response = yield* request(
                  { _tag: "Write", key, expectedOffset: written, bytes: chunk },
                  [chunk.buffer]
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
        ).pipe(Effect.withSpan("BrowserAttachmentStorage.append"))

      const stage: AttachmentStorage.Service["stage"] = Effect.fnUntraced(function*<
        E extends { readonly _tag: string },
        R,
      >(bytes: Stream.Stream<Uint8Array, E, R>) {
        return yield* Effect.acquireUseRelease(
          create(),
          Effect.fnUntraced(function*(key) {
            let written = 0
            const write = Effect.fnUntraced(function*(chunk: Uint8Array) {
              const response = yield* request(
                { _tag: "Write", key, expectedOffset: written, bytes: chunk },
                [chunk.buffer]
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
              return undefined
            })
            const hashed = yield* hashCoalesced(bytes, write)
            return {
              key,
              reference: Attachment.Reference.make({ _tag: "Attachment", ...hashed })
            }
          }),
          Effect.fnUntraced(function*(key, exit) {
            if (Exit.isFailure(exit)) {
              yield* cleanupRemove(key).pipe(
                Effect.catchTag("AttachmentStorageError", (error) =>
                  Effect.logWarning("Failed to remove an interrupted staged attachment").pipe(
                    Effect.annotateLogs("operation", error.operation)
                  ))
              )
            }
          })
        ).pipe(Effect.withSpan("BrowserAttachmentStorage.stage"))
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
          if (range === undefined) {
            return { start: 0, end: reference.bytes }
          }
          const start = range.offset
          let length = reference.bytes - start
          if (range.length !== undefined) {
            length = range.length
          }
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
              if (position >= end) {
                return undefined
              }
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
        ))).pipe(Stream.withSpan("BrowserAttachmentStorage.read"))
      }

      const verify: AttachmentStorage.Service["verify"] = Effect.fn("BrowserAttachmentStorage.verify")(function*(
        key,
        reference
      ) {
        const actual = yield* offset(key)
        if (actual !== reference.bytes) {
          yield* new Attachment.AttachmentLengthMismatch({ expected: reference.bytes, actual })
        }
        const hashed = yield* Attachment.hash(read(key, reference), { maximumBytes }).pipe(
          Effect.catchTag(
            "InvalidAttachmentRange",
            (error) =>
              Effect.fail(new Attachment.AttachmentStorageError({ operation: "verify.range", cause: error }))
          )
        )
        if (hashed.digest !== reference.digest) {
          yield* new Attachment.AttachmentDigestMismatch({
            expected: reference.digest,
            actual: hashed.digest
          })
        }
      })

      const exists: AttachmentStorage.Service["exists"] = Effect.fn("BrowserAttachmentStorage.exists")(function*(key) {
        const response = yield* request({ _tag: "Exists", key })
        if (response._tag !== "Exists") {
          let operation: string = response._tag
          if (response._tag === "StorageError") {
            operation = response.operation
          }
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
