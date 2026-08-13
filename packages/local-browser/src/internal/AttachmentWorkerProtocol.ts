import * as AttachmentStorage from "@lucas-barake/effect-local-sql/AttachmentStorage"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as AttachmentDirectory from "./AttachmentDirectory.js"

const RequestId = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)
)

const BaseRequest = { id: RequestId, key: AttachmentStorage.ObjectKey }

export const Request = Schema.Union([
  Schema.Struct({ _tag: Schema.tag("Create"), ...BaseRequest }),
  Schema.Struct({ _tag: Schema.tag("Offset"), ...BaseRequest }),
  Schema.Struct({
    _tag: Schema.tag("Write"),
    ...BaseRequest,
    expectedOffset: Attachment.ByteLength,
    bytes: Schema.Uint8Array
  }),
  Schema.Struct({
    _tag: Schema.tag("Read"),
    ...BaseRequest,
    offset: Attachment.ByteLength,
    length: Attachment.ByteLength
  }),
  Schema.Struct({ _tag: Schema.tag("Exists"), ...BaseRequest }),
  Schema.Struct({ _tag: Schema.tag("Remove"), ...BaseRequest })
])
export type Request = typeof Request.Type
export type RequestWithoutId = Request extends infer R ? R extends { readonly id: number } ? Omit<R, "id"> : never
  : never

const BaseResponse = { id: RequestId }

export const Response = Schema.Union([
  Schema.Struct({ _tag: Schema.tag("Created"), ...BaseResponse }),
  Schema.Struct({ _tag: Schema.tag("Offset"), ...BaseResponse, offset: Attachment.ByteLength }),
  Schema.Struct({ _tag: Schema.tag("Written"), ...BaseResponse, offset: Attachment.ByteLength }),
  Schema.Struct({ _tag: Schema.tag("Bytes"), ...BaseResponse, bytes: Schema.Uint8Array }),
  Schema.Struct({ _tag: Schema.tag("Exists"), ...BaseResponse, exists: Schema.Boolean }),
  Schema.Struct({ _tag: Schema.tag("Removed"), ...BaseResponse }),
  Schema.Struct({
    _tag: Schema.tag("NotFound"),
    ...BaseResponse,
    key: AttachmentStorage.ObjectKey
  }),
  Schema.Struct({
    _tag: Schema.tag("OffsetConflict"),
    ...BaseResponse,
    expected: Attachment.ByteLength,
    actual: Attachment.ByteLength
  }),
  Schema.Struct({ _tag: Schema.tag("TooLarge"), ...BaseResponse, limit: Attachment.ByteLength }),
  Schema.Struct({ _tag: Schema.tag("StorageError"), ...BaseResponse, operation: Schema.String })
])
export type Response = typeof Response.Type

export interface Options {
  readonly maximumBytes: number
}

const send = (port: MessagePort, response: Response) =>
  Effect.try({
    try: () => {
      if (response._tag === "Bytes") port.postMessage(response, [response.bytes.buffer])
      else port.postMessage(response)
    },
    catch: (cause) => new Attachment.AttachmentStorageError({ operation: "worker.send", cause })
  })

export const serve = Effect.fnUntraced(function*(port: MessagePort, options: Options) {
  if (!Number.isSafeInteger(options.maximumBytes) || options.maximumBytes <= 0) {
    yield* new Attachment.AttachmentStorageError({
      operation: "worker.configure.maximumBytes",
      cause: options.maximumBytes
    })
  }
  const directory = yield* AttachmentDirectory.AttachmentDirectory
  const queue = yield* Effect.acquireRelease(
    Queue.unbounded<MessageEvent<unknown>>(),
    Queue.shutdown
  )
  const receive = (event: MessageEvent<unknown>) => void Queue.offerUnsafe(queue, event)
  yield* Effect.acquireRelease(
    Effect.sync(() => {
      port.addEventListener("message", receive)
      port.start()
    }),
    () => Effect.sync(() => port.removeEventListener("message", receive))
  )
  const handleRequest = Effect.fnUntraced(function*(event: MessageEvent<unknown>) {
    const decoded = yield* Schema.decodeUnknownEffect(Request)(event.data).pipe(
      Effect.mapError((cause) => new Attachment.AttachmentStorageError({ operation: "worker.decode", cause }))
    )
    let response: Response
    switch (decoded._tag) {
      case "Create":
        response = yield* directory.create(decoded.key).pipe(
          Effect.as<Response>({ _tag: "Created", id: decoded.id }),
          Effect.catchTag(
            "AttachmentStorageError",
            () => Effect.succeed<Response>({ _tag: "StorageError", id: decoded.id, operation: "create" })
          )
        )
        break
      case "Offset":
        response = yield* directory.offset(decoded.key).pipe(
          Effect.map((offset): Response => ({ _tag: "Offset", id: decoded.id, offset })),
          Effect.catchTags({
            AttachmentNotFound: (): Effect.Effect<Response> =>
              Effect.succeed({ _tag: "NotFound", id: decoded.id, key: decoded.key }),
            AttachmentStorageError: (): Effect.Effect<Response> =>
              Effect.succeed({ _tag: "StorageError", id: decoded.id, operation: "offset" })
          })
        )
        break
      case "Write":
        if (decoded.expectedOffset + decoded.bytes.length > options.maximumBytes) {
          response = { _tag: "TooLarge", id: decoded.id, limit: options.maximumBytes }
          break
        }
        response = yield* directory.write(decoded.key, decoded.expectedOffset, decoded.bytes).pipe(
          Effect.map((offset): Response => ({ _tag: "Written", id: decoded.id, offset })),
          Effect.catchTags({
            AttachmentNotFound: (): Effect.Effect<Response> =>
              Effect.succeed({ _tag: "NotFound", id: decoded.id, key: decoded.key }),
            AttachmentOffsetConflict: (error): Effect.Effect<Response> =>
              Effect.succeed({
                _tag: "OffsetConflict",
                id: decoded.id,
                expected: error.expected,
                actual: error.actual
              }),
            AttachmentStorageError: (): Effect.Effect<Response> =>
              Effect.succeed({ _tag: "StorageError", id: decoded.id, operation: "write" })
          })
        )
        break
      case "Read":
        response = yield* directory.read(decoded.key, decoded.offset, decoded.length).pipe(
          Effect.map((bytes): Response => ({ _tag: "Bytes", id: decoded.id, bytes })),
          Effect.catchTags({
            AttachmentNotFound: (): Effect.Effect<Response> =>
              Effect.succeed({ _tag: "NotFound", id: decoded.id, key: decoded.key }),
            AttachmentStorageError: (): Effect.Effect<Response> =>
              Effect.succeed({ _tag: "StorageError", id: decoded.id, operation: "read" })
          })
        )
        break
      case "Exists":
        response = yield* directory.exists(decoded.key).pipe(
          Effect.map((exists): Response => ({ _tag: "Exists", id: decoded.id, exists })),
          Effect.catchTag(
            "AttachmentStorageError",
            () => Effect.succeed<Response>({ _tag: "StorageError", id: decoded.id, operation: "exists" })
          )
        )
        break
      case "Remove":
        response = yield* directory.remove(decoded.key).pipe(
          Effect.as<Response>({ _tag: "Removed", id: decoded.id }),
          Effect.catchTag(
            "AttachmentStorageError",
            () => Effect.succeed<Response>({ _tag: "StorageError", id: decoded.id, operation: "remove" })
          )
        )
        break
    }
    yield* send(port, response)
  })
  const handleRequestSafely = (event: MessageEvent<unknown>) =>
    handleRequest(event).pipe(
      Effect.catchTag(
        "AttachmentStorageError",
        (error) =>
          Effect.logWarning("Attachment worker rejected a malformed request").pipe(
            Effect.annotateLogs("operation", error.operation)
          )
      )
    )
  yield* Stream.fromQueue(queue).pipe(
    Stream.mapEffect(handleRequestSafely),
    Stream.runDrain
  )
}, Effect.scoped)
