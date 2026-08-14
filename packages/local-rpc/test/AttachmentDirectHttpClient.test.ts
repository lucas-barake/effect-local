import { assert, describe, it } from "@effect/vitest"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
import * as AttachmentTransfer from "@lucas-barake/effect-local/AttachmentTransfer"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"
import * as Tracer from "effect/Tracer"
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient"
import * as AttachmentDirectHttpClient from "../src/AttachmentDirectHttpClient.js"

/* oxlint-disable effect-local/noFunctionEffectGen, effect-local/noNestedCalls, effect/noAs, effect/noAsyncFunction, effect/noNewError, effect/noNewPromise, effect/noThrowStatement, typescript/no-base-to-string, typescript/no-unnecessary-type-assertion -- Fetch is the external boundary under test, so the fixture must implement the native Promise based interface and inspect its request objects. */

const origin = "https://objects.example"
const secret = "secret-signature-value"

const uploadGrant = (
  url = `${origin}/upload?signature=${secret}`,
  headers: ReadonlyArray<AttachmentTransfer.GrantHeader> = []
) =>
  AttachmentTransfer.UploadPart.make({
    attemptId: AttachmentTransfer.AttemptId.make("attempt"),
    partNumber: 1,
    offset: 0,
    bytes: 3,
    expiresAt: 10_000,
    request: AttachmentTransfer.DirectUploadRequest.make({
      method: "PUT",
      url: AttachmentTransfer.GrantUrl.make(url),
      headers
    })
  })

const downloadGrant = (
  url = `${origin}/download?signature=${secret}`,
  options?: {
    readonly headers?: ReadonlyArray<AttachmentTransfer.GrantHeader>
    readonly offset?: number
    readonly bytes?: number
    readonly objectBytes?: number
  }
) => {
  const offset = options?.offset ?? 0
  const bytes = options?.bytes ?? 3
  const objectBytes = options?.objectBytes ?? bytes
  return AttachmentTransfer.DownloadGrant.make({
    grantId: AttachmentTransfer.GrantId.make("grant"),
    objectVersion: AttachmentTransfer.ObjectVersion.make("version"),
    objectBytes,
    expiresAt: 10_000,
    chunk: AttachmentTransfer.VerifiedChunk.make({
      index: 0,
      offset,
      bytes,
      digest: Attachment.Digest.make(`sha256:${"0".repeat(64)}`)
    }),
    slice: { offset: 0, length: bytes },
    request: AttachmentTransfer.DirectDownloadRequest.make({
      method: "GET",
      url: AttachmentTransfer.GrantUrl.make(url),
      headers: options?.headers ?? []
    })
  })
}

const layer = (fetch: typeof globalThis.fetch, allowedOrigins: ReadonlyArray<string> = [origin]) =>
  AttachmentDirectHttpClient.layer({ allowedOrigins }).pipe(
    Layer.provide(FetchHttpClient.layer),
    Layer.provide(Layer.succeed(FetchHttpClient.Fetch, fetch))
  )

const bytesFrom = (chunks: ReadonlyArray<Uint8Array>) => {
  const bytes = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0))
  let offset = 0
  for (const chunk of chunks) {
    bytes.set(chunk, offset)
    offset += chunk.length
  }
  return bytes
}

const failureTag = <E extends { readonly _tag: string },>(exit: Exit.Exit<unknown, E>) => {
  assert(Exit.isFailure(exit))
  const result = Cause.findError(exit.cause)
  assert(Result.isSuccess(result))
  return result.success._tag
}

describe("AttachmentDirectHttpClient", () => {
  it.effect("requires a nonempty public HTTPS origin allowlist", () =>
    Effect.gen(function*() {
      for (const allowedOrigins of [[], ["http://objects.example"], ["https://127.0.0.1"]]) {
        const exit = yield* AttachmentDirectHttpClient.AttachmentDirectHttpClient.pipe(
          Effect.provide(layer(globalThis.fetch, allowedOrigins)),
          Effect.exit
        )
        assert(Exit.isFailure(exit))
        assert.strictEqual(failureTag(exit), "AttachmentDirectHttpConfigurationError")
      }
    }))

  it.effect("streams upload and download bytes directly with locked fetch options", () =>
    Effect.gen(function*() {
      const calls: Array<{ readonly url: string; readonly init: RequestInit }> = []
      const fetch = (async (input: URL | RequestInfo, init?: RequestInit) => {
        calls.push({ url: String(input), init: init ?? {} })
        const request = new Request(input, init)
        if (request.method === "PUT") {
          assert.deepStrictEqual(new Uint8Array(await request.arrayBuffer()), Uint8Array.of(1, 2, 3))
          return new Response(null, { status: 200 })
        }
        return new Response(Uint8Array.of(1, 2, 3), {
          status: 200,
          headers: { "content-length": "3" }
        })
      }) as typeof globalThis.fetch

      const client = yield* AttachmentDirectHttpClient.AttachmentDirectHttpClient.pipe(Effect.provide(layer(fetch)))
      yield* client.upload(uploadGrant(), Stream.make(Uint8Array.of(1), Uint8Array.of(2, 3)))
      const downloaded = yield* client.download(downloadGrant()).pipe(Stream.runCollect)

      assert.deepStrictEqual(bytesFrom(downloaded), Uint8Array.of(1, 2, 3))
      assert.strictEqual(calls.length, 2)
      for (const call of calls) {
        assert.strictEqual(call.init.redirect, "manual")
        assert.strictEqual(call.init.credentials, "omit")
        assert.strictEqual(call.init.referrerPolicy, "no-referrer")
        assert.strictEqual(call.init.cache, "no-store")
      }
    }))

  it.effect("refuses redirects without following them and cleans up the body", () =>
    Effect.gen(function*() {
      let calls = 0
      let signal: AbortSignal | undefined
      const fetch = (async (_input: URL | RequestInfo, init?: RequestInit) => {
        calls++
        signal = init?.signal ?? undefined
        return new Response(new ReadableStream({ pull() {} }), {
          status: 307,
          headers: { location: "https://attacker.example/stolen" }
        })
      }) as typeof globalThis.fetch
      const client = yield* AttachmentDirectHttpClient.AttachmentDirectHttpClient.pipe(Effect.provide(layer(fetch)))
      const exit = yield* client.download(downloadGrant()).pipe(Stream.runDrain, Effect.exit)
      assert(Exit.isFailure(exit))
      assert.strictEqual(failureTag(exit), "AttachmentTransferStatusError")
      assert.strictEqual(calls, 1)
      assert.isTrue(signal?.aborted)
    }))

  it.effect("rejects unlisted, private, userinfo, fragment, and non HTTPS grants before fetch", () =>
    Effect.gen(function*() {
      let calls = 0
      const fetch = (async () => {
        calls++
        return new Response(null, { status: 200 })
      }) as typeof globalThis.fetch
      const client = yield* AttachmentDirectHttpClient.AttachmentDirectHttpClient.pipe(Effect.provide(layer(fetch)))
      const urls = [
        "https://other.example/object",
        "https://127.0.0.1/object",
        "https://2130706433/object",
        "https://[::ffff:127.0.0.1]/object",
        "https://user:password@objects.example/object",
        "https://objects.example/object#fragment",
        "http://objects.example/object"
      ]
      for (const url of urls) {
        const exit = yield* client.upload(uploadGrant(url), Stream.make(Uint8Array.of(1, 2, 3))).pipe(Effect.exit)
        assert(Exit.isFailure(exit))
        assert.strictEqual(failureTag(exit), "AttachmentGrantRejected")
      }
      assert.strictEqual(calls, 0)
    }))

  it.effect("omits ambient credentials and rejects unsafe granted headers", () =>
    Effect.gen(function*() {
      let seenHeaders: Headers | undefined
      const fetch = (async (input: URL | RequestInfo, init?: RequestInit) => {
        seenHeaders = new Request(input, init).headers
        return new Response(null, { status: 200 })
      }) as typeof globalThis.fetch
      const client = yield* AttachmentDirectHttpClient.AttachmentDirectHttpClient.pipe(Effect.provide(layer(fetch)))
      yield* client.upload(uploadGrant(), Stream.make(Uint8Array.of(1, 2, 3)))
      assert.strictEqual(seenHeaders?.get("cookie"), null)
      assert.strictEqual(seenHeaders?.get("authorization"), null)
      assert.strictEqual(seenHeaders?.get("traceparent"), null)

      for (
        const name of ["authorization", "cookie", "connection", "content-length", "proxy-authorization", "traceparent"]
      ) {
        const exit = yield* client.upload(
          uploadGrant(undefined, [AttachmentTransfer.GrantHeader.make({
            name: AttachmentTransfer.GrantHeaderName.make(name),
            value: AttachmentTransfer.GrantHeaderValue.make(secret)
          })]),
          Stream.make(Uint8Array.of(1, 2, 3))
        ).pipe(Effect.exit)
        assert(Exit.isFailure(exit))
        assert.strictEqual(failureTag(exit), "AttachmentGrantRejected")
      }
    }))

  it.effect("keeps signed URL and header secrets out of spans and errors", () =>
    Effect.gen(function*() {
      const spans: Array<Tracer.NativeSpan> = []
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options)
          spans.push(span)
          return span
        }
      })
      const fetch = (async () => {
        throw new Error(secret)
      }) as typeof globalThis.fetch
      const client = yield* AttachmentDirectHttpClient.AttachmentDirectHttpClient.pipe(Effect.provide(layer(fetch)))
      const exit = yield* client.upload(
        uploadGrant(undefined, [AttachmentTransfer.GrantHeader.make({
          name: AttachmentTransfer.GrantHeaderName.make("x-amz-meta-token"),
          value: AttachmentTransfer.GrantHeaderValue.make(secret)
        })]),
        Stream.make(Uint8Array.of(1, 2, 3))
      ).pipe(Effect.provideService(Tracer.Tracer, tracer), Effect.exit)
      assert(Exit.isFailure(exit))
      assert.strictEqual(failureTag(exit), "AttachmentTransferUnavailable")
      assert(!String(exit).includes(secret))
      assert(!String(exit).includes("signature"))
      assert.strictEqual(spans.length, 1)
      assert.strictEqual(spans[0]?.name, "AttachmentDirectHttpClient.upload")
      assert(!String([...spans[0]!.attributes.entries()]).includes(secret))
    }))

  it.effect("aborts an interrupted upload", () =>
    Effect.gen(function*() {
      let signal: AbortSignal | undefined
      let notifyStarted!: () => void
      const started = new Promise<void>((resolve) => {
        notifyStarted = resolve
      })
      const fetch = ((_: URL | RequestInfo, init?: RequestInit) => {
        signal = init?.signal ?? undefined
        notifyStarted()
        return new Promise<Response>(() => {
        })
      }) as typeof globalThis.fetch
      const client = yield* AttachmentDirectHttpClient.AttachmentDirectHttpClient.pipe(Effect.provide(layer(fetch)))
      const fiber = yield* client.upload(uploadGrant(), Stream.make(Uint8Array.of(1, 2, 3))).pipe(Effect.forkChild)
      yield* Effect.promise(() => started)
      yield* Fiber.interrupt(fiber)
      assert.isTrue(signal?.aborted)
    }))

  it.effect("enforces expiry, upload status, download length, and exact ranges", () =>
    Effect.gen(function*() {
      const fallback = (async () => new Response(null, { status: 200 })) as typeof globalThis.fetch
      const client = yield* AttachmentDirectHttpClient.AttachmentDirectHttpClient.pipe(Effect.provide(layer(fallback)))
      const expired = yield* client.upload(
        { ...uploadGrant(), expiresAt: 0 },
        Stream.make(Uint8Array.of(1, 2, 3))
      ).pipe(Effect.exit)
      assert(Exit.isFailure(expired))
      assert.strictEqual(failureTag(expired), "AttachmentGrantExpired")

      const shortUpload = yield* client.upload(uploadGrant(), Stream.make(Uint8Array.of(1, 2))).pipe(
        Effect.provideService(FetchHttpClient.Fetch, async (_input, init) => {
          await new Request(_input, init).arrayBuffer()
          return new Response(null, { status: 200 })
        }),
        Effect.exit
      )
      assert(Exit.isFailure(shortUpload))
      assert.strictEqual(failureTag(shortUpload), "AttachmentTransferLengthMismatch")

      const badUpload = yield* client.upload(uploadGrant(), Stream.make(Uint8Array.of(1, 2, 3))).pipe(
        Effect.provideService(FetchHttpClient.Fetch, async () => new Response(null, { status: 204 })),
        Effect.exit
      )
      assert(Exit.isFailure(badUpload))
      assert.strictEqual(failureTag(badUpload), "AttachmentTransferStatusError")

      const wrongLength = yield* client.download(downloadGrant()).pipe(
        Stream.provideService(FetchHttpClient.Fetch, async () =>
          new Response(Uint8Array.of(1, 2), {
            status: 200,
            headers: { "content-length": "2" }
          })),
        Stream.runDrain,
        Effect.exit
      )
      assert(Exit.isFailure(wrongLength))
      assert.strictEqual(failureTag(wrongLength), "AttachmentTransferLengthMismatch")

      const truncatedBody = yield* client.download(downloadGrant()).pipe(
        Stream.provideService(FetchHttpClient.Fetch, async () =>
          new Response(Uint8Array.of(1, 2), {
            status: 200,
            headers: { "content-length": "3" }
          })),
        Stream.runDrain,
        Effect.exit
      )
      assert(Exit.isFailure(truncatedBody))
      assert.strictEqual(failureTag(truncatedBody), "AttachmentTransferLengthMismatch")

      const range = AttachmentTransfer.GrantHeader.make({
        name: AttachmentTransfer.GrantHeaderName.make("range"),
        value: AttachmentTransfer.GrantHeaderValue.make("bytes=10-12")
      })
      const wrongRange = yield* client.download(
        downloadGrant(undefined, { headers: [range], offset: 10, objectBytes: 20 })
      ).pipe(
        Stream.provideService(FetchHttpClient.Fetch, async () =>
          new Response(Uint8Array.of(1, 2, 3), {
            status: 206,
            headers: { "content-length": "3", "content-range": "bytes 11-13/20" }
          })),
        Stream.runDrain,
        Effect.exit
      )
      assert(Exit.isFailure(wrongRange))
      assert.strictEqual(failureTag(wrongRange), "AttachmentTransferRangeMismatch")

      const wrongTotal = yield* client.download(
        downloadGrant(undefined, { headers: [range], offset: 10, objectBytes: 20 })
      ).pipe(
        Stream.provideService(FetchHttpClient.Fetch, async () =>
          new Response(Uint8Array.of(1, 2, 3), {
            status: 206,
            headers: { "content-length": "3", "content-range": "bytes 10-12/21" }
          })),
        Stream.runDrain,
        Effect.exit
      )
      assert(Exit.isFailure(wrongTotal))
      assert.strictEqual(failureTag(wrongTotal), "AttachmentTransferRangeMismatch")
    }))
})
