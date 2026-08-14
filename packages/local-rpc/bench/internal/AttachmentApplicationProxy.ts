import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse"

/* oxlint-disable effect/noTernary, typescript/consistent-return -- The fixture preserves the production proxy protocol shape and maps HTTP responses directly. */

export interface Monitor {
  readonly recordApplicationIngress: (bytes: number) => void
  readonly recordApplicationEgress: (bytes: number) => void
  readonly recordProviderBytes: (bytes: number) => void
}

export interface Payload {
  readonly bytes: Uint8Array
  readonly digest: string
}

const chunkBytes = 256 * 1024
const bytesHeader = "x-effect-local-attachment-bytes"
const offsetHeader = "upload-offset"
const completeHeader = "upload-complete"

const streamPayload = (payload: Uint8Array) => {
  const chunks: Array<Uint8Array> = []
  for (let offset = 0; offset < payload.length; offset += chunkBytes) {
    const end = Math.min(payload.length, offset + chunkBytes)
    chunks.push(payload.subarray(offset, end))
  }
  return Stream.fromIterable(chunks)
}

export const make = (payloads: ReadonlyMap<string, Payload>) => {
  let monitor: Monitor | undefined
  const completed = new Set<string>()
  const layerProxyRoute = Layer.effectDiscard(Effect.gen(function*() {
    const router = yield* HttpRouter.HttpRouter
    const payload = (digest: string | undefined, bytes: string | undefined) => {
      if (digest === undefined || bytes === undefined) return undefined
      const found = payloads.get(digest)
      if (found === undefined || String(found.bytes.length) !== bytes) return undefined
      return found
    }
    yield* router.addAll([
      HttpRouter.route(
        "HEAD",
        "/attachments/:lane/:digest",
        Effect.fnUntraced(function*(request) {
          const params = yield* HttpRouter.params
          const found = payload(params.digest, request.headers[bytesHeader])
          if (found === undefined) return HttpServerResponse.empty({ status: 422 })
          const key = `${params.lane}:${found.digest}`
          return HttpServerResponse.empty({
            status: 204,
            headers: {
              [offsetHeader]: completed.has(key) ? String(found.bytes.length) : "0",
              [completeHeader]: String(completed.has(key))
            }
          })
        })
      ),
      HttpRouter.route(
        "PATCH",
        "/attachments/:lane/:digest",
        Effect.fnUntraced(function*(request) {
          const params = yield* HttpRouter.params
          const found = payload(params.digest, request.headers[bytesHeader])
          if (found === undefined || request.headers[offsetHeader] !== "0") {
            return HttpServerResponse.empty({ status: 422 })
          }
          let offset = 0
          let equal = true
          yield* Stream.runForEach(request.stream, (chunk) => {
            monitor?.recordApplicationIngress(chunk.length)
            monitor?.recordProviderBytes(chunk.length)
            if (!equal || offset + chunk.length > found.bytes.length) {
              equal = false
            } else {
              const expected = found.bytes.subarray(offset, offset + chunk.length)
              equal = Buffer.compare(Buffer.from(chunk), Buffer.from(expected)) === 0
            }
            offset += chunk.length
            return Effect.void
          })
          if (!equal || offset !== found.bytes.length) return HttpServerResponse.empty({ status: 422 })
          completed.add(`${params.lane}:${found.digest}`)
          return HttpServerResponse.empty({
            status: 204,
            headers: {
              [offsetHeader]: String(offset),
              [completeHeader]: "true"
            }
          })
        })
      ),
      HttpRouter.route(
        "GET",
        "/attachments/:lane/:digest",
        Effect.fnUntraced(function*(request) {
          const params = yield* HttpRouter.params
          const found = payload(params.digest, request.headers[bytesHeader])
          if (found === undefined || !completed.has(`${params.lane}:${found.digest}`)) {
            return HttpServerResponse.empty({ status: 404 })
          }
          const body = streamPayload(found.bytes).pipe(Stream.tap((chunk) => {
            monitor?.recordProviderBytes(chunk.length)
            monitor?.recordApplicationEgress(chunk.length)
            return Effect.void
          }))
          return HttpServerResponse.stream(body, { contentLength: found.bytes.length })
        })
      )
    ])
  }))
  const web = HttpRouter.toWebHandler(layerProxyRoute, { disableLogger: true })
  const fetch =
    ((input: URL | RequestInfo, init?: RequestInit) =>
      web.handler(new Request(input, init))) satisfies typeof globalThis.fetch

  const run = Effect.gen(function*() {
    const client = yield* HttpClient.HttpClient
    const transfer = Effect.fnUntraced(function*(lane: number, payload: Payload) {
      const url = `http://application.test/attachments/${lane}/${payload.digest}`
      const headers = { [bytesHeader]: String(payload.bytes.length) }
      const head = yield* client.execute(HttpClientRequest.head(url, { headers }))
      if (head.status !== 204) return yield* Effect.die(`Proxy HEAD failed with ${head.status}`)
      if (head.headers[completeHeader] !== "true") {
        const upload = HttpClientRequest.patch(url, { headers: { ...headers, [offsetHeader]: "0" } }).pipe(
          HttpClientRequest.bodyStream(streamPayload(payload.bytes), { contentLength: payload.bytes.length })
        )
        const response = yield* client.execute(upload)
        if (response.status !== 204 || response.headers[completeHeader] !== "true") {
          return yield* Effect.die(`Proxy PATCH failed with ${response.status}`)
        }
      }
      const download = yield* client.execute(HttpClientRequest.get(url, { headers }))
      if (download.status !== 200) return yield* Effect.die(`Proxy GET failed with ${download.status}`)
      let offset = 0
      let equal = true
      yield* Stream.runForEach(download.stream, (chunk) => {
        if (!equal || offset + chunk.length > payload.bytes.length) {
          equal = false
        } else {
          const expected = payload.bytes.subarray(offset, offset + chunk.length)
          equal = Buffer.compare(Buffer.from(chunk), Buffer.from(expected)) === 0
        }
        offset += chunk.length
        return Effect.void
      })
      if (!equal || offset !== payload.bytes.length) return yield* Effect.die("Proxy download bytes differed")
    })
    return transfer
  }).pipe(
    Effect.provide(FetchHttpClient.layer),
    Effect.provideService(FetchHttpClient.Fetch, fetch)
  )

  return {
    dispose: () => web.dispose(),
    reset: (nextMonitor: Monitor) => {
      completed.clear()
      monitor = nextMonitor
    },
    run
  }
}
