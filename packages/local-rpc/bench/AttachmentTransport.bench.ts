import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import { createHash } from "node:crypto"
import { performance } from "node:perf_hooks"
import * as Proxy from "./internal/AttachmentApplicationProxy.ts"

/* oxlint-disable effect/noAsyncFunction, effect/noGlobals, eslint/no-await-in-loop, effect/noThrowStatement, effect/noNewError, effect/noTernary, typescript/consistent-return -- This executable benchmark owns its process entry point, byte validation loops, and Web Fetch fixtures. */

const MiB = 1024 * 1024
const maximumAttachmentBytes = 64 * MiB
const sizes = [1 * MiB, 8 * MiB, maximumAttachmentBytes] as const
const warmups = 1
const repetitions = 3
const boundedConcurrency = 4
const chunkBytes = 256 * 1024

interface Counters {
  applicationIngressBytes: number
  applicationEgressBytes: number
  providerBytes: number
  heapStart: number
  heapEnd: number
  heapPeak: number
  rssStart: number
  rssEnd: number
  rssPeak: number
}

interface Sample extends Counters {
  readonly path: "application proxy" | "direct provider"
  readonly sizeBytes: number
  readonly concurrency: number
  readonly milliseconds: number
  readonly mibPerSecond: number
}

const memory = () => process.memoryUsage()

const makeCounters = (): Counters & Proxy.Monitor => {
  const start = memory()
  const counters: Counters & Proxy.Monitor = {
    applicationIngressBytes: 0,
    applicationEgressBytes: 0,
    providerBytes: 0,
    heapStart: start.heapUsed,
    heapEnd: start.heapUsed,
    heapPeak: start.heapUsed,
    rssStart: start.rss,
    rssEnd: start.rss,
    rssPeak: start.rss,
    recordApplicationIngress: (bytes) => {
      counters.applicationIngressBytes += bytes
      sampleMemory(counters)
    },
    recordApplicationEgress: (bytes) => {
      counters.applicationEgressBytes += bytes
      sampleMemory(counters)
    },
    recordProviderBytes: (bytes) => {
      counters.providerBytes += bytes
      sampleMemory(counters)
    }
  }
  return counters
}

const sampleMemory = (counters: Counters) => {
  const next = memory()
  counters.heapEnd = next.heapUsed
  counters.rssEnd = next.rss
  counters.heapPeak = Math.max(counters.heapPeak, next.heapUsed)
  counters.rssPeak = Math.max(counters.rssPeak, next.rss)
}

const payloadFor = (size: number): Proxy.Payload => {
  const bytes = new Uint8Array(size)
  for (let offset = 0; offset < bytes.length; offset++) bytes[offset] = (offset * 31 + 17) & 255
  const digest = `sha256:${createHash("sha256").update(bytes).digest("hex")}`
  return { bytes, digest }
}

const readable = (bytes: Uint8Array) => {
  let offset = 0
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (offset === bytes.length) {
        controller.close()
        return
      }
      const end = Math.min(bytes.length, offset + chunkBytes)
      controller.enqueue(bytes.subarray(offset, end))
      offset = end
    }
  })
}

const streamPayload = (bytes: Uint8Array) => {
  const chunks: Array<Uint8Array> = []
  for (let offset = 0; offset < bytes.length; offset += chunkBytes) {
    const end = Math.min(bytes.length, offset + chunkBytes)
    chunks.push(bytes.subarray(offset, end))
  }
  return Stream.fromIterable(chunks)
}

const consumeAndVerify = async (response: Response, payload: Proxy.Payload, counters: Counters) => {
  if (response.body === null) throw new Error("Missing response body")
  const reader = response.body.getReader()
  let offset = 0
  for (;;) {
    const next = await reader.read()
    if (next.done) break
    const chunk = next.value
    counters.providerBytes += chunk.length
    sampleMemory(counters)
    const expected = payload.bytes.subarray(offset, offset + chunk.length)
    if (Buffer.compare(Buffer.from(chunk), Buffer.from(expected)) !== 0) {
      throw new Error("Direct download bytes differed")
    }
    offset += chunk.length
  }
  if (offset !== payload.bytes.length) throw new Error("Direct download length differed")
}

const directFetch =
  (payloads: ReadonlyMap<string, Proxy.Payload>, counters: Counters) =>
  async (input: URL | RequestInfo, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init)
    const url = new URL(request.url)
    const path = url.pathname.split("/").filter(Boolean)
    if (url.origin === "http://application.test") {
      const operation = path[1]
      const lane = path[2]
      const digest = path[3]
      if ((operation !== "upload" && operation !== "download") || lane === undefined || digest === undefined) {
        return new Response(null, { status: 404 })
      }
      return new Response(null, {
        status: 204,
        headers: { location: `http://provider.test/${operation}/${lane}/${digest}` }
      })
    }
    if (url.origin !== "http://provider.test") return new Response(null, { status: 404 })
    const operation = path[0]
    const digest = path[2]
    const payload = digest === undefined ? undefined : payloads.get(digest)
    if (payload === undefined) return new Response(null, { status: 404 })
    if (operation === "upload" && request.method === "PUT") {
      if (request.body === null) return new Response(null, { status: 422 })
      const reader = request.body.getReader()
      let offset = 0
      for (;;) {
        const next = await reader.read()
        if (next.done) break
        const chunk = next.value
        counters.providerBytes += chunk.length
        sampleMemory(counters)
        const expected = payload.bytes.subarray(offset, offset + chunk.length)
        if (Buffer.compare(Buffer.from(chunk), Buffer.from(expected)) !== 0) return new Response(null, { status: 422 })
        offset += chunk.length
      }
      return new Response(null, { status: offset === payload.bytes.length ? 204 : 422 })
    }
    if (operation === "download" && request.method === "GET") {
      return new Response(readable(payload.bytes), { status: 200 })
    }
    return new Response(null, { status: 404 })
  }

const runDirect = Effect.fnUntraced(function*(
  payloads: ReadonlyMap<string, Proxy.Payload>,
  counters: Counters,
  lane: number,
  payload: Proxy.Payload
) {
  const fetch = directFetch(payloads, counters)
  const appBase = `http://application.test/grants`
  const uploadGrant = yield* Effect.promise(() =>
    fetch(`${appBase}/upload/${lane}/${payload.digest}`, { method: "POST" })
  )
  const uploadUrl = uploadGrant.headers.get("location")
  if (uploadUrl === null) return yield* Effect.die("Missing direct upload grant")
  const uploadRequest = HttpClientRequest.put(uploadUrl).pipe(
    HttpClientRequest.bodyStream(
      streamPayload(payload.bytes),
      { contentLength: payload.bytes.length }
    )
  )
  const client = yield* HttpClient.HttpClient
  const uploaded = yield* client.execute(uploadRequest).pipe(Effect.provideService(FetchHttpClient.Fetch, fetch))
  if (uploaded.status !== 204) return yield* Effect.die(`Direct PUT failed with ${uploaded.status}`)
  const downloadGrant = yield* Effect.promise(() =>
    fetch(`${appBase}/download/${lane}/${payload.digest}`, { method: "POST" })
  )
  const downloadUrl = downloadGrant.headers.get("location")
  if (downloadUrl === null) return yield* Effect.die("Missing direct download grant")
  const downloaded = yield* Effect.promise(() => fetch(downloadUrl))
  yield* Effect.promise(() => consumeAndVerify(downloaded, payload, counters))
})

const percentile = (values: ReadonlyArray<number>, fraction: number) => {
  const sorted = values.toSorted((left, right) => left - right)
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)]
}

const formatBytes = (bytes: number) => (bytes / MiB).toFixed(2)
const formatNumber = (value: number) => value.toFixed(2)

const main = Effect.scoped(Effect.gen(function*() {
  const payloadBySize = new Map(sizes.map((size) => {
    const payload = payloadFor(size)
    return [size, payload] as const
  }))
  const payloads = new Map([...payloadBySize.values()].map((payload) => [payload.digest, payload] as const))
  const proxy = Proxy.make(payloads)
  yield* Effect.addFinalizer(() => Effect.promise(proxy.dispose))
  const proxyRun = yield* proxy.run
  const samples: Array<Sample> = []

  for (const size of sizes) {
    const payload = payloadBySize.get(size)
    if (payload === undefined) return yield* Effect.die(`Missing ${size} byte payload`)
    for (const concurrency of [1, boundedConcurrency]) {
      for (const path of ["application proxy", "direct provider"] as const) {
        for (let iteration = 0; iteration < warmups + repetitions; iteration++) {
          const counters = makeCounters()
          if (path === "application proxy") proxy.reset(counters)
          const started = performance.now()
          const operations = Array.from({ length: concurrency }, (_, lane) => {
            if (path === "application proxy") return proxyRun(lane, payload)
            return runDirect(payloads, counters, lane, payload).pipe(Effect.provide(FetchHttpClient.layer))
          })
          yield* Effect.all(operations, { concurrency: "unbounded", discard: true })
          const milliseconds = performance.now() - started
          sampleMemory(counters)
          const expectedPayloadBytes = size * concurrency
          if (counters.providerBytes !== expectedPayloadBytes * 2) {
            return yield* Effect.die(`Provider byte count differed for ${path}`)
          }
          if (path === "application proxy") {
            if (
              counters.applicationIngressBytes !== expectedPayloadBytes ||
              counters.applicationEgressBytes !== expectedPayloadBytes
            ) return yield* Effect.die("Proxy application byte count differed")
          } else if (counters.applicationIngressBytes !== 0 || counters.applicationEgressBytes !== 0) {
            return yield* Effect.die("Direct provider payload crossed the application origin")
          }
          if (iteration >= warmups) {
            samples.push({
              ...counters,
              path,
              sizeBytes: size,
              concurrency,
              milliseconds,
              mibPerSecond: (expectedPayloadBytes * 2 / MiB) / (milliseconds / 1_000)
            })
          }
        }
      }
    }
  }

  const node = process.version
  const platform = `${process.platform} ${process.arch}`
  const cpu = process.env.EFFECT_LOCAL_BENCH_CPU ?? "Apple Silicon host"
  const rows = [
    "# Attachment transport byte path benchmark",
    "",
    `Environment: Node ${node}, ${platform}, ${cpu}.`,
    "",
    `Method: ${warmups} fixed warmup and ${repetitions} measured repetitions per case. Payloads use deterministic bytes in 256 KiB chunks. Each operation uploads and downloads the complete payload, then compares every returned chunk. Concurrency is either sequential or bounded at ${boundedConcurrency}. Memory peaks are sampled at every application or provider chunk boundary through process.memoryUsage(). Deltas compare the last sample with the pre-operation sample. Application byte counters include attachment payload only. Grant metadata is excluded.`,
    "",
    `Configured maximum attachment size: ${maximumAttachmentBytes / MiB} MiB.`,
    "",
    "## Aggregate",
    "",
    "| Path | Size MiB | Concurrency | Median ms | p95 ms | Median MiB/s | Heap peak delta MiB | RSS peak delta MiB | App ingress MiB | App egress MiB | Provider MiB |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
  ]
  for (const size of sizes) {
    for (const concurrency of [1, boundedConcurrency]) {
      for (const path of ["application proxy", "direct provider"] as const) {
        const group = samples.filter((sample) =>
          sample.sizeBytes === size && sample.concurrency === concurrency && sample.path === path
        )
        const medianMs = percentile(group.map((sample) => sample.milliseconds), 0.5)
        const p95Ms = percentile(group.map((sample) => sample.milliseconds), 0.95)
        const medianRate = percentile(group.map((sample) => sample.mibPerSecond), 0.5)
        const medianHeapPeak = percentile(group.map((sample) => sample.heapPeak - sample.heapStart), 0.5)
        const medianRssPeak = percentile(group.map((sample) => sample.rssPeak - sample.rssStart), 0.5)
        const representative = group[0]
        rows.push(
          `| ${path} | ${size / MiB} | ${concurrency} | ${formatNumber(medianMs)} | ${formatNumber(p95Ms)} | ${
            formatNumber(medianRate)
          } | ${formatBytes(medianHeapPeak)} | ${formatBytes(medianRssPeak)} | ${
            formatBytes(representative.applicationIngressBytes)
          } | ${formatBytes(representative.applicationEgressBytes)} | ${formatBytes(representative.providerBytes)} |`
        )
      }
    }
  }
  rows.push(
    "",
    "## Raw samples",
    "",
    "Heap and RSS values are MiB. Negative end deltas mean garbage collection reclaimed memory during a sample.",
    "",
    "| Path | Size MiB | Concurrency | Sample | ms | MiB/s | Heap end delta | Heap peak delta | RSS end delta | RSS peak delta |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
  )
  const groupCounts = new Map<string, number>()
  for (const sample of samples) {
    const key = `${sample.path}:${sample.sizeBytes}:${sample.concurrency}`
    const sequence = (groupCounts.get(key) ?? 0) + 1
    groupCounts.set(key, sequence)
    rows.push(
      `| ${sample.path} | ${sample.sizeBytes / MiB} | ${sample.concurrency} | ${sequence} | ${
        formatNumber(sample.milliseconds)
      } | ${formatNumber(sample.mibPerSecond)} | ${formatBytes(sample.heapEnd - sample.heapStart)} | ${
        formatBytes(sample.heapPeak - sample.heapStart)
      } | ${formatBytes(sample.rssEnd - sample.rssStart)} | ${formatBytes(sample.rssPeak - sample.rssStart)} |`
    )
  }
  return rows.join("\n")
}))

// oxlint-disable-next-line effect-local/noManualEffectBoundary -- This file is the benchmark process entry point.
void Effect.runPromise(main).then(
  (report) => process.stdout.write(`${report}\n`),
  (cause) => {
    process.stderr.write(`${String(cause)}\n`)
    process.exitCode = 1
  }
)
