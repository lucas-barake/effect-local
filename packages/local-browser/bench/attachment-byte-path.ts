/* oxlint-disable effect/noGlobals, effect/noNewPromise, effect/noNewError, effect/noAsyncFunction, effect/noThrowStatement, no-await-in-loop -- The browser benchmark measures native Worker, Promise, timing, and sequential storage operations at the host boundary. */
import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import * as Effect from "effect/Effect"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"

interface Sample {
  readonly path: "sqlite-clone" | "sqlite-transfer" | "sqlite-read" | "opfs-write" | "opfs-read"
  readonly bytes: number
  readonly milliseconds: number
  readonly mebibytesPerSecond: number
}

interface Summary {
  readonly path: Sample["path"]
  readonly bytes: number
  readonly repetitions: number
  readonly medianMilliseconds: number
  readonly p95Milliseconds: number
  readonly medianMebibytesPerSecond: number
}

interface OpfsRequest {
  readonly id: number
  readonly operation: "write" | "read" | "remove"
  readonly name: string
  readonly bytes?: Uint8Array
}

declare global {
  interface Window {
    __attachmentBenchmark?: {
      readonly status: "running" | "complete" | "failed"
      readonly userAgent: string
      readonly samples?: ReadonlyArray<Sample>
      readonly summaries?: ReadonlyArray<Summary>
      readonly storageDelta?: number
      readonly error?: string
    }
  }
}

const status = document.querySelector<HTMLParagraphElement>("#status")!
const results = document.querySelector<HTMLPreElement>("#results")!
const query = new URLSearchParams(location.search)
const repetitions = Number(query.get("repetitions") ?? 7)
const sizes = (query.get("sizes") ?? "65536,1048576,8388608").split(",").map(Number)
const samples: Array<Sample> = []
let nextRequestId = 0

const sqliteWorker = new Worker(new URL("./attachment-sqlite-worker.ts", import.meta.url), { type: "module" })
const opfsWorker = new Worker(new URL("./attachment-opfs-worker.ts", import.meta.url), { type: "module" })
const runtime = ManagedRuntime.make(SqliteClient.layer({
  worker: Effect.acquireRelease(
    Effect.succeed(sqliteWorker),
    () => Effect.sync(() => sqliteWorker.terminate())
  )
}))

const requestOpfs = <A,>(message: Omit<OpfsRequest, "id">, transfer: ReadonlyArray<Transferable> = []) =>
  new Promise<A>((resolve, reject) => {
    const id = nextRequestId++
    const listener = (event: MessageEvent<{ readonly id: number; readonly error?: string } & A>) => {
      if (event.data.id !== id) return
      opfsWorker.removeEventListener("message", listener)
      if (event.data.error) reject(new Error(event.data.error))
      else resolve(event.data)
    }
    opfsWorker.addEventListener("message", listener)
    opfsWorker.postMessage({ ...message, id }, [...transfer])
  })

const makeBytes = (length: number) => {
  const bytes = new Uint8Array(length)
  for (let index = 0; index < bytes.length; index++) bytes[index] = index % 251
  return bytes
}

const measure = async (path: Sample["path"], bytes: number, operation: () => Promise<void>) => {
  const started = performance.now()
  await operation()
  const milliseconds = performance.now() - started
  samples.push({
    path,
    bytes,
    milliseconds,
    mebibytesPerSecond: bytes / 1024 / 1024 / (milliseconds / 1_000)
  })
}

const percentile = (values: ReadonlyArray<number>, fraction: number) => {
  const sorted = values.toSorted((left, right) => left - right)
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)]
}

const summarize = (): Array<Summary> => {
  const groups = new Map<string, Array<Sample>>()
  for (const sample of samples) {
    const key = `${sample.path}:${sample.bytes}`
    groups.set(key, [...(groups.get(key) ?? []), sample])
  }
  return [...groups.values()].map((group) => ({
    path: group[0].path,
    bytes: group[0].bytes,
    repetitions: group.length,
    medianMilliseconds: percentile(group.map((sample) => sample.milliseconds), 0.5),
    p95Milliseconds: percentile(group.map((sample) => sample.milliseconds), 0.95),
    medianMebibytesPerSecond: percentile(group.map((sample) => sample.mebibytesPerSecond), 0.5)
  }))
}

const verify = (actual: Uint8Array, expected: Uint8Array) => {
  if (actual.length !== expected.length) throw new Error(`length mismatch: ${actual.length} !== ${expected.length}`)
  for (let index = 0; index < actual.length; index++) {
    if (actual[index] !== expected[index]) throw new Error(`byte mismatch at ${index}`)
  }
}

const run = async () => {
  window.__attachmentBenchmark = { status: "running", userAgent: navigator.userAgent }
  const beforeStorage = (await navigator.storage.estimate()).usage ?? 0
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- The browser entrypoint must start the Effect runtime from a Promise host.
  await runtime.runPromise(Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* sql.unsafe("DROP TABLE IF EXISTS attachment_byte_path")
    yield* sql.unsafe("CREATE TABLE attachment_byte_path (id TEXT PRIMARY KEY, bytes BLOB NOT NULL)")
  }))

  for (const size of sizes) {
    status.textContent = `Running ${size} byte samples…`
    const expected = makeBytes(size)
    const fileName = `attachment-${size}.bin`
    for (let repetition = 0; repetition < repetitions; repetition++) {
      const cloneId = `clone-${size}-${repetition}`
      const cloneBytes = expected.slice()
      await measure("sqlite-clone", size, () =>
        // oxlint-disable-next-line effect-local/noManualEffectBoundary -- The benchmark timer invokes each measured Effect through its Promise host.
        runtime.runPromise(Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          yield* sql.unsafe("INSERT INTO attachment_byte_path (id, bytes) VALUES (?, ?)", [cloneId, cloneBytes])
        })))

      const transferId = `transfer-${size}-${repetition}`
      const transferBytes = expected.slice()
      await measure("sqlite-transfer", size, () =>
        // oxlint-disable-next-line effect-local/noManualEffectBoundary -- The benchmark timer invokes each measured Effect through its Promise host.
        runtime.runPromise(Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          yield* SqliteClient.withTransferables([transferBytes.buffer])(
            sql.unsafe("INSERT INTO attachment_byte_path (id, bytes) VALUES (?, ?)", [transferId, transferBytes])
          )
        })))

      await measure("sqlite-read", size, () =>
        // oxlint-disable-next-line effect-local/noManualEffectBoundary -- The benchmark timer invokes each measured Effect through its Promise host.
        runtime.runPromise(Effect.gen(function*() {
          const sql = yield* SqlClient.SqlClient
          const rows = yield* sql.unsafe<{ readonly bytes: Uint8Array }>(
            "SELECT bytes FROM attachment_byte_path WHERE id = ?",
            [cloneId]
          )
          verify(rows[0].bytes, expected)
        })))

      const opfsBytes = expected.slice()
      await measure(
        "opfs-write",
        size,
        () =>
          requestOpfs({ operation: "write", name: fileName, bytes: opfsBytes }, [opfsBytes.buffer]).then(() => void 0)
      )
      await measure("opfs-read", size, async () => {
        const response = await requestOpfs<{ readonly bytes: Uint8Array }>({ operation: "read", name: fileName })
        verify(response.bytes, expected)
      })
    }
    await requestOpfs({ operation: "remove", name: fileName })
  }

  const afterStorage = (await navigator.storage.estimate()).usage ?? 0
  const summaries = summarize()
  window.__attachmentBenchmark = {
    status: "complete",
    userAgent: navigator.userAgent,
    samples,
    summaries,
    storageDelta: afterStorage - beforeStorage
  }
  status.textContent = "Complete"
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- The browser benchmark Promise host renders its measured result here.
  results.textContent = await Schema.encodePromise(Schema.fromJsonString(Schema.Unknown, { space: 2 }))(
    window.__attachmentBenchmark
  )
}

run().catch((error) => {
  window.__attachmentBenchmark = { status: "failed", userAgent: navigator.userAgent, error: String(error) }
  status.textContent = "Failed"
  results.textContent = String(error)
}).finally(async () => {
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Browser shutdown must await ManagedRuntime disposal from the Promise host.
  await runtime.dispose()
  opfsWorker.terminate()
})
