import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { AddLabel, ListLabels, Task } from "./domain"
import { ReplicaLive } from "./replica"

export interface SelfTestResult {
  readonly name: string
  readonly ok: boolean
  readonly detail?: string
}

const pass = (name: string, detail?: string): SelfTestResult => ({ name, ok: true, detail })
const fail = (name: string, detail: string): SelfTestResult => ({ name, ok: false, detail })

const check = (name: string, run: () => boolean | string): SelfTestResult => {
  try {
    const outcome = run()
    return outcome === true ? pass(name) : fail(name, String(outcome))
  } catch (error) {
    return fail(name, error instanceof Error ? error.message : String(error))
  }
}

const replicaBattery: Effect.Effect<ReadonlyArray<SelfTestResult>, never, Crypto.Crypto | Replica.Replica> = Effect.gen(
  function*() {
    const replica = yield* Replica.Replica

    const documentId = yield* replica.create(Task, {
      commandId: yield* Identity.makeCommandId,
      value: { title: "self test", labels: [] }
    })

    yield* replica.mutate(AddLabel, {
      commandId: yield* Identity.makeCommandId,
      documentId,
      payload: "verified"
    })

    const snapshot = yield* replica.get(Task, documentId)
    const rows = yield* replica.query(ListLabels, { prefix: "" })
    const chunks = yield* replica.exportBackup({ maxBytes: 16 * 1024 * 1024 }).pipe(Stream.runCollect)

    return [
      pass("replica create", documentId),
      pass("replica mutate"),
      snapshot.value.labels.length === 1 && snapshot.value.labels[0] === "verified"
        ? pass("replica read-after-write")
        : fail("replica read-after-write", JSON.stringify(snapshot.value)),
      rows.length === 1 && rows[0].label === "verified"
        ? pass("projection query")
        : fail("projection query", JSON.stringify(rows)),
      chunks.length > 0 ? pass("backup export", `${chunks.length} chunk(s)`) : fail("backup export", "no chunks")
    ]
  }
).pipe(
  Effect.catchCause((cause) => Effect.succeed([fail("replica battery", String(cause))]))
)

/**
 * Boot-time battery proving the runtime capabilities Effect Local depends on,
 * then a full replica round trip on the real expo-sqlite database.
 */
export const runSelfTest = async (): Promise<ReadonlyArray<SelfTestResult>> => {
  const results: Array<SelfTestResult> = []

  results.push(check("WebAssembly global", () => typeof WebAssembly !== "undefined" || "WebAssembly is undefined"))
  results.push(check("crypto.getRandomValues", () => {
    const bytes = new Uint8Array(16)
    globalThis.crypto.getRandomValues(bytes)
    return bytes.some((byte) => byte !== 0) || "getRandomValues returned zeros"
  }))
  results.push(check(
    "atob/btoa",
    () => (globalThis.atob("aGVsbG8=") === "hello" && globalThis.btoa("hello") === "aGVsbG8=") || "base64 mismatch"
  ))
  results.push(check("TextEncoder/TextDecoder", () => {
    const encoded = new TextEncoder().encode("✓")
    return (encoded.length === 3 && new TextDecoder().decode(encoded) === "✓") || "UTF-8 roundtrip failed"
  }))

  try {
    const A = await import("@automerge/automerge")
    const doc = A.change(A.init<{ title: string }>(), (draft) => {
      draft.title = "automerge ok"
    })
    results.push(
      doc.title === "automerge ok" ? pass("automerge WASM init") : fail("automerge WASM init", "unexpected value")
    )
  } catch (error) {
    results.push(fail("automerge WASM init", error instanceof Error ? error.message : String(error)))
  }

  const replicaResults = await Effect.runPromise(Effect.provide(replicaBattery, ReplicaLive))
  return [...results, ...replicaResults]
}
