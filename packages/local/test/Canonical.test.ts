import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as vm from "node:vm"
import * as Canonical from "../src/Canonical.js"

describe("Canonical", () => {
  it("orders object keys independently of declaration order", () => {
    assert.strictEqual(Canonical.stringify({ b: 2, a: 1 }), Canonical.stringify({ a: 1, b: 2 }))
    assert.strictEqual(Canonical.hash({ b: 2, a: 1 }), Canonical.hash({ a: 1, b: 2 }))
  })

  it("preserves the canonical Uint8Array representation", () => {
    const value = { attachment: new Uint8Array([0, 1, 15, 16, 254, 255]) }
    assert.strictEqual(
      Canonical.stringify(value),
      "{\"attachment\":\"\\u001dbytes:00010f10feff\"}"
    )
    assert.strictEqual(Canonical.hash(value), "a2fe00cb44c0967c")
  })

  it.effect("computes a stable SHA-256 digest", () =>
    Effect.gen(function*() {
      const first = yield* Canonical.digest({ b: 2, a: 1 })
      const second = yield* Canonical.digest({ a: 1, b: 2 })
      assert.strictEqual(first, second)
      assert.strictEqual(first.length, 64)
    }).pipe(Effect.provide(NodeCrypto.layer)))
})

describe("Canonical injectivity", () => {
  it("distinguishes bigint from its decimal string", () => {
    assert.notStrictEqual(Canonical.stringify(10n), Canonical.stringify("10"))
    assert.notStrictEqual(Canonical.hash(10n), Canonical.hash("10"))
  })

  it("distinguishes Date from a plain object of its encoded shape", () => {
    const date = new Date("2020-01-02T03:04:05.006Z")
    assert.notStrictEqual(
      Canonical.hash(date),
      Canonical.hash({ _tag: "Date", value: date.toISOString() })
    )
  })

  it("distinguishes Uint8Array from a plain object of its encoded shape", () => {
    assert.notStrictEqual(
      Canonical.hash(new Uint8Array([0, 1, 15, 16, 254, 255])),
      Canonical.hash({ _tag: "Uint8Array", value: "00010f10feff" })
    )
  })

  it("does not let re-encoding a value's own output forge its identity", () => {
    const values = [10n, new Date("2020-01-02T03:04:05.006Z"), new Uint8Array([0, 1, 255])]
    for (const value of values) {
      const forged: unknown = JSON.parse(Canonical.stringify(value))
      assert.notStrictEqual(Canonical.hash(forged), Canonical.hash(value))
    }
  })

  it("normalizes a cross-realm Date identically to a native Date", () => {
    const foreign: unknown = vm.runInNewContext(`new Date("2020-01-02T03:04:05.006Z")`)
    const native = new Date("2020-01-02T03:04:05.006Z")
    assert.strictEqual(Canonical.hash(foreign), Canonical.hash(native))
    assert.notStrictEqual(Canonical.hash(foreign), Canonical.hash({}))
  })

  it("normalizes a cross-realm Uint8Array identically to a native Uint8Array", () => {
    const foreign: unknown = vm.runInNewContext("new Uint8Array([0, 1, 255])")
    assert.strictEqual(Canonical.hash(foreign), Canonical.hash(new Uint8Array([0, 1, 255])))
    assert.notStrictEqual(Canonical.hash(foreign), Canonical.hash({ 0: 0, 1: 1, 2: 255 }))
  })

  it("distinguishes non-Uint8Array views from plain objects and from each other", () => {
    const view = new Uint16Array([1, 2])
    assert.notStrictEqual(Canonical.hash(view), Canonical.hash({ 0: 1, 1: 2 }))
    assert.notStrictEqual(Canonical.hash(view), Canonical.hash(new Int8Array([1, 2])))
    assert.notStrictEqual(Canonical.hash(view), Canonical.hash(new Float64Array([1, 2])))
    assert.notStrictEqual(Canonical.hash(view), Canonical.hash(new Uint8Array([1, 0, 2, 0])))
  })

  it("distinguishes DataView contents from the empty object and other DataViews", () => {
    const first = new DataView(new ArrayBuffer(4))
    first.setUint8(0, 255)
    const second = new DataView(new ArrayBuffer(8))
    assert.notStrictEqual(Canonical.hash(first), Canonical.hash({}))
    assert.notStrictEqual(Canonical.hash(first), Canonical.hash(second))
  })

  it("distinguishes an absent property from an undefined property", () => {
    assert.notStrictEqual(Canonical.hash({}), Canonical.hash({ value: undefined }))
  })

  it("stringifies undefined to a string", () => {
    assert.strictEqual(typeof Canonical.stringify(undefined), "string")
  })

  it("distinguishes non-finite numbers from null and from each other", () => {
    assert.notStrictEqual(Canonical.hash(Number.NaN), Canonical.hash(null))
    assert.notStrictEqual(Canonical.hash(Number.POSITIVE_INFINITY), Canonical.hash(null))
    assert.notStrictEqual(Canonical.hash(Number.NaN), Canonical.hash(Number.POSITIVE_INFINITY))
    assert.notStrictEqual(
      Canonical.hash(Number.POSITIVE_INFINITY),
      Canonical.hash(Number.NEGATIVE_INFINITY)
    )
  })

  it("distinguishes a function from its source string", () => {
    const fn = (): number => 1
    assert.notStrictEqual(Canonical.hash(fn), Canonical.hash(String(fn)))
  })

  it("distinguishes a symbol from its description string", () => {
    const symbol = Symbol("marker")
    assert.notStrictEqual(Canonical.hash(symbol), Canonical.hash(String(symbol)))
  })

  it("distinguishes a circular reference from its collapsed marker string", () => {
    const circular: Record<string, unknown> = { name: "root" }
    circular.self = circular
    assert.notStrictEqual(
      Canonical.hash(circular),
      Canonical.hash({ name: "root", self: "[Circular]" })
    )
  })
})
