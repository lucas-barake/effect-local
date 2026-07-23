import { assert, describe, it } from "@effect/vitest"
import * as Canonical from "../src/Canonical.js"

describe("Canonical stability", () => {
  it("orders object keys recursively, not just at the top level", () => {
    const a = { outer: { b: 2, a: 1 }, items: [{ y: 1, x: 2 }] }
    const b = { items: [{ x: 2, y: 1 }], outer: { a: 1, b: 2 } }
    assert.strictEqual(Canonical.stringify(a), Canonical.stringify(b))
    assert.strictEqual(Canonical.hash(a), Canonical.hash(b))
  })

  it("keeps array element order significant", () => {
    assert.notStrictEqual(Canonical.stringify([1, 2, 3]), Canonical.stringify([3, 2, 1]))
    assert.notStrictEqual(Canonical.hash([1, 2, 3]), Canonical.hash([3, 2, 1]))
  })

  it("encodes Date as a sentinel-prefixed ISO string", () => {
    assert.strictEqual(
      Canonical.stringify({ at: new Date("2020-01-02T03:04:05.006Z") }),
      "{\"at\":\"\\u001ddate:2020-01-02T03:04:05.006Z\"}"
    )
  })

  it("encodes bigint as a sentinel-prefixed decimal string", () => {
    assert.strictEqual(Canonical.stringify({ n: 10n }), "{\"n\":\"\\u001dbigint:10\"}")
  })

  it("escapes plain strings that start with the sentinel", () => {
    assert.strictEqual(Canonical.stringify("\u001dbigint:10"), "\"\\u001d\\u001dbigint:10\"")
    assert.notStrictEqual(Canonical.hash("\u001dbigint:10"), Canonical.hash(10n))
    assert.notStrictEqual(Canonical.hash("\u001d\u001dbigint:10"), Canonical.hash("\u001dbigint:10"))
  })

  it("treats negative zero as canonical zero", () => {
    assert.strictEqual(Canonical.stringify(-0), "0")
    assert.strictEqual(Canonical.hash(-0), Canonical.hash(0))
  })

  it("collapses circular references instead of throwing", () => {
    const value: Record<string, unknown> = { name: "root" }
    value.self = value
    assert.strictEqual(Canonical.stringify(value), "{\"name\":\"root\",\"self\":\"\\u001dcircular\"}")
  })
})
