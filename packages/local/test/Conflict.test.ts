import * as Automerge from "@automerge/automerge"
import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import { createRequire } from "node:module"
import * as Conflict from "../src/Conflict.js"

const encodeValue = Schema.encodeSync(Conflict.Value)
const decodeValue = Schema.decodeUnknownSync(Conflict.Value)

describe("Conflict", () => {
  it("round trips every supported conflict value through JSON", () => {
    const values: ReadonlyArray<Automerge.AutomergeValue> = [
      null,
      true,
      1.5,
      "text",
      new Date("2026-07-29T00:00:00.000Z"),
      new Uint8Array([0, 1, 255]),
      new Automerge.Counter(Number.MAX_SAFE_INTEGER),
      new Automerge.ImmutableString("atomic"),
      { nested: [null, false, new Automerge.Counter(-1)] },
      ["one", { two: 2 }]
    ]

    for (const value of values) {
      const encoded = encodeValue(value)
      const decoded = decodeValue(JSON.parse(JSON.stringify(encoded)))
      assert.deepStrictEqual(encodeValue(decoded), encoded)
    }
  })

  it("round trips every resolution choice", () => {
    const alternativeId = Conflict.AlternativeId.make("1@actor")
    const heads = ["b", "a", "a"]
    const path = {
      parents: [{ _tag: "Key", key: "messages", alternative: alternativeId }],
      target: { _tag: "Index", index: 1 }
    } as const
    const choices: ReadonlyArray<Conflict.Choice> = [
      { _tag: "SelectAlternative", alternativeId },
      { _tag: "ReplaceValue", value: new Automerge.ImmutableString("chosen") },
      { _tag: "DeleteValue" }
    ]

    for (const choice of choices) {
      const resolution = Conflict.Resolution.make({ heads, path, choice })
      const encoded = Schema.encodeSync(Conflict.Resolution)(resolution)
      const decoded = Schema.decodeUnknownSync(Conflict.Resolution)(encoded)
      assert.deepStrictEqual(decoded.heads, ["a", "b"])
      assert.deepStrictEqual(Schema.encodeSync(Conflict.Resolution)(decoded), encoded)
    }
  })

  it("rejects malformed portable variants", () => {
    const malformed: ReadonlyArray<unknown> = [
      {},
      { _tag: "Unknown" },
      { _tag: "Date", value: "2026-07-29" },
      { _tag: "Bytes", value: "not base64" },
      { _tag: "Counter", value: Number.MAX_SAFE_INTEGER + 1 },
      { _tag: "Map", entries: [{ key: "one", value: { _tag: "Null" } }, { key: "one", value: { _tag: "Null" } }] },
      { _tag: "List", values: [{ _tag: "Unknown" }] }
    ]

    for (const value of malformed) assert.throws(() => decodeValue(value))
  })

  it("rejects unsupported native values during encoding", () => {
    const cycle: { self?: unknown } = {}
    cycle.self = cycle
    const sparse: Array<unknown> = []
    sparse.length = 1
    class Unsupported {
      readonly value = 1
    }
    const document = Automerge.from({ value: 1 })

    try {
      const unsupported: ReadonlyArray<unknown> = [
        undefined,
        () => undefined,
        1n,
        new Unsupported(),
        document,
        cycle,
        sparse,
        new Automerge.Counter(Number.MAX_SAFE_INTEGER + 1)
      ]
      for (const value of unsupported) assert.throws(() => encodeValue(value as Automerge.AutomergeValue))
    } finally {
      Automerge.free(document)
    }
  })

  it("accepts wrappers from another module instance and pins safe counter endpoints", () => {
    const OtherAutomerge = createRequire(import.meta.url)("@automerge/automerge") as typeof Automerge
    for (const value of [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER]) {
      const counter = new OtherAutomerge.Counter(value)
      assert.deepStrictEqual(encodeValue(counter), { _tag: "Counter", value })
    }
    assert.deepStrictEqual(
      encodeValue(new OtherAutomerge.ImmutableString("one")),
      { _tag: "ImmutableString", value: "one" }
    )
  })

  it("enforces iterative value depth, node, and UTF8 byte limits", () => {
    let deep: unknown = { _tag: "Null" }
    for (let depth = 0; depth <= 32; depth++) deep = { _tag: "List", values: [deep] }
    assert.strictEqual(
      Conflict.preflightPortableValue(deep, {
        maxConflictDepth: 32,
        maxConflictNodes: 100,
        maxConflictValueBytes: 100_000
      })?._tag,
      "Depth"
    )

    assert.strictEqual(
      Conflict.preflightPortableValue(
        { _tag: "List", values: [{ _tag: "Null" }, { _tag: "Null" }] },
        {
          maxConflictDepth: 2,
          maxConflictNodes: 2,
          maxConflictValueBytes: 1000
        }
      )?._tag,
      "Nodes"
    )

    const multibyte = { _tag: "Text", value: "é" }
    const exactBytes = new TextEncoder().encode(JSON.stringify(multibyte)).byteLength
    assert.isUndefined(Conflict.preflightPortableValue(multibyte, {
      maxConflictDepth: 1,
      maxConflictNodes: 1,
      maxConflictValueBytes: exactBytes
    }))
    assert.strictEqual(
      Conflict.preflightPortableValue(multibyte, {
        maxConflictDepth: 1,
        maxConflictNodes: 1,
        maxConflictValueBytes: exactBytes - 1
      })?._tag,
      "Bytes"
    )
  })

  it("normalizes canonical output", () => {
    assert.deepStrictEqual(Schema.decodeUnknownSync(Conflict.Heads)(["b", "a", "b"]), ["a", "b"])
    assert.deepStrictEqual(
      encodeValue({ z: 1, a: 2 }),
      {
        _tag: "Map",
        entries: [
          { key: "a", value: { _tag: "Number", value: 2 } },
          { key: "z", value: { _tag: "Number", value: 1 } }
        ]
      }
    )

    const one = Conflict.AlternativeId.make("one")
    const two = Conflict.AlternativeId.make("two")
    assert.deepStrictEqual(
      Conflict.normalizeAlternatives([
        { id: two, value: 2 },
        { id: one, value: 1 }
      ]).map((alternative) => alternative.id),
      [one, two]
    )
  })

  it("rejects prototype sensitive keys without mutating a prototype", () => {
    const keys = [...Object.getOwnPropertyNames(Object.prototype), "prototype"]
    const before = Object.getPrototypeOf({})
    for (const key of keys) {
      assert.throws(() =>
        Schema.decodeUnknownSync(Conflict.Path)({
          parents: [],
          target: { _tag: "Key", key }
        })
      )
      assert.throws(() =>
        decodeValue({
          _tag: "Map",
          entries: [{ key, value: { _tag: "Null" } }]
        })
      )
    }
    assert.strictEqual(Object.getPrototypeOf({}), before)
  })

  it("normalizes heads and preserves opaque path segments", () => {
    const alternative = Conflict.AlternativeId.make("op:1.with.dots")
    const resolution = Schema.decodeUnknownSync(Conflict.Resolution)({
      heads: ["z", "a", "z"],
      path: {
        parents: [
          { _tag: "Key", key: "1.2:3", alternative },
          { _tag: "Index", index: 0 }
        ],
        target: { _tag: "Key", key: "final:key" }
      },
      choice: { _tag: "DeleteValue" }
    })
    assert.deepStrictEqual(resolution.heads, ["a", "z"])
    assert.strictEqual(resolution.path.parents[0]?.alternative, alternative)
    assert.isTrue(Conflict.sameHeads(["b", "a"], ["a", "b"]))
  })
})
