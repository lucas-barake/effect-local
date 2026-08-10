import * as Automerge from "@automerge/automerge"
import * as OtherAutomerge from "@automerge/automerge/slim"
import { assert, describe, it } from "@effect/vitest"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Conflict from "../src/Conflict.js"
import * as Identity from "../src/Identity.js"

const encodeValue = Schema.encodeSync(Conflict.Value)
const decodeValue = Schema.decodeUnknownSync(Conflict.Value)
const maxHeadsHardLimit = 1_024
const maxHeadBytesHardLimit = 64 * 1_024
const maxHeadsBytesHardLimit = 64 * 1_024
const maxConflictDepthHardLimit = 128
const maxConflictNodesHardLimit = 100_000
const maxConflictAlternativesHardLimit = 10_000
const maxConflictPathSegmentsHardLimit = 128
const maxConflictValueBytesHardLimit = 16 * 1_024 * 1_024
const JsonString = Schema.fromJsonString(Schema.Unknown)
const roundTripJson = (value: unknown): unknown =>
  Schema.decodeUnknownSync(JsonString)(Schema.encodeSync(JsonString)(value))
const encodeUnknownValue = (value: unknown) => Schema.encodeUnknownSync(Conflict.Value)(value)
const makeError = (message: string): Error => {
  // oxlint-disable-next-line effect/noNewError -- this fixture must preserve an arbitrary Error value
  return new Error(message)
}
const throwError = (message: string): never => {
  // oxlint-disable-next-line effect/noThrowStatement, effect/noNewError -- hostile accessors must throw their direct host Error
  throw new Error(message)
}
const throwRangeError = (message: string): never => {
  // oxlint-disable-next-line effect/noThrowStatement, effect/noNewError -- hostile accessors must throw their direct host RangeError
  throw new RangeError(message)
}

const assertSchemaError = (evaluate: () => unknown, message?: string): void => {
  const failure = Effect.runSync(Effect.flip(Effect.try({ try: evaluate, catch: (cause) => cause })))
  assert.isTrue(Schema.isSchemaError(failure), message)
}

const makePath = (key: string) => ({
  parents: [],
  target: { _tag: "Key", key }
})

const makeAlternative = (id: string, value: unknown = { _tag: "Null" }) => ({
  id,
  value
})

const record = (
  key: string,
  alternatives: ReadonlyArray<{ readonly id: string; readonly value: unknown }>
) => ({
  path: makePath(key),
  visible: alternatives[0].id,
  alternatives
})

describe("Conflict", () => {
  it("round trips every supported conflict value through JSON", () => {
    const values: ReadonlyArray<Automerge.AutomergeValue> = [
      null,
      true,
      1.5,
      "text",
      DateTime.toDate(DateTime.makeUnsafe("2026-07-29T00:00:00.000Z")),
      new Uint8Array([0, 1, 255]),
      new Automerge.Counter(Number.MAX_SAFE_INTEGER - 1),
      new Automerge.ImmutableString("atomic"),
      { nested: [null, false, new Automerge.Counter(-1)] },
      ["one", { two: 2 }]
    ]

    for (const value of values) {
      const encoded = encodeValue(value)
      const decoded = decodeValue(roundTripJson(encoded))
      assert.deepStrictEqual(encodeValue(decoded), encoded)
    }
  })

  it("round trips every resolution choice", () => {
    const alternativeId = Conflict.AlternativeId.make("1@actor")
    const heads = ["b", "a", "a"]
    const resolutionPath = {
      parents: [{ _tag: "Key", key: "messages", alternative: alternativeId }],
      target: { _tag: "Index", index: 1 }
    } satisfies Conflict.Path
    const choices: ReadonlyArray<Conflict.Choice> = [
      { _tag: "SelectAlternative", alternativeId },
      { _tag: "ReplaceValue", value: new Automerge.ImmutableString("chosen") },
      { _tag: "DeleteValue" }
    ]

    for (const choice of choices) {
      const resolution = Conflict.Resolution.make({ heads, path: resolutionPath, choice })
      const encoded = Schema.encodeSync(Conflict.Resolution)(resolution)
      const decoded = Schema.decodeUnknownSync(Conflict.Resolution)(encoded)
      assert.deepStrictEqual(decoded.heads, ["a", "b"])
      assert.deepStrictEqual(Schema.encodeSync(Conflict.Resolution)(decoded), encoded)
    }
  })

  it("round trips composite replacement values through the JSON resolution codec", () => {
    const resolution = Conflict.Resolution.make({
      heads: ["one"],
      path: makePath("value"),
      choice: { _tag: "ReplaceValue", value: ["replacement"] }
    })
    const codec = Schema.toCodecJson(Conflict.Resolution)
    const encoded = Schema.encodeSync(codec)(resolution)
    const decoded = Schema.decodeUnknownSync(codec)(roundTripJson(encoded))
    assert.deepStrictEqual(decoded, resolution)
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

    for (const value of malformed) assertSchemaError(() => decodeValue(value))
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

    Effect.runSync(Effect.acquireUseRelease(
      Effect.succeed(document),
      (currentDocument) =>
        Effect.sync(() => {
          const unsupported: ReadonlyArray<unknown> = [
            undefined,
            () => undefined,
            1n,
            new Unsupported(),
            currentDocument,
            cycle,
            sparse,
            new Automerge.Counter(Number.MAX_SAFE_INTEGER + 1)
          ]
          for (const value of unsupported) {
            assertSchemaError(() => encodeUnknownValue(value))
          }
        }),
      (currentDocument) => Effect.sync(() => Automerge.free(currentDocument))
    ))
  })

  it("accepts wrappers from another module instance and pins lossless counter endpoints", () => {
    for (const value of [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER - 1]) {
      const counter = new OtherAutomerge.Counter(value)
      assert.deepStrictEqual(encodeValue(counter), { _tag: "Counter", value })
    }
    assertSchemaError(() => encodeValue(new OtherAutomerge.Counter(Number.MAX_SAFE_INTEGER)))
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
    const exactBytes = new TextEncoder().encode(Schema.encodeSync(JsonString)(multibyte)).byteLength
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

  it("preflights portable values before recursive schema decoding", () => {
    let calls = 0
    const terminal = {}
    Object.defineProperty(terminal, "_tag", {
      enumerable: true,
      get: () => {
        calls++
        return throwRangeError("recursive schema reached past the hard depth limit")
      }
    })
    let deep: unknown = terminal
    for (let depth = 0; depth <= maxConflictDepthHardLimit; depth++) {
      deep = { _tag: "List", values: [deep] }
    }

    let failure: unknown
    failure = Effect.runSync(Effect.flip(Effect.try({
      try: () => decodeValue(deep),
      catch: (cause) => cause
    })))
    assert.isDefined(failure)
    assert.notInstanceOf(failure, RangeError)
    assert.strictEqual(calls, 0)
  })

  it("rejects accessors without executing them during preflight", () => {
    const cases: ReadonlyArray<{
      readonly name: string
      readonly input: () => unknown
      readonly preflight: (input: unknown) => Conflict.PreflightIssue | undefined
    }> = [
      {
        name: "native array",
        input: () => {
          const value = [null]
          Object.defineProperty(value, 0, {
            enumerable: true,
            get: () => {
              return throwError("native array getter executed")
            }
          })
          return value
        },
        preflight: Conflict.preflightNativeValue
      },
      {
        name: "portable tag",
        input: () => {
          const value = {}
          Object.defineProperty(value, "_tag", {
            enumerable: true,
            get: () => {
              return throwError("portable tag getter executed")
            }
          })
          return value
        },
        preflight: Conflict.preflightPortableValue
      },
      {
        name: "portable list",
        input: () => {
          const values = [{ _tag: "Null" }]
          Object.defineProperty(values, 0, {
            enumerable: true,
            get: () => {
              return throwError("portable list getter executed")
            }
          })
          return { _tag: "List", values }
        },
        preflight: Conflict.preflightPortableValue
      },
      {
        name: "portable map entry",
        input: () => {
          const entry = { key: "one" }
          Object.defineProperty(entry, "value", {
            enumerable: true,
            get: () => {
              return throwError("portable map entry getter executed")
            }
          })
          return { _tag: "Map", entries: [entry] }
        },
        preflight: Conflict.preflightPortableValue
      },
      {
        name: "unknown array",
        input: () => {
          const value = [null]
          Object.defineProperty(value, 0, {
            enumerable: true,
            get: () => {
              return throwError("unknown array getter executed")
            }
          })
          return value
        },
        preflight: Conflict.preflightUnknown
      }
    ]

    for (const test of cases) {
      assert.doesNotThrow(() => {
        assert.strictEqual(test.preflight(test.input())?._tag, "UnsupportedValue", test.name)
      })
    }
  })

  it("enforces exact native, portable, and unknown preflight boundaries", () => {
    const cases = [
      {
        name: "native",
        value: [null],
        preflight: Conflict.preflightNativeValue
      },
      {
        name: "portable",
        value: { _tag: "List", values: [{ _tag: "Null" }] },
        preflight: Conflict.preflightPortableValue
      },
      {
        name: "unknown",
        value: [null],
        preflight: Conflict.preflightUnknown
      }
    ] satisfies ReadonlyArray<{
      readonly name: string
      readonly value: unknown
      readonly preflight: (value: unknown, limits: Conflict.PreflightLimits) => Conflict.PreflightIssue | undefined
    }>

    for (const test of cases) {
      assert.isUndefined(
        test.preflight(test.value, {
          ...Conflict.hardPreflightLimits,
          maxConflictDepth: 1,
          maxConflictNodes: 2
        }),
        test.name
      )
      assert.strictEqual(
        test.preflight(test.value, {
          ...Conflict.hardPreflightLimits,
          maxConflictDepth: 0,
          maxConflictNodes: 2
        })?._tag,
        "Depth",
        test.name
      )
      assert.strictEqual(
        test.preflight(test.value, {
          ...Conflict.hardPreflightLimits,
          maxConflictDepth: 1,
          maxConflictNodes: 1
        })?._tag,
        "Nodes",
        test.name
      )
    }
  })

  it("rejects wide containers before reading descriptors beyond the remaining node budget", () => {
    const cases: ReadonlyArray<{
      readonly name: string
      readonly make: (descriptorRead: () => void) => unknown
      readonly preflight: (input: unknown, limits: Conflict.PreflightLimits) => Conflict.PreflightIssue | undefined
    }> = [
      {
        name: "native array",
        make: (descriptorRead) =>
          new Proxy(Array.from({ length: 32 }, () => null), {
            getOwnPropertyDescriptor(target, property) {
              descriptorRead()
              return Reflect.getOwnPropertyDescriptor(target, property)
            }
          }),
        preflight: Conflict.preflightNativeValue
      },
      {
        name: "native object",
        make: (descriptorRead) =>
          new Proxy(
            Object.fromEntries(Array.from({ length: 32 }, (_, index) => [`key${index}`, null])),
            {
              getOwnPropertyDescriptor(target, property) {
                descriptorRead()
                return Reflect.getOwnPropertyDescriptor(target, property)
              }
            }
          ),
        preflight: Conflict.preflightNativeValue
      },
      {
        name: "portable list",
        make: (descriptorRead) => ({
          _tag: "List",
          values: new Proxy(Array.from({ length: 32 }, () => ({ _tag: "Null" })), {
            getOwnPropertyDescriptor(target, property) {
              descriptorRead()
              return Reflect.getOwnPropertyDescriptor(target, property)
            }
          })
        }),
        preflight: Conflict.preflightPortableValue
      },
      {
        name: "portable map",
        make: (descriptorRead) => ({
          _tag: "Map",
          entries: new Proxy(
            Array.from({ length: 32 }, (_, index) => ({
              key: `key${index}`,
              value: { _tag: "Null" }
            })),
            {
              getOwnPropertyDescriptor(target, property) {
                descriptorRead()
                return Reflect.getOwnPropertyDescriptor(target, property)
              }
            }
          )
        }),
        preflight: Conflict.preflightPortableValue
      },
      {
        name: "unknown array",
        make: (descriptorRead) =>
          new Proxy(Array.from({ length: 32 }, () => null), {
            getOwnPropertyDescriptor(target, property) {
              descriptorRead()
              return Reflect.getOwnPropertyDescriptor(target, property)
            }
          }),
        preflight: Conflict.preflightUnknown
      },
      {
        name: "unknown object",
        make: (descriptorRead) =>
          new Proxy(
            Object.fromEntries(Array.from({ length: 32 }, (_, index) => [`key${index}`, null])),
            {
              getOwnPropertyDescriptor(target, property) {
                descriptorRead()
                return Reflect.getOwnPropertyDescriptor(target, property)
              }
            }
          ),
        preflight: Conflict.preflightUnknown
      }
    ]

    for (const test of cases) {
      let descriptorReads = 0
      const issue = test.preflight(test.make(() => descriptorReads++), {
        ...Conflict.hardPreflightLimits,
        maxConflictNodes: 2
      })
      assert.strictEqual(issue?._tag, "Nodes", test.name)
      assert.isAtMost(descriptorReads, 1, test.name)
    }
  })

  it("counts exact encoded JSON bytes without whole value serialization", () => {
    const nativeValues: ReadonlyArray<Automerge.AutomergeValue> = [
      null,
      true,
      -0,
      "é\"\n\uD800",
      DateTime.toDate(DateTime.makeUnsafe("2026-07-29T00:00:00.000Z")),
      new Uint8Array([0, 1, 255]),
      new Automerge.Counter(Number.MIN_SAFE_INTEGER),
      new Automerge.ImmutableString("é"),
      { "é": ["text", false] }
    ]
    const cases = [
      ...nativeValues.map((value, index) => ({
        name: `native ${index}`,
        value,
        encoded: encodeValue(value),
        preflight: Conflict.preflightNativeValue
      })),
      ...nativeValues.map((value, index) => {
        const encoded = encodeValue(value)
        return {
          name: `portable ${index}`,
          value: encoded,
          encoded,
          preflight: Conflict.preflightPortableValue
        }
      }),
      ...[
        null,
        true,
        -0,
        "é\"\n\uD800",
        ["text", false],
        { "é": ["text", false] }
      ].map((value, index) => ({
        name: `unknown ${index}`,
        value,
        encoded: value,
        preflight: Conflict.preflightUnknown
      }))
    ]

    for (const test of cases) {
      const exactBytes = new TextEncoder().encode(Schema.encodeSync(JsonString)(test.encoded)).byteLength
      assert.isUndefined(
        test.preflight(test.value, {
          ...Conflict.hardPreflightLimits,
          maxConflictValueBytes: exactBytes
        }),
        test.name
      )
      assert.strictEqual(
        test.preflight(test.value, {
          ...Conflict.hardPreflightLimits,
          maxConflictValueBytes: exactBytes - 1
        })?._tag,
        "Bytes",
        test.name
      )
    }
  })

  it("exposes a reusable exact JSON UTF8 byte budget", () => {
    const value = "\u0000\b\t\n\f\r\\\"é\uD800"
    const exactBytes = new TextEncoder().encode(Schema.encodeSync(JsonString)(value)).byteLength
    const budget = Conflict.createJsonByteBudget(exactBytes)

    assert.isFalse(Conflict.addJsonStringBytes(budget, value))
    assert.strictEqual(Conflict.jsonByteBudgetRemaining(budget), 0)
    assert.isFalse(Conflict.jsonByteBudgetExceeded(budget))
    assert.isTrue(Conflict.addJsonBytes(budget, 1))
    assert.strictEqual(Conflict.jsonByteBudgetRemaining(budget), 0)
    assert.isTrue(Conflict.jsonByteBudgetExceeded(budget))
  })

  it("stops traversing native, portable, and unknown values once their byte limit is exceeded", () => {
    const nativeCycle: { readonly large: string; cycle?: unknown } = {
      large: "x".repeat(100)
    }
    nativeCycle.cycle = nativeCycle
    const portableCycle: {
      readonly _tag: "Map"
      readonly entries: Array<{ readonly key: string; readonly value: unknown }>
    } = {
      _tag: "Map",
      entries: [
        { key: "large", value: { _tag: "Text", value: "x".repeat(100) } }
      ]
    }
    portableCycle.entries.push({ key: "cycle", value: portableCycle })
    const unknownCycle: Array<unknown> = ["x".repeat(100)]
    unknownCycle.push(unknownCycle)

    const cases = [
      ["native", nativeCycle, Conflict.preflightNativeValue],
      ["portable", portableCycle, Conflict.preflightPortableValue],
      ["unknown", unknownCycle, Conflict.preflightUnknown]
    ] satisfies ReadonlyArray<
      readonly [
        string,
        unknown,
        (value: unknown, limits: Conflict.PreflightLimits) => Conflict.PreflightIssue | undefined
      ]
    >
    for (const [name, value, preflight] of cases) {
      assert.strictEqual(
        preflight(value, {
          ...Conflict.hardPreflightLimits,
          maxConflictValueBytes: 1
        })?._tag,
        "Bytes",
        name
      )
      assert.strictEqual(
        preflight(value, Conflict.hardPreflightLimits)?._tag,
        "Cycle",
        name
      )
    }
  })

  it("recognizes only actual conflict metadata during unknown preflight", () => {
    const ordinary = [
      { alternatives: [null, null, null] },
      { parents: [null, null, null], target: null }
    ]
    const limits: Conflict.PreflightLimits = {
      ...Conflict.hardPreflightLimits,
      maxConflictAlternatives: 2,
      maxConflictPathSegments: 2
    }
    for (const value of ordinary) assert.isUndefined(Conflict.preflightUnknown(value, limits))

    const twoAlternatives = [
      makeAlternative("one"),
      makeAlternative("two")
    ]
    assert.isUndefined(Conflict.preflightUnknown(
      { conflicts: [record("one", twoAlternatives)] },
      limits
    ))
    assert.strictEqual(
      Conflict.preflightUnknown(
        { conflicts: [record("one", twoAlternatives), record("two", twoAlternatives)] },
        limits
      )?._tag,
      "Alternatives"
    )
    assert.strictEqual(
      Conflict.preflightUnknown({
        conflicts: [{
          ...record("one", twoAlternatives),
          path: {
            parents: [
              { _tag: "Key", key: "one" },
              { _tag: "Key", key: "two" }
            ],
            target: { _tag: "Key", key: "three" }
          }
        }]
      }, limits)?._tag,
      "PathSegments"
    )
  })

  it("rejects invalid public preflight limits before traversal", () => {
    const fields = [
      ["maxConflictDepth", "Depth", maxConflictDepthHardLimit],
      ["maxConflictNodes", "Nodes", maxConflictNodesHardLimit],
      ["maxConflictAlternatives", "Alternatives", maxConflictAlternativesHardLimit],
      ["maxConflictPathSegments", "PathSegments", maxConflictPathSegmentsHardLimit],
      ["maxConflictValueBytes", "Bytes", maxConflictValueBytesHardLimit]
    ] satisfies ReadonlyArray<readonly [keyof Conflict.PreflightLimits, string, number]>
    const invalid = [-1, 0, 1.5, Number.NaN, Number.POSITIVE_INFINITY]

    for (const [field, issue, hardLimit] of fields) {
      for (const value of [...invalid, hardLimit + 1]) {
        assert.strictEqual(
          Conflict.preflightUnknown(null, {
            ...Conflict.hardPreflightLimits,
            [field]: value
          })?._tag,
          issue,
          `${field} accepted ${value}`
        )
      }
    }

    for (const preflight of [Conflict.preflightNativeValue, Conflict.preflightPortableValue]) {
      for (const [field, issue, hardLimit] of fields.slice(0, 2).concat([fields[4]])) {
        assert.strictEqual(
          preflight(null, {
            ...Conflict.hardPreflightLimits,
            [field]: hardLimit + 1
          })?._tag,
          issue
        )
      }
    }
  })

  it("bounds and normalizes heads before durable use", () => {
    const decodeHeads = Schema.decodeUnknownSync(Conflict.Heads)
    assert.deepStrictEqual(decodeHeads(["b", "a", "a"]), ["a", "b"])
    assert.doesNotThrow(() => decodeHeads(Array.from({ length: maxHeadsHardLimit }, (_, index) => String(index))))
    assertSchemaError(() => decodeHeads(Array.from({ length: maxHeadsHardLimit + 1 }, (_, index) => String(index))))
    assert.doesNotThrow(() => decodeHeads(["x".repeat(maxHeadBytesHardLimit)]))
    assertSchemaError(() => decodeHeads(["x".repeat(maxHeadBytesHardLimit + 1)]))
    assert.doesNotThrow(() =>
      decodeHeads([
        "x".repeat(maxHeadsBytesHardLimit / 2),
        "y".repeat(maxHeadsBytesHardLimit / 2)
      ])
    )
    assertSchemaError(() =>
      decodeHeads([
        "x".repeat(maxHeadsBytesHardLimit / 2),
        "y".repeat(maxHeadsBytesHardLimit / 2 + 1)
      ])
    )
    assert.isTrue(Conflict.sameHeads(["b", "a", "a"], ["a", "b"]))
    assert.isFalse(Conflict.sameHeads(["a"], ["b"]))
  })

  it("normalizes records and enforces conflict inspection invariants", () => {
    const one = makeAlternative("one")
    const two = makeAlternative("two")
    const three = makeAlternative("three")

    assertSchemaError(() => Schema.decodeUnknownSync(Conflict.Record)(record("one", [one])))
    assertSchemaError(() =>
      Schema.decodeUnknownSync(Conflict.Record)({
        ...record("one", [one, two]),
        alternatives: [one, one]
      })
    )
    assertSchemaError(() =>
      Schema.decodeUnknownSync(Conflict.Record)({
        ...record("one", [one, two]),
        visible: "missing"
      })
    )

    const decodedRecord = Schema.decodeUnknownSync(Conflict.Record)(
      record("one", [three, one, two])
    )
    assert.deepStrictEqual(decodedRecord.alternatives.map(({ id }) => id), ["one", "three", "two"])

    const Inspection = Conflict.inspection(Schema.Struct({ title: Schema.String }))
    const snapshot = {
      documentId: "doc_00000000-0000-4000-8000-000000000001",
      value: { title: "one" },
      version: 1,
      heads: ["b", "a", "a"],
      tombstone: false,
      projection: "Ready"
    }
    assertSchemaError(() =>
      Schema.decodeUnknownSync(Inspection)({
        snapshot,
        conflicts: [record("one", [one, two]), record("one", [one, two])]
      })
    )

    const decoded = Schema.decodeUnknownSync(Inspection)({
      snapshot,
      conflicts: [record("z", [two, one]), record("a", [two, one])]
    })
    assert.deepStrictEqual(decoded.snapshot.heads, ["a", "b"])
    assert.deepStrictEqual(
      decoded.conflicts.map((conflict) => conflict.path.target._tag === "Key" && conflict.path.target.key),
      ["a", "z"]
    )
    for (const conflict of decoded.conflicts) {
      assert.deepStrictEqual(conflict.alternatives.map(({ id }) => id), ["one", "two"])
    }

    const half = maxConflictAlternativesHardLimit / 2 + 1
    const many = (prefix: string) => Array.from({ length: half }, (_, index) => makeAlternative(`${prefix}${index}`))
    assertSchemaError(() =>
      Schema.decodeUnknownSync(Inspection)({
        snapshot,
        conflicts: [record("one", many("a")), record("two", many("b"))]
      })
    )
  })

  it("preflights semantic resolutions and inspections with native values", () => {
    const native = {
      date: DateTime.toDate(DateTime.makeUnsafe("2026-07-29T00:00:00.000Z")),
      counter: new Automerge.Counter(1),
      immutable: new Automerge.ImmutableString("one"),
      bytes: new Uint8Array([0, 1, 2])
    }
    const resolution = Conflict.Resolution.make({
      heads: ["one"],
      path: makePath("value"),
      choice: { _tag: "ReplaceValue", value: native }
    })
    assert.isUndefined(Conflict.preflightResolution(resolution))
    assert.strictEqual(
      Conflict.preflightResolution(resolution, {
        ...Conflict.hardPreflightLimits,
        maxConflictValueBytes: 1
      })?._tag,
      "Bytes"
    )

    const inspected: Conflict.Inspection<typeof native> = {
      snapshot: {
        documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
        value: native,
        version: 1,
        heads: ["one"],
        tombstone: false,
        projection: "Ready"
      },
      conflicts: [{
        path: makePath("value"),
        visible: Conflict.AlternativeId.make("one"),
        alternatives: [
          {
            id: Conflict.AlternativeId.make("one"),
            value: native
          },
          {
            id: Conflict.AlternativeId.make("two"),
            value: new Uint8Array([3, 4])
          }
        ]
      }]
    }
    assert.isUndefined(Conflict.preflightInspection(inspected))
    assert.strictEqual(
      Conflict.preflightInspection(inspected, {
        ...Conflict.hardPreflightLimits,
        maxConflictAlternatives: 1
      })?._tag,
      "Alternatives"
    )
  })

  it("round trips every public conflict error codec with exact encoded fields", () => {
    const errorPath = makePath("title")
    const alternativeId = Conflict.AlternativeId.make("one")
    const cases: ReadonlyArray<readonly [Conflict.ResolutionError, unknown]> = [
      [
        new Conflict.UnsupportedConflictValue({ pathDepth: 1, kind: "function" }),
        { _tag: "UnsupportedConflictValue", pathDepth: 1, kind: "function" }
      ],
      [
        new Conflict.UnsupportedConflictKey({ pathDepth: 2 }),
        { _tag: "UnsupportedConflictKey", pathDepth: 2 }
      ],
      [
        new Conflict.StaleConflictResolution({
          expectedHeads: ["b", "a", "a"],
          observedHeads: ["d", "c", "c"]
        }),
        {
          _tag: "StaleConflictResolution",
          expectedHeads: ["a", "b"],
          observedHeads: ["c", "d"]
        }
      ],
      [
        new Conflict.ConflictPathNotFound({ path: errorPath, segmentIndex: 1 }),
        { _tag: "ConflictPathNotFound", path: errorPath, segmentIndex: 1 }
      ],
      [
        new Conflict.ConflictPathTypeMismatch({
          path: errorPath,
          segmentIndex: 1,
          expected: "Map",
          observed: "Scalar"
        }),
        {
          _tag: "ConflictPathTypeMismatch",
          path: errorPath,
          segmentIndex: 1,
          expected: "Map",
          observed: "Scalar"
        }
      ],
      [
        new Conflict.ConflictNotFound({ path: errorPath }),
        { _tag: "ConflictNotFound", path: errorPath }
      ],
      [
        new Conflict.ConflictAlternativeNotFound({ path: errorPath, alternativeId }),
        { _tag: "ConflictAlternativeNotFound", path: errorPath, alternativeId: "one" }
      ],
      [
        new Conflict.CompositeAlternativeRequiresReplacement({
          path: errorPath,
          alternativeId,
          composite: "List"
        }),
        {
          _tag: "CompositeAlternativeRequiresReplacement",
          path: errorPath,
          alternativeId: "one",
          composite: "List"
        }
      ],
      [
        new Conflict.ConflictResolutionSchemaError({
          path: errorPath,
          cause: makeError("invalid replacement")
        }),
        {
          _tag: "ConflictResolutionSchemaError",
          path: errorPath,
          cause: { name: "Error", message: "invalid replacement" }
        }
      ]
    ]
    const resolutionCodec = Schema.toCodecJson(Conflict.ResolutionError)
    for (const [error, expected] of cases) {
      const encoded = Schema.encodeSync(resolutionCodec)(error)
      assert.deepStrictEqual(encoded, expected)
      const decoded = Schema.decodeUnknownSync(resolutionCodec)(expected)
      assert.deepStrictEqual(Schema.encodeSync(resolutionCodec)(decoded), expected)
      switch (decoded._tag) {
        case "UnsupportedConflictValue":
          assert.strictEqual(decoded.pathDepth, 1)
          assert.strictEqual(decoded.kind, "function")
          break
        case "UnsupportedConflictKey":
          assert.strictEqual(decoded.pathDepth, 2)
          break
        case "StaleConflictResolution":
          assert.deepStrictEqual(decoded.expectedHeads, ["a", "b"])
          assert.deepStrictEqual(decoded.observedHeads, ["c", "d"])
          break
        case "ConflictPathNotFound":
          assert.deepStrictEqual(decoded.path, errorPath)
          assert.strictEqual(decoded.segmentIndex, 1)
          break
        case "ConflictPathTypeMismatch":
          assert.deepStrictEqual(decoded.path, errorPath)
          assert.strictEqual(decoded.segmentIndex, 1)
          assert.strictEqual(decoded.expected, "Map")
          assert.strictEqual(decoded.observed, "Scalar")
          break
        case "ConflictNotFound":
          assert.deepStrictEqual(decoded.path, errorPath)
          break
        case "ConflictAlternativeNotFound":
          assert.deepStrictEqual(decoded.path, errorPath)
          assert.strictEqual(decoded.alternativeId, alternativeId)
          break
        case "CompositeAlternativeRequiresReplacement":
          assert.deepStrictEqual(decoded.path, errorPath)
          assert.strictEqual(decoded.alternativeId, alternativeId)
          assert.strictEqual(decoded.composite, "List")
          break
        case "ConflictResolutionSchemaError":
          assert.deepStrictEqual(decoded.path, errorPath)
          assert.instanceOf(decoded.cause, Error)
          assert.strictEqual(decoded.cause.message, "invalid replacement")
          break
      }
    }

    const inspectionCodec = Schema.toCodecJson(Conflict.InspectionError)
    for (const [error, expected] of cases) {
      if (error._tag !== "UnsupportedConflictValue" && error._tag !== "UnsupportedConflictKey") continue
      assert.deepStrictEqual(Schema.encodeSync(inspectionCodec)(error), expected)
      const decoded = Schema.decodeUnknownSync(inspectionCodec)(expected)
      assert.deepStrictEqual(Schema.encodeSync(inspectionCodec)(decoded), expected)
    }
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

  it("uses deterministic code unit ordering for non ASCII keys, IDs, and paths", () => {
    assert.deepStrictEqual(
      encodeValue({ "ä": 1, z: 2 }),
      {
        _tag: "Map",
        entries: [
          { key: "z", value: { _tag: "Number", value: 2 } },
          { key: "ä", value: { _tag: "Number", value: 1 } }
        ]
      }
    )
    assert.deepStrictEqual(
      Conflict.normalizeAlternatives([
        { id: Conflict.AlternativeId.make("ä"), value: 1 },
        { id: Conflict.AlternativeId.make("z"), value: 2 }
      ]).map(({ id }) => id),
      ["z", "ä"]
    )
    assert.deepStrictEqual(
      Conflict.normalizeRecords([
        Schema.decodeUnknownSync(Conflict.Record)(
          record("ä", [makeAlternative("one"), makeAlternative("two")])
        ),
        Schema.decodeUnknownSync(Conflict.Record)(
          record("z", [makeAlternative("one"), makeAlternative("two")])
        )
      ]).map((entry) => entry.path.target._tag === "Key" && entry.path.target.key),
      ["z", "ä"]
    )
  })

  it("rejects prototype sensitive keys without mutating a prototype", () => {
    const keys = [...Object.getOwnPropertyNames(Object.prototype), "prototype"]
    const before = Object.getOwnPropertyDescriptors(Object.prototype)
    for (const key of keys) {
      assertSchemaError(() =>
        Schema.decodeUnknownSync(Conflict.Path)({
          parents: [],
          target: { _tag: "Key", key }
        })
      )
      assertSchemaError(() =>
        decodeValue({
          _tag: "Map",
          entries: [{ key, value: { _tag: "Null" } }]
        })
      )
      assert.deepStrictEqual(Object.getOwnPropertyDescriptors(Object.prototype), before, key)
    }
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
