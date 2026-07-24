import { assert, describe, it } from "@effect/vitest"
import * as Combiner from "effect/Combiner"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import * as Canonical from "../src/Canonical.js"
import * as SchemaDescriptor from "../src/SchemaDescriptor.js"

describe("SchemaDescriptor", () => {
  const hash = (schema: Schema.Constraint) => Canonical.hash(SchemaDescriptor.make(schema))
  const metadata = <A,>(value: A): never => value as never

  it("ignores documentation annotations", () => {
    const plain = Schema.Struct({ title: Schema.String })
    const documented = Schema.Struct({
      title: Schema.String.pipe(Schema.annotate({
        description: "The title",
        title: "Title",
        examples: ["Buy milk"],
        documentation: "Long form docs"
      }))
    }).pipe(Schema.annotate({ description: "A task" }))
    assert.strictEqual(hash(plain), hash(documented))
  })

  it("ignores documentation annotations merged into checks", () => {
    const plain = Schema.String.check(Schema.isMinLength(3))
    const documented = Schema.String.check(Schema.isMinLength(3)).annotate({ description: "At least three" })
    assert.strictEqual(hash(plain), hash(documented))
  })

  it("ignores key and encoded documentation annotations", () => {
    assert.strictEqual(
      hash(Schema.Struct({ value: Schema.String })),
      hash(Schema.Struct({
        value: Schema.String.pipe(Schema.annotateKey({ description: "The value" }))
      }))
    )
    assert.strictEqual(
      hash(Schema.NumberFromString),
      hash(Schema.NumberFromString.pipe(Schema.annotateEncoded({ description: "Encoded number" })))
    )
  })

  it("distinguishes codecs that share an encoded shape", () => {
    assert.notStrictEqual(hash(Schema.String), hash(Schema.NumberFromString))
    assert.notStrictEqual(hash(Schema.String), hash(Schema.Trim))
  })

  it("distinguishes transformations with identical endpoints", () => {
    assert.notStrictEqual(hash(Schema.StringFromBase64), hash(Schema.StringFromHex))
  })

  it("distinguishes semantic parse options", () => {
    const plain = Schema.Struct({ value: Schema.String })
    const strict = plain.annotate({ parseOptions: { onExcessProperty: "error" } })
    assert.notStrictEqual(hash(plain), hash(strict))
  })

  it("distinguishes check grouping, aborting, and structural evaluation", () => {
    const minimum = Schema.makeFilter(
      (value: string) => value.length >= 2,
      { meta: metadata({ _tag: "MinimumLength", value: 2 }) }
    )
    const includes = Schema.makeFilter(
      (value: string) => value.includes("b"),
      { meta: metadata({ _tag: "Includes", value: "b" }) }
    )
    assert.notStrictEqual(
      hash(Schema.String.check(minimum, includes)),
      hash(Schema.String.check(minimum.and(includes)))
    )
    assert.notStrictEqual(
      hash(Schema.String.check(minimum.abort(), includes)),
      hash(Schema.String.check(minimum, includes))
    )
    const predicate = (value: string) => value.length >= 2
    const ordinary = Schema.String.check(Schema.makeFilter(predicate, {
      meta: metadata({ _tag: "Length" })
    }))
    const structural = Schema.String.check(Schema.makeFilter(predicate, {
      meta: metadata({ _tag: "Length" }),
      "~structural": true
    }))
    assert.notStrictEqual(hash(ordinary), hash(structural))
  })

  it("rejects opaque executable behavior without stable metadata", () => {
    assert.throws(
      () => hash(Schema.String.check(Schema.makeFilter((value) => value === "a"))),
      /identifier or meta annotation/
    )
    const identifiedSource = Schema.String.annotate({ identifier: "WireString" })
    const fromIdentifiedSource = (transform: (value: string) => string) =>
      identifiedSource.pipe(Schema.decodeTo(Schema.String, {
        decode: SchemaGetter.transform(transform),
        encode: SchemaGetter.passthrough()
      }))
    assert.throws(
      () => hash(fromIdentifiedSource((value) => value.toUpperCase())),
      /identifier or meta annotation/
    )
    assert.throws(
      () => hash(Schema.declare((value): value is string => typeof value === "string")),
      /identifier or meta annotation/
    )
    assert.throws(
      () =>
        hash(Schema.String.pipe(Schema.decodeTo(Schema.String, {
          decode: SchemaGetter.transform((value) => `${value}a`),
          encode: SchemaGetter.transform((value) => value.slice(0, -1))
        }))),
      /identifier or meta annotation/
    )
    assert.throws(
      () =>
        hash(Schema.String.pipe(
          Schema.middlewareDecoding((effect) => effect)
        )),
      /identifier or meta annotation/
    )
    assert.throws(
      () =>
        hash(Schema.Struct({
          value: Schema.String.pipe(
            Schema.optionalKey,
            Schema.withConstructorDefault(Effect.succeed("default"))
          )
        })),
      /identifier or meta annotation/
    )
    assert.throws(
      () =>
        hash(Schema.Record(Schema.String, Schema.Number, {
          keyValueCombiner: { decode: Combiner.first() }
        })),
      /identifier or meta annotation/
    )
  })

  it("distinguishes structural checks and their parameters", () => {
    assert.notStrictEqual(hash(Schema.String), hash(Schema.String.check(Schema.isMinLength(1))))
    assert.notStrictEqual(
      hash(Schema.String.check(Schema.isMinLength(1))),
      hash(Schema.String.check(Schema.isMinLength(2)))
    )
  })

  it("distinguishes brands", () => {
    assert.notStrictEqual(hash(Schema.String), hash(Schema.String.pipe(Schema.brand("UserId"))))
    assert.notStrictEqual(
      hash(Schema.String.pipe(Schema.brand("UserId"))),
      hash(Schema.String.pipe(Schema.brand("TaskId")))
    )
  })

  it("normalizes brand sets", () => {
    assert.strictEqual(
      hash(Schema.String.pipe(Schema.brand("A"), Schema.brand("B"))),
      hash(Schema.String.pipe(Schema.brand("B"), Schema.brand("A"), Schema.brand("A")))
    )
  })

  it("distinguishes optional from required properties", () => {
    assert.notStrictEqual(
      hash(Schema.Struct({ title: Schema.String })),
      hash(Schema.Struct({ title: Schema.optionalKey(Schema.String) }))
    )
  })

  it("serializes recursive schemas deterministically", () => {
    interface Category {
      readonly name: string
      readonly children: ReadonlyArray<Category>
    }
    const makeCategory = (name: Schema.Codec<string, string>): Schema.Codec<Category, Category> => {
      const schema: Schema.Codec<Category, Category> = Schema.Struct({
        name,
        children: Schema.Array(Schema.suspend((): Schema.Codec<Category, Category> => schema))
      })
      return schema
    }
    const first = makeCategory(Schema.String)
    const second = makeCategory(Schema.String)
    const documented = makeCategory(Schema.String.pipe(Schema.annotate({ description: "The name" })))
    const recoded = makeCategory(Schema.Trim)
    assert.strictEqual(hash(first), hash(second))
    assert.strictEqual(hash(first), hash(documented))
    assert.notStrictEqual(hash(first), hash(recoded))
  })

  it("treats transparent suspension as the suspended schema", () => {
    assert.strictEqual(hash(Schema.String), hash(Schema.suspend(() => Schema.String)))
  })

  it("ignores non-semantic context on transparent suspension", () => {
    const bare = Schema.suspend(() => Schema.String)
    const documented = bare.annotate({ description: "Documentation only" })
    assert.strictEqual(hash(bare), hash(documented))

    const required = Schema.Struct({
      value: Schema.suspend(() => Schema.Literal("seed"))
    })
    const defaulted = Schema.Struct({
      value: Schema.suspend(() => Schema.Literal("seed")).pipe(
        Schema.withConstructorDefault(Effect.succeed("seed"))
      )
    })
    const typeHash = (schema: Schema.Constraint) =>
      Canonical.hash(SchemaDescriptor.make(Schema.toType(schema), {
        includeConstructorDefaults: false
      }))
    assert.strictEqual(typeHash(required), typeHash(defaulted))
  })

  it("preserves context attached to suspended schemas", () => {
    const required = Schema.Struct({ value: Schema.suspend(() => Schema.String) })
    const optional = Schema.Struct({
      value: Schema.optionalKey(Schema.suspend(() => Schema.String))
    })
    assert.throws(() => Schema.decodeUnknownSync(required)({}))
    assert.deepStrictEqual(Schema.decodeUnknownSync(optional)({}), {})
    assert.notStrictEqual(hash(required), hash(optional))
  })

  it("distinguishes built-in declared types", () => {
    assert.notStrictEqual(hash(Schema.Date), hash(Schema.URL))
    assert.notStrictEqual(
      hash(Schema.Struct({ at: Schema.Date })),
      hash(Schema.Struct({ at: Schema.URL }))
    )
  })

  it("preserves property key kinds and rejects local symbol identity", () => {
    assert.notStrictEqual(
      hash(Schema.Struct({ "Symbol(id)": Schema.String })),
      hash(Schema.Struct({ [Symbol.for("id")]: Schema.String }))
    )
    assert.throws(() => hash(Schema.UniqueSymbol(Symbol("id"))), /local symbols/)
    assert.throws(() => hash(Schema.Struct({ [Symbol("id")]: Schema.String })), /local symbols/)
  })

  it("keeps reordered struct fields equal", () => {
    assert.strictEqual(
      hash(Schema.Struct({ title: Schema.String, done: Schema.Boolean })),
      hash(Schema.Struct({ done: Schema.Boolean, title: Schema.String }))
    )
  })

  it("keeps shared subtrees structurally equal to duplicated ones", () => {
    const shared = Schema.String.check(Schema.isMinLength(1))
    const duplicated = Schema.Struct({
      a: Schema.String.check(Schema.isMinLength(1)),
      b: Schema.String.check(Schema.isMinLength(1))
    })
    assert.strictEqual(hash(Schema.Struct({ a: shared, b: shared })), hash(duplicated))
  })

  it("keeps non-JSON semantic metadata collision free", () => {
    const metaHash = (value: unknown) => hash(Schema.String.annotate({ meta: { _tag: "TestMeta", value } as never }))
    assert.notStrictEqual(metaHash(undefined), metaHash(null))
    assert.notStrictEqual(metaHash(Number.NaN), metaHash("NaN"))
    assert.notStrictEqual(metaHash(0), metaHash(-0))
    assert.notStrictEqual(metaHash(Symbol.for("id")), metaHash("Symbol(id)"))
    assert.notStrictEqual(metaHash(Symbol.iterator), metaHash(Symbol.for("iterator")))
    assert.notStrictEqual(metaHash(1n), metaHash("1"))
    assert.notStrictEqual(
      metaHash(new Date("2024-01-01T00:00:00.000Z")),
      metaHash("2024-01-01T00:00:00.000Z")
    )
    assert.notStrictEqual(metaHash(["value"]), metaHash({ 0: "value" }))
    assert.notStrictEqual(metaHash(/value/i), metaHash({ source: "value", flags: "i" }))
    assert.throws(() => metaHash(() => true), /functions/)
    assert.throws(() => metaHash(new Map([["key", "value"]])), /non-plain objects/)
    let getterCalls = 0
    const accessor = Object.defineProperty({}, "value", {
      enumerable: true,
      get: () => {
        getterCalls++
        return "value"
      }
    })
    assert.throws(() => metaHash(accessor), /accessor properties/)
    assert.strictEqual(getterCalls, 0)
    const circular: { self?: unknown } = {}
    circular.self = circular
    assert.notStrictEqual(metaHash(circular), metaHash({ self: "[Circular]" }))
  })

  it("rejects stateful regular expressions in semantic metadata", () => {
    assert.throws(
      () => hash(Schema.String.check(Schema.isPattern(/a/g))),
      /stateful regular expressions/
    )
    assert.throws(
      () => hash(Schema.String.check(Schema.isPattern(/a/y))),
      /stateful regular expressions/
    )
  })

  it("normalizes cyclic semantic metadata independent of key order and sharing", () => {
    const makePair = () => {
      const x: { readonly name: string; next?: unknown } = { name: "x" }
      const y: { readonly name: string; next?: unknown } = { name: "y" }
      x.next = y
      y.next = x
      return { x, y }
    }
    const metaHash = (value: unknown) => hash(Schema.String.annotate({ meta: { _tag: "Cycle", value } as never }))

    const pair = makePair()
    assert.strictEqual(metaHash({ x: pair.x, y: pair.y }), metaHash({ y: pair.y, x: pair.x }))

    const left = makePair()
    const right = makePair()
    assert.strictEqual(
      metaHash({ x: pair.x, y: pair.y }),
      metaHash({ x: left.x, y: right.y })
    )
  })

  it("keeps shared acyclic descriptor growth bounded", () => {
    let decoded: Schema.Constraint = Schema.String
    for (let index = 0; index < 20; index++) {
      decoded = Schema.Union([decoded, decoded])
    }
    const codec = Schema.String.pipe(
      Schema.decodeTo(decoded),
      Schema.annotate({ meta: metadata({ _tag: "SharedDecodedDag" }) })
    )
    assert.isBelow(JSON.stringify(SchemaDescriptor.make(codec)).length, 1_000_000)
  })

  it("keeps shared semantic metadata growth bounded", () => {
    let value: unknown = { leaf: true }
    for (let index = 0; index < 16; index++) {
      value = { left: value, right: value }
    }
    const schema = Schema.String.annotate({ meta: { _tag: "SharedMeta", value } as never })
    assert.isBelow(JSON.stringify(SchemaDescriptor.make(schema)).length, 1_000_000)
  })

  it("captures the remaining structural AST contracts", () => {
    assert.notStrictEqual(hash(Schema.Literal("a")), hash(Schema.Literal("b")))
    assert.notStrictEqual(hash(Schema.Enum({ A: "a" })), hash(Schema.Enum({ A: "b" })))
    assert.strictEqual(
      hash(Schema.Enum({ A: "a", B: "b" })),
      hash(Schema.Enum({ B: "b", A: "a" }))
    )
    assert.notStrictEqual(
      hash(Schema.TemplateLiteral(["item-", Schema.Number])),
      hash(Schema.TemplateLiteral(["entry-", Schema.Number]))
    )
    assert.notStrictEqual(
      hash(Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Number])),
      hash(Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Boolean]))
    )
    assert.notStrictEqual(
      hash(Schema.Array(Schema.String)),
      hash(Schema.mutable(Schema.Array(Schema.String)))
    )
    assert.notStrictEqual(
      hash(Schema.Record(Schema.String, Schema.String)),
      hash(Schema.Record(Schema.String, Schema.Number))
    )
    assert.notStrictEqual(
      hash(Schema.Union([Schema.NumberFromString, Schema.String])),
      hash(Schema.Union([Schema.String, Schema.NumberFromString]))
    )
    assert.notStrictEqual(
      hash(Schema.Union([Schema.Literal("a"), Schema.String], { mode: "anyOf" })),
      hash(Schema.Union([Schema.Literal("a"), Schema.String], { mode: "oneOf" }))
    )
    const plain = Schema.Struct({ a: Schema.String, b: Schema.String })
    const encodedChecked = plain.pipe(
      Schema.flip,
      Schema.check(Schema.isMaxProperties(1)),
      Schema.flip
    )
    assert.notStrictEqual(hash(plain), hash(encodedChecked))
  })

  it("distinguishes mutable keys and encoding checks on every supporting AST", () => {
    assert.notStrictEqual(
      hash(Schema.Struct({ value: Schema.String })),
      hash(Schema.Struct({ value: Schema.mutableKey(Schema.String) }))
    )

    const encodedFilter = <A,>(predicate: (value: A) => boolean, tag: string) =>
      Schema.makeFilter(predicate, { meta: metadata({ _tag: tag }) })
    const tuple = Schema.Tuple([Schema.String])
    const checkedTuple = tuple.pipe(
      Schema.flip,
      Schema.check(encodedFilter((value: readonly [string]) => value[0].length > 1, "TupleLength")),
      Schema.flip
    )
    const union = Schema.Union([Schema.Literal("a"), Schema.Literal("aa")])
    const checkedUnion = union.pipe(
      Schema.flip,
      Schema.check(encodedFilter((value: "a" | "aa") => value === "aa", "UnionValue")),
      Schema.flip
    )
    const declaration = Schema.declare(
      (value): value is string => typeof value === "string",
      { identifier: "StringDeclaration" }
    )
    const checkedDeclaration = declaration.pipe(
      Schema.flip,
      Schema.check(encodedFilter((value: string) => value.length > 1, "DeclarationLength")),
      Schema.flip
    )

    assert.notStrictEqual(hash(tuple), hash(checkedTuple))
    assert.notStrictEqual(hash(union), hash(checkedUnion))
    assert.notStrictEqual(hash(declaration), hash(checkedDeclaration))
  })

  it("serializes mutual recursion with stable back references", () => {
    interface Left {
      readonly right: Right | null
    }
    interface Right {
      readonly left: Left | null
      readonly label: string
    }
    const makePair = (label: Schema.Codec<string, string>) => {
      let LeftSchema!: Schema.Codec<Left, Left>
      let RightSchema!: Schema.Codec<Right, Right>
      LeftSchema = Schema.Struct({
        right: Schema.NullOr(Schema.suspend((): Schema.Codec<Right, Right> => RightSchema))
      })
      RightSchema = Schema.Struct({
        left: Schema.NullOr(Schema.suspend((): Schema.Codec<Left, Left> => LeftSchema)),
        label
      })
      return [hash(LeftSchema), hash(RightSchema)] as const
    }
    assert.deepStrictEqual(makePair(Schema.String), makePair(Schema.String))
    assert.notDeepEqual(makePair(Schema.String), makePair(Schema.Trim))
  })

  it("keeps shared and duplicated recursive subtrees equal", () => {
    interface Left {
      readonly right: Right | null
    }
    interface Right {
      readonly left: Left | null
    }
    const makeLeft = (): Schema.Codec<Left, Left> => {
      let LeftSchema!: Schema.Codec<Left, Left>
      let RightSchema!: Schema.Codec<Right, Right>
      LeftSchema = Schema.Struct({
        right: Schema.NullOr(Schema.suspend((): Schema.Codec<Right, Right> => RightSchema))
      })
      RightSchema = Schema.Struct({
        left: Schema.NullOr(Schema.suspend((): Schema.Codec<Left, Left> => LeftSchema))
      })
      return LeftSchema
    }
    const shared = makeLeft()
    assert.strictEqual(
      hash(Schema.Struct({ first: shared, second: shared })),
      hash(Schema.Struct({ first: makeLeft(), second: makeLeft() }))
    )
  })

  it("keeps mutually recursive component sharing equal to duplication", () => {
    interface A {
      readonly _tag: "A"
      readonly b: B
    }
    interface B {
      readonly _tag: "B"
      readonly a: A
    }
    const makePair = () => {
      let A!: Schema.Codec<A, A>
      let B!: Schema.Codec<B, B>
      A = Schema.Struct({
        _tag: Schema.Literal("A"),
        b: Schema.suspend((): Schema.Codec<B, B> => B)
      })
      B = Schema.Struct({
        _tag: Schema.Literal("B"),
        a: Schema.suspend((): Schema.Codec<A, A> => A)
      })
      return { A, B }
    }

    const pair = makePair()
    const shared = Schema.Struct({ a: pair.A, b: pair.B })
    const left = makePair()
    const right = makePair()
    const duplicated = Schema.Struct({ a: left.A, b: right.B })
    assert.strictEqual(hash(shared), hash(duplicated))
  })

  it("treats recursive transparent suspension as its suspended schema", () => {
    let root!: Schema.Constraint
    const suspended = Schema.suspend(() => root)
    root = Schema.Struct({
      next: Schema.NullOr(suspended)
    })
    assert.strictEqual(hash(root), hash(suspended))
  })

  it("accepts explicitly identified executable behavior and literal defaults", () => {
    const transform = Schema.String.pipe(
      Schema.decodeTo(Schema.String, {
        decode: SchemaGetter.transform((value) => `${value}a`),
        encode: SchemaGetter.transform((value) => value.slice(0, -1))
      }),
      Schema.annotate({ meta: metadata({ _tag: "AppendSuffix", suffix: "a" }) })
    )
    const defaulted = Schema.String.pipe(
      Schema.annotate({ meta: metadata({ _tag: "ConstructorDefault", value: "x" }) }),
      Schema.optionalKey,
      Schema.withConstructorDefault(Effect.succeed("x"))
    )
    const combined = Schema.Record(Schema.String, Schema.Number, {
      keyValueCombiner: { decode: Combiner.first() }
    }).annotate({ meta: metadata({ _tag: "FirstValue" }) })
    assert.doesNotThrow(() => hash(transform))
    assert.doesNotThrow(() => hash(defaulted))
    assert.doesNotThrow(() => hash(combined))
    assert.doesNotThrow(() => hash(Schema.tag("Task")))
    assert.doesNotThrow(() =>
      hash(
        Schema.ArrayEnsure(Schema.String).annotate({
          meta: metadata({ _tag: "ArrayEnsure" })
        })
      )
    )
    assert.doesNotThrow(() =>
      hash(
        Schema.Struct({ value: Schema.String })
          .pipe(Schema.encodeKeys({ value: "wire_value" }))
          .annotate({ meta: metadata({ _tag: "EncodeKeys", value: "wire_value" }) })
      )
    )
    assert.doesNotThrow(() =>
      hash(
        Schema.toCodecArrayFromSingle(Schema.Array(Schema.String))
          .annotate({ meta: metadata({ _tag: "ToCodecArrayFromSingle" }) })
      )
    )
    assert.doesNotThrow(() =>
      hash(
        Schema.tagDefaultOmit("Task").annotate({
          meta: metadata({ _tag: "TagDefaultOmit" })
        })
      )
    )
    assert.doesNotThrow(() => hash(Schema.RedactedFromValue(Schema.String)))
  })

  it("includes constructor defaults by default and fully excludes them when requested", () => {
    const descriptorHash = (
      schema: Schema.Constraint,
      includeConstructorDefaults?: boolean
    ) =>
      Canonical.hash(SchemaDescriptor.make(
        schema,
        includeConstructorDefaults === undefined ? undefined : { includeConstructorDefaults }
      ))
    const literal = Schema.Literal("Task")
    const defaultedLiteral = Schema.tag("Task")
    assert.notStrictEqual(descriptorHash(literal), descriptorHash(defaultedLiteral))
    assert.strictEqual(
      descriptorHash(literal, false),
      descriptorHash(defaultedLiteral, false)
    )

    const optional = Schema.optionalKey(Schema.String)
    const defaulted = optional.pipe(
      Schema.withConstructorDefault(Effect.succeed("seed"))
    )
    assert.doesNotThrow(() => descriptorHash(defaulted, false))
    assert.strictEqual(descriptorHash(optional, false), descriptorHash(defaulted, false))
  })
})
