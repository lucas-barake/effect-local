import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as SchemaAST from "effect/SchemaAST"
import * as SchemaGetter from "effect/SchemaGetter"
import * as Canonical from "../src/Canonical.js"
import * as Definition from "../src/Definition.js"
import * as Model from "../src/Model.js"
import * as SchemaDescriptor from "../src/SchemaDescriptor.js"

const UserId = Schema.String.pipe(Schema.brand("UserId"))

const Opaque = Schema.Struct({ a: Schema.String }).pipe(
  Schema.decodeTo(Schema.Struct({ b: Schema.String }), {
    decode: SchemaGetter.transform((value: { readonly a: string }) => ({ b: value.a })),
    encode: SchemaGetter.transform((value: { readonly b: string }) => ({ a: value.b }))
  }),
  Schema.annotate({ identifier: "Opaque" })
)

const persistedHashes: ReadonlyArray<readonly [string, Schema.Top, string]> = [
  ["String", Schema.String, "3c4f845888eade9c"],
  ["Number", Schema.Number, "372d2ad2e7f05d44"],
  ["Boolean", Schema.Boolean, "f8248ba0bf21ea4d"],
  ["NumberFromString", Schema.NumberFromString, "a3f45e6e5dae4a66"],
  ["Null", Schema.Null, "0e177ab8a35325b4"],
  ["Literal", Schema.Literal("x"), "7b47772f88579375"],
  ["Literals", Schema.Literals(["dm", "group"]), "079fa2ca0ddab917"],
  ["Brand", UserId, "3c4f845888eade9c"],
  [
    "Struct",
    Schema.Struct({
      id: UserId,
      kind: Schema.Literals(["a", "b"]),
      members: Schema.Array(UserId),
      n: Schema.NullOr(Schema.Number),
      o: Schema.optionalKey(Schema.String),
      u: Schema.optional(Schema.Number),
      un: Schema.UndefinedOr(Schema.String)
    }),
    "d8e803504cb198b7"
  ],
  ["TaggedStruct", Schema.TaggedStruct("T", { v: Schema.String }), "facc735253850c12"],
  ["Union", Schema.Union([Schema.String, Schema.Number]), "4a60ac8936df1799"],
  ["Tuple", Schema.Tuple([Schema.String, Schema.Number]), "37856878c2d2dad5"],
  ["NonEmptyArray", Schema.NonEmptyArray(Schema.String), "f1ad3176fe07a07a"],
  ["IdentifiedTransformation", Opaque, "2ee1ec72ca22ca82"]
]

describe("SchemaDescriptor", () => {
  it("keeps the persisted hash of every structural schema stable across Effect releases", () => {
    for (const [name, schema, expected] of persistedHashes) {
      const descriptor = SchemaDescriptor.make(schema)
      assert.strictEqual(Canonical.hash(descriptor), expected, name)
    }
  })

  it("describes the filters and declarations Effect ships, keeping their parameters in the descriptor", () => {
    const builtIns: ReadonlyArray<readonly [string, Schema.Top]> = [
      ["NonEmptyString", Schema.NonEmptyString],
      ["Int", Schema.Int],
      ["Finite", Schema.Finite],
      ["GreaterThan", Schema.Number.check(Schema.isGreaterThan(0))],
      ["Pattern", Schema.String.check(Schema.isPattern(/^a/))],
      ["MaxLength", Schema.String.check(Schema.isMaxLength(10))],
      ["Date", Schema.Date],
      ["OptionOfString", Schema.Option(Schema.String)],
      ["OptionOfNumber", Schema.Option(Schema.Number)]
    ]
    const hashes = builtIns.map(([, schema]) => Canonical.hash(SchemaDescriptor.make(schema)))
    assert.strictEqual(new Set(hashes).size, builtIns.length)
    const hashOf = (check: SchemaAST.Check<string>) =>
      Schema.String.check(check).pipe(SchemaDescriptor.make, Canonical.hash)
    const minimumOne = hashOf(Schema.isMinLength(1))
    const minimumTwo = hashOf(Schema.isMinLength(2))
    const patternA = hashOf(Schema.isPattern(/^a/))
    const patternB = hashOf(Schema.isPattern(/^b/))
    assert.notStrictEqual(minimumOne, minimumTwo)
    assert.notStrictEqual(patternA, patternB)
  })

  it("still rejects a filter that carries no stable identity", () => {
    const Even = Schema.Number.check(Schema.makeFilter((value: number) => value % 2 === 0))
    assert.throws(() => SchemaDescriptor.make(Even), /Opaque schema checks/)
  })

  it("does not let a built-in declaration vouch for filters, transformations, or defaults added to it", () => {
    const OptionalNumber = Schema.Option(Schema.Number)
    const Present = OptionalNumber.check(Schema.makeFilter(Option.isSome))
    const FromNumber = Schema.Number.pipe(Schema.decodeTo(OptionalNumber, {
      decode: SchemaGetter.transform(Option.some),
      encode: SchemaGetter.transform(Option.getOrElse(() => 0))
    }))
    const defaultValue = Effect.succeed(Option.some(1))
    const Defaulted = Schema.Struct({ value: OptionalNumber.pipe(Schema.withConstructorDefault(defaultValue)) })
    const InYear = Schema.DateFromString.check(Schema.makeFilter((date: Date) => date.getUTCFullYear() === 2025))
    assert.throws(() => SchemaDescriptor.make(Present), /Opaque schema checks/)
    assert.throws(() => SchemaDescriptor.make(InYear), /Opaque schema checks/)
    assert.throws(() => SchemaDescriptor.make(FromNumber), /Opaque schema transformations/)
    assert.throws(() => SchemaDescriptor.make(Defaulted), /Opaque constructor defaults/)
  })

  it("lets a definition use models whose fields are refined by Effect filters", () => {
    const Task = Model.make("Task", {
      version: 1,
      key: Schema.String,
      schema: Schema.Struct({ title: Schema.NonEmptyString, rank: Schema.Int, score: Schema.Finite })
    })
    const definition = Definition.make({ version: 1, models: [Task], mutations: [] })
    assert.strictEqual(definition.models.length, 1)
  })
})
