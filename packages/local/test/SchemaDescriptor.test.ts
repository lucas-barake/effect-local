import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import * as Canonical from "../src/Canonical.js"
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
})
