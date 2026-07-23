import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as Canonical from "../src/Canonical.js"
import * as SchemaDescriptor from "../src/SchemaDescriptor.js"

describe("SchemaDescriptor", () => {
  const hash = (schema: Schema.Constraint) => Canonical.hash(SchemaDescriptor.make(schema))

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

  it("distinguishes codecs that share an encoded shape", () => {
    assert.notStrictEqual(hash(Schema.String), hash(Schema.NumberFromString))
    assert.notStrictEqual(hash(Schema.String), hash(Schema.Trim))
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

  it("distinguishes built-in declared types", () => {
    assert.notStrictEqual(hash(Schema.Date), hash(Schema.URL))
    assert.notStrictEqual(
      hash(Schema.Struct({ at: Schema.Date })),
      hash(Schema.Struct({ at: Schema.URL }))
    )
  })

  it("keeps struct properties whose symbol keys share a description", () => {
    const first = Symbol("id")
    const second = Symbol("id")
    assert.notStrictEqual(
      hash(Schema.Struct({ [first]: Schema.String, [second]: Schema.Number })),
      hash(Schema.Struct({ [first]: Schema.Boolean, [second]: Schema.Number }))
    )
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
})
