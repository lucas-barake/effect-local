import { assert, describe, it } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as Mutation from "../src/Mutation.js"

describe("Mutation", () => {
  class TitleFormat extends Context.Service<TitleFormat, {
    readonly format: (title: string) => string
  }>()("@lucas-barake/effect-local/test/TitleFormat") {}

  class Rejected extends Schema.TaggedErrorClass<Rejected>()("Rejected", {}) {}

  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const Rename = Mutation.make("Rename", {
    document: Task,
    payload: { title: Schema.String }
  })

  it("uses void and never defaults", () => {
    assert.isTrue(Schema.isSchema(Rename.payloadSchema))
    assert.strictEqual(Rename.successSchema, Schema.Void)
    assert.strictEqual(Rename.errorSchema, Schema.Never)
  })

  it.effect("provides a handler through its generated service", () =>
    Effect.gen(function*() {
      const handler = yield* Rename.handler
      const draft = { title: "old" }
      assert.strictEqual(handler({ draft, payload: { title: "new" }, current: { title: "old" } }), undefined)
      assert.strictEqual(draft.title, "new")
    }).pipe(Effect.provide(Rename.toLayer(({ draft, payload }) => void (draft.title = payload.title)))))

  it.effect("builds a handler effectfully", () =>
    Effect.gen(function*() {
      const handler = yield* Rename.handler
      const draft = { title: "old" }
      handler({ draft, payload: { title: "new" }, current: { title: "old" } })
      assert.strictEqual(draft.title, "prefix:NEW")
    }).pipe(
      Effect.provide(
        Rename.toLayer(Effect.gen(function*() {
          const titleFormat = yield* TitleFormat
          return ({ draft, payload }) => void (draft.title = titleFormat.format(payload.title))
        })).pipe(
          Layer.provide(Layer.succeed(TitleFormat, {
            format: (title) => `prefix:${title.toUpperCase()}`
          }))
        )
      )
    ))

  it("rejects reserved mutation names", () => {
    assert.throws(
      () => Mutation.make("$create", { document: Task, payload: Schema.String }),
      TypeError
    )
    assert.throws(
      () => Mutation.make("$anything", { document: Task, payload: Schema.String }),
      TypeError
    )
  })

  it("supports typed synchronous rejection", () => {
    const Checked = Mutation.make("Checked", {
      document: Task,
      payload: Schema.String,
      error: Rejected
    })
    const handler = Checked.of(() => Result.fail(new Rejected()))
    assert.deepStrictEqual(
      handler({ draft: { title: "" }, payload: "", current: { title: "" } }),
      Result.fail(new Rejected())
    )
  })

  it.effect("keeps same-name handler services independent", () => {
    const OtherRename = Mutation.make("Rename", { document: Task, payload: Schema.Struct({ title: Schema.String }) })
    assert.notStrictEqual(Rename.handler.key, OtherRename.handler.key)
    return Effect.gen(function*() {
      const rename = yield* Rename.handler
      const other = yield* OtherRename.handler
      const first = { title: "old" }
      const second = { title: "old" }
      rename({ draft: first, payload: { title: "first" }, current: first })
      other({ draft: second, payload: { title: "second" }, current: second })
      assert.strictEqual(first.title, "first")
      assert.strictEqual(second.title, "SECOND")
    }).pipe(Effect.provide(Layer.merge(
      Rename.toLayer(({ draft, payload }) => void (draft.title = payload.title)),
      OtherRename.toLayer(({ draft, payload }) => void (draft.title = payload.title.toUpperCase()))
    )))
  })
})
