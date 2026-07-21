import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as Mutation from "../src/Mutation.js"

describe("Mutation", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const Rename = Mutation.make("Rename", {
    document: Task,
    payload: Schema.Struct({ title: Schema.String })
  })

  it("uses void and never defaults", () => {
    assert.strictEqual(Rename.success, Schema.Void)
    assert.strictEqual(Rename.error, Schema.Never)
  })

  it.effect("provides a handler through its generated service", () =>
    Effect.gen(function*() {
      const handler = yield* Rename.handler
      const draft = { title: "old" }
      assert.strictEqual(handler({ draft, payload: { title: "new" }, current: { title: "old" } }), undefined)
      assert.strictEqual(draft.title, "new")
    }).pipe(Effect.provide(Mutation.layer(Rename, ({ draft, payload }) => void (draft.title = payload.title)))))

  it("supports typed synchronous rejection", () => {
    const Checked = Mutation.make("Checked", {
      document: Task,
      payload: Schema.String,
      error: Schema.String
    })
    const handler = Mutation.handler(Checked, () => Result.fail("no"))
    assert.deepStrictEqual(handler({ draft: { title: "" }, payload: "", current: { title: "" } }), Result.fail("no"))
  })

  it.effect("keeps same named handler services isolated", () => {
    const OtherRename = Mutation.make("Rename", { document: Task, payload: Schema.Struct({ title: Schema.String }) })
    return Effect.gen(function*() {
      const first = yield* Rename.handler
      const second = yield* OtherRename.handler
      const firstDraft = { title: "old" }
      const secondDraft = { title: "old" }
      first({ draft: firstDraft, payload: { title: "first" }, current: { title: "old" } })
      second({ draft: secondDraft, payload: { title: "second" }, current: { title: "old" } })
      assert.strictEqual(firstDraft.title, "first")
      assert.strictEqual(secondDraft.title, "second")
    }).pipe(Effect.provide(Layer.merge(
      Mutation.layer(Rename, ({ draft, payload }) => void (draft.title = payload.title)),
      Mutation.layer(OtherRename, ({ draft, payload }) => void (draft.title = payload.title))
    )))
  })
})
