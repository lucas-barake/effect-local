import * as Automerge from "@automerge/automerge"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as Identity from "../src/Identity.js"

describe("Document codec", () => {
  const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String, done: Schema.Boolean }),
    version: 1
  })

  it.effect("round trips through encode and decode", () =>
    Effect.gen(function*() {
      const encoded = yield* Document.encode(Task, documentId, { title: "one", done: false })
      assert.deepStrictEqual(encoded, { title: "one", done: false })
      const decoded = yield* Document.decode(Task, documentId, encoded)
      assert.deepStrictEqual(decoded, { title: "one", done: false })
    }))

  it.effect("maps decode failures to a tagged DocumentDecodeError carrying the id", () =>
    Effect.gen(function*() {
      const error = yield* Document.decode(Task, documentId, { title: 1, done: false }).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "DocumentDecodeError")
      if (error.reason._tag !== "DocumentDecodeError") return
      assert.strictEqual(error.reason.documentId, documentId)
    }))

  it.effect("maps encode failures to a tagged DocumentEncodeError carrying the id", () =>
    Effect.gen(function*() {
      const error = yield* Document.encode(Task, documentId, { title: 1, done: false } as never).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "DocumentEncodeError")
      if (error.reason._tag !== "DocumentEncodeError") return
      assert.strictEqual(error.reason.documentId, documentId)
    }))

  it("classifies the remaining Automerge value branches", () => {
    assert.isTrue(Document.isAutomergeValue(new Date()))
    assert.isTrue(Document.isAutomergeValue(new Uint8Array([1, 2])))
    assert.isTrue(Document.isAutomergeValue(new Automerge.Counter(1)))
    assert.isTrue(Document.isAutomergeValue({ a: { b: [1, "x", true, null] } }))
    assert.isFalse(Document.isAutomergeValue({ v: Number.NaN }))
    assert.isFalse(Document.isAutomergeValue({ v: 1n }))
    assert.isFalse(Document.isAutomergeValue(
      new (class Point {
        x = 1
      })()
    ))
    const circular: Record<string, unknown> = {}
    circular.self = circular
    assert.isFalse(Document.isAutomergeValue(circular))
  })
})
