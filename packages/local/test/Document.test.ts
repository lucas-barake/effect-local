import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as DocumentSet from "../src/DocumentSet.js"

describe("Document", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String, done: Schema.Boolean }),
    version: 1
  })

  it("preserves the schema and stable identity", () => {
    assert.strictEqual(Task.name, "Task")
    assert.strictEqual(Task.version, 1)
    assert.deepStrictEqual(Task.schema.make({ title: "one", done: false }), { title: "one", done: false })
  })

  it("rejects invalid names and versions", () => {
    assert.throws(() => Document.make("", { schema: Schema.String, version: 1 }))
    assert.throws(() => Document.make("Task", { schema: Schema.String, version: 0 }))
  })

  it("rejects unsupported Automerge values", () => {
    assert.isFalse(Document.isAutomergeValue({ value: undefined }))
    assert.isFalse(Document.isAutomergeValue({ value: () => undefined }))
    assert.isTrue(Document.isAutomergeValue({ value: [1, "ok", null] }))
  })

  it("rejects duplicate document names", () => {
    assert.throws(() => DocumentSet.make(Task, Task))
    assert.strictEqual(DocumentSet.get(DocumentSet.make(Task), "Task"), Task)
  })
})
