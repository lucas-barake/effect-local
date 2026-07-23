import * as Automerge from "@automerge/automerge"
import { assert, describe, it } from "@effect/vitest"
import { createRequire } from "node:module"
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

  it("accepts Automerge scalar wrappers from another module instance", () => {
    const OtherAutomerge = createRequire(import.meta.url)("@automerge/automerge") as typeof Automerge
    const counter = new OtherAutomerge.Counter(1)
    const immutableString = new OtherAutomerge.ImmutableString("one")

    assert.isTrue(Automerge.isCounter(counter))
    assert.isTrue(Automerge.isImmutableString(immutableString))
    assert.isTrue(Document.isAutomergeValue(counter))
    assert.isTrue(Document.isAutomergeValue(immutableString))
  })

  it("rejects duplicate document names", () => {
    assert.throws(() => DocumentSet.make(Task, Task))
    assert.strictEqual(DocumentSet.get(DocumentSet.make(Task), "Task"), Task)
  })
})
