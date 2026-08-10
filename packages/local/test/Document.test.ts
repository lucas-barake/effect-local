import * as Automerge from "@automerge/automerge"
import * as OtherAutomerge from "@automerge/automerge/slim"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
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
    const counter = new OtherAutomerge.Counter(1)
    const immutableString = new OtherAutomerge.ImmutableString("one")

    assert.isTrue(Automerge.isCounter(counter))
    assert.isTrue(Automerge.isImmutableString(immutableString))
    assert.isTrue(Document.isAutomergeValue(counter))
    assert.isTrue(Document.isAutomergeValue(immutableString))
  })

  it("accepts only numbers and counters that remain lossless through Automerge persistence", () => {
    const values = [
      {
        name: "number minimum safe integer minus one",
        value: Number.MIN_SAFE_INTEGER - 1,
        expected: true
      },
      {
        name: "number negative maximum",
        value: -Number.MAX_VALUE,
        expected: true
      },
      {
        name: "number maximum safe integer minus one",
        value: Number.MAX_SAFE_INTEGER - 1,
        expected: true
      },
      {
        name: "number maximum safe integer",
        value: Number.MAX_SAFE_INTEGER,
        expected: false
      },
      {
        name: "number maximum",
        value: Number.MAX_VALUE,
        expected: false
      }
    ]

    for (const test of values) {
      const original = Automerge.from({ value: test.value })
      const loaded = Automerge.load<{ value: unknown }>(Automerge.save(original))
      Effect.runSync(Effect.acquireUseRelease(
        Effect.succeed({ original, loaded }),
        ({ loaded: currentLoaded }) =>
          Effect.sync(() => {
            assert.strictEqual(Document.isAutomergeValue(test.value), test.expected, test.name)
            if (test.expected) {
              assert.strictEqual(typeof currentLoaded.value, "number", test.name)
              assert.isTrue(Object.is(currentLoaded.value, test.value), test.name)
              assert.isTrue(Document.isAutomergeValue(currentLoaded.value), test.name)
            } else {
              assert.isFalse(
                typeof currentLoaded.value === "number" && Object.is(currentLoaded.value, test.value),
                test.name
              )
              assert.isFalse(Document.isAutomergeValue(currentLoaded.value), test.name)
            }
          }),
        ({ original: currentOriginal, loaded: currentLoaded }) =>
          Effect.sync(() => {
            Automerge.free(currentOriginal)
            Automerge.free(currentLoaded)
          })
      ))
    }

    const counters = [
      {
        name: "counter minimum safe integer",
        value: Number.MIN_SAFE_INTEGER,
        expected: true
      },
      {
        name: "counter maximum safe integer minus one",
        value: Number.MAX_SAFE_INTEGER - 1,
        expected: true
      },
      {
        name: "counter maximum safe integer",
        value: Number.MAX_SAFE_INTEGER,
        expected: false
      },
      {
        name: "counter minimum safe integer minus one",
        value: Number.MIN_SAFE_INTEGER - 1,
        expected: false
      }
    ]

    for (const test of counters) {
      const counter = new Automerge.Counter(test.value)
      const original = Automerge.from({ counter })
      const loaded = Automerge.load<{ counter: Automerge.Counter }>(Automerge.save(original))
      Effect.runSync(Effect.acquireUseRelease(
        Effect.succeed({ original, loaded }),
        ({ loaded: currentLoaded }) =>
          Effect.sync(() => {
            assert.strictEqual(Document.isAutomergeValue(counter), test.expected, test.name)
            if (test.expected) {
              assert.strictEqual(typeof currentLoaded.counter.value, "number", test.name)
              assert.isTrue(Object.is(currentLoaded.counter.value, test.value), test.name)
              assert.isTrue(Document.isAutomergeValue(currentLoaded.counter), test.name)
            } else {
              assert.isFalse(
                typeof currentLoaded.counter.value === "number" &&
                  Object.is(currentLoaded.counter.value, test.value),
                test.name
              )
              assert.isFalse(Document.isAutomergeValue(currentLoaded.counter), test.name)
            }
          }),
        ({ original: currentOriginal, loaded: currentLoaded }) =>
          Effect.sync(() => {
            Automerge.free(currentOriginal)
            Automerge.free(currentLoaded)
          })
      ))
    }

    assert.isFalse(Document.isAutomergeValue(Number.POSITIVE_INFINITY))
    assert.isFalse(Document.isAutomergeValue(Number.NEGATIVE_INFINITY))
    assert.isFalse(Document.isAutomergeValue(Number.NaN))
  })

  it("rejects sparse arrays that Automerge treats as undefined", () => {
    const items: Array<string> = []
    items.length = 1

    assert.throws(() => {
      const document = Automerge.from({ items })
      Automerge.free(document)
    })
    assert.isFalse(Document.isAutomergeValue({ items }))
  })

  it("rejects duplicate document names", () => {
    assert.throws(() => DocumentSet.make(Task, Task))
    assert.strictEqual(DocumentSet.get(DocumentSet.make(Task), "Task"), Task)
  })
})
