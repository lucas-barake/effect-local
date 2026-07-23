import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as Identity from "../src/Identity.js"

describe("Document migrations", () => {
  const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
  const V1 = Schema.Struct({ title: Schema.String })
  const V2 = Schema.Struct({ title: Schema.String, done: Schema.Boolean })
  const V3 = Schema.Struct({ title: Schema.String, done: Schema.Boolean, priority: Schema.Int })
  const Task = Document.make("Task", {
    schema: V3,
    version: 3,
    migrations: [
      Document.migration({ from: 1, schema: V1, migrate: (value) => ({ ...value, done: false }) }),
      Document.migration({ from: 2, schema: V2, migrate: (value) => ({ ...value, priority: 0 }) })
    ]
  })

  it.effect("decodes a stored value through the full migration chain", () =>
    Effect.gen(function*() {
      const value = yield* Document.decodeStored(Task, documentId, 1, { title: "write" })
      assert.deepStrictEqual(value, { title: "write", done: false, priority: 0 })
    }))

  it.effect("decodes a stored value starting mid chain", () =>
    Effect.gen(function*() {
      const value = yield* Document.decodeStored(Task, documentId, 2, { title: "write", done: true })
      assert.deepStrictEqual(value, { title: "write", done: true, priority: 0 })
    }))

  it.effect("decodes a stored value already at the current version without migrations", () =>
    Effect.gen(function*() {
      const value = yield* Document.decodeStored(Task, documentId, 3, { title: "write", done: true, priority: 2 })
      assert.deepStrictEqual(value, { title: "write", done: true, priority: 2 })
    }))

  it.effect("fails with UnsupportedDocumentVersion when the stored version predates the chain", () =>
    Effect.gen(function*() {
      const Partial = Document.make("Task", {
        schema: V3,
        version: 3,
        migrations: [Document.migration({ from: 2, schema: V2, migrate: (value) => ({ ...value, priority: 0 }) })]
      })
      const result = yield* Effect.result(Document.decodeStored(Partial, documentId, 1, { title: "write" }))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "UnsupportedDocumentVersion")
      }
    }))

  it.effect("fails with UnsupportedDocumentVersion when the stored version is newer than supported", () =>
    Effect.gen(function*() {
      const result = yield* Effect.result(
        Document.decodeStored(Task, documentId, 4, { title: "write", done: true, priority: 2 })
      )
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "UnsupportedDocumentVersion")
      }
    }))

  it.effect("fails with DocumentDecodeError when a migration throws", () =>
    Effect.gen(function*() {
      const Broken = Document.make("Task", {
        schema: V2,
        version: 2,
        migrations: [
          Document.migration({
            from: 1,
            schema: V1,
            migrate: () => {
              throw new Error("boom")
            }
          })
        ]
      })
      const result = yield* Effect.result(Document.decodeStored(Broken, documentId, 1, { title: "write" }))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "DocumentDecodeError")
      }
    }))

  it.effect("fails with DocumentDecodeError when a migration output does not satisfy the next schema", () =>
    Effect.gen(function*() {
      const Broken = Document.make("Task", {
        schema: V2,
        version: 2,
        migrations: [
          Document.migration({ from: 1, schema: V1, migrate: (value) => ({ title: value.title }) })
        ]
      })
      const result = yield* Effect.result(Document.decodeStored(Broken, documentId, 1, { title: "write" }))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "DocumentDecodeError")
      }
    }))

  it("rejects a migration chain with a gap", () => {
    assert.throws(
      () =>
        Document.make("Task", {
          schema: V3,
          version: 4,
          migrations: [
            Document.migration({ from: 1, schema: V1, migrate: (value) => value }),
            Document.migration({ from: 3, schema: V3, migrate: (value) => value })
          ]
        }),
      TypeError
    )
  })

  it("rejects duplicate migration source versions", () => {
    assert.throws(
      () =>
        Document.make("Task", {
          schema: V2,
          version: 2,
          migrations: [
            Document.migration({ from: 1, schema: V1, migrate: (value) => value }),
            Document.migration({ from: 1, schema: V1, migrate: (value) => value })
          ]
        }),
      TypeError
    )
  })

  it("rejects a migration source at or above the document version", () => {
    assert.throws(
      () =>
        Document.make("Task", {
          schema: V2,
          version: 2,
          migrations: [Document.migration({ from: 2, schema: V2, migrate: (value) => value })]
        }),
      TypeError
    )
  })

  it("reports which stored versions are supported", () => {
    assert.isTrue(Document.supportsStoredVersion(Task, 1))
    assert.isTrue(Document.supportsStoredVersion(Task, 2))
    assert.isTrue(Document.supportsStoredVersion(Task, 3))
    assert.isFalse(Document.supportsStoredVersion(Task, 4))
    assert.isFalse(Document.supportsStoredVersion(Task, 0))
    const Bare = Document.make("Note", { schema: V1, version: 2 })
    assert.isFalse(Document.supportsStoredVersion(Bare, 1))
    assert.isTrue(Document.supportsStoredVersion(Bare, 2))
  })
})
