import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as Identity from "../src/Identity.js"
import * as Projection from "../src/Projection.js"

describe("Projection evaluate failures", () => {
  const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const snapshot = {
    documentId,
    value: { title: "one" },
    version: 1,
    heads: [],
    tombstone: false,
    projection: "Ready" as const
  }

  it.effect("wraps colliding keys as a tagged ProjectionBlocked reason", () =>
    Effect.gen(function*() {
      const Dupe = Projection.make("Dupe", {
        document: Task,
        version: 1,
        Row: Schema.Struct({ id: Schema.String }),
        key: (row) => row.id,
        project: () => [{ id: "x" }, { id: "x" }]
      })
      const error = yield* Projection.evaluate(Dupe, snapshot).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProjectionBlocked")
      if (error.reason._tag !== "ProjectionBlocked") return
      assert.strictEqual(error.reason.projection, "Dupe")
    }))

  it.effect("wraps invalid rows as a tagged ProjectionBlocked reason", () =>
    Effect.gen(function*() {
      const Invalid = Projection.make("Invalid", {
        document: Task,
        version: 1,
        Row: Schema.Struct({ id: Schema.String }),
        key: (row) => row.id,
        project: () => [{ id: 1 } as never]
      })
      const error = yield* Projection.evaluate(Invalid, snapshot).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, "ProjectionBlocked")
    }))

  it("rejects an empty name and a nonpositive version", () => {
    const options = {
      document: Task,
      version: 1,
      Row: Schema.Struct({ id: Schema.String }),
      key: (row: { id: string }) => row.id,
      project: () => []
    }
    assert.throws(() => Projection.make("", options))
    assert.throws(() => Projection.make("P", { ...options, version: 0 }))
  })
})
