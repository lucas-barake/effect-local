import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as Identity from "../src/Identity.js"
import * as Projection from "../src/Projection.js"

describe("Projection", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const Rows = Projection.make("TaskRows", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, title: Schema.String }),
    key: (row) => row.sourceDocumentId,
    project: (snapshot) => [{ sourceDocumentId: snapshot.documentId, title: snapshot.value.title }]
  })

  it.effect("projects validated rows with stable keys", () =>
    Effect.gen(function*() {
      const documentId = Identity.makeDocumentId()
      const rows = yield* Projection.evaluate(Rows, {
        documentId,
        value: { title: "one" },
        version: 1,
        heads: [],
        tombstone: false,
        projection: "Ready"
      })
      assert.deepStrictEqual(rows, [{ sourceDocumentId: documentId, title: "one" }])
    }))

  it.effect("validates the decoded side of transforming row codecs", () =>
    Effect.gen(function*() {
      const Transformed = Projection.make("Transformed", {
        document: Task,
        version: 1,
        Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, priority: Schema.NumberFromString }),
        key: (row) => row.sourceDocumentId,
        project: (snapshot) => [{ sourceDocumentId: snapshot.documentId, priority: 1 }]
      })
      const documentId = Identity.makeDocumentId()
      const rows = yield* Projection.evaluate(Transformed, {
        documentId,
        value: { title: "one" },
        version: 1,
        heads: [],
        tombstone: false,
        projection: "Ready"
      })
      assert.strictEqual(rows[0]?.priority, 1)
    }))

  it("rejects duplicate row keys", () => {
    const documentId = Identity.makeDocumentId()
    assert.throws(() =>
      Projection.assertUniqueKeys(Rows, [
        { sourceDocumentId: documentId, title: "one" },
        { sourceDocumentId: documentId, title: "two" }
      ])
    )
  })
})
