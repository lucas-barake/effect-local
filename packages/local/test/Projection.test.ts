import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Document from "../src/Document.js"
import * as Identity from "../src/Identity.js"
import * as Projection from "../src/Projection.js"

it.layer(NodeCrypto.layer)("Projection", (it) => {
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
      const documentId = yield* Identity.makeDocumentId
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
      const documentId = yield* Identity.makeDocumentId
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
    const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
    assert.throws(() =>
      Projection.assertUniqueKeys(Rows, [
        { sourceDocumentId: documentId, title: "one" },
        { sourceDocumentId: documentId, title: "two" }
      ])
    )
  })
})
