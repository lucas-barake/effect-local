import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { vi } from "vitest"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

describe("DocumentStore", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String, labels: Schema.Array(Schema.String) }),
    version: 1
  })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Base = Layer.merge(Database, Bootstrap)
  const Gate = ReplicaGate.layer.pipe(Layer.provide(Base))
  const StoreService = DocumentStore.layer.pipe(Layer.provide(Layer.merge(Base, Gate)))
  const Store = Layer.merge(Base, StoreService)

  it.effect("persists explicit changes and reconstructs canonical state", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
        draft.labels.push("local")
      })
      const persisted = yield* store.persist(Task, documentId, created, staged)
      assert.deepStrictEqual(persisted.snapshot.value, { title: "two", labels: ["local"] })
      assert.strictEqual(persisted.commitSequence, 2)
      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot, persisted.snapshot)
      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Store)))

  it.effect("frees the initialized automerge document when create fails", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      InternalAutomerge.free(created.automerge)
      const initSpy = vi.spyOn(InternalAutomerge, "initialize")
      // Reusing an existing documentId violates the effect_local_documents primary key,
      // failing the insert after the Automerge document is already initialized.
      const exit = yield* Effect.exit(store.create(Task, documentId, { title: "duplicate", labels: [] }))
      assert.strictEqual(exit._tag, "Failure")
      const leaked = initSpy.mock.results.at(-1)?.value as InternalAutomerge.AnyDocument | undefined
      initSpy.mockRestore()
      assert.isDefined(leaked)
      // A failed create must free the document it initialized; a freed document
      // throws on any access, a leaked one is still usable.
      assert.throws(() => InternalAutomerge.heads(leaked!))
    }).pipe(Effect.provide(Store)))

  it.effect("rolls application rows back with an outer transaction", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      yield* Effect.exit(sql.withTransaction(
        store.create(Task, documentId, { title: "rollback", labels: [] }).pipe(
          Effect.andThen(Effect.fail("rollback"))
        )
      ))
      const rows = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_documents WHERE document_id = ${documentId}
      `
      assert.strictEqual(rows[0]?.count, 0)
    }).pipe(Effect.provide(Store)))
})
