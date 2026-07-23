import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as Compaction from "../src/Compaction.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

describe("Recovery", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
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
  const RecoveryService = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(Base, Gate)))
  const CompactionService = Compaction.layer.pipe(Layer.provide(Layer.mergeAll(Base, Gate, RecoveryService)))
  const Services = Layer.mergeAll(Base, Gate, StoreService, RecoveryService, CompactionService)

  it.effect("reconstructs current heads from a retained prior checkpoint after corrupting the newest", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      yield* compaction.compact(Task, documentId)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, created, staged)
      const latest = yield* compaction.compact(Task, documentId)
      yield* compaction.prune(documentId)
      yield* sql`UPDATE effect_local_checkpoints SET bytes = ${new Uint8Array([1, 2, 3])}
        WHERE checkpoint_hash = ${latest.checkpoint.checkpointHash}`
      const recovered = yield* recovery.recover(Task, documentId)
      assert.deepStrictEqual(recovered.snapshot.value, { title: "two" })
      assert.deepStrictEqual(recovered.materializedHeads, persisted.materializedHeads)
      const rows = yield* sql<{ readonly verified: number }>`
        SELECT verified FROM effect_local_checkpoints WHERE checkpoint_hash = ${latest.checkpoint.checkpointHash}
      `
      assert.strictEqual(rows[0]?.verified, 0)
      InternalAutomerge.free(recovered.automerge)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("quarantines the document when a required change is corrupt", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      yield* sql`UPDATE effect_local_changes SET bytes = ${new Uint8Array([1, 2, 3])}
        WHERE document_id = ${documentId}`
      const exit = yield* recovery.recover(Task, documentId).pipe(Effect.exit)
      assert.isTrue(exit._tag === "Failure")
      const quarantine = yield* sql<{ readonly reason: string }>`
        SELECT reason FROM effect_local_quarantine WHERE document_id = ${documentId}
      `
      assert.strictEqual(quarantine.length, 1)
      const raw = yield* recovery.exportRaw(documentId)
      assert.strictEqual(raw.changes.length, 1)
      assert.deepStrictEqual(raw.changes[0]?.bytes, new Uint8Array([1, 2, 3]))
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("reports invalid local rows as storage corruption during raw export", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      yield* sql`PRAGMA ignore_check_constraints = ON`
      yield* sql`UPDATE effect_local_documents SET projection_status = 'Invalid'
        WHERE document_id = ${documentId}`
      const result = yield* Effect.result(recovery.exportRaw(documentId))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
        if (result.failure.reason._tag === "StorageCorrupt") {
          assert.isTrue(Schema.is(Schema.Error())(result.failure.reason.cause))
        }
      }
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("rolls back quarantine writes when the replica permit changes", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      yield* sql`UPDATE effect_local_changes SET bytes = ${new Uint8Array([1, 2, 3])}
        WHERE document_id = ${documentId}`
      yield* sql`CREATE TRIGGER fence_quarantine
        AFTER UPDATE OF projection_status ON effect_local_documents
        BEGIN
          UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1;
        END`

      const result = yield* Effect.exit(recovery.recover(Task, documentId))
      assert.strictEqual(result._tag, "Failure")
      const quarantine = yield* sql`SELECT reason FROM effect_local_quarantine WHERE document_id = ${documentId}`
      const documents = yield* sql<{ readonly projection_status: string }>`
        SELECT projection_status FROM effect_local_documents WHERE document_id = ${documentId}`
      assert.deepStrictEqual(quarantine, [])
      assert.strictEqual(documents[0]?.projection_status, "Ready")
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("fails with DocumentNotFound when recovering an unknown document", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const result = yield* Effect.flip(recovery.recover(Task, yield* Identity.makeDocumentId))
      assert.strictEqual(result.reason._tag, "DocumentNotFound")
    }).pipe(Effect.provide(Services)))

  it.effect("rejects a stored schema version newer than the definition", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      InternalAutomerge.free(created.automerge)
      yield* sql`UPDATE effect_local_documents SET schema_version = 999 WHERE document_id = ${documentId}`
      const result = yield* Effect.flip(recovery.recover(Task, documentId))
      assert.strictEqual(result.reason._tag, "UnsupportedDocumentVersion")
    }).pipe(Effect.provide(Services)))
})
