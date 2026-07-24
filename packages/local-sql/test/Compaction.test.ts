import * as Automerge from "@automerge/automerge"
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
import * as Compaction from "../src/Compaction.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

describe("Compaction", () => {
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
  const RecoveryService = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(Base, Gate)))
  const CompactionService = Compaction.layer.pipe(Layer.provide(Layer.mergeAll(Base, Gate, RecoveryService)))
  const Services = Layer.mergeAll(Base, Gate, StoreService, RecoveryService, CompactionService)

  it.effect("publishes a checkpoint only when heads and commit sequence still match", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const prepared = yield* compaction.prepare(Task, documentId)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, created, staged)
      assert.isFalse(yield* compaction.publish(prepared))
      const rows = yield* sql`SELECT checkpoint_hash FROM effect_local_checkpoints`
      assert.deepStrictEqual(rows, [])
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("rejects prepared provenance that conflicts with durable history", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const prepared = yield* compaction.prepare(Task, documentId)
      const result = yield* Effect.exit(compaction.publish({
        ...prepared,
        writerProvenance: prepared.writerProvenance.map((entry) =>
          Object.assign({}, entry, { writerDefinitionHash: "forged-definition" })
        )
      }))
      assert.strictEqual(result._tag, "Failure")
      assert.deepStrictEqual(
        yield* sql`SELECT checkpoint_hash FROM effect_local_checkpoints WHERE document_id = ${documentId}`,
        []
      )
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("reuses an unchanged checkpoint after another document advances the commit sequence", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const firstDocumentId = yield* Identity.makeDocumentId
      const secondDocumentId = yield* Identity.makeDocumentId
      const first = yield* store.create(Task, firstDocumentId, { title: "one", labels: [] })
      const initial = yield* compaction.compact(Task, firstDocumentId)
      const second = yield* store.create(Task, secondDocumentId, { title: "two", labels: [] })
      const repeated = yield* compaction.compact(Task, firstDocumentId)
      assert.isTrue(repeated.published)
      assert.strictEqual(repeated.checkpoint.checkpointHash, initial.checkpoint.checkpointHash)
      InternalAutomerge.free(second.automerge)
      InternalAutomerge.free(first.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("rolls back checkpoint publication when the replica permit changes in the transaction", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const prepared = yield* compaction.prepare(Task, documentId)
      yield* sql`CREATE TRIGGER fence_checkpoint_publication
        AFTER UPDATE OF checkpoint_hash ON effect_local_documents
        BEGIN
          UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1;
        END`

      const result = yield* Effect.exit(compaction.publish(prepared))
      assert.strictEqual(result._tag, "Failure")
      const checkpoints = yield* sql`SELECT checkpoint_hash FROM effect_local_checkpoints`
      assert.deepStrictEqual(checkpoints, [])
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("retains one prior verified checkpoint", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      let durable = yield* store.create(Task, documentId, { title: "one", labels: [] })
      assert.isTrue((yield* compaction.compact(Task, documentId)).published)
      for (const title of ["two", "three"]) {
        const staged = yield* store.stage(durable, (draft) => {
          draft.title = title
        })
        const next = yield* store.persist(Task, documentId, durable, staged)
        InternalAutomerge.free(staged)
        InternalAutomerge.free(durable.automerge)
        durable = next
        assert.isTrue((yield* compaction.compact(Task, documentId)).published)
      }
      const rows = yield* sql<{ readonly verified: number }>`
        SELECT verified FROM effect_local_checkpoints WHERE document_id = ${documentId}
      `
      assert.strictEqual(rows.length, 2)
      assert.isTrue(rows.every((row) => row.verified === 1))
      InternalAutomerge.free(durable.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("deletes only changes dominated by the oldest retained checkpoint", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const firstHeads = created.materializedHeads
      yield* compaction.compact(Task, documentId)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, created, staged)
      yield* compaction.compact(Task, documentId)
      assert.strictEqual(yield* compaction.prune(documentId), 1)
      const rows = yield* sql<{ readonly change_hash: string }>`
        SELECT change_hash FROM effect_local_changes WHERE document_id = ${documentId}
      `
      assert.deepStrictEqual(rows.map((row) => row.change_hash), persisted.materializedHeads)
      const recovered = yield* recovery.recover(Task, documentId)
      assert.deepStrictEqual(recovered.snapshot.value, { title: "two", labels: [] })
      assert.isTrue(Automerge.hasHeads(recovered.automerge, [...firstHeads]))
      assert.strictEqual(Automerge.getChangesSince(recovered.automerge, [...firstHeads]).length, 1)
      InternalAutomerge.free(recovered.automerge)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("does not prune a change whose provenance conflicts with retained checkpoints", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      yield* compaction.compact(Task, documentId)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, created, staged)
      yield* compaction.compact(Task, documentId)
      yield* sql`UPDATE effect_local_changes
        SET writer_definition_hash = 'conflicting-definition'
        WHERE document_id = ${documentId} AND change_hash != ${persisted.materializedHeads[0]}`

      const result = yield* Effect.exit(compaction.prune(documentId))
      assert.strictEqual(result._tag, "Failure")
      const rows = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_changes WHERE document_id = ${documentId}
      `
      assert.strictEqual(rows[0]?.count, 2)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("recovers after interruption before checkpoint publication", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      yield* compaction.prepare(Task, documentId)
      const rows = yield* sql`SELECT checkpoint_hash FROM effect_local_checkpoints`
      assert.deepStrictEqual(rows, [])
      const recovered = yield* recovery.recover(Task, documentId)
      assert.deepStrictEqual(recovered.snapshot.value, { title: "one", labels: [] })
      InternalAutomerge.free(recovered.automerge)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("does not prune after canonical heads advance", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      yield* compaction.compact(Task, documentId)
      const second = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, created, second)
      yield* compaction.compact(Task, documentId)
      const third = yield* store.stage(persisted, (draft) => {
        draft.title = "three"
      })
      const current = yield* store.persist(Task, documentId, persisted, third)
      assert.strictEqual(yield* compaction.prune(documentId), 0)
      const rows = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_changes WHERE document_id = ${documentId}
      `
      assert.strictEqual(rows[0]?.count, 3)
      InternalAutomerge.free(current.automerge)
      InternalAutomerge.free(third)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(second)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))
})
