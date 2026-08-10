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
import { withGateLimits } from "./fixtures/limits.js"

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
  const Gate = ReplicaGate.layer.pipe(withGateLimits, Layer.provide(Base))
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
      yield* sql`UPDATE effect_local_changes
        SET writer_schema_version = 9, writer_definition_hash = 'historical-definition'
        WHERE document_id = ${documentId}
          AND sequence = (
            SELECT MAX(sequence) FROM effect_local_changes WHERE document_id = ${documentId}
          )`
      const latest = yield* compaction.compact(Task, documentId)
      yield* compaction.prune(documentId)
      yield* sql`UPDATE effect_local_checkpoints SET bytes = ${new Uint8Array([1, 2, 3])}
        WHERE checkpoint_hash = ${latest.checkpoint.checkpointHash}`
      const recovered = yield* recovery.recover(Task, documentId)
      assert.deepStrictEqual(recovered.snapshot.value, { title: "two" })
      assert.deepStrictEqual(recovered.materializedHeads, persisted.materializedHeads)
      const provenance = yield* sql<{
        readonly writer_definition_hash: string
        readonly writer_schema_version: number
      }>`SELECT writer_definition_hash, writer_schema_version
        FROM effect_local_changes
        WHERE document_id = ${documentId}
        ORDER BY sequence`
      assert.deepStrictEqual(provenance, [
        { writer_definition_hash: "historical-definition", writer_schema_version: 9 }
      ])
      const rows = yield* sql<{ readonly verified: number }>`
        SELECT verified FROM effect_local_checkpoints WHERE checkpoint_hash = ${latest.checkpoint.checkpointHash}
      `
      assert.strictEqual(rows[0]?.verified, 0)
      InternalAutomerge.free(recovered.automerge)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("demotes a checkpoint whose lineage does not match its document row", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const checkpointsOf = (documentId: Identity.DocumentId) =>
        sql<{ readonly lineage: string; readonly verified: number }>`
          SELECT lineage, verified FROM effect_local_checkpoints WHERE document_id = ${documentId}
        `
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      InternalAutomerge.free(created.automerge)
      const lineage = yield* compaction.rewriteHistory(
        Task,
        documentId,
        Compaction.OperationId.make("rewrite-history")
      )

      // A rewrite leaves the document and its checkpoint on one lineage, and recovery replays that
      // checkpoint and leaves it verified. This half is what keeps the check below from demoting
      // every legitimate checkpoint, genesis ones included.
      const agreeing = yield* recovery.recover(Task, documentId)
      assert.deepStrictEqual(agreeing.snapshot.value, { title: "one" })
      InternalAutomerge.free(agreeing.automerge)
      assert.deepStrictEqual(yield* checkpointsOf(documentId), [{ lineage, verified: 1 }])

      // A checkpoint left on the genesis lineage under a rewritten document. Replaying it would
      // rebuild the document from a history it no longer belongs to, and its heads happen to match
      // because it is the very checkpoint the rewrite wrote.
      yield* sql`UPDATE effect_local_checkpoints SET lineage = ${Identity.genesisLineage}
        WHERE document_id = ${documentId}`

      const recovered = yield* recovery.recover(Task, documentId)
      assert.deepStrictEqual(recovered.snapshot.value, { title: "one" })
      InternalAutomerge.free(recovered.automerge)
      // Demoted through the ordinary invalidation path rather than quarantined: the document itself
      // is intact and its change rows still rebuild it.
      assert.deepStrictEqual(yield* checkpointsOf(documentId), [{ lineage: "", verified: 0 }])
      const quarantine = yield* sql`SELECT reason FROM effect_local_quarantine WHERE document_id = ${documentId}`
      assert.deepStrictEqual(quarantine, [])
    }).pipe(Effect.provide(Services)))

  it.effect("rejects provenance conflicts between a checkpoint and surviving changes", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      yield* compaction.compact(Task, documentId)
      yield* sql`UPDATE effect_local_changes
        SET writer_definition_hash = 'conflicting-definition'
        WHERE document_id = ${documentId}`

      const result = yield* Effect.result(recovery.recover(Task, documentId))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
      }
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

  it.effect("rejects partially null durable history counters as storage corruption", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      yield* sql`UPDATE effect_local_documents SET history_operations = NULL
        WHERE document_id = ${documentId}`

      const result = yield* Effect.result(recovery.recover(Task, documentId))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
      } else {
        InternalAutomerge.free(result.success.automerge)
      }
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("rejects numeric history counters that do not match complete retained history", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      yield* sql`UPDATE effect_local_documents SET history_operations = history_operations + 1
        WHERE document_id = ${documentId}`

      const result = yield* Effect.result(recovery.recover(Task, documentId))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
      } else {
        InternalAutomerge.free(result.success.automerge)
      }
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("recovers complete retained history without a checkpoint", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, created, staged)
      const recovered = yield* recovery.recover(Task, documentId)

      assert.deepStrictEqual(
        yield* sql`SELECT checkpoint_hash FROM effect_local_checkpoints WHERE document_id = ${documentId}`,
        []
      )
      assert.deepStrictEqual(recovered.snapshot.value, { title: "two" })
      assert.deepStrictEqual(recovered.snapshot.heads, persisted.materializedHeads)
      assert.deepStrictEqual(recovered.materializedHeads, persisted.materializedHeads)
      assert.deepStrictEqual(recovered.acceptedHeads, persisted.acceptedHeads)
      assert.strictEqual(recovered.historyChanges, persisted.historyChanges)
      assert.strictEqual(recovered.historyOperations, persisted.historyOperations)
      assert.strictEqual(recovered.historyBytes, persisted.historyBytes)
      assert.deepStrictEqual(
        yield* sql`SELECT reason FROM effect_local_quarantine WHERE document_id = ${documentId}`,
        []
      )
      InternalAutomerge.free(recovered.automerge)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Services)))

  // The absent row and an undecodable one are different faults and must not collapse. This one is
  // replica-wide: reporting it as `StorageCorrupt` tells `ReplicaEvolution` to quarantine the one
  // document it happened to be reading, and `BackupStore` to call the operator's backup invalid.
  it.effect("reports a missing metadata singleton as a replica-wide failure, not document corruption", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })

      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`

      const result = yield* Effect.result(recovery.recover(Task, documentId)).pipe(
        Effect.ensuring(Effect.sync(() => InternalAutomerge.free(created.automerge)))
      )

      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
      }
    }).pipe(Effect.provide(Services)))

  // Precedence, pinned deliberately: the replica-wide answer wins over the per-document one, because
  // `gate.validate` guards the same transaction. Answering `DocumentNotFound` on a replica that has
  // lost its identity would invite the caller to create the document again.
  it.effect("reports the replica-wide failure over DocumentNotFound when the singleton is gone", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`
      const result = yield* Effect.result(recovery.recover(Task, documentId))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
      }
    }).pipe(Effect.provide(Services)))

  it.effect("reports invalid commit sequence metadata as storage corruption", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })

      yield* sql`UPDATE effect_local_metadata
        SET commit_sequence = -1
        WHERE singleton = 1`

      const result = yield* Effect.result(recovery.recover(Task, documentId)).pipe(
        Effect.ensuring(Effect.sync(() => InternalAutomerge.free(created.automerge)))
      )

      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
      }
    }).pipe(Effect.provide(Services)))

  it.effect("rejects mismatched required change document type", () =>
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })

      yield* sql`UPDATE effect_local_changes
        SET document_type = 'Other'
        WHERE document_id = ${documentId}`

      const result = yield* Effect.result(recovery.recover(Task, documentId))
      const quarantine = yield* sql<{ readonly reason: string }>`
        SELECT reason
        FROM effect_local_quarantine
        WHERE document_id = ${documentId}
      `

      if (Result.isSuccess(result)) {
        InternalAutomerge.free(result.success.automerge)
      }
      InternalAutomerge.free(created.automerge)

      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
      }
      assert.deepStrictEqual(quarantine, [{
        reason: "Canonical recovery failed"
      }])
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
