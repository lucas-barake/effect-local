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
import * as CommandExecutor from "../src/CommandExecutor.js"
import * as Compaction from "../src/Compaction.js"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import { withGateLimits } from "./fixtures/limits.js"

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
  const Gate = ReplicaGate.layer.pipe(withGateLimits, Layer.provide(Base))
  const StoreService = DocumentStore.layer.pipe(Layer.provide(Layer.merge(Base, Gate)))
  const RecoveryService = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(Base, Gate)))
  const CompactionService = Compaction.layer.pipe(Layer.provide(Layer.mergeAll(Base, Gate, RecoveryService)))
  const Projections = ProjectionStore.layer([]).pipe(Layer.provide(Base))
  // `definition` declares no mutations, so `MutationHandlers` resolves to `never` and the executor
  // needs no handler layer. It is here only to write real command receipts through production code.
  const Executor = CommandExecutor.layer(definition).pipe(
    Layer.provide(Layer.mergeAll(Base, Gate, StoreService, Projections))
  )
  const Services = Layer.mergeAll(Base, Gate, StoreService, RecoveryService, CompactionService, Executor)

  /** Commits a real create command under the current incarnation and reports the receipt it wrote. */
  const commitReceipt = Effect.gen(function*() {
    const executor = yield* CommandExecutor.CommandExecutor
    const gate = yield* ReplicaGate.ReplicaGate
    const permit = yield* gate.current
    const documentId = yield* Identity.makeDocumentId
    const commandId = yield* Identity.makeCommandId
    const value = { title: "one", labels: [] as ReadonlyArray<string> }
    const encoded = yield* Document.encode(Task, documentId, value)
    const requestHash = yield* CommandExecutor.createRequestHash({
      incarnation: permit.incarnation,
      commandId,
      document: Task,
      documentId,
      encoded
    })
    yield* executor.create(Task, { commandId, documentId, permit, requestHash, value })
    return { command_id: commandId as string, replica_incarnation: permit.incarnation as number }
  })

  const listReceipts = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    return yield* sql<{ readonly command_id: string; readonly replica_incarnation: number }>`
      SELECT command_id, replica_incarnation FROM effect_local_command_receipts
      ORDER BY replica_incarnation, command_id
    `
  })

  const byKey = (
    rows: ReadonlyArray<{ readonly command_id: string; readonly replica_incarnation: number }>
  ) =>
    rows.toSorted((left, right) =>
      left.replica_incarnation - right.replica_incarnation ||
      (left.command_id < right.command_id ? -1 : left.command_id > right.command_id ? 1 : 0)
    )

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
  it.effect("prunes every superseded incarnation, not only the previous one", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const gate = yield* ReplicaGate.ReplicaGate
      const oldest = yield* commitReceipt
      yield* gate.claim(() => Effect.void)
      const middle = yield* commitReceipt
      yield* gate.claim(() => Effect.void)
      const live = yield* commitReceipt
      assert.deepStrictEqual(
        [oldest.replica_incarnation, middle.replica_incarnation, live.replica_incarnation],
        [0, 1, 2]
      )

      assert.strictEqual(yield* compaction.pruneCommandReceipts, 2)
      assert.deepStrictEqual(yield* listReceipts, [live])
    }).pipe(Effect.provide(Services)))

  it.effect("keeps every receipt and reports zero on a replica that has never restored", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      // Deliberately no `gate.claim`. Every other prune test claims first, which raises the
      // incarnation past zero and skips the short circuit this covers: the never restored replica,
      // where the sweep must not touch storage and must report nothing reclaimed.
      const first = yield* commitReceipt
      const second = yield* commitReceipt
      assert.deepStrictEqual([first.replica_incarnation, second.replica_incarnation], [0, 0])

      assert.strictEqual(yield* compaction.pruneCommandReceipts, 0)
      assert.deepStrictEqual(yield* listReceipts, byKey([first, second]))
    }).pipe(Effect.provide(Services)))

  it.effect("is idempotent across runs", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const gate = yield* ReplicaGate.ReplicaGate
      yield* commitReceipt
      yield* gate.claim(() => Effect.void)
      const live = yield* commitReceipt

      assert.strictEqual(yield* compaction.pruneCommandReceipts, 1)
      assert.strictEqual(yield* compaction.pruneCommandReceipts, 0)
      assert.deepStrictEqual(yield* listReceipts, [live])
    }).pipe(Effect.provide(Services)))

  it.effect("reclaims an exact multiple of the batch size without losing or over-counting rows", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      // Claim first so the live receipt sits above zero and the seeded rows below it are superseded.
      yield* gate.claim(() => Effect.void)
      const live = yield* commitReceipt
      const permit = yield* gate.current
      const supersededIncarnation = permit.incarnation - 1
      assert.isAtLeast(supersededIncarnation, 0)
      // An exact multiple of the 512 row batch size, so the last batch that deletes anything returns
      // a full batch rather than a short one. Two batches then have to be followed by a third that
      // deletes nothing before the sweep may stop: exiting on `removed <= receiptPruneBatchSize`
      // would stop after the first batch and report half the rows as reclaimed.
      const supersededCount = 1024
      const result = new TextEncoder().encode("{}")
      // One transaction for the whole seed. Per-row transactions dominate this test's runtime.
      yield* sql.withTransaction(Effect.gen(function*() {
        for (let index = 0; index < supersededCount; index++) {
          yield* sql`INSERT INTO effect_local_command_receipts (
            replica_incarnation, command_id, request_hash, mutation_name, result,
            document_id, heads, commit_sequence
          ) VALUES (
            ${supersededIncarnation}, ${`superseded-${index}`}, ${`hash-${index}`}, ${"$create"},
            ${result}, ${`document-${index}`}, ${"[]"}, ${0}
          )`
        }
      }))

      assert.strictEqual(yield* compaction.pruneCommandReceipts, supersededCount)
      assert.deepStrictEqual(yield* listReceipts, [live])
      assert.strictEqual(yield* compaction.pruneCommandReceipts, 0)
      assert.deepStrictEqual(yield* listReceipts, [live])
    }).pipe(Effect.provide(Services)))

  it.effect("fails without deleting when the permit is fenced", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const superseded = yield* commitReceipt
      yield* gate.claim(() => Effect.void)
      const live = yield* commitReceipt
      // A concurrent writer claiming the epoch mid-batch, expressed as an in-transaction generation
      // bump so the delete and the fence land inside the same transaction.
      yield* sql`CREATE TRIGGER fence_receipt_prune
        AFTER DELETE ON effect_local_command_receipts
        BEGIN
          UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1;
        END`

      const error = yield* Effect.flip(compaction.pruneCommandReceipts)
      assert.strictEqual(error.reason._tag, "ReplicaFenced")
      assert.deepStrictEqual(yield* listReceipts, byKey([superseded, live]))
    }).pipe(Effect.provide(Services)))

  it.effect("reports storage unavailable when the receipt sweep cannot delete", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const superseded = yield* commitReceipt
      yield* gate.claim(() => Effect.void)
      const live = yield* commitReceipt
      // Storage refusing the sweep's own delete, expressed as a trigger that aborts the statement
      // from inside the batch transaction.
      yield* sql`CREATE TRIGGER block_receipt_prune
        BEFORE DELETE ON effect_local_command_receipts
        BEGIN
          SELECT RAISE(ABORT, 'storage down');
        END`

      const error = yield* Effect.flip(compaction.pruneCommandReceipts)
      assert.strictEqual(error.reason._tag, "StorageUnavailable")
      assert.deepStrictEqual(yield* listReceipts, byKey([superseded, live]))
    }).pipe(Effect.provide(Services)))
})
