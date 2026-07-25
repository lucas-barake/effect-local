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

  /** Overwrites `title` `count` times through the real store, leaving one change per write. */
  const churn = (documentId: Identity.DocumentId, count: number) =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      let durable = yield* store.create(Task, documentId, { title: "write-0", labels: [] })
      for (let index = 1; index <= count; index++) {
        const staged = yield* store.stage(durable, (draft) => {
          draft.title = `write-${index}`
        })
        const next = yield* store.persist(Task, documentId, durable, staged)
        InternalAutomerge.free(staged)
        InternalAutomerge.free(durable.automerge)
        durable = next
      }
      InternalAutomerge.free(durable.automerge)
      return { title: `write-${count}`, labels: [] as ReadonlyArray<string> }
    })

  const checkpointsOf = (documentId: Identity.DocumentId) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      return yield* sql<{
        readonly bytes: Uint8Array
        readonly commit_sequence: number
        readonly lineage: string
        readonly verified: number
        readonly writer_provenance: string
      }>`SELECT bytes, commit_sequence, lineage, verified, writer_provenance
        FROM effect_local_checkpoints WHERE document_id = ${documentId}`
    })

  const changesOf = (documentId: Identity.DocumentId) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      return yield* sql<{
        readonly applied: number
        readonly change_hash: string
        readonly commit_sequence: number
        readonly peer_id: string | null
      }>`SELECT applied, change_hash, commit_sequence, peer_id
        FROM effect_local_changes WHERE document_id = ${documentId}`
    })

  const documentRowOf = (documentId: Identity.DocumentId) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const rows = yield* sql<{
        readonly lineage: string
        readonly projection_status: string
        readonly tombstone: number
      }>`SELECT lineage, projection_status, tombstone
        FROM effect_local_documents WHERE document_id = ${documentId}`
      return rows[0]!
    })

  /**
   * Every column of the one checkpoint a rewritten document keeps. Whole-row comparison is what
   * separates "the rewrite ran once" from "it ran twice and again left one checkpoint behind".
   */
  const rewrittenCheckpointOf = (documentId: Identity.DocumentId) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const rows = yield* sql<{
        readonly bytes: Uint8Array
        readonly checkpoint_hash: string
        readonly checksum: string
        readonly commit_sequence: number
        readonly heads: string
        readonly lineage: string
        readonly verified: number
        readonly writer_provenance: string
      }>`SELECT bytes, checkpoint_hash, checksum, commit_sequence, heads, lineage, verified, writer_provenance
        FROM effect_local_checkpoints WHERE document_id = ${documentId} ORDER BY checkpoint_hash`
      assert.strictEqual(rows.length, 1)
      return rows[0]!
    })

  /** The durable rewrite markers, which is where a second rewrite of one request would be visible. */
  const rewriteMarkers = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    return yield* sql<{
      readonly document_id: string
      readonly lineage: string
      readonly operation_id: string
      readonly replica_incarnation: number
    }>`SELECT replica_incarnation, operation_id, document_id, lineage
      FROM effect_local_history_rewrites ORDER BY replica_incarnation, rewritten_at, operation_id`
  })

  const commitOutboxOf = (documentId: Identity.DocumentId) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      return yield* sql<{
        readonly commit_sequence: number
        readonly document_id: string
        readonly invalidation_keys: string
        readonly published: number
      }>`SELECT commit_sequence, document_id, invalidation_keys, published
        FROM effect_local_commit_outbox
        WHERE document_id = ${documentId}
        ORDER BY commit_sequence`
    })

  /**
   * The operator request every single-rewrite test below serves. Each test builds its own in-memory
   * database, so one id is unambiguous across them; the tests that need a second request name their
   * own.
   */
  const operationId = Compaction.OperationId.make("rewrite-history")

  /** The change hashes a saved Automerge document actually contains. */
  const changeHashesOf = (bytes: Uint8Array) => {
    const loaded = Automerge.load(bytes)
    try {
      return Automerge.getAllChanges(loaded).map((change) => Automerge.decodeChange(change).hash)
    } finally {
      Automerge.free(loaded)
    }
  }

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

  it.effect("rewrites a high-churn document down to a single change and checkpoint", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const value = yield* churn(documentId, 200)
      assert.isTrue((yield* compaction.compact(Task, documentId)).published)
      const churned = (yield* checkpointsOf(documentId))[0]!
      assert.strictEqual((yield* changesOf(documentId)).length, 201)

      // The floor a single write costs: the same value written once into a fresh document and
      // checkpointed. A rewrite that still carried the discarded history would sit far above it.
      const floorDocumentId = yield* Identity.makeDocumentId
      const floor = yield* store.create(Task, floorDocumentId, value)
      InternalAutomerge.free(floor.automerge)
      yield* compaction.compact(Task, floorDocumentId)
      const floorBytes = (yield* checkpointsOf(floorDocumentId))[0]!.bytes.byteLength

      const lineage = yield* compaction.rewriteHistory(Task, documentId, operationId)
      assert.notStrictEqual(lineage, "")

      const checkpoints = yield* checkpointsOf(documentId)
      assert.strictEqual(checkpoints.length, 1)
      const rewritten = checkpoints[0]!
      assert.strictEqual(rewritten.verified, 1)
      assert.strictEqual(rewritten.lineage, lineage)
      assert.isBelow(rewritten.bytes.byteLength, churned.bytes.byteLength)
      // Measured: 1928 bytes churned, 204 rewritten, 204 for the single write floor. The rewrite
      // lands exactly on the floor, because it is one change setting the same two root fields.
      assert.isAtMost(rewritten.bytes.byteLength, floorBytes)

      const changes = yield* changesOf(documentId)
      assert.strictEqual(changes.length, 1)
      assert.strictEqual(changes[0]!.applied, 1)
      assert.strictEqual(changes[0]!.peer_id, null)
      assert.strictEqual(changes[0]!.commit_sequence, rewritten.commit_sequence)
      assert.deepStrictEqual(
        (yield* commitOutboxOf(documentId)).filter((row) => row.commit_sequence === rewritten.commit_sequence),
        [{
          commit_sequence: rewritten.commit_sequence,
          document_id: documentId as string,
          invalidation_keys: JSON.stringify([Task.name]),
          published: 0
        }]
      )

      const hashes = changeHashesOf(rewritten.bytes)
      assert.strictEqual(hashes.length, 1)
      assert.deepStrictEqual(hashes, [changes[0]!.change_hash])
      const provenance = JSON.parse(rewritten.writer_provenance) as ReadonlyArray<{ readonly changeHash: string }>
      assert.strictEqual(provenance.length, 1)
      assert.strictEqual(provenance[0]!.changeHash, changes[0]!.change_hash)

      const row = yield* documentRowOf(documentId)
      assert.strictEqual(row.lineage, lineage)
      assert.notStrictEqual(row.lineage, "")

      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, value)
      assert.isFalse(reloaded.snapshot.tombstone)
      InternalAutomerge.free(reloaded.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("rewrites a tombstoned document without resurrecting it", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const executor = yield* CommandExecutor.CommandExecutor
      const gate = yield* ReplicaGate.ReplicaGate
      const recovery = yield* Recovery.Recovery
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      InternalAutomerge.free(created.automerge)
      // Deleted through the real command path so the tombstone is a durable Automerge change, not a
      // hand written row.
      const permit = yield* gate.current
      const commandId = yield* Identity.makeCommandId
      yield* executor.delete(Task, {
        commandId,
        documentId,
        permit,
        requestHash: yield* CommandExecutor.deleteRequestHash({
          incarnation: permit.incarnation,
          commandId,
          document: Task,
          documentId
        })
      })
      assert.strictEqual((yield* documentRowOf(documentId)).tombstone, 1)

      yield* compaction.rewriteHistory(Task, documentId, operationId)

      assert.strictEqual((yield* documentRowOf(documentId)).tombstone, 1)
      const recovered = yield* recovery.recover(Task, documentId)
      assert.isTrue(recovered.snapshot.tombstone)
      assert.isTrue(InternalAutomerge.tombstone(recovered.automerge))
      InternalAutomerge.free(recovered.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("refuses to rewrite while materialized and accepted heads are diverged", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const sql = yield* SqlClient.SqlClient
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      InternalAutomerge.free(created.automerge)
      yield* compaction.compact(Task, documentId)
      yield* sql`UPDATE effect_local_documents
        SET accepted_heads = ${"[\"0000000000000000000000000000000000000000000000000000000000000000\"]"}
        WHERE document_id = ${documentId}`

      const error = yield* Effect.flip(compaction.rewriteHistory(Task, documentId, operationId))
      assert.strictEqual(error.reason._tag, "StorageCorrupt")
      assert.strictEqual((yield* checkpointsOf(documentId)).length, 1)
      assert.strictEqual((yield* changesOf(documentId)).length, 1)
      assert.strictEqual((yield* documentRowOf(documentId)).lineage, "")
    }).pipe(Effect.provide(Services)))

  it.effect("refuses to rewrite while an unapplied change row exists", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const sql = yield* SqlClient.SqlClient
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      InternalAutomerge.free(created.automerge)
      yield* compaction.compact(Task, documentId)
      // An accepted peer change this replica cannot apply yet. Recovery skips it, so only the
      // rewrite's own guard can see that the canonical history is still incomplete.
      yield* sql`INSERT INTO effect_local_changes (
        change_hash, document_id, document_type, writer_schema_version, writer_definition_hash,
        actor, sequence, dependencies, bytes, applied, peer_id, accepted_at, commit_sequence
      ) VALUES (
        ${"a".repeat(64)}, ${documentId}, ${"Task"}, ${1}, ${"peer-definition"},
        ${"deadbeefdeadbeefdeadbeefdeadbeef"}, ${1}, ${"[]"}, ${new Uint8Array([1, 2, 3])}, ${0},
        ${"peer_00000000-0000-4000-8000-000000000000"}, ${"2020-01-01T00:00:00.000Z"}, ${1}
      )`

      const error = yield* Effect.flip(compaction.rewriteHistory(Task, documentId, operationId))
      assert.strictEqual(error.reason._tag, "StorageCorrupt")
      assert.strictEqual((yield* checkpointsOf(documentId)).length, 1)
      assert.strictEqual((yield* changesOf(documentId)).length, 2)
      assert.strictEqual((yield* documentRowOf(documentId)).lineage, "")
    }).pipe(Effect.provide(Services)))

  it.effect("refuses to rewrite while a peer receipt still holds an undecoded pending message", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      InternalAutomerge.free(created.automerge)
      yield* compaction.compact(Task, documentId)
      // An orphan peer change: accepted, but its dependency never arrived, so it produced no change
      // row at all. The `pending_message` blob is its only durable record, and the rewrite deletes
      // the receipt table, so a guard that only counted unapplied change rows would lose the write.
      const permit = yield* gate.current
      yield* sql`INSERT INTO effect_local_peer_receipts (
        replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id, message_hash,
        reply, reply_hash, pending_message, heads, accepted_heads, commit_sequence, accepted_at,
        writer_provenance
      ) VALUES (
        ${permit.incarnation}, ${"peer_00000000-0000-4000-8000-000000000000"}, ${"epoch-1"}, ${1},
        ${documentId}, ${"b".repeat(64)}, NULL, NULL, ${new Uint8Array([9, 9, 9])}, ${"[]"}, ${"[]"},
        ${1}, ${"2020-01-01T00:00:00.000Z"}, ${"[]"}
      )`

      const error = yield* Effect.flip(compaction.rewriteHistory(Task, documentId, operationId))
      assert.strictEqual(error.reason._tag, "StorageCorrupt")
      const retained = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_peer_receipts WHERE document_id = ${documentId}
      `
      assert.strictEqual(retained[0]!.count, 1)
      assert.strictEqual((yield* documentRowOf(documentId)).lineage, "")
    }).pipe(Effect.provide(Services)))

  it.effect("refuses to rewrite while a pending peer outbox row is unsent", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      InternalAutomerge.free(created.automerge)
      yield* compaction.compact(Task, documentId)
      const permit = yield* gate.current
      yield* sql`INSERT INTO effect_local_peer_outbox (
        replica_incarnation, peer_id, connection_epoch, document_id, send_sequence, message,
        message_hash, heads, status, created_at, writer_provenance, lineage
      ) VALUES (
        ${permit.incarnation}, ${"peer_00000000-0000-4000-8000-000000000000"}, ${"epoch-1"},
        ${documentId}, ${1}, ${new Uint8Array([7, 7, 7])}, ${"c".repeat(64)}, ${"[]"}, ${"Pending"},
        ${"2020-01-01T00:00:00.000Z"}, ${"[]"}, ${""}
      )`

      const error = yield* Effect.flip(compaction.rewriteHistory(Task, documentId, operationId))
      assert.strictEqual(error.reason._tag, "StorageCorrupt")
      const retained = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_peer_outbox WHERE document_id = ${documentId}
      `
      assert.strictEqual(retained[0]!.count, 1)
      assert.strictEqual((yield* documentRowOf(documentId)).lineage, "")
    }).pipe(Effect.provide(Services)))

  it.effect("refuses to rewrite when the document advances between recovery and the transaction", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const sql = yield* SqlClient.SqlClient
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      InternalAutomerge.free(created.automerge)
      yield* compaction.compact(Task, documentId)
      const changesBefore = yield* changesOf(documentId)
      // A concurrent writer advancing the document after `recover` committed its own transaction and
      // before the rewrite opens its own. Expressed as a trigger on the permit validation both of
      // those transactions run, so it lands in exactly that window. Both head columns move together,
      // so the settled-history guard still passes and only the compare-and-swap can catch it: the
      // rewrite would otherwise commit a document rebuilt from the value it read before the advance.
      // `sql.unsafe` because SQLite rejects bound parameters inside a trigger body.
      const advanced = `'["${"d".repeat(64)}"]'`
      yield* sql.unsafe(`CREATE TRIGGER advance_during_rewrite
        AFTER UPDATE OF writer_generation ON effect_local_metadata
        BEGIN
          UPDATE effect_local_documents
            SET materialized_heads = ${advanced}, accepted_heads = ${advanced}
            WHERE document_id = '${documentId}';
        END`)

      const error = yield* Effect.flip(compaction.rewriteHistory(Task, documentId, operationId))
      assert.strictEqual(error.reason._tag, "StorageCorrupt")
      assert.strictEqual((yield* checkpointsOf(documentId)).length, 1)
      assert.deepStrictEqual(
        (yield* changesOf(documentId)).map((row) => row.change_hash),
        changesBefore.map((row) => row.change_hash)
      )
      assert.strictEqual((yield* documentRowOf(documentId)).lineage, "")
    }).pipe(Effect.provide(Services)))

  it.effect("rolls back the whole rewrite when the replica permit changes in the transaction", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const sql = yield* SqlClient.SqlClient
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const value = yield* churn(documentId, 3)
      yield* compaction.compact(Task, documentId)
      const checkpointsBefore = yield* checkpointsOf(documentId)
      const changesBefore = yield* changesOf(documentId)
      const outboxBefore = yield* commitOutboxOf(documentId)
      const markersBefore = yield* rewriteMarkers
      yield* sql`CREATE TRIGGER fence_history_rewrite
        AFTER UPDATE OF checkpoint_hash ON effect_local_documents
        BEGIN
          UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1;
        END`

      const error = yield* Effect.flip(compaction.rewriteHistory(Task, documentId, operationId))
      assert.strictEqual(error.reason._tag, "ReplicaFenced")
      assert.deepStrictEqual(
        (yield* checkpointsOf(documentId)).map((row) => row.commit_sequence),
        checkpointsBefore.map((row) => row.commit_sequence)
      )
      assert.deepStrictEqual(
        (yield* changesOf(documentId)).map((row) => row.change_hash).toSorted(),
        changesBefore.map((row) => row.change_hash).toSorted()
      )
      assert.strictEqual((yield* documentRowOf(documentId)).lineage, "")
      assert.deepStrictEqual(yield* commitOutboxOf(documentId), outboxBefore)
      assert.deepStrictEqual(yield* rewriteMarkers, markersBefore)
      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, value)
      InternalAutomerge.free(reloaded.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("recovers a rewritten document from its checkpoint and from its change row alone", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const recovery = yield* Recovery.Recovery
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const value = yield* churn(documentId, 20)
      yield* compaction.compact(Task, documentId)
      yield* compaction.rewriteHistory(Task, documentId, operationId)

      const fromCheckpoint = yield* recovery.recover(Task, documentId)
      assert.deepStrictEqual(fromCheckpoint.snapshot.value, value)
      assert.strictEqual(Automerge.getAllChanges(fromCheckpoint.automerge).length, 1)
      InternalAutomerge.free(fromCheckpoint.automerge)

      // The rows-only tail. Dropping the single checkpoint leaves the re-rooted change row as the
      // only source, which is exactly why the rewrite writes one.
      yield* sql`DELETE FROM effect_local_checkpoints WHERE document_id = ${documentId}`
      const fromChanges = yield* recovery.recover(Task, documentId)
      assert.deepStrictEqual(fromChanges.snapshot.value, value)
      assert.strictEqual(Automerge.getAllChanges(fromChanges.automerge).length, 1)
      InternalAutomerge.free(fromChanges.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("performs exactly one rewrite when the same operation id is replayed", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const value = yield* churn(documentId, 20)
      yield* compaction.compact(Task, documentId)

      const lineage = yield* compaction.rewriteHistory(Task, documentId, operationId)
      const rewritten = yield* rewrittenCheckpointOf(documentId)
      const changes = yield* changesOf(documentId)

      // The hazard the marker closes, reproduced at the seam it guards: the activity re-running with
      // the same arguments after its own transaction already committed, with no workflow in between
      // to dedupe it.
      const replayed = yield* compaction.rewriteHistory(Task, documentId, operationId)

      assert.strictEqual(replayed, lineage)
      // Byte identical, not merely "still one checkpoint": a second rewrite also leaves exactly one,
      // with a new lineage, new bytes, a new hash and a new commit sequence. Comparing the whole row
      // is what makes a hidden second rewrite impossible to pass.
      assert.deepStrictEqual(yield* rewrittenCheckpointOf(documentId), rewritten)
      assert.deepStrictEqual(yield* changesOf(documentId), changes)
      assert.strictEqual((yield* documentRowOf(documentId)).lineage, lineage)
      assert.deepStrictEqual(
        (yield* rewriteMarkers).map((row) => ({ document_id: row.document_id, lineage: row.lineage })),
        [{ document_id: documentId as string, lineage: lineage as string }]
      )

      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, value)
      InternalAutomerge.free(reloaded.automerge)
    }).pipe(Effect.provide(Services)))

  it.effect("rewrites a second time under a different operation id", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const documentId = yield* Identity.makeDocumentId
      yield* churn(documentId, 20)
      yield* compaction.compact(Task, documentId)

      const first = yield* compaction.rewriteHistory(Task, documentId, operationId)
      const firstCheckpoint = yield* rewrittenCheckpointOf(documentId)
      const second = yield* compaction.rewriteHistory(
        Task,
        documentId,
        Compaction.OperationId.make("rewrite-history-again")
      )

      assert.notStrictEqual(second, first)
      const secondCheckpoint = yield* rewrittenCheckpointOf(documentId)
      assert.strictEqual(secondCheckpoint.lineage, second)
      assert.notStrictEqual(secondCheckpoint.checkpoint_hash, firstCheckpoint.checkpoint_hash)
      assert.isAbove(secondCheckpoint.commit_sequence, firstCheckpoint.commit_sequence)
      assert.strictEqual((yield* documentRowOf(documentId)).lineage, second)
      assert.deepStrictEqual((yield* rewriteMarkers).map((row) => row.lineage), [first as string, second as string])
    }).pipe(Effect.provide(Services)))

  it.effect("refuses a stale operation marker after a later rewrite", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      yield* churn(documentId, 20)
      yield* compaction.compact(Task, documentId)

      yield* compaction.rewriteHistory(Task, documentId, operationId)
      const currentLineage = yield* compaction.rewriteHistory(
        Task,
        documentId,
        Compaction.OperationId.make("rewrite-history-again")
      )
      const checkpointBefore = yield* rewrittenCheckpointOf(documentId)
      const changesBefore = yield* changesOf(documentId)
      const markersBefore = yield* rewriteMarkers
      const metadataBefore = yield* sql<{ readonly commit_sequence: number }>`
        SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`

      const error = yield* Effect.flip(compaction.rewriteHistory(Task, documentId, operationId))

      assert.strictEqual(error.reason._tag, "StorageCorrupt")
      assert.strictEqual((yield* documentRowOf(documentId)).lineage, currentLineage)
      assert.deepStrictEqual(yield* rewrittenCheckpointOf(documentId), checkpointBefore)
      assert.deepStrictEqual(yield* changesOf(documentId), changesBefore)
      assert.deepStrictEqual(yield* rewriteMarkers, markersBefore)
      assert.deepStrictEqual(
        yield* sql<{ readonly commit_sequence: number }>`
          SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`,
        metadataBefore
      )
    }).pipe(Effect.provide(Services)))

  it.effect("stamps the document's current lineage on a checkpoint published after a rewrite", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      InternalAutomerge.free(created.automerge)
      const lineage = yield* compaction.rewriteHistory(Task, documentId, operationId)
      assert.notStrictEqual(lineage, Identity.genesisLineage)

      // A real write between the rewrite and the compaction, so `publish` installs a NEW checkpoint
      // row rather than colliding with the one the rewrite already wrote under the new lineage.
      const durable = yield* store.load(Task, documentId)
      const staged = yield* store.stage(durable, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, durable, staged)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(durable.automerge)

      assert.isTrue((yield* compaction.compact(Task, documentId)).published)

      const checkpoints = yield* checkpointsOf(documentId)
      assert.strictEqual(checkpoints.length, 2)
      // Every retained checkpoint names the lineage its document is actually on. A genesis label
      // here would make the document and its own checkpoints disagree about which history they
      // belong to.
      assert.deepStrictEqual([...new Set(checkpoints.map((row) => row.lineage))], [lineage as string])
      assert.strictEqual((yield* documentRowOf(documentId)).lineage, lineage)
    }).pipe(Effect.provide(Services)))

  it.effect("prunes only within the document's current lineage", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const sql = yield* SqlClient.SqlClient
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      yield* compaction.compact(Task, documentId)
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      const persisted = yield* store.persist(Task, documentId, created, staged)
      yield* compaction.compact(Task, documentId)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
      assert.strictEqual((yield* checkpointsOf(documentId)).length, 2)

      // The document moves to a new lineage without its checkpoints following it. This is exactly
      // the state an unstamped `publish` used to leave behind, and the pair `prune` would otherwise
      // retain now straddles two histories.
      const lineage = yield* Identity.makeDocumentLineage
      yield* sql`UPDATE effect_local_documents SET lineage = ${lineage} WHERE document_id = ${documentId}`

      assert.strictEqual(yield* compaction.prune(documentId), 0)
      // Nothing was deleted, so the change the straddling pair would have dominated survives.
      assert.strictEqual((yield* changesOf(documentId)).length, 2)
    }).pipe(Effect.provide(Services)))

  it.effect("refuses to reuse an operation id for a different document", () =>
    Effect.gen(function*() {
      const compaction = yield* Compaction.Compaction
      const store = yield* DocumentStore.DocumentStore
      const rewrittenId = yield* Identity.makeDocumentId
      const otherId = yield* Identity.makeDocumentId
      yield* churn(rewrittenId, 5)
      const other = yield* store.create(Task, otherId, { title: "other", labels: [] })
      InternalAutomerge.free(other.automerge)
      yield* compaction.compact(Task, rewrittenId)
      yield* compaction.compact(Task, otherId)

      const lineage = yield* compaction.rewriteHistory(Task, rewrittenId, operationId)
      const otherCheckpoints = yield* checkpointsOf(otherId)
      const otherChanges = yield* changesOf(otherId)

      const error = yield* Effect.flip(compaction.rewriteHistory(Task, otherId, operationId))
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")

      // The second document keeps its history, its checkpoints and its genesis lineage: the reuse is
      // refused before anything destructive runs, not repaired afterwards.
      assert.deepStrictEqual(yield* checkpointsOf(otherId), otherCheckpoints)
      assert.deepStrictEqual(yield* changesOf(otherId), otherChanges)
      assert.strictEqual((yield* documentRowOf(otherId)).lineage, Identity.genesisLineage)
      assert.strictEqual((yield* documentRowOf(rewrittenId)).lineage, lineage)
      assert.deepStrictEqual((yield* rewriteMarkers).map((row) => row.document_id), [rewrittenId as string])
    }).pipe(Effect.provide(Services)))
})
