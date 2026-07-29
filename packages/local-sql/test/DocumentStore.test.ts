import * as Automerge from "@automerge/automerge"
import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import { vi } from "vitest"
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import { withGateLimits } from "./fixtures/limits.js"
import { makeProbe, probeLayer, withFault } from "./helpers/sqlProbe.js"

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
  const Gate = ReplicaGate.layer.pipe(withGateLimits, Layer.provide(Base))
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
      const sql = yield* SqlClient.SqlClient
      const changes = yield* sql<{ readonly bytes: Uint8Array }>`
        SELECT bytes FROM effect_local_changes
        WHERE document_id = ${documentId} AND applied = 1
      `
      assert.strictEqual(persisted.historyChanges, changes.length)
      assert.strictEqual(
        persisted.historyOperations,
        changes.reduce((total, change) => total + Automerge.decodeChange(change.bytes).ops.length, 0)
      )
      assert.strictEqual(
        persisted.historyBytes,
        changes.reduce((total, change) => total + change.bytes.byteLength, 0)
      )
      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot, persisted.snapshot)
      assert.strictEqual(reloaded.historyChanges, persisted.historyChanges)
      assert.strictEqual(reloaded.historyOperations, persisted.historyOperations)
      assert.strictEqual(reloaded.historyBytes, persisted.historyBytes)
      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Store)))

  it.effect("rejects extending an unmeasured history without durable writes", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      yield* sql`UPDATE effect_local_documents SET
        history_changes = NULL, history_operations = NULL, history_bytes = NULL
        WHERE document_id = ${documentId}`
      const unmeasured = yield* store.load(Task, documentId)
      const staged = yield* store.stage(unmeasured, (draft) => {
        draft.title = "two"
      })
      const before = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_changes WHERE document_id = ${documentId}
      `
      const result = yield* Effect.result(store.persist(Task, documentId, unmeasured, staged))
      assert.strictEqual(result._tag, "Failure")
      if (result._tag === "Failure") {
        assert.strictEqual(result.failure.reason._tag, "QuotaExceeded")
        if (result.failure.reason._tag === "QuotaExceeded") {
          assert.strictEqual(result.failure.reason.resource, "unmeasured conflict source history")
        }
      }
      const after = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_changes WHERE document_id = ${documentId}
      `
      assert.deepStrictEqual(after, before)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(unmeasured.automerge)
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

  // `persist` builds its result from the document it just wrote rather than
  // reconstructing it. That result must stay indistinguishable from a fresh
  // recovery, including the fields it has to carry over rather than re-read.
  const assertPersistMatchesRecovery = (
    stage: (
      store: DocumentStore.DocumentStore["Service"],
      durable: DocumentStore.Stored<typeof Task>
    ) => Effect.Effect<
      Automerge.Doc<InternalAutomerge.Root<typeof Task["schema"]["Encoded"]>>,
      ReplicaError.ReplicaError
    >
  ) =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      InternalAutomerge.free(created.automerge)

      const durable = yield* store.load(Task, documentId)
      assert.strictEqual(durable.snapshot.projection, "Ready")
      // Blocked only after the load, so a result that carried the status over
      // from `durable` instead of reading the row would report the stale value.
      yield* sql`UPDATE effect_local_documents SET projection_status = 'Blocked' WHERE document_id = ${documentId}`
      const staged = yield* stage(store, durable)
      const persisted = yield* store.persist(Task, documentId, durable, staged)
      const reloaded = yield* store.load(Task, documentId)

      assert.deepStrictEqual(persisted.encoded, reloaded.encoded)
      assert.deepStrictEqual(persisted.snapshot, reloaded.snapshot)
      assert.deepStrictEqual(persisted.materializedHeads, reloaded.materializedHeads)
      assert.deepStrictEqual(persisted.acceptedHeads, reloaded.acceptedHeads)
      assert.strictEqual(persisted.commitSequence, reloaded.commitSequence)
      assert.strictEqual(persisted.snapshot.projection, "Blocked")
      // The returned handle keeps the replica scoped actor a recovered handle has.
      assert.strictEqual(
        Automerge.getActorId(persisted.automerge),
        Automerge.getActorId(reloaded.automerge)
      )
      // It is also a distinct handle the caller owns, so freeing it leaves
      // `staged` usable.
      assert.notStrictEqual(persisted.automerge, staged)
      InternalAutomerge.free(persisted.automerge)
      assert.deepStrictEqual(InternalAutomerge.value(staged), reloaded.encoded)

      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(staged)
      InternalAutomerge.free(durable.automerge)
    }).pipe(Effect.provide(Store))

  it.effect("persist returns the same stored view as a fresh recovery", () =>
    assertPersistMatchesRecovery((store, durable) =>
      store.stage(durable, (draft) => {
        draft.title = "two"
        draft.labels.push("local")
      })
    ))

  it.effect("persist returns the same stored view as a fresh recovery when tombstoning", () =>
    assertPersistMatchesRecovery((store, durable) => store.tombstone(durable)))

  // `persist` publishes `materialized_heads` from the staged document it is
  // handed, so a `durable` that no longer describes the stored heads would
  // publish heads the retained change history cannot reproduce. No later load
  // can recover from that, so the commit has to be rejected.
  it.effect("rejects a persist whose heads abandon an already applied change", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const theirs = InternalAutomerge.stage(
        created.automerge,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        (draft) => {
          draft.title = "theirs"
        }
      )
      const mine = InternalAutomerge.stage(
        created.automerge,
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        (draft) => {
          draft.title = "mine"
        }
      )
      const advanced = yield* store.persist(Task, documentId, created, theirs)

      // `created` is stale now: persisting `mine` against it would set the
      // document heads to `mine` alone and orphan the applied change `theirs`.
      const error = yield* Effect.flip(store.persist(Task, documentId, created, mine))
      assert.strictEqual(error.reason._tag, "StorageCorrupt")

      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, { title: "theirs", labels: [] })

      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(advanced.automerge)
      InternalAutomerge.free(mine)
      InternalAutomerge.free(theirs)
      InternalAutomerge.free(created.automerge)
    }).pipe(Effect.provide(Store)))

  const probe = makeProbe()
  const ProbedDatabase = Layer.merge(
    probeLayer(probe).pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))),
    NodeCrypto.layer
  )
  const ProbedBootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(ProbedDatabase))
  const ProbedBase = Layer.merge(ProbedDatabase, ProbedBootstrap)
  const ProbedGate = ReplicaGate.layer.pipe(withGateLimits, Layer.provide(ProbedBase))
  const ProbedStore = Layer.merge(
    ProbedBase,
    DocumentStore.layer.pipe(Layer.provide(Layer.merge(ProbedBase, ProbedGate)))
  )

  // `persist` forks the handle it returns as its last step, so the only failure
  // that can strand one is a failing commit. `sql.withTransaction` raises that as
  // a defect, which the typed handlers cannot observe.
  it.effect("frees the handle it forked when the commit fails", () =>
    Effect.scoped(Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one", labels: [] })
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })
      // A deferred foreign key violation is only reported at commit time.
      yield* withFault(probe, {
        before: (text) =>
          text.includes("INSERT INTO effect_local_commit_outbox")
            ? Effect.andThen(
              sql`PRAGMA defer_foreign_keys = ON`,
              sql`INSERT INTO effect_local_checkpoints (
                checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified
              ) VALUES ('ghost', 'doc_missing', '[]', x'00', 'c', 1, 0)`
            )
            : undefined
      })
      const forked = vi.spyOn(InternalAutomerge, "clone")

      const exit = yield* Effect.exit(store.persist(Task, documentId, created, staged))

      const owned = forked.mock.results.at(-1)?.value as InternalAutomerge.AnyDocument | undefined
      forked.mockRestore()
      assert.strictEqual(exit._tag, "Failure")
      assert.isDefined(owned)
      // A freed document throws on access; a leaked one would still be usable.
      assert.throws(() => InternalAutomerge.heads(owned!))
      // The caller keeps the handles it passed in.
      assert.deepStrictEqual(InternalAutomerge.value(staged), { title: "two", labels: [] })
      assert.deepStrictEqual(InternalAutomerge.value(created.automerge), { title: "one", labels: [] })
      InternalAutomerge.free(staged)
      InternalAutomerge.free(created.automerge)
    })).pipe(Effect.provide(ProbedStore)))
})
