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
import * as DocumentStore from "../src/DocumentStore.js"
import * as InternalAutomerge from "../src/internal/automerge.js"
import * as Recovery from "../src/Recovery.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import { withGateLimits } from "./fixtures/limits.js"

/**
 * `verifyPersisted` re-reads the replica-wide commit sequence after `persist` has written its rows and
 * compares it to the sequence this transaction allocated. That check is what stands between an
 * interleaved allocation and a commit whose change and outbox rows carry a sequence the allocator has
 * already moved past.
 *
 * It had no test. The sequence used to arrive as a scalar subquery smuggled into the per-document
 * `SELECT`, and moving it to its own statement is exactly the kind of change that can quietly turn a
 * real guard into a tautology, so it gets one now.
 */
describe("DocumentStore commit sequence read-back", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Dependencies = Layer.merge(Database, Bootstrap)
  const Gate = ReplicaGate.layer.pipe(withGateLimits, Layer.provide(Dependencies))
  const RecoveryLayer = Recovery.layer.pipe(Layer.provide(Layer.mergeAll(Dependencies, Gate)))
  const Store = Layer.mergeAll(
    Dependencies,
    Gate,
    RecoveryLayer,
    DocumentStore.layer.pipe(Layer.provide(Layer.mergeAll(Dependencies, Gate, RecoveryLayer)))
  )

  it.effect("rejects and rolls back a commit whose sequence was reallocated after the writes", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })

      // Fires on the last write `persist` makes before verifying, so the allocator has moved past the
      // sequence this transaction stamped on its change and outbox rows.
      yield* sql`CREATE TRIGGER reentrant_allocation
        AFTER INSERT ON effect_local_commit_outbox
        BEGIN
          UPDATE effect_local_metadata SET commit_sequence = commit_sequence + 1 WHERE singleton = 1;
        END`

      const result = yield* Effect.result(store.persist(Task, documentId, created, staged))
      yield* sql`DROP TRIGGER reentrant_allocation`

      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
      }
      const outbox = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_commit_outbox WHERE commit_sequence = 2`
      assert.strictEqual(outbox[0]?.count, 0)
      const changes = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_changes WHERE commit_sequence = 2`
      assert.strictEqual(changes[0]?.count, 0)

      const reloaded = yield* store.load(Task, documentId)
      assert.deepStrictEqual(reloaded.snapshot.value, { title: "one" })
      assert.strictEqual(reloaded.commitSequence, 1)
      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(created.automerge)
      InternalAutomerge.free(staged)
    }).pipe(Effect.provide(Store)))

  // The counterweight: without the trigger the same commit must succeed, and the sequence it reports
  // must be the one a later load reads back. Otherwise the test above would pass on a guard that
  // rejects everything.
  it.effect("reports the allocated sequence and reads the same value back", () =>
    Effect.gen(function*() {
      const store = yield* DocumentStore.DocumentStore
      const documentId = yield* Identity.makeDocumentId
      const created = yield* store.create(Task, documentId, { title: "one" })
      const staged = yield* store.stage(created, (draft) => {
        draft.title = "two"
      })

      const persisted = yield* store.persist(Task, documentId, created, staged)
      assert.strictEqual(persisted.commitSequence, created.commitSequence + 1)

      const reloaded = yield* store.load(Task, documentId)
      assert.strictEqual(reloaded.commitSequence, persisted.commitSequence)
      assert.deepStrictEqual(reloaded.snapshot.value, { title: "two" })
      InternalAutomerge.free(reloaded.automerge)
      InternalAutomerge.free(persisted.automerge)
      InternalAutomerge.free(created.automerge)
      InternalAutomerge.free(staged)
    }).pipe(Effect.provide(Store)))
})
