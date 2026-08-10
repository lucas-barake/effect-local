import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as SqlReplica from "../src/SqlReplica.js"
import { gateLimits } from "./fixtures/limits.js"

describe("commit outbox drain", () => {
  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "outbox-drain",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Live = SqlReplica.layerWithBindings(definition, { projections: [] }).pipe(
    Layer.provideMerge(Layer.mergeAll(Database, ReplicaLimits.layer(gateLimits)))
  )

  const backlog = (rows: number) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      yield* sql`WITH RECURSIVE sequence(commit_sequence) AS (
        VALUES (1) UNION ALL SELECT commit_sequence + 1 FROM sequence WHERE commit_sequence < ${rows}
      ) INSERT INTO effect_local_commit_outbox (
        commit_sequence, document_id, invalidation_keys, published
      ) SELECT commit_sequence, ${documentId}, '["Task"]', 0 FROM sequence`
    })

  const unpublished = Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const rows = yield* sql<{ readonly pending: number }>`
      SELECT COUNT(*) AS pending FROM effect_local_commit_outbox WHERE published = 0`
    return rows[0]?.pending
  })

  it.effect("flush leaves nothing unpublished behind a backlog wider than one batch", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      yield* backlog(CommitPublisher.pendingCommitBatchSize + 72)
      yield* replica.flush
      assert.strictEqual(yield* unpublished, 0)
    }).pipe(Effect.provide(Live), Effect.provide(NodeCrypto.layer), TestClock.withLive))

  it.effect("the poller drains a backlog wider than one batch in a single tick", () =>
    Effect.gen(function*() {
      const publisher = yield* CommitPublisher.CommitPublisher
      const rows = CommitPublisher.pendingCommitBatchSize * 3
      const subscription = yield* publisher.subscribe
      yield* backlog(rows)
      yield* TestClock.adjust("1 second")
      yield* subscription.events.pipe(
        Stream.takeUntil((event) => event._tag === "Commit" && event.commitSequence >= rows),
        Stream.runDrain
      )
      assert.strictEqual(yield* unpublished, 0)
    }).pipe(Effect.scoped, Effect.provide(Live), Effect.provide(NodeCrypto.layer)))
})
