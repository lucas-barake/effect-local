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
import { TestClock } from "effect/testing"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"

describe("CommitPublisher coverage", () => {
  const Item = Document.make("Item", { schema: Schema.Struct({ name: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "publisher",
    documents: DocumentSet.make(Item),
    mutations: [],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Reactive = Reactivity.layer
  const Publisher = CommitPublisher.layer.pipe(Layer.provide(Layer.merge(Database, Reactive)))
  const Live = Layer.mergeAll(Database, Bootstrap, Reactive, Publisher)

  const publishedOf = (commitSequence: number) =>
    SqlClient.SqlClient.pipe(
      Effect.flatMap((sql) =>
        sql<{ readonly published: number }>`SELECT published FROM effect_local_commit_outbox
          WHERE commit_sequence = ${commitSequence}`
      ),
      Effect.map((rows) => rows[0]?.published ?? null)
    )

  it.effect("keeps the background publisher draining after a transient delivery failure", () =>
    Effect.gen(function*() {
      yield* CommitPublisher.CommitPublisher
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      yield* sql`INSERT INTO effect_local_commit_outbox (
        commit_sequence, document_id, invalidation_keys, published
      ) VALUES (1, ${documentId}, '["Items"]', 0)`
      yield* sql`CREATE TRIGGER fail_outbox_delivery
        BEFORE UPDATE OF published ON effect_local_commit_outbox
        WHEN NEW.published = 1
        BEGIN SELECT RAISE(ABORT, 'delivery failed'); END`
      assert.strictEqual(yield* publishedOf(1), 0)
      yield* TestClock.adjust("1 second")
      assert.strictEqual(yield* publishedOf(1), 0)
      yield* sql`DROP TRIGGER fail_outbox_delivery`
      yield* TestClock.adjust("1 second")
      assert.strictEqual(yield* publishedOf(1), 1)
    }).pipe(Effect.provide(Live)))
})
