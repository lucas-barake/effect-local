import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"

describe("CommitPublisher", () => {
  const Item = Document.make("Item", { schema: Schema.Struct({ name: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "publisher",
    documents: DocumentSet.make(Item),
    mutations: [],
    projections: [],
    queries: []
  })
  const Database = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Reactive = Reactivity.layer
  const Publisher = CommitPublisher.layer.pipe(Layer.provide(Layer.merge(Database, Reactive)))
  const Live = Layer.mergeAll(Database, Bootstrap, Reactive, Publisher)

  it.effect("publishes committed invalidations and marks the outbox afterward", () =>
    Effect.gen(function*() {
      const publisher = yield* CommitPublisher.CommitPublisher
      const reactivity = yield* Reactivity.Reactivity
      const sql = yield* SqlClient.SqlClient
      const documentId = Identity.makeDocumentId()
      let invalidations = 0
      const unregister = reactivity.registerUnsafe(["Items"], () => invalidations++)
      yield* sql`INSERT INTO effect_local_commit_outbox (
        commit_sequence, document_id, invalidation_keys, published
      ) VALUES (1, ${documentId}, '["Items"]', 0)`
      assert.strictEqual(yield* publisher.publishPending, 1)
      assert.strictEqual(invalidations, 1)
      assert.strictEqual(yield* publisher.publishPending, 0)
      assert.strictEqual(invalidations, 1)
      const retained = yield* sql<{ readonly commit_sequence: number; readonly published: number }>`
        SELECT commit_sequence, published FROM effect_local_commit_outbox
      `
      assert.deepStrictEqual(retained, [{ commit_sequence: 1, published: 1 }])
      unregister()
    }).pipe(Effect.provide(Live)))

  it.effect("streams commits after a durable watermark and signals full refresh invalidations", () =>
    Effect.scoped(
      Effect.gen(function*() {
        const publisher = yield* CommitPublisher.CommitPublisher
        const sql = yield* SqlClient.SqlClient
        const documentId = Identity.makeDocumentId()
        const subscription = yield* publisher.subscribe
        assert.strictEqual(subscription.watermark, 0)
        const commitFiber = yield* Stream.runHead(subscription.events).pipe(Effect.forkChild)
        yield* sql`INSERT INTO effect_local_commit_outbox (
        commit_sequence, document_id, invalidation_keys, published
      ) VALUES (1, ${documentId}, '["Items"]', 0)`
        yield* publisher.publishPending
        const commit = Option.getOrThrow(yield* Fiber.join(commitFiber))
        assert.deepStrictEqual(commit, {
          _tag: "Commit",
          commitSequence: Identity.CommitSequence.make(1),
          documentId,
          keys: ["Items"],
          refreshGeneration: 0
        })
        const refreshed = yield* publisher.subscribe
        assert.strictEqual(refreshed.watermark, 1)
        assert.strictEqual(refreshed.refreshGeneration, 0)
        const refreshFiber = yield* Stream.runHead(refreshed.events).pipe(Effect.forkChild)
        yield* publisher.invalidate(["Items"])
        assert.deepStrictEqual(Option.getOrThrow(yield* Fiber.join(refreshFiber)), {
          _tag: "FullRefreshRequired",
          refreshGeneration: 1
        })
      }).pipe(Effect.provide(Live))
    ))

  it.effect("serializes concurrent publishers", () =>
    Effect.gen(function*() {
      const publisher = yield* CommitPublisher.CommitPublisher
      const reactivity = yield* Reactivity.Reactivity
      const sql = yield* SqlClient.SqlClient
      const documentId = Identity.makeDocumentId()
      let invalidations = 0
      const unregister = reactivity.registerUnsafe(["Items"], () => invalidations++)
      yield* sql`INSERT INTO effect_local_commit_outbox (
        commit_sequence, document_id, invalidation_keys, published
      ) VALUES (1, ${documentId}, '["Items"]', 0)`
      const published = yield* Effect.all([publisher.publishPending, publisher.publishPending], {
        concurrency: "unbounded"
      })
      assert.deepStrictEqual([...published].toSorted(), [0, 1])
      assert.strictEqual(invalidations, 1)
      unregister()
    }).pipe(Effect.provide(Live)))

  it.effect("publishes before marking delivery and tolerates duplicate recovery", () =>
    Effect.scoped(Effect.gen(function*() {
      const publisher = yield* CommitPublisher.CommitPublisher
      const sql = yield* SqlClient.SqlClient
      const documentId = Identity.makeDocumentId()
      const subscription = yield* publisher.subscribe
      const events = yield* subscription.events.pipe(Stream.take(2), Stream.runCollect, Effect.forkChild)
      yield* sql`INSERT INTO effect_local_commit_outbox (
        commit_sequence, document_id, invalidation_keys, published
      ) VALUES (1, ${documentId}, '["Items"]', 0)`
      yield* sql`CREATE TRIGGER fail_outbox_delivery
        BEFORE UPDATE OF published ON effect_local_commit_outbox
        WHEN NEW.published = 1
        BEGIN SELECT RAISE(ABORT, 'delivery failed'); END`
      assert.strictEqual((yield* Effect.exit(publisher.publishPending))._tag, "Failure")
      assert.strictEqual((yield* publisher.subscribe).watermark, 0)
      yield* sql`DROP TRIGGER fail_outbox_delivery`
      assert.strictEqual(yield* publisher.publishPending, 1)
      assert.deepStrictEqual([...yield* Fiber.join(events)], [
        {
          _tag: "Commit",
          commitSequence: Identity.CommitSequence.make(1),
          documentId,
          keys: ["Items"],
          refreshGeneration: 0
        },
        {
          _tag: "Commit",
          commitSequence: Identity.CommitSequence.make(1),
          documentId,
          keys: ["Items"],
          refreshGeneration: 0
        }
      ])
      assert.strictEqual((yield* publisher.subscribe).watermark, 1)
    })).pipe(Effect.provide(Live)))

  it.effect("drains pending commits on a background wake", () =>
    Effect.scoped(Effect.gen(function*() {
      const publisher = yield* CommitPublisher.CommitPublisher
      const sql = yield* SqlClient.SqlClient
      const documentId = Identity.makeDocumentId()
      const subscription = yield* publisher.subscribe
      const event = yield* Stream.runHead(subscription.events).pipe(Effect.forkChild)
      yield* sql`INSERT INTO effect_local_commit_outbox (
        commit_sequence, document_id, invalidation_keys, published
      ) VALUES (1, ${documentId}, '["Items"]', 0)`
      yield* TestClock.adjust("1 second")
      assert.strictEqual(Option.getOrThrow(yield* Fiber.join(event))._tag, "Commit")
      assert.strictEqual((yield* publisher.subscribe).watermark, 1)
    })).pipe(Effect.provide(Live)))

  it.effect("keeps refresh generation visible after the refresh event slides out", () =>
    Effect.scoped(Effect.gen(function*() {
      const publisher = yield* CommitPublisher.CommitPublisher
      const sql = yield* SqlClient.SqlClient
      const documentId = Identity.makeDocumentId()
      const subscription = yield* publisher.subscribe
      assert.strictEqual(subscription.refreshGeneration, 0)
      yield* publisher.invalidate(["Items"])
      yield* sql`WITH RECURSIVE sequence(commit_sequence) AS (
        VALUES (1) UNION ALL SELECT commit_sequence + 1 FROM sequence WHERE commit_sequence < 257
      ) INSERT INTO effect_local_commit_outbox (
        commit_sequence, document_id, invalidation_keys, published
      ) SELECT commit_sequence, ${documentId}, '["Items"]', 0 FROM sequence`
      assert.strictEqual(yield* publisher.publishPending, 257)
      const retained = [...yield* subscription.events.pipe(Stream.take(2), Stream.runCollect)]
      assert.deepStrictEqual(retained[0], { _tag: "FullRefreshRequired", refreshGeneration: 1 })
      assert.strictEqual(retained[1]?._tag, "Commit")
      if (retained[1]?._tag === "Commit") {
        assert.strictEqual(retained[1].commitSequence, 2)
        assert.strictEqual(retained[1].refreshGeneration, 1)
      }
    })).pipe(Effect.provide(Live)))
})
