import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as ProjectionStore from "../src/ProjectionStore.js"
import * as QueryExecutor from "../src/QueryExecutor.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as SqlProjection from "../src/SqlProjection.js"

describe("QueryExecutor", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ labels: Schema.Array(Schema.String) }),
    version: 1
  })
  const Labels = Projection.make("Labels", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, label: Schema.String }),
    key: (row) => `${row.sourceDocumentId}:${row.label}`,
    project: (snapshot) => snapshot.value.labels.map((label) => ({ sourceDocumentId: snapshot.documentId, label }))
  })
  const LabelsSql = SqlProjection.make(Labels, {
    table: "query_task_labels_v1",
    migrations: [{
      id: 1,
      name: "query_task_labels_v1",
      run: (sql, table) =>
        sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
          sourceDocumentId TEXT NOT NULL,
          label TEXT NOT NULL
        )`.pipe(Effect.asVoid)
    }],
    deleteByDocument: (sql, table, documentId) =>
      sql`DELETE FROM ${sql(table)} WHERE sourceDocumentId = ${documentId}`.pipe(Effect.asVoid),
    insert: (sql, table, row) =>
      sql`INSERT INTO ${sql(table)} (sourceDocumentId, label)
      VALUES (${row.sourceDocumentId}, ${row.label})`.pipe(Effect.asVoid)
  })
  const ListLabels = Query.make("ListLabels", {
    payload: { prefix: Schema.String },
    success: Schema.Array(Labels.Row),
    dependsOn: [Labels]
  })
  const definition = ReplicaDefinition.make({
    name: "query-tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [Labels],
    queries: [ListLabels]
  })
  const listLabels = SqlSchema.findAll({
    Request: ListLabels.payloadSchema,
    Result: Labels.Row,
    execute: ({ prefix }) =>
      SqlClient.SqlClient.use((sql) =>
        sql`SELECT sourceDocumentId, label FROM query_task_labels_v1
        WHERE label LIKE ${`${prefix}%`} ORDER BY label`
      )
  })
  const Database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )
  const Reactive = Reactivity.layer
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Base = Layer.merge(Database, Bootstrap)
  const Projections = ProjectionStore.layer([LabelsSql]).pipe(
    Layer.provide(Layer.merge(Base, LabelsSql.layer))
  )
  const Handler = ListLabels.toLayer((request) => listLabels(request).pipe(Effect.orDie)).pipe(
    Layer.provide(Database)
  )
  const Executor = QueryExecutor.layer(definition).pipe(
    Layer.provide(Layer.mergeAll(Database, Handler, Reactive))
  )
  const Live = Layer.mergeAll(Base, Projections, Handler, Reactive, Executor)

  it.effect("encodes requests and decodes every returned row with SqlSchema", () =>
    Effect.gen(function*() {
      const executor = yield* QueryExecutor.QueryExecutor
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      yield* sql`INSERT INTO query_task_labels_v1 (sourceDocumentId, label)
        VALUES (${documentId}, 'alpha'), (${documentId}, 'beta')`
      const rows = yield* executor.execute(ListLabels, { prefix: "a" })
      assert.deepStrictEqual(rows, [{ sourceDocumentId: documentId, label: "alpha" }])
    }).pipe(Effect.provide(Live)))

  it.effect("fails ProjectionBlocked rather than returning stale rows", () =>
    Effect.gen(function*() {
      const executor = yield* QueryExecutor.QueryExecutor
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_projection_registry SET status = 'Blocked'
        WHERE projection_name = ${Labels.name}`
      const result = yield* Effect.exit(executor.execute(ListLabels, { prefix: "" }))
      assert.strictEqual(result._tag, "Failure")
    }).pipe(Effect.provide(Live)))

  it.effect("blocks stale rows when a source document projection is blocked", () =>
    Effect.gen(function*() {
      const executor = yield* QueryExecutor.QueryExecutor
      const sql = yield* SqlClient.SqlClient
      const documentId = yield* Identity.makeDocumentId
      yield* sql`INSERT INTO effect_local_documents (
        document_id, document_type, schema_version, observed_versions,
        materialized_heads, accepted_heads, tombstone, projection_status, checkpoint_hash
      ) VALUES (${documentId}, ${Task.name}, 1, '[1]', '["remote"]', '["remote"]', 0, 'Blocked', NULL)`
      yield* sql`INSERT INTO query_task_labels_v1 (sourceDocumentId, label)
        VALUES (${documentId}, 'stale')`
      const result = yield* Effect.exit(executor.execute(ListLabels, { prefix: "" }))
      assert.strictEqual(result._tag, "Failure")
    }).pipe(Effect.provide(Live)))

  it.effect("serializes readiness validation with the query handler", () =>
    Effect.gen(function*() {
      const handlerEntered = yield* Deferred.make<void>()
      const releaseHandler = yield* Deferred.make<void>()
      const projectionBlocked = yield* Deferred.make<void>()
      const blockingHandler = ListLabels.toLayer((request) =>
        Effect.gen(function*() {
          yield* Deferred.succeed(handlerEntered, void 0)
          yield* Deferred.await(releaseHandler)
          return yield* listLabels(request).pipe(Effect.orDie)
        })
      ).pipe(Layer.provide(Database))
      const blockingExecutor = QueryExecutor.layer(definition).pipe(
        Layer.provide(Layer.mergeAll(Database, blockingHandler, Reactive))
      )
      const blockingLive = Layer.mergeAll(Base, Projections, blockingHandler, Reactive, blockingExecutor)

      yield* Effect.gen(function*() {
        const executor = yield* QueryExecutor.QueryExecutor
        const sql = yield* SqlClient.SqlClient
        const documentId = yield* Identity.makeDocumentId
        yield* sql`INSERT INTO query_task_labels_v1 (sourceDocumentId, label)
          VALUES (${documentId}, 'stale')`
        const queryFiber = yield* executor.execute(ListLabels, { prefix: "" }).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(handlerEntered)
        const blockFiber = yield* sql`UPDATE effect_local_projection_registry SET status = 'Blocked'
          WHERE projection_name = ${Labels.name}`.pipe(
          Effect.andThen(Deferred.succeed(projectionBlocked, void 0)),
          Effect.forkChild({ startImmediately: true })
        )
        assert.isFalse(yield* Deferred.isDone(projectionBlocked))
        yield* Deferred.succeed(releaseHandler, void 0)
        assert.deepStrictEqual(yield* Fiber.join(queryFiber), [
          { sourceDocumentId: documentId, label: "stale" }
        ])
        yield* Fiber.join(blockFiber)
      }).pipe(Effect.provide(blockingLive))
    }))

  it.effect("refreshes reactive queries when a dependency document changes", () =>
    Effect.gen(function*() {
      const executor = yield* QueryExecutor.QueryExecutor
      const reactivity = yield* Reactivity.Reactivity
      const sql = yield* SqlClient.SqlClient
      const firstValue = yield* Deferred.make<void>()
      const documentId = yield* Identity.makeDocumentId
      yield* sql`INSERT INTO query_task_labels_v1 (sourceDocumentId, label)
        VALUES (${documentId}, 'alpha')`
      const values = yield* executor.reactive(ListLabels, { prefix: "" }).pipe(
        Stream.tap(() => Deferred.succeed(firstValue, void 0)),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild
      )
      yield* Deferred.await(firstValue)
      yield* sql`INSERT INTO query_task_labels_v1 (sourceDocumentId, label)
        VALUES (${documentId}, 'beta')`
      yield* reactivity.invalidate([Task.name])
      const results = yield* Fiber.join(values)
      assert.deepStrictEqual(results, [
        [{ sourceDocumentId: documentId, label: "alpha" }],
        [
          { sourceDocumentId: documentId, label: "alpha" },
          { sourceDocumentId: documentId, label: "beta" }
        ]
      ])
    }).pipe(Effect.provide(Live)))

  it.effect("closes reactive queries whose projections share a document", () => {
    const LabelCounts = Projection.make("LabelCounts", {
      document: Task,
      version: 1,
      Row: Schema.Struct({
        sourceDocumentId: Identity.DocumentId,
        count: Schema.Number
      }),
      key: (row) => row.sourceDocumentId,
      project: (snapshot) => [{
        sourceDocumentId: snapshot.documentId,
        count: snapshot.value.labels.length
      }]
    })
    const LabelCountsSql = SqlProjection.make(LabelCounts, {
      table: "query_task_label_counts_v1",
      migrations: [{
        id: 1,
        name: "query_task_label_counts_v1",
        run: (sql, table) =>
          sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
            sourceDocumentId TEXT NOT NULL,
            count INTEGER NOT NULL
          )`.pipe(Effect.asVoid)
      }],
      deleteByDocument: (sql, table, documentId) =>
        sql`DELETE FROM ${sql(table)} WHERE sourceDocumentId = ${documentId}`.pipe(Effect.asVoid),
      insert: (sql, table, row) =>
        sql`INSERT INTO ${sql(table)} (sourceDocumentId, count)
          VALUES (${row.sourceDocumentId}, ${row.count})`.pipe(Effect.asVoid)
    })
    const Combined = Query.make("Combined", {
      success: Schema.Number,
      dependsOn: [Labels, LabelCounts]
    })
    const combinedDefinition = ReplicaDefinition.make({
      name: "shared-document-reactivity",
      documents: DocumentSet.make(Task),
      projections: [Labels, LabelCounts],
      queries: [Combined]
    })
    const combinedDatabase = Layer.merge(
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer
    )
    const combinedBootstrap = ReplicaBootstrap.layer(combinedDefinition).pipe(
      Layer.provide(combinedDatabase)
    )
    const combinedBase = Layer.merge(combinedDatabase, combinedBootstrap)
    const combinedProjections = ProjectionStore.layer([LabelsSql, LabelCountsSql]).pipe(
      Layer.provide(Layer.mergeAll(
        combinedBase,
        LabelsSql.layer,
        LabelCountsSql.layer
      ))
    )
    const combinedHandler = Combined.toLayer(() => Effect.succeed(1))
    const combinedReactive = Reactivity.layer
    const combinedExecutor = QueryExecutor.layer(combinedDefinition).pipe(
      Layer.provide(Layer.mergeAll(
        combinedDatabase,
        combinedHandler,
        combinedReactive
      ))
    )
    const combinedLive = Layer.mergeAll(
      combinedBase,
      combinedProjections,
      combinedHandler,
      combinedReactive,
      combinedExecutor
    )

    return Effect.gen(function*() {
      const executor = yield* QueryExecutor.QueryExecutor
      const first = yield* Effect.scoped(
        Effect.gen(function*() {
          const pull = yield* Stream.toPull(executor.reactive(Combined, undefined))
          return yield* pull
        })
      )
      assert.deepStrictEqual(first, [1])
    }).pipe(Effect.provide(combinedLive))
  })
})
