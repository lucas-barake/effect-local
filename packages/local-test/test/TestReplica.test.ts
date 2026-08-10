import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import * as TestReplica from "../src/TestReplica.js"
import { definition, Rename, Task } from "./fixtures.js"

it.layer(NodeCrypto.layer)("TestReplica", (test) => {
  const Handler = Rename.toLayer(({ draft, payload }) => {
    draft.title = payload
    return undefined
  })
  const Live = TestReplica.layer(definition, { projections: [] }).pipe(Layer.provide(Handler))
  const TaskTitle = Projection.make("TaskTitle", {
    document: Task,
    version: 1,
    Row: Schema.Struct({ sourceDocumentId: Identity.DocumentId, title: Schema.String }),
    key: (row) => row.sourceDocumentId,
    project: (snapshot) => [{ sourceDocumentId: snapshot.documentId, title: snapshot.value.title }]
  })
  const TaskTitleSql = SqlProjection.make(TaskTitle, {
    table: "task_title_v1",
    migrations: [{
      id: 1,
      name: "task_title_v1",
      run: (sql, table) =>
        sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
          source_document_id TEXT PRIMARY KEY,
          title TEXT NOT NULL
        )`.pipe(Effect.asVoid)
    }],
    deleteByDocument: (sql, table, documentId) =>
      sql`DELETE FROM ${sql(table)} WHERE source_document_id = ${documentId}`.pipe(Effect.asVoid),
    insert: (sql, table, row) =>
      sql`INSERT INTO ${sql(table)} (source_document_id, title)
        VALUES (${row.sourceDocumentId}, ${row.title})`.pipe(Effect.asVoid)
  })
  const ProjectedDefinition = ReplicaDefinition.make({
    name: "projected-test-replica",
    documents: definition.documents,
    mutations: [Rename],
    projections: [TaskTitle]
  })
  const ProjectedLive = TestReplica.layer(ProjectedDefinition, { projections: [TaskTitleSql] }).pipe(
    Layer.provide(Handler)
  )
  const ProjectedSyncLive = TestReplica.layerWithSync(ProjectedDefinition, { projections: [TaskTitleSql] }).pipe(
    Layer.provide(Handler)
  )

  test.effect("runs the production SQL replica over an in-memory database", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const created = yield* replica.create(Task, {
        commandId: (yield* Identity.makeCommandId),
        value: { title: "one" }
      })
      yield* replica.mutate(Rename, {
        commandId: (yield* Identity.makeCommandId),
        documentId: created,
        payload: "two"
      })
      assert.strictEqual((yield* replica.get(Task, created)).value.title, "two")
    }).pipe(Effect.provide(Live), TestClock.withLive))

  test.effect("installs projection bindings for every test layer variant", () => {
    const create = Effect.gen(function*() {
      const replica = yield* Replica.Replica
      return yield* replica.create(Task, {
        commandId: yield* Identity.makeCommandId,
        value: { title: "projected" }
      })
    })
    return Effect.all([
      create.pipe(Effect.provide(ProjectedLive)),
      create.pipe(Effect.provide(ProjectedSyncLive))
    ], { concurrency: 1 }).pipe(
      // Both variants committing is the assertion: `create` fails unless the command is durable.
      Effect.map((documentIds) => assert.strictEqual(documentIds.length, 2)),
      TestClock.withLive
    )
  })
})
