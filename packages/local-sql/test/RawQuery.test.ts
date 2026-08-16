import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as LocalStore from "../src/LocalStore.js"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryExecutor from "../src/QueryExecutor.js"
import * as QueryReactivity from "../src/QueryReactivity.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000201")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000201")

const TodoSchema = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  authorId: Schema.String,
  count: Schema.Number
})
const Todo = Model.make("Todo", { version: 1, key: Schema.String, schema: TodoSchema })
const AuthorSchema = Schema.Struct({ id: Schema.String, name: Schema.String })
const Author = Model.make("Author", { version: 1, key: Schema.String, schema: AuthorSchema })
// `key` collides with the injected entity-key column; the entity column must win.
const CollidingSchema = Schema.Struct({ id: Schema.String, key: Schema.String })
const Colliding = Model.make("Colliding", { version: 1, key: Schema.String, schema: CollidingSchema })

// A field name carrying a single quote must survive the generated json_extract path literal.
const QuotedSchema = Schema.Struct({ id: Schema.String, "it's": Schema.Number })
const Quoted = Model.make("Quoted", { version: 1, key: Schema.String, schema: QuotedSchema })

const PutTodo = Mutation.make("PutTodo", { version: 1, payload: TodoSchema, success: Schema.String })
const PutAuthor = Mutation.make("PutAuthor", { version: 1, payload: AuthorSchema, success: Schema.String })
const PutQuoted = Mutation.make("PutQuoted", { version: 1, payload: QuotedSchema, success: Schema.String })
const PutColliding = Mutation.make("PutColliding", {
  version: 1,
  payload: CollidingSchema,
  success: Schema.String
})

const CountByAuthor = Query.make("CountByAuthor", {
  payload: { minimum: Schema.Number },
  success: Schema.Array(Schema.Struct({ author: Schema.String, todos: Schema.Number }))
})
const RecursiveSeries = Query.make("RecursiveSeries", {
  success: Schema.Array(Schema.Number)
})
const OwnWith = Query.make("OwnWith", { success: Schema.Array(Schema.Number) })
const MissingTable = Query.make("MissingTable", { success: Schema.Array(Schema.Number) })
const CollidingKeys = Query.make("CollidingKeys", {
  success: Schema.Array(Schema.Struct({ key: Schema.String, embedded: Schema.String }))
})
const EmptyModels = Query.make("EmptyModels", { success: Schema.Array(Schema.Number) })
const QuotedIds = Query.make("QuotedIds", { success: Schema.Array(Schema.String) })

const definition = Definition.make({
  version: 1,
  models: [Todo, Author, Colliding, Quoted],
  mutations: [PutTodo, PutAuthor, PutColliding, PutQuoted],
  queries: [CountByAuthor, RecursiveSeries, OwnWith, MissingTable, CollidingKeys, EmptyModels, QuotedIds]
})

const layerHandlers = Layer.mergeAll(
  PutTodo.toLayer(({ payload, transaction }) => transaction.set(Todo, payload.id, payload).pipe(Effect.as(payload.id))),
  PutAuthor.toLayer(({ payload, transaction }) =>
    transaction.set(Author, payload.id, payload).pipe(Effect.as(payload.id))
  ),
  PutColliding.toLayer(({ payload, transaction }) =>
    transaction.set(Colliding, payload.id, payload).pipe(Effect.as(payload.id))
  ),
  // A join with an aggregate and a subquery across two models, decoded through SqlSchema.
  CountByAuthor.toLayer(({ payload, query }) =>
    SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ author: Schema.String, todos: Schema.Number }),
      execute: () =>
        query.sql([Todo, Author], (sql) =>
          sql`SELECT a."name" AS author, COUNT(t."id") AS todos
            FROM "Todo" t
            JOIN "Author" a ON a."id" = t."authorId"
            WHERE t."count" >= ${payload.minimum}
              AND a."id" IN (SELECT "authorId" FROM "Todo")
            GROUP BY a."name"
            ORDER BY todos DESC, author ASC`)
    })(undefined).pipe(
      Effect.catchTag("SchemaError", (cause) => Effect.die(cause))
    )
  ),
  // Continues the generated CTE list with its own recursive member.
  RecursiveSeries.toLayer(({ query }) =>
    SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ n: Schema.Number }),
      execute: () =>
        query.sql([Todo], (sql) =>
          sql`, series(n) AS (
            SELECT 1 UNION ALL SELECT n + 1 FROM series WHERE n < (SELECT COUNT(*) FROM "Todo")
          ) SELECT n FROM series ORDER BY n`)
    })(undefined).pipe(
      Effect.map((rows) => rows.map((row) => row.n)),
      Effect.catchTag("SchemaError", (cause) => Effect.die(cause))
    )
  ),
  OwnWith.toLayer(({ query }) =>
    query.sql([Todo], (sql) => sql`WITH own AS (SELECT 1 AS n) SELECT n FROM own`).pipe(
      Effect.map(() => [])
    )
  ),
  MissingTable.toLayer(({ query }) =>
    query.sql([Todo], (sql) => sql`SELECT n FROM this_table_does_not_exist`).pipe(
      Effect.map(() => [])
    )
  ),
  CollidingKeys.toLayer(({ query }) =>
    SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({
        key: Schema.String,
        embedded: Schema.String
      }),
      execute: () =>
        query.sql([Colliding], (sql) => sql`SELECT "key", json_extract("value", '$.key') AS embedded FROM "Colliding"`)
    })(undefined).pipe(
      Effect.catchTag("SchemaError", (cause) => Effect.die(cause))
    )
  ),
  PutQuoted.toLayer(({ payload, transaction }) =>
    transaction.set(Quoted, payload.id, payload).pipe(Effect.as(payload.id))
  ),
  QuotedIds.toLayer(({ query }) =>
    SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ id: Schema.String }),
      execute: () => query.sql([Quoted], (sql) => sql`SELECT "id" FROM "Quoted" ORDER BY "id"`)
    })(undefined).pipe(
      Effect.map((rows) => rows.map((row) => row.id)),
      Effect.catchTag("SchemaError", (cause) => Effect.die(cause))
    )
  ),
  EmptyModels.toLayer(({ query }) => query.sql([], (sql) => sql`SELECT 1`).pipe(Effect.map(() => [])))
)

const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const layerDatabase = () =>
  Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
const layerRuntime = MutationRuntime.layer(definition).pipe(Layer.provide(layerHandlers))
const clientHistory = {
  defaultScope: Protocol.ReplicationScope.make({ models: [Todo.name, Author.name, Colliding.name, Quoted.name] }),
  scope: Protocol.ReplicationScope.make({ models: [Todo.name, Author.name, Colliding.name, Quoted.name] }),
  maximumActiveSpaces: 4,
  foregroundActiveSpaces: 2,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  migration
}

const expectedFailure = Effect.fnUntraced(function*<A, E extends { readonly _tag: string }, R,>(
  effect: Effect.Effect<A, E, R>
) {
  const exit = yield* Effect.exit(effect)
  if (Exit.isSuccess(exit)) return yield* Effect.die(new Error("expected the query to fail"))
  const failure = Cause.findErrorOption(exit.cause)
  if (Option.isNone(failure)) return yield* Effect.failCause(exit.cause)
  return failure.value
})

const harness = Effect.fnUntraced(function*() {
  const layerLocal = LocalStore.layer({ ...clientHistory, definition, spaceId, clientId }).pipe(
    Layer.provide(layerRuntime)
  )
  const layerQueries = QueryExecutor.layer(definition, spaceId).pipe(Layer.provide(layerHandlers))
  const context = yield* Layer.build(Layer.merge(layerLocal, layerQueries).pipe(Layer.provideMerge(layerDatabase())))
  return {
    store: Context.get(context, LocalStore.Store),
    executor: Context.get(context, QueryExecutor.QueryExecutor),
    sql: Context.get(context, SqlClient.SqlClient)
  }
})

describe("raw SQL queries", () => {
  it.effect(
    "joins, aggregates, and subqueries across models in one statement",
    Effect.fnUntraced(function*() {
      const { executor, store } = yield* harness()
      yield* store.mutate(PutAuthor, { id: "author-1", name: "Ada" })
      yield* store.mutate(PutAuthor, { id: "author-2", name: "Grace" })
      yield* store.mutate(PutTodo, { id: "t1", title: "one", authorId: "author-1", count: 3 })
      yield* store.mutate(PutTodo, { id: "t2", title: "two", authorId: "author-1", count: 5 })
      yield* store.mutate(PutTodo, { id: "t3", title: "three", authorId: "author-2", count: 1 })

      assert.deepStrictEqual(yield* executor.execute(CountByAuthor, { minimum: 0 }), [
        { author: "Ada", todos: 2 },
        { author: "Grace", todos: 1 }
      ])
      assert.deepStrictEqual(yield* executor.execute(CountByAuthor, { minimum: 2 }), [
        { author: "Ada", todos: 2 }
      ])
    }, Effect.scoped)
  )

  it.effect(
    "lets a statement continue the generated CTE list, including recursively",
    Effect.fnUntraced(function*() {
      const { executor, store } = yield* harness()
      yield* store.mutate(PutTodo, { id: "t1", title: "one", authorId: "author-1", count: 1 })
      yield* store.mutate(PutTodo, { id: "t2", title: "two", authorId: "author-1", count: 2 })
      yield* store.mutate(PutTodo, { id: "t3", title: "three", authorId: "author-1", count: 3 })
      assert.deepStrictEqual(yield* executor.execute(RecursiveSeries, undefined), [1, 2, 3])
    }, Effect.scoped)
  )

  it.effect(
    "fails QueryFailed when the statement opens its own WITH clause",
    Effect.fnUntraced(function*() {
      const { executor } = yield* harness()
      const error = yield* expectedFailure(executor.execute(OwnWith, undefined))
      assert.strictEqual(error._tag, "QueryFailed")
    }, Effect.scoped)
  )

  it.effect(
    "fails QueryFailed when the SQL itself is invalid",
    Effect.fnUntraced(function*() {
      const { executor } = yield* harness()
      const error = yield* expectedFailure(executor.execute(MissingTable, undefined))
      assert.strictEqual(error._tag, "QueryFailed")
    }, Effect.scoped)
  )

  it.effect(
    "keeps the entity key column when a model field is named key",
    Effect.fnUntraced(function*() {
      const { executor, store } = yield* harness()
      yield* store.mutate(PutColliding, { id: "c1", key: "embedded-value" })
      assert.deepStrictEqual(yield* executor.execute(CollidingKeys, undefined), [
        { key: "\"c1\"", embedded: "embedded-value" }
      ])
    }, Effect.scoped)
  )

  it.effect(
    "reads a model whose field name contains a single quote",
    Effect.fnUntraced(function*() {
      const { executor, store } = yield* harness()
      yield* store.mutate(PutQuoted, { id: "q1", "it's": 7 })
      assert.deepStrictEqual(yield* executor.execute(QuotedIds, undefined), ["q1"])
    }, Effect.scoped)
  )

  it.effect(
    "classifies a malformed stored entity value as storage corruption, not a broken statement",
    Effect.fnUntraced(function*() {
      const { executor, sql, store } = yield* harness()
      yield* store.mutate(PutAuthor, { id: "author-1", name: "Ada" })
      yield* store.mutate(PutTodo, { id: "t1", title: "one", authorId: "author-1", count: 3 })
      yield* sql`UPDATE effect_local_client_visible_entities_data SET value_json = 'not json'
        WHERE space_id = ${spaceId} AND model = ${Todo.name}`
      const error = yield* expectedFailure(executor.execute(CountByAuthor, { minimum: 0 }))
      assert.strictEqual(error._tag, "StorageCorrupt")
    }, Effect.scoped)
  )

  it.effect(
    "dies when a statement declares no models",
    Effect.fnUntraced(function*() {
      const { executor } = yield* harness()
      const exit = yield* Effect.exit(executor.execute(EmptyModels, undefined))
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        const failure = Cause.findErrorOption(exit.cause)
        assert.isTrue(Option.isNone(failure))
      }
    }, Effect.scoped)
  )
})
