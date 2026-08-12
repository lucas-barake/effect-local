import { SqliteClient } from "@effect/sql-sqlite-node"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { afterAll, assert, beforeAll, bench } from "vitest"
import * as IndexStore from "../src/IndexStore.js"
import * as Codec from "../src/internal/codec.js"
import * as Migrations from "../src/Migrations.js"
import * as QueryExecutor from "../src/QueryExecutor.js"
import * as QueryReactivity from "../src/QueryReactivity.js"

/* oxlint-disable effect/noTestLifecycleHooks, effect/noAsyncFunction -- Vitest owns benchmark fixture setup, timing callbacks, and teardown. */

let decodedRows = 0
const StoredItem = Schema.Struct({ id: Schema.String, bucket: Schema.Number, score: Schema.Number })
const ItemSchema = StoredItem.pipe(Schema.decodeTo(StoredItem, {
  decode: SchemaGetter.transform((item) => {
    decodedRows++
    return item
  }),
  encode: SchemaGetter.transform((item) => item)
})).annotate({ identifier: "EffectLocalBenchItem" })
const Item = Model.make("BenchItem", {
  version: 1,
  key: Schema.String,
  schema: ItemSchema,
  indexes: {
    byScore: {
      version: 1,
      partition: [],
      sort: [{
        name: "score",
        affinity: "real",
        schema: Schema.Number,
        extract: (item: typeof ItemSchema.Type) => item.score
      }]
    },
    byBucketScore: {
      version: 1,
      partition: [],
      sort: [{
        name: "bucket",
        affinity: "integer",
        schema: Schema.Number,
        extract: (item: typeof ItemSchema.Type) => item.bucket
      }, {
        name: "score",
        affinity: "real",
        schema: Schema.Number,
        extract: (item: typeof ItemSchema.Type) => item.score
      }]
    }
  }
})
const Indexed = Query.make("BenchIndexed", {
  payload: { minimum: Schema.Number },
  success: Schema.Array(Item.schema)
})
const MultiColumn = Query.make("BenchMultiColumn", { success: Schema.Array(Item.schema) })
const definition = Definition.make({
  version: 1,
  models: [Item],
  mutations: [],
  queries: [Indexed, MultiColumn]
})
const handlers = Layer.mergeAll(
  Indexed.toLayer(({ payload, query }) =>
    query.from(Item, "byScore").where({ score: { gte: payload.minimum } }).limit(25).page().pipe(
      Effect.map((page) => page.items)
    )
  ),
  MultiColumn.toLayer(({ query }) =>
    Effect.gen(function*() {
      const builder = query.from(Item, "byBucketScore").limit(25)
      const first = yield* builder.page()
      if (first.next === undefined) return []
      return (yield* builder.after(first.next).page()).items
    })
  )
)
const database = Layer.merge(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  Reactivity.layer
)
const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const dependencies = Layer.mergeAll(database, handlers, QueryReactivity.layer)
const executor = QueryExecutor.layer(definition, spaceId).pipe(Layer.provide(dependencies))
const runtime = ManagedRuntime.make(Layer.merge(executor, dependencies))

beforeAll(async () => {
  await runtime.runPromise(Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* Migrations.client({
      definition,
      spaceId,
      clientId,
      migration: { retryDelay: "1 millis", maximumAttempts: 8 }
    })
    const rows = Array.from({ length: 10_000 }, (_, score) => {
      const id = `item-${score.toString().padStart(5, "0")}`
      return {
        space_id: spaceId,
        schema_generation: 0,
        projection_generation: 0,
        model: Item.name,
        entity_key: Canonical.stringify(id),
        value_json: Canonical.stringify({ id, bucket: score % 4, score }),
        model_version: Item.version
      }
    })
    for (let offset = 0; offset < rows.length; offset += 500) {
      yield* sql`INSERT INTO effect_local_client_visible_entities_data
        ${sql.insert(rows.slice(offset, offset + 500))}`
    }
    yield* IndexStore.install(sql, definition, {
      spaceId,
      schemaGeneration: 0,
      projectionGeneration: 0
    })
  }))
  decodedRows = 0
})

afterAll(async () => {
  await runtime.dispose()
})

bench("indexed selective page avoids decoding the complete model", async () => {
  decodedRows = 0
  const result = await runtime.runPromise(
    QueryExecutor.QueryExecutor.use((service) => service.execute(Indexed, { minimum: 9_900 }))
  )
  assert.strictEqual(result.length, 25)
  assert.strictEqual(decodedRows, 50)
}, { iterations: 20, time: 0, warmupIterations: 3, warmupTime: 0, throws: true })

bench("low cardinality multicolumn cursor returns a stable second page", async () => {
  decodedRows = 0
  const result = await runtime.runPromise(
    QueryExecutor.QueryExecutor.use((service) => service.execute(MultiColumn, undefined))
  )
  assert.strictEqual(result.length, 25)
  assert.strictEqual(decodedRows, 75)
}, { iterations: 20, time: 0, warmupIterations: 3, warmupTime: 0, throws: true })

bench("explicit unindexed scan decodes the complete model", async () => {
  decodedRows = 0
  const result = await runtime.runPromise(Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const rows = yield* SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ value_json: Schema.String }),
      execute: () =>
        sql`SELECT value_json FROM effect_local_client_visible_entities_data
        WHERE space_id = ${spaceId} AND schema_generation = 0 AND projection_generation = 0
          AND model = ${Item.name} ORDER BY entity_key`
    })(undefined)
    return yield* Effect.forEach(rows, (row) =>
      Codec.parse(row.value_json).pipe(
        Effect.flatMap((encoded) => Codec.decode(Item.schema, encoded))
      ))
  }))
  assert.strictEqual(result.length, 10_000)
  assert.strictEqual(decodedRows, 10_000)
}, { iterations: 5, time: 0, warmupIterations: 1, warmupTime: 0, throws: true })
