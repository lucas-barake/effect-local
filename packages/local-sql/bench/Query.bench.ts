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
import { afterAll, assert, beforeAll, bench } from "vitest"
import * as IndexStore from "../src/IndexStore.js"
import * as Migrations from "../src/Migrations.js"
import * as QueryExecutor from "../src/QueryExecutor.js"

/* oxlint-disable effect/noTestLifecycleHooks, effect/noAsyncFunction -- Vitest owns benchmark fixture setup, timing callbacks, and teardown. */

let decodedRows = 0
const StoredItem = Schema.Struct({ id: Schema.String, score: Schema.Number })
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
    }
  }
})
const Indexed = Query.make("BenchIndexed", {
  payload: { minimum: Schema.Number },
  success: Schema.Array(Item.schema)
})
const Full = Query.make("BenchFull", { success: Schema.Array(Item.schema) })
const definition = Definition.make({ version: 1, models: [Item], mutations: [], queries: [Indexed, Full] })
const handlers = Layer.merge(
  Indexed.toLayer(({ payload, query }) =>
    query.from(Item, "byScore").where({ score: { gte: payload.minimum } }).limit(25).page().pipe(
      Effect.map((page) => page.items)
    )
  ),
  Full.toLayer(({ query }) => query.all(Item))
)
const database = Layer.merge(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  Reactivity.layer
)
const dependencies = Layer.merge(database, handlers)
const executor = QueryExecutor.layer(definition).pipe(Layer.provide(dependencies))
const runtime = ManagedRuntime.make(Layer.merge(executor, dependencies))
const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

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
        generation: 0,
        model: Item.name,
        entity_key: Canonical.stringify(id),
        value_json: Canonical.stringify({ id, score }),
        model_version: Item.version
      }
    })
    for (let offset = 0; offset < rows.length; offset += 500) {
      yield* sql`INSERT INTO effect_local_client_visible_entities_data
        ${sql.insert(rows.slice(offset, offset + 500))}`
    }
    yield* IndexStore.install(sql, definition)
  }))
  decodedRows = 0
})

afterAll(async () => {
  await runtime.dispose()
})

bench("indexed selective page decodes only its 25 returned rows", async () => {
  decodedRows = 0
  const result = await runtime.runPromise(
    QueryExecutor.QueryExecutor.use((service) => service.execute(Indexed, { minimum: 9_900 }))
  )
  assert.strictEqual(result.length, 25)
  assert.strictEqual(decodedRows, 25)
}, { iterations: 20, time: 100, warmupIterations: 3, warmupTime: 25 })

bench("compatibility all decodes the complete 10000 row model", async () => {
  decodedRows = 0
  const result = await runtime.runPromise(
    QueryExecutor.QueryExecutor.use((service) => service.execute(Full, undefined))
  )
  assert.strictEqual(result.length, 10_000)
  assert.strictEqual(decodedRows, 10_000)
}, { iterations: 5, time: 100, warmupIterations: 1, warmupTime: 25 })
