import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import { assert, beforeAll, bench, describe } from "vitest"
import * as QueryReactivity from "../src/QueryReactivity.js"

/* oxlint-disable effect/noTestLifecycleHooks, effect/noAsyncFunction, no-await-in-loop -- Vitest owns benchmark fixture setup, timing callbacks, and teardown. */

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")

const entityKeys = (count: number): ReadonlyArray<string> =>
  Array.from({ length: count }, (_, position) => `message-${position}`)

const layerReactivity = QueryReactivity.makeLayer()
const layerFreshReactivity = Layer.fresh(layerReactivity)
const runtime = ManagedRuntime.make(layerReactivity)

const services = new Map<number, QueryReactivity.Service>()
const batches = new Map<number, ReadonlyArray<string>>()
const querySizes = [100, 1_000] as const
const keySizes = [1, 4, 512, 2_048, 8_192] as const

const buildService = Effect.fnUntraced(
  function*(queries: number) {
    const service = yield* QueryReactivity.QueryReactivity
    for (let position = 0; position < queries; position++) {
      const key = `query-${position}`
      yield* service.retain(key)
      // Half the retained queries watch a model; the other half watch one exact entity.
      if (position % 2 === 0) {
        yield* service.record(key, [{ _tag: "Model", spaceId, model: `model-${position}` }])
      } else {
        yield* service.record(key, [{ _tag: "Entity", spaceId, key: `entity-${position}` }])
      }
    }
    return service
  },
  Effect.provide(layerFreshReactivity)
)

const affected = (queries: number, keys: ReadonlyArray<string>, models: ReadonlyArray<string> = []) =>
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Vitest invokes this synchronous benchmark host callback.
  runtime.runSync(services.get(queries)!.affected({
    spaceId,
    entityKeys: new Set(keys),
    models: new Set(models)
  }))

beforeAll(async () => {
  for (const queries of querySizes) {
    // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Vitest owns this asynchronous benchmark fixture setup boundary.
    services.set(queries, await runtime.runPromise(buildService(queries)))
  }
  for (const size of keySizes) {
    batches.set(size, entityKeys(size))
  }
  assert.deepStrictEqual(affected(1_000, ["entity-7"]), ["query-7"])
  assert.deepStrictEqual(affected(1_000, [], ["model-8"]), ["query-8"])
  assert.deepStrictEqual(affected(1_000, ["entity-8"], ["model-7"]), [])
})

describe("invalidation cost", () => {
  for (const queries of querySizes) {
    for (const keys of keySizes) {
      bench(`affected ${keys} entity keys against ${queries} retained queries`, () => {
        affected(queries, batches.get(keys)!)
      })
    }
  }
  bench("affected one model change against 1000 retained queries", () => {
    affected(1_000, [], ["model-500"])
  })
})
