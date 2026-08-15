import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as ManagedRuntime from "effect/ManagedRuntime"
import { assert, beforeAll, bench, describe } from "vitest"
import type * as IndexStore from "../src/IndexStore.js"
import * as QueryReactivity from "../src/QueryReactivity.js"

/* oxlint-disable effect/noTestLifecycleHooks, effect/noAsyncFunction, no-await-in-loop -- Vitest owns benchmark fixture setup, timing callbacks, and teardown. */

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const descriptor = "message-by-chat"

const footprint = (chat: string, lower: number, upper: number): IndexStore.Footprint => ({
  spaceId,
  descriptor,
  model: "Message",
  index: "byChat",
  partition: [chat],
  lower,
  lowerInclusive: true,
  upper,
  upperInclusive: false,
  direction: "desc",
  cursor: undefined,
  boundary: [lower, "entity-boundary"],
  hasMore: true,
  full: false
})

const point = (chat: string, sentAt: number, entityKey: string): IndexStore.Point => ({
  spaceId,
  descriptor,
  model: "Message",
  index: "byChat",
  partition: [chat],
  sort: [sentAt],
  entityKey
})

const singleChatPoints = (count: number): ReadonlyArray<IndexStore.Point> =>
  Array.from({ length: count }, (_, position) => point("chat-0", 5_000 + position, `message-${position}`))

const spreadPoints = (count: number, chats: number): ReadonlyArray<IndexStore.Point> =>
  Array.from(
    { length: count },
    (_, position) => point(`chat-${position % chats}`, 5_000 + position, `message-${position}`)
  )

const layerReactivity = QueryReactivity.makeLayer()
const runtime = ManagedRuntime.make(layerReactivity)

const services = new Map<number, QueryReactivity.Service>()
const batches = new Map<number, ReadonlyArray<IndexStore.Point>>()
const spreadBatch = spreadPoints(8_192, 1_024)
const querySizes = [100, 1_000] as const
const pointSizes = [512, 2_048, 8_192] as const

const buildService = Effect.fnUntraced(
  function*(queries: number) {
    const service = yield* QueryReactivity.QueryReactivity
    for (let position = 0; position < queries; position++) {
      const key = `query-${position}`
      yield* service.retain(key)
      const read = { _tag: "Index", footprint: footprint(`chat-${position}`, 9_000, 10_000) } as const
      yield* service.record(key, [read])
    }
    return service
  },
  Effect.provide(layerReactivity)
)

const affected = (queries: number, points: ReadonlyArray<IndexStore.Point>) =>
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Vitest invokes this synchronous benchmark host callback.
  runtime.runSync(services.get(queries)!.affected({
    spaceId,
    entityKeys: new Set(),
    points
  }))

beforeAll(async () => {
  for (const queries of querySizes) {
    // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Vitest owns this asynchronous benchmark fixture setup boundary.
    services.set(queries, await runtime.runPromise(buildService(queries)))
  }
  for (const size of pointSizes) {
    batches.set(size, singleChatPoints(size))
  }
  assert.deepStrictEqual(affected(1_000, [point("chat-7", 9_500, "message-live")]), ["query-7"])
  assert.deepStrictEqual(affected(1_000, [point("chat-7", 5_000, "message-old")]), [])
})

describe("invalidation cost", () => {
  for (const queries of querySizes) {
    for (const points of pointSizes) {
      bench(`affected ${points} single-chat points against ${queries} retained queries`, () => {
        affected(queries, batches.get(points)!)
      })
    }
  }
  bench("affected 8192 points spread over 1024 chats against 1000 retained queries", () => {
    affected(1_000, spreadBatch)
  })
})
