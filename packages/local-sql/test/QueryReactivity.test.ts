import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import type * as IndexStore from "../src/IndexStore.js"
import * as QueryReactivity from "../src/QueryReactivity.js"

const footprint = (options: {
  readonly lower: string
  readonly upper: string
  readonly boundary?: ReadonlyArray<string | number>
  readonly full?: boolean
}): IndexStore.Footprint => ({
  descriptor: "todo-by-title",
  model: "Todo",
  index: "byTitle",
  partition: [],
  lower: options.lower,
  lowerInclusive: true,
  upper: options.upper,
  upperInclusive: false,
  direction: "asc",
  cursor: undefined,
  boundary: options.boundary,
  hasMore: false,
  full: options.full ?? false
})

const point = (title: string, entityKey: string): IndexStore.Point => ({
  descriptor: "todo-by-title",
  model: "Todo",
  index: "byTitle",
  partition: [],
  sort: [title],
  entityKey
})

const changes = (points: ReadonlyArray<IndexStore.Point>): QueryReactivity.Changes => ({
  entityKeys: new Set(),
  models: new Set(["Todo"]),
  points,
  broadModels: new Set()
})

describe("query range reactivity", () => {
  it.effect("intersects old and new index points with only the result ranges they can change", () =>
    Effect.gen(function*() {
      const service = yield* Reactivity.make
      const registry = QueryReactivity.get(service)
      const releaseRelated = QueryReactivity.retain("related")
      const releaseUnrelated = QueryReactivity.retain("unrelated")
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          releaseUnrelated()
          releaseRelated()
        })
      )
      registry.record("related", [{ _tag: "Index", footprint: footprint({ lower: "a", upper: "m" }) }])
      registry.record("unrelated", [{ _tag: "Index", footprint: footprint({ lower: "n", upper: "z" }) }])

      assert.deepStrictEqual(registry.affected(changes([point("beta", "\"1\"")])), ["related"])
      assert.deepStrictEqual(
        registry.affected(changes([point("beta", "\"1\""), point("omega", "\"1\"")])),
        ["related", "unrelated"]
      )
    }))

  it.effect("does not invalidate a full limited page for a point beyond its result boundary", () =>
    Effect.gen(function*() {
      const service = yield* Reactivity.make
      const registry = QueryReactivity.get(service)
      const release = QueryReactivity.retain("page")
      yield* Effect.addFinalizer(() => Effect.sync(release))
      registry.record("page", [{
        _tag: "Index",
        footprint: footprint({ lower: "a", upper: "z", boundary: ["middle", "\"2\""], full: true })
      }])

      assert.deepStrictEqual(registry.affected(changes([point("omega", "\"3\"")])), [])
      assert.deepStrictEqual(registry.affected(changes([point("beta", "\"1\"")])), ["page"])
    }))
})
