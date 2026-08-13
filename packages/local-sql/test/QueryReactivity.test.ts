import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import type * as IndexStore from "../src/IndexStore.js"
import * as QueryReactivity from "../src/QueryReactivity.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const secondSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")

const footprint = (options: {
  readonly lower: string
  readonly upper: string
  readonly boundary?: ReadonlyArray<string | number>
  readonly full?: boolean
  readonly spaceId?: Identity.SpaceId
}): IndexStore.Footprint => ({
  spaceId: options.spaceId ?? spaceId,
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
  spaceId,
  descriptor: "todo-by-title",
  model: "Todo",
  index: "byTitle",
  partition: [],
  sort: [title],
  entityKey
})

const changes = (points: ReadonlyArray<IndexStore.Point>): QueryReactivity.Changes => ({
  spaceId,
  entityKeys: new Set(),
  points,
  broadModels: new Set()
})
const provideQueryReactivity = Effect.provide(QueryReactivity.layer)

describe("query range reactivity", () => {
  it.effect(
    "intersects old and new index points with only the result ranges they can change",
    Effect.fnUntraced(
      function*() {
        const registry = yield* QueryReactivity.QueryReactivity
        const releaseRelated = yield* registry.retain("related")
        const releaseUnrelated = yield* registry.retain("unrelated")
        yield* Effect.addFinalizer(() => releaseUnrelated.pipe(Effect.andThen(releaseRelated)))
        yield* registry.record("related", [{ _tag: "Index", footprint: footprint({ lower: "a", upper: "m" }) }])
        yield* registry.record("unrelated", [{ _tag: "Index", footprint: footprint({ lower: "n", upper: "z" }) }])

        const betaChanges = changes([point("beta", "\"1\"")])
        assert.deepStrictEqual(yield* registry.affected(betaChanges), ["related"])
        const betaAndOmegaChanges = changes([point("beta", "\"1\""), point("omega", "\"1\"")])
        assert.deepStrictEqual(
          yield* registry.affected(betaAndOmegaChanges),
          ["related", "unrelated"]
        )
      },
      provideQueryReactivity
    )
  )

  it.effect(
    "does not invalidate a full limited page for a point beyond its result boundary",
    Effect.fnUntraced(
      function*() {
        const registry = yield* QueryReactivity.QueryReactivity
        const release = yield* registry.retain("page")
        yield* Effect.addFinalizer(() => release)
        yield* registry.record("page", [{
          _tag: "Index",
          footprint: footprint({ lower: "a", upper: "z", boundary: ["middle", "\"2\""], full: true })
        }])

        const omegaChanges = changes([point("omega", "\"3\"")])
        assert.deepStrictEqual(yield* registry.affected(omegaChanges), [])
        const betaChanges = changes([point("beta", "\"1\"")])
        assert.deepStrictEqual(yield* registry.affected(betaChanges), ["page"])
      },
      provideQueryReactivity
    )
  )

  it.effect(
    "indexes changed points by descriptor before range intersection",
    Effect.fnUntraced(function*() {
      const registry = yield* QueryReactivity.QueryReactivity
      const releases: Array<Effect.Effect<void>> = []
      for (let index = 0; index < 1_000; index++) {
        const key = `query-${index}`
        releases.push(yield* registry.retain(key))
        yield* registry.record(key, [{ _tag: "Index", footprint: footprint({ lower: "a", upper: "z" }) }])
      }
      yield* Effect.addFinalizer(() => Effect.all(releases, { discard: true }))
      let comparisons = 0
      const unrelated = Array.from({ length: 1_000 }, (_, index): IndexStore.Point => ({
        spaceId,
        get descriptor() {
          comparisons++
          return "another-index"
        },
        model: "Todo",
        index: "byTitle",
        partition: [],
        sort: [index],
        entityKey: `"${index}"`
      }))

      assert.deepStrictEqual(yield* registry.affected(changes(unrelated)), [])
      assert.isAtMost(comparisons, 2_000)
    }, provideQueryReactivity)
  )

  it.effect(
    "does not inspect retained reads from another space",
    Effect.fnUntraced(function*() {
      const registry = yield* QueryReactivity.QueryReactivity
      const releases: Array<Effect.Effect<void>> = []
      let inspected = 0
      for (let index = 0; index < 1_000; index++) {
        const key = `other-space-query-${index}`
        releases.push(yield* registry.retain(key))
        yield* registry.record(key, [{
          get _tag() {
            inspected++
            return "Index" as const
          },
          footprint: footprint({ lower: "a", upper: "z", spaceId: secondSpaceId })
        }])
      }
      const targetKey = "target-space-query"
      releases.push(yield* registry.retain(targetKey))
      yield* registry.record(targetKey, [{
        _tag: "Index",
        footprint: footprint({ lower: "a", upper: "z" })
      }])
      yield* Effect.addFinalizer(() => Effect.all(releases, { discard: true }))
      inspected = 0

      assert.deepStrictEqual(yield* registry.affected(changes([point("middle", "\"1\"")])), [targetKey])
      assert.strictEqual(inspected, 0)
    }, provideQueryReactivity)
  )
})
