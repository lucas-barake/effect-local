import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as QueryReactivity from "../src/QueryReactivity.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const secondSpaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")

const changes = (options: {
  readonly entityKeys?: ReadonlyArray<string>
  readonly models?: ReadonlyArray<string>
  readonly spaceId?: Identity.SpaceId
}): QueryReactivity.Changes => ({
  spaceId: options.spaceId ?? spaceId,
  entityKeys: new Set(options.entityKeys ?? []),
  models: new Set(options.models ?? [])
})

const provideQueryReactivity = Effect.provide(QueryReactivity.layer)

describe("query reactivity", () => {
  it.effect(
    "matches model reads against the models a change set touches",
    Effect.fnUntraced(
      function*() {
        const registry = yield* QueryReactivity.QueryReactivity
        const releaseTodos = yield* registry.retain("todos")
        const releaseMessages = yield* registry.retain("messages")
        yield* Effect.addFinalizer(() => releaseMessages.pipe(Effect.andThen(releaseTodos)))
        yield* registry.record("todos", [{ _tag: "Model", spaceId, model: "Todo" }])
        yield* registry.record("messages", [{ _tag: "Model", spaceId, model: "Message" }])

        assert.deepStrictEqual(yield* registry.affected(changes({ models: ["Todo"] })), ["todos"])
        assert.deepStrictEqual(
          yield* registry.affected(changes({ models: ["Todo", "Message"] })),
          ["todos", "messages"]
        )
        assert.deepStrictEqual(yield* registry.affected(changes({ models: ["Other"] })), [])
      },
      provideQueryReactivity
    )
  )

  it.effect(
    "keeps entity reads at exact-key precision",
    Effect.fnUntraced(function*() {
      const registry = yield* QueryReactivity.QueryReactivity
      const release = yield* registry.retain("one-entity")
      yield* Effect.addFinalizer(() => release)
      yield* registry.record("one-entity", [{ _tag: "Entity", spaceId, key: "entity-a" }])

      assert.deepStrictEqual(yield* registry.affected(changes({ entityKeys: ["entity-a"] })), ["one-entity"])
      assert.deepStrictEqual(
        yield* registry.affected(changes({ entityKeys: ["entity-b"], models: ["Todo"] })),
        []
      )
    }, provideQueryReactivity)
  )

  it.effect(
    "isolates retained reads by space",
    Effect.fnUntraced(function*() {
      const registry = yield* QueryReactivity.QueryReactivity
      const releases: Array<Effect.Effect<void>> = []
      for (let index = 0; index < 1_000; index++) {
        const key = `other-space-query-${index}`
        releases.push(yield* registry.retain(key))
        yield* registry.record(key, [{ _tag: "Model", spaceId: secondSpaceId, model: "Todo" }])
      }
      const targetKey = "target-space-query"
      releases.push(yield* registry.retain(targetKey))
      yield* registry.record(targetKey, [{ _tag: "Model", spaceId, model: "Todo" }])
      yield* Effect.addFinalizer(() => Effect.all(releases, { discard: true }))
      assert.deepStrictEqual(yield* registry.affected(changes({ models: ["Todo"] })), [targetKey])
    }, provideQueryReactivity)
  )

  it.effect(
    "ignores reads recorded for a token that was never retained and drops reads on release",
    Effect.fnUntraced(function*() {
      const registry = yield* QueryReactivity.QueryReactivity
      yield* registry.record("never-retained", [{ _tag: "Model", spaceId, model: "Todo" }])
      assert.deepStrictEqual(yield* registry.affected(changes({ models: ["Todo"] })), [])

      const release = yield* registry.retain("released")
      yield* registry.record("released", [{ _tag: "Model", spaceId, model: "Todo" }])
      assert.deepStrictEqual(yield* registry.affected(changes({ models: ["Todo"] })), ["released"])
      yield* release
      assert.deepStrictEqual(yield* registry.affected(changes({ models: ["Todo"] })), [])
    }, provideQueryReactivity)
  )
})
