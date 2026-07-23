import { assert, describe, it } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Query from "../src/Query.js"

describe("Query dependencies", () => {
  class Prefix extends Context.Service<Prefix, { readonly value: string }>()(
    "@lucas-barake/effect-local/test/Prefix"
  ) {}

  const Labelled = Query.make("Labelled", {
    payload: Schema.String,
    success: Schema.Array(Schema.Struct({ title: Schema.String })),
    dependsOn: []
  })

  it.effect("provides handler runtime service dependencies through toLayer", () =>
    Effect.gen(function*() {
      const handler = yield* Labelled.handler
      assert.deepStrictEqual(yield* handler("one"), [{ title: "p:one" }])
    }).pipe(
      Effect.provide(
        Labelled.toLayer((title) =>
          Effect.gen(function*() {
            const prefix = yield* Prefix
            return [{ title: `${prefix.value}:${title}` }]
          })
        ).pipe(Layer.provide(Layer.succeed(Prefix, { value: "p" })))
      )
    ))

  it("rejects an empty name and a nonpositive version", () => {
    assert.throws(() => Query.make("", { dependsOn: [] }))
    assert.throws(() => Query.make("Q", { version: 0, dependsOn: [] }))
  })
})
