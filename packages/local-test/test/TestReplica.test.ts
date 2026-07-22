import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { TestClock } from "effect/testing"
import * as TestReplica from "../src/TestReplica.js"
import { definition, Rename, Task } from "./fixtures.js"

it.layer(NodeCrypto.layer)("TestReplica", (it) => {
  const Handler = Rename.toLayer(({ draft, payload }) => {
    draft.title = payload
    return undefined
  })
  const Live = TestReplica.layer(definition, { projections: [] }).pipe(Layer.provide(Handler))

  it.effect("runs the production SQL replica over an in-memory database", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const created = yield* replica.create(Task, {
        commandId: (yield* Identity.makeCommandId),
        value: { title: "one" }
      })
      assert.strictEqual(created._tag, "DurablyCommittedLocal")
      if (created._tag !== "DurablyCommittedLocal") return
      yield* replica.mutate(Rename, {
        commandId: (yield* Identity.makeCommandId),
        documentId: created.value,
        payload: "two"
      })
      assert.strictEqual((yield* replica.get(Task, created.value)).value.title, "two")
    }).pipe(Effect.provide(Live), TestClock.withLive))
})
