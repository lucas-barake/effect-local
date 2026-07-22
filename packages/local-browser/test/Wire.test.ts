import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Wire from "../src/internal/wire.js"

it.layer(NodeCrypto.layer)("browser wire", (it) => {
  it.effect("represents void outcomes as JSON null", () =>
    Effect.gen(function*() {
      const commandId = yield* Identity.makeCommandId
      const encoded = yield* Wire.encodeOutcome(
        Schema.Void,
        Schema.Never,
        CommandOutcome.durablyCommitted(commandId, undefined)
      )
      assert.deepStrictEqual(encoded, CommandOutcome.durablyCommitted(commandId, null))
      assert.deepStrictEqual(
        yield* Wire.decodeOutcome(Schema.Void, Schema.Never, encoded),
        CommandOutcome.durablyCommitted(commandId, undefined)
      )
    }))

  it.effect("round trips transformed mutation outcomes", () =>
    Effect.gen(function*() {
      const commandId = yield* Identity.makeCommandId
      const encoded = yield* Wire.encodeOutcome(
        Schema.NumberFromString,
        Schema.Never,
        CommandOutcome.durablyCommitted(commandId, 42)
      )
      assert.deepStrictEqual(encoded, CommandOutcome.durablyCommitted(commandId, "42"))
      assert.deepStrictEqual(
        yield* Wire.decodeOutcome(Schema.NumberFromString, Schema.Never, encoded),
        CommandOutcome.durablyCommitted(commandId, 42)
      )
    }))
})
