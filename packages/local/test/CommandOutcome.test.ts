import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as CommandOutcome from "../src/CommandOutcome.js"
import * as Identity from "../src/Identity.js"

describe("CommandOutcome", () => {
  it.effect("extracts committed values", () =>
    Effect.gen(function*() {
      const commandId = Identity.makeCommandId()
      const value = yield* CommandOutcome.committedOrFail(CommandOutcome.durablyCommitted(commandId, 42))
      assert.strictEqual(value, 42)
    }))

  it.effect("preserves rejection and ambiguity", () =>
    Effect.gen(function*() {
      const commandId = Identity.makeCommandId()
      const rejected = yield* CommandOutcome.committedOrFail(CommandOutcome.rejected(commandId, "no")).pipe(Effect.exit)
      const unknown = yield* CommandOutcome.committedOrFail(CommandOutcome.unknown(commandId)).pipe(Effect.exit)
      assert.deepStrictEqual(rejected, Exit.fail(CommandOutcome.rejected(commandId, "no")))
      assert.deepStrictEqual(unknown, Exit.fail(CommandOutcome.unknown(commandId)))
    }))

  it("round trips every durable outcome through its generated schema", () => {
    const commandId = Identity.makeCommandId()
    const Outcome = CommandOutcome.schema(Schema.Number, Schema.String)
    const values = [
      CommandOutcome.rejected(commandId, "no"),
      CommandOutcome.durablyCommitted(commandId, 1),
      CommandOutcome.unknown(commandId)
    ]
    for (const value of values) {
      const encoded = Schema.encodeSync(Outcome)(value)
      assert.deepStrictEqual(Schema.decodeUnknownSync(Outcome)(encoded), value)
    }
  })
})
