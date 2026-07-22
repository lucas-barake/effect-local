import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as CommandOutcome from "../src/CommandOutcome.js"
import * as Identity from "../src/Identity.js"

class Rejected extends Schema.TaggedErrorClass<Rejected>("@test/CommandOutcome/Rejected")("Rejected", {
  message: Schema.String
}) {}

it.layer(NodeCrypto.layer)("CommandOutcome", (it) => {
  it.effect("extracts committed values", () =>
    Effect.gen(function*() {
      const commandId = yield* Identity.makeCommandId
      const value = yield* CommandOutcome.committedOrFail(CommandOutcome.durablyCommitted(commandId, 42))
      assert.strictEqual(value, 42)
    }))

  it.effect("fails with the domain rejection or a tagged ambiguity error", () =>
    Effect.gen(function*() {
      const commandId = yield* Identity.makeCommandId
      const error = new Rejected({ message: "no" })
      const rejected = yield* CommandOutcome.committedOrFail(CommandOutcome.rejected(commandId, error)).pipe(
        Effect.exit
      )
      const unknown = yield* CommandOutcome.committedOrFail(CommandOutcome.unknown(commandId)).pipe(Effect.exit)
      assert.deepStrictEqual(rejected, Exit.fail(error))
      assert.deepStrictEqual(unknown, Exit.fail(new CommandOutcome.CommandOutcomeUnknown({ commandId })))
    }))

  it("round trips every durable outcome through its generated schema", () => {
    const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000001")
    const Outcome = CommandOutcome.schema(Schema.Number, Rejected)
    const values = [
      CommandOutcome.rejected(commandId, new Rejected({ message: "no" })),
      CommandOutcome.durablyCommitted(commandId, 1),
      CommandOutcome.unknown(commandId)
    ]
    for (const value of values) {
      const encoded = Schema.encodeSync(Outcome)(value)
      assert.deepStrictEqual(Schema.decodeUnknownSync(Outcome)(encoded), value)
    }
  })
})
