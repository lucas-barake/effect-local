import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as CommandOutcome from "../src/CommandOutcome.js"
import * as Identity from "../src/Identity.js"
import * as ReplicaError from "../src/ReplicaError.js"

const makeError = (message: string): Error => {
  // oxlint-disable-next-line effect/noNewError -- this fixture must preserve an arbitrary Error value
  return new Error(message)
}

class Rejected extends Schema.TaggedErrorClass<Rejected>("@test/CommandOutcome/Rejected")("Rejected", {
  message: Schema.String
}) {}

it.layer(NodeCrypto.layer)("CommandOutcome", (layered) => {
  layered.effect("extracts committed values", () =>
    Effect.gen(function*() {
      const commandId = yield* Identity.makeCommandId
      const value = yield* CommandOutcome.committedOrFail(CommandOutcome.durablyCommitted(commandId, 42))
      assert.strictEqual(value, 42)
    }))

  layered.effect("fails with the domain rejection or a tagged ambiguity error", () =>
    Effect.gen(function*() {
      const commandId = yield* Identity.makeCommandId
      const error = new Rejected({ message: "no" })
      const rejected = yield* CommandOutcome.committedOrFail(CommandOutcome.rejected(commandId, error)).pipe(
        Effect.exit
      )
      const cause = makeError("the owner never answered")
      const unknown = yield* CommandOutcome.committedOrFail(CommandOutcome.unknown(commandId), cause).pipe(
        Effect.exit
      )
      assert.deepStrictEqual(rejected, Exit.fail(error))
      assert.deepStrictEqual(
        unknown,
        Exit.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CommandOutcomeUnknown({ commandId, cause })
          })
        )
      )
    }))

  layered.effect("carries an outcome back out to a boundary that has to transmit one", () =>
    Effect.gen(function*() {
      const commandId = yield* Identity.makeCommandId
      const error = new Rejected({ message: "no" })
      const toOutcome = <A, E,>(effect: Effect.Effect<A, E>) => CommandOutcome.toOutcome(commandId, effect)

      assert.deepStrictEqual(
        yield* toOutcome(Effect.succeed(42)),
        CommandOutcome.durablyCommitted(commandId, 42)
      )
      // A declared rejection becomes `Rejected` rather than staying a failure.
      assert.deepStrictEqual(
        yield* toOutcome(Effect.fail(error)),
        CommandOutcome.rejected(commandId, error)
      )
      // Ambiguity is the third command result, so it crosses as a value the peer can look up.
      assert.deepStrictEqual(
        yield* toOutcome(Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.CommandOutcomeUnknown({
              commandId,
              cause: makeError("lost")
            })
          })
        )),
        CommandOutcome.unknown(commandId)
      )
      // Any other replica failure is not a command result and stays in the error channel.
      const storage = new ReplicaError.ReplicaError({
        reason: new ReplicaError.StorageUnavailable({
          cause: makeError("disk")
        })
      })
      assert.deepStrictEqual(yield* toOutcome(Effect.fail(storage)).pipe(Effect.exit), Exit.fail(storage))
    }))

  layered("round trips every durable outcome through its generated schema", () => {
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
