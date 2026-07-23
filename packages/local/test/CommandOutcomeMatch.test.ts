import { assert, describe, it } from "@effect/vitest"
import * as CommandOutcome from "../src/CommandOutcome.js"
import * as Identity from "../src/Identity.js"

describe("CommandOutcome match", () => {
  it("dispatches each tag to its handler", () => {
    const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000001")
    const outcomes = [
      CommandOutcome.rejected(commandId, "boom"),
      CommandOutcome.durablyCommitted(commandId, 42),
      CommandOutcome.unknown(commandId)
    ]
    const labels = outcomes.map((outcome) =>
      CommandOutcome.match(outcome, {
        onRejected: (rejected) => `rejected:${rejected.error}`,
        onCommitted: (committed) => `committed:${committed.value}`,
        onUnknown: (unknown) => `unknown:${unknown.commandId}`
      })
    )
    assert.deepStrictEqual(labels, [
      "rejected:boom",
      "committed:42",
      `unknown:${commandId}`
    ])
  })
})
