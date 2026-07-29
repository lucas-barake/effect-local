import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as Wire from "../src/internal/wire.js"

const conflictLimits: Wire.ConflictLimits = {
  maxConflictDepth: 16,
  maxConflictNodes: 128,
  maxConflictAlternatives: 8,
  maxConflictPathSegments: 8,
  maxConflictValueBytes: 4_096
}

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

  it.effect("round trips bounded conflict JSON as flat text", () =>
    Effect.gen(function*() {
      const value = {
        snapshot: {
          value: { title: "edited" }
        },
        conflicts: [{
          path: {
            parents: [{ _tag: "Key", key: "messages" }],
            target: { _tag: "Index", index: 0 }
          },
          alternatives: [
            { id: "1@actor", value: { _tag: "Text", value: "first" } },
            {
              id: "2@actor",
              value: {
                _tag: "Map",
                entries: [{ key: "nested", value: { _tag: "Text", value: "second" } }]
              }
            }
          ]
        }]
      }
      const encoded = yield* Wire.encodeConflictText(value, conflictLimits)
      assert.strictEqual(typeof encoded, "string")
      assert.deepStrictEqual(yield* Wire.decodeConflictText(encoded, conflictLimits), value)
    }))

  it.effect("rejects cyclic, deeply nested, oversized, and invalid conflict text", () =>
    Effect.gen(function*() {
      const cyclic: Array<unknown> = []
      cyclic.push(cyclic)
      const deep = { value: { value: { value: { value: null } } } }
      const shallowLimits = { ...conflictLimits, maxConflictDepth: 2 }
      const byteLimits = { ...conflictLimits, maxConflictValueBytes: 8 }

      const cyclicExit = yield* Effect.exit(Wire.encodeConflictText(cyclic, conflictLimits))
      const deepExit = yield* Effect.exit(Wire.encodeConflictText(deep, shallowLimits))
      const oversizedEncodeExit = yield* Effect.exit(Wire.encodeConflictText("🚲🚲", byteLimits))
      const oversizedDecodeExit = yield* Effect.exit(Wire.decodeConflictText("\"🚲🚲\"", byteLimits))
      const invalidExit = yield* Effect.exit(Wire.decodeConflictText("{", conflictLimits))

      for (
        const exit of [
          cyclicExit,
          deepExit,
          oversizedEncodeExit,
          oversizedDecodeExit,
          invalidExit
        ]
      ) {
        assert(Exit.isFailure(exit))
        assert.strictEqual(exit.cause.reasons[0]?._tag, "Fail")
        if (exit.cause.reasons[0]?._tag === "Fail") {
          assert.strictEqual(exit.cause.reasons[0].error.reason._tag, "ProtocolMismatch")
        }
      }
    }))
})
