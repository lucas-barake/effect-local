import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
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

  it.effect("rejects an inspection that exceeds its configured aggregate alternative limit", () =>
    Effect.gen(function*() {
      const first = Conflict.AlternativeId.make("1@actor")
      const second = Conflict.AlternativeId.make("2@actor")
      const third = Conflict.AlternativeId.make("3@actor")
      const fourth = Conflict.AlternativeId.make("4@actor")
      const inspection: Conflict.Inspection<Schema.Json> = {
        snapshot: {
          documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
          value: { title: "visible" },
          version: 1,
          heads: [],
          tombstone: false,
          projection: "Ready"
        },
        conflicts: [
          {
            path: { parents: [], target: { _tag: "Key", key: "first" } },
            visible: first,
            alternatives: [
              { id: first, value: "first" },
              { id: second, value: "second" }
            ]
          },
          {
            path: { parents: [], target: { _tag: "Key", key: "second" } },
            visible: third,
            alternatives: [
              { id: third, value: "third" },
              { id: fourth, value: "fourth" }
            ]
          }
        ]
      }
      const limits = { ...conflictLimits, maxConflictAlternatives: 3 }
      const exit = yield* Effect.exit(
        Wire.encodeConflict(
          Conflict.inspection(Schema.Json),
          inspection,
          limits,
          Conflict.preflightInspection
        )
      )

      assert(Exit.isFailure(exit))
      assert.strictEqual(exit.cause.reasons[0]?._tag, "Fail")
      if (exit.cause.reasons[0]?._tag === "Fail") {
        assert.strictEqual(exit.cause.reasons[0].error.reason._tag, "ProtocolMismatch")
      }
    }))

  it.effect("does not charge tagged schema wrappers against semantic conflict limits", () =>
    Effect.gen(function*() {
      const resolution = Conflict.Resolution.make({
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "value" } },
        choice: { _tag: "ReplaceValue", value: ["replacement"] }
      })
      const limits = {
        ...conflictLimits,
        maxConflictDepth: 3,
        maxConflictNodes: 32
      }

      const encoded = yield* Wire.encodeConflict(
        Conflict.Resolution,
        resolution,
        limits,
        Conflict.preflightResolution
      )
      assert.deepStrictEqual(
        yield* Wire.decodeConflict(
          Conflict.Resolution,
          encoded,
          limits,
          Conflict.preflightResolution
        ),
        resolution
      )
    }))

  it.effect("checks the conflict resolution when suspended encoding begins", () =>
    Effect.gen(function*() {
      const value = { replacement: "small" }
      const resolution = Conflict.Resolution.make({
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "value" } },
        choice: { _tag: "ReplaceValue", value }
      })
      const limits = { ...conflictLimits, maxConflictValueBytes: 256 }
      yield* Wire.encodeConflict(
        Conflict.Resolution,
        resolution,
        limits,
        Conflict.preflightResolution
      )
      const encoded = Wire.encodeConflict(
        Conflict.Resolution,
        resolution,
        limits,
        Conflict.preflightResolution
      )
      value.replacement = "x".repeat(512)

      const exit = yield* Effect.exit(encoded)
      assert(Exit.isFailure(exit))
      assert.strictEqual(exit.cause.reasons[0]?._tag, "Fail")
      if (exit.cause.reasons[0]?._tag === "Fail") {
        assert.strictEqual(exit.cause.reasons[0].error.reason._tag, "ProtocolMismatch")
      }
    }))

  it.effect("checks encoded bytes after semantic preflight through the production conflict schema", () =>
    Effect.gen(function*() {
      const limits = { ...conflictLimits, maxConflictValueBytes: 256 }
      const resolution = Conflict.Resolution.make({
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "value" } },
        choice: { _tag: "DeleteValue" }
      })
      let semanticPhase = true
      const hostile = new Proxy(resolution, {
        getOwnPropertyDescriptor(target, property) {
          const descriptor = Reflect.getOwnPropertyDescriptor(target, property)
          return property === "choice" &&
              !semanticPhase &&
              descriptor !== undefined &&
              "value" in descriptor
            ? { ...descriptor, value: { _tag: "ReplaceValue", value: "x".repeat(512) } }
            : descriptor
        }
      })
      const preflight: Wire.ConflictPreflight<Conflict.Resolution> = (value, currentLimits) => {
        const issue = Conflict.preflightResolution(value, currentLimits)
        semanticPhase = false
        return issue
      }

      const exit = yield* Effect.exit(
        Wire.encodeConflict(
          Conflict.Resolution,
          hostile,
          limits,
          preflight
        )
      )
      assert(Exit.isFailure(exit))
      assert.strictEqual(exit.cause.reasons[0]?._tag, "Fail")
      if (exit.cause.reasons[0]?._tag === "Fail") {
        assert.strictEqual(exit.cause.reasons[0].error.reason._tag, "ProtocolMismatch")
        if (exit.cause.reasons[0].error.reason._tag === "ProtocolMismatch") {
          assert.strictEqual(exit.cause.reasons[0].error.reason.expected, "bounded conflict JSON text")
        }
      }
      assert.isFalse(semanticPhase)
    }))

  it.effect("rejects invalid replacement values through the conflict schema", () =>
    Effect.gen(function*() {
      const invalid = {
        heads: [],
        path: { parents: [], target: { _tag: "Key", key: "value" } },
        choice: { _tag: "ReplaceValue", value: Number.NaN }
      }
      const exit = yield* Effect.exit(
        Wire.encodeConflict(Conflict.Resolution, invalid as never, conflictLimits)
      )

      assert(Exit.isFailure(exit))
      assert.strictEqual(exit.cause.reasons[0]?._tag, "Fail")
      if (exit.cause.reasons[0]?._tag === "Fail") {
        assert.strictEqual(exit.cause.reasons[0].error.reason._tag, "ProtocolMismatch")
      }
    }))

  it.effect("rejects deeply nested inspection JSON as a typed protocol mismatch", () =>
    Effect.gen(function*() {
      const nested = `${"{\"value\":".repeat(12_000)}null${"}".repeat(12_000)}`
      const encoded = JSON.stringify({
        snapshot: {
          documentId: "doc_00000000-0000-4000-8000-000000000001",
          value: "__nested__",
          version: 1,
          heads: [],
          tombstone: false,
          projection: "Ready"
        },
        conflicts: []
      }).replace("\"__nested__\"", nested)
      const exit = yield* Effect.exit(
        Wire.decodeConflict(
          Conflict.inspection(Schema.Json),
          encoded,
          { ...conflictLimits, maxConflictValueBytes: 1_000_000 },
          Conflict.preflightInspection
        )
      )

      assert(Exit.isFailure(exit))
      assert.strictEqual(exit.cause.reasons[0]?._tag, "Fail")
      if (exit.cause.reasons[0]?._tag === "Fail") {
        assert.strictEqual(exit.cause.reasons[0].error.reason._tag, "ProtocolMismatch")
      }
    }))

  it.effect("rejects deeply nested inspection encoding as a typed protocol mismatch", () =>
    Effect.gen(function*() {
      const deeplyNested = JSON.parse(
        `${"{\"value\":".repeat(12_000)}null${"}".repeat(12_000)}`
      ) as Schema.Json
      const inspection: Conflict.Inspection<Schema.Json> = {
        snapshot: {
          documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
          value: null,
          version: 1,
          heads: [],
          tombstone: false,
          projection: "Ready"
        },
        conflicts: []
      }
      let semanticPhase = true
      const hostile = new Proxy(inspection, {
        getOwnPropertyDescriptor(target, property) {
          const descriptor = Reflect.getOwnPropertyDescriptor(target, property)
          return property === "snapshot" &&
              !semanticPhase &&
              descriptor !== undefined &&
              "value" in descriptor
            ? { ...descriptor, value: { ...descriptor.value, value: deeplyNested } }
            : descriptor
        }
      })
      const preflight: Wire.ConflictPreflight<Conflict.Inspection<Schema.Json>> = (value, limits) => {
        const issue = Conflict.preflightInspection(value, limits)
        semanticPhase = false
        return issue
      }
      const exit = yield* Effect.exit(
        Wire.encodeConflict(
          Conflict.inspection(Schema.Json),
          hostile,
          { ...conflictLimits, maxConflictValueBytes: 1_000_000 },
          preflight
        )
      )

      assert(Exit.isFailure(exit))
      assert.strictEqual(exit.cause.reasons[0]?._tag, "Fail")
      if (exit.cause.reasons[0]?._tag === "Fail") {
        assert.strictEqual(exit.cause.reasons[0].error.reason._tag, "ProtocolMismatch")
      }
      assert.isFalse(semanticPhase)
    }))

  it.effect("encodes a stable descriptor snapshot instead of rereading hostile values", () =>
    Effect.gen(function*() {
      const deeplyNested = JSON.parse(
        `${"{\"value\":".repeat(12_000)}null${"}".repeat(12_000)}`
      ) as Schema.Json
      let phase: "Semantic" | "Snapshot" | "Schema" = "Semantic"
      const statefulValue = new Proxy({ value: Number.NaN }, {
        get(target, property, receiver) {
          if (property !== "value") return Reflect.get(target, property, receiver)
          if (phase === "Semantic") return null
          if (phase === "Snapshot") {
            phase = "Schema"
            return null
          }
          return deeplyNested
        }
      })
      const inspection: Conflict.Inspection<Schema.Json> = {
        snapshot: {
          documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
          value: statefulValue,
          version: 1,
          heads: [],
          tombstone: false,
          projection: "Ready"
        },
        conflicts: []
      }
      const preflight: Wire.ConflictPreflight<Conflict.Inspection<Schema.Json>> = (value, limits) => {
        const issue = Conflict.preflightInspection(value, limits)
        phase = "Snapshot"
        return issue
      }
      const exit = yield* Effect.exit(
        Wire.encodeConflict(
          Conflict.inspection(Schema.Json),
          inspection,
          { ...conflictLimits, maxConflictValueBytes: 1_000_000 },
          preflight
        )
      )

      assert(Exit.isFailure(exit))
      assert.strictEqual(exit.cause.reasons[0]?._tag, "Fail")
      if (exit.cause.reasons[0]?._tag === "Fail") {
        assert.strictEqual(exit.cause.reasons[0].error.reason._tag, "ProtocolMismatch")
      }
      assert.strictEqual(phase, "Snapshot")
    }))

  it.effect("bounds wide container inspection by the remaining node budget", () =>
    Effect.gen(function*() {
      let arrayDescriptorReads = 0
      const wideArray = new Proxy(Array.from({ length: 100_000 }, () => null as unknown), {
        getOwnPropertyDescriptor(target, property) {
          arrayDescriptorReads++
          return Reflect.getOwnPropertyDescriptor(target, property)
        }
      })
      let objectDescriptorReads = 0
      const wideObject = new Proxy(
        Object.fromEntries(Array.from({ length: 100_000 }, (_, index) => [`key${index}`, null])),
        {
          getOwnPropertyDescriptor(target, property) {
            objectDescriptorReads++
            return Reflect.getOwnPropertyDescriptor(target, property)
          }
        }
      )
      const limits = { ...conflictLimits, maxConflictNodes: 2, maxConflictValueBytes: 16 * 1024 * 1024 }
      const arrayExit = yield* Effect.exit(Wire.encodeConflictText(wideArray, limits))
      const objectExit = yield* Effect.exit(Wire.encodeConflictText(wideObject, limits))

      for (const exit of [arrayExit, objectExit]) {
        assert(Exit.isFailure(exit))
        assert.strictEqual(exit.cause.reasons[0]?._tag, "Fail")
        if (exit.cause.reasons[0]?._tag === "Fail") {
          assert.strictEqual(exit.cause.reasons[0].error.reason._tag, "ProtocolMismatch")
        }
      }
      assert.isAtMost(arrayDescriptorReads, 1)
      assert.isAtMost(objectDescriptorReads, 1)
    }))

  it.effect("preserves sparse, accessor, and unsafe key rejection", () =>
    Effect.gen(function*() {
      const sparse: Array<unknown> = []
      sparse.length = 1
      const accessorArray: Array<unknown> = []
      accessorArray.length = 1
      Object.defineProperty(accessorArray, "0", { enumerable: true, get: () => null })
      const accessorObject = {}
      Object.defineProperty(accessorObject, "value", { enumerable: true, get: () => null })
      const unsafeObject = {}
      Object.defineProperty(unsafeObject, "__proto__", { enumerable: true, value: null })

      for (const value of [sparse, accessorArray, accessorObject, unsafeObject]) {
        const exit = yield* Effect.exit(Wire.encodeConflictText(value, conflictLimits))
        assert(Exit.isFailure(exit))
        assert.strictEqual(exit.cause.reasons[0]?._tag, "Fail")
        if (exit.cause.reasons[0]?._tag === "Fail") {
          assert.strictEqual(exit.cause.reasons[0].error.reason._tag, "ProtocolMismatch")
        }
      }
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
