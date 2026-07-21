import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as Presence from "../src/Presence.js"

describe("Presence", () => {
  const Payload = Schema.Struct({ cursor: Schema.Number, status: Schema.Literals(["active", "idle"]) })

  it.effect("expires schema-valid transport peer state", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { ttlMillis: 1_000 })
      const peerId = Identity.makePeerId()
      yield* presence.receive(peerId, { cursor: 3, status: "active" })
      assert.deepStrictEqual(yield* presence.values, [{
        peerId,
        value: { cursor: 3, status: "active" },
        expiresAtMillis: 1_000,
        identity: "transport-peer"
      }])
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("removes scoped publications without removing newer state", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { ttlMillis: 1_000 })
      const peerId = Identity.makePeerId()
      yield* Effect.scoped(Effect.gen(function*() {
        yield* presence.publish(peerId, { cursor: 1, status: "active" })
        yield* presence.receive(peerId, { cursor: 2, status: "idle" })
      }))
      const entries = yield* presence.values
      assert.strictEqual(entries.length, 1)
      assert.deepStrictEqual(entries[0]?.value, { cursor: 2, status: "idle" })
      assert.strictEqual(entries[0]?.identity, "transport-peer")
      assert.isFalse("userId" in entries[0]!)
    }))

  it.effect("rejects invalid payloads without replacing active state", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { ttlMillis: 1_000 })
      const peerId = Identity.makePeerId()
      yield* presence.receive(peerId, { cursor: 1, status: "active" })
      assert.strictEqual(
        (yield* Effect.exit(presence.receive(peerId, { cursor: "bad", status: "active" })))._tag,
        "Failure"
      )
      assert.deepStrictEqual((yield* presence.values)[0]?.value, { cursor: 1, status: "active" })
    }))

  it.effect("does not carry state into a new presence instance", () =>
    Effect.gen(function*() {
      const first = yield* Presence.make(Payload, { ttlMillis: 1_000 })
      yield* first.receive(Identity.makePeerId(), { cursor: 1, status: "active" })
      const restarted = yield* Presence.make(Payload, { ttlMillis: 1_000 })
      assert.deepStrictEqual(yield* restarted.values, [])
    }))
})
