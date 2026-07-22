import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as Presence from "../src/Presence.js"

it.layer(NodeCrypto.layer)("Presence", (it) => {
  const Payload = Schema.Struct({ cursor: Schema.Number, status: Schema.Literals(["active", "idle"]) })

  it.effect("rejects a nonfinite normalized time to live", () =>
    Effect.gen(function*() {
      const error = yield* Effect.flip(Presence.make(Payload, { timeToLive: Number.POSITIVE_INFINITY }))
      assert.strictEqual(error._tag, "ReplicaError")
      assert.strictEqual(error.reason._tag, "ProtocolMismatch")
    }))

  it.effect("expires schema-valid transport peer state", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
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
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
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

  it.effect("removes received state explicitly and treats repeated removal as a no-op", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      yield* presence.receive(peerId, { cursor: 1, status: "active" })
      assert.strictEqual((yield* presence.values).length, 1)
      yield* presence.remove(peerId)
      assert.deepStrictEqual(yield* presence.values, [])
      yield* presence.remove(peerId)
      assert.deepStrictEqual(yield* presence.values, [])
    }))

  it.effect("rejects invalid payloads without replacing active state", () =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 second" })
      const peerId = yield* Identity.makePeerId
      yield* presence.receive(peerId, { cursor: 1, status: "active" })
      assert.strictEqual(
        (yield* Effect.exit(presence.receive(peerId, { cursor: "bad", status: "active" })))._tag,
        "Failure"
      )
      assert.deepStrictEqual((yield* presence.values)[0]?.value, { cursor: 1, status: "active" })
    }))

  it.effect("does not carry state into a new presence instance", () =>
    Effect.gen(function*() {
      const first = yield* Presence.make(Payload, { timeToLive: "1 second" })
      yield* first.receive(yield* Identity.makePeerId, { cursor: 1, status: "active" })
      const restarted = yield* Presence.make(Payload, { timeToLive: "1 second" })
      assert.deepStrictEqual(yield* restarted.values, [])
    }))
})
