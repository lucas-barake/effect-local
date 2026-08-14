import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Ephemeral from "../src/Ephemeral.js"

const ConversationId = Schema.String.pipe(Schema.brand("ConversationId"))

const Typing = Ephemeral.make("Typing", {
  kind: "event",
  payload: {
    conversationId: ConversationId,
    active: Schema.Boolean
  }
})

const ReadPosition = Ephemeral.make("ReadPosition", {
  kind: "state",
  key: ConversationId,
  payload: {
    messageId: Schema.String
  }
})

describe("Ephemeral definitions", () => {
  it("rejects invalid names at construction", () => {
    assert.throws(() => Ephemeral.make("", { kind: "event" }), /must be nonempty/)
    assert.throws(() => Ephemeral.make("$Typing", { kind: "event" }), /must not start/)
    assert.throws(
      () => Ephemeral.make("x".repeat(257), { kind: "event" }),
      /must be at most/
    )
  })

  it("rejects duplicate names when a group is constructed", () => {
    const Duplicate = Ephemeral.make("Typing", { kind: "event", payload: { active: Schema.Boolean } })
    assert.throws(() => Ephemeral.group([Typing, Duplicate]), /Duplicate ephemeral name: Typing/)
    const grouped = Ephemeral.group([Typing, ReadPosition])
    assert.strictEqual(grouped.byName.get("Typing"), Typing)
    assert.strictEqual(grouped.byName.get("ReadPosition"), ReadPosition)
  })

  it("exposes the declared kind and name", () => {
    assert.strictEqual(Typing.kind, "event")
    assert.strictEqual(Typing.name, "Typing")
    assert.strictEqual(ReadPosition.kind, "state")
    assert.strictEqual(ReadPosition.name, "ReadPosition")
  })

  it.effect(
    "normalizes field payload input into a wire struct schema",
    Effect.fnUntraced(function*() {
      const encoded = yield* Schema.encodeEffect(Typing.payloadSchema)({
        conversationId: ConversationId.make("conversation-1"),
        active: true
      })
      assert.deepStrictEqual(encoded, { conversationId: "conversation-1", active: true })
      const decoded = yield* Schema.decodeUnknownEffect(ReadPosition.payloadSchema)({
        messageId: "message-9"
      })
      assert.deepStrictEqual(decoded, { messageId: "message-9" })
    })
  )

  it.effect(
    "state definitions carry a key codec that encodes to the wire string",
    Effect.fnUntraced(function*() {
      const encoded = yield* Schema.encodeEffect(ReadPosition.keySchema)(
        ConversationId.make("conversation-2")
      )
      assert.strictEqual(encoded, "conversation-2")
    })
  )

  it.effect(
    "defaults a missing payload to the void wire schema",
    Effect.fnUntraced(function*() {
      const Ping = Ephemeral.make("Ping", { kind: "event" })
      const encoded = yield* Schema.encodeEffect(Ping.payloadSchema)(undefined)
      assert.strictEqual(encoded, null)
    })
  )
})
