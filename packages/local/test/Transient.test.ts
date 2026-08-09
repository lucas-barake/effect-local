import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Document from "../src/Document.js"
import * as Identity from "../src/Identity.js"
import * as Transient from "../src/Transient.js"

describe("Transient", () => {
  class Typing extends Schema.TaggedClass<Typing>()("Typing", { userId: Schema.String }) {}
  class ReadPosition extends Schema.TaggedClass<ReadPosition>()("ReadPosition", { messageId: Schema.String }) {}
  const Chat = Document.make("Chat", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const ChatTransient = Transient.make("ChatActivity", {
    document: Chat,
    payload: Schema.Union([Typing, ReadPosition])
  })

  it.effect("encodes, routes, and decodes a tagged payload", () =>
    Effect.scoped(Effect.gen(function*() {
      const peerId = yield* Identity.makePeerId
      const documentId = yield* Identity.makeDocumentId
      const deliveries = yield* PubSub.sliding<Transient.Delivery>(2)
      const subscription = yield* PubSub.subscribe(deliveries)
      const Transport = Layer.succeed(Transient.Transport, {
        send: (peerId, documentId, payload) =>
          PubSub.publish(deliveries, { peerId, documentId, payload }).pipe(Effect.asVoid),
        messages: Stream.fromSubscription(subscription)
      })
      const program = Effect.gen(function*() {
        const forDocument = yield* ChatTransient.client
        const client = forDocument(documentId)
        const pull = yield* Stream.toPull(client.messages)
        yield* client.publish(peerId, new Typing({ userId: "lucas" }))
        const received = yield* pull
        assert.deepStrictEqual(received[0], {
          peerId,
          payload: new Typing({ userId: "lucas" })
        })
      })
      yield* program.pipe(
        Effect.provide(Transient.layer([ChatTransient])),
        Effect.provide(Transport)
      )
    })).pipe(Effect.provide(NodeCrypto.layer)))

  it("rejects invalid contract names", () => {
    assert.throws(() => Transient.make("", { document: Chat, payload: Typing }))
    assert.throws(() => Transient.make("$reserved", { document: Chat, payload: Typing }))
  })

  it.effect("fails malformed wire values with a typed decode error", () =>
    Effect.gen(function*() {
      const peerId = yield* Identity.makePeerId
      const documentId = yield* Identity.makeDocumentId
      const Transport = Layer.succeed(Transient.Transport, {
        send: () => Effect.void,
        messages: Stream.make({
          peerId,
          documentId,
          payload: new TextEncoder().encode("not-json")
        })
      })
      const exit = yield* Effect.gen(function*() {
        const forDocument = yield* ChatTransient.client
        return yield* Stream.runHead(forDocument(documentId).messages)
      }).pipe(
        Effect.provide(Transient.layer([ChatTransient])),
        Effect.provide(Transport),
        Effect.exit
      )
      assert.strictEqual(exit._tag, "Failure")
      if (exit._tag === "Failure") {
        const reason = exit.cause.reasons[0]
        assert.strictEqual(reason?._tag, "Fail")
        if (reason?._tag === "Fail") {
          assert.strictEqual(reason.error.reason._tag, "TransientDecodeError")
        }
      }
    }).pipe(Effect.provide(NodeCrypto.layer)))
})
