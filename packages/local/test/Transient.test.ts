import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as Document from "../src/Document.js"
import * as Identity from "../src/Identity.js"
import * as Transient from "../src/Transient.js"

it.layer(NodeCrypto.layer)("Transient", (layered) => {
  class Typing extends Schema.TaggedClass<Typing>()("Typing", { userId: Schema.String }) {}
  class ReadPosition extends Schema.TaggedClass<ReadPosition>()("ReadPosition", { messageId: Schema.String }) {}
  const Chat = Document.make("Chat", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const ChatTransient = Transient.make("ChatActivity", {
    document: Chat,
    payload: Schema.Union([Typing, ReadPosition])
  })

  layered.effect("encodes, routes, and decodes a tagged payload", () =>
    Effect.gen(function*() {
      const peerId = yield* Identity.makePeerId
      const documentId = yield* Identity.makeDocumentId
      const deliveries = yield* Queue.make<Transient.Delivery>()
      const received = yield* Effect.gen(function*() {
        const forDocument = yield* ChatTransient.client
        const client = forDocument(documentId)
        yield* client.publish(peerId, new Typing({ userId: "lucas" }))
        return yield* client.messages.pipe(Stream.take(1), Stream.runCollect)
      }).pipe(
        Effect.provide(Transient.layer([ChatTransient])),
        Effect.provideService(Transient.Transport, {
          send: (innerPeerId, innerDocumentId, payload) =>
            Effect.asVoid(Queue.offer(deliveries, { peerId: innerPeerId, documentId: innerDocumentId, payload })),
          messages: Stream.fromQueue(deliveries)
        })
      )
      assert.deepStrictEqual(received, [{ peerId, payload: new Typing({ userId: "lucas" }) }])
    }))

  layered("rejects invalid contract names", () => {
    assert.throws(() => Transient.make("", { document: Chat, payload: Typing }))
    assert.throws(() => Transient.make("$reserved", { document: Chat, payload: Typing }))
  })

  layered.effect("fails malformed wire values with a typed decode error", () =>
    Effect.gen(function*() {
      const peerId = yield* Identity.makePeerId
      const documentId = yield* Identity.makeDocumentId
      const error = yield* Effect.gen(function*() {
        const forDocument = yield* ChatTransient.client
        return yield* Stream.runHead(forDocument(documentId).messages)
      }).pipe(
        Effect.provide(Transient.layer([ChatTransient])),
        Effect.provideService(Transient.Transport, {
          send: () => Effect.void,
          messages: Stream.make({ peerId, documentId, payload: new TextEncoder().encode("not-json") })
        }),
        Effect.flip
      )
      assert.strictEqual(error.reason._tag, "TransientDecodeError")
    }))
})
