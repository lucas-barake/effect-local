import { NodeCrypto } from "@effect/platform-node"
import type * as BrowserPeerSession from "@lucas-barake/effect-local-browser/PeerSession"
import * as Presence from "@lucas-barake/effect-local-browser/Presence"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import assert from "node:assert/strict"

class InMemoryPeerSession extends Context.Service<InMemoryPeerSession, BrowserPeerSession.PeerSession>()(
  "@effect-local/advanced-api/InMemoryPeerSession"
) {}

const PeerSessionLive = Layer.effect(
  InMemoryPeerSession,
  Effect.gen(function*() {
    const peerId = yield* Identity.makePeerId
    const crypto = yield* Crypto.Crypto
    const dirty = yield* Ref.make(new Set<Identity.DocumentId>())
    const observed = yield* Ref.make(new Set<Identity.DocumentId>())

    return InMemoryPeerSession.of({
      peerId,
      connectionEpoch: yield* crypto.randomUUIDv4,
      markDirty: (documentId) => Ref.update(dirty, (documents) => new Set(documents).add(documentId)),
      flush: Effect.gen(function*() {
        const documents = yield* Ref.getAndSet(dirty, new Set())
        yield* Ref.update(observed, (current) => new Set([...current, ...documents]))
      }),
      observedByPeer: (documentId) => Ref.get(observed).pipe(Effect.map((documents) => documents.has(documentId))),
      durableConfirmation: () => Effect.succeed(false)
    })
  })
)

const Cursor = Schema.Struct({
  cursor: Schema.Number,
  status: Schema.Literals(["active", "idle"])
})

const program = Effect.gen(function*() {
  const session = yield* InMemoryPeerSession
  const presence = yield* Presence.make(Cursor, { timeToLive: "1 second" })
  const documentId = yield* Identity.makeDocumentId
  const remotePeerId = yield* Identity.makePeerId

  yield* Effect.scoped(Effect.gen(function*() {
    yield* presence.publish(session.peerId, { cursor: 1, status: "active" })
    yield* session.markDirty(documentId)
    yield* session.flush
    assert.equal(yield* session.observedByPeer(documentId), true)

    yield* presence.receive(remotePeerId, { cursor: 8, status: "idle" })
    const invalid = yield* Effect.exit(presence.receive(remotePeerId, { cursor: "invalid", status: "idle" }))
    assert.equal(Exit.isFailure(invalid), true)

    const values = yield* presence.values
    assert.equal(values.length, 2)
    assert.deepEqual(values.find((entry) => entry.peerId === remotePeerId)?.value, { cursor: 8, status: "idle" })
    yield* Effect.log("active presence", values)
  }))

  assert.equal((yield* presence.values).some((entry) => entry.peerId === session.peerId), false)
  yield* presence.remove(remotePeerId)
  assert.deepEqual(yield* presence.values, [])

  yield* presence.receive(remotePeerId, { cursor: 13, status: "active" })
  assert.equal((yield* presence.values).length, 1)
  yield* Effect.sleep("1100 millis")
  assert.deepEqual(yield* presence.values, [])
  yield* Effect.log("presence expired", yield* presence.values)
})

Effect.runPromise(program.pipe(Effect.provide(PeerSessionLive), Effect.provide(NodeCrypto.layer)))
