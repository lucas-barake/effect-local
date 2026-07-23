import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Identity from "../src/Identity.js"

describe("Identity", () => {
  it.effect("round trips every public identifier through its schema", () =>
    Effect.gen(function*() {
      const replicaId = yield* Identity.makeReplicaId
      const sessionId = yield* Identity.makeSessionId
      const commandId = yield* Identity.makeCommandId
      const documentId = yield* Identity.makeDocumentId
      const peerId = yield* Identity.makePeerId
      assert.strictEqual(Schema.decodeUnknownSync(Identity.ReplicaId)(replicaId), replicaId)
      assert.strictEqual(Schema.decodeUnknownSync(Identity.SessionId)(sessionId), sessionId)
      assert.strictEqual(Schema.decodeUnknownSync(Identity.CommandId)(commandId), commandId)
      assert.strictEqual(Schema.decodeUnknownSync(Identity.DocumentId)(documentId), documentId)
      assert.strictEqual(Schema.decodeUnknownSync(Identity.PeerId)(peerId), peerId)
      assert.throws(() => Schema.decodeUnknownSync(Identity.DocumentId)(commandId as unknown as Identity.DocumentId))
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it("rejects empty and malformed identifiers", () => {
    assert.throws(() => Schema.decodeUnknownSync(Identity.DocumentId)(""))
    assert.throws(() => Schema.decodeUnknownSync(Identity.DocumentId)("not-a-uuid"))
  })

  it("rejects an uppercase prefix", () => {
    assert.throws(() => Schema.decodeUnknownSync(Identity.DocumentId)("DOC_00000000-0000-4000-8000-00000000000a"))
  })

  it("rejects uppercase hex so a UUID cannot decode as two distinct identifiers", () => {
    const canonical = "doc_00000000-0000-4000-8000-00000000000a"
    const upperHex = "doc_00000000-0000-4000-8000-00000000000A"
    assert.strictEqual(Schema.decodeUnknownSync(Identity.DocumentId)(canonical), canonical)
    assert.throws(() => Schema.decodeUnknownSync(Identity.DocumentId)(upperHex))
  })

  it("rejects unsafe commit sequences", () => {
    assert.throws(() => Schema.decodeUnknownSync(Identity.CommitSequence)(Number.MAX_SAFE_INTEGER + 1))
    assert.strictEqual(Schema.decodeUnknownSync(Identity.CommitSequence)(0), 0)
  })
})
