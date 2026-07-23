import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Identity from "../src/Identity.js"

describe("Identity derivation", () => {
  it.effect("derives a valid DocumentId that shares the CommandId uuid", () =>
    Effect.gen(function*() {
      const commandId = yield* Identity.makeCommandId
      const documentId = Identity.documentIdFromCommandId(commandId)
      assert.isTrue(documentId.startsWith("doc_"))
      assert.strictEqual(documentId.slice(4), commandId.slice(4))
      assert.strictEqual(Schema.decodeUnknownSync(Identity.DocumentId)(documentId), documentId)
    }).pipe(Effect.provide(NodeCrypto.layer)))

  it("rejects a nonpositive ProjectionVersion and accepts one", () => {
    assert.strictEqual(Schema.decodeUnknownSync(Identity.ProjectionVersion)(1), 1)
    assert.throws(() => Schema.decodeUnknownSync(Identity.ProjectionVersion)(0))
  })
})
