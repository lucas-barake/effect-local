import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as Identity from "../src/Identity.js"

describe("Identity", () => {
  it("round trips every public identifier through its schema", () => {
    const commandId = Identity.makeCommandId()
    const documentId = Identity.makeDocumentId()
    assert.strictEqual(Schema.decodeUnknownSync(Identity.CommandId)(commandId), commandId)
    assert.strictEqual(Schema.decodeUnknownSync(Identity.DocumentId)(documentId), documentId)
    assert.throws(() => Schema.decodeUnknownSync(Identity.DocumentId)(commandId as unknown as Identity.DocumentId))
  })

  it("rejects empty and malformed identifiers", () => {
    assert.throws(() => Schema.decodeUnknownSync(Identity.DocumentId)(""))
    assert.throws(() => Schema.decodeUnknownSync(Identity.DocumentId)("not-a-uuid"))
  })

  it("rejects unsafe commit sequences", () => {
    assert.throws(() => Schema.decodeUnknownSync(Identity.CommitSequence)(Number.MAX_SAFE_INTEGER + 1))
    assert.strictEqual(Schema.decodeUnknownSync(Identity.CommitSequence)(0), 0)
  })
})
