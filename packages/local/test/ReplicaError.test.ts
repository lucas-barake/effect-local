import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as Identity from "../src/Identity.js"
import * as ReplicaError from "../src/ReplicaError.js"

describe("ReplicaError", () => {
  it("round trips public reasons", () => {
    const error = new ReplicaError.ReplicaError({
      reason: new ReplicaError.DocumentNotFound({
        documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
      })
    })
    const encoded = Schema.encodeSync(ReplicaError.ReplicaError)(error)
    assert.deepStrictEqual(Schema.decodeUnknownSync(ReplicaError.ReplicaError)(encoded), error)
  })

  it("round trips arbitrary defect causes", () => {
    const error = new ReplicaError.ReplicaError({
      reason: new ReplicaError.StorageUnavailable({
        cause: new Error("database closed")
      })
    })
    const encoded = Schema.encodeSync(ReplicaError.ReplicaError)(error)
    assert.strictEqual(encoded.reason._tag, "StorageUnavailable")
    if (encoded.reason._tag !== "StorageUnavailable") return
    assert.deepStrictEqual(encoded.reason.cause, { name: "Error", message: "database closed" })
    const decoded = Schema.decodeUnknownSync(ReplicaError.ReplicaError)(encoded)
    assert.isTrue(Schema.is(Schema.Error())(decoded.reason.cause))
    if (!Schema.is(Schema.Error())(decoded.reason.cause)) return
    assert.strictEqual(decoded.reason.cause.message, "database closed")
  })
})
