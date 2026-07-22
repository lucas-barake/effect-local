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

  it("preserves structured causes without arbitrary exception objects", () => {
    const error = new ReplicaError.ReplicaError({
      reason: new ReplicaError.StorageUnavailable({
        cause: new ReplicaError.SqlCause({
          message: "database closed",
          code: "SQLITE_CANTOPEN"
        })
      })
    })
    const encoded = Schema.encodeSync(ReplicaError.ReplicaError)(error)
    assert.strictEqual(encoded.reason._tag, "StorageUnavailable")
    if (encoded.reason._tag !== "StorageUnavailable") return
    assert.deepStrictEqual(encoded.reason.cause, {
      _tag: "SqlCause",
      message: "database closed",
      code: "SQLITE_CANTOPEN"
    })
    assert.throws(() =>
      Schema.decodeUnknownSync(ReplicaError.ReplicaError)({
        _tag: "ReplicaError",
        reason: { _tag: "StorageUnavailable", cause: new Error("no") }
      })
    )
  })
})
