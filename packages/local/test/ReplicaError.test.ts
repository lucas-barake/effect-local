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

  it("round trips unsupported storage format version metadata", () => {
    const error = new ReplicaError.ReplicaError({
      reason: new ReplicaError.UnsupportedStorageFormatVersion({
        observedVersion: 2,
        supportedVersion: 1
      })
    })
    const encoded = Schema.encodeSync(ReplicaError.ReplicaError)(error)
    assert.deepStrictEqual(encoded.reason, {
      _tag: "UnsupportedStorageFormatVersion",
      observedVersion: 2,
      supportedVersion: 1
    })
    assert.deepStrictEqual(Schema.decodeUnknownSync(ReplicaError.ReplicaError)(encoded), error)
  })

  it("round trips superseded checkpoint metadata", () => {
    const error = new ReplicaError.ReplicaError({
      reason: new ReplicaError.CheckpointSuperseded({
        documentIds: [Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")],
        attempts: 9
      })
    })
    const encoded = Schema.encodeSync(ReplicaError.ReplicaError)(error)
    assert.deepStrictEqual(encoded.reason, {
      _tag: "CheckpointSuperseded",
      documentIds: ["doc_00000000-0000-4000-8000-000000000001"],
      attempts: 9
    })
    assert.deepStrictEqual(Schema.decodeUnknownSync(ReplicaError.ReplicaError)(encoded), error)
  })

  it("round trips changed document lineage metadata", () => {
    const error = new ReplicaError.ReplicaError({
      reason: new ReplicaError.DocumentLineageChanged({
        documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
        localLineage: Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000002"),
        remoteLineage: Identity.genesisLineage
      })
    })
    const encoded = Schema.encodeSync(ReplicaError.ReplicaError)(error)
    assert.deepStrictEqual(encoded.reason, {
      _tag: "DocumentLineageChanged",
      documentId: "doc_00000000-0000-4000-8000-000000000001",
      localLineage: "lin_00000000-0000-4000-8000-000000000002",
      remoteLineage: ""
    })
    assert.deepStrictEqual(Schema.decodeUnknownSync(ReplicaError.ReplicaError)(encoded), error)
  })

  it("round trips bounded checkpoint rejection metadata", () => {
    const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
    const error = new ReplicaError.ReplicaError({
      reason: new ReplicaError.CheckpointRejected({
        documentId,
        reason: "local history diverges from the checkpoint"
      })
    })
    const encoded = Schema.encodeSync(ReplicaError.ReplicaError)(error)
    assert.deepStrictEqual(encoded.reason, {
      _tag: "CheckpointRejected",
      documentId: "doc_00000000-0000-4000-8000-000000000001",
      reason: "local history diverges from the checkpoint"
    })
    assert.deepStrictEqual(Schema.decodeUnknownSync(ReplicaError.ReplicaError)(encoded), error)
    assert.throws(() =>
      Schema.decodeUnknownSync(ReplicaError.ReplicaError)({
        _tag: "ReplicaError",
        reason: {
          _tag: "CheckpointRejected",
          documentId,
          reason: "x".repeat(1_025)
        }
      })
    )
  })

  it("round trips operation timeout metadata", () => {
    const error = new ReplicaError.ReplicaError({
      reason: new ReplicaError.OperationTimeout({
        operation: "Get",
        timeoutMillis: 2_000
      })
    })
    const encoded = Schema.encodeSync(ReplicaError.ReplicaError)(error)
    assert.deepStrictEqual(encoded.reason, {
      _tag: "OperationTimeout",
      operation: "Get",
      timeoutMillis: 2_000
    })
    assert.deepStrictEqual(Schema.decodeUnknownSync(ReplicaError.ReplicaError)(encoded), error)
  })
})
