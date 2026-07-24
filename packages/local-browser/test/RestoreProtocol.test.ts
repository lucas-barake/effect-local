import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as RestoreProtocol from "../src/internal/restoreProtocol.js"
import * as ReplicaRpc from "../src/ReplicaRpc.js"

it.effect("round trips a typed restore error through its wire schema", () =>
  Effect.gen(function*() {
    const original = new ReplicaError.ReplicaError({
      reason: new ReplicaError.ProtocolMismatch({
        expected: "expected protocol",
        observed: "observed protocol"
      })
    })
    const encoded = RestoreProtocol.encodeReplicaError(original, 4_096)
    const decoded = yield* Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)(encoded)
    assert.deepStrictEqual(RestoreProtocol.replicaErrorFromWire(decoded), original)
  }))

it.effect("bounds a multi-field restore error by the complete UTF-8 string budget", () =>
  Effect.gen(function*() {
    const maxBytes = 64
    const original = new ReplicaError.ReplicaError({
      reason: new ReplicaError.ProtocolMismatch({
        expected: "e".repeat(maxBytes),
        observed: "o".repeat(maxBytes)
      })
    })

    const encoded = RestoreProtocol.encodeReplicaError(original, maxBytes)
    assert.isTrue(RestoreProtocol.preflight(encoded, maxBytes))

    const decoded = yield* Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)(encoded)
    const roundTripped = RestoreProtocol.replicaErrorFromWire(decoded)
    assert.strictEqual(roundTripped.reason._tag, "ProtocolMismatch")
  }))

it.effect("encodes every restore error and defect at the minimum configured budget", () =>
  Effect.gen(function*() {
    assert.strictEqual(ReplicaLimits.minimumRestoreErrorBytes, 111)
    const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
    const commandId = Identity.CommandId.make("cmd_00000000-0000-4000-8000-000000000001")
    const cause = new Error("failure".repeat(32))
    const reasons: ReadonlyArray<ReplicaError.Reason> = [
      new ReplicaError.DocumentNotFound({ documentId }),
      new ReplicaError.DocumentDecodeError({ documentId, cause }),
      new ReplicaError.DocumentEncodeError({ documentId, cause }),
      new ReplicaError.UnsupportedDocumentVersion({
        documentId,
        observedVersion: 2,
        supportedVersion: 1
      }),
      new ReplicaError.ProjectionBlocked({ projection: "projection".repeat(32), cause }),
      new ReplicaError.CommandIdConflict({ commandId }),
      new ReplicaError.ReceiptOperationMismatch({
        commandId,
        expected: "expected".repeat(32),
        observed: "observed".repeat(32)
      }),
      new ReplicaError.StorageUnavailable({ cause }),
      new ReplicaError.CanonicalEncodeError({ cause }),
      new ReplicaError.StorageCorrupt({ cause }),
      new ReplicaError.QuotaExceeded({ resource: "resource".repeat(32), limit: 1 }),
      new ReplicaError.MigrationFailed({ migration: "migration".repeat(32), cause }),
      new ReplicaError.BackupInvalid({ cause }),
      new ReplicaError.BackupTooLarge({ limit: 1, observed: 2 }),
      new ReplicaError.RestoreBusy({ replica: "replica".repeat(32) }),
      new ReplicaError.RestoreFailed({ cause }),
      new ReplicaError.ProtocolMismatch({
        expected: "expected".repeat(32),
        observed: "observed".repeat(32)
      }),
      new ReplicaError.ReplicaFenced({
        expectedGeneration: Identity.WriterGeneration.make(1),
        observedGeneration: Identity.WriterGeneration.make(2)
      }),
      new ReplicaError.OperationTimeout({ operation: "operation".repeat(32), timeoutMillis: 1 })
    ]

    for (const reason of reasons) {
      const encoded = RestoreProtocol.encodeReplicaError(
        new ReplicaError.ReplicaError({ reason }),
        ReplicaLimits.minimumRestoreErrorBytes
      )
      assert.isTrue(
        RestoreProtocol.preflight(encoded, ReplicaLimits.minimumRestoreErrorBytes),
        reason._tag
      )
      assert.strictEqual(encoded._tag, reason._tag)
      yield* Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)(encoded)
      assert.deepStrictEqual(
        RestoreProtocol.encodeReplicaError(
          new ReplicaError.ReplicaError({ reason }),
          ReplicaLimits.minimumRestoreErrorBytes
        ),
        encoded
      )
    }

    const defect = RestoreProtocol.encodeDefect(cause, ReplicaLimits.minimumRestoreErrorBytes)
    assert.isTrue(RestoreProtocol.preflight(defect, ReplicaLimits.minimumRestoreErrorBytes))
    yield* Schema.decodeUnknownEffect(RestoreProtocol.BoundedErrorDescription)(defect)
  }))

it.effect("validates branded identities before reconstructing a typed error", () =>
  Effect.gen(function*() {
    const decoded = yield* Effect.exit(
      Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)({
        _tag: "DocumentNotFound",
        documentId: "not-a-document-id"
      })
    )
    assert.isTrue(Exit.isFailure(decoded))

    const valid = yield* Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)({
      _tag: "DocumentNotFound",
      documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
    })
    assert.strictEqual(RestoreProtocol.replicaErrorFromWire(valid).reason._tag, "DocumentNotFound")
  }))

it("bounds redacted defects and rejects hostile preflight values", () => {
  const throwing = Object.create(null)
  Object.defineProperty(throwing, "name", {
    get() {
      throw new Error("name getter")
    }
  })
  Object.defineProperty(throwing, "message", {
    get() {
      throw new Error("message getter")
    }
  })
  const description = RestoreProtocol.encodeDefect(throwing, 32)
  const encodedBytes = new TextEncoder().encode(description.name + description.message).byteLength
  assert.isAtMost(encodedBytes, 32)
  assert.isTrue(RestoreProtocol.preflight(description, 32))

  const recursive: { self?: unknown } = {}
  recursive.self = recursive
  assert.isFalse(RestoreProtocol.preflight(recursive, 4_096))

  const hostile = Object.create(null)
  Object.defineProperty(hostile, "value", {
    enumerable: true,
    get() {
      throw new Error("hostile getter")
    }
  })
  assert.isFalse(RestoreProtocol.preflight(hostile, 4_096))
})

it.effect("guards transferred MessagePort values", () =>
  Effect.gen(function*() {
    const channel = new MessageChannel()
    yield* Schema.decodeUnknownEffect(ReplicaRpc.MessagePortSchema)(channel.port1)

    const hostile = Object.create(null)
    Object.defineProperty(hostile, "postMessage", {
      get() {
        throw new Error("hostile getter")
      }
    })
    const decoded = yield* Effect.exit(Schema.decodeUnknownEffect(ReplicaRpc.MessagePortSchema)(hostile))
    assert.isTrue(Exit.isFailure(decoded))
    channel.port1.close()
    channel.port2.close()
  }))
