import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as Rpc from "effect/unstable/rpc/Rpc"
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

it.effect("round trips a superseded checkpoint reason through the restore wire", () =>
  Effect.gen(function*() {
    const original = new ReplicaError.ReplicaError({
      reason: new ReplicaError.CheckpointSuperseded({
        documentIds: [
          Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
          Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
        ],
        attempts: 9
      })
    })
    const encoded = RestoreProtocol.encodeReplicaError(original, 4_096)
    const decoded = yield* Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)(encoded)
    assert.deepStrictEqual(RestoreProtocol.replicaErrorFromWire(decoded), original)
  }))

it.effect("drops whole document ids rather than truncating them when the budget is tight", () =>
  Effect.gen(function*() {
    const encoded = RestoreProtocol.encodeReplicaError(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.CheckpointSuperseded({
          documentIds: [
            Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
            Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
          ],
          attempts: 9
        })
      }),
      ReplicaLimits.minimumRestoreErrorBytes
    )
    assert.isTrue(RestoreProtocol.preflight(encoded, ReplicaLimits.minimumRestoreErrorBytes))
    // Every surviving id must still decode as a branded DocumentId, so none may be truncated.
    const decoded = yield* Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)(encoded)
    const reason = RestoreProtocol.replicaErrorFromWire(decoded).reason
    assert.strictEqual(reason._tag, "CheckpointSuperseded")
    if (reason._tag !== "CheckpointSuperseded") return
    assert.strictEqual(reason.attempts, 9)
    for (const documentId of reason.documentIds) {
      assert.isTrue(Schema.is(Identity.DocumentId)(documentId))
    }
  }))

it.effect("round trips a changed document lineage reason through the restore wire", () =>
  Effect.gen(function*() {
    const original = new ReplicaError.ReplicaError({
      reason: new ReplicaError.DocumentLineageChanged({
        documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
        localLineage: Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000003"),
        remoteLineage: Identity.genesisLineage
      })
    })
    const encoded = RestoreProtocol.encodeReplicaError(original, 4_096)
    const decoded = yield* Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)(encoded)
    assert.deepStrictEqual(RestoreProtocol.replicaErrorFromWire(decoded), original)
  }))

it.effect("drops both lineages whole rather than truncating them when the budget is tight", () =>
  Effect.gen(function*() {
    const encoded = RestoreProtocol.encodeReplicaError(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.DocumentLineageChanged({
          documentId: Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001"),
          localLineage: Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000003"),
          remoteLineage: Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000004")
        })
      }),
      ReplicaLimits.minimumRestoreErrorBytes
    )
    assert.isTrue(RestoreProtocol.preflight(encoded, ReplicaLimits.minimumRestoreErrorBytes))
    // A surviving lineage must still decode as its own branded value, so none may be truncated.
    const decoded = yield* Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)(encoded)
    const reason = RestoreProtocol.replicaErrorFromWire(decoded).reason
    assert.strictEqual(reason._tag, "DocumentLineageChanged")
    if (reason._tag !== "DocumentLineageChanged") return
    assert.strictEqual(reason.documentId, "doc_00000000-0000-4000-8000-000000000001")
    // Neither lineage fits beside the document id at the minimum budget, so the pair drops together.
    assert.strictEqual(reason.localLineage, Identity.genesisLineage)
    assert.strictEqual(reason.remoteLineage, Identity.genesisLineage)
  }))

const finishRpc = ReplicaRpc.group.requests.get("FinishRestoreBackup")!
const finishExitCodec = Schema.toCodecJson(Rpc.exitSchema(finishRpc))
const restoreFailure = (replica: string) =>
  new RestoreProtocol.RestoreResultRestoreFailure({
    error: { _tag: "RestoreBusy", replica }
  })
const restoreFailureReasonTag = (
  reason: Cause.Reason<unknown>
): ReplicaError.ReplicaError["reason"]["_tag"] | undefined => {
  if (
    !Cause.isFailReason(reason) ||
    !Schema.is(RestoreProtocol.RestoreResultRestoreFailure)(reason.error)
  ) {
    return undefined
  }
  return RestoreProtocol.replicaErrorFromWire(reason.error.error).reason._tag
}
const roundTripFinishExit = (
  exit: Exit.Exit<void, RestoreProtocol.RestoreResultFailure>
) =>
  Schema.encodeUnknownEffect(finishExitCodec)(exit).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(finishExitCodec))
  )

it.effect("preserves more than eight typed reasons through the production Finish RPC codec", () =>
  Effect.gen(function*() {
    const original = Exit.failCause(Cause.fromReasons(
      Array.from(
        { length: 9 },
        (_, index) => Cause.makeFailReason(restoreFailure(`replica-${index}`))
      )
    ))
    const decoded = yield* roundTripFinishExit(original)
    assert.isTrue(Exit.isFailure(decoded))
    if (Exit.isFailure(decoded)) {
      assert.deepStrictEqual(
        decoded.cause.reasons.map((reason) => reason._tag),
        Array.from({ length: 9 }, () => "Fail")
      )
      assert.deepStrictEqual(
        decoded.cause.reasons.flatMap((reason) => {
          const tag = restoreFailureReasonTag(reason)
          return tag === undefined ? [] : [tag]
        }),
        Array.from({ length: 9 }, () => "RestoreBusy")
      )
    }
  }))

it.effect("preserves mixed failure, redacted defect, and interruption through the production Finish RPC codec", () =>
  Effect.gen(function*() {
    const secret = "SELECT password=credential FROM /private/archive"
    const mixed = yield* roundTripFinishExit(
      Exit.failCause(Cause.fromReasons([
        Cause.makeFailReason(restoreFailure("typed")),
        Cause.makeDieReason(new Error(secret))
      ]))
    )
    assert.isTrue(Exit.isFailure(mixed))
    if (Exit.isFailure(mixed)) {
      assert.deepStrictEqual(mixed.cause.reasons.map((reason) => reason._tag), ["Fail", "Die"])
      const failureTag = mixed.cause.reasons
        .map(restoreFailureReasonTag)
        .find((tag) => tag !== undefined)
      assert.strictEqual(
        failureTag,
        "RestoreBusy"
      )
      const defect = mixed.cause.reasons.find(Cause.isDieReason)
      assert.notInclude(String(defect?.defect), "SELECT")
      assert.notInclude(String(defect?.defect), "password")
      assert.notInclude(String(defect?.defect), "/private/archive")
    }

    const interrupted = yield* roundTripFinishExit(
      Exit.failCause(Cause.fromReasons([
        Cause.makeFailReason(restoreFailure("typed")),
        Cause.makeInterruptReason(987)
      ]))
    )
    assert.isTrue(Exit.isFailure(interrupted))
    if (Exit.isFailure(interrupted)) {
      assert.deepStrictEqual(
        interrupted.cause.reasons.map((reason) => reason._tag),
        ["Fail", "Interrupt"]
      )
      const interrupt = interrupted.cause.reasons.find(Cause.isInterruptReason)
      assert.strictEqual(interrupt?.fiberId, 987)
    }
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
      new ReplicaError.TransientDecodeError({ topic: "activity", documentId, cause }),
      new ReplicaError.TransientEncodeError({ topic: "activity", documentId, cause }),
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
      new ReplicaError.OperationTimeout({ operation: "operation".repeat(32), timeoutMillis: 1 }),
      new ReplicaError.UnsupportedStorageFormatVersion({ observedVersion: 2, supportedVersion: 1 }),
      new ReplicaError.CheckpointSuperseded({
        documentIds: Array.from({ length: 32 }, () => documentId),
        attempts: 9
      }),
      new ReplicaError.DocumentLineageChanged({
        documentId,
        localLineage: Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000003"),
        remoteLineage: Identity.DocumentLineage.make("lin_00000000-0000-4000-8000-000000000004")
      })
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
  const secret = "SELECT password=credential FROM /private/archive"
  const redacted = RestoreProtocol.encodeDefect(new Error(secret), 4_096)
  assert.notInclude(redacted.name, secret)
  assert.notInclude(redacted.message, "SELECT")
  assert.notInclude(redacted.message, "password")
  assert.notInclude(redacted.message, "credential")
  assert.notInclude(redacted.message, "/private/archive")

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

it("rejects excess nested ArrayBuffer properties before schema decoding", () => {
  assert.isFalse(
    RestoreProtocol.preflight({
      _tag: "StorageUnavailable",
      cause: {
        name: "Error",
        message: "bounded",
        excess: new ArrayBuffer(1)
      }
    }, 4_096)
  )
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

it.effect("preserves every ReplicaGate operation at the minimum configured error budget", () =>
  Effect.gen(function*() {
    for (const operation of ["ReplicaGate.claim", "ReplicaGate.refresh", "ReplicaGate.validate"]) {
      const encoded = RestoreProtocol.encodeReplicaError(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.ReplicaMetadataMissing({ operation })
        }),
        ReplicaLimits.minimumRestoreErrorBytes
      )
      const decoded = yield* Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)(encoded)
      const reason = RestoreProtocol.replicaErrorFromWire(decoded).reason
      assert.strictEqual(reason._tag, "ReplicaMetadataMissing")
      if (reason._tag !== "ReplicaMetadataMissing") return
      assert.strictEqual(reason.operation, operation)
    }
  }))

// The page must still learn the replica has no identity even when the budget forces truncation, so
// the member keeps its own tag and only `operation` is shortened.
it.effect("keeps its own tag when the budget is exhausted and only truncates the operation", () =>
  Effect.gen(function*() {
    // The fixed shape costs 35 bytes (`_tag`, the tag itself, `operation`), so 40 leaves five.
    const maxBytes = 40
    const encoded = RestoreProtocol.encodeReplicaError(
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.ReplicaMetadataMissing({ operation: "ReplicaGate.validate" })
      }),
      maxBytes
    )
    const decoded = yield* Schema.decodeUnknownEffect(RestoreProtocol.RestoreWireError)(encoded)
    const reason = RestoreProtocol.replicaErrorFromWire(decoded).reason
    assert.strictEqual(reason._tag, "ReplicaMetadataMissing")
    if (reason._tag !== "ReplicaMetadataMissing") return
    assert.strictEqual(reason.operation, "Repli")
  }))

// `RestoreWireError` derives its field metadata from one record, so the three wire lists cannot drift
// from each other. Nothing makes that union agree with `ReplicaError.Reason`, which is a different
// module -- this guard covers exactly that seam, and it is the reason a forgotten reason is a test
// failure rather than a silently rejected preflight.
it("wires every ReplicaError reason into the restore wire field record", () => {
  const tags = ReplicaError.Reason.members.map((member) =>
    (member.fields._tag.ast as { readonly literal: string }).literal
  )
  assert.include(tags, "ReplicaMetadataMissing")
  for (const tag of tags) {
    assert.isTrue(
      RestoreProtocol.restoreWireErrorFields[tag] !== undefined,
      `${tag} is missing from restoreWireErrorFields`
    )
  }
})
