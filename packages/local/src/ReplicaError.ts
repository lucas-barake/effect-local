import * as Schema from "effect/Schema"
import * as Identity from "./Identity.js"

export class DocumentNotFound extends Schema.TaggedErrorClass<DocumentNotFound>(
  "@lucas-barake/effect-local/ReplicaError/DocumentNotFound"
)("DocumentNotFound", {
  documentId: Identity.DocumentId
}) {}

export class DocumentDecodeError extends Schema.TaggedErrorClass<DocumentDecodeError>(
  "@lucas-barake/effect-local/ReplicaError/DocumentDecodeError"
)("DocumentDecodeError", {
  documentId: Identity.DocumentId,
  cause: Schema.Defect()
}) {}

export class DocumentEncodeError extends Schema.TaggedErrorClass<DocumentEncodeError>(
  "@lucas-barake/effect-local/ReplicaError/DocumentEncodeError"
)("DocumentEncodeError", {
  documentId: Identity.DocumentId,
  cause: Schema.Defect()
}) {}

export class TransientDecodeError extends Schema.TaggedErrorClass<TransientDecodeError>(
  "@lucas-barake/effect-local/ReplicaError/TransientDecodeError"
)("TransientDecodeError", {
  topic: Schema.String,
  documentId: Identity.DocumentId,
  cause: Schema.Defect()
}) {}

export class TransientEncodeError extends Schema.TaggedErrorClass<TransientEncodeError>(
  "@lucas-barake/effect-local/ReplicaError/TransientEncodeError"
)("TransientEncodeError", {
  topic: Schema.String,
  documentId: Identity.DocumentId,
  cause: Schema.Defect()
}) {}

export class UnsupportedDocumentVersion extends Schema.TaggedErrorClass<UnsupportedDocumentVersion>(
  "@lucas-barake/effect-local/ReplicaError/UnsupportedDocumentVersion"
)("UnsupportedDocumentVersion", {
  documentId: Identity.DocumentId,
  observedVersion: Schema.Int,
  supportedVersion: Schema.Int
}) {}

export class ProjectionBlocked extends Schema.TaggedErrorClass<ProjectionBlocked>(
  "@lucas-barake/effect-local/ReplicaError/ProjectionBlocked"
)("ProjectionBlocked", {
  projection: Schema.String,
  cause: Schema.Defect()
}) {}

export class CommandIdConflict extends Schema.TaggedErrorClass<CommandIdConflict>(
  "@lucas-barake/effect-local/ReplicaError/CommandIdConflict"
)("CommandIdConflict", {
  commandId: Identity.CommandId
}) {}

export class ReceiptOperationMismatch extends Schema.TaggedErrorClass<ReceiptOperationMismatch>(
  "@lucas-barake/effect-local/ReplicaError/ReceiptOperationMismatch"
)("ReceiptOperationMismatch", {
  commandId: Identity.CommandId,
  expected: Schema.String,
  observed: Schema.String
}) {}

export class StorageUnavailable extends Schema.TaggedErrorClass<StorageUnavailable>(
  "@lucas-barake/effect-local/ReplicaError/StorageUnavailable"
)("StorageUnavailable", { cause: Schema.Defect() }) {}

export class CanonicalEncodeError extends Schema.TaggedErrorClass<CanonicalEncodeError>(
  "@lucas-barake/effect-local/ReplicaError/CanonicalEncodeError"
)("CanonicalEncodeError", { cause: Schema.Defect() }) {}

export class StorageCorrupt extends Schema.TaggedErrorClass<StorageCorrupt>(
  "@lucas-barake/effect-local/ReplicaError/StorageCorrupt"
)("StorageCorrupt", { cause: Schema.Defect() }) {}

/**
 * The replica's metadata singleton row is gone, so the replica has no identity.
 *
 * Replica-wide and fatal, which is why it is not `StorageCorrupt`: that reason means one document's
 * stored bytes are unusable, and consumers treat it that way. `ReplicaEvolution` quarantines the one
 * document it was reading, and `BackupStore` reports it as an invalid backup. Routing a lost replica
 * identity through either of those blames the wrong thing. `ReplicaHealth` already reports this
 * condition separately, as `Failed { "Replica metadata is missing" }`.
 *
 * `operation` names the read that observed the absence, because the reason carries no cause of its
 * own: nothing failed, a row that must exist simply is not there, and the only useful context is
 * where that was noticed.
 */
export class ReplicaMetadataMissing extends Schema.TaggedErrorClass<ReplicaMetadataMissing>(
  "@lucas-barake/effect-local/ReplicaError/ReplicaMetadataMissing"
)("ReplicaMetadataMissing", { operation: Schema.String }) {}

export class QuotaExceeded extends Schema.TaggedErrorClass<QuotaExceeded>(
  "@lucas-barake/effect-local/ReplicaError/QuotaExceeded"
)("QuotaExceeded", {
  resource: Schema.String,
  limit: Schema.Int
}) {}

export class MigrationFailed extends Schema.TaggedErrorClass<MigrationFailed>(
  "@lucas-barake/effect-local/ReplicaError/MigrationFailed"
)("MigrationFailed", {
  migration: Schema.String,
  cause: Schema.Defect()
}) {}

export class BackupInvalid extends Schema.TaggedErrorClass<BackupInvalid>(
  "@lucas-barake/effect-local/ReplicaError/BackupInvalid"
)("BackupInvalid", { cause: Schema.Defect() }) {}

export class BackupTooLarge extends Schema.TaggedErrorClass<BackupTooLarge>(
  "@lucas-barake/effect-local/ReplicaError/BackupTooLarge"
)("BackupTooLarge", {
  limit: Schema.Int,
  observed: Schema.Int
}) {}

export class RestoreBusy extends Schema.TaggedErrorClass<RestoreBusy>(
  "@lucas-barake/effect-local/ReplicaError/RestoreBusy"
)("RestoreBusy", { replica: Schema.String }) {}

export class RestoreFailed extends Schema.TaggedErrorClass<RestoreFailed>(
  "@lucas-barake/effect-local/ReplicaError/RestoreFailed"
)("RestoreFailed", { cause: Schema.Defect() }) {}

export class ProtocolMismatch extends Schema.TaggedErrorClass<ProtocolMismatch>(
  "@lucas-barake/effect-local/ReplicaError/ProtocolMismatch"
)("ProtocolMismatch", {
  expected: Schema.String,
  observed: Schema.String
}) {}

export class ReplicaFenced extends Schema.TaggedErrorClass<ReplicaFenced>(
  "@lucas-barake/effect-local/ReplicaError/ReplicaFenced"
)("ReplicaFenced", {
  expectedGeneration: Identity.WriterGeneration,
  observedGeneration: Identity.WriterGeneration
}) {}

export class OperationTimeout extends Schema.TaggedErrorClass<OperationTimeout>(
  "@lucas-barake/effect-local/ReplicaError/OperationTimeout"
)("OperationTimeout", {
  operation: Schema.String,
  timeoutMillis: Schema.Int
}) {}

export class UnsupportedStorageFormatVersion extends Schema.TaggedErrorClass<UnsupportedStorageFormatVersion>(
  "@lucas-barake/effect-local/ReplicaError/UnsupportedStorageFormatVersion"
)("UnsupportedStorageFormatVersion", {
  observedVersion: Schema.Int,
  supportedVersion: Schema.Int
}) {}

export class CheckpointSuperseded extends Schema.TaggedErrorClass<CheckpointSuperseded>(
  "@lucas-barake/effect-local/ReplicaError/CheckpointSuperseded"
)("CheckpointSuperseded", {
  documentIds: Schema.Array(Identity.DocumentId),
  attempts: Schema.Int
}) {}

export class DocumentLineageChanged extends Schema.TaggedErrorClass<DocumentLineageChanged>(
  "@lucas-barake/effect-local/ReplicaError/DocumentLineageChanged"
)("DocumentLineageChanged", {
  documentId: Identity.DocumentId,
  localLineage: Identity.DocumentLineage,
  remoteLineage: Identity.DocumentLineage
}) {}

/**
 * The command may or may not have committed, and the replica cannot tell which.
 *
 * Not retryable. Reissuing the command would apply it twice if the first attempt did commit; the
 * recovery is to keep `commandId` and ask, with `lookupCreate`, `lookupMutation` or `lookupDelete`.
 * That is why the id is on the error: it is the only handle those methods accept.
 *
 * `cause` is whatever produced the ambiguity - a transport failure, a timeout, or a lookup that
 * found no receipt. It is kept rather than flattened to a tag, because "we timed out" and "the
 * owner answered but had no record" call for different operator responses.
 */
export class CommandOutcomeUnknown extends Schema.TaggedErrorClass<CommandOutcomeUnknown>(
  "@lucas-barake/effect-local/ReplicaError/CommandOutcomeUnknown"
)("CommandOutcomeUnknown", {
  commandId: Identity.CommandId,
  cause: Schema.Defect()
}) {}

export const Reason = Schema.Union([
  DocumentNotFound,
  DocumentDecodeError,
  DocumentEncodeError,
  TransientDecodeError,
  TransientEncodeError,
  UnsupportedDocumentVersion,
  ProjectionBlocked,
  CommandIdConflict,
  ReceiptOperationMismatch,
  StorageUnavailable,
  CanonicalEncodeError,
  StorageCorrupt,
  ReplicaMetadataMissing,
  QuotaExceeded,
  MigrationFailed,
  BackupInvalid,
  BackupTooLarge,
  RestoreBusy,
  RestoreFailed,
  ProtocolMismatch,
  ReplicaFenced,
  OperationTimeout,
  UnsupportedStorageFormatVersion,
  CheckpointSuperseded,
  DocumentLineageChanged,
  CommandOutcomeUnknown
])
export type Reason = typeof Reason.Type

export class ReplicaError extends Schema.TaggedErrorClass<ReplicaError>("@lucas-barake/effect-local/ReplicaError")(
  "ReplicaError",
  {
    reason: Reason
  }
) {
  override readonly cause = this.reason

  override get message(): string {
    return this.reason._tag
  }
}

/**
 * Narrows a mixed failure channel to this error.
 *
 * A command's channel carries both this and the mutation's own declared error, and those are told
 * apart by the schema rather than by probing for a `_tag` a consumer error could also happen to
 * have.
 */
export const isReplicaError: (input: unknown) => input is ReplicaError = Schema.is(ReplicaError)
