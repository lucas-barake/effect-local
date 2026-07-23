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

export const Reason = Schema.Union([
  DocumentNotFound,
  DocumentDecodeError,
  DocumentEncodeError,
  UnsupportedDocumentVersion,
  ProjectionBlocked,
  CommandIdConflict,
  ReceiptOperationMismatch,
  StorageUnavailable,
  CanonicalEncodeError,
  StorageCorrupt,
  QuotaExceeded,
  MigrationFailed,
  BackupInvalid,
  BackupTooLarge,
  RestoreBusy,
  RestoreFailed,
  ProtocolMismatch,
  ReplicaFenced
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
