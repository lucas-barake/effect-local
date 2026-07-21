import * as Schema from "effect/Schema"
import * as Identity from "./Identity.js"

export const SqlCause = Schema.TaggedStruct("SqlCause", {
  message: Schema.String,
  code: Schema.NullOr(Schema.String)
})

export const SchemaCause = Schema.TaggedStruct("SchemaCause", {
  message: Schema.String,
  path: Schema.Array(Schema.String)
})

export const WorkerCause = Schema.TaggedStruct("WorkerCause", {
  message: Schema.String
})

export const RpcCause = Schema.TaggedStruct("RpcCause", {
  message: Schema.String
})

export const AutomergeCause = Schema.TaggedStruct("AutomergeCause", {
  message: Schema.String
})

export const Cause = Schema.Union([SqlCause, SchemaCause, WorkerCause, RpcCause, AutomergeCause])
export type Cause = typeof Cause.Type

export const DocumentNotFound = Schema.TaggedStruct("DocumentNotFound", {
  documentId: Identity.DocumentId
})
export const DocumentDecodeError = Schema.TaggedStruct("DocumentDecodeError", {
  documentId: Identity.DocumentId,
  cause: SchemaCause
})
export const UnsupportedDocumentVersion = Schema.TaggedStruct("UnsupportedDocumentVersion", {
  documentId: Identity.DocumentId,
  observedVersion: Schema.Int,
  supportedVersion: Schema.Int
})
export const ProjectionBlocked = Schema.TaggedStruct("ProjectionBlocked", {
  projection: Schema.String,
  cause: SchemaCause
})
export const CommandIdConflict = Schema.TaggedStruct("CommandIdConflict", {
  commandId: Identity.CommandId
})
export const StorageUnavailable = Schema.TaggedStruct("StorageUnavailable", { cause: Cause })
export const StorageCorrupt = Schema.TaggedStruct("StorageCorrupt", { cause: Cause })
export const QuotaExceeded = Schema.TaggedStruct("QuotaExceeded", {
  resource: Schema.String,
  limit: Schema.Int
})
export const MigrationFailed = Schema.TaggedStruct("MigrationFailed", {
  migration: Schema.String,
  cause: Cause
})
export const BackupInvalid = Schema.TaggedStruct("BackupInvalid", { cause: Cause })
export const BackupTooLarge = Schema.TaggedStruct("BackupTooLarge", {
  limit: Schema.Int,
  observed: Schema.Int
})
export const RestoreBusy = Schema.TaggedStruct("RestoreBusy", { replica: Schema.String })
export const RestoreFailed = Schema.TaggedStruct("RestoreFailed", { cause: Cause })
export const ProtocolMismatch = Schema.TaggedStruct("ProtocolMismatch", {
  expected: Schema.String,
  observed: Schema.String
})
export const ReplicaFenced = Schema.TaggedStruct("ReplicaFenced", {
  expectedGeneration: Identity.WriterGeneration,
  observedGeneration: Identity.WriterGeneration
})

export const Reason = Schema.Union([
  DocumentNotFound,
  DocumentDecodeError,
  UnsupportedDocumentVersion,
  ProjectionBlocked,
  CommandIdConflict,
  StorageUnavailable,
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

export class ReplicaError extends Schema.TaggedErrorClass<ReplicaError>()("ReplicaError", {
  reason: Reason
}) {}
