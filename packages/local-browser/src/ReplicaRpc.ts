import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Schema from "effect/Schema"
import { Rpc, RpcGroup } from "effect/unstable/rpc"
import * as Transferable from "effect/unstable/workers/Transferable"

const Snapshot = Schema.Struct({
  documentId: Identity.DocumentId,
  value: Schema.Json,
  version: Schema.Int,
  heads: Schema.Array(Schema.String),
  tombstone: Schema.Boolean,
  projection: Schema.Literals(["Ready", "Blocked", "Rebuilding"])
})

const ExportedDocument = Schema.Struct({
  documentName: Schema.String,
  schemaVersion: Schema.Int,
  value: Schema.Json
})

const JsonOutcome = CommandOutcome.schema(Schema.Json, Schema.Json)
const DocumentIdOutcome = CommandOutcome.schema(Identity.DocumentId, Schema.Never)
export const protocolVersion = 3
const SessionLease = Schema.Struct({ leaseMillis: Schema.Int })
const SessionHandshake = Schema.Struct({
  leaseMillis: Schema.Int,
  protocolVersion: Schema.Int,
  definitionHash: Schema.String,
  ownerEpoch: Schema.String
})

export const Invalidation = Schema.Union([
  Schema.TaggedStruct("Invalidation", {
    ownerEpoch: Schema.String,
    sequence: Identity.CommitSequence,
    keys: Schema.Array(Schema.String)
  }),
  Schema.TaggedStruct("FullRefreshRequired", {
    ownerEpoch: Schema.String,
    keys: Schema.Array(Schema.String)
  })
])
export type Invalidation = typeof Invalidation.Type

export const InvalidationMessage = Schema.Union([
  Schema.TaggedStruct("InvalidationsReady", {
    ownerEpoch: Schema.String,
    watermark: Identity.CommitSequence,
    refreshGeneration: Schema.Int
  }),
  Invalidation
])
export type InvalidationMessage = typeof InvalidationMessage.Type

export class ReplicaQueryError extends Schema.TaggedErrorClass<ReplicaQueryError>(
  "@lucas-barake/effect-local-browser/ReplicaQueryError"
)("ReplicaQueryError", {
  error: Schema.Json
}) {}

export const group = RpcGroup.make(
  Rpc.make("OpenSession", {
    payload: {
      sessionId: Identity.SessionId,
      protocolVersion: Schema.optional(Schema.Int),
      definitionHash: Schema.String
    },
    success: SessionHandshake,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("RenewSession", {
    payload: { sessionId: Identity.SessionId },
    success: SessionLease,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("CloseSession", {
    payload: { sessionId: Identity.SessionId },
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("Create", {
    payload: {
      sessionId: Identity.SessionId,
      document: Schema.String,
      commandId: Identity.CommandId,
      value: Schema.Json
    },
    success: DocumentIdOutcome,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("Get", {
    payload: { sessionId: Identity.SessionId, document: Schema.String, documentId: Identity.DocumentId },
    success: Snapshot,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("Mutate", {
    payload: {
      sessionId: Identity.SessionId,
      mutation: Schema.String,
      commandId: Identity.CommandId,
      documentId: Identity.DocumentId,
      payload: Schema.Json
    },
    success: JsonOutcome,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("Delete", {
    payload: {
      sessionId: Identity.SessionId,
      document: Schema.String,
      commandId: Identity.CommandId,
      documentId: Identity.DocumentId
    },
    success: JsonOutcome,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("Query", {
    payload: { sessionId: Identity.SessionId, query: Schema.String, payload: Schema.Json },
    success: Schema.Json,
    error: Schema.Union([ReplicaQueryError, ReplicaError.ReplicaError])
  }),
  Rpc.make("LookupMutation", {
    payload: { sessionId: Identity.SessionId, mutation: Schema.String, commandId: Identity.CommandId },
    success: JsonOutcome,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("LookupCreate", {
    payload: { sessionId: Identity.SessionId, document: Schema.String, commandId: Identity.CommandId },
    success: DocumentIdOutcome,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("LookupDelete", {
    payload: { sessionId: Identity.SessionId, document: Schema.String, commandId: Identity.CommandId },
    success: JsonOutcome,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("Flush", { payload: { sessionId: Identity.SessionId }, error: ReplicaError.ReplicaError }),
  Rpc.make("Invalidations", {
    payload: { sessionId: Identity.SessionId, ownerEpoch: Schema.String },
    success: InvalidationMessage,
    error: ReplicaError.ReplicaError,
    stream: true
  }),
  Rpc.make("Status", {
    payload: { sessionId: Identity.SessionId },
    success: ReplicaStatus.ReplicaStatus,
    error: ReplicaError.ReplicaError,
    stream: true
  }),
  Rpc.make("ExportBackup", {
    payload: { sessionId: Identity.SessionId, maxBytes: Schema.Number },
    success: Transferable.Uint8Array,
    error: ReplicaError.ReplicaError,
    stream: true
  }),
  Rpc.make("RestoreBackup", {
    payload: {
      sessionId: Identity.SessionId,
      chunks: Schema.Array(Transferable.Uint8Array),
      mode: Schema.Literals(["clone", "replace"]),
      maxBytes: Schema.Number,
      expectedDefinitionHash: Schema.String,
      installationId: Identity.BackupInstallationId
    },
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("ExportDocument", {
    payload: { sessionId: Identity.SessionId, document: Schema.String, documentId: Identity.DocumentId },
    success: ExportedDocument,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("ImportDocument", {
    payload: {
      sessionId: Identity.SessionId,
      document: Schema.String,
      commandId: Identity.CommandId,
      value: ExportedDocument
    },
    success: DocumentIdOutcome,
    error: ReplicaError.ReplicaError
  })
)
