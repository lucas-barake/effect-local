import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as CommandDelivery from "@lucas-barake/effect-local/CommandDelivery"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Schema from "effect/Schema"
import { Rpc, RpcGroup } from "effect/unstable/rpc"
import * as Transferable from "effect/unstable/workers/Transferable"
import * as RestoreProtocol from "./internal/restoreProtocol.js"

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
/**
 * There is exactly one version of the tab to owner protocol, and a `SharedWorker` outlives the page
 * that started it, so a tab from a new deployment can meet an owner from the old one.
 *
 * That mismatch is deliberately NOT expressed as a schema literal on either side. A payload the
 * owner cannot decode is answered with `sendRequestDefect`, and a success value the tab cannot
 * decode is rethrown by `RpcClient` under `Effect.orDie`. Both are defects, so the one peer that
 * most needs to be told to reload would instead lose its replica to a cause it cannot discriminate
 * on. The version has to survive decoding for a handler to refuse it with a typed
 * `ReplicaError.ProtocolMismatch`.
 *
 * It stays at 1 until the first release. Skew is only possible between two deployments a consumer
 * actually installed, and no version of these packages has been published, so every change to this
 * group before then belongs to the same unreleased version 1. `PeerSyncEnvelope.relayProtocolVersion`
 * is 1 for the same reason. Bump this on the first change that ships after release.
 */
export const protocolVersion = 2
export const commandDeliveryInvalidationKey = "@lucas-barake/effect-local/command-delivery"
const DeliveryEventSequence = Schema.Int.check(Schema.isGreaterThan(0))
const DeliveryCursor = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
export const maximumTransientPayloadBytes = 4 * 1024
export const TransientPayload = Schema.Uint8Array.check(
  Schema.makeFilter(
    (payload) => payload.byteLength <= maximumTransientPayloadBytes,
    { expected: `Uint8Array with at most ${maximumTransientPayloadBytes} bytes` }
  )
)
export const TransientMessage = Schema.Struct({
  peerId: Identity.PeerId,
  documentId: Identity.DocumentId,
  payload: TransientPayload
})
export type TransientMessage = typeof TransientMessage.Type
const SessionLease = Schema.Struct({ leaseMillis: Schema.Int })
export const ConflictLimits = Schema.Struct({
  maxConflictDepth: Schema.Int,
  maxConflictNodes: Schema.Int,
  maxConflictAlternatives: Schema.Int,
  maxConflictPathSegments: Schema.Int,
  maxConflictValueBytes: Schema.Int
})
export type ConflictLimits = typeof ConflictLimits.Type
export const SessionHandshake = Schema.Struct({
  leaseMillis: Schema.Int,
  protocolVersion: Schema.Int,
  definitionHash: Schema.String,
  ownerEpoch: Schema.String,
  conflictLimits: Schema.optional(ConflictLimits),
  maxChunkBytes: Schema.optional(Schema.Int),
  maxRestoreCoalesceMillis: Schema.optional(Schema.Int),
  maxRestoreErrorBytes: Schema.optional(Schema.Int)
})
export type SessionHandshake = typeof SessionHandshake.Type

export const isMessagePort = (input: unknown): input is MessagePort => {
  if (typeof input !== "object" || input === null) return false
  try {
    return (
      typeof Reflect.get(input, "postMessage") === "function" &&
      typeof Reflect.get(input, "addEventListener") === "function" &&
      typeof Reflect.get(input, "removeEventListener") === "function" &&
      typeof Reflect.get(input, "start") === "function" &&
      typeof Reflect.get(input, "close") === "function"
    )
  } catch {
    return false
  }
}

export const MessagePortSchema = Transferable.schema(
  Schema.declare<MessagePort>(isMessagePort, {
    identifier: "@lucas-barake/effect-local-browser/MessagePort"
  }),
  (port) => [port]
)

export const Invalidation = Schema.Union([
  Schema.TaggedStruct("Invalidation", {
    ownerEpoch: Schema.String,
    sequence: Identity.CommitSequence,
    keys: Schema.Array(Schema.String)
  }),
  Schema.TaggedStruct("FullRefreshRequired", {
    ownerEpoch: Schema.String,
    keys: Schema.Array(Schema.String)
  }),
  Schema.TaggedStruct("DeliveryInvalidation", {
    ownerEpoch: Schema.String,
    sequence: DeliveryEventSequence,
    keys: Schema.Array(Schema.String)
  }),
  Schema.TaggedStruct("DeliveryFullRefreshRequired", {
    ownerEpoch: Schema.String,
    keys: Schema.Array(Schema.String)
  })
])
export type Invalidation = typeof Invalidation.Type

export const InvalidationMessage = Schema.Union([
  Schema.TaggedStruct("InvalidationsReady", {
    ownerEpoch: Schema.String,
    watermark: Identity.CommitSequence,
    refreshGeneration: Schema.Int,
    deliveryWatermark: DeliveryCursor,
    deliveryRefreshEpoch: DeliveryCursor
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
      // Optional, so a tab old enough to omit the field is refused by the handler with a reason
      // rather than by the decoder with a defect.
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
  Rpc.make("InspectConflicts", {
    payload: { sessionId: Identity.SessionId, document: Schema.String, documentId: Identity.DocumentId },
    success: Schema.String,
    error: Schema.Union([Conflict.InspectionError, ReplicaError.ReplicaError])
  }),
  Rpc.make("ResolveConflict", {
    payload: {
      sessionId: Identity.SessionId,
      document: Schema.String,
      commandId: Identity.CommandId,
      documentId: Identity.DocumentId,
      resolution: Schema.String
    },
    success: Schema.String,
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
  Rpc.make("LookupConflictResolution", {
    payload: {
      sessionId: Identity.SessionId,
      document: Schema.String,
      commandId: Identity.CommandId,
      documentId: Identity.DocumentId,
      resolution: Schema.String
    },
    success: Schema.String,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("LookupCommandDelivery", {
    payload: { sessionId: Identity.SessionId, commandId: Identity.CommandId },
    success: CommandDelivery.CommandDelivery,
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("CommandDeliveryChanges", {
    payload: { sessionId: Identity.SessionId, commandId: Identity.CommandId },
    success: CommandDelivery.CommandDelivery,
    error: ReplicaError.ReplicaError,
    stream: true
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
  Rpc.make("PeerConnectionStatus", {
    payload: { sessionId: Identity.SessionId, peerId: Identity.PeerId },
    success: PeerConnectionStatus.Status,
    error: ReplicaError.ReplicaError,
    stream: true
  }),
  // No peerId: this is the one socket every peer session runs over, so it is a property of the
  // replica rather than of any peer.
  Rpc.make("RelayConnectionStatus", {
    payload: { sessionId: Identity.SessionId },
    success: RelayConnectionStatus.Status,
    error: ReplicaError.ReplicaError,
    stream: true
  }),
  Rpc.make("Transient", {
    payload: {
      sessionId: Identity.SessionId,
      peerId: Identity.PeerId,
      documentId: Identity.DocumentId,
      payload: TransientPayload
    },
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("Transients", {
    payload: { sessionId: Identity.SessionId },
    success: TransientMessage,
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
    payload: { sessionId: Identity.SessionId },
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("BeginRestoreBackup", {
    payload: Schema.Union([
      Schema.Struct({
        sessionId: Identity.SessionId,
        mode: Schema.Literals(["clone", "replace"]),
        maxBytes: Schema.Number,
        expectedDefinitionHash: Schema.String,
        installationId: Identity.BackupInstallationId
      }),
      Schema.Struct({
        sessionId: Identity.SessionId,
        mode: Schema.Literal("document"),
        document: Schema.String,
        documentId: Identity.DocumentId,
        maxBytes: Schema.Number,
        expectedDefinitionHash: Schema.String,
        installationId: Identity.BackupInstallationId
      })
    ]),
    success: Schema.Struct({
      nonce: RestoreProtocol.RestoreNonce,
      port: MessagePortSchema
    }),
    error: ReplicaError.ReplicaError
  }),
  Rpc.make("FinishRestoreBackup", {
    payload: {
      sessionId: Identity.SessionId,
      nonce: RestoreProtocol.RestoreNonce
    },
    error: RestoreProtocol.RestoreResultFailure,
    defect: RestoreProtocol.RestoreResultDefect
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
