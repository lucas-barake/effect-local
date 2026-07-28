import * as Schema from "effect/Schema"
import * as ReplicaRpc from "../ReplicaRpc.js"

/**
 * There is exactly one version of the tab to coordinator control protocol, for the same reason
 * `ReplicaRpc.protocolVersion` exists: a `SharedWorker` outlives the page that started it, so a tab
 * from a new deployment can meet a coordinator from the old one. The version rides inside the
 * `Attach` frame so a mismatched peer is refused with an `OwnerError` frame it can decode, never
 * with a decode defect it cannot discriminate on.
 */
export const protocolVersion = 1

export const ProvisionNonce = Schema.String.pipe(
  Schema.brand("@lucas-barake/effect-local-browser/ProvisionNonce")
)
export type ProvisionNonce = typeof ProvisionNonce.Type

export const Attach = Schema.TaggedStruct("Attach", {
  protocolVersion: Schema.Int,
  rpcPort: ReplicaRpc.MessagePortSchema
})
export type Attach = typeof Attach.Type

export const Provision = Schema.TaggedStruct("Provision", {
  nonce: ProvisionNonce,
  databasePort: ReplicaRpc.MessagePortSchema
})
export type Provision = typeof Provision.Type

export const Detach = Schema.TaggedStruct("Detach", {})
export type Detach = typeof Detach.Type

export const PageToOwnerFrame = Schema.Union([Attach, Provision, Detach])
export type PageToOwnerFrame = typeof PageToOwnerFrame.Type

export const Attached = Schema.TaggedStruct("Attached", {
  ownerId: Schema.String,
  provider: Schema.Boolean,
  info: Schema.Unknown
})
export type Attached = typeof Attached.Type

export const ProvisionRequested = Schema.TaggedStruct("Provision", {
  nonce: ProvisionNonce
})
export type ProvisionRequested = typeof ProvisionRequested.Type

export const ProvisionAccepted = Schema.TaggedStruct("ProvisionAccepted", {
  nonce: ProvisionNonce
})
export type ProvisionAccepted = typeof ProvisionAccepted.Type

export const ProvisionRejected = Schema.TaggedStruct("ProvisionRejected", {
  nonce: ProvisionNonce
})
export type ProvisionRejected = typeof ProvisionRejected.Type

export const Reattach = Schema.TaggedStruct("Reattach", {
  ownerId: Schema.String
})
export type Reattach = typeof Reattach.Type

export const OwnerError = Schema.TaggedStruct("OwnerError", {
  message: Schema.String,
  reason: Schema.optional(Schema.Unknown)
})
export type OwnerError = typeof OwnerError.Type

export const OwnerToPageFrame = Schema.Union([
  Attached,
  ProvisionRequested,
  ProvisionAccepted,
  ProvisionRejected,
  Reattach,
  OwnerError
])
export type OwnerToPageFrame = typeof OwnerToPageFrame.Type

/**
 * The single message the durable database worker accepts: the database port it must serve for the
 * lifetime of the worker.
 */
export const DatabaseWorkerStart = Schema.TaggedStruct("DatabaseWorkerStart", {
  databasePort: ReplicaRpc.MessagePortSchema
})
export type DatabaseWorkerStart = typeof DatabaseWorkerStart.Type

export const decodePageToOwner = Schema.decodeUnknownSync(PageToOwnerFrame)
export const decodeOwnerToPage = Schema.decodeUnknownSync(OwnerToPageFrame)
export const decodeDatabaseWorkerStart = Schema.decodeUnknownSync(DatabaseWorkerStart)

export const encodePageToOwner = Schema.encodeSync(PageToOwnerFrame)
export const encodeOwnerToPage = Schema.encodeSync(OwnerToPageFrame)
export const encodeDatabaseWorkerStart = Schema.encodeSync(DatabaseWorkerStart)

/**
 * Frames that carry a transferred port are posted with the port in the transfer list. Every other
 * frame travels as a plain structured clone.
 */
export const transferOf = (frame: PageToOwnerFrame): ReadonlyArray<Transferable> =>
  frame._tag === "Attach" ? [frame.rpcPort] : frame._tag === "Provision" ? [frame.databasePort] : []
