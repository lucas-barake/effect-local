import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { PeerPrincipal } from "./internal/peerPrincipal.js"
import * as PeerRpc from "./PeerRpc.js"

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const DocumentIds = Schema.Array(Identity.DocumentId).check(Schema.isMinLength(1))

export const ChannelKey = Schema.Struct({
  tenantId: Schema.NonEmptyString,
  senderSubjectId: Schema.NonEmptyString,
  senderPeerId: Identity.PeerId,
  senderReplicaIncarnation: Identity.ReplicaIncarnation,
  recipientSubjectId: Schema.NonEmptyString,
  recipientPeerId: Identity.PeerId
})
export type ChannelKey = typeof ChannelKey.Type

export const Admission = Schema.Struct({
  channel: ChannelKey,
  relayMessageId: Identity.RelayMessageId,
  relayPeerId: Identity.PeerId,
  documentIds: DocumentIds,
  senderConnectionEpoch: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  senderSequence: NonNegativeInt,
  payloadVersion: Schema.Literal(1),
  messageHash: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  outerEnvelopeDigest: PeerRpc.RelayDigest,
  payload: Schema.Uint8Array,
  messageTtlMillis: PositiveInt,
  senderRetryHorizonMillis: PositiveInt,
  minimumTerminalRetentionMillis: PositiveInt
})
export type Admission = typeof Admission.Type

export const AdmissionResult = Schema.Struct({
  status: Schema.Literals(["Accepted", "Duplicate"]),
  channel: ChannelKey,
  ready: Schema.Boolean,
  nextEligibleAt: NonNegativeInt,
  lane: Schema.Literal("New")
})
export type AdmissionResult = typeof AdmissionResult.Type

export const ClaimRequest = Schema.Struct({
  recipient: PeerPrincipal,
  sender: Schema.Struct({
    subjectId: Schema.NonEmptyString,
    peerId: Identity.PeerId
  }),
  sessionGeneration: NonNegativeInt,
  authorizedDocumentIds: DocumentIds
})
export type ClaimRequest = typeof ClaimRequest.Type

export const ClaimedMessage = Schema.Struct({
  rowId: PositiveInt,
  channel: ChannelKey,
  relayMessageId: Identity.RelayMessageId,
  relayPeerId: Identity.PeerId,
  senderConnectionEpoch: Schema.NonEmptyString,
  senderSequence: NonNegativeInt,
  documentIds: DocumentIds,
  payloadVersion: Schema.Literal(1),
  messageHash: Schema.NonEmptyString,
  outerEnvelopeDigest: PeerRpc.RelayDigest,
  payloadBytes: NonNegativeInt,
  claimToken: PeerRpc.ClaimToken,
  claimDeadline: NonNegativeInt,
  sessionGeneration: NonNegativeInt
})
export type ClaimedMessage = typeof ClaimedMessage.Type

export interface ClaimResult {
  readonly message: Option.Option<ClaimedMessage>
  readonly ready: boolean
  readonly nextEligibleAt: Option.Option<number>
  readonly lane: "New" | "Retry"
}

export const LoadClaimedPayloadRequest = Schema.Struct({
  rowId: PositiveInt,
  channel: ChannelKey,
  relayMessageId: Identity.RelayMessageId,
  claimToken: PeerRpc.ClaimToken,
  sessionGeneration: NonNegativeInt,
  payloadBytes: NonNegativeInt
})
export type LoadClaimedPayloadRequest = typeof LoadClaimedPayloadRequest.Type

export const TerminalRequest = Schema.Struct({
  channel: ChannelKey,
  relayMessageId: Identity.RelayMessageId,
  claimToken: PeerRpc.ClaimToken,
  messageHash: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  sessionGeneration: NonNegativeInt,
  recipient: PeerPrincipal
})
export type TerminalRequest = typeof TerminalRequest.Type

export const RejectRequest = Schema.Struct({
  ...TerminalRequest.fields,
  reason: PeerRpc.RejectReason
})
export type RejectRequest = typeof RejectRequest.Type

export const ReleaseRequest = Schema.Struct({
  channel: ChannelKey,
  relayMessageId: Identity.RelayMessageId,
  claimToken: PeerRpc.ClaimToken,
  sessionGeneration: NonNegativeInt
})
export type ReleaseRequest = typeof ReleaseRequest.Type

export interface TransitionResult {
  readonly status: "Changed" | "Duplicate" | "Stale"
  readonly ready: boolean
  readonly nextEligibleAt: Option.Option<number>
  readonly lane: "New" | "Retry"
}

export const MaintenanceRequest = Schema.Struct({
  cursor: Schema.optionalKey(NonNegativeInt),
  batchSize: PositiveInt
})
export type MaintenanceRequest = typeof MaintenanceRequest.Type

export interface MaintenanceResult {
  readonly cursor?: number
  readonly processed: number
  readonly hasMore: boolean
}

export const UsageRequest = Schema.Struct({
  scopeKind: Schema.Literals(["SenderPeer", "RecipientPeer", "RecipientSubject", "Tenant", "Shard"]),
  scopeKey: Schema.String
})
export type UsageRequest = typeof UsageRequest.Type

export interface Usage {
  readonly activeCount: number
  readonly activeBytes: number
  readonly retainedCount: number
  readonly retainedBytes: number
}

export type StoreError = ReplicaError.ReplicaError

export interface Service {
  readonly admit: (input: Admission) => Effect.Effect<AdmissionResult, StoreError>
  readonly claim: (input: ClaimRequest) => Effect.Effect<ClaimResult, StoreError>
  readonly loadClaimedPayload: (
    input: LoadClaimedPayloadRequest
  ) => Effect.Effect<Uint8Array, StoreError>
  readonly acknowledge: (input: TerminalRequest) => Effect.Effect<TransitionResult, StoreError>
  readonly reject: (input: RejectRequest) => Effect.Effect<TransitionResult, StoreError>
  readonly release: (input: ReleaseRequest) => Effect.Effect<TransitionResult, StoreError>
  readonly recover: (input: MaintenanceRequest) => Effect.Effect<MaintenanceResult, StoreError>
  readonly expire: (input: MaintenanceRequest) => Effect.Effect<MaintenanceResult, StoreError>
  readonly repair: (input: MaintenanceRequest) => Effect.Effect<MaintenanceResult, StoreError>
  readonly reconcile: (input: MaintenanceRequest) => Effect.Effect<MaintenanceResult, StoreError>
  readonly collect: (input: MaintenanceRequest) => Effect.Effect<MaintenanceResult, StoreError>
  readonly usage: (input?: UsageRequest) => Effect.Effect<Usage, StoreError>
}

export class PeerRelayStore extends Context.Service<PeerRelayStore, Service>()(
  "@lucas-barake/effect-local-rpc/PeerRelayStore"
) {}
