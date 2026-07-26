import * as Identity from "@lucas-barake/effect-local/Identity"
import * as PeerSyncEnvelope from "@lucas-barake/effect-local-sql/PeerSyncEnvelope"
import type * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Rpc from "effect/unstable/rpc/Rpc"
import { make as makeClient } from "effect/unstable/rpc/RpcClient"
import type * as RpcClient_ from "effect/unstable/rpc/RpcClient"
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError"
import * as RpcGroup from "effect/unstable/rpc/RpcGroup"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import * as PeerAuthentication from "./PeerAuthentication.js"
import * as PeerRpc from "./PeerRpc.js"
import * as PeerRpcError from "./PeerRpcError.js"

export const protocolVersion = 3

export const maximumNegotiatedDurationMillis = 90 * 24 * 60 * 60 * 1_000
export const maximumRelayPayloadBytes = PeerSyncEnvelope.maximumRelayPayloadBytes
export const maximumRequestedDocuments = 1_000

const DurationMillis = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(maximumNegotiatedDurationMillis)
)

const Credential = Schema.optionalKey(Schema.RedactedFromValue(Schema.String))
const BoundedName = Schema.NonEmptyString.check(Schema.isMaxLength(256))
const BoundedPeerPrincipal = PeerSyncEnvelope.RelayPeerPrincipal
const MessageHash = PeerSyncEnvelope.RelayOuterEnvelope.fields.messageHash
const Payload = PeerSyncEnvelope.RelayOuterEnvelope.fields.payload

export const ClaimToken = Schema.String.check(
  Schema.isPattern(/^clm_[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
).pipe(Schema.brand("@lucas-barake/effect-local-rpc/ClaimToken"))
export type ClaimToken = typeof ClaimToken.Type

export const RelayDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))
export type RelayDigest = typeof RelayDigest.Type

export const RelayOpened = Schema.TaggedStruct("RelayOpened", {
  version: Schema.Literal(protocolVersion),
  sessionId: Identity.SessionId,
  remotePeerId: Identity.PeerId,
  authenticatedLocal: BoundedPeerPrincipal,
  capabilities: Schema.Struct({ storeAndForward: Schema.Literal(true) })
})
export type RelayOpened = typeof RelayOpened.Type

export const StoredMessage = Schema.TaggedStruct("StoredMessage", {
  relayMessageId: Identity.RelayMessageId,
  claimToken: ClaimToken,
  relayPeerId: Identity.PeerId,
  sender: Schema.Struct({
    tenantId: PeerSyncEnvelope.RelayPeerPrincipal.fields.tenantId,
    subjectId: PeerSyncEnvelope.RelayPeerPrincipal.fields.subjectId,
    peerId: PeerSyncEnvelope.RelayPeerPrincipal.fields.peerId,
    replicaIncarnation: Identity.ReplicaIncarnation,
    connectionEpoch: PeerSyncEnvelope.RelayOuterEnvelope.fields.senderConnectionEpoch,
    sequence: PeerSyncEnvelope.RelayOuterEnvelope.fields.senderSequence
  }),
  recipient: BoundedPeerPrincipal,
  payloadVersion: PeerSyncEnvelope.RelayOuterEnvelope.fields.payloadVersion,
  document: PeerSyncEnvelope.RelayOuterEnvelope.fields.document,
  writerProvenance: PeerSyncEnvelope.RelayOuterEnvelope.fields.writerProvenance,
  messageHash: MessageHash,
  outerEnvelopeDigest: RelayDigest,
  payload: Payload
})
export type StoredMessage = typeof StoredMessage.Type

export const OpenRelayEvent = Schema.Union([RelayOpened, StoredMessage])
export type OpenRelayEvent = typeof OpenRelayEvent.Type

export const RejectReason = Schema.Literals(["ProtocolInvalid", "ApplicationRejected"])
export type RejectReason = typeof RejectReason.Type

export class OpenRelayRpc extends Rpc.make("OpenRelay", {
  payload: {
    version: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    expectedRelayPeerId: Identity.PeerId,
    expectedLocal: BoundedPeerPrincipal,
    senderReplicaIncarnation: Identity.ReplicaIncarnation,
    remote: Schema.Struct({
      subjectId: BoundedName,
      peerId: Identity.PeerId
    }),
    documents: Schema.Array(PeerRpc.RequestedDocument).check(
      Schema.isMinLength(1),
      Schema.isMaxLength(maximumRequestedDocuments)
    ),
    receiptRetentionMillis: DurationMillis,
    senderRetryHorizonMillis: DurationMillis,
    credential: Credential
  },
  success: OpenRelayEvent,
  error: PeerRpcError.PeerRpcError,
  defect: PeerRpcError.Defect,
  stream: true
}) {}

export class PushRelayRpc extends Rpc.make("PushRelay", {
  payload: {
    sessionId: Identity.SessionId,
    relayMessageId: Identity.RelayMessageId,
    payload: Payload,
    credential: Credential
  },
  error: PeerRpcError.PeerRpcError,
  defect: PeerRpcError.Defect
}) {}

const TerminalPayload = {
  sessionId: Identity.SessionId,
  relayMessageId: Identity.RelayMessageId,
  claimToken: ClaimToken,
  messageHash: MessageHash,
  credential: Credential
}

export class AcknowledgeRelayRpc extends Rpc.make("AcknowledgeRelay", {
  payload: TerminalPayload,
  error: PeerRpcError.PeerRpcError,
  defect: PeerRpcError.Defect
}) {}

export class RejectRelayRpc extends Rpc.make("RejectRelay", {
  payload: {
    ...TerminalPayload,
    reason: RejectReason
  },
  error: PeerRpcError.PeerRpcError,
  defect: PeerRpcError.Defect
}) {}

export class Rpcs extends RpcGroup.make(
  OpenRelayRpc,
  PushRelayRpc,
  AcknowledgeRelayRpc,
  RejectRelayRpc
).middleware(PeerAuthentication.PeerAuthentication) {}

export interface RpcClient extends RpcClient_.FromGroup<typeof Rpcs, RpcClientError> {}

export const makeRpcClient: Effect.Effect<
  RpcClient,
  never,
  RpcClient_.Protocol | RpcMiddleware.ForClient<PeerAuthentication.PeerAuthentication> | Scope.Scope
> = makeClient(Rpcs)
