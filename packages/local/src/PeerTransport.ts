import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import type * as Stream from "effect/Stream"
import type * as Identity from "./Identity.js"
import type * as ReplicaError from "./ReplicaError.js"

export interface Capabilities {
  /**
   * Whether the peer compares document lineage before it merges a sync message.
   *
   * Optional rather than required: a peer that predates lineage advertises nothing, and an absent
   * value means exactly that. Such a peer has no cross lineage check, so it would union a rewritten
   * document and push the discarded history straight back. The send path treats an absent value as
   * "not lineage aware" and refuses to emit a rewritten document to it.
   */
  readonly lineageAware?: boolean
}

export interface RelayPrincipal {
  readonly tenantId: string
  readonly subjectId: string
  readonly peerId: Identity.PeerId
}

export interface RelayEndpoint {
  readonly expectedLocal: RelayPrincipal
  readonly remote: RelayPrincipal
  readonly relayPeerId: Identity.PeerId
}

export type PermanentRejectReason = "ProtocolInvalid" | "ApplicationRejected"

export interface RelayDeliveryIdentity {
  readonly relayMessageId: Identity.RelayMessageId
  readonly relayPeerId: Identity.PeerId
  readonly senderTenantId: string
  readonly senderSubjectId: string
  readonly senderPeerId: Identity.PeerId
  readonly senderReplicaIncarnation: Identity.ReplicaIncarnation
  readonly messageHash: string
  readonly outerEnvelopeDigest: string
}

export interface AcknowledgedDelivery {
  readonly message: Uint8Array
  readonly identity: RelayDeliveryIdentity
  readonly receiptRetentionMillis: number
  readonly acknowledge: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly reject: (
    reason: PermanentRejectReason
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
}

export interface Connection {
  readonly peerId: Identity.PeerId
  readonly relayPeerId: Identity.PeerId
  readonly relayEndpoint?: RelayEndpoint
  readonly capabilities: Capabilities
  readonly receive: Stream.Stream<AcknowledgedDelivery, ReplicaError.ReplicaError>
  readonly send: (message: Uint8Array) => Effect.Effect<void, ReplicaError.ReplicaError>
  /**
   * Terminates `receive`. A fiber parked consuming it observes an interrupt only `Exit`, never a normal end and
   * never a failure. A normal end would be read as a retryable transport fault, and a failure would be reported as
   * a session failure, so neither is a valid way to honour this.
   *
   * Must not wait on the consumer of `receive`, which calls `close` from that consumer's own fiber. An
   * implementation may still suspend on its own in-flight work: `RpcPeerTransport` awaits the cleanup of an
   * in-flight `send`, and makes a concurrent caller await the first. It is idempotent, and a consumer already
   * inside its element handler finishes that handler and observes the interrupt on its next pull.
   *
   * `close` only initiates termination, and it is not the release path an implementation may rely on: closing the
   * connection scope must on its own release every transport resource, including revoking or ceasing to renew any
   * credential or lease the connection holds. Whether `send` still succeeds after `close` is implementation
   * defined.
   */
  readonly close: Effect.Effect<void>
}

export interface ConnectOptions {
  readonly replicaId: Identity.ReplicaId
  readonly peerId: Identity.PeerId
}

export class PeerTransport extends Context.Service<PeerTransport, {
  readonly capabilities: Capabilities
  readonly connect: (options: ConnectOptions) => Effect.Effect<Connection, ReplicaError.ReplicaError, Scope.Scope>
}>()("@lucas-barake/effect-local/PeerTransport") {}
