import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as PeerRpc from "./PeerRpc.js"

/**
 * Durable storage for one peer device's relay inbox.
 *
 * The owning cluster entity is the sole writer for a given `inboxKey`, and that single fact is
 * what keeps this contract small. Distributed custody transfer — claim tokens, lease deadlines,
 * session generations, work ownership, a global write lock — exists only to arbitrate between
 * competing processes, and there are none here. Sharding elects one owner per device and the store
 * only has to remember what that owner has not yet finished.
 *
 * There is deliberately no in-flight state. A message stays `Pending` until it reaches a terminal
 * state, and the owner tracks what it is currently delivering in memory. If the owning runner
 * dies, that memory is lost and the rows are still `Pending`, so the next owner simply delivers
 * them again. At-least-once delivery therefore falls out of the schema rather than out of a lease
 * recovery protocol.
 */

/**
 * A message at rest in a recipient's durable inbox.
 *
 * This is the wire `StoredMessage` without its claim token. The claim token is minted per delivery
 * attempt rather than stored, so a token from an earlier attempt can never settle a later one.
 */
export const InboxEnvelope = Schema.Struct({
  relayMessageId: Identity.RelayMessageId,
  relayPeerId: Identity.PeerId,
  sender: PeerRpc.StoredMessage.fields.sender,
  recipient: PeerRpc.StoredMessage.fields.recipient,
  payloadVersion: PeerRpc.StoredMessage.fields.payloadVersion,
  document: PeerRpc.StoredMessage.fields.document,
  writerProvenance: PeerRpc.StoredMessage.fields.writerProvenance,
  messageHash: PeerRpc.StoredMessage.fields.messageHash,
  outerEnvelopeDigest: PeerRpc.RelayDigest,
  payload: PeerRpc.StoredMessage.fields.payload
})
export type InboxEnvelope = typeof InboxEnvelope.Type

export const InboxState = Schema.Literals([
  "Pending",
  "Acknowledged",
  "Rejected",
  "DeadLettered",
  "Expired"
])
export type InboxState = typeof InboxState.Type

export const TerminalOutcome = Schema.Literals(["Acknowledged", "Rejected"])
export type TerminalOutcome = typeof TerminalOutcome.Type

/**
 * Identifies the ordered stream a message belongs to.
 *
 * Messages are ordered within a channel and independent across channels, so one stalled sender
 * cannot hold up another sender's messages to the same device. A single queue per device would
 * serialize every sender behind whichever message happens to be at the head.
 */
export const ChannelKey = Schema.Struct({
  tenantId: Schema.NonEmptyString,
  senderSubjectId: Schema.NonEmptyString,
  senderPeerId: Identity.PeerId,
  senderReplicaIncarnation: Identity.ReplicaIncarnation,
  /**
   * Part of the channel identity, not merely payload.
   *
   * The sender allocates `senderSequence` per connection epoch, so it restarts at zero every time
   * the sender reconnects. Without the epoch in the key, one channel would hold two runs of
   * sequences and head selection would be arbitrary — and interleaving two epochs makes the
   * recipient repeatedly reset the other epoch's sync state, discarding its pending replies.
   */
  senderConnectionEpoch: Schema.NonEmptyString
})
export type ChannelKey = typeof ChannelKey.Type

export interface AdmissionRequest {
  readonly inboxKey: string
  readonly channel: ChannelKey
  readonly envelope: InboxEnvelope
  /** Wall clock at admission. Passed in so the caller owns the clock. */
  readonly now: number
  /** How long an undelivered message survives before it is expired. */
  readonly messageTtlMillis: number
  /**
   * The retry horizon this sender negotiated for its session.
   *
   * Threaded per message rather than read from static configuration because the sender advertises
   * it at `Open` and it can exceed any fixed server value. The deduplication horizon is derived
   * from it, so a message's identity always outlives the window in which its sender may replay it.
   * Without this, a replay can arrive after the identity was collected and be applied twice.
   */
  readonly senderRetryHorizonMillis: number
  /** Evaluated in the same transaction as the insert. */
  readonly quota: AdmissionQuota
}

/**
 * `Admitted` covers both a first admission and the revival of a message whose previous row had
 * already been given up on. A sender that replays an identity still holds custody of it, and that
 * is better evidence than the relay's own earlier decision to expire or dead-letter it.
 *
 * `Duplicate` reports the state of the row that is already present. It is only safe to treat as
 * success while that row can still be delivered or has already been settled — on `Push` success
 * the sender deletes its outbox entry, so reporting success for a `DeadLettered` or `Expired` row
 * would make the sender's own recovery replay destroy the last durable copy of the message.
 *
 * `Conflict` means the same identity arrived carrying different content, which is never a legal
 * retry and must not silently overwrite the stored message.
 */
export const AdmissionResult = Schema.Union([
  Schema.TaggedStruct("Admitted", {}),
  Schema.TaggedStruct("Duplicate", { state: InboxState }),
  Schema.TaggedStruct("Conflict", {}),
  Schema.TaggedStruct("QuotaExceeded", {})
])
export type AdmissionResult = typeof AdmissionResult.Type

export interface PendingMessage {
  readonly relayMessageId: Identity.RelayMessageId
  readonly channel: ChannelKey
  readonly senderSequence: number
  readonly deliveries: number
  readonly envelope: InboxEnvelope
}

/**
 * Reports whether recording this delivery attempt exhausted the message's budget.
 *
 * A message that is repeatedly delivered but never settled — a recipient that reads it and then
 * disconnects every time — would otherwise block its channel forever, because a disconnect is an
 * interruption and interruptions are deliberately not counted as failures. Counting deliveries
 * rather than failures is what bounds that, and `DeadLettered` keeps the outcome observable
 * instead of silently dropping the message.
 */
export const DeliveryRecord = Schema.Union([
  Schema.TaggedStruct("Recorded", { deliveries: Schema.Int }),
  Schema.TaggedStruct("DeadLettered", { deliveries: Schema.Int })
])
export type DeliveryRecord = typeof DeliveryRecord.Type

/**
 * Whether a settlement actually transitioned a row.
 *
 * `NotPending` means the message had already left `Pending` — settled by an earlier attempt, or
 * abandoned by dead-lettering or expiry. `HashMismatch` means the identity matched but the content
 * did not, which is never a legal settlement.
 */
export const SettleResult = Schema.Literals(["Settled", "NotPending", "HashMismatch"])
export type SettleResult = typeof SettleResult.Type

export interface AbandonedMessage {
  readonly relayMessageId: Identity.RelayMessageId
  readonly state: InboxState
  readonly deliveries: number
  readonly terminalAt: number
}

export interface Usage {
  readonly pendingCount: number
  readonly pendingBytes: number
  readonly retainedCount: number
}

/**
 * Caps evaluated inside the admission transaction.
 *
 * Passed with the request rather than read beforehand: the entity's rpc lane and its forked
 * dispatcher fibers write to the same inbox concurrently, so a quota read followed by a separate
 * insert is a race. The predicate has to live in the write itself.
 */
export interface AdmissionQuota {
  readonly maxPendingMessages: number
  readonly maxPendingBytes: number
}

export type StoreError = ReplicaError.ReplicaError

export class RelayInboxStore extends Context.Service<RelayInboxStore, {
  /**
   * Durably accepts a message, deduplicating on `(inboxKey, relayMessageId)`.
   *
   * Terminal rows remain present until their retention horizon lapses, so a replay that arrives
   * after settlement is still recognised as a duplicate rather than delivered a second time.
   *
   * Conflicts are detected on `outerEnvelopeDigest`, not on `messageHash`. The digest binds the
   * sender, recipient, relay, sequence, epoch, document, lineage and provenance, whereas the
   * message hash covers only the inner payload — and both peers already key their own
   * deduplication on the digest, so anything weaker lets the three layers disagree about what an
   * identity means.
   */
  readonly admit: (request: AdmissionRequest) => Effect.Effect<AdmissionResult, StoreError>

  /**
   * The oldest undelivered message of every channel in this inbox.
   *
   * One head per channel: the owner delivers heads concurrently across channels and strictly in
   * order within a channel.
   *
   * The order is a delivery guarantee, not presentation. Each sender's most recently started
   * channel comes first (live channels among themselves oldest first), then every superseded
   * channel oldest first. A sender mints a fresh connection epoch per session, so a crash looping
   * sender leaves one channel per lapsed epoch; the owner takes heads in this order into a bounded
   * number of delivery slots, and age order alone would hand every slot to that stale trail while
   * the sender's live epoch starves behind it.
   *
   * Priority alone would starve in the other direction: live channels that are never empty never
   * leave a slot idle, and a superseded message the relay accepted custody of would expire
   * undelivered — the sender only replays its current epoch's outbox, so expiry destroys the last
   * copy. A superseded head that has burned half its TTL by `now` is therefore promoted into the
   * priority class, where its age puts it at the front. Deprioritization is bounded at half the
   * message's lifetime; the other half is the delivery window it was promoted to use.
   */
  readonly pendingHeads: (
    inboxKey: string,
    options: { readonly limit: number; readonly now: number }
  ) => Effect.Effect<ReadonlyArray<PendingMessage>, StoreError>

  readonly recordDelivery: (
    inboxKey: string,
    relayMessageId: Identity.RelayMessageId,
    options: { readonly maxDeliveries: number; readonly now: number }
  ) => Effect.Effect<DeliveryRecord, StoreError>

  /**
   * Terminally settles a message.
   *
   * Takes the `messageHash` as part of the write predicate rather than trusting an in-memory
   * check: the delivering entity's memory is lost on runner death and on defect restart, and the
   * durable transition must not depend on state that can vanish.
   *
   * Reports whether it applied. A settle that silently matched no row — because the message was
   * already dead-lettered, expired, or settled — must be distinguishable from success, otherwise
   * the acknowledgement the recipient believes is durable never happened.
   *
   * `deduplicate_until` only ever grows. Shrinking it would reopen a deduplication window that a
   * sender may still be replaying into.
   */
  readonly settle: (
    inboxKey: string,
    relayMessageId: Identity.RelayMessageId,
    options: {
      readonly outcome: TerminalOutcome
      readonly messageHash: string
      readonly now: number
      readonly terminalRetentionMillis: number
    }
  ) => Effect.Effect<SettleResult, StoreError>

  readonly usage: (inboxKey: string) => Effect.Effect<Usage, StoreError>

  /**
   * Messages this inbox has given up on.
   *
   * Dead-lettering and expiry destroy a message the sender already released custody of, so both
   * have to be answerable after the fact rather than only appearing once in a log line.
   */
  readonly abandoned: (
    inboxKey: string,
    options: { readonly limit: number }
  ) => Effect.Effect<ReadonlyArray<AbandonedMessage>, StoreError>

  /**
   * Moves undelivered messages past their TTL to `Expired`.
   *
   * Takes the retention window because expiry is a terminal transition like any other: the
   * identity has to stay deduplicated for as long as its sender may still replay it.
   */
  readonly expire: (
    options: {
      readonly now: number
      readonly limit: number
      readonly terminalRetentionMillis: number
    }
  ) => Effect.Effect<number, StoreError>

  /** Removes terminal rows whose deduplication horizon has lapsed. */
  readonly collect: (
    options: { readonly now: number; readonly limit: number }
  ) => Effect.Effect<number, StoreError>
}>()("@lucas-barake/effect-local-rpc/RelayInboxStore") {}
