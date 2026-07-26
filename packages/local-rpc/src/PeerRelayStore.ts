import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as PlatformError from "effect/PlatformError"
import * as Random from "effect/Random"
import * as Schema from "effect/Schema"
import type * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { PeerPrincipal } from "./internal/peerPrincipal.js"
import * as PeerRelayMigrations from "./internal/peerRelayMigrations.js"
import {
  make as makeImmediateTransaction,
  type NestedPeerRelayTransactionError
} from "./internal/peerRelaySqliteTransaction.js"
import * as PeerRelayLimits from "./PeerRelayLimits.js"
import * as PeerRelayRpc from "./PeerRelayRpc.js"

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
  outerEnvelopeDigest: PeerRelayRpc.RelayDigest,
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
  outerEnvelopeDigest: PeerRelayRpc.RelayDigest,
  payloadBytes: NonNegativeInt,
  claimToken: PeerRelayRpc.ClaimToken,
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
  claimToken: PeerRelayRpc.ClaimToken,
  sessionGeneration: NonNegativeInt,
  payloadBytes: NonNegativeInt
})
export type LoadClaimedPayloadRequest = typeof LoadClaimedPayloadRequest.Type

export const TerminalRequest = Schema.Struct({
  channel: ChannelKey,
  relayMessageId: Identity.RelayMessageId,
  claimToken: PeerRelayRpc.ClaimToken,
  messageHash: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  sessionGeneration: NonNegativeInt,
  recipient: PeerPrincipal
})
export type TerminalRequest = typeof TerminalRequest.Type

export const RejectRequest = Schema.Struct({
  ...TerminalRequest.fields,
  reason: PeerRelayRpc.RejectReason
})
export type RejectRequest = typeof RejectRequest.Type

export const ReleaseRequest = Schema.Struct({
  channel: ChannelKey,
  relayMessageId: Identity.RelayMessageId,
  claimToken: PeerRelayRpc.ClaimToken,
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

export type StoreError =
  | ReplicaError.ReplicaError
  | NestedPeerRelayTransactionError

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

type UsageKind = UsageRequest["scopeKind"]

interface UsageScope {
  readonly kind: UsageKind
  readonly key: string
  readonly activeCountLimit: number
  readonly activeBytesLimit: number
  readonly retainedCountLimit: number
  readonly retainedBytesLimit: number
}

const storageUnavailable = (cause: unknown) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.StorageUnavailable({ cause })
  })

const storageCorrupt = (cause: unknown) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.StorageCorrupt({ cause })
  })

const protocolMismatch = (expected: string, observed: string) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.ProtocolMismatch({ expected, observed })
  })

const quotaExceeded = (resource: string, limit: number) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.QuotaExceeded({ resource, limit })
  })

type BoundaryError =
  | StoreError
  | SqlError.SqlError
  | Schema.SchemaError
  | PlatformError.PlatformError
  | Cause.NoSuchElementError

const mapStorageErrors = <A, R,>(effect: Effect.Effect<A, BoundaryError, R>) =>
  effect.pipe(
    Effect.catchTag("SqlError", (cause) => Effect.fail(storageUnavailable(cause))),
    Effect.catchTag("SchemaError", (cause) => Effect.fail(storageCorrupt(cause))),
    Effect.catchTag("PlatformError", (cause) => Effect.fail(storageUnavailable(cause))),
    Effect.catchTag("NoSuchElementError", (cause) => Effect.fail(storageCorrupt(cause)))
  )

const encodeKey = (...parts: ReadonlyArray<string | number>) => JSON.stringify(parts)

const channelScopes = (
  channel: ChannelKey,
  limits: PeerRelayLimits.Values
): ReadonlyArray<UsageScope> => [
  {
    kind: "SenderPeer",
    key: encodeKey(channel.tenantId, channel.senderSubjectId, channel.senderPeerId),
    activeCountLimit: limits.maxActiveMessagesPerSenderPeer,
    activeBytesLimit: limits.maxActiveBytesPerSenderPeer,
    retainedCountLimit: limits.maxRetainedRowsPerSenderPeer,
    retainedBytesLimit: limits.maxRetainedBytesPerSenderPeer
  },
  {
    kind: "RecipientPeer",
    key: encodeKey(channel.tenantId, channel.recipientSubjectId, channel.recipientPeerId),
    activeCountLimit: limits.maxActiveMessagesPerRecipientPeer,
    activeBytesLimit: limits.maxActiveBytesPerRecipientPeer,
    retainedCountLimit: limits.maxRetainedRowsPerRecipientPeer,
    retainedBytesLimit: limits.maxRetainedBytesPerRecipientPeer
  },
  {
    kind: "RecipientSubject",
    key: encodeKey(channel.tenantId, channel.recipientSubjectId),
    activeCountLimit: limits.maxActiveMessagesPerRecipientSubject,
    activeBytesLimit: limits.maxActiveBytesPerRecipientSubject,
    retainedCountLimit: limits.maxRetainedRowsPerRecipientSubject,
    retainedBytesLimit: limits.maxRetainedBytesPerRecipientSubject
  },
  {
    kind: "Tenant",
    key: encodeKey(channel.tenantId),
    activeCountLimit: limits.maxActiveMessagesPerTenant,
    activeBytesLimit: limits.maxActiveBytesPerTenant,
    retainedCountLimit: limits.maxRetainedRowsPerTenant,
    retainedBytesLimit: limits.maxRetainedBytesPerTenant
  },
  {
    kind: "Shard",
    key: encodeKey("local"),
    activeCountLimit: limits.maxActiveMessagesPerShard,
    activeBytesLimit: limits.maxActiveBytesPerShard,
    retainedCountLimit: limits.maxRetainedRowsPerShard,
    retainedBytesLimit: limits.maxRetainedBytesPerShard
  }
]

const TimeRow = Schema.Struct({ now: NonNegativeInt })
const UnitRow = Schema.Struct({ value: Schema.Int })

const nowQuery = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Schema.Void,
    Result: TimeRow,
    execute: () => sql`SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) AS now`
  })(undefined)

const decodeDocuments = Schema.decodeUnknownEffect(
  Schema.fromJsonString(DocumentIds)
)

const parseDocuments = (value: string) =>
  decodeDocuments(value).pipe(
    Effect.mapError((cause) => storageCorrupt(cause))
  )

const validateInput = <S extends Schema.Top,>(schema: S, input: unknown) =>
  Schema.decodeUnknownEffect(schema)(input).pipe(
    Effect.mapError((cause) => protocolMismatch("valid relay store request", String(cause)))
  )

const checkPragmas = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const journal = SqlSchema.findOne({
    Request: Schema.Void,
    Result: Schema.Struct({ journal_mode: Schema.String }),
    execute: () => sql`PRAGMA journal_mode = WAL`
  })
  const synchronous = SqlSchema.findOne({
    Request: Schema.Void,
    Result: Schema.Struct({ synchronous: Schema.Int }),
    execute: () => sql`PRAGMA synchronous`
  })
  const mode = yield* journal(undefined)
  if (mode.journal_mode.toLowerCase() !== "wal") {
    return yield* storageCorrupt(new Error("Relay custody requires SQLite WAL mode"))
  }
  yield* sql`PRAGMA synchronous = FULL`
  const setting = yield* synchronous(undefined)
  if (setting.synchronous !== 2) {
    return yield* storageCorrupt(new Error("Relay custody requires SQLite FULL synchronous mode"))
  }
})

const DuplicateRow = Schema.Struct({
  channelId: PositiveInt,
  tenantId: Schema.NonEmptyString,
  senderSubjectId: Schema.NonEmptyString,
  senderPeerId: Identity.PeerId,
  senderReplicaIncarnation: Identity.ReplicaIncarnation,
  recipientSubjectId: Schema.NonEmptyString,
  recipientPeerId: Identity.PeerId,
  outerEnvelopeDigest: PeerRelayRpc.RelayDigest,
  state: Schema.Literals(["Pending", "Claimed", "Acknowledged", "DeadLettered", "Expired"]),
  nextEligibleAt: NonNegativeInt
})

const ChannelRow = Schema.Struct({
  channelId: PositiveInt,
  nextSequence: NonNegativeInt
})

const MessageIdRow = Schema.Struct({ messageId: PositiveInt })

const CandidateRow = Schema.Struct({
  messageId: PositiveInt,
  channelId: PositiveInt,
  tenantId: Schema.NonEmptyString,
  senderSubjectId: Schema.NonEmptyString,
  senderPeerId: Identity.PeerId,
  senderReplicaIncarnation: Identity.ReplicaIncarnation,
  recipientSubjectId: Schema.NonEmptyString,
  recipientPeerId: Identity.PeerId,
  relayMessageId: Identity.RelayMessageId,
  relayPeerId: Identity.PeerId,
  senderConnectionEpoch: Schema.NonEmptyString,
  senderSequence: NonNegativeInt,
  documentIds: Schema.String,
  payloadVersion: Schema.Literal(1),
  messageHash: Schema.NonEmptyString,
  outerEnvelopeDigest: PeerRelayRpc.RelayDigest,
  payloadBytes: NonNegativeInt,
  createdAt: NonNegativeInt,
  nextEligibleAt: NonNegativeInt,
  retryCount: NonNegativeInt
})

const ReservationRow = Schema.Struct({
  senderPeerUsageKey: Schema.String,
  recipientPeerUsageKey: Schema.String,
  recipientSubjectUsageKey: Schema.String,
  tenantUsageKey: Schema.String,
  shardUsageKey: Schema.String,
  activeCountDelta: Schema.Literal(1),
  activeBytesDelta: NonNegativeInt,
  retainedCountDelta: Schema.Literal(1),
  retainedBytesDelta: NonNegativeInt,
  activeConsumed: Schema.Literals([0, 1]),
  retainedConsumed: Schema.Literals([0, 1])
})

const UsageRow = Schema.Struct({
  activeCount: NonNegativeInt,
  activeBytes: NonNegativeInt,
  retainedCount: NonNegativeInt,
  retainedBytes: NonNegativeInt
})

const KeyRow = Schema.Struct({ messageId: PositiveInt })

const makeService = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const crypto = yield* Crypto.Crypto
  const limits = yield* PeerRelayLimits.PeerRelayLimits
  yield* PeerRelayMigrations.run
  yield* checkPragmas.pipe(
    Effect.catchTag("NoSuchElementError", (cause) => Effect.fail(storageCorrupt(cause)))
  )

  const write = makeImmediateTransaction(sql, {
    maxAcquireAttempts: limits.sqliteLockRetryMaxAttempts,
    acquireRetryBaseDelayMillis: limits.sqliteLockRetryBaseDelayMillis,
    acquireRetryMaximumDelayMillis: limits.sqliteLockRetryMaximumDelayMillis
  })

  const findDuplicate = SqlSchema.findOneOption({
    Request: Schema.Struct({
      tenantId: Schema.String,
      senderSubjectId: Schema.String,
      senderPeerId: Schema.String,
      relayMessageId: Schema.String
    }),
    Result: DuplicateRow,
    execute: (request) =>
      sql`SELECT
          m.channel_id AS channelId,
          c.tenant_id AS tenantId,
          c.sender_subject_id AS senderSubjectId,
          c.sender_peer_id AS senderPeerId,
          c.sender_replica_incarnation AS senderReplicaIncarnation,
          c.recipient_subject_id AS recipientSubjectId,
          c.recipient_peer_id AS recipientPeerId,
          m.outer_envelope_digest AS outerEnvelopeDigest,
          m.state,
          m.next_eligible_at AS nextEligibleAt
        FROM effect_local_relay_messages m
        JOIN effect_local_relay_channels c ON c.channel_id = m.channel_id
        JOIN effect_local_relay_reservations r ON r.message_id = m.message_id
        WHERE m.tenant_id = ${request.tenantId}
          AND m.sender_subject_id = ${request.senderSubjectId}
          AND m.sender_peer_id = ${request.senderPeerId}
          AND m.relay_message_id = ${request.relayMessageId}`
  })

  const findChannel = SqlSchema.findOne({
    Request: ChannelKey,
    Result: ChannelRow,
    execute: (channel) =>
      sql`SELECT
          channel_id AS channelId,
          next_sequence AS nextSequence
        FROM effect_local_relay_channels
        WHERE tenant_id = ${channel.tenantId}
          AND sender_subject_id = ${channel.senderSubjectId}
          AND sender_peer_id = ${channel.senderPeerId}
          AND sender_replica_incarnation = ${channel.senderReplicaIncarnation}
          AND recipient_subject_id = ${channel.recipientSubjectId}
          AND recipient_peer_id = ${channel.recipientPeerId}`
  })

  const reserveUsage = (
    scope: UsageScope,
    payloadBytes: number
  ): Effect.Effect<void, ReplicaError.ReplicaError | SqlError.SqlError | Schema.SchemaError> =>
    Effect.gen(function*() {
      yield* sql`INSERT INTO effect_local_relay_usage (
          scope_kind,
          scope_key,
          active_count,
          active_bytes,
          retained_count,
          retained_bytes
        ) VALUES (${scope.kind}, ${scope.key}, 0, 0, 0, 0)
        ON CONFLICT(scope_kind, scope_key) DO NOTHING`
      const changed = yield* SqlSchema.findAll({
        Request: Schema.Void,
        Result: UnitRow,
        execute: () =>
          sql`UPDATE effect_local_relay_usage
            SET active_count = active_count + 1,
                active_bytes = active_bytes + ${payloadBytes},
                retained_count = retained_count + 1,
                retained_bytes = retained_bytes + ${payloadBytes}
            WHERE scope_kind = ${scope.kind}
              AND scope_key = ${scope.key}
              AND active_count + 1 <= ${scope.activeCountLimit}
              AND active_bytes + ${payloadBytes} <= ${scope.activeBytesLimit}
              AND retained_count + 1 <= ${scope.retainedCountLimit}
              AND retained_bytes + ${payloadBytes} <= ${scope.retainedBytesLimit}
            RETURNING 1 AS value`
      })(undefined)
      if (changed.length !== 1) {
        return yield* quotaExceeded(
          `${scope.kind} relay custody`,
          Math.min(scope.activeCountLimit, scope.retainedCountLimit)
        )
      }
    })

  const findReservation = SqlSchema.findOneOption({
    Request: PositiveInt,
    Result: ReservationRow,
    execute: (messageId) =>
      sql`SELECT
          sender_peer_usage_key AS senderPeerUsageKey,
          recipient_peer_usage_key AS recipientPeerUsageKey,
          recipient_subject_usage_key AS recipientSubjectUsageKey,
          tenant_usage_key AS tenantUsageKey,
          shard_usage_key AS shardUsageKey,
          active_count_delta AS activeCountDelta,
          active_bytes_delta AS activeBytesDelta,
          retained_count_delta AS retainedCountDelta,
          retained_bytes_delta AS retainedBytesDelta,
          active_consumed AS activeConsumed,
          retained_consumed AS retainedConsumed
        FROM effect_local_relay_reservations
        WHERE message_id = ${messageId}`
  })

  const usageKeys = (
    reservation: typeof ReservationRow.Type
  ): ReadonlyArray<readonly [UsageKind, string]> => [
    ["SenderPeer", reservation.senderPeerUsageKey],
    ["RecipientPeer", reservation.recipientPeerUsageKey],
    ["RecipientSubject", reservation.recipientSubjectUsageKey],
    ["Tenant", reservation.tenantUsageKey],
    ["Shard", reservation.shardUsageKey]
  ]

  const releaseActiveUsage = (
    messageId: number
  ): Effect.Effect<void, ReplicaError.ReplicaError | SqlError.SqlError | Schema.SchemaError> =>
    Effect.gen(function*() {
      const reservationOption = yield* findReservation(messageId)
      if (Option.isNone(reservationOption)) {
        return yield* storageCorrupt(new Error("Missing relay quota reservation"))
      }
      const reservation = reservationOption.value
      if (reservation.activeConsumed === 1) {
        return
      }
      for (const [kind, key] of usageKeys(reservation)) {
        const changed = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: UnitRow,
          execute: () =>
            sql`UPDATE effect_local_relay_usage
              SET active_count = active_count - ${reservation.activeCountDelta},
                  active_bytes = active_bytes - ${reservation.activeBytesDelta}
              WHERE scope_kind = ${kind}
                AND scope_key = ${key}
                AND active_count >= ${reservation.activeCountDelta}
                AND active_bytes >= ${reservation.activeBytesDelta}
              RETURNING 1 AS value`
        })(undefined)
        if (changed.length !== 1) {
          return yield* storageCorrupt(new Error("Invalid active relay quota reservation"))
        }
        yield* sql`DELETE FROM effect_local_relay_usage
          WHERE scope_kind = ${kind}
            AND scope_key = ${key}
            AND active_count = 0
            AND active_bytes = 0
            AND retained_count = 0
            AND retained_bytes = 0`
      }
      yield* sql`UPDATE effect_local_relay_reservations
        SET active_consumed = 1
        WHERE message_id = ${messageId} AND active_consumed = 0`
    })

  const releaseRetainedUsage = (
    messageId: number
  ): Effect.Effect<void, ReplicaError.ReplicaError | SqlError.SqlError | Schema.SchemaError> =>
    Effect.gen(function*() {
      const reservationOption = yield* findReservation(messageId)
      if (Option.isNone(reservationOption)) {
        return yield* storageCorrupt(new Error("Missing relay quota reservation"))
      }
      const reservation = reservationOption.value
      if (reservation.retainedConsumed === 1) {
        return
      }
      for (const [kind, key] of usageKeys(reservation)) {
        const changed = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: UnitRow,
          execute: () =>
            sql`UPDATE effect_local_relay_usage
              SET retained_count = retained_count - ${reservation.retainedCountDelta},
                  retained_bytes = retained_bytes - ${reservation.retainedBytesDelta}
              WHERE scope_kind = ${kind}
                AND scope_key = ${key}
                AND retained_count >= ${reservation.retainedCountDelta}
                AND retained_bytes >= ${reservation.retainedBytesDelta}
              RETURNING 1 AS value`
        })(undefined)
        if (changed.length !== 1) {
          return yield* storageCorrupt(new Error("Invalid retained relay quota reservation"))
        }
      }
      yield* sql`UPDATE effect_local_relay_reservations
        SET retained_consumed = 1
        WHERE message_id = ${messageId} AND retained_consumed = 0`
    })

  const terminalize = (
    messageId: number,
    state: "Acknowledged" | "DeadLettered" | "Expired",
    now: number,
    terminal: {
      readonly token?: PeerRelayRpc.ClaimToken
      readonly sessionGeneration?: number
      readonly reason: string
    }
  ) =>
    Effect.gen(function*() {
      yield* releaseActiveUsage(messageId)
      yield* sql`UPDATE effect_local_relay_messages
        SET state = ${state},
            payload = NULL,
            payload_length = 0,
            claim_token = NULL,
            claim_session_generation = NULL,
            claim_deadline = NULL,
            terminal_at = ${now},
            terminal_claim_token = ${terminal.token ?? null},
            terminal_session_generation = ${terminal.sessionGeneration ?? null},
            terminal_reason = ${terminal.reason},
            deduplicate_until = MAX(
              deduplicate_until,
              ${now + limits.minimumTerminalRetentionMillis}
            )
        WHERE message_id = ${messageId}
          AND state IN ('Pending', 'Claimed')`
      yield* sql`UPDATE effect_local_relay_channels
        SET claimed_message_id = NULL,
            claim_session_generation = NULL,
            claim_token = NULL,
            claim_deadline = NULL
        WHERE claimed_message_id = ${messageId}`
    })

  const admit: Service["admit"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(Admission, unsafeInput)
      const payloadBytes = input.payload.byteLength
      if (
        input.messageTtlMillis > limits.messageTtlMillis ||
        input.senderRetryHorizonMillis > limits.maximumSenderRetryHorizonMillis ||
        input.minimumTerminalRetentionMillis < limits.minimumTerminalRetentionMillis
      ) {
        return yield* protocolMismatch("configured relay retention horizon", "unsupported horizon")
      }
      if (
        payloadBytes > limits.maxActiveBytesPerSenderPeer ||
        payloadBytes > limits.maxActiveBytesPerRecipientPeer ||
        payloadBytes > limits.maxActiveBytesPerRecipientSubject ||
        payloadBytes > limits.maxActiveBytesPerTenant ||
        payloadBytes > limits.maxActiveBytesPerShard
      ) {
        return yield* quotaExceeded("relay payload bytes", limits.maxActiveBytesPerShard)
      }
      return yield* write(Effect.gen(function*() {
        const now = (yield* nowQuery(sql)).now
        const existing = yield* findDuplicate({
          tenantId: input.channel.tenantId,
          senderSubjectId: input.channel.senderSubjectId,
          senderPeerId: input.channel.senderPeerId,
          relayMessageId: input.relayMessageId
        })
        if (Option.isSome(existing)) {
          if (existing.value.outerEnvelopeDigest !== input.outerEnvelopeDigest) {
            return yield* protocolMismatch(
              existing.value.outerEnvelopeDigest,
              input.outerEnvelopeDigest
            )
          }
          return AdmissionResult.make({
            status: "Duplicate",
            channel: ChannelKey.make({
              tenantId: existing.value.tenantId,
              senderSubjectId: existing.value.senderSubjectId,
              senderPeerId: existing.value.senderPeerId,
              senderReplicaIncarnation: existing.value.senderReplicaIncarnation,
              recipientSubjectId: existing.value.recipientSubjectId,
              recipientPeerId: existing.value.recipientPeerId
            }),
            ready: existing.value.state === "Pending" && existing.value.nextEligibleAt <= now,
            nextEligibleAt: existing.value.nextEligibleAt,
            lane: "New"
          })
        }
        yield* sql`INSERT INTO effect_local_relay_channels (
            tenant_id,
            sender_subject_id,
            sender_peer_id,
            sender_replica_incarnation,
            recipient_subject_id,
            recipient_peer_id
          ) VALUES (
            ${input.channel.tenantId},
            ${input.channel.senderSubjectId},
            ${input.channel.senderPeerId},
            ${input.channel.senderReplicaIncarnation},
            ${input.channel.recipientSubjectId},
            ${input.channel.recipientPeerId}
          )
          ON CONFLICT (
            tenant_id,
            sender_subject_id,
            sender_peer_id,
            sender_replica_incarnation,
            recipient_subject_id,
            recipient_peer_id
          ) DO NOTHING`
        const channel = yield* findChannel(input.channel)
        const scopes = channelScopes(input.channel, limits)
        for (const scope of scopes) {
          yield* reserveUsage(scope, payloadBytes)
        }
        const duplicateHorizon = Math.max(
          input.messageTtlMillis,
          input.senderRetryHorizonMillis
        )
        const inserted = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: MessageIdRow,
          execute: () =>
            sql`INSERT INTO effect_local_relay_messages (
                channel_id,
                channel_sequence,
                tenant_id,
                sender_subject_id,
                sender_peer_id,
                relay_message_id,
                relay_peer_id,
                sender_connection_epoch,
                sender_sequence,
                document_ids,
                payload_version,
                message_hash,
                outer_envelope_digest,
                payload,
                payload_length,
                state,
                created_at,
                expires_at,
                deduplicate_until,
                next_eligible_at
              ) VALUES (
                ${channel.channelId},
                ${channel.nextSequence},
                ${input.channel.tenantId},
                ${input.channel.senderSubjectId},
                ${input.channel.senderPeerId},
                ${input.relayMessageId},
                ${input.relayPeerId},
                ${input.senderConnectionEpoch},
                ${input.senderSequence},
                ${JSON.stringify(input.documentIds)},
                ${input.payloadVersion},
                ${input.messageHash},
                ${input.outerEnvelopeDigest},
                ${input.payload},
                ${payloadBytes},
                'Pending',
                ${now},
                ${now + input.messageTtlMillis},
                ${now + duplicateHorizon},
                ${now}
              )
              RETURNING message_id AS messageId`
        })(undefined)
        yield* sql`INSERT INTO effect_local_relay_reservations (
            message_id,
            sender_peer_usage_key,
            recipient_peer_usage_key,
            recipient_subject_usage_key,
            tenant_usage_key,
            shard_usage_key,
            active_count_delta,
            active_bytes_delta,
            retained_count_delta,
            retained_bytes_delta
          ) VALUES (
            ${inserted.messageId},
            ${scopes[0]!.key},
            ${scopes[1]!.key},
            ${scopes[2]!.key},
            ${scopes[3]!.key},
            ${scopes[4]!.key},
            1,
            ${payloadBytes},
            1,
            ${payloadBytes}
          )`
        yield* sql`UPDATE effect_local_relay_channels
          SET next_sequence = next_sequence + 1
          WHERE channel_id = ${channel.channelId}
            AND next_sequence = ${channel.nextSequence}`
        return AdmissionResult.make({
          status: "Accepted",
          channel: input.channel,
          ready: true,
          nextEligibleAt: now,
          lane: "New"
        })
      }))
    })).pipe(Effect.withSpan("PeerRelayStore.admit"))

  const findCandidate = SqlSchema.findOneOption({
    Request: Schema.Struct({
      tenantId: Schema.String,
      recipientSubjectId: Schema.String,
      recipientPeerId: Schema.String,
      senderSubjectId: Schema.String,
      senderPeerId: Schema.String,
      authorizedDocumentIds: Schema.String,
      now: NonNegativeInt
    }),
    Result: CandidateRow,
    execute: (request) =>
      sql`SELECT
          m.message_id AS messageId,
          m.channel_id AS channelId,
          c.tenant_id AS tenantId,
          c.sender_subject_id AS senderSubjectId,
          c.sender_peer_id AS senderPeerId,
          c.sender_replica_incarnation AS senderReplicaIncarnation,
          c.recipient_subject_id AS recipientSubjectId,
          c.recipient_peer_id AS recipientPeerId,
          m.relay_message_id AS relayMessageId,
          m.relay_peer_id AS relayPeerId,
          m.sender_connection_epoch AS senderConnectionEpoch,
          m.sender_sequence AS senderSequence,
          m.document_ids AS documentIds,
          m.payload_version AS payloadVersion,
          m.message_hash AS messageHash,
          m.outer_envelope_digest AS outerEnvelopeDigest,
          m.payload_length AS payloadBytes,
          m.created_at AS createdAt,
          m.next_eligible_at AS nextEligibleAt,
          m.retry_count AS retryCount
        FROM effect_local_relay_messages m
        JOIN effect_local_relay_channels c ON c.channel_id = m.channel_id
        JOIN effect_local_relay_reservations r ON r.message_id = m.message_id
        WHERE c.tenant_id = ${request.tenantId}
          AND c.recipient_subject_id = ${request.recipientSubjectId}
          AND c.recipient_peer_id = ${request.recipientPeerId}
          AND c.sender_subject_id = ${request.senderSubjectId}
          AND c.sender_peer_id = ${request.senderPeerId}
          AND m.tenant_id = c.tenant_id
          AND m.sender_subject_id = c.sender_subject_id
          AND m.sender_peer_id = c.sender_peer_id
          AND c.claimed_message_id IS NULL
          AND m.state = 'Pending'
          AND m.next_eligible_at <= ${request.now}
          AND m.expires_at > ${request.now}
          AND m.payload IS NOT NULL
          AND m.payload_length = length(m.payload)
          AND r.active_consumed = 0
          AND NOT EXISTS (
            SELECT 1
            FROM effect_local_relay_messages earlier
            WHERE earlier.channel_id = m.channel_id
              AND earlier.channel_sequence < m.channel_sequence
              AND earlier.state IN ('Pending', 'Claimed')
          )
          AND NOT EXISTS (
            SELECT 1
            FROM json_each(m.document_ids) document
            WHERE document.value NOT IN (
              SELECT value FROM json_each(${request.authorizedDocumentIds})
            )
          )
        ORDER BY m.created_at, m.message_id
        LIMIT 1`
  })

  const claim: Service["claim"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(ClaimRequest, unsafeInput)
      return yield* write(Effect.gen(function*() {
        const now = (yield* nowQuery(sql)).now
        const candidateOption = yield* findCandidate({
          tenantId: input.recipient.tenantId,
          recipientSubjectId: input.recipient.subjectId,
          recipientPeerId: input.recipient.peerId,
          senderSubjectId: input.sender.subjectId,
          senderPeerId: input.sender.peerId,
          authorizedDocumentIds: JSON.stringify(input.authorizedDocumentIds),
          now
        })
        if (Option.isNone(candidateOption)) {
          return {
            message: Option.none(),
            ready: false,
            nextEligibleAt: Option.none(),
            lane: "New" as const
          }
        }
        const candidate = candidateOption.value
        const documentIds = yield* parseDocuments(candidate.documentIds)
        const uuid = yield* crypto.randomUUIDv4
        const token = PeerRelayRpc.ClaimToken.make(`clm_${uuid}`)
        const deadline = now + limits.claimLeaseMillis
        const claimed = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: UnitRow,
          execute: () =>
            sql`UPDATE effect_local_relay_messages
              SET state = 'Claimed',
                  claim_token = ${token},
                  claim_session_generation = ${input.sessionGeneration},
                  claim_deadline = ${deadline}
              WHERE message_id = ${candidate.messageId}
                AND state = 'Pending'
              RETURNING 1 AS value`
        })(undefined)
        if (claimed.length !== 1) {
          return yield* storageCorrupt(new Error("Relay claim compare and swap failed"))
        }
        const channelClaimed = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: UnitRow,
          execute: () =>
            sql`UPDATE effect_local_relay_channels
              SET claimed_message_id = ${candidate.messageId},
                  claim_session_generation = ${input.sessionGeneration},
                  claim_token = ${token},
                  claim_deadline = ${deadline}
              WHERE channel_id = ${candidate.channelId}
                AND claimed_message_id IS NULL
              RETURNING 1 AS value`
        })(undefined)
        if (channelClaimed.length !== 1) {
          return yield* storageCorrupt(new Error("Relay channel claim compare and swap failed"))
        }
        return {
          message: Option.some(ClaimedMessage.make({
            rowId: candidate.messageId,
            channel: ChannelKey.make({
              tenantId: candidate.tenantId,
              senderSubjectId: candidate.senderSubjectId,
              senderPeerId: candidate.senderPeerId,
              senderReplicaIncarnation: candidate.senderReplicaIncarnation,
              recipientSubjectId: candidate.recipientSubjectId,
              recipientPeerId: candidate.recipientPeerId
            }),
            relayMessageId: candidate.relayMessageId,
            relayPeerId: candidate.relayPeerId,
            senderConnectionEpoch: candidate.senderConnectionEpoch,
            senderSequence: candidate.senderSequence,
            documentIds,
            payloadVersion: candidate.payloadVersion,
            messageHash: candidate.messageHash,
            outerEnvelopeDigest: candidate.outerEnvelopeDigest,
            payloadBytes: candidate.payloadBytes,
            claimToken: token,
            claimDeadline: deadline,
            sessionGeneration: input.sessionGeneration
          })),
          ready: false,
          nextEligibleAt: Option.none(),
          lane: candidate.retryCount === 0 ? "New" as const : "Retry" as const
        }
      }))
    })).pipe(Effect.withSpan("PeerRelayStore.claim"))

  const loadClaimedPayload: Service["loadClaimedPayload"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(LoadClaimedPayloadRequest, unsafeInput)
      const rows = yield* SqlSchema.findAll({
        Request: Schema.Void,
        Result: Schema.Struct({ payload: Schema.Uint8Array }),
        execute: () =>
          sql`SELECT m.payload
            FROM effect_local_relay_messages m
            JOIN effect_local_relay_channels c ON c.channel_id = m.channel_id
            WHERE m.message_id = ${input.rowId}
              AND m.channel_id = c.channel_id
              AND c.tenant_id = ${input.channel.tenantId}
              AND c.sender_subject_id = ${input.channel.senderSubjectId}
              AND c.sender_peer_id = ${input.channel.senderPeerId}
              AND c.sender_replica_incarnation = ${input.channel.senderReplicaIncarnation}
              AND c.recipient_subject_id = ${input.channel.recipientSubjectId}
              AND c.recipient_peer_id = ${input.channel.recipientPeerId}
              AND m.tenant_id = c.tenant_id
              AND m.sender_subject_id = c.sender_subject_id
              AND m.sender_peer_id = c.sender_peer_id
              AND m.relay_message_id = ${input.relayMessageId}
              AND m.state = 'Claimed'
              AND m.claim_token = ${input.claimToken}
              AND m.claim_session_generation = ${input.sessionGeneration}
              AND c.claimed_message_id = m.message_id
              AND c.claim_token = m.claim_token
              AND c.claim_session_generation = m.claim_session_generation
              AND c.claim_deadline = m.claim_deadline
              AND m.claim_deadline > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
              AND m.expires_at > CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
              AND m.payload_length = ${input.payloadBytes}
              AND length(m.payload) = ${input.payloadBytes}
              AND m.payload_length <= ${limits.maxActiveBytesPerShard}`
      })(undefined)
      if (rows.length !== 1) {
        return yield* protocolMismatch("active relay claim", "stale relay claim")
      }
      return rows[0]!.payload
    })).pipe(Effect.withSpan("PeerRelayStore.loadClaimedPayload"))

  const TerminalRow = Schema.Struct({
    messageId: PositiveInt,
    channelId: PositiveInt,
    state: Schema.Literals(["Pending", "Claimed", "Acknowledged", "DeadLettered", "Expired"]),
    messageHash: Schema.NonEmptyString,
    claimToken: Schema.NullOr(PeerRelayRpc.ClaimToken),
    claimSessionGeneration: Schema.NullOr(NonNegativeInt),
    claimDeadline: Schema.NullOr(NonNegativeInt),
    expiresAt: NonNegativeInt,
    channelClaimedMessageId: Schema.NullOr(PositiveInt),
    channelClaimToken: Schema.NullOr(PeerRelayRpc.ClaimToken),
    channelClaimSessionGeneration: Schema.NullOr(NonNegativeInt),
    channelClaimDeadline: Schema.NullOr(NonNegativeInt),
    terminalClaimToken: Schema.NullOr(PeerRelayRpc.ClaimToken),
    terminalSessionGeneration: Schema.NullOr(NonNegativeInt),
    terminalReason: Schema.NullOr(Schema.String)
  })

  const findTerminal = SqlSchema.findOneOption({
    Request: Schema.Struct({
      channel: ChannelKey,
      relayMessageId: Identity.RelayMessageId
    }),
    Result: TerminalRow,
    execute: ({ channel, relayMessageId }) =>
      sql`SELECT
          m.message_id AS messageId,
          m.channel_id AS channelId,
          m.state,
          m.message_hash AS messageHash,
          m.claim_token AS claimToken,
          m.claim_session_generation AS claimSessionGeneration,
          m.claim_deadline AS claimDeadline,
          m.expires_at AS expiresAt,
          c.claimed_message_id AS channelClaimedMessageId,
          c.claim_token AS channelClaimToken,
          c.claim_session_generation AS channelClaimSessionGeneration,
          c.claim_deadline AS channelClaimDeadline,
          m.terminal_claim_token AS terminalClaimToken,
          m.terminal_session_generation AS terminalSessionGeneration,
          m.terminal_reason AS terminalReason
        FROM effect_local_relay_messages m
        JOIN effect_local_relay_channels c ON c.channel_id = m.channel_id
        WHERE c.tenant_id = ${channel.tenantId}
          AND c.sender_subject_id = ${channel.senderSubjectId}
          AND c.sender_peer_id = ${channel.senderPeerId}
          AND c.sender_replica_incarnation = ${channel.senderReplicaIncarnation}
          AND c.recipient_subject_id = ${channel.recipientSubjectId}
          AND c.recipient_peer_id = ${channel.recipientPeerId}
          AND m.relay_message_id = ${relayMessageId}`
  })

  const ReadyRow = Schema.Struct({
    nextEligibleAt: NonNegativeInt,
    retryCount: NonNegativeInt
  })

  const nextReady = (
    channel: ChannelKey,
    now: number
  ): Effect.Effect<
    { readonly ready: boolean; readonly nextEligibleAt: Option.Option<number>; readonly lane: "New" | "Retry" },
    SqlError.SqlError | Schema.SchemaError
  > =>
    SqlSchema.findOneOption({
      Request: Schema.Void,
      Result: ReadyRow,
      execute: () =>
        sql`SELECT
            m.next_eligible_at AS nextEligibleAt,
            m.retry_count AS retryCount
          FROM effect_local_relay_messages m
          JOIN effect_local_relay_channels c ON c.channel_id = m.channel_id
          WHERE c.tenant_id = ${channel.tenantId}
            AND c.sender_subject_id = ${channel.senderSubjectId}
            AND c.sender_peer_id = ${channel.senderPeerId}
            AND c.recipient_subject_id = ${channel.recipientSubjectId}
            AND c.recipient_peer_id = ${channel.recipientPeerId}
            AND c.claimed_message_id IS NULL
            AND m.state = 'Pending'
            AND m.expires_at > ${now}
            AND NOT EXISTS (
              SELECT 1
              FROM effect_local_relay_messages earlier
              WHERE earlier.channel_id = m.channel_id
                AND earlier.channel_sequence < m.channel_sequence
                AND earlier.state IN ('Pending', 'Claimed')
            )
          ORDER BY m.next_eligible_at, m.created_at, m.message_id
          LIMIT 1`
    })(undefined).pipe(
      Effect.map((row) =>
        Option.isNone(row)
          ? {
            ready: false,
            nextEligibleAt: Option.none(),
            lane: "New" as const
          }
          : {
            ready: row.value.nextEligibleAt <= now,
            nextEligibleAt: Option.some(row.value.nextEligibleAt),
            lane: row.value.retryCount === 0 ? "New" as const : "Retry" as const
          }
      )
    )

  const terminalTransition = (
    unsafeInput: TerminalRequest,
    state: "Acknowledged" | "DeadLettered",
    reason: string
  ): Effect.Effect<TransitionResult, StoreError> =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(TerminalRequest, unsafeInput)
      if (
        input.recipient.tenantId !== input.channel.tenantId ||
        input.recipient.subjectId !== input.channel.recipientSubjectId ||
        input.recipient.peerId !== input.channel.recipientPeerId
      ) {
        return {
          status: "Stale",
          ready: false,
          nextEligibleAt: Option.none(),
          lane: "New"
        } as const
      }
      return yield* write(Effect.gen(function*() {
        const now = (yield* nowQuery(sql)).now
        const rowOption = yield* findTerminal({
          channel: input.channel,
          relayMessageId: input.relayMessageId
        })
        if (Option.isNone(rowOption)) {
          return {
            status: "Stale",
            ready: false,
            nextEligibleAt: Option.none(),
            lane: "New"
          } as const
        }
        const row = rowOption.value
        const active = row.state === "Claimed" &&
          row.messageHash === input.messageHash &&
          row.claimToken === input.claimToken &&
          row.claimSessionGeneration === input.sessionGeneration &&
          row.claimDeadline !== null &&
          row.claimDeadline > now &&
          row.expiresAt > now &&
          row.channelClaimedMessageId === row.messageId &&
          row.channelClaimToken === row.claimToken &&
          row.channelClaimSessionGeneration === row.claimSessionGeneration &&
          row.channelClaimDeadline === row.claimDeadline
        if (active) {
          yield* terminalize(row.messageId, state, now, {
            token: input.claimToken,
            sessionGeneration: input.sessionGeneration,
            reason
          })
          const hint = yield* nextReady(input.channel, now)
          return { status: "Changed", ...hint } as const
        }
        const duplicate = row.state === state &&
          row.messageHash === input.messageHash &&
          row.terminalClaimToken === input.claimToken &&
          row.terminalSessionGeneration === input.sessionGeneration &&
          row.terminalReason === reason
        if (duplicate) {
          const hint = yield* nextReady(input.channel, now)
          return { status: "Duplicate", ...hint } as const
        }
        return {
          status: "Stale",
          ready: false,
          nextEligibleAt: Option.none(),
          lane: "New"
        } as const
      }))
    }))

  const acknowledge: Service["acknowledge"] = (input) =>
    terminalTransition(input, "Acknowledged", "Acknowledged").pipe(
      Effect.withSpan("PeerRelayStore.acknowledge")
    )

  const reject: Service["reject"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(RejectRequest, unsafeInput)
      return yield* terminalTransition(
        TerminalRequest.make({
          channel: input.channel,
          relayMessageId: input.relayMessageId,
          claimToken: input.claimToken,
          messageHash: input.messageHash,
          sessionGeneration: input.sessionGeneration,
          recipient: input.recipient
        }),
        "DeadLettered",
        input.reason
      )
    })).pipe(Effect.withSpan("PeerRelayStore.reject"))

  const release: Service["release"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(ReleaseRequest, unsafeInput)
      return yield* write(Effect.gen(function*() {
        const now = (yield* nowQuery(sql)).now
        const rowOption = yield* findTerminal({
          channel: input.channel,
          relayMessageId: input.relayMessageId
        })
        if (Option.isNone(rowOption)) {
          return {
            status: "Stale",
            ready: false,
            nextEligibleAt: Option.none(),
            lane: "Retry"
          } as const
        }
        const row = rowOption.value
        if (
          row.state !== "Claimed" ||
          row.claimToken !== input.claimToken ||
          row.claimSessionGeneration !== input.sessionGeneration
        ) {
          return {
            status: "Stale",
            ready: false,
            nextEligibleAt: Option.none(),
            lane: "Retry"
          } as const
        }
        const random = yield* Random.next
        const retry = yield* SqlSchema.findOne({
          Request: Schema.Void,
          Result: Schema.Struct({ retryCount: PositiveInt }),
          execute: () =>
            sql`SELECT retry_count + 1 AS retryCount
              FROM effect_local_relay_messages
              WHERE message_id = ${row.messageId}`
        })(undefined)
        const maximum = Math.min(
          limits.retryMaximumDelayMillis,
          limits.retryBaseDelayMillis * 2 ** Math.min(retry.retryCount - 1, 30)
        )
        const delay = Math.max(1, Math.floor(maximum / 2 + random * maximum / 2))
        const nextEligibleAt = now + delay
        yield* sql`UPDATE effect_local_relay_messages
          SET state = 'Pending',
              retry_count = ${retry.retryCount},
              next_eligible_at = ${nextEligibleAt},
              claim_token = NULL,
              claim_session_generation = NULL,
              claim_deadline = NULL
          WHERE message_id = ${row.messageId}
            AND state = 'Claimed'
            AND claim_token = ${input.claimToken}
            AND claim_session_generation = ${input.sessionGeneration}`
        yield* sql`UPDATE effect_local_relay_channels
          SET claimed_message_id = NULL,
              claim_session_generation = NULL,
              claim_token = NULL,
              claim_deadline = NULL
          WHERE channel_id = ${row.channelId}
            AND claimed_message_id = ${row.messageId}
            AND claim_token = ${input.claimToken}
            AND claim_session_generation = ${input.sessionGeneration}`
        return {
          status: "Changed",
          ready: false,
          nextEligibleAt: Option.some(nextEligibleAt),
          lane: "Retry"
        } as const
      }))
    })).pipe(Effect.withSpan("PeerRelayStore.release"))

  const maintenanceResult = (
    ids: ReadonlyArray<number>,
    requestedBatchSize: number
  ): MaintenanceResult => {
    const processedIds = ids.slice(0, requestedBatchSize)
    const cursor = processedIds.at(-1)
    return cursor === undefined
      ? { processed: 0, hasMore: false }
      : {
        cursor,
        processed: processedIds.length,
        hasMore: ids.length > requestedBatchSize
      }
  }

  const recover: Service["recover"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(MaintenanceRequest, unsafeInput)
      const effectiveBatch = Math.min(input.batchSize, limits.claimRecoveryBatchSize)
      return yield* write(Effect.gen(function*() {
        const now = (yield* nowQuery(sql)).now
        const rows = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: Schema.Struct({
            messageId: PositiveInt,
            retryCount: NonNegativeInt
          }),
          execute: () =>
            sql`SELECT
                message_id AS messageId,
                retry_count AS retryCount
              FROM effect_local_relay_messages
              WHERE message_id > ${input.cursor ?? 0}
                AND state = 'Claimed'
                AND claim_deadline <= ${now}
              ORDER BY message_id
              LIMIT ${effectiveBatch + 1}`
        })(undefined)
        for (const row of rows.slice(0, effectiveBatch)) {
          const random = yield* Random.next
          const retryCount = row.retryCount + 1
          const maximum = Math.min(
            limits.retryMaximumDelayMillis,
            limits.retryBaseDelayMillis * 2 ** Math.min(retryCount - 1, 30)
          )
          const delay = Math.max(1, Math.floor(maximum / 2 + random * maximum / 2))
          yield* sql`UPDATE effect_local_relay_messages
            SET state = 'Pending',
                retry_count = ${retryCount},
                next_eligible_at = ${now + delay},
                claim_token = NULL,
                claim_session_generation = NULL,
                claim_deadline = NULL
            WHERE message_id = ${row.messageId}
              AND state = 'Claimed'
              AND claim_deadline <= ${now}`
          yield* sql`UPDATE effect_local_relay_channels
            SET claimed_message_id = NULL,
                claim_session_generation = NULL,
                claim_token = NULL,
                claim_deadline = NULL
            WHERE claimed_message_id = ${row.messageId}
              AND claim_deadline <= ${now}`
        }
        return maintenanceResult(rows.map((row) => row.messageId), effectiveBatch)
      }))
    })).pipe(Effect.withSpan("PeerRelayStore.recover"))

  const expire: Service["expire"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(MaintenanceRequest, unsafeInput)
      const effectiveBatch = Math.min(input.batchSize, limits.expiryBatchSize)
      return yield* write(Effect.gen(function*() {
        const now = (yield* nowQuery(sql)).now
        const rows = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: KeyRow,
          execute: () =>
            sql`SELECT message_id AS messageId
              FROM effect_local_relay_messages
              WHERE message_id > ${input.cursor ?? 0}
                AND state IN ('Pending', 'Claimed')
                AND expires_at <= ${now}
              ORDER BY message_id
              LIMIT ${effectiveBatch + 1}`
        })(undefined)
        for (const row of rows.slice(0, effectiveBatch)) {
          yield* terminalize(row.messageId, "Expired", now, { reason: "Expired" })
        }
        return maintenanceResult(rows.map((row) => row.messageId), effectiveBatch)
      }))
    })).pipe(Effect.withSpan("PeerRelayStore.expire"))

  const IntegrityRow = Schema.Struct({
    messageId: PositiveInt,
    state: Schema.String,
    messageTenantId: Schema.NonEmptyString,
    channelTenantId: Schema.NonEmptyString,
    messageSenderSubjectId: Schema.NonEmptyString,
    channelSenderSubjectId: Schema.NonEmptyString,
    messageSenderPeerId: Schema.String,
    channelSenderPeerId: Schema.String,
    payloadLength: NonNegativeInt,
    actualLength: Schema.NullOr(NonNegativeInt),
    reservationPresent: Schema.Literals([0, 1]),
    activeConsumed: Schema.NullOr(Schema.Literals([0, 1])),
    retainedConsumed: Schema.NullOr(Schema.Literals([0, 1]))
  })

  const repair: Service["repair"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(MaintenanceRequest, unsafeInput)
      const effectiveBatch = Math.min(input.batchSize, limits.integrityBatchSize)
      return yield* write(Effect.gen(function*() {
        const now = (yield* nowQuery(sql)).now
        const rows = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: IntegrityRow,
          execute: () =>
            sql`SELECT
                m.message_id AS messageId,
                m.state,
                m.tenant_id AS messageTenantId,
                c.tenant_id AS channelTenantId,
                m.sender_subject_id AS messageSenderSubjectId,
                c.sender_subject_id AS channelSenderSubjectId,
                m.sender_peer_id AS messageSenderPeerId,
                c.sender_peer_id AS channelSenderPeerId,
                m.payload_length AS payloadLength,
                length(m.payload) AS actualLength,
                CASE WHEN r.message_id IS NULL THEN 0 ELSE 1 END AS reservationPresent,
                r.active_consumed AS activeConsumed,
                r.retained_consumed AS retainedConsumed
              FROM effect_local_relay_messages m
              JOIN effect_local_relay_channels c ON c.channel_id = m.channel_id
              LEFT JOIN effect_local_relay_reservations r ON r.message_id = m.message_id
              WHERE m.message_id > ${input.cursor ?? 0}
              ORDER BY m.message_id
              LIMIT ${effectiveBatch + 1}`
        })(undefined)
        for (const row of rows.slice(0, effectiveBatch)) {
          if (
            row.reservationPresent === 0 ||
            row.activeConsumed === null ||
            row.retainedConsumed === null
          ) {
            return yield* storageCorrupt(new Error("Relay reservation reconciliation required"))
          }
          const active = row.state === "Pending" || row.state === "Claimed"
          const terminal = row.state === "Acknowledged" ||
            row.state === "DeadLettered" ||
            row.state === "Expired"
          if (
            (active && row.activeConsumed !== 0) ||
            (terminal && row.activeConsumed !== 1)
          ) {
            return yield* storageCorrupt(new Error("Corrupt relay reservation entitlement"))
          }
          const corrupt = (!active && !terminal) ||
            row.messageTenantId !== row.channelTenantId ||
            row.messageSenderSubjectId !== row.channelSenderSubjectId ||
            row.messageSenderPeerId !== row.channelSenderPeerId ||
            (active && (
              row.actualLength === null ||
              row.actualLength !== row.payloadLength
            )) ||
            (terminal && (
              row.actualLength !== null ||
              row.payloadLength !== 0
            ))
          if (corrupt && active) {
            yield* terminalize(row.messageId, "DeadLettered", now, { reason: "Corrupt" })
          } else if (corrupt) {
            return yield* storageCorrupt(new Error("Corrupt terminal relay row"))
          }
        }
        return maintenanceResult(rows.map((row) => row.messageId), effectiveBatch)
      }))
    })).pipe(Effect.withSpan("PeerRelayStore.repair"))

  const reconcile: Service["reconcile"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(MaintenanceRequest, unsafeInput)
      const effectiveBatch = Math.min(input.batchSize, limits.reconciliationBatchSize)
      return yield* write(Effect.gen(function*() {
        const rows = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: KeyRow,
          execute: () =>
            sql`SELECT m.message_id AS messageId
              FROM effect_local_relay_messages m
              LEFT JOIN effect_local_relay_reservations r ON r.message_id = m.message_id
              WHERE m.message_id > ${input.cursor ?? 0}
                AND r.message_id IS NULL
              ORDER BY m.message_id
              LIMIT ${effectiveBatch + 1}`
        })(undefined)
        if (rows.length > 0) {
          return yield* storageCorrupt(new Error("Missing immutable relay quota reservation"))
        }
        if (input.cursor === undefined || input.cursor === 0) {
          const orphanReservations = yield* SqlSchema.findAll({
            Request: Schema.Void,
            Result: KeyRow,
            execute: () =>
              sql`SELECT r.message_id AS messageId
                FROM effect_local_relay_reservations r
                LEFT JOIN effect_local_relay_messages m ON m.message_id = r.message_id
                WHERE m.message_id IS NULL
                ORDER BY r.message_id
                LIMIT ${effectiveBatch + 1}`
          })(undefined)
          if (orphanReservations.length > 0) {
            return yield* storageCorrupt(new Error("Orphan relay quota reservation"))
          }
        }
        yield* sql`DELETE FROM effect_local_relay_usage`
        yield* sql`INSERT INTO effect_local_relay_usage (
            scope_kind,
            scope_key,
            active_count,
            active_bytes,
            retained_count,
            retained_bytes
          )
          SELECT
            scope_kind,
            scope_key,
            SUM(CASE WHEN active_consumed = 0 THEN active_count_delta ELSE 0 END),
            SUM(CASE WHEN active_consumed = 0 THEN active_bytes_delta ELSE 0 END),
            SUM(CASE WHEN retained_consumed = 0 THEN retained_count_delta ELSE 0 END),
            SUM(CASE WHEN retained_consumed = 0 THEN retained_bytes_delta ELSE 0 END)
          FROM (
            SELECT
              'SenderPeer' AS scope_kind,
              sender_peer_usage_key AS scope_key,
              active_count_delta,
              active_bytes_delta,
              retained_count_delta,
              retained_bytes_delta,
              active_consumed,
              retained_consumed
            FROM effect_local_relay_reservations
            UNION ALL
            SELECT
              'RecipientPeer',
              recipient_peer_usage_key,
              active_count_delta,
              active_bytes_delta,
              retained_count_delta,
              retained_bytes_delta,
              active_consumed,
              retained_consumed
            FROM effect_local_relay_reservations
            UNION ALL
            SELECT
              'RecipientSubject',
              recipient_subject_usage_key,
              active_count_delta,
              active_bytes_delta,
              retained_count_delta,
              retained_bytes_delta,
              active_consumed,
              retained_consumed
            FROM effect_local_relay_reservations
            UNION ALL
            SELECT
              'Tenant',
              tenant_usage_key,
              active_count_delta,
              active_bytes_delta,
              retained_count_delta,
              retained_bytes_delta,
              active_consumed,
              retained_consumed
            FROM effect_local_relay_reservations
            UNION ALL
            SELECT
              'Shard',
              shard_usage_key,
              active_count_delta,
              active_bytes_delta,
              retained_count_delta,
              retained_bytes_delta,
              active_consumed,
              retained_consumed
            FROM effect_local_relay_reservations
          )
          GROUP BY scope_kind, scope_key
          HAVING
            SUM(CASE WHEN active_consumed = 0 THEN active_count_delta ELSE 0 END) > 0
            OR SUM(CASE WHEN retained_consumed = 0 THEN retained_count_delta ELSE 0 END) > 0`
        return { processed: 0, hasMore: false }
      }))
    })).pipe(Effect.withSpan("PeerRelayStore.reconcile"))

  const collect: Service["collect"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = yield* validateInput(MaintenanceRequest, unsafeInput)
      const effectiveBatch = Math.min(input.batchSize, limits.terminalCollectionBatchSize)
      return yield* write(Effect.gen(function*() {
        const now = (yield* nowQuery(sql)).now
        const rows = yield* SqlSchema.findAll({
          Request: Schema.Void,
          Result: KeyRow,
          execute: () =>
            sql`SELECT message_id AS messageId
              FROM effect_local_relay_messages
              WHERE message_id > ${input.cursor ?? 0}
                AND state IN ('Acknowledged', 'DeadLettered', 'Expired')
                AND deduplicate_until <= ${now}
              ORDER BY message_id
              LIMIT ${effectiveBatch + 1}`
        })(undefined)
        for (const row of rows.slice(0, effectiveBatch)) {
          yield* releaseRetainedUsage(row.messageId)
          yield* sql`DELETE FROM effect_local_relay_messages
            WHERE message_id = ${row.messageId}
              AND state IN ('Acknowledged', 'DeadLettered', 'Expired')
              AND deduplicate_until <= ${now}`
        }
        yield* sql`DELETE FROM effect_local_relay_usage
          WHERE active_count = 0
            AND active_bytes = 0
            AND retained_count = 0
            AND retained_bytes = 0`
        yield* sql`DELETE FROM effect_local_relay_channels
          WHERE claimed_message_id IS NULL
            AND NOT EXISTS (
              SELECT 1
              FROM effect_local_relay_messages m
              WHERE m.channel_id = effect_local_relay_channels.channel_id
            )
          LIMIT ${limits.orphanChannelCleanupBatchSize}`
        return maintenanceResult(rows.map((row) => row.messageId), effectiveBatch)
      }))
    })).pipe(Effect.withSpan("PeerRelayStore.collect"))

  const usage: Service["usage"] = (unsafeInput) =>
    mapStorageErrors(Effect.gen(function*() {
      const input = unsafeInput === undefined
        ? UsageRequest.make({ scopeKind: "Shard", scopeKey: encodeKey("local") })
        : yield* validateInput(UsageRequest, unsafeInput)
      const row = yield* SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: UsageRow,
        execute: () =>
          sql`SELECT
              active_count AS activeCount,
              active_bytes AS activeBytes,
              retained_count AS retainedCount,
              retained_bytes AS retainedBytes
            FROM effect_local_relay_usage
            WHERE scope_kind = ${input.scopeKind}
              AND scope_key = ${input.scopeKey}`
      })(undefined)
      return Option.getOrElse(row, (): Usage => ({
        activeCount: 0,
        activeBytes: 0,
        retainedCount: 0,
        retainedBytes: 0
      }))
    })).pipe(Effect.withSpan("PeerRelayStore.usage"))

  return PeerRelayStore.of({
    admit,
    claim,
    loadClaimedPayload,
    acknowledge,
    reject,
    release,
    recover,
    expire,
    repair,
    reconcile,
    collect,
    usage
  })
})

export const make = makeService

export const layerSqlite: Layer.Layer<
  PeerRelayStore,
  | Migrator.MigrationError
  | SqlError.SqlError
  | Schema.SchemaError
  | ReplicaError.ReplicaError,
  SqlClient.SqlClient | Crypto.Crypto | PeerRelayLimits.PeerRelayLimits
> = Layer.effect(PeerRelayStore, makeService)
