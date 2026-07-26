import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import type * as SchemaIssue from "effect/SchemaIssue"
import * as PeerRpcProtocol from "./internal/peerRpcProtocol.js"

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const PositiveNumber = Schema.Number.check(Schema.isFinite(), Schema.isGreaterThan(0))
const NegotiatedDuration = PositiveInt.check(
  Schema.isLessThanOrEqualTo(PeerRpcProtocol.maximumNegotiatedDurationMillis)
)
const Percentage = PositiveNumber.check(Schema.isLessThanOrEqualTo(100))

export const Values = Schema.Struct({
  maxActiveMessagesPerSenderPeer: PositiveInt,
  maxActiveBytesPerSenderPeer: PositiveInt,
  maxActiveMessagesPerRecipientPeer: PositiveInt,
  maxActiveBytesPerRecipientPeer: PositiveInt,
  maxActiveMessagesPerRecipientSubject: PositiveInt,
  maxActiveBytesPerRecipientSubject: PositiveInt,
  maxActiveMessagesPerTenant: PositiveInt,
  maxActiveBytesPerTenant: PositiveInt,
  maxActiveMessagesPerShard: PositiveInt,
  maxActiveBytesPerShard: PositiveInt,

  maxRetainedRowsPerSenderPeer: PositiveInt,
  maxRetainedBytesPerSenderPeer: PositiveInt,
  maxRetainedRowsPerRecipientPeer: PositiveInt,
  maxRetainedBytesPerRecipientPeer: PositiveInt,
  maxRetainedRowsPerRecipientSubject: PositiveInt,
  maxRetainedBytesPerRecipientSubject: PositiveInt,
  maxRetainedRowsPerTenant: PositiveInt,
  maxRetainedBytesPerTenant: PositiveInt,
  maxRetainedRowsPerShard: PositiveInt,
  maxRetainedBytesPerShard: PositiveInt,

  messageTtlMillis: NegotiatedDuration,
  maximumSenderRetryHorizonMillis: NegotiatedDuration,
  minimumTerminalRetentionMillis: NegotiatedDuration,
  maximumReceiptRetentionMillis: NegotiatedDuration,
  claimLeaseMillis: PositiveInt,
  maximumRecipientProcessingMillis: PositiveInt,
  acknowledgementTimeoutMillis: PositiveInt,
  claimSafetyMarginMillis: PositiveInt,
  retryBaseDelayMillis: PositiveInt,
  retryMaximumDelayMillis: PositiveInt,
  maximumDeliveryAttempts: PositiveInt,
  sqliteLockRetryBaseDelayMillis: PositiveInt,
  sqliteLockRetryMaximumDelayMillis: PositiveInt,
  sqliteLockRetryMaxAttempts: PositiveInt,

  maxRelayConnections: PositiveInt,
  maximumRawChunkBytes: PositiveInt,
  maximumDeclaredFrameBytes: PositiveInt,
  maximumIncompleteFrameBytes: PositiveInt,
  incompleteFrameTimeoutMillis: PositiveInt,
  maximumSharedPayloadBytes: PositiveInt,
  maximumByteReservationWaiters: PositiveInt,
  maxInFlightAuthentication: PositiveInt,
  authenticationRatePerSecond: PositiveNumber,
  authenticationBurst: PositiveInt,

  maxSessionsPerSubject: PositiveInt,
  maxInFlightOpen: PositiveInt,
  maxInFlightOpenPerSubject: PositiveInt,
  openRatePerSecond: PositiveNumber,
  openBurst: PositiveInt,
  maxInFlightPush: PositiveInt,
  maxInFlightPushPerSubject: PositiveInt,
  admissionRatePerSecond: PositiveNumber,
  admissionBurst: PositiveInt,
  maxRetainedRateLimitedConnections: PositiveInt,
  maxRetainedRateLimitedSubjects: PositiveInt,
  rateLimitIdleRetentionMillis: PositiveInt,

  terminalResponseQueueCapacity: PositiveInt,
  maxInFlightTerminalResponses: PositiveInt,
  maxInFlightTerminalResponsesPerSubject: PositiveInt,
  terminalResponseRatePerSecond: PositiveNumber,
  terminalResponseBurst: PositiveInt,
  maxRetainedTerminalResponseSubjects: PositiveInt,
  terminalResponseSubjectIdleRetentionMillis: PositiveInt,

  newWorkQueueCapacity: PositiveInt,
  retryQueueCapacity: PositiveInt,
  relayWorkerConcurrency: PositiveInt,
  newWorkWeight: PositiveInt,
  retryWorkWeight: PositiveInt,
  maxActiveChannels: PositiveInt,
  compensationIntervalMillis: PositiveInt,
  compensationBatchSize: PositiveInt,

  sqlAdmissionQueueCapacity: PositiveInt,
  sqlTerminalQueueCapacity: PositiveInt,
  sqlDeliveryQueueCapacity: PositiveInt,
  sqlMaintenanceQueueCapacity: PositiveInt,
  maxInFlightSqlTransactions: PositiveInt,
  maxInFlightSqlAdmission: PositiveInt,
  maxInFlightSqlTerminal: PositiveInt,
  maxInFlightSqlDelivery: PositiveInt,
  maxInFlightSqlMaintenance: PositiveInt,

  maintenanceIntervalMillis: PositiveInt,
  claimRecoveryBatchSize: PositiveInt,
  claimRecoveryRowsPerSecond: PositiveNumber,
  expiryBatchSize: PositiveInt,
  expiryRowsPerSecond: PositiveNumber,
  integrityBatchSize: PositiveInt,
  integrityRowsPerSecond: PositiveNumber,
  reconciliationBatchSize: PositiveInt,
  reconciliationRowsPerSecond: PositiveNumber,
  terminalCollectionBatchSize: PositiveInt,
  terminalCollectionRowsPerSecond: PositiveNumber,
  orphanChannelCleanupBatchSize: PositiveInt,
  orphanChannelCleanupRowsPerSecond: PositiveNumber,
  sqliteTransactionCapacityPerSecond: PositiveNumber,
  sqliteCapacityHeadroomPercent: Percentage,

  shutdownReleaseConcurrency: PositiveInt,
  shutdownReleaseTimeoutMillis: PositiveInt
})
export type Values = typeof Values.Type

const kibibyte = 1_024
const mebibyte = 1_024 * kibibyte
const gibibyte = 1_024 * mebibyte
const day = 24 * 60 * 60 * 1_000

export const defaults: Values = Values.make({
  maxActiveMessagesPerSenderPeer: 10_000,
  maxActiveBytesPerSenderPeer: 256 * mebibyte,
  maxActiveMessagesPerRecipientPeer: 10_000,
  maxActiveBytesPerRecipientPeer: 256 * mebibyte,
  maxActiveMessagesPerRecipientSubject: 40_000,
  maxActiveBytesPerRecipientSubject: gibibyte,
  maxActiveMessagesPerTenant: 100_000,
  maxActiveBytesPerTenant: 4 * gibibyte,
  maxActiveMessagesPerShard: 1_000_000,
  maxActiveBytesPerShard: 16 * gibibyte,

  maxRetainedRowsPerSenderPeer: 10_000,
  maxRetainedBytesPerSenderPeer: 64 * mebibyte,
  maxRetainedRowsPerRecipientPeer: 10_000,
  maxRetainedBytesPerRecipientPeer: 64 * mebibyte,
  maxRetainedRowsPerRecipientSubject: 40_000,
  maxRetainedBytesPerRecipientSubject: 256 * mebibyte,
  maxRetainedRowsPerTenant: 100_000,
  maxRetainedBytesPerTenant: gibibyte,
  maxRetainedRowsPerShard: 1_000_000,
  maxRetainedBytesPerShard: 8 * gibibyte,

  messageTtlMillis: 7 * day,
  maximumSenderRetryHorizonMillis: 7 * day,
  minimumTerminalRetentionMillis: day,
  maximumReceiptRetentionMillis: 8 * day,
  claimLeaseMillis: 60_000,
  maximumRecipientProcessingMillis: 40_000,
  acknowledgementTimeoutMillis: 10_000,
  claimSafetyMarginMillis: 5_000,
  retryBaseDelayMillis: 250,
  retryMaximumDelayMillis: 30_000,
  maximumDeliveryAttempts: 16,
  sqliteLockRetryBaseDelayMillis: 10,
  sqliteLockRetryMaximumDelayMillis: 250,
  sqliteLockRetryMaxAttempts: 5,

  maxRelayConnections: 1_024,
  maximumRawChunkBytes: 8 * mebibyte + 4,
  maximumDeclaredFrameBytes: 8 * mebibyte,
  maximumIncompleteFrameBytes: 8 * mebibyte + 4,
  incompleteFrameTimeoutMillis: 10_000,
  maximumSharedPayloadBytes: 64 * mebibyte,
  maximumByteReservationWaiters: 1_024,
  maxInFlightAuthentication: 64,
  authenticationRatePerSecond: 16,
  authenticationBurst: 64,

  maxSessionsPerSubject: 4,
  maxInFlightOpen: 32,
  maxInFlightOpenPerSubject: 2,
  openRatePerSecond: 16,
  openBurst: 32,
  maxInFlightPush: 128,
  maxInFlightPushPerSubject: 8,
  admissionRatePerSecond: 100,
  admissionBurst: 128,
  maxRetainedRateLimitedConnections: 10_000,
  maxRetainedRateLimitedSubjects: 10_000,
  rateLimitIdleRetentionMillis: 10 * 60_000,

  terminalResponseQueueCapacity: 256,
  maxInFlightTerminalResponses: 64,
  maxInFlightTerminalResponsesPerSubject: 4,
  terminalResponseRatePerSecond: 100,
  terminalResponseBurst: 128,
  maxRetainedTerminalResponseSubjects: 10_000,
  terminalResponseSubjectIdleRetentionMillis: 10 * 60_000,

  newWorkQueueCapacity: 1_024,
  retryQueueCapacity: 1_024,
  relayWorkerConcurrency: 8,
  newWorkWeight: 3,
  retryWorkWeight: 1,
  maxActiveChannels: 10_000,
  compensationIntervalMillis: 1_000,
  compensationBatchSize: 256,

  sqlAdmissionQueueCapacity: 256,
  sqlTerminalQueueCapacity: 256,
  sqlDeliveryQueueCapacity: 256,
  sqlMaintenanceQueueCapacity: 256,
  maxInFlightSqlTransactions: 16,
  maxInFlightSqlAdmission: 4,
  maxInFlightSqlTerminal: 4,
  maxInFlightSqlDelivery: 4,
  maxInFlightSqlMaintenance: 4,

  maintenanceIntervalMillis: 1_000,
  claimRecoveryBatchSize: 100,
  claimRecoveryRowsPerSecond: 100,
  expiryBatchSize: 100,
  expiryRowsPerSecond: 100,
  integrityBatchSize: 100,
  integrityRowsPerSecond: 100,
  reconciliationBatchSize: 100,
  reconciliationRowsPerSecond: 100,
  terminalCollectionBatchSize: 100,
  terminalCollectionRowsPerSecond: 100,
  orphanChannelCleanupBatchSize: 100,
  orphanChannelCleanupRowsPerSecond: 100,
  sqliteTransactionCapacityPerSecond: 200,
  sqliteCapacityHeadroomPercent: 20,

  shutdownReleaseConcurrency: 16,
  shutdownReleaseTimeoutMillis: 30_000
})

export class InvalidPeerRelayLimits extends Schema.TaggedErrorClass<InvalidPeerRelayLimits>(
  "@lucas-barake/effect-local-rpc/PeerRelayLimits/InvalidPeerRelayLimits"
)("InvalidPeerRelayLimits", { field: Schema.String }) {}

export class PeerRelayLimits extends Context.Service<PeerRelayLimits, Values>()(
  "@lucas-barake/effect-local-rpc/PeerRelayLimits"
) {}

const invalid = (field: keyof Values) => Effect.fail(new InvalidPeerRelayLimits({ field }))

const firstField = (issue: SchemaIssue.Issue): string | undefined => {
  if (issue._tag === "Pointer") {
    const field = issue.path[0]
    return typeof field === "string" ? field : firstField(issue.issue)
  }
  if (issue._tag === "Composite" || issue._tag === "AnyOf") {
    for (const nested of issue.issues) {
      const field = firstField(nested)
      if (field !== undefined) return field
    }
  }
  return undefined
}

const validate = (values: Values) => {
  const relations: ReadonlyArray<readonly [field: keyof Values, valid: boolean]> = [
    [
      "maxActiveMessagesPerRecipientSubject",
      values.maxActiveMessagesPerRecipientSubject >= values.maxActiveMessagesPerRecipientPeer
    ],
    [
      "maxActiveBytesPerRecipientSubject",
      values.maxActiveBytesPerRecipientSubject >= values.maxActiveBytesPerRecipientPeer
    ],
    [
      "maxActiveMessagesPerTenant",
      values.maxActiveMessagesPerTenant >= values.maxActiveMessagesPerRecipientSubject &&
      values.maxActiveMessagesPerTenant >= values.maxActiveMessagesPerSenderPeer
    ],
    [
      "maxActiveBytesPerTenant",
      values.maxActiveBytesPerTenant >= values.maxActiveBytesPerRecipientSubject &&
      values.maxActiveBytesPerTenant >= values.maxActiveBytesPerSenderPeer
    ],
    ["maxActiveMessagesPerShard", values.maxActiveMessagesPerShard >= values.maxActiveMessagesPerTenant],
    ["maxActiveBytesPerShard", values.maxActiveBytesPerShard >= values.maxActiveBytesPerTenant],
    [
      "maxRetainedRowsPerRecipientSubject",
      values.maxRetainedRowsPerRecipientSubject >= values.maxRetainedRowsPerRecipientPeer
    ],
    [
      "maxRetainedBytesPerRecipientSubject",
      values.maxRetainedBytesPerRecipientSubject >= values.maxRetainedBytesPerRecipientPeer
    ],
    [
      "maxRetainedRowsPerTenant",
      values.maxRetainedRowsPerTenant >= values.maxRetainedRowsPerRecipientSubject &&
      values.maxRetainedRowsPerTenant >= values.maxRetainedRowsPerSenderPeer
    ],
    [
      "maxRetainedBytesPerTenant",
      values.maxRetainedBytesPerTenant >= values.maxRetainedBytesPerRecipientSubject &&
      values.maxRetainedBytesPerTenant >= values.maxRetainedBytesPerSenderPeer
    ],
    ["maxRetainedRowsPerShard", values.maxRetainedRowsPerShard >= values.maxRetainedRowsPerTenant],
    ["maxRetainedBytesPerShard", values.maxRetainedBytesPerShard >= values.maxRetainedBytesPerTenant],
    [
      "maximumReceiptRetentionMillis",
      values.maximumReceiptRetentionMillis >=
        Math.max(values.messageTtlMillis, values.maximumSenderRetryHorizonMillis) +
          values.minimumTerminalRetentionMillis
    ],
    [
      "claimLeaseMillis",
      values.maximumRecipientProcessingMillis +
          values.acknowledgementTimeoutMillis +
          values.claimSafetyMarginMillis <
        values.claimLeaseMillis
    ],
    ["claimLeaseMillis", values.claimLeaseMillis < values.messageTtlMillis],
    ["retryMaximumDelayMillis", values.retryMaximumDelayMillis >= values.retryBaseDelayMillis],
    ["retryMaximumDelayMillis", values.retryMaximumDelayMillis < values.claimLeaseMillis],
    [
      "sqliteLockRetryMaximumDelayMillis",
      values.sqliteLockRetryMaximumDelayMillis >= values.sqliteLockRetryBaseDelayMillis
    ],
    ["maximumRawChunkBytes", values.maximumRawChunkBytes <= values.maximumIncompleteFrameBytes],
    ["maximumDeclaredFrameBytes", values.maximumDeclaredFrameBytes + 4 <= values.maximumIncompleteFrameBytes],
    ["maximumDeclaredFrameBytes", values.maximumDeclaredFrameBytes >= PeerRpcProtocol.maximumRelayPayloadBytes],
    ["maximumSharedPayloadBytes", values.maximumSharedPayloadBytes >= values.maximumIncompleteFrameBytes],
    [
      "maximumSharedPayloadBytes",
      values.relayWorkerConcurrency * PeerRpcProtocol.maximumRelayPayloadBytes <=
        values.maximumSharedPayloadBytes
    ],
    ["maximumByteReservationWaiters", values.maximumByteReservationWaiters >= values.maxRelayConnections],
    ["authenticationBurst", values.authenticationBurst >= values.maxInFlightAuthentication],
    ["maxSessionsPerSubject", values.maxSessionsPerSubject <= values.maxRelayConnections],
    ["maxInFlightOpen", values.maxInFlightOpen <= values.maxRelayConnections],
    ["maxInFlightOpenPerSubject", values.maxInFlightOpenPerSubject <= values.maxInFlightOpen],
    ["openBurst", values.openBurst >= values.maxInFlightOpen],
    ["maxInFlightPushPerSubject", values.maxInFlightPushPerSubject <= values.maxInFlightPush],
    ["admissionBurst", values.admissionBurst >= values.maxInFlightPush],
    ["maxRetainedRateLimitedConnections", values.maxRetainedRateLimitedConnections >= values.maxRelayConnections],
    ["maxRetainedRateLimitedSubjects", values.maxRetainedRateLimitedSubjects >= values.maxInFlightOpen],
    [
      "maxInFlightTerminalResponsesPerSubject",
      values.maxInFlightTerminalResponsesPerSubject <= values.maxInFlightTerminalResponses
    ],
    ["terminalResponseQueueCapacity", values.terminalResponseQueueCapacity >= values.maxInFlightTerminalResponses],
    ["terminalResponseBurst", values.terminalResponseBurst >= values.maxInFlightTerminalResponses],
    ["terminalResponseRatePerSecond", values.terminalResponseRatePerSecond >= values.admissionRatePerSecond],
    [
      "maxRetainedTerminalResponseSubjects",
      values.maxRetainedTerminalResponseSubjects >= values.maxInFlightTerminalResponses
    ],
    ["compensationBatchSize", values.compensationBatchSize <= values.maxActiveChannels],
    ["sqlAdmissionQueueCapacity", values.sqlAdmissionQueueCapacity >= values.maxInFlightSqlAdmission],
    ["sqlTerminalQueueCapacity", values.sqlTerminalQueueCapacity >= values.maxInFlightSqlTerminal],
    ["sqlDeliveryQueueCapacity", values.sqlDeliveryQueueCapacity >= values.maxInFlightSqlDelivery],
    ["sqlMaintenanceQueueCapacity", values.sqlMaintenanceQueueCapacity >= values.maxInFlightSqlMaintenance],
    [
      "maxInFlightSqlTransactions",
      values.maxInFlightSqlAdmission +
          values.maxInFlightSqlTerminal +
          values.maxInFlightSqlDelivery +
          values.maxInFlightSqlMaintenance <=
        values.maxInFlightSqlTransactions
    ],
    [
      "maintenanceIntervalMillis",
      [
        values.claimRecoveryBatchSize,
        values.expiryBatchSize,
        values.integrityBatchSize,
        values.reconciliationBatchSize,
        values.terminalCollectionBatchSize
      ].every((batchSize) => batchSize * 1_000 / values.maintenanceIntervalMillis >= values.admissionRatePerSecond)
    ],
    ["claimRecoveryRowsPerSecond", values.claimRecoveryRowsPerSecond >= values.admissionRatePerSecond],
    [
      "claimRecoveryRowsPerSecond",
      values.claimRecoveryRowsPerSecond >=
        values.claimRecoveryBatchSize * 1_000 / values.maintenanceIntervalMillis
    ],
    ["expiryRowsPerSecond", values.expiryRowsPerSecond >= values.admissionRatePerSecond],
    [
      "expiryRowsPerSecond",
      values.expiryRowsPerSecond >=
        values.expiryBatchSize * 1_000 / values.maintenanceIntervalMillis
    ],
    ["integrityRowsPerSecond", values.integrityRowsPerSecond >= values.admissionRatePerSecond],
    [
      "integrityRowsPerSecond",
      values.integrityRowsPerSecond >=
        values.integrityBatchSize * 1_000 / values.maintenanceIntervalMillis
    ],
    ["reconciliationRowsPerSecond", values.reconciliationRowsPerSecond >= values.admissionRatePerSecond],
    [
      "reconciliationRowsPerSecond",
      values.reconciliationRowsPerSecond >=
        values.reconciliationBatchSize * 1_000 / values.maintenanceIntervalMillis
    ],
    ["terminalCollectionRowsPerSecond", values.terminalCollectionRowsPerSecond >= values.admissionRatePerSecond],
    [
      "terminalCollectionRowsPerSecond",
      values.terminalCollectionRowsPerSecond >=
        values.terminalCollectionBatchSize * 1_000 / values.maintenanceIntervalMillis
    ],
    ["orphanChannelCleanupRowsPerSecond", values.orphanChannelCleanupRowsPerSecond >= values.admissionRatePerSecond],
    ["shutdownReleaseConcurrency", values.shutdownReleaseConcurrency <= values.maxInFlightSqlTransactions],
    ["shutdownReleaseTimeoutMillis", values.shutdownReleaseTimeoutMillis < values.claimLeaseMillis]
  ]
  const firstInvalid = relations.find(([, valid]) => !valid)
  if (firstInvalid !== undefined) return invalid(firstInvalid[0])

  const requiredTransactionsPerSecond = values.admissionRatePerSecond +
    values.claimRecoveryRowsPerSecond / values.claimRecoveryBatchSize +
    values.expiryRowsPerSecond / values.expiryBatchSize +
    values.integrityRowsPerSecond / values.integrityBatchSize +
    values.reconciliationRowsPerSecond / values.reconciliationBatchSize +
    values.terminalCollectionRowsPerSecond / values.terminalCollectionBatchSize +
    values.orphanChannelCleanupRowsPerSecond / values.orphanChannelCleanupBatchSize
  return values.sqliteTransactionCapacityPerSecond >=
      requiredTransactionsPerSecond * (1 + values.sqliteCapacityHeadroomPercent / 100)
    ? Effect.succeed(values)
    : invalid("sqliteTransactionCapacityPerSecond")
}

export const make = (values: Values) =>
  Values.makeEffect(values).pipe(
    Effect.catchTag("SchemaError", (error) =>
      Effect.fail(new InvalidPeerRelayLimits({ field: firstField(error.issue) ?? "values" }))),
    Effect.flatMap(validate)
  )

export const layer = (values: Values) => Layer.effect(PeerRelayLimits, make(values))

export const layerDefaults = layer(defaults)
