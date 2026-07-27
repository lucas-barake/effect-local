import * as Context from "effect/Context"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

export const maximumReceiptRetentionMillis = 90 * 24 * 60 * 60 * 1_000

const ReceiptRetentionMillis = PositiveInt.check(
  Schema.isLessThanOrEqualTo(maximumReceiptRetentionMillis)
)

export const Values = Schema.Struct({
  maxReceiptsPerRemote: PositiveInt,
  maxEncodedBytesPerRemote: PositiveInt,
  maxReceiptsPerReplica: PositiveInt,
  maxEncodedBytesPerReplica: PositiveInt,
  receiptRetentionMillis: ReceiptRetentionMillis,
  pruneBatchSize: PositiveInt,
  pruneRowsPerSecond: PositiveInt,
  maintenanceIntervalMillis: PositiveInt
}).check(
  Schema.makeFilter(
    (values) => values.maxReceiptsPerRemote <= values.maxReceiptsPerReplica,
    { expected: "maxReceiptsPerRemote being at most maxReceiptsPerReplica" }
  ),
  Schema.makeFilter(
    (values) => values.maxEncodedBytesPerRemote <= values.maxEncodedBytesPerReplica,
    { expected: "maxEncodedBytesPerRemote being at most maxEncodedBytesPerReplica" }
  )
)
export type Values = typeof Values.Type

export const defaults: Values = Values.make({
  maxReceiptsPerRemote: 10_000,
  maxEncodedBytesPerRemote: 64 * 1_024 * 1_024,
  maxReceiptsPerReplica: 100_000,
  maxEncodedBytesPerReplica: 512 * 1_024 * 1_024,
  receiptRetentionMillis: 8 * 24 * 60 * 60 * 1_000,
  pruneBatchSize: 1_000,
  pruneRowsPerSecond: 10_000,
  maintenanceIntervalMillis: 1_000
})

export class PeerRelayReceiptLimits extends Context.Service<PeerRelayReceiptLimits, Values>()(
  "@lucas-barake/effect-local-sql/PeerRelayReceiptLimits"
) {}

export const make = (values: Values) => Values.makeEffect(values)

export const layer = (values: Values) => Layer.effect(PeerRelayReceiptLimits, make(values))

export const layerDefaults = layer(defaults)
