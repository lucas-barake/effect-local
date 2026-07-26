import * as Context from "effect/Context"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

export const maximumRetryHorizonMillis = 90 * 24 * 60 * 60 * 1_000

const RetryHorizonMillis = PositiveInt.check(
  Schema.isLessThanOrEqualTo(maximumRetryHorizonMillis)
)

export const Values = Schema.Struct({
  maxMessagesPerRemote: PositiveInt,
  maxEncodedBytesPerRemote: PositiveInt,
  maxMessagesPerReplica: PositiveInt,
  maxEncodedBytesPerReplica: PositiveInt,
  maxRetryHorizonMillis: RetryHorizonMillis,
  pruneBatchSize: PositiveInt,
  pruneRowsPerSecond: PositiveInt,
  maintenanceIntervalMillis: PositiveInt
}).check(
  Schema.makeFilter(
    (values) => values.maxMessagesPerRemote <= values.maxMessagesPerReplica,
    { expected: "maxMessagesPerRemote being at most maxMessagesPerReplica" }
  ),
  Schema.makeFilter(
    (values) => values.maxEncodedBytesPerRemote <= values.maxEncodedBytesPerReplica,
    { expected: "maxEncodedBytesPerRemote being at most maxEncodedBytesPerReplica" }
  )
)
export type Values = typeof Values.Type

export const defaults: Values = Values.make({
  maxMessagesPerRemote: 10_000,
  maxEncodedBytesPerRemote: 64 * 1_024 * 1_024,
  maxMessagesPerReplica: 100_000,
  maxEncodedBytesPerReplica: 512 * 1_024 * 1_024,
  maxRetryHorizonMillis: 7 * 24 * 60 * 60 * 1_000,
  pruneBatchSize: 1_000,
  pruneRowsPerSecond: 10_000,
  maintenanceIntervalMillis: 1_000
})

export class PeerRelayOutboxLimits extends Context.Service<PeerRelayOutboxLimits, Values>()(
  "@lucas-barake/effect-local-sql/PeerRelayOutboxLimits"
) {}

export const make = (values: Values) => Values.makeEffect(values)

export const layer = (values: Values) => Layer.effect(PeerRelayOutboxLimits, make(values))

export const layerDefaults = layer(defaults)
