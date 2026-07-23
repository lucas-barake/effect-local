import * as PeerSession from "@lucas-barake/effect-local-sql/PeerSession"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const PositiveNumber = Schema.Number.check(Schema.isFinite(), Schema.isGreaterThan(0))

export const Values = Schema.Struct({
  maxSessionsPerSubject: PositiveInt,
  inboundItemCapacity: PositiveInt,
  outboundItemCapacity: PositiveInt,
  maxInboundBufferedBytesPerSession: PositiveInt,
  maxOutboundBufferedBytesPerSession: PositiveInt,
  maxBufferedBytes: PositiveInt,
  maxInFlightAuthentication: PositiveInt,
  authenticationRatePerSecond: PositiveNumber,
  authenticationBurst: PositiveInt,
  maxInFlightOpen: PositiveInt,
  maxInFlightOpenPerSubject: PositiveInt,
  maxInFlightPush: PositiveInt,
  maxInFlightPushPerSubject: PositiveInt,
  openRatePerSecond: PositiveNumber,
  openBurst: PositiveInt,
  pushRatePerSecond: PositiveNumber,
  pushBurst: PositiveInt,
  maxRetainedRateLimitedConnections: PositiveInt,
  maxRetainedRateLimitedSubjects: PositiveInt,
  rateLimitIdleRetention: PositiveInt,
  maximumReauthorizationInterval: PositiveInt,
  commitFlushConcurrency: PositiveInt,
  shutdownCleanupConcurrency: PositiveInt
})
export type Values = typeof Values.Type

export const defaults: Values = Values.make({
  maxSessionsPerSubject: 4,
  inboundItemCapacity: 1,
  outboundItemCapacity: 1,
  maxInboundBufferedBytesPerSession: 4 * 1_024 * 1_024,
  maxOutboundBufferedBytesPerSession: 4 * 1_024 * 1_024,
  maxBufferedBytes: 64 * 1_024 * 1_024,
  maxInFlightAuthentication: 64,
  authenticationRatePerSecond: 16,
  authenticationBurst: 32,
  maxInFlightOpen: 16,
  maxInFlightOpenPerSubject: 2,
  maxInFlightPush: 128,
  maxInFlightPushPerSubject: 8,
  openRatePerSecond: 2,
  openBurst: 4,
  pushRatePerSecond: 64,
  pushBurst: 128,
  maxRetainedRateLimitedConnections: 10_000,
  maxRetainedRateLimitedSubjects: 10_000,
  rateLimitIdleRetention: 10 * 60_000,
  maximumReauthorizationInterval: 5 * 60_000,
  commitFlushConcurrency: 8,
  shutdownCleanupConcurrency: 16
})

export class InvalidPeerRpcLimits extends Schema.TaggedErrorClass<InvalidPeerRpcLimits>(
  "@lucas-barake/effect-local-rpc/PeerRpcLimits/InvalidPeerRpcLimits"
)("InvalidPeerRpcLimits", { field: Schema.String }) {}

export class PeerRpcLimits extends Context.Service<PeerRpcLimits, Values>()(
  "@lucas-barake/effect-local-rpc/PeerRpcLimits"
) {}

const validate = (values: Values, replicaLimits: ReplicaLimits.Values) => {
  const envelope = PeerSession.maximumSyncEnvelopeBytes(replicaLimits.maxSyncMessageBytes)
  const checks: ReadonlyArray<readonly [field: keyof Values, valid: boolean]> = [
    ["maxInboundBufferedBytesPerSession", values.maxInboundBufferedBytesPerSession >= envelope],
    ["maxOutboundBufferedBytesPerSession", values.maxOutboundBufferedBytesPerSession >= envelope],
    [
      "inboundItemCapacity",
      values.inboundItemCapacity * envelope <= values.maxInboundBufferedBytesPerSession
    ],
    [
      "outboundItemCapacity",
      values.outboundItemCapacity * envelope <= values.maxOutboundBufferedBytesPerSession
    ],
    [
      "maxBufferedBytes",
      values.maxBufferedBytes >= envelope
    ]
  ]
  const invalid = checks.find(([, valid]) => !valid)
  return invalid === undefined
    ? Effect.succeed(values)
    : Effect.fail(new InvalidPeerRpcLimits({ field: invalid[0] }))
}

export const make = (values: Values) =>
  Values.makeEffect(values).pipe(
    Effect.flatMap((validated) =>
      ReplicaLimits.ReplicaLimits.use((replicaLimits) => validate(validated, replicaLimits))
    )
  )

export const layer = (values: Values) => Layer.effect(PeerRpcLimits, make(values))

export const layerDefaults = layer(defaults)
