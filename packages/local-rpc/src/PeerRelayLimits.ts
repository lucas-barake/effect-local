import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as SchemaIssue from "effect/SchemaIssue"
import * as PeerRpcProtocol from "./internal/peerRpcProtocol.js"

/**
 * The relay's deployment-tunable limits.
 *
 * Every value here is read by something. That is a deliberate property rather than an accident:
 * this used to carry the tuning surface of a single-process relay that owned its own socket
 * framing, claim leases, SQL lanes and cross-scope quota counters, and none of that machinery
 * survives the move to cluster entities. Configuration that no longer reaches any code is worse
 * than absent — an operator who sets a quota that is silently never enforced believes their
 * deployment is bounded when it is not — so the orphaned values were removed rather than retained
 * for a future that may never use them.
 *
 * Two families are gone with no replacement here, and both are accepted regressions rather than
 * oversights. Per-scope quotas (per sender peer, recipient peer, recipient subject, tenant, shard)
 * span many inboxes, so no entity is their sole writer and enforcing them would reintroduce exactly
 * the cross-process arbitration this design removes; admission is now bounded **per inbox** by
 * `RelayInbox.Options`, inside the same transaction as the write. Connection and frame accounting
 * belonged to the bespoke length-prefixed framing, which standard Effect RPC over a socket replaces;
 * a single relayed payload is still bounded, by `PeerRpc`'s schema check against
 * `maximumRelayPayloadBytes`, but concurrent connections and in-flight frame bytes are now the
 * deployment's socket server to bound.
 */

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const PositiveNumber = Schema.Number.check(Schema.isFinite(), Schema.isGreaterThan(0))

/** Milliseconds for a finite, positive duration input, or `undefined` if it is neither. */
const finiteMillis = (input: unknown): number | undefined =>
  Option.match(Duration.fromInput(input as Duration.Input), {
    onNone: () => undefined,
    onSome: (duration) => Duration.isFinite(duration) ? Duration.toMillis(duration) : undefined
  })

/**
 * A configured duration.
 *
 * `Duration.Input` rather than a number of milliseconds, so the unit lives in the type instead of
 * in the field name and a deployment can write whichever form reads best: `"7 days"`,
 * `Duration.hours(1)`, or a plain number.
 */
const PositiveDuration = Schema.declare<Duration.Input>(
  (input): input is Duration.Input => {
    const millis = finiteMillis(input)
    return millis !== undefined && millis > 0
  },
  { expected: "a finite positive duration" }
)

/** A duration the wire contract also has to be able to carry, so it is bounded by the protocol. */
const NegotiatedDuration = Schema.declare<Duration.Input>(
  (input): input is Duration.Input => {
    const millis = finiteMillis(input)
    return millis !== undefined && millis > 0 &&
      millis <= PeerRpcProtocol.maximumNegotiatedDurationMillis
  },
  {
    expected: "a finite positive duration no longer than " +
      `${PeerRpcProtocol.maximumNegotiatedDurationMillis}ms`
  }
)

export const Values = Schema.Struct({
  /** How long an undelivered message survives in an inbox before it is expired. */
  messageTtl: NegotiatedDuration,
  /** The longest replay window a client may negotiate at `Open`. */
  maximumSenderRetryHorizon: NegotiatedDuration,
  /** The slack a receipt must outlive its message and that message's replay window by. */
  minimumTerminalRetention: NegotiatedDuration,
  /** The longest receipt retention a client may negotiate at `Open`. */
  maximumReceiptRetention: NegotiatedDuration,

  /** Credentials verified concurrently. Beyond this a caller is told to come back. */
  maxInFlightAuthentication: PositiveInt,
  /** Sustained authentication attempts allowed per client. */
  authenticationRatePerSecond: PositiveNumber,
  /** Authentication attempts a client may spend at once before the sustained rate applies. */
  authenticationBurst: PositiveInt,
  /** Rate-limiter state retained for idle clients, so the table cannot grow without bound. */
  maxRetainedRateLimitedConnections: PositiveInt,
  /** How long a client's rate-limiter state is kept after its last request. */
  rateLimitIdleRetention: PositiveDuration,

  /**
   * Live relay sessions one subject may hold.
   *
   * Sessions are the front door's only unbounded resource and an authenticated client can open them
   * in a loop, so this is the cap that makes a refusal possible.
   */
  maxSessionsPerSubject: PositiveInt
})
export type Values = typeof Values.Type

export const defaults: Values = Values.make({
  messageTtl: Duration.days(7),
  maximumSenderRetryHorizon: Duration.days(7),
  minimumTerminalRetention: Duration.days(1),
  maximumReceiptRetention: Duration.days(8),

  maxInFlightAuthentication: 64,
  authenticationRatePerSecond: 16,
  authenticationBurst: 64,
  maxRetainedRateLimitedConnections: 10_000,
  rateLimitIdleRetention: Duration.minutes(10),

  maxSessionsPerSubject: 4
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
      // The windows have to nest. A receipt that lapses while its sender may still replay the
      // message lets that replay land after the deduplication record is gone, and the recipient
      // applies it a second time. The front door refuses a handshake that would breach this, so the
      // two checks have to agree — this is the one that makes the deployment's own numbers legal.
      "maximumReceiptRetention",
      Duration.toMillis(values.maximumReceiptRetention) >=
        Math.max(
            Duration.toMillis(values.messageTtl),
            Duration.toMillis(values.maximumSenderRetryHorizon)
          ) + Duration.toMillis(values.minimumTerminalRetention)
    ],
    [
      // A burst smaller than the concurrency cap would refuse callers the concurrency limit was
      // sized to admit, so the rate limiter would be the binding constraint rather than the pool.
      "authenticationBurst",
      values.authenticationBurst >= values.maxInFlightAuthentication
    ]
  ]
  const firstInvalid = relations.find(([, valid]) => !valid)
  return firstInvalid === undefined ? Effect.succeed(values) : invalid(firstInvalid[0])
}

export const make = (values: Values) =>
  Values.makeEffect(values).pipe(
    Effect.catchTag("SchemaError", (error) =>
      Effect.fail(new InvalidPeerRelayLimits({ field: firstField(error.issue) ?? "values" }))),
    Effect.flatMap(validate)
  )

export const layer = (values: Values) => Layer.effect(PeerRelayLimits, make(values))

export const layerDefaults = layer(defaults)
