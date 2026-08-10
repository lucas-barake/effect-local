import * as Clock from "effect/Clock"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schedule from "effect/Schedule"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import * as Singleton from "effect/unstable/cluster/Singleton"
import * as RelayInboxStore from "./RelayInboxStore.js"

/**
 * Retention for every relay inbox in the deployment.
 *
 * `expire` and `collect` are global sweeps rather than per-inbox operations, and the entity that
 * owns an inbox is passivated as soon as it goes idle, so no entity can host them: the inboxes that
 * most need expiring are exactly the ones with nobody connected to run the sweep. This is therefore
 * a cluster singleton — one runner owns it at a time, and ownership moves with the shard rather than
 * being pinned to a particular process.
 *
 * Without it `messageTtl` and `terminalRetention` are inert: undelivered messages are
 * never expired, terminal rows are never collected, the table grows without bound, and the retained
 * row count climbs until admission is permanently refused.
 */

export interface Options {
  /** How often the sweep runs. */
  readonly interval: Duration.Input
  /**
   * Rows touched per sweep, per operation.
   *
   * Bounded so one sweep cannot hold long write transactions over a large backlog; the loop simply
   * runs again on the next interval.
   */
  readonly batchLimit: number
  /**
   * How long an expired message's identity is retained.
   *
   * Expiry is a terminal transition like settlement, so the identity has to outlive the window in
   * which its sender may still replay it. The store only ever grows this horizon, never shrinks it.
   */
  readonly terminalRetention: Duration.Input
  /**
   * Whether this deployment runs retention at all.
   *
   * Required rather than defaulted. Enabling it silently would have every deployment delete durable
   * rows on a schedule it never chose, and defaulting it off would make TTL quietly inert.
   */
  readonly enabled: boolean
}

const sweep = (
  store: RelayInboxStore.RelayInboxStore["Service"],
  options: Options,
  terminalRetentionMillis: number
) =>
  Effect.gen(function*() {
    const now = yield* Clock.currentTimeMillis
    const expired = yield* store.expire({
      now,
      limit: options.batchLimit,
      terminalRetentionMillis
    })
    const collected = yield* store.collect({ now, limit: options.batchLimit })
    if (expired > 0 || collected > 0) {
      // Expiry destroys a message whose sender was told the relay had taken custody, so it is
      // reported rather than performed silently.
      yield* Effect.logInfo("Relay inbox retention swept").pipe(
        Effect.annotateLogs({ expired, collected })
      )
    }
  }).pipe(
    // A sweep failure must not end the singleton. `Sharding` turns a failed singleton into a
    // defect, and retention would then stop for the whole deployment until a rebalance happened to
    // move ownership — a silent, unbounded outage of the only thing bounding table growth.
    Effect.catchTag("ReplicaError", (error) =>
      Effect.logWarning("Relay inbox retention sweep failed; retrying on the next interval").pipe(
        Effect.annotateLogs({ reason: error.reason._tag })
      )),
    Effect.withSpan("RelayInboxMaintenance.sweep")
  )

/**
 * Registers the retention sweep as a cluster singleton.
 *
 * Requires `Sharding` but never builds one, so the deployment shape stays the consumer's choice.
 */
export const layer = (
  options: Options
): Layer.Layer<never, never, RelayInboxStore.RelayInboxStore | Sharding.Sharding> => {
  if (options.enabled) {
    return Singleton.make(
      "EffectLocalRelayInboxMaintenance",
      Effect.gen(function*() {
        // Resolved once here rather than per sweep, so the loop carries no requirement of its own.
        const store = yield* RelayInboxStore.RelayInboxStore
        // Converted once, alongside the store: the column it feeds is a millisecond timestamp.
        const terminalRetentionMillis = Duration.toMillis(options.terminalRetention)
        yield* sweep(store, options, terminalRetentionMillis).pipe(
          Effect.repeat(Schedule.spaced(options.interval))
        )
      })
    )
  }
  return Layer.empty
}
