import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Latch from "effect/Latch"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Entity from "effect/unstable/cluster/Entity"
import * as Rpc from "effect/unstable/rpc/Rpc"
import * as PeerRpc from "./PeerRpc.js"
import * as PeerRpcError from "./PeerRpcError.js"
import * as RelayInboxStore from "./RelayInboxStore.js"

/**
 * One cluster entity per recipient device. The cluster guarantees a single live owner per entity
 * id across every runner, which is what makes the relay horizontally scalable: endpoint ownership,
 * routing, and cross node wakeups come from sharding rather than from process local registries.
 *
 * Because the entity is the sole writer for its device, custody needs no distributed protocol. A
 * message is `Pending` in the store until it reaches a terminal state, and what is currently being
 * delivered is tracked only in memory. Losing the owning runner loses that memory and nothing
 * else, so the next owner finds the same `Pending` rows and delivers them again.
 */

const InboxError = Schema.Union([
  PeerRpcError.SessionUnavailable,
  PeerRpcError.InvalidRequest,
  PeerRpcError.RequestCapacityExceeded,
  PeerRpcError.ServerUnavailable
])

/**
 * Accepts a message into the recipient's durable inbox.
 *
 * Deliberately volatile rather than `ClusterSchema.Persisted`: durability is the store write this
 * handler performs, so persisting the cluster message as well would hold the payload twice under
 * two independent retry ledgers. The at-least-once guarantee therefore rests on this rpc failing
 * whenever the durable write did not land, which leaves the sender's outbox entry intact to be
 * retried under the same `relayMessageId`.
 */
export class DeliverRpc extends Rpc.make("Deliver", {
  payload: {
    channel: RelayInboxStore.ChannelKey,
    envelope: RelayInboxStore.InboxEnvelope,
    /**
     * The horizon the sending session negotiated at `Open`.
     *
     * Carried per message because the deduplication horizon is derived from it: a message's
     * identity must outlive the window in which its sender may still replay it, and that window is
     * a property of the sender's session rather than of this server's configuration.
     */
    senderRetryHorizonMillis: Schema.Int.pipe(Schema.check(Schema.isGreaterThan(0)))
  },
  error: InboxError
}) {}

/**
 * Opens the recipient's live session and streams its durable inbox.
 *
 * Replaces any previous session for this device, which is how the cluster enforces the "one live
 * session per endpoint" rule that the previous implementation kept in a process local map.
 */
export class SubscribeRpc extends Rpc.make("Subscribe", {
  payload: { sessionId: Identity.SessionId },
  success: PeerRpc.StoredMessage,
  error: InboxError,
  stream: true
}) {}

/**
 * Terminally settles one delivered message. Both acknowledgement and rejection settle: the
 * recipient has made a durable decision either way, so neither should be redelivered.
 */
export class SettleRpc extends Rpc.make("Settle", {
  payload: {
    sessionId: Identity.SessionId,
    relayMessageId: Identity.RelayMessageId,
    claimToken: PeerRpc.ClaimToken,
    messageHash: PeerRpc.StoredMessage.fields.messageHash,
    outcome: RelayInboxStore.TerminalOutcome
  },
  error: InboxError
}) {}

/**
 * Extends the session's liveness deadline.
 *
 * Driven by the front door while it holds a live socket, never by the client, so it stays off the
 * public wire contract. A cluster cannot rely on disconnects announcing themselves: the cross node
 * interrupt is best effort and is never sent at all when the front door's own node fails. The
 * deadline bounds how long a failed front door can hold a device's channels open.
 */
export class HeartbeatRpc extends Rpc.make("Heartbeat", {
  payload: { sessionId: Identity.SessionId },
  error: InboxError
}) {}

/**
 * Ends a delivery attempt without settling it.
 *
 * The front door re-authorizes every message it takes off this entity's stream, and a message it
 * may not hand over has already left the queue by then: the delivering fiber is waiting for a
 * settlement that can never arrive, and it holds its channel while it waits. Releasing ends that
 * attempt, leaves the row `Pending` for a later session, and frees the channel so the rest of the
 * inbox keeps moving — without this, one withheld message stops every channel sharing the
 * session's delivery slots, including channels the recipient is fully entitled to.
 *
 * Off the public wire contract, like `Heartbeat`: it is the front door's own bookkeeping.
 */
export class ReleaseRpc extends Rpc.make("Release", {
  payload: {
    sessionId: Identity.SessionId,
    relayMessageId: Identity.RelayMessageId,
    claimToken: PeerRpc.ClaimToken
  },
  error: InboxError
}) {}

export class EndSessionRpc extends Rpc.make("EndSession", {
  payload: { sessionId: Identity.SessionId },
  error: InboxError
}) {}

export const RelayInbox = Entity.make("EffectLocalRelayInbox", [
  DeliverRpc,
  SubscribeRpc,
  SettleRpc,
  ReleaseRpc,
  HeartbeatRpc,
  EndSessionRpc
])

export interface Options {
  /**
   * How many times a message may be handed to a session before it is dead lettered.
   *
   * Counts deliveries rather than failures. A recipient that reads a message and then disconnects
   * without settling it is not a failure at any layer, yet left uncounted it would redeliver
   * forever and block its channel, so the budget has to be spent by the attempt itself.
   */
  readonly maxDeliveries: number
  /** How long an undelivered message survives before it is expired. */
  readonly messageTtlMillis: number
  /**
   * How long a settled message's identity is retained. Must cover the sender's retry horizon, or a
   * replay could outlive the deduplication record and be applied twice.
   */
  readonly terminalRetentionMillis: number
  /** How long a session survives without a heartbeat. */
  readonly sessionDeadlineMillis: number
  /** How often the entity checks for a lapsed session deadline. */
  readonly sessionSweepMillis: number
  /** Maximum channels delivered concurrently to one session. */
  readonly maxConcurrentChannels: number
  /** Backoff after a store failure, so a failing database cannot spin the dispatcher. */
  readonly storeRetryMillis: number
  /** Per-inbox admission caps, enforced inside the admission transaction. */
  readonly maxPendingMessages: number
  readonly maxPendingBytes: number
  /**
   * Entity mailbox depth. Surfaces to callers as `MailboxFull`.
   *
   * Note the forked `Subscribe` handler occupies one slot for the whole life of a session, so the
   * usable depth for `Deliver` and `Settle` is one less than this.
   */
  readonly mailboxCapacity: number
  /** How long an idle entity survives before the cluster passivates it. */
  readonly maxIdleTimeMillis: number
}

/**
 * How a delivery attempt ended.
 *
 * `Released` is not a terminal state: the front door took the message off the queue and then
 * decided the recipient may not have it, so the row stays `Pending` for a later session. It has to
 * be distinguishable from a settlement, because the attempt still has to end — otherwise the
 * delivering fiber waits forever for a settlement that can never come, holding its channel.
 */
type AttemptOutcome = RelayInboxStore.TerminalOutcome | "Released"

interface Settlement {
  readonly claimToken: PeerRpc.ClaimToken
  readonly messageHash: string
  /** The channel this attempt belongs to, so releasing it can free exactly that channel. */
  readonly channelId: string
  /** Completed by the recipient's `Settle` or `Release` call. */
  readonly requested: Deferred.Deferred<AttemptOutcome>
  /**
   * Completed by the delivering fiber once the terminal transition is durable.
   *
   * `Settle` waits on this before replying. Replying earlier would tell the recipient its
   * acknowledgement is safe while the row is still `Pending`, and the recipient prunes its own
   * receipt on that reply.
   */
  readonly durable: Deferred.Deferred<void, PeerRpcError.ServerUnavailable>
}

interface Session {
  readonly sessionId: Identity.SessionId
  readonly outbound: Queue.Queue<PeerRpc.StoredMessage, Cause.Done>
  readonly settlements: Map<Identity.RelayMessageId, Settlement>
  /** Channels with a delivery in flight. One message per channel at a time preserves order. */
  readonly busyChannels: Set<string>
  /**
   * Channels whose head this session may not receive.
   *
   * Held for the life of the session only. Without it the dispatcher would immediately re-offer the
   * head the front door just withheld and spin on it; skipping the whole channel rather than the
   * message keeps per-channel ordering intact, since the head cannot be passed over. A reconnect
   * starts with an empty set, so a grant that comes back is picked up.
   */
  readonly withheldChannels: Set<string>
  /** Opened whenever there may be new work: a fresh admission, or a channel falling idle. */
  readonly wake: Latch.Latch
  readonly scope: Scope.Closeable
  readonly deadlineAt: Ref.Ref<number>
}

const makeClaimToken = Effect.sync(() => `clm_${crypto.randomUUID()}` as PeerRpc.ClaimToken)

/**
 * The in-flight key for a channel.
 *
 * Must name every component of `ChannelKey`. The store partitions heads by the full key including
 * the connection epoch, so two epochs of one sender are two heads at once; a coarser key here marks
 * both busy from one delivery and lets a stale epoch's unsettled head hold the live epoch's stream
 * for the life of the session.
 */
const channelId = (channel: RelayInboxStore.ChannelKey): string =>
  JSON.stringify([
    channel.tenantId,
    channel.senderSubjectId,
    channel.senderPeerId,
    channel.senderReplicaIncarnation,
    channel.senderConnectionEpoch
  ])

export const layer = (options: Options) =>
  RelayInbox.toLayer(
    Effect.gen(function*() {
      const address = yield* Entity.CurrentAddress
      const inboxKey = address.entityId
      const store = yield* RelayInboxStore.RelayInboxStore

      const sessionRef = yield* Ref.make(Option.none<Session>())
      // Serializes session installation so two concurrent reconnects cannot both install
      // themselves, which would leave two dispatchers draining one inbox.
      const sessionLock = yield* Semaphore.make(1)

      /**
       * Closes a session and waits for it to finish closing.
       *
       * Awaiting matters: closing the scope interrupts the dispatcher and every in flight channel
       * delivery. Returning before that completes would let a replacement session start delivering
       * while the outgoing one is still mid delivery, and the same message could then be offered
       * to two sessions at once.
       */
      const closeSession = (session: Session) =>
        Effect.gen(function*() {
          yield* Ref.update(
            sessionRef,
            Option.filter((current) => current.sessionId !== session.sessionId)
          )
          for (const settlement of session.settlements.values()) {
            // Only the delivering fiber awaits this, and closing the scope interrupts it anyway.
            yield* Deferred.interrupt(settlement.requested)
            // The recipient awaits this one through the `Settle` rpc, which declares a closed error
            // union. Interrupting it answers the caller outside that union: an interrupt cannot be
            // caught by the front door's typed handlers, so instead of a retryable failure it takes
            // down the peer's acknowledgement fiber.
            yield* Effect.ignore(
              Deferred.fail(settlement.durable, new PeerRpcError.ServerUnavailable())
            )
          }
          session.settlements.clear()
          yield* Scope.close(session.scope, Exit.void)
          yield* Queue.end(session.outbound)
        })

      const closeCurrentSession = Ref.get(sessionRef).pipe(
        Effect.flatMap(Option.match({ onNone: () => Effect.void, onSome: closeSession }))
      )

      /**
       * Delivers one channel's head message and waits for the recipient to settle it.
       *
       * Interruption — a disconnect, a replaced session, a lapsed deadline — simply abandons the
       * attempt. The row is still `Pending`, so the next session picks it up. Nothing has to be
       * released and no lease has to expire first.
       */
      const deliverHead = (session: Session, head: RelayInboxStore.PendingMessage) =>
        Effect.gen(function*() {
          const claimToken = yield* makeClaimToken
          const requested = yield* Deferred.make<AttemptOutcome>()
          const durable = yield* Deferred.make<void, PeerRpcError.ServerUnavailable>()
          // Registered before the message is offered. The recipient can settle as soon as it reads
          // the message, and it does so on a different fiber, so the settlement must already be
          // addressable by the time the message becomes visible.
          session.settlements.set(head.relayMessageId, {
            claimToken,
            messageHash: head.envelope.messageHash,
            channelId: channelId(head.channel),
            requested,
            durable
          })

          // The outbound queue is a rendezvous, so this suspends until the recipient actually
          // takes the message. Everything before it is bookkeeping that costs the message nothing.
          yield* Queue.offer(session.outbound, {
            _tag: "StoredMessage" as const,
            ...head.envelope,
            claimToken
          })

          // Charged only now, because the message has provably reached the transport. Counting at
          // the start would spend the budget on messages that were prepared for a channel and then
          // abandoned when the recipient disconnected, which is ordinary behaviour on a flaky
          // connection and would dead letter messages that were never transmitted once.
          const deliveredAt = yield* Clock.currentTimeMillis
          const record = yield* store.recordDelivery(inboxKey, head.relayMessageId, {
            maxDeliveries: options.maxDeliveries,
            now: deliveredAt
          })
          if (record._tag === "DeadLettered") {
            yield* Effect.logWarning(
              "Relay inbox message exhausted its delivery budget and was dead lettered"
            ).pipe(
              Effect.annotateLogs({
                inboxKey,
                relayMessageId: head.relayMessageId,
                deliveries: record.deliveries
              })
            )
            return
          }

          const outcome = yield* Deferred.await(requested)
          if (outcome === "Released") {
            // The front door took the message and then found the recipient may not have it. Nothing
            // is written: the row stays `Pending` for a later session. Returning here is what frees
            // the channel, which is the whole point — a released attempt that kept waiting for a
            // settlement would hold its channel, and enough of them would hold every delivery slot
            // and starve channels the recipient is perfectly entitled to.
            yield* Effect.sync(() => session.withheldChannels.add(channelId(head.channel)))
            yield* Effect.logDebug("Relay inbox delivery released without settling").pipe(
              Effect.annotateLogs({ inboxKey, relayMessageId: head.relayMessageId })
            )
            return
          }

          const settledAt = yield* Clock.currentTimeMillis
          const settled = yield* store.settle(inboxKey, head.relayMessageId, {
            outcome,
            messageHash: head.envelope.messageHash,
            now: settledAt,
            terminalRetentionMillis: options.terminalRetentionMillis
          })
          if (settled !== "Settled") {
            yield* Effect.logWarning("Relay inbox settlement did not apply").pipe(
              Effect.annotateLogs({ inboxKey, relayMessageId: head.relayMessageId, settled })
            )
            yield* Deferred.fail(durable, new PeerRpcError.ServerUnavailable())
            return
          }
          yield* Deferred.succeed(durable, void 0)
        }).pipe(
          // Only the store's typed failure is recovered here. Defects and interruption stay
          // untouched: a defect is a bug that must surface, and an interruption simply abandons the
          // attempt, leaving the row `Pending` for the next session to pick up.
          Effect.catchTag("ReplicaError", (error) =>
            Effect.logWarning("Relay inbox delivery failed; message stays pending").pipe(
              Effect.annotateLogs({
                inboxKey,
                relayMessageId: head.relayMessageId,
                reason: error.reason._tag
              }),
              Effect.andThen(Effect.sleep(options.storeRetryMillis))
            )),
          // Fails any settle still waiting on this delivery so the recipient retries rather than
          // believing an acknowledgement landed.
          //
          // Suspended because the settlement is registered by the body above, which has not run
          // when this pipeline is built. Reading the map eagerly here always found nothing, and the
          // failure went to a throwaway deferred while the recipient's `Settle` waited forever —
          // holding the entity's single handler permit against every other request for the device.
          Effect.ensuring(Effect.suspend(() => {
            const settlement = session.settlements.get(head.relayMessageId)
            return settlement === undefined
              ? Effect.void
              : Effect.ignore(Deferred.fail(settlement.durable, new PeerRpcError.ServerUnavailable()))
          })),
          Effect.ensuring(Effect.sync(() => {
            session.settlements.delete(head.relayMessageId)
            session.busyChannels.delete(channelId(head.channel))
          })),
          // Frees this channel and re-polls, which is also how the next message in the channel and
          // any newly admitted message get picked up.
          Effect.ensuring(session.wake.open)
        )

      const dispatch = (session: Session): Effect.Effect<never> =>
        Effect.gen(function*() {
          while (true) {
            // Closed before polling so an admission that arrives mid poll is not lost: it reopens
            // the latch and the following await returns immediately.
            yield* session.wake.close
            // Widened by the channels this session has already been told it may not receive.
            // Heads come back oldest first, so asking only for as many as can be delivered would
            // return nothing but withheld ones once enough of them accumulate, and the channels
            // behind them — which the recipient is entitled to — would never even be looked at.
            const heads = yield* store.pendingHeads(inboxKey, {
              limit: options.maxConcurrentChannels + session.withheldChannels.size
            })
            for (const head of heads) {
              const channel = channelId(head.channel)
              if (session.busyChannels.has(channel)) continue
              // Skipped for this session only. Re-offering a head the front door just withheld
              // would spin, and passing over it to reach the next message in the same channel
              // would break the ordering the channel exists to preserve.
              if (session.withheldChannels.has(channel)) continue
              if (session.busyChannels.size >= options.maxConcurrentChannels) break
              session.busyChannels.add(channel)
              yield* Effect.forkIn(deliverHead(session, head), session.scope)
            }
            yield* session.wake.await
          }
        }).pipe(
          // Recovers only the store's typed failure. A defect here is a bug in the dispatcher and
          // must propagate rather than restart invisibly; interruption ends the session normally.
          Effect.catchTag("ReplicaError", (error) =>
            Effect.logWarning("Relay inbox dispatch failed; retrying").pipe(
              Effect.annotateLogs({ inboxKey, reason: error.reason._tag }),
              Effect.andThen(Effect.sleep(options.storeRetryMillis)),
              Effect.andThen(dispatch(session))
            ))
        ) as Effect.Effect<never>

      // A single long lived sweeper rather than a fiber inside each session scope, so that closing
      // a session is never performed by a fiber the close itself must interrupt.
      yield* Effect.gen(function*() {
        const current = yield* Ref.get(sessionRef)
        if (Option.isNone(current)) return
        const now = yield* Clock.currentTimeMillis
        const deadlineAt = yield* Ref.get(current.value.deadlineAt)
        if (now < deadlineAt) return
        yield* sessionLock.withPermit(
          Effect.gen(function*() {
            const latest = yield* Ref.get(sessionRef)
            if (Option.isNone(latest) || latest.value.sessionId !== current.value.sessionId) return
            yield* Effect.logInfo("Relay inbox session deadline lapsed; releasing session").pipe(
              Effect.annotateLogs({ inboxKey, sessionId: current.value.sessionId })
            )
            yield* closeSession(latest.value)
          })
        )
      }).pipe(
        Effect.repeat(Schedule.spaced(options.sessionSweepMillis)),
        Effect.forkScoped
      )

      // Runs before the framework's own termination handshake, which otherwise waits the full
      // `entityTerminationTimeout` for a forked streaming handler that never returns on its own,
      // rejecting deliveries and settlements for the whole of that window on every rebalance.
      yield* Effect.addFinalizer(() => Effect.orDie(closeCurrentSession))

      const currentSession = (sessionId: Identity.SessionId) =>
        Ref.get(sessionRef).pipe(
          Effect.flatMap(Option.match({
            onNone: () => Effect.fail(new PeerRpcError.SessionUnavailable()),
            onSome: (session) =>
              session.sessionId === sessionId
                ? Effect.succeed(session)
                : Effect.fail(new PeerRpcError.SessionUnavailable())
          }))
        )

      const extendDeadline = (session: Session) =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((now) => Ref.set(session.deadlineAt, now + options.sessionDeadlineMillis))
        )

      return RelayInbox.of({
        Deliver: ({ payload }) =>
          Effect.gen(function*() {
            const now = yield* Clock.currentTimeMillis
            const result = yield* store.admit({
              inboxKey,
              channel: payload.channel,
              envelope: payload.envelope,
              now,
              messageTtlMillis: options.messageTtlMillis,
              senderRetryHorizonMillis: payload.senderRetryHorizonMillis,
              quota: {
                maxPendingMessages: options.maxPendingMessages,
                maxPendingBytes: options.maxPendingBytes
              }
            })
            switch (result._tag) {
              case "Conflict":
                return yield* new PeerRpcError.InvalidRequest()
              case "QuotaExceeded":
                return yield* new PeerRpcError.RequestCapacityExceeded()
              case "Duplicate":
                // A duplicate of a message this inbox already gave up on must not be reported as
                // success: the sender deletes its outbox entry on success, and that entry is the
                // only remaining copy of a message the relay can no longer deliver.
                if (result.state === "DeadLettered" || result.state === "Expired") {
                  return yield* new PeerRpcError.ServerUnavailable()
                }
                break
              case "Admitted":
                break
            }
            // Duplicates still wake the dispatcher: the original may be sitting undelivered.
            const session = yield* Ref.get(sessionRef)
            if (Option.isSome(session)) {
              yield* session.value.wake.open
            }
          }).pipe(
            // Discriminated per reason rather than collapsed. Reporting a permanent fault as a
            // transient one makes the sender's outbox retry it forever.
            Effect.catchReason("ReplicaError", "QuotaExceeded", () => new PeerRpcError.RequestCapacityExceeded()),
            Effect.catchReason("ReplicaError", "StorageUnavailable", () => new PeerRpcError.ServerUnavailable()),
            // Corruption is permanent. Reporting it as transient would have the sender's outbox
            // retry it until the message expires.
            Effect.catchReason("ReplicaError", "StorageCorrupt", (reason) =>
              Effect.logError("Relay inbox storage is corrupt").pipe(
                Effect.annotateLogs({ inboxKey, reason: reason._tag }),
                Effect.andThen(new PeerRpcError.InvalidRequest())
              )),
            Effect.catchTag("ReplicaError", (error) =>
              Effect.logWarning("Relay inbox admission failed").pipe(
                Effect.annotateLogs({ inboxKey, reason: error.reason._tag }),
                Effect.andThen(new PeerRpcError.ServerUnavailable())
              )),
            // `inboxKey` is a digest and the ids are opaque, so both are safe to record. The
            // envelope and its payload never become attributes.
            Effect.withSpan("RelayInbox.Deliver", {
              attributes: {
                inbox_key: inboxKey,
                relay_message_id: payload.envelope.relayMessageId
              }
            })
          ),

        // Forked so it does not hold the entity's sequential handler permit. Without this the
        // session would occupy the only permit for its whole lifetime and the settlement that ends
        // each delivery could never be handled, stalling the inbox with no error anywhere.
        Subscribe: ({ payload }) =>
          Rpc.fork(
            // Spans the installation of the session, not its lifetime: the handler returns the
            // queue as soon as the session is reachable, and the stream outlives it.
            Effect.withSpan(
              sessionLock.withPermit(
                // Installation is uninterruptible up to the point the session becomes reachable
                // through `sessionRef`. An interrupt in between would strand the scope — nothing
                // else holds a reference to close it — and could leave a dispatcher polling for a
                // session no one owns, delivering alongside its own replacement.
                Effect.uninterruptibleMask((restore) =>
                  Effect.gen(function*() {
                    yield* restore(closeCurrentSession)

                    const scope = yield* Scope.make()
                    const install = Effect.gen(function*() {
                      const outbound = yield* Queue.bounded<PeerRpc.StoredMessage, Cause.Done>(0)
                      const now = yield* Clock.currentTimeMillis
                      const deadlineAt = yield* Ref.make(now + options.sessionDeadlineMillis)
                      const session: Session = {
                        sessionId: payload.sessionId,
                        outbound,
                        settlements: new Map(),
                        busyChannels: new Set(),
                        withheldChannels: new Set(),
                        wake: yield* Latch.make(true),
                        scope,
                        deadlineAt
                      }
                      yield* Effect.forkIn(dispatch(session), scope)
                      yield* Ref.set(sessionRef, Option.some(session))
                      return outbound
                    })
                    return yield* Effect.onError(
                      install,
                      () => Effect.orDie(Scope.close(scope, Exit.void))
                    )
                  })
                )
              ),
              "RelayInbox.Subscribe",
              { attributes: { inbox_key: inboxKey } }
            )
          ),

        Settle: ({ payload }) =>
          Effect.gen(function*() {
            const session = yield* currentSession(payload.sessionId)
            const settlement = session.settlements.get(payload.relayMessageId)
            if (settlement === undefined) {
              return yield* new PeerRpcError.SessionUnavailable()
            }
            // The claim token is minted per delivery attempt, so this also rejects a token
            // replayed from an earlier attempt of the same message.
            if (
              settlement.claimToken !== payload.claimToken ||
              settlement.messageHash !== payload.messageHash
            ) {
              return yield* new PeerRpcError.SessionUnavailable()
            }
            yield* extendDeadline(session)
            yield* Deferred.succeed(settlement.requested, payload.outcome)
            // Replies only once the terminal transition is durable. The recipient prunes its own
            // relay receipt on this reply, so a premature success would leave neither side holding
            // the message.
            yield* Deferred.await(settlement.durable)
          }).pipe(
            Effect.withSpan("RelayInbox.Settle", {
              attributes: {
                inbox_key: inboxKey,
                relay_message_id: payload.relayMessageId,
                outcome: payload.outcome
              }
            })
          ),

        Release: ({ payload }) =>
          Effect.gen(function*() {
            const session = yield* currentSession(payload.sessionId)
            const settlement = session.settlements.get(payload.relayMessageId)
            if (settlement === undefined || settlement.claimToken !== payload.claimToken) {
              // Nothing to release: the attempt already ended, or this token belongs to an earlier
              // one. Either way the channel is not held by it, so there is nothing to undo.
              return
            }
            yield* extendDeadline(session)
            // Marked here as well as on the delivering fiber, so the channel is skipped from the
            // moment the release is accepted rather than whenever that fiber next runs.
            yield* Effect.sync(() => session.withheldChannels.add(settlement.channelId))
            yield* Deferred.succeed(settlement.requested, "Released")
          }).pipe(
            Effect.withSpan("RelayInbox.Release", {
              attributes: { inbox_key: inboxKey, relay_message_id: payload.relayMessageId }
            })
          ),

        Heartbeat: ({ payload }) =>
          currentSession(payload.sessionId).pipe(
            Effect.flatMap(extendDeadline),
            Effect.withSpan("RelayInbox.Heartbeat", { attributes: { inbox_key: inboxKey } })
          ),

        EndSession: ({ payload }) =>
          sessionLock.withPermit(
            currentSession(payload.sessionId).pipe(
              Effect.flatMap(closeSession),
              Effect.catchTag("SessionUnavailable", () => Effect.void)
            )
          ).pipe(
            Effect.withSpan("RelayInbox.EndSession", { attributes: { inbox_key: inboxKey } })
          )
      })
    }),
    {
      // Stated rather than inherited. Serialising the entity's handlers is what lets `Deliver` and
      // `Settle` reason about session state without their own locking, and it is why `Subscribe`
      // has to be forked — so the value is load bearing and belongs in the source, not in a
      // framework default that could change underneath these invariants.
      concurrency: 1,
      mailboxCapacity: options.mailboxCapacity,
      maxIdleTime: options.maxIdleTimeMillis
    }
  )
