import { Message, type MessageId, SendMessage, spaceId } from "@effect-local/example-chat-shared/domain"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import { Atom } from "effect/unstable/reactivity"

/**
 * Client-side failed-message overlay and the settlement daemon that maintains
 * it. Platform-neutral: production mounts the daemon body through the browser
 * graph runtime, and the smoke tests mount the same body through
 * `ReplicaAtom.make` over a Node stack.
 */

/** The failed-message overlay: messages whose optimistic write was rolled back after a terminal failure. */
export type FailedMessages = Atom.Writable<ReadonlyMap<MessageId, Message>, ReadonlyMap<MessageId, Message>>

/** In-memory per tab by design; the durable log only holds accepted history. */
export const makeFailedMessages = (): FailedMessages => Atom.make<ReadonlyMap<MessageId, Message>>(new Map())

/**
 * Terminal receipts drive the failed-message overlay and acknowledge the
 * settlement floor so replay stays bounded. Streams from the acknowledged
 * floor (not "live") so a rejection that settles during a MultiTab leadership
 * handoff - or while the tab was closed - is replayed on the next mount
 * instead of silently dropping the message from the overlay.
 */
export const makeSettlementDaemonBody = (failedMessages: FailedMessages) => (get: Atom.AtomContext) =>
  Effect.fnUntraced(function*() {
    const replica = yield* Replica.Replica
    const space = yield* replica.space(spaceId)
    yield* Stream.runForEach(
      space.settlementsFor(SendMessage, { from: "acknowledged" }),
      Effect.fnUntraced(function*(settled) {
        const settlement = settled.settlement
        // Legacy receipts belong to pre-evolution schema versions and carry
        // no typed payload; the demo only has v1, so they never occur.
        if (!("payload" in settlement.pending)) {
          yield* space.acknowledgeSettlements(settled.sequence)
          return
        }
        const pending = settlement.pending
        // Write the overlay before acknowledging: if the daemon is torn down
        // between the two, replay from the acknowledged floor re-emits the
        // settlement and the re-add is idempotent (keyed by message id). The
        // reverse order would open a drop window where a terminally failed
        // message is neither durable nor shown as failed.
        // get.once: reading with get would register failedMessages as a
        // dependency of this daemon atom, so every overlay write would
        // invalidate and interrupt the daemon itself (replay livelock).
        const next = new Map(get.once(failedMessages))
        if (settlement.receipt._tag === "Rejected") {
          next.set(pending.payload.id, pending.payload)
          get.set(failedMessages, next)
        } else if (settlement.receipt._tag === "Expired") {
          // History was compacted past this mutation while we were offline.
          // If the durable write did not survive the rebase, surface it as
          // failed rather than letting the message silently vanish.
          const durable = yield* space.get(Message, pending.payload.id)
          if (Option.isNone(durable)) {
            // Re-read after the await: the pre-await snapshot would resurrect
            // entries discarded while the durability check was in flight.
            const fresh = new Map(get.once(failedMessages))
            fresh.set(pending.payload.id, pending.payload)
            get.set(failedMessages, fresh)
          }
        } else if (settlement.receipt._tag === "Accepted" && next.has(pending.payload.id)) {
          next.delete(pending.payload.id)
          get.set(failedMessages, next)
        }
        yield* space.acknowledgeSettlements(settled.sequence)
      })
    )
  })()
