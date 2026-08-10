import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import { literal } from "./internal/literal.js"

export const Disconnected = Schema.TaggedStruct("Disconnected", {})
export type Disconnected = typeof Disconnected.Type

export const Connecting = Schema.TaggedStruct("Connecting", {})
export type Connecting = typeof Connecting.Type

export const Connected = Schema.TaggedStruct("Connected", {})
export type Connected = typeof Connected.Type

/**
 * Branded so a peer status cannot stand in for a relay status, in either direction. They answer
 * different questions about different links, and with a shared socket every peer goes quiet at once
 * when the relay drops, which is exactly when a UI is most likely to bind the wrong one. The brand
 * is type level only, so the encoded wire form is unchanged.
 */
export const Status = Schema.Union([Disconnected, Connecting, Connected]).pipe(
  Schema.brand("@lucas-barake/effect-local-sql/PeerConnectionStatus")
)
export type Status = typeof Status.Type

/** Branding rejects bare object literals, so every value is built through these. */
export const disconnected: Status = Status.make({ _tag: "Disconnected" })
export const connecting: Status = Status.make({ _tag: "Connecting" })
export const connected: Status = Status.make({ _tag: "Connected" })

export interface Attempt {
  readonly connected: Effect.Effect<void>
  readonly disconnected: Effect.Effect<void>
}

export class PeerConnectionStatus extends Context.Service<PeerConnectionStatus, {
  readonly status: (
    peerId: Identity.PeerId
  ) => Stream.Stream<Status, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/PeerConnectionStatus") {}

export class Reporter extends Context.Service<Reporter, {
  /**
   * The caller owned Scope defines exactly one PeerSession connection epoch and retires it on close.
   */
  readonly connecting: (
    peerId: Identity.PeerId
  ) => Effect.Effect<Attempt, never, Scope.Scope>
}>()("@lucas-barake/effect-local-sql/PeerConnectionStatus/Reporter") {}

type AttemptState = "Connecting" | "Connected"

interface State {
  readonly nextAttemptId: number
  readonly attempts: ReadonlyMap<Identity.PeerId, ReadonlyMap<number, AttemptState>>
  readonly statuses: ReadonlyMap<Identity.PeerId, Status>
  readonly closed: boolean
}

const derive = (attempts: ReadonlyMap<number, AttemptState>): Status => {
  for (const state of attempts.values()) {
    if (state === "Connected") return connected
  }
  return (() => {
    if (attempts.size > 0) return connecting
    return disconnected
  })()
}

const sameStatus = (left: Status, right: Status) => left._tag === right._tag

/**
 * A value rather than a constructor, because the reader and the Reporter are two projections of one
 * owner and Layer memoization is by reference. A `layer()` that minted a fresh Layer per call let a
 * caller compose a second owner alongside the one `SqlReplica` already builds, after which the
 * Reporter wrote into one owner while the reader observed the other and every peer read as
 * permanently `Disconnected`.
 */
export const layer: Layer.Layer<PeerConnectionStatus | Reporter> = Layer.effectContext(
  Effect.gen(function*() {
    const state = yield* Ref.make<State>({
      nextAttemptId: 0,
      attempts: new Map(),
      statuses: new Map(),
      closed: false
    })
    const writes = yield* Semaphore.make(1)
    // Capacity 2, not 1. `PubSub.sliding` picks `BoundedPubSubSingle` at capacity 1, which keeps one
    // shared subscriber counter and one value slot, so a subscriber that falls behind can consume
    // the slot twice and strand another subscriber on a stale value until the next transition. With
    // `Connected` being a steady state that can last a whole session, that leaves a second tab
    // reporting the wrong thing indefinitely. Any capacity above 1 selects a ring buffer with per
    // entry subscriber counts. Latest wins is unaffected: the buffer still drops the oldest, the
    // replay window is still 1, and the reader still dedupes.
    const statuses = yield* Effect.acquireRelease(
      PubSub.sliding<ReadonlyMap<Identity.PeerId, Status>>({ capacity: 2, replay: 1 }),
      (pubsub) =>
        writes.withPermit(
          Effect.gen(function*() {
            yield* Ref.update(state, (current) => ({ ...current, closed: true }))
            yield* PubSub.shutdown(pubsub)
          })
        ).pipe(Effect.uninterruptible)
    )
    yield* PubSub.publish(statuses, new Map())

    const updateAttempt = (
      peerId: Identity.PeerId,
      attemptId: number,
      update: (current: AttemptState | undefined) => AttemptState | undefined
    ) =>
      writes.withPermit(
        Effect.gen(function*() {
          const current = yield* Ref.get(state)
          if (current.closed) return
          const currentPeer = current.attempts.get(peerId) ?? new Map()
          const previousAttempt = currentPeer.get(attemptId)
          const nextAttempt = update(previousAttempt)
          if (nextAttempt === previousAttempt) return

          const nextPeer = new Map(currentPeer)
          if (nextAttempt === undefined) nextPeer.delete(attemptId)
          else nextPeer.set(attemptId, nextAttempt)

          const nextAttempts = new Map(current.attempts)
          if (nextPeer.size === 0) nextAttempts.delete(peerId)
          else nextAttempts.set(peerId, nextPeer)

          const nextStatus = derive(nextPeer)
          const previousStatus = current.statuses.get(peerId) ?? disconnected
          const nextStatuses = new Map(current.statuses)
          if (nextStatus._tag === "Disconnected") nextStatuses.delete(peerId)
          else nextStatuses.set(peerId, nextStatus)

          if (!sameStatus(previousStatus, nextStatus)) {
            yield* PubSub.publish(statuses, nextStatuses)
          }
          yield* Ref.set(state, {
            ...current,
            attempts: nextAttempts,
            statuses: nextStatuses
          })
        })
      ).pipe(Effect.uninterruptible)

    const begin = (peerId: Identity.PeerId) =>
      writes.withPermit(
        Effect.gen(function*() {
          const current = yield* Ref.get(state)
          const attemptId = current.nextAttemptId
          if (current.closed) {
            return {
              connected: Effect.void,
              disconnected: Effect.void
            } satisfies Attempt
          }
          const currentPeer = current.attempts.get(peerId) ?? new Map()
          const nextPeer = new Map(currentPeer).set(attemptId, literal("Connecting"))
          const nextAttempts = new Map(current.attempts).set(peerId, nextPeer)
          const nextStatuses = new Map(current.statuses)
          const previousStatus = current.statuses.get(peerId) ?? disconnected
          const nextStatus = derive(nextPeer)
          nextStatuses.set(peerId, nextStatus)
          if (!sameStatus(previousStatus, nextStatus)) {
            yield* PubSub.publish(statuses, nextStatuses)
          }
          yield* Ref.set(state, {
            nextAttemptId: attemptId + 1,
            attempts: nextAttempts,
            statuses: nextStatuses,
            closed: false
          })
          return {
            connected: updateAttempt(
              peerId,
              attemptId,
              (value) =>
                (() => {
                  if (value === "Connecting") return ("Connected")
                  return value
                })()
            ),
            disconnected: updateAttempt(peerId, attemptId, () => undefined)
          } satisfies Attempt
        })
      ).pipe(Effect.uninterruptible)

    const reader = PeerConnectionStatus.of({
      status: (peerId) =>
        Stream.fromPubSub(statuses).pipe(
          Stream.map((current) => current.get(peerId) ?? disconnected),
          Stream.concat(Stream.make(disconnected)),
          Stream.changesWith(sameStatus)
        )
    })
    const reporter = Reporter.of({
      connecting: (peerId) => Effect.acquireRelease(begin(peerId), (attempt) => attempt.disconnected)
    })
    return Context.make(PeerConnectionStatus, reader).pipe(Context.add(Reporter, reporter))
  })
)
