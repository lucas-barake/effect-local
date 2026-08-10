import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as LogLevel from "effect/LogLevel"
import * as PubSub from "effect/PubSub"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"

type Lane = "interactive" | "background"

type Request =
  | { readonly _tag: "Acquire"; readonly granted: Deferred.Deferred<void>; readonly lane: Lane }
  | { readonly _tag: "Interrupt"; readonly granted: Deferred.Deferred<void>; readonly lane: Lane }
  | { readonly _tag: "Release"; readonly lane: Lane }

interface LaneState {
  readonly queued: Set<Deferred.Deferred<void>>
  readonly running: Set<Deferred.Deferred<void>>
}

interface AdmissionState {
  closed: boolean
  readonly interactive: LaneState
  readonly background: LaneState
}

export interface Reservations {
  readonly interactive: number
  readonly background: number
}

export class ReplicaOperationScheduler extends Context.Service<ReplicaOperationScheduler, {
  /** The caller's scope owns the admission and releases it when the operation finishes. */
  readonly interactive: Effect.Effect<void, ReplicaError.ReplicaError, Scope.Scope>
  /** The caller's scope owns the admission and releases it when the operation finishes. */
  readonly background: Effect.Effect<void, ReplicaError.ReplicaError, Scope.Scope>
  /** Current active and queued reservations, exposed for operational diagnostics. */
  readonly reservations: Effect.Effect<Reservations>
  /** Emits current reservation counts and later latest-state snapshots. Intermediate states may coalesce. */
  readonly reservationChanges: Stream.Stream<Reservations>
}>()("@lucas-barake/effect-local-sql/ReplicaOperationScheduler") {}

// The Layer scope owns the request queue, arbiter fiber, and reservation feed. Shutdown first closes
// admission and interrupts retained reservations, then interrupts the arbiter before closing the feed
// and request queue.
export const layer: Layer.Layer<ReplicaOperationScheduler, never, ReplicaLimits.ReplicaLimits> = Layer.effect(
  ReplicaOperationScheduler,
  Effect.gen(function*() {
    const limits = yield* ReplicaLimits.ReplicaLimits
    const state = yield* Ref.make<AdmissionState>({
      closed: false,
      interactive: { queued: new Set(), running: new Set() },
      background: { queued: new Set(), running: new Set() }
    })
    const requests = yield* Effect.acquireRelease(Queue.unbounded<Request>(), Queue.shutdown)
    const reservationEvents = yield* Effect.acquireRelease(PubSub.sliding<Reservations>(1), PubSub.shutdown)
    const interactive = new Set<Deferred.Deferred<void>>()
    const background = new Set<Deferred.Deferred<void>>()
    let interactiveCursor = interactive.values()
    let backgroundCursor = background.values()
    let activeBackground = false
    let activeInteractive = 0
    let backgroundTurn = false
    const maxActiveInteractive = limits.maxQueuedRpc + 1

    const snapshot = (current: AdmissionState): Reservations => ({
      interactive: current.interactive.queued.size + current.interactive.running.size,
      background: current.background.queued.size + current.background.running.size
    })
    const publishReservations = Ref.get(state).pipe(
      Effect.flatMap((current) => PubSub.publish(reservationEvents, snapshot(current))),
      Effect.asVoid
    )
    const cancelReservation = (lane: Lane, granted: Deferred.Deferred<void>) =>
      Ref.update(state, (current) => {
        current[lane].queued.delete(granted)
        current[lane].running.delete(granted)
        return current
      }).pipe(
        Effect.andThen(publishReservations)
      )
    // The queued entry moves into the running set, so a reservation is counted exactly once and an
    // acquirer that cancelled first is no longer there to grant.
    const beginGrant = (lane: Lane, granted: Deferred.Deferred<void>) =>
      Ref.modify(state, (current) => {
        if (!current[lane].queued.delete(granted)) return [false, current]
        current[lane].running.add(granted)
        return [true, current]
      }).pipe(Effect.tap(() => publishReservations))
    const endGrant = (lane: Lane, granted: Deferred.Deferred<void>) =>
      Ref.update(state, (current) => {
        current[lane].running.delete(granted)
        return current
      }).pipe(Effect.andThen(publishReservations))

    yield* Effect.gen(function*() {
      while (true) {
        const request = yield* Queue.take(requests)
        switch (request._tag) {
          case "Acquire":
            ;(request.lane === "interactive" ? interactive : background).add(request.granted)
            if (request.lane === "background" && activeInteractive > 0) backgroundTurn = true
            break
          case "Interrupt":
            ;(request.lane === "interactive" ? interactive : background).delete(request.granted)
            if (request.lane === "background" && background.size === 0) backgroundTurn = false
            break
          case "Release":
            if (request.lane === "interactive") activeInteractive--
            else activeBackground = false
            break
        }
        while (!activeBackground) {
          if (activeInteractive > 0) {
            if (activeInteractive >= maxActiveInteractive && !backgroundTurn) break
            if (interactive.size === 0) {
              if (background.size > 0) backgroundTurn = true
              break
            }
            if (backgroundTurn) break
          } else if (interactive.size === 0 && background.size === 0) {
            break
          }
          const lane = activeInteractive === 0 && backgroundTurn && background.size > 0
            ? "background"
            : interactive.size > 0
            ? "interactive"
            : "background"
          const waiters = lane === "interactive" ? interactive : background
          let cursor = lane === "interactive" ? interactiveCursor : backgroundCursor
          let next = cursor.next()
          if (next.done) {
            cursor = waiters.values()
            next = cursor.next()
            if (lane === "interactive") interactiveCursor = cursor
            else backgroundCursor = cursor
          }
          const granted = next.value!
          waiters.delete(granted)
          if (yield* beginGrant(lane, granted)) {
            if (yield* Deferred.succeed(granted, undefined)) {
              if (lane === "interactive") {
                activeInteractive++
              } else {
                activeBackground = true
                backgroundTurn = false
              }
            } else {
              yield* endGrant(lane, granted)
            }
          }
        }
      }
    }).pipe(Effect.forkScoped({ startImmediately: true }))

    const release = (lane: Lane, granted: Deferred.Deferred<void>) =>
      endGrant(lane, granted).pipe(
        Effect.andThen(Queue.offer(requests, { _tag: "Release", lane })),
        Effect.asVoid
      )
    const rejectionLog = (lane: Lane) =>
      Effect.logDebug("Replica operation admission rejected").pipe(
        Effect.annotateLogs({ lane, resource: "queued operation permits", limit: limits.maxQueuedRpc }),
        Effect.when(LogLevel.isEnabled("Debug"))
      )
    const requestAdmission = (lane: Lane) =>
      Deferred.make<void>().pipe(
        Effect.flatMap((granted) =>
          Effect.uninterruptible(
            Effect.gen(function*() {
              const registered = yield* Ref.modify(state, (current) => {
                const queued = current[lane].queued
                if (current.closed || queued.size >= limits.maxQueuedRpc) return [false, current]
                queued.add(granted)
                return [true, current]
              })
              if (!registered) {
                yield* rejectionLog(lane)
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.QuotaExceeded({
                    resource: "queued operation permits",
                    limit: limits.maxQueuedRpc
                  })
                })
              }
              yield* publishReservations
              yield* Queue.offer(requests, { _tag: "Acquire", granted, lane })
              yield* Effect.interruptible(Deferred.await(granted))
              return granted
            }).pipe(
              Effect.onInterrupt(() =>
                Deferred.interrupt(granted).pipe(
                  Effect.flatMap((interrupted) =>
                    interrupted
                      ? cancelReservation(lane, granted).pipe(
                        Effect.andThen(Queue.offer(requests, { _tag: "Interrupt", granted, lane })),
                        Effect.asVoid
                      )
                      : Deferred.await(granted).pipe(Effect.andThen(release(lane, granted)), Effect.ignore)
                  )
                )
              )
            )
          )
        )
      )

    yield* Effect.addFinalizer(() =>
      Ref.modify(state, (current) => {
        const waiters: Array<Deferred.Deferred<void>> = []
        current.closed = true
        for (const lane of [current.interactive, current.background]) {
          for (const granted of lane.queued) waiters.push(granted)
          for (const granted of lane.running) waiters.push(granted)
          lane.queued.clear()
          lane.running.clear()
        }
        return [waiters, current]
      }).pipe(
        Effect.flatMap((waiters) => Effect.forEach(waiters, Deferred.interrupt, { discard: true })),
        Effect.asVoid
      )
    )

    const acquire = (lane: Lane) =>
      Effect.acquireRelease(requestAdmission(lane), (granted) => release(lane, granted)).pipe(Effect.asVoid)
    return ReplicaOperationScheduler.of({
      interactive: acquire("interactive"),
      background: acquire("background"),
      reservations: Ref.get(state).pipe(
        Effect.map(snapshot)
      ),
      reservationChanges: Stream.unwrap(Effect.gen(function*() {
        const subscription = yield* PubSub.subscribe(reservationEvents)
        const current = yield* Ref.get(state)
        return Stream.make(snapshot(current)).pipe(
          Stream.concat(Stream.fromSubscription(subscription)),
          Stream.changes
        )
      }))
    })
  })
)
