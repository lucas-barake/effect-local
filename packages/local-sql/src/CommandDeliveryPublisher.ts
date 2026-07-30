import type * as CommandDelivery from "@lucas-barake/effect-local/CommandDelivery"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as CommandDeliveryStore from "./CommandDeliveryStore.js"

export interface Options {
  readonly eventCapacity: number
  readonly publishInterval: Duration.Input
}

export const defaultOptions: Options = {
  eventCapacity: 256,
  publishInterval: "1 second"
}

export type DeliveryEvent =
  | {
    readonly _tag: "Delivery"
    readonly sequence: number
    readonly refreshEpoch: number
    readonly commandId: Identity.CommandId | null
    readonly documentId: Identity.DocumentId
  }
  | {
    readonly _tag: "FullRefreshRequired"
    readonly sequence: number
    readonly refreshEpoch: number
  }

export interface DeliverySubscription {
  readonly sequence: number
  readonly refreshEpoch: number
  readonly events: Stream.Stream<DeliveryEvent>
}

export class CommandDeliveryPublisher extends Context.Service<CommandDeliveryPublisher, {
  readonly publishPending: Effect.Effect<number, ReplicaError.ReplicaError>
  readonly refresh: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly subscribe: Effect.Effect<
    DeliverySubscription,
    ReplicaError.ReplicaError,
    Scope.Scope
  >
  readonly changes: (
    commandId: Identity.CommandId
  ) => Stream.Stream<CommandDelivery.CommandDelivery, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/CommandDeliveryPublisher") {}

export const layer = (options: Options): Layer.Layer<
  CommandDeliveryPublisher,
  never,
  CommandDeliveryStore.CommandDeliveryStore
> => {
  if (!Number.isSafeInteger(options.eventCapacity) || options.eventCapacity < 1) {
    throw new TypeError("Command delivery event capacity must be a positive integer")
  }
  const publishIntervalMillis = Duration.toMillis(Duration.fromInputUnsafe(options.publishInterval))
  if (!Number.isFinite(publishIntervalMillis) || publishIntervalMillis <= 0) {
    throw new TypeError("Command delivery publish interval must be finite and positive")
  }

  return Layer.effect(
    CommandDeliveryPublisher,
    Effect.gen(function*() {
      const store = yield* CommandDeliveryStore.CommandDeliveryStore
      const lock = yield* Semaphore.make(1)
      const events = yield* Effect.acquireRelease(
        PubSub.sliding<DeliveryEvent>(options.eventCapacity),
        PubSub.shutdown
      )

      const publishPending = lock.withPermit(Effect.gen(function*() {
        const pending = yield* store.pendingEvents
        if (pending.length === 0) return 0
        const cursor = yield* store.cursor
        for (const event of pending) {
          yield* PubSub.publish(events, {
            _tag: "Delivery",
            sequence: event.sequence,
            refreshEpoch: cursor.refreshEpoch,
            commandId: event.commandId,
            documentId: event.documentId
          })
        }
        yield* store.markEventsPublished(pending.map((event) => event.sequence))
        return pending.length
      }))

      const refresh = lock.withPermit(Effect.gen(function*() {
        const cursor = yield* store.cursor
        yield* PubSub.publish(events, {
          _tag: "FullRefreshRequired",
          sequence: cursor.sequence,
          refreshEpoch: cursor.refreshEpoch
        })
      }))

      // The caller's Scope owns the subscription and closes it when the consumer is done.
      const subscribe = Effect.gen(function*() {
        const subscription = yield* PubSub.subscribe(events)
        const cursor = yield* store.cursor
        return {
          sequence: cursor.sequence,
          refreshEpoch: cursor.refreshEpoch,
          events: Stream.fromSubscription(subscription)
        }
      })

      const changes = (commandId: Identity.CommandId) =>
        Stream.unwrap(Effect.gen(function*() {
          const subscription = yield* PubSub.subscribe(events)
          const [snapshot, cursor] = yield* store.snapshotWithCursor(commandId)
          const updates = Stream.fromSubscription(subscription).pipe(
            Stream.mapAccum(
              () => cursor,
              (observed, event) => {
                const gap = event.sequence > observed.sequence + 1
                const refreshChanged = event.refreshEpoch !== observed.refreshEpoch
                const relevant = event._tag === "FullRefreshRequired" ||
                  event.commandId === null ||
                  event.commandId === commandId
                const next = {
                  sequence: Math.max(observed.sequence, event.sequence),
                  refreshEpoch: Math.max(observed.refreshEpoch, event.refreshEpoch)
                }
                return [
                  next,
                  event.sequence <= observed.sequence && !refreshChanged
                    ? []
                    : gap || refreshChanged || relevant
                    ? [undefined]
                    : []
                ] as const
              }
            ),
            Stream.mapEffect(() => store.lookup(commandId))
          )
          return Stream.make(snapshot).pipe(
            Stream.concat(updates),
            Stream.changes
          )
        }))

      const drainPending = Effect.gen(function*() {
        let published: number
        do {
          published = yield* publishPending
        } while (published === CommandDeliveryStore.pendingEventBatchSize)
      })

      yield* Effect.sleep(publishIntervalMillis).pipe(
        Effect.andThen(
          drainPending.pipe(
            Effect.catchTag(
              "ReplicaError",
              (error) => Effect.logError("Command delivery event publication failed", error)
            )
          )
        ),
        Effect.forever,
        Effect.forkScoped
      )

      return CommandDeliveryPublisher.of({
        publishPending,
        refresh,
        subscribe,
        changes
      })
    })
  )
}
