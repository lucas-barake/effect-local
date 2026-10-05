import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as PubSub from "effect/PubSub"
import * as Scheduler from "effect/Scheduler"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { capacityExceeded } from "./errors.js"

interface Delivery<A,> {
  readonly sequence: number
  readonly value: A
}

export interface SequencedPubSub<A,> {
  readonly resource: ReplicaError.CapacityResource
  readonly capacity: number
  readonly pubsub: PubSub.PubSub<Delivery<A>>
  published: number
}

export const sliding = <A,>(
  resource: ReplicaError.CapacityResource,
  capacity: number
): Effect.Effect<SequencedPubSub<A>> =>
  Effect.map(PubSub.sliding<Delivery<A>>(capacity), (pubsub) => ({ resource, capacity, pubsub, published: 0 }))

export const publish = <A,>(self: SequencedPubSub<A>, value: A): Effect.Effect<void> =>
  Effect.suspend(() => {
    self.published = self.published + 1
    return PubSub.publish(self.pubsub, { sequence: self.published, value })
  }).pipe(Effect.asVoid)

export const subscribe = <A,>(
  self: SequencedPubSub<A>
): Effect.Effect<Stream.Stream<A, ReplicaError.CapacityExceeded>, never, Scope.Scope> =>
  PubSub.subscribe(self.pubsub).pipe(
    Effect.map((subscription) => {
      const firstExpected = self.published + 1
      return Stream.fromSubscription(subscription).pipe(
        Stream.mapAccumEffect(
          () => firstExpected,
          (expected, delivery) => {
            if (delivery.sequence > expected) return Effect.fail(capacityExceeded(self.resource, self.capacity))
            return Effect.succeed([delivery.sequence + 1, [delivery.value]] as const)
          }
        )
      )
    }),
    Effect.provideService(Scheduler.PreventSchedulerYield, true)
  )

export const shutdown = <A,>(self: SequencedPubSub<A>): Effect.Effect<void> => PubSub.shutdown(self.pubsub)
