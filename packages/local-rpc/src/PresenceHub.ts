import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import type * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"

export interface Service {
  readonly publish: (
    update: Protocol.PresenceUpdate,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly watch: (
    spaceId: Identity.SpaceId,
    principal: typeof Schema.Json.Type
  ) => Stream.Stream<Protocol.PresenceUpdate, ReplicaError.ReplicaError>
}

export class PresenceHub extends Context.Service<PresenceHub, Service>()(
  "@lucas-barake/effect-local-rpc/PresenceHub"
) {}

export const layer = <R = never,>(options?: {
  readonly capacity?: number
  readonly authorize?: (input: {
    readonly spaceId: Identity.SpaceId
    readonly principal: typeof Schema.Json.Type
  }) => Effect.Effect<void, typeof Schema.Json.Type, R>
}): Layer.Layer<PresenceHub, never, R> =>
  Layer.effect(
    PresenceHub,
    Effect.gen(function*() {
      const context = yield* Effect.context<R>()
      const updates = yield* PubSub.sliding<Protocol.PresenceUpdate>(options?.capacity ?? 1_024)
      yield* Effect.addFinalizer(() => PubSub.shutdown(updates))
      const authorize = (spaceId: Identity.SpaceId, principal: typeof Schema.Json.Type) =>
        options?.authorize === undefined ? Effect.void : options.authorize({ spaceId, principal }).pipe(
          Effect.provide(context),
          Effect.mapError((reason) => new ReplicaError.AuthorizationDenied({ reason }))
        )
      return PresenceHub.of({
        publish: (update, principal) =>
          authorize(update.spaceId, principal).pipe(
            Effect.andThen(
              Protocol.encodedBytes(update) > Protocol.maximumPresenceBytes
                ? Effect.fail(
                  new ReplicaError.CapacityExceeded({
                    resource: "presence bytes",
                    limit: Protocol.maximumPresenceBytes
                  })
                )
                : PubSub.publish(updates, update).pipe(Effect.asVoid)
            )
          ),
        watch: (spaceId, principal) =>
          Stream.unwrap(
            PubSub.subscribe(updates).pipe(
              Effect.flatMap((subscription) => authorize(spaceId, principal).pipe(Effect.as(subscription))),
              Effect.map((subscription) =>
                Stream.fromSubscription(subscription).pipe(
                  Stream.filter((update) => update.spaceId === spaceId)
                )
              )
            )
          )
      })
    })
  )
