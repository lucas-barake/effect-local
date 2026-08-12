import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as PubSub from "effect/PubSub"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"

export const PublishAuthorization = Schema.TaggedStruct("Publish", {
  spaceId: Identity.SpaceId,
  clientId: Identity.ClientId,
  principal: Schema.Json
})

export const WatchAuthorization = Schema.TaggedStruct("Watch", {
  spaceId: Identity.SpaceId,
  principal: Schema.Json
})

export const AuthorizationInput = Schema.Union([PublishAuthorization, WatchAuthorization])
export type AuthorizationInput = typeof AuthorizationInput.Type

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

export const layer = <R = never,>(options: {
  readonly capacity?: number
  readonly maximumWatchersPerSpace: number
  readonly authorize: (
    input: AuthorizationInput
  ) => Effect.Effect<void, typeof Schema.Json.Type, R>
}): Layer.Layer<PresenceHub, ReplicaError.InvalidConfiguration, R> =>
  Layer.effect(
    PresenceHub,
    Effect.gen(function*() {
      const context = yield* Effect.context<R>()
      const capacity = options.capacity ?? 1_024
      if (!Number.isSafeInteger(capacity) || capacity <= 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "capacity",
          message: "capacity must be a positive safe integer"
        })
      }
      if (!Number.isSafeInteger(options.maximumWatchersPerSpace) || options.maximumWatchersPerSpace <= 0) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "maximumWatchersPerSpace",
          message: "maximumWatchersPerSpace must be a positive safe integer"
        })
      }
      const watcherCount = Metric.gauge("effect_local_server_presence_watcher_count")
      const updates = yield* RcMap.make({
        lookup: () =>
          Effect.acquireRelease(
            Effect.gen(function*() {
              const channel = yield* PubSub.sliding<Protocol.PresenceUpdate>(capacity)
              const watcherPermits = yield* Semaphore.make(options.maximumWatchersPerSpace)
              return { channel, watcherPermits } as const
            }),
            ({ channel }) => PubSub.shutdown(channel)
          )
      })
      return PresenceHub.of({
        publish: (update, principal) =>
          Effect.suspend(() => {
            const spaceId = update.spaceId
            const clientId = update.clientId
            return Effect.gen(function*() {
              yield* options.authorize(PublishAuthorization.make({
                spaceId,
                clientId,
                principal
              })).pipe(
                Effect.provide(context),
                Effect.mapError((reason) => new ReplicaError.AuthorizationDenied({ reason }))
              )
              if ((yield* Protocol.encodedBytesEffect(update)) > Protocol.maximumPresenceBytes) {
                return yield* new ReplicaError.CapacityExceeded({
                  resource: "presence bytes",
                  limit: Protocol.maximumPresenceBytes
                })
              }
              if (yield* RcMap.has(updates, spaceId)) {
                yield* RcMap.get(updates, spaceId).pipe(
                  Effect.flatMap(({ channel }) => PubSub.publish(channel, update)),
                  Effect.scoped
                )
              }
              return yield* Effect.void
            }).pipe(
              Effect.asVoid,
              Effect.withSpan("PresenceHub.publish", {
                attributes: {
                  "presence.space_id": spaceId,
                  "presence.client_id": clientId
                }
              })
            )
          }),
        watch: (spaceId, principal) =>
          Stream.unwrap(
            Effect.gen(function*() {
              const { channel, watcherPermits } = yield* RcMap.get(updates, spaceId)
              yield* options.authorize(WatchAuthorization.make({ spaceId, principal })).pipe(
                Effect.provide(context),
                Effect.mapError((reason) => new ReplicaError.AuthorizationDenied({ reason }))
              )
              yield* Effect.acquireRelease(
                Effect.gen(function*() {
                  if (!(yield* Semaphore.takeIfAvailable(watcherPermits, 1))) {
                    yield* new ReplicaError.CapacityExceeded({
                      resource: "presence watchers",
                      limit: options.maximumWatchersPerSpace
                    })
                  }
                  yield* Metric.modify(watcherCount, 1)
                }),
                () =>
                  Effect.all([
                    Semaphore.release(watcherPermits, 1),
                    Metric.modify(watcherCount, -1)
                  ], { discard: true })
              )
              const subscription = yield* PubSub.subscribe(channel)
              return Stream.fromSubscription(subscription)
            })
          ).pipe(
            Stream.withSpan("PresenceHub.watch", {
              attributes: { "presence.space_id": spaceId }
            })
          )
      })
    })
  )

export const layerTrusted = (options: {
  readonly capacity?: number
  readonly maximumWatchersPerSpace: number
}): Layer.Layer<PresenceHub, ReplicaError.InvalidConfiguration> =>
  layer({
    ...options,
    authorize: () => Effect.void
  })
