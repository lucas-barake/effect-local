import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as ClusterSchema from "effect/unstable/cluster/ClusterSchema"
import * as Entity from "effect/unstable/cluster/Entity"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import * as Rpc from "effect/unstable/rpc/Rpc"
import * as PresenceHub from "./PresenceHub.js"

const volatileAnnotations = Context.make(ClusterSchema.Persisted, false).pipe(
  Context.add(ClusterSchema.WithTransaction, false),
  Context.add(ClusterSchema.Uninterruptible, false)
)

export class Submit extends Rpc.make("Submit", {
  payload: {
    request: Protocol.SubmitRequest,
    principal: Schema.Json
  },
  success: Protocol.Receipt,
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export class Pull extends Rpc.make("Pull", {
  payload: {
    request: Protocol.PullRequest,
    principal: Schema.Json
  },
  success: Protocol.PullResult,
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export class Bootstrap extends Rpc.make("Bootstrap", {
  payload: {
    request: Protocol.BootstrapRequest,
    principal: Schema.Json
  },
  success: Protocol.BootstrapPage,
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export class Watch extends Rpc.make("Watch", {
  payload: { request: Protocol.WatchRequest, principal: Schema.Json },
  success: Protocol.Wake,
  error: ReplicaError.ReplicaError,
  stream: true
}).annotateMerge(volatileAnnotations) {}

export class PublishPresence extends Rpc.make("PublishPresence", {
  payload: {
    update: Protocol.PresenceUpdate,
    principal: Schema.Json
  },
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export class WatchPresence extends Rpc.make("WatchPresence", {
  payload: { principal: Schema.Json },
  success: Protocol.PresenceUpdate,
  error: ReplicaError.ReplicaError,
  stream: true
}).annotateMerge(volatileAnnotations) {}

export const SpaceEntity = Entity.make("EffectLocal/Space", [
  Submit,
  Pull,
  Bootstrap,
  Watch,
  PublishPresence,
  WatchPresence
])

export interface ClientService {
  readonly submit: (
    spaceId: Identity.SpaceId,
    request: Protocol.SubmitRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError>
  readonly pull: (
    spaceId: Identity.SpaceId,
    request: Protocol.PullRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Protocol.PullResult, ReplicaError.ReplicaError>
  readonly bootstrap: (
    spaceId: Identity.SpaceId,
    request: Protocol.BootstrapRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<Protocol.BootstrapPage, ReplicaError.ReplicaError>
  readonly watch: (
    spaceId: Identity.SpaceId,
    request: Protocol.WatchRequest,
    principal: typeof Schema.Json.Type
  ) => Stream.Stream<Protocol.Wake, ReplicaError.ReplicaError>
  readonly publishPresence: (
    spaceId: Identity.SpaceId,
    update: Protocol.PresenceUpdate,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly watchPresence: (
    spaceId: Identity.SpaceId,
    principal: typeof Schema.Json.Type
  ) => Stream.Stream<Protocol.PresenceUpdate, ReplicaError.ReplicaError>
}

export class Client extends Context.Service<Client, ClientService>()(
  "@lucas-barake/effect-local-rpc/SpaceEntity/Client"
) {}

const mapClient = (makeClient: Effect.Success<typeof SpaceEntity.client>): ClientService => ({
  submit: (spaceId, request, principal) =>
    makeClient(spaceId).Submit({ request, principal }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  pull: (spaceId, request, principal) =>
    makeClient(spaceId).Pull({ request, principal }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  bootstrap: (spaceId, request, principal) =>
    makeClient(spaceId).Bootstrap({ request, principal }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  watch: (spaceId, request, principal) =>
    makeClient(spaceId).Watch({ request, principal }).pipe(
      Stream.catchTags({
        MailboxFull: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Stream.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  publishPresence: (spaceId, update, principal) =>
    makeClient(spaceId).PublishPresence({ update, principal }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  watchPresence: (spaceId, principal) =>
    makeClient(spaceId).WatchPresence({ principal }).pipe(
      Stream.catchTags({
        MailboxFull: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Stream.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    )
})

export interface HandlerOptions {
  readonly maxIdleTime?: Duration.Input
  readonly mailboxCapacity?: number | "unbounded"
  readonly disableFatalDefects?: boolean
  readonly defectRetryPolicy?: Schedule.Schedule<any>
  readonly spanAttributes?: Record<string, string>
}

export const layerHandlers = (options: HandlerOptions = {}) =>
  SpaceEntity.toLayer(
    Effect.gen(function*() {
      const address = yield* Entity.CurrentAddress
      const store = yield* ServerStore.ServerStore
      const presence = yield* PresenceHub.PresenceHub
      let spaceId: Identity.SpaceId | undefined
      if (Schema.is(Identity.SpaceId)(address.entityId)) spaceId = address.entityId

      return SpaceEntity.of({
        Submit: ({ payload }) => {
          if (spaceId === undefined || payload.request.envelope.spaceId !== spaceId) {
            return Effect.fail(
              new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
            )
          }
          return store.admit(payload.request, payload.principal)
        },
        Pull: ({ payload }) =>
          Rpc.fork(Effect.suspend(() => {
            if (spaceId === undefined || payload.request.spaceId !== spaceId) {
              return Effect.fail(
                new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
              )
            }
            return store.pullAuthorized(payload.request, payload.principal)
          })),
        Bootstrap: ({ payload }) =>
          Effect.suspend(() => {
            if (spaceId === undefined || payload.request.spaceId !== spaceId) {
              return Effect.fail(
                new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
              )
            }
            return store.bootstrapAuthorized(payload.request, payload.principal)
          }),
        Watch: ({ payload }) => {
          if (spaceId === undefined || payload.request.spaceId !== spaceId) {
            return Rpc.fork(
              Stream.fail(new ReplicaError.ProtocolInvalid({ message: "The routed space is invalid" }))
            )
          }
          return Rpc.fork(Stream.unwrap(store.watchAuthorized(payload.request, payload.principal)))
        },
        PublishPresence: ({ payload }) =>
          Rpc.fork(Effect.suspend(() => {
            if (spaceId === undefined || payload.update.spaceId !== spaceId) {
              return Effect.fail(
                new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
              )
            }
            return presence.publish(payload.update, payload.principal)
          })),
        WatchPresence: ({ payload }) => {
          if (spaceId === undefined) {
            return Rpc.fork(
              Stream.fail(new ReplicaError.ProtocolInvalid({ message: "The routed space is invalid" }))
            )
          }
          return Rpc.fork(presence.watch(spaceId, payload.principal))
        }
      })
    }),
    { ...options, concurrency: 1 }
  )

export const layerClient: Layer.Layer<Client, never, Sharding.Sharding> = Layer.effect(
  Client,
  SpaceEntity.client.pipe(Effect.map(mapClient))
)

export const layer = (options: HandlerOptions = {}) => Layer.merge(layerHandlers(options), layerClient)
