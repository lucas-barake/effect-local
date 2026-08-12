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
import * as PrincipalAssertion from "./PrincipalAssertion.js"

const volatileAnnotations = Context.make(ClusterSchema.Persisted, false).pipe(
  Context.add(ClusterSchema.WithTransaction, false),
  Context.add(ClusterSchema.Uninterruptible, false)
)

export class Submit extends Rpc.make("Submit", {
  payload: {
    request: Protocol.SubmitRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  },
  success: Protocol.Receipt,
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export class Discard extends Rpc.make("Discard", {
  payload: {
    request: Protocol.DiscardRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  },
  success: Protocol.Receipt,
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export class Pull extends Rpc.make("Pull", {
  payload: {
    request: Protocol.PullRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  },
  success: Protocol.PullResult,
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export class Bootstrap extends Rpc.make("Bootstrap", {
  payload: {
    request: Protocol.BootstrapRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  },
  success: Protocol.BootstrapPage,
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export class Watch extends Rpc.make("Watch", {
  payload: { request: Protocol.WatchRequest, assertion: PrincipalAssertion.PrincipalAssertion },
  success: Protocol.Wake,
  error: ReplicaError.ReplicaError,
  stream: true
}).annotateMerge(volatileAnnotations) {}

export class PublishPresence extends Rpc.make("PublishPresence", {
  payload: {
    update: Protocol.PresenceUpdate,
    assertion: PrincipalAssertion.PrincipalAssertion
  },
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export class WatchPresence extends Rpc.make("WatchPresence", {
  payload: { assertion: PrincipalAssertion.PrincipalAssertion },
  success: Protocol.PresenceUpdate,
  error: ReplicaError.ReplicaError,
  stream: true
}).annotateMerge(volatileAnnotations) {}

export const SpaceEntity = Entity.make("EffectLocal/Space", [
  Submit,
  Discard,
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
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError>
  readonly discard: (
    spaceId: Identity.SpaceId,
    request: Protocol.DiscardRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError>
  readonly pull: (
    spaceId: Identity.SpaceId,
    request: Protocol.PullRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Effect.Effect<Protocol.PullResult, ReplicaError.ReplicaError>
  readonly bootstrap: (
    spaceId: Identity.SpaceId,
    request: Protocol.BootstrapRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Effect.Effect<Protocol.BootstrapPage, ReplicaError.ReplicaError>
  readonly watch: (
    spaceId: Identity.SpaceId,
    request: Protocol.WatchRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Stream.Stream<Protocol.Wake, ReplicaError.ReplicaError>
  readonly publishPresence: (
    spaceId: Identity.SpaceId,
    update: Protocol.PresenceUpdate,
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly watchPresence: (
    spaceId: Identity.SpaceId,
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Stream.Stream<Protocol.PresenceUpdate, ReplicaError.ReplicaError>
}

export class Client extends Context.Service<Client, ClientService>()(
  "@lucas-barake/effect-local-rpc/SpaceEntity/Client"
) {}

const mapClient = (makeClient: Effect.Success<typeof SpaceEntity.client>): ClientService => ({
  submit: (spaceId, request, assertion) =>
    makeClient(spaceId).Submit({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  discard: (spaceId, request, assertion) =>
    makeClient(spaceId).Discard({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  pull: (spaceId, request, assertion) =>
    makeClient(spaceId).Pull({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  bootstrap: (spaceId, request, assertion) =>
    makeClient(spaceId).Bootstrap({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  watch: (spaceId, request, assertion) =>
    makeClient(spaceId).Watch({ request, assertion }).pipe(
      Stream.catchTags({
        MailboxFull: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Stream.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  publishPresence: (spaceId, update, assertion) =>
    makeClient(spaceId).PublishPresence({ update, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  watchPresence: (spaceId, assertion) =>
    makeClient(spaceId).WatchPresence({ assertion }).pipe(
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
      const verifier = yield* PrincipalAssertion.Verifier
      let spaceId: Identity.SpaceId | undefined
      if (Schema.is(Identity.SpaceId)(address.entityId)) spaceId = address.entityId

      return SpaceEntity.of({
        Submit: ({ payload }) => {
          if (spaceId === undefined || payload.request.envelope.spaceId !== spaceId) {
            return Effect.fail(
              new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
            )
          }
          return verifier.verify(payload.assertion).pipe(
            Effect.flatMap((principal) => store.admit(payload.request, principal))
          )
        },
        Discard: ({ payload }) => {
          if (spaceId === undefined || payload.request.envelope.spaceId !== spaceId) {
            return Effect.fail(
              new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
            )
          }
          return verifier.verify(payload.assertion).pipe(
            Effect.flatMap((principal) => store.discard(payload.request, principal))
          )
        },
        Pull: ({ payload }) =>
          Rpc.fork(Effect.suspend(() => {
            if (spaceId === undefined || payload.request.spaceId !== spaceId) {
              return Effect.fail(
                new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
              )
            }
            return verifier.verify(payload.assertion).pipe(
              Effect.flatMap((principal) => store.pullAuthorized(payload.request, principal))
            )
          })),
        Bootstrap: ({ payload }) =>
          Effect.suspend(() => {
            if (spaceId === undefined || payload.request.spaceId !== spaceId) {
              return Effect.fail(
                new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
              )
            }
            return verifier.verify(payload.assertion).pipe(
              Effect.flatMap((principal) => store.bootstrapAuthorized(payload.request, principal))
            )
          }),
        Watch: ({ payload }) => {
          if (spaceId === undefined || payload.request.spaceId !== spaceId) {
            return Rpc.fork(
              Stream.fail(new ReplicaError.ProtocolInvalid({ message: "The routed space is invalid" }))
            )
          }
          return Rpc.fork(Stream.unwrap(
            verifier.verify(payload.assertion).pipe(
              Effect.flatMap((principal) => store.watchAuthorized(payload.request, principal))
            )
          ))
        },
        PublishPresence: ({ payload }) =>
          Rpc.fork(Effect.suspend(() => {
            if (spaceId === undefined || payload.update.spaceId !== spaceId) {
              return Effect.fail(
                new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
              )
            }
            return verifier.verify(payload.assertion).pipe(
              Effect.flatMap((principal) => presence.publish(payload.update, principal))
            )
          })),
        WatchPresence: ({ payload }) => {
          if (spaceId === undefined) {
            return Rpc.fork(
              Stream.fail(new ReplicaError.ProtocolInvalid({ message: "The routed space is invalid" }))
            )
          }
          return Rpc.fork(Stream.unwrap(
            verifier.verify(payload.assertion).pipe(
              Effect.map((principal) => presence.watch(spaceId, principal))
            )
          ))
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
