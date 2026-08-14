import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as ClusterSchema from "effect/unstable/cluster/ClusterSchema"
import * as Entity from "effect/unstable/cluster/Entity"
import type * as Sharding from "effect/unstable/cluster/Sharding"
import * as Rpc from "effect/unstable/rpc/Rpc"
import * as EphemeralHub from "./EphemeralHub.js"
import { capacityExceeded, invalidConfiguration } from "./internal/errors.js"
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

export class JoinEphemeral extends Rpc.make("JoinEphemeral", {
  payload: {
    request: Protocol.EphemeralJoinRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  },
  success: Protocol.EphemeralJoinMessage,
  error: ReplicaError.ReplicaError,
  stream: true
}).annotateMerge(volatileAnnotations) {}

export class PublishEphemeral extends Rpc.make("PublishEphemeral", {
  payload: {
    request: Protocol.EphemeralPublishRequest,
    sessionToken: Identity.EphemeralSessionToken,
    assertion: PrincipalAssertion.PrincipalAssertion
  },
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export class HeartbeatEphemeral extends Rpc.make("HeartbeatEphemeral", {
  payload: {
    request: Protocol.EphemeralHeartbeatRequest,
    sessionToken: Identity.EphemeralSessionToken,
    assertion: PrincipalAssertion.PrincipalAssertion
  },
  error: ReplicaError.ReplicaError
}).annotateMerge(volatileAnnotations) {}

export const SpaceAdmissionEntity = Entity.make("EffectLocal/SpaceAdmission", [
  Submit,
  Discard
])

export const SpaceReadEntity = Entity.make("EffectLocal/SpaceRead", [
  Pull,
  Bootstrap
])

export const SpaceWatchEntity = Entity.make("EffectLocal/SpaceWatch", [Watch])

export const SpaceEphemeralJoinEntity = Entity.make("EffectLocal/SpaceEphemeralJoin", [
  JoinEphemeral
])

export const SpaceEphemeralCommandEntity = Entity.make("EffectLocal/SpaceEphemeralCommand", [
  PublishEphemeral,
  HeartbeatEphemeral
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
  readonly joinEphemeral: (
    spaceId: Identity.SpaceId,
    request: Protocol.EphemeralJoinRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Stream.Stream<Protocol.EphemeralJoinMessage, ReplicaError.ReplicaError>
  readonly publishEphemeral: (
    spaceId: Identity.SpaceId,
    request: Protocol.EphemeralPublishRequest,
    sessionToken: Identity.EphemeralSessionToken,
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly heartbeatEphemeral: (
    spaceId: Identity.SpaceId,
    request: Protocol.EphemeralHeartbeatRequest,
    sessionToken: Identity.EphemeralSessionToken,
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
}

export class Client extends Context.Service<Client, ClientService>()(
  "@lucas-barake/effect-local-rpc/SpaceEntity/Client"
) {}

const mapClient = (
  makeAdmissionClient: Effect.Success<typeof SpaceAdmissionEntity.client>,
  makeReadClient: Effect.Success<typeof SpaceReadEntity.client>,
  makeWatchClient: Effect.Success<typeof SpaceWatchEntity.client>,
  makeEphemeralJoinClient: Effect.Success<typeof SpaceEphemeralJoinEntity.client>,
  makeEphemeralCommandClient: Effect.Success<typeof SpaceEphemeralCommandEntity.client>
): ClientService => ({
  submit: (spaceId, request, assertion) =>
    makeAdmissionClient(spaceId).Submit({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  discard: (spaceId, request, assertion) =>
    makeAdmissionClient(spaceId).Discard({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  pull: (spaceId, request, assertion) =>
    makeReadClient(spaceId).Pull({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  bootstrap: (spaceId, request, assertion) =>
    makeReadClient(spaceId).Bootstrap({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  watch: (spaceId, request, assertion) =>
    makeWatchClient(spaceId).Watch({ request, assertion }).pipe(
      Stream.catchTags({
        MailboxFull: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Stream.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  joinEphemeral: (spaceId, request, assertion) =>
    makeEphemeralJoinClient(spaceId).JoinEphemeral({ request, assertion }).pipe(
      Stream.catchTags({
        MailboxFull: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Stream.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  publishEphemeral: (spaceId, request, sessionToken, assertion) =>
    makeEphemeralCommandClient(spaceId).PublishEphemeral({ request, sessionToken, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  heartbeatEphemeral: (spaceId, request, sessionToken, assertion) =>
    makeEphemeralCommandClient(spaceId).HeartbeatEphemeral({ request, sessionToken, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    )
})

export interface HandlerOptions {
  readonly admissionMailboxCapacity: number
  readonly readMailboxCapacity: number
  readonly watchMailboxCapacity: number
  readonly ephemeralCommandMailboxCapacity: number
  readonly maximumConcurrentBootstrapAuthorizations: number
  readonly maximumConcurrentBootstrapPagesPerSpace: number
  readonly maximumConcurrentEphemeralRequestsPerSpace: number
  readonly maxIdleTime?: Duration.Input
  readonly disableFatalDefects?: boolean
  readonly defectRetryPolicy?: Schedule.Schedule<any>
  readonly spanAttributes?: Record<string, string>
}

const commonHandlerOptions = (options: HandlerOptions) => ({
  maxIdleTime: options.maxIdleTime,
  disableFatalDefects: options.disableFatalDefects,
  defectRetryPolicy: options.defectRetryPolicy,
  spanAttributes: options.spanAttributes
})

export const layerHandlers = (options: HandlerOptions) =>
  Layer.unwrap(
    Effect.gen(function*() {
      if (!Number.isSafeInteger(options.admissionMailboxCapacity) || options.admissionMailboxCapacity <= 0) {
        return yield* invalidConfiguration(
          "admissionMailboxCapacity",
          "admissionMailboxCapacity must be a positive safe integer"
        )
      }
      if (!Number.isSafeInteger(options.readMailboxCapacity) || options.readMailboxCapacity <= 0) {
        return yield* invalidConfiguration(
          "readMailboxCapacity",
          "readMailboxCapacity must be a positive safe integer"
        )
      }
      if (!Number.isSafeInteger(options.watchMailboxCapacity) || options.watchMailboxCapacity <= 0) {
        return yield* invalidConfiguration(
          "watchMailboxCapacity",
          "watchMailboxCapacity must be a positive safe integer"
        )
      }
      if (
        !Number.isSafeInteger(options.ephemeralCommandMailboxCapacity) ||
        options.ephemeralCommandMailboxCapacity <= 0
      ) {
        return yield* invalidConfiguration(
          "ephemeralCommandMailboxCapacity",
          "ephemeralCommandMailboxCapacity must be a positive safe integer"
        )
      }
      if (
        !Number.isSafeInteger(options.maximumConcurrentBootstrapAuthorizations) ||
        options.maximumConcurrentBootstrapAuthorizations <= 0
      ) {
        return yield* invalidConfiguration(
          "maximumConcurrentBootstrapAuthorizations",
          "maximumConcurrentBootstrapAuthorizations must be a positive safe integer"
        )
      }
      if (
        !Number.isSafeInteger(options.maximumConcurrentBootstrapPagesPerSpace) ||
        options.maximumConcurrentBootstrapPagesPerSpace <= 0
      ) {
        return yield* invalidConfiguration(
          "maximumConcurrentBootstrapPagesPerSpace",
          "maximumConcurrentBootstrapPagesPerSpace must be a positive safe integer"
        )
      }
      if (
        !Number.isSafeInteger(options.maximumConcurrentEphemeralRequestsPerSpace) ||
        options.maximumConcurrentEphemeralRequestsPerSpace <= 0
      ) {
        return yield* invalidConfiguration(
          "maximumConcurrentEphemeralRequestsPerSpace",
          "maximumConcurrentEphemeralRequestsPerSpace must be a positive safe integer"
        )
      }

      const common = commonHandlerOptions(options)
      const bootstrapAuthorizations = yield* Semaphore.make(options.maximumConcurrentBootstrapAuthorizations)
      const layerAdmissionHandlers = SpaceAdmissionEntity.toLayer(
        Effect.gen(function*() {
          const address = yield* Entity.CurrentAddress
          const store = yield* ServerStore.ServerStore
          const verifier = yield* PrincipalAssertion.Verifier
          let spaceId: Identity.SpaceId | undefined
          if (Schema.is(Identity.SpaceId)(address.entityId)) spaceId = address.entityId

          return SpaceAdmissionEntity.of({
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
            }
          })
        }),
        { ...common, concurrency: 1, mailboxCapacity: options.admissionMailboxCapacity }
      )

      const layerReadHandlers = SpaceReadEntity.toLayer(
        Effect.gen(function*() {
          const address = yield* Entity.CurrentAddress
          const store = yield* ServerStore.ServerStore
          const verifier = yield* PrincipalAssertion.Verifier
          const bootstrapPages = yield* Semaphore.make(options.maximumConcurrentBootstrapPagesPerSpace)
          let spaceId: Identity.SpaceId | undefined
          if (Schema.is(Identity.SpaceId)(address.entityId)) spaceId = address.entityId

          return SpaceReadEntity.of({
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
              Rpc.fork(
                Effect.gen(function*() {
                  if (spaceId === undefined || payload.request.spaceId !== spaceId) {
                    return yield* new ReplicaError.ProtocolInvalid({
                      message: "The routed space does not match the payload"
                    })
                  }
                  const prepared = yield* Semaphore.withPermitsIfAvailable(
                    bootstrapAuthorizations,
                    1,
                    verifier.verify(payload.assertion).pipe(
                      Effect.flatMap((principal) => store.prepareBootstrapAuthorized(payload.request, principal))
                    )
                  )
                  if (Option.isNone(prepared)) {
                    return yield* capacityExceeded(
                      "bootstrap authorizations",
                      options.maximumConcurrentBootstrapAuthorizations
                    )
                  }
                  const result = yield* Semaphore.withPermitsIfAvailable(
                    bootstrapPages,
                    1,
                    prepared.value
                  )
                  if (Option.isSome(result)) return result.value
                  return yield* capacityExceeded(
                    "bootstrap pages",
                    options.maximumConcurrentBootstrapPagesPerSpace
                  )
                })
              )
          })
        }),
        { ...common, concurrency: 1, mailboxCapacity: options.readMailboxCapacity }
      )

      const layerWatchHandlers = SpaceWatchEntity.toLayer(
        Effect.gen(function*() {
          const address = yield* Entity.CurrentAddress
          const store = yield* ServerStore.ServerStore
          const verifier = yield* PrincipalAssertion.Verifier
          let spaceId: Identity.SpaceId | undefined
          if (Schema.is(Identity.SpaceId)(address.entityId)) spaceId = address.entityId

          return SpaceWatchEntity.of({
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
            }
          })
        }),
        { ...common, concurrency: 1, mailboxCapacity: options.watchMailboxCapacity }
      )

      const ephemeralHandlers = Effect.gen(function*() {
        const address = yield* Entity.CurrentAddress
        const ephemeral = yield* EphemeralHub.EphemeralHub
        const verifier = yield* PrincipalAssertion.Verifier
        let spaceId: Identity.SpaceId | undefined
        if (Schema.is(Identity.SpaceId)(address.entityId)) spaceId = address.entityId

        const routed = (request: { readonly spaceId: Identity.SpaceId }) =>
          spaceId !== undefined && request.spaceId === spaceId

        return {
          JoinEphemeral: ({ payload }: {
            readonly payload: {
              readonly request: Protocol.EphemeralJoinRequest
              readonly assertion: PrincipalAssertion.PrincipalAssertion
            }
          }) => {
            if (!routed(payload.request)) {
              return Rpc.fork(
                Stream.fail(
                  new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
                )
              )
            }
            return Rpc.fork(Stream.unwrap(
              verifier.verify(payload.assertion).pipe(
                Effect.map((principal) => ephemeral.join(payload.request, principal))
              )
            ))
          },
          PublishEphemeral: ({ payload }: {
            readonly payload: {
              readonly request: Protocol.EphemeralPublishRequest
              readonly sessionToken: Identity.EphemeralSessionToken
              readonly assertion: PrincipalAssertion.PrincipalAssertion
            }
          }) =>
            Effect.suspend(() => {
              if (!routed(payload.request)) {
                return Effect.fail(
                  new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
                )
              }
              return verifier.verify(payload.assertion).pipe(
                Effect.flatMap((principal) =>
                  ephemeral.publish(
                    payload.request,
                    payload.sessionToken,
                    principal
                  )
                )
              )
            }),
          HeartbeatEphemeral: ({ payload }: {
            readonly payload: {
              readonly request: Protocol.EphemeralHeartbeatRequest
              readonly sessionToken: Identity.EphemeralSessionToken
              readonly assertion: PrincipalAssertion.PrincipalAssertion
            }
          }) =>
            Effect.suspend(() => {
              if (!routed(payload.request)) {
                return Effect.fail(
                  new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })
                )
              }
              return verifier.verify(payload.assertion).pipe(
                Effect.flatMap((principal) =>
                  ephemeral.heartbeat(
                    payload.request,
                    payload.sessionToken,
                    principal
                  )
                )
              )
            })
        }
      })

      const layerEphemeralJoinHandlers = SpaceEphemeralJoinEntity.toLayer(
        ephemeralHandlers.pipe(Effect.map((handlers) =>
          SpaceEphemeralJoinEntity.of({
            JoinEphemeral: handlers.JoinEphemeral
          })
        )),
        { ...common, concurrency: 1, mailboxCapacity: "unbounded" }
      )

      const layerEphemeralCommandHandlers = SpaceEphemeralCommandEntity.toLayer(
        ephemeralHandlers.pipe(Effect.map((handlers) =>
          SpaceEphemeralCommandEntity.of({
            PublishEphemeral: handlers.PublishEphemeral,
            HeartbeatEphemeral: handlers.HeartbeatEphemeral
          })
        )),
        {
          ...common,
          concurrency: options.maximumConcurrentEphemeralRequestsPerSpace,
          mailboxCapacity: options.ephemeralCommandMailboxCapacity
        }
      )

      return Layer.mergeAll(
        layerAdmissionHandlers,
        layerReadHandlers,
        layerWatchHandlers,
        layerEphemeralJoinHandlers,
        layerEphemeralCommandHandlers
      )
    })
  )

export const layerClient: Layer.Layer<Client, never, Sharding.Sharding> = Layer.effect(
  Client,
  Effect.gen(function*() {
    const admission = yield* SpaceAdmissionEntity.client
    const read = yield* SpaceReadEntity.client
    const watch = yield* SpaceWatchEntity.client
    const ephemeralJoin = yield* SpaceEphemeralJoinEntity.client
    const ephemeralCommand = yield* SpaceEphemeralCommandEntity.client
    return mapClient(admission, read, watch, ephemeralJoin, ephemeralCommand)
  })
)

export const layer = (options: HandlerOptions) => Layer.merge(layerHandlers(options), layerClient)
