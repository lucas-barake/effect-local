import * as ServerStore from "@lucas-barake/effect-local-sql/ServerStore"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ClusterSchema from "effect/cluster/ClusterSchema"
import * as Entity from "effect/cluster/Entity"
import type * as Sharding from "effect/cluster/Sharding"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Rpc from "effect/rpc/Rpc"
import type * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as EphemeralHub from "./EphemeralHub.js"
import { capacityExceeded, invalidConfiguration } from "./internal/errors.js"
import * as PrincipalAssertion from "./PrincipalAssertion.js"

const volatileAnnotations = Context.make(ClusterSchema.Persisted, false).pipe(
  Context.add(ClusterSchema.WithTransaction, false),
  Context.add(ClusterSchema.Uninterruptible, false)
)

export class SubmitBatch extends Rpc.make("SubmitBatch", {
  payload: {
    request: Protocol.SubmitBatchRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  },
  success: Protocol.SubmitBatchResult,
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

export const Space = Entity.make("EffectLocal/Space", [
  SubmitBatch,
  Discard,
  Pull,
  Bootstrap,
  Watch,
  JoinEphemeral,
  PublishEphemeral,
  HeartbeatEphemeral
])

export interface ClientService {
  readonly submitBatch: (
    spaceId: Identity.SpaceId,
    request: Protocol.SubmitBatchRequest,
    assertion: PrincipalAssertion.PrincipalAssertion
  ) => Effect.Effect<Protocol.SubmitBatchResult, ReplicaError.ReplicaError>
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

const mapClient = (makeClient: Effect.Success<typeof Space.client>): ClientService => ({
  submitBatch: (spaceId, request, assertion) =>
    makeClient(spaceId).SubmitBatch({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        EntityNotAssignedToRunner: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  discard: (spaceId, request, assertion) =>
    makeClient(spaceId).Discard({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        EntityNotAssignedToRunner: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  pull: (spaceId, request, assertion) =>
    makeClient(spaceId).Pull({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        EntityNotAssignedToRunner: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  bootstrap: (spaceId, request, assertion) =>
    makeClient(spaceId).Bootstrap({ request, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        EntityNotAssignedToRunner: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  watch: (spaceId, request, assertion) =>
    makeClient(spaceId).Watch({ request, assertion }).pipe(
      Stream.catchTags({
        MailboxFull: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        EntityNotAssignedToRunner: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Stream.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  joinEphemeral: (spaceId, request, assertion) =>
    makeClient(spaceId).JoinEphemeral({ request, assertion }).pipe(
      Stream.catchTags({
        MailboxFull: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        EntityNotAssignedToRunner: () => Stream.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Stream.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  publishEphemeral: (spaceId, request, sessionToken, assertion) =>
    makeClient(spaceId).PublishEphemeral({ request, sessionToken, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        EntityNotAssignedToRunner: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    ),
  heartbeatEphemeral: (spaceId, request, sessionToken, assertion) =>
    makeClient(spaceId).HeartbeatEphemeral({ request, sessionToken, assertion }).pipe(
      Effect.catchTags({
        MailboxFull: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        AlreadyProcessingMessage: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        EntityNotAssignedToRunner: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        PersistenceError: (error) => Effect.fail(new ReplicaError.StorageUnavailable({ cause: error.cause }))
      })
    )
})

export interface HandlerOptions {
  readonly mailboxCapacity?: number | "unbounded" | undefined
  readonly maximumConcurrentBootstrapAuthorizations?: number | undefined
  readonly maximumConcurrentBootstrapPagesPerSpace?: number | undefined
  readonly maximumConcurrentEphemeralJoinVerificationsPerSpace?: number | undefined
  readonly maximumConcurrentEphemeralRequestsPerSpace?: number | undefined
  readonly maxIdleTime?: Duration.Input | undefined
  readonly disableFatalDefects?: boolean | undefined
  readonly defectRetryPolicy?: Schedule.Schedule<any> | undefined
  readonly spanAttributes?: Record<string, string> | undefined
}

export const defaults = {
  maximumConcurrentBootstrapAuthorizations: 64,
  maximumConcurrentBootstrapPagesPerSpace: 4,
  maximumConcurrentEphemeralJoinVerificationsPerSpace: 64,
  maximumConcurrentEphemeralRequestsPerSpace: 64
} as const

const positiveInteger = (option: string, value: number) => {
  if (Number.isSafeInteger(value) && value > 0) return Effect.succeed(value)
  return invalidConfiguration(option, `${option} must be a positive safe integer`)
}

export const layerHandlers = (options: HandlerOptions = {}) =>
  Layer.unwrap(
    Effect.gen(function*() {
      const maximumConcurrentBootstrapAuthorizations = yield* positiveInteger(
        "maximumConcurrentBootstrapAuthorizations",
        options.maximumConcurrentBootstrapAuthorizations ?? defaults.maximumConcurrentBootstrapAuthorizations
      )
      const maximumConcurrentBootstrapPagesPerSpace = yield* positiveInteger(
        "maximumConcurrentBootstrapPagesPerSpace",
        options.maximumConcurrentBootstrapPagesPerSpace ?? defaults.maximumConcurrentBootstrapPagesPerSpace
      )
      const maximumConcurrentEphemeralJoinVerificationsPerSpace = yield* positiveInteger(
        "maximumConcurrentEphemeralJoinVerificationsPerSpace",
        options.maximumConcurrentEphemeralJoinVerificationsPerSpace ??
          defaults.maximumConcurrentEphemeralJoinVerificationsPerSpace
      )
      const maximumConcurrentEphemeralRequestsPerSpace = yield* positiveInteger(
        "maximumConcurrentEphemeralRequestsPerSpace",
        options.maximumConcurrentEphemeralRequestsPerSpace ?? defaults.maximumConcurrentEphemeralRequestsPerSpace
      )
      if (
        options.mailboxCapacity !== undefined && options.mailboxCapacity !== "unbounded" &&
        (!Number.isSafeInteger(options.mailboxCapacity) || options.mailboxCapacity <= 0)
      ) {
        return yield* invalidConfiguration("mailboxCapacity", "mailboxCapacity must be a positive safe integer")
      }
      const bootstrapAuthorizations = yield* Semaphore.make(maximumConcurrentBootstrapAuthorizations)

      return Space.toLayer(
        Effect.gen(function*() {
          const address = yield* Entity.CurrentAddress
          const store = yield* ServerStore.ServerStore
          const ephemeral = yield* EphemeralHub.EphemeralHub
          const verifier = yield* PrincipalAssertion.Verifier
          const admission = yield* Semaphore.make(1)
          const bootstrapPages = yield* Semaphore.make(maximumConcurrentBootstrapPagesPerSpace)
          const joinVerifications = yield* Semaphore.make(maximumConcurrentEphemeralJoinVerificationsPerSpace)
          const ephemeralRequests = yield* Semaphore.make(maximumConcurrentEphemeralRequestsPerSpace)
          let spaceId: Identity.SpaceId | undefined
          if (Schema.is(Identity.SpaceId)(address.entityId)) spaceId = address.entityId
          const routed = (requested: Identity.SpaceId) => spaceId !== undefined && requested === spaceId
          const misrouted = new ReplicaError.ProtocolInvalid({ message: "The routed space does not match the payload" })

          return Space.of({
            SubmitBatch: ({ payload }) => {
              if (!payload.request.envelopes.every((envelope) => routed(envelope.spaceId))) {
                return Effect.fail(misrouted)
              }
              return verifier.verify(payload.assertion).pipe(
                Effect.flatMap((principal) => store.admitBatch(payload.request, principal)),
                admission.withPermits(1)
              )
            },
            Discard: ({ payload }) => {
              if (!routed(payload.request.envelope.spaceId)) return Effect.fail(misrouted)
              return verifier.verify(payload.assertion).pipe(
                Effect.flatMap((principal) => store.discard(payload.request, principal)),
                admission.withPermits(1)
              )
            },
            Pull: ({ payload }) => {
              if (!routed(payload.request.spaceId)) return Effect.fail(misrouted)
              return verifier.verify(payload.assertion).pipe(
                Effect.flatMap((principal) => store.pullAuthorized(payload.request, principal))
              )
            },
            Bootstrap: Effect.fnUntraced(function*({ payload }) {
              if (!routed(payload.request.spaceId)) return yield* misrouted
              const prepared = yield* Semaphore.withPermitsIfAvailable(
                bootstrapAuthorizations,
                1,
                verifier.verify(payload.assertion).pipe(
                  Effect.flatMap((principal) => store.prepareBootstrapAuthorized(payload.request, principal))
                )
              )
              if (Option.isNone(prepared)) {
                return yield* capacityExceeded("bootstrap authorizations", maximumConcurrentBootstrapAuthorizations)
              }
              const result = yield* Semaphore.withPermitsIfAvailable(bootstrapPages, 1, prepared.value)
              if (Option.isSome(result)) return result.value
              return yield* capacityExceeded("bootstrap pages", maximumConcurrentBootstrapPagesPerSpace)
            }),
            Watch: ({ payload }) => {
              if (!routed(payload.request.spaceId)) return Stream.fail(misrouted)
              return Stream.unwrap(
                verifier.verify(payload.assertion).pipe(
                  Effect.flatMap((principal) => store.watchAuthorized(payload.request, principal))
                )
              )
            },
            JoinEphemeral: ({ payload }) => {
              if (!routed(payload.request.spaceId)) return Stream.fail(misrouted)
              const verified = Effect.gen(function*() {
                const result = yield* Semaphore.withPermitsIfAvailable(
                  joinVerifications,
                  1,
                  verifier.verify(payload.assertion).pipe(
                    Effect.map((principal) => ephemeral.join(payload.request, principal))
                  )
                )
                if (Option.isSome(result)) return result.value
                return yield* capacityExceeded(
                  "ephemeral join verifications",
                  maximumConcurrentEphemeralJoinVerificationsPerSpace
                )
              })
              return Stream.unwrap(verified)
            },
            PublishEphemeral: ({ payload }) => {
              if (!routed(payload.request.spaceId)) return Effect.fail(misrouted)
              return verifier.verify(payload.assertion).pipe(
                Effect.flatMap((principal) => ephemeral.publish(payload.request, payload.sessionToken, principal)),
                ephemeralRequests.withPermits(1)
              )
            },
            HeartbeatEphemeral: ({ payload }) => {
              if (!routed(payload.request.spaceId)) return Effect.fail(misrouted)
              return verifier.verify(payload.assertion).pipe(
                Effect.flatMap((principal) => ephemeral.heartbeat(payload.request, payload.sessionToken, principal)),
                ephemeralRequests.withPermits(1)
              )
            }
          })
        }),
        {
          concurrency: "unbounded",
          mailboxCapacity: options.mailboxCapacity,
          maxIdleTime: options.maxIdleTime,
          disableFatalDefects: options.disableFatalDefects,
          defectRetryPolicy: options.defectRetryPolicy,
          spanAttributes: options.spanAttributes
        }
      )
    })
  )

export const layerClient: Layer.Layer<Client, never, Sharding.Sharding> = Layer.effect(
  Client,
  Space.client.pipe(Effect.map(mapClient))
)

export const layer = (options: HandlerOptions = {}) => Layer.merge(layerHandlers(options), layerClient)
