import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Exit from "effect/Exit"
import * as Hash from "effect/Hash"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import * as Pull from "effect/Pull"
import * as RcMap from "effect/RcMap"
import * as Result from "effect/Result"
import type * as RpcClient from "effect/rpc/RpcClient"
import type * as RpcMiddleware from "effect/rpc/RpcMiddleware"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import type * as Authentication from "./Authentication.js"
import { positiveFiniteDurationMillis, positiveSafeInteger, reconnectPolicy } from "./internal/configuration.js"
import * as EphemeralView from "./internal/ephemeralView.js"
import { invalidConfiguration } from "./internal/errors.js"
import * as LosslessQueue from "./internal/losslessQueue.js"
import * as ProtocolSessionRetry from "./internal/protocolSession.js"
import { hasRemoteDefect } from "./internal/remoteDefect.js"
import { findDecodeDefect } from "./internal/responseDecoding.js"
import * as SequencedPubSub from "./internal/sequencedPubSub.js"
import * as ProtocolSession from "./ProtocolSession.js"
import * as Transport from "./Transport.js"

type JoinInput = Omit<Protocol.EphemeralJoinRequest, "ttlMillis"> & {
  readonly ttl: Duration.Input
}

export interface PublishTarget {
  readonly spaceId: Identity.SpaceId
  readonly member: Protocol.EphemeralMember
}

export interface EventPublishOptions<D extends Ephemeral.AnyEvent,> extends PublishTarget {
  readonly payload: Ephemeral.Payload<D>
  readonly ttl: Duration.Input
}

export interface StatePublishOptions<D extends Ephemeral.AnyState,> extends PublishTarget {
  readonly key: Ephemeral.Key<D>
  readonly payload: Ephemeral.Payload<D>
  readonly ttl: Duration.Input
}

export interface StateRemoveOptions<D extends Ephemeral.AnyState,> extends PublishTarget {
  readonly key: Ephemeral.Key<D>
}

export interface SessionOptions<M extends Ephemeral.AnyMember,> extends PublishTarget {
  readonly value: Ephemeral.Payload<M>
  readonly ttl: Duration.Input
}

export interface EventEnvelope<D extends Ephemeral.AnyEvent,> {
  readonly member: Protocol.EphemeralMember
  readonly payload: Ephemeral.Payload<D>
}

export interface StateEntry<D extends Ephemeral.AnyState,> {
  readonly member: Protocol.EphemeralMember
  readonly key: Ephemeral.Key<D>
  readonly value: Ephemeral.Payload<D>
  readonly expiresAtMillis: number
}

export interface MemberEntry<M extends Ephemeral.AnyMember,> {
  readonly member: Protocol.EphemeralMember
  readonly value: Ephemeral.Payload<M>
  readonly expiresAtMillis: number
}

export interface Session<M extends Ephemeral.AnyMember,> {
  readonly spaceId: Identity.SpaceId
  readonly member: Protocol.EphemeralMember
  readonly events: <D extends Ephemeral.AnyEvent,>(
    definition: D
  ) => Stream.Stream<EventEnvelope<D>, Ephemeral.DecodeError | ReplicaError.ReplicaError>
  readonly state: <D extends Ephemeral.AnyState,>(
    definition: D
  ) => Stream.Stream<ReadonlyArray<StateEntry<D>>, Ephemeral.DecodeError | ReplicaError.ReplicaError>
  readonly members: Stream.Stream<
    ReadonlyArray<MemberEntry<M>>,
    Ephemeral.DecodeError | ReplicaError.ReplicaError
  >
  readonly updateMember: (
    value: Ephemeral.Payload<M>
  ) => Effect.Effect<void, ReplicaError.ReplicaError | Ephemeral.EncodeError>
}

export interface Service {
  readonly session: <M extends Ephemeral.AnyMember,>(
    profile: M,
    options: SessionOptions<M>
  ) => Effect.Effect<Session<M>, ReplicaError.ReplicaError | Ephemeral.EncodeError, Scope.Scope>
  readonly publish: {
    <D extends Ephemeral.AnyEvent,>(
      definition: D,
      options: EventPublishOptions<D>
    ): Effect.Effect<void, ReplicaError.ReplicaError | Ephemeral.EncodeError>
    <D extends Ephemeral.AnyState,>(
      definition: D,
      options: StatePublishOptions<D>
    ): Effect.Effect<void, ReplicaError.ReplicaError | Ephemeral.EncodeError>
  }
  readonly clear: (
    definition: Ephemeral.AnyEvent,
    target: PublishTarget
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly remove: <D extends Ephemeral.AnyState,>(
    definition: D,
    options: StateRemoveOptions<D>
  ) => Effect.Effect<void, ReplicaError.ReplicaError | Ephemeral.EncodeError>
}

export class EphemeralClient extends Context.Service<EphemeralClient, Service>()(
  "@lucas-barake/effect-local-rpc/EphemeralClient"
) {}

export interface Options extends ProtocolSession.Options {
  readonly rpcTimeout?: Duration.Input
  readonly heartbeatInterval?: Duration.Input
  readonly rejoinPolicy?: Schedule.Schedule<unknown, ReplicaError.ReplicaError>
  readonly eventCapacity?: number
}

const boundedTtlMillis = Effect.fnUntraced(function*(
  input: Duration.Input,
  minimum: number,
  maximum: number
) {
  const millis = yield* positiveFiniteDurationMillis("ttl", input)
  if (millis >= minimum && millis <= maximum) return millis
  return yield* invalidConfiguration(
    "ttl",
    `ttl must resolve to between ${minimum} and ${maximum} milliseconds`
  )
})

const normalizeJoinRequest = Effect.fnUntraced(function*(request: JoinInput) {
  const ttlMillis = yield* boundedTtlMillis(
    request.ttl,
    Protocol.minimumEphemeralMemberTtlMillis,
    Protocol.maximumEphemeralMemberTtlMillis
  )
  return Protocol.EphemeralJoinRequest.make({
    spaceId: request.spaceId,
    member: request.member,
    value: request.value,
    ttlMillis
  })
})

class SessionIdentity implements Equal.Equal {
  readonly value: string
  readonly request: Protocol.EphemeralJoinRequest
  constructor(request: Protocol.EphemeralJoinRequest) {
    this.value = `${request.spaceId}:${request.member.clientId}:${request.member.membershipIncarnation}`
    this.request = request
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof SessionIdentity && this.value === that.value
  }
  [Hash.symbol](): number {
    return Hash.string(this.value)
  }
}

interface SessionRuntime {
  readonly requestHash: string
  readonly views: PubSub.PubSub<EphemeralView.View>
  readonly events: SequencedPubSub.SequencedPubSub<Protocol.EphemeralEventEntry>
  readonly ready: Deferred.Deferred<void, ReplicaError.ReplicaError>
  readonly failure: Deferred.Deferred<never, ReplicaError.ReplicaError>
  readonly memberUpdates: Semaphore.Semaphore
  readonly recordMemberValue: (value: typeof Schema.Json.Type) => void
}

const isTransportFailure = (error: ReplicaError.ReplicaError) =>
  error._tag === "ServerUnavailable" || error._tag === "OperationTimeout"

const isTransientFailure = (error: ReplicaError.ReplicaError) =>
  error._tag === "ServerUnavailable" ||
  error._tag === "OperationTimeout" ||
  error._tag === "AuthenticatorUnavailable"

export const layerFromSession = (
  options?: Pick<Options, "rpcTimeout" | "heartbeatInterval" | "rejoinPolicy" | "eventCapacity">
): Layer.Layer<
  EphemeralClient,
  ReplicaError.InvalidConfiguration,
  ProtocolSession.ProtocolSession | Transport.Transport
> =>
  Layer.effect(
    EphemeralClient,
    Effect.gen(function*() {
      const rpcTimeoutMillis = yield* positiveFiniteDurationMillis(
        "rpcTimeout",
        options?.rpcTimeout ?? "10 seconds"
      )
      const heartbeatIntervalMillis = yield* positiveFiniteDurationMillis(
        "heartbeatInterval",
        options?.heartbeatInterval ?? "20 seconds"
      )
      const eventCapacity = yield* positiveSafeInteger("eventCapacity", options?.eventCapacity ?? 1_024)
      const rejoinPolicy = Schedule.while(
        options?.rejoinPolicy ?? reconnectPolicy,
        ({ input }) => isTransientFailure(input)
      )
      const session = yield* ProtocolSession.ProtocolSession
      const transport = yield* Transport.Transport
      const client = session.client
      interface ActiveSession {
        readonly owner: object
        readonly sessionToken: Identity.EphemeralSessionToken
      }
      const sessions = new Map<string, ActiveSession>()
      const sessionKey = (request: Protocol.EphemeralHeartbeatRequest) =>
        `${request.spaceId}:${request.member.clientId}:${request.member.membershipIncarnation}`
      const requireSession = (
        request: Protocol.EphemeralHeartbeatRequest
      ): Effect.Effect<ActiveSession, ReplicaError.EphemeralSessionUnavailable> =>
        Effect.suspend(() => {
          const active = sessions.get(sessionKey(request))
          if (active !== undefined) return Effect.succeed(active)
          return Effect.fail(
            new ReplicaError.EphemeralSessionUnavailable({
              spaceId: request.spaceId,
              clientId: request.member.clientId,
              membershipIncarnation: request.member.membershipIncarnation
            })
          )
        })

      const publishWire = (request: Protocol.EphemeralPublishRequest) =>
        requireSession(request).pipe(
          Effect.flatMap((active) =>
            ProtocolSessionRetry.run(
              session,
              (version) =>
                client.PublishEphemeral({
                  request,
                  sessionToken: active.sessionToken,
                  protocolVersion: version
                }).pipe(
                  Effect.catchReasons(
                    "RpcClientError",
                    {
                      WorkerSpawnError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      WorkerSendError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      WorkerReceiveError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      WorkerUnknownError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      SocketReadError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      SocketWriteError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      SocketOpenError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      SocketCloseError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      SocketUpgradeError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      HttpError: (reason, error) => {
                        if (reason.kind === "TransportError") {
                          return Effect.fail(new ReplicaError.ServerUnavailable())
                        }
                        return Effect.fail(
                          new ReplicaError.ProtocolInvalid({
                            message: "The PublishEphemeral RPC failed",
                            cause: error
                          })
                        )
                      },
                      RpcClientDefect: (_, error) =>
                        Effect.fail(
                          new ReplicaError.ProtocolInvalid({
                            message: "The PublishEphemeral RPC failed",
                            cause: error
                          })
                        )
                    },
                    (_, error) => Effect.die(error)
                  ),
                  Effect.catchCause((cause) => {
                    if (Cause.hasInterruptsOnly(cause)) return Effect.fail(new ReplicaError.ServerUnavailable())
                    const undecodable = findDecodeDefect(cause)
                    if (undecodable === undefined) return Effect.failCause(cause)
                    return Effect.fail(
                      new ReplicaError.ProtocolInvalid({
                        message: "The PublishEphemeral RPC response could not be decoded",
                        cause: undecodable
                      })
                    )
                  }),
                  Effect.timeoutOrElse({
                    duration: rpcTimeoutMillis,
                    orElse: () =>
                      Effect.fail(
                        new ReplicaError.OperationTimeout({
                          operation: "PublishEphemeral",
                          timeoutMillis: rpcTimeoutMillis
                        })
                      )
                  })
                )
            )
          ),
          Effect.catchCause((cause) => {
            if (!hasRemoteDefect(cause)) return Effect.failCause(cause)
            return Effect.fail(
              new ReplicaError.ProtocolInvalid({
                message: "The PublishEphemeral RPC failed on the server",
                cause: Cause.squash(cause)
              })
            )
          }),
          Effect.asVoid,
          Effect.withSpan("EphemeralClient.publish", {
            attributes: {
              "space.id": request.spaceId,
              "client.id": request.member.clientId
            }
          })
        )

      const heartbeat = (request: Protocol.EphemeralHeartbeatRequest) =>
        requireSession(request).pipe(
          Effect.flatMap((active) =>
            ProtocolSessionRetry.run(
              session,
              (version) =>
                client.HeartbeatEphemeral({
                  ...request,
                  sessionToken: active.sessionToken,
                  protocolVersion: version
                }).pipe(
                  Effect.catchReasons(
                    "RpcClientError",
                    {
                      WorkerSpawnError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      WorkerSendError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      WorkerReceiveError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      WorkerUnknownError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      SocketReadError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      SocketWriteError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      SocketOpenError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      SocketCloseError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      SocketUpgradeError: () => Effect.fail(new ReplicaError.ServerUnavailable()),
                      HttpError: (reason, error) => {
                        if (reason.kind === "TransportError") {
                          return Effect.fail(new ReplicaError.ServerUnavailable())
                        }
                        return Effect.fail(
                          new ReplicaError.ProtocolInvalid({
                            message: "The HeartbeatEphemeral RPC failed",
                            cause: error
                          })
                        )
                      },
                      RpcClientDefect: (_, error) =>
                        Effect.fail(
                          new ReplicaError.ProtocolInvalid({
                            message: "The HeartbeatEphemeral RPC failed",
                            cause: error
                          })
                        )
                    },
                    (_, error) => Effect.die(error)
                  ),
                  Effect.catchCause((cause) => {
                    if (Cause.hasInterruptsOnly(cause)) return Effect.fail(new ReplicaError.ServerUnavailable())
                    const undecodable = findDecodeDefect(cause)
                    if (undecodable === undefined) return Effect.failCause(cause)
                    return Effect.fail(
                      new ReplicaError.ProtocolInvalid({
                        message: "The HeartbeatEphemeral RPC response could not be decoded",
                        cause: undecodable
                      })
                    )
                  }),
                  Effect.timeoutOrElse({
                    duration: rpcTimeoutMillis,
                    orElse: () =>
                      Effect.fail(
                        new ReplicaError.OperationTimeout({
                          operation: "HeartbeatEphemeral",
                          timeoutMillis: rpcTimeoutMillis
                        })
                      )
                  })
                )
            )
          ),
          Effect.catchCause((cause) => {
            if (!hasRemoteDefect(cause)) return Effect.failCause(cause)
            return Effect.fail(
              new ReplicaError.ProtocolInvalid({
                message: "The HeartbeatEphemeral RPC failed on the server",
                cause: Cause.squash(cause)
              })
            )
          }),
          Effect.asVoid,
          Effect.withSpan("EphemeralClient.heartbeat", {
            attributes: {
              "space.id": request.spaceId,
              "client.id": request.member.clientId
            }
          })
        )

      const joinAttempt = (
        request: Protocol.EphemeralJoinRequest,
        memberUpdates: Semaphore.Semaphore,
        memberValue: () => typeof Schema.Json.Type,
        version: Protocol.ProtocolVersion,
        synchronized: Deferred.Deferred<void>
      ) =>
        Stream.unwrap(Effect.gen(function*() {
          const owner = {}
          const started = yield* Deferred.make<Protocol.EphemeralSessionStarted>()
          const holding = yield* Deferred.make<void>()
          yield* Deferred.succeed(holding, undefined).pipe(
            Effect.andThen(Deferred.await(started)),
            Semaphore.withPermit(memberUpdates),
            Effect.forkScoped({ startImmediately: true })
          )
          yield* Deferred.await(holding)
          const queue = yield* client.JoinEphemeral(
            { ...request, value: memberValue(), protocolVersion: version },
            { asQueue: true }
          )
          const messages = LosslessQueue.stream(queue).pipe(
            Stream.catchReasons(
              "RpcClientError",
              {
                WorkerSpawnError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                WorkerSendError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                WorkerReceiveError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                WorkerUnknownError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                SocketReadError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                SocketWriteError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                SocketOpenError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                SocketCloseError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                SocketUpgradeError: () => Stream.fail(new ReplicaError.ServerUnavailable()),
                HttpError: (reason, error) => {
                  if (reason.kind === "TransportError") {
                    return Stream.fail(new ReplicaError.ServerUnavailable())
                  }
                  return Stream.fail(
                    new ReplicaError.ProtocolInvalid({
                      message: "The JoinEphemeral RPC failed",
                      cause: error
                    })
                  )
                },
                RpcClientDefect: (_, error) =>
                  Stream.fail(
                    new ReplicaError.ProtocolInvalid({
                      message: "The JoinEphemeral RPC failed",
                      cause: error
                    })
                  )
              },
              (_, error) => Stream.die(error)
            ),
            Stream.catchCause((cause) => {
              if (Cause.hasInterruptsOnly(cause)) return Stream.fail(new ReplicaError.ServerUnavailable())
              const undecodable = findDecodeDefect(cause)
              if (undecodable === undefined) return Stream.failCause(cause)
              return Stream.fail(
                new ReplicaError.ProtocolInvalid({
                  message: "The JoinEphemeral RPC response could not be decoded",
                  cause: undecodable
                })
              )
            })
          )
          const visible = messages.pipe(
            Stream.tap((message) => {
              if (message._tag === "Snapshot") return Deferred.succeed(synchronized, undefined)
              if (message._tag !== "SessionStarted") return Effect.void
              sessions.set(sessionKey(request), { owner, sessionToken: message.sessionToken })
              return Deferred.succeed(started, message)
            }),
            Stream.filter(
              (message): message is Protocol.EphemeralMessage => message._tag !== "SessionStarted"
            )
          )
          const heartbeatLoop = Deferred.await(started).pipe(
            Effect.flatMap((accepted) => {
              const halfLeaseMillis = Math.floor(accepted.leaseMillis / 2)
              const interval = Math.max(1, Math.min(heartbeatIntervalMillis, halfLeaseMillis))
              return Effect.sleep(interval).pipe(
                Effect.andThen(heartbeat({ spaceId: request.spaceId, member: request.member })),
                Effect.forever
              )
            })
          )
          return LosslessQueue.mergeEffect(visible, heartbeatLoop).pipe(
            Stream.ensuring(Effect.sync(() => {
              if (sessions.get(sessionKey(request))?.owner === owner) {
                sessions.delete(sessionKey(request))
              }
            }))
          )
        }))

      const joinWire = (
        request: Protocol.EphemeralJoinRequest,
        memberUpdates: Semaphore.Semaphore,
        memberValue: () => typeof Schema.Json.Type
      ) =>
        ProtocolSessionRetry.runStream(
          session,
          (version) =>
            Stream.unwrap(Effect.gen(function*() {
              const synchronized = yield* Deferred.make<void>()
              const handshakeDeadline = Deferred.await(synchronized).pipe(
                Effect.timeoutOrElse({
                  duration: rpcTimeoutMillis,
                  orElse: () =>
                    Effect.fail(
                      new ReplicaError.OperationTimeout({
                        operation: "JoinEphemeral",
                        timeoutMillis: rpcTimeoutMillis
                      })
                    )
                })
              )
              return LosslessQueue.mergeEffect(
                joinAttempt(request, memberUpdates, memberValue, version, synchronized),
                handshakeDeadline
              )
            }))
        ).pipe(
          Stream.catchCause((cause) => {
            if (!hasRemoteDefect(cause)) return Stream.failCause(cause)
            return Stream.fail(
              new ReplicaError.ProtocolInvalid({
                message: "The JoinEphemeral RPC failed on the server",
                cause: Cause.squash(cause)
              })
            )
          }),
          Stream.withSpan("EphemeralClient.join", {
            attributes: {
              "space.id": request.spaceId,
              "client.id": request.member.clientId
            }
          })
        )

      const runtimes = yield* RcMap.make({
        lookup: Effect.fnUntraced(function*(identity: SessionIdentity) {
          const views = yield* PubSub.sliding<EphemeralView.View>({ capacity: 1, replay: 1 })
          const events = yield* SequencedPubSub.sliding<Protocol.EphemeralEventEntry>("ephemeral events", eventCapacity)
          const ready = yield* Deferred.make<void, ReplicaError.ReplicaError>()
          const failure = yield* Deferred.make<never, ReplicaError.ReplicaError>()
          let view: EphemeralView.View | undefined
          let unknownPresented = false
          let memberValue = identity.request.value
          const memberUpdates = yield* Semaphore.make(1)
          const consume = (message: Protocol.EphemeralMessage) => {
            if (message._tag === "Event") return SequencedPubSub.publish(events, message.entry)
            if (message._tag === "EventCleared") return Effect.void
            const next = EphemeralView.reduce(view, message)
            if (next === undefined) return Effect.void
            view = next
            unknownPresented = false
            return PubSub.publish(views, next).pipe(
              Effect.andThen(Deferred.succeed(ready, undefined)),
              Effect.asVoid
            )
          }
          const presentUnknown = Effect.suspend(() => {
            if (unknownPresented) return Effect.void
            view = undefined
            unknownPresented = true
            return PubSub.publish(views, EphemeralView.empty()).pipe(
              Effect.andThen(Deferred.succeed(ready, undefined)),
              Effect.asVoid
            )
          })
          const joinUntilTerminal = Effect.gen(function*() {
            let step = yield* Schedule.toStepWithMetadata(rejoinPolicy)
            while (true) {
              let joined = false
              const transportGeneration = yield* transport.generation
              const attempt = yield* joinWire(identity.request, memberUpdates, () => memberValue).pipe(
                Stream.runForEach((message) => {
                  joined = true
                  return consume(message)
                }),
                Effect.result
              )
              if (joined) step = yield* Schedule.toStepWithMetadata(rejoinPolicy)
              if (Result.isSuccess(attempt)) continue
              const error = attempt.failure
              if (isTransientFailure(error)) yield* presentUnknown
              let backoff = step(error).pipe(
                Effect.as(true),
                Pull.catchDone(() => Effect.succeed(false))
              )
              if (isTransportFailure(error)) {
                backoff = Effect.raceFirst(backoff, transport.waitForChange(transportGeneration).pipe(Effect.as(true)))
              }
              if (!(yield* backoff)) return yield* Effect.fail(error)
            }
          })
          yield* joinUntilTerminal.pipe(
            Effect.catchCause((cause) =>
              Deferred.failCause(ready, cause).pipe(
                Effect.andThen(Deferred.failCause(failure, cause))
              )
            ),
            Effect.forkScoped
          )
          yield* Effect.addFinalizer(() => {
            const closed = new ReplicaError.EphemeralSessionUnavailable({
              spaceId: identity.request.spaceId,
              clientId: identity.request.member.clientId,
              membershipIncarnation: identity.request.member.membershipIncarnation
            })
            return Deferred.fail(ready, closed).pipe(
              Effect.andThen(Deferred.fail(failure, closed)),
              Effect.andThen(PubSub.shutdown(views)),
              Effect.andThen(SequencedPubSub.shutdown(events))
            )
          })
          const runtime: SessionRuntime = {
            requestHash: Canonical.hash(identity.request),
            views,
            events,
            ready,
            failure,
            memberUpdates,
            recordMemberValue: (value) => {
              memberValue = value
            }
          }
          return runtime
        })
      })

      const makeSession = <M extends Ephemeral.AnyMember,>(
        profile: M,
        target: PublishTarget,
        runtime: SessionRuntime
      ): Session<M> => {
        const failureStream = Stream.fromEffect(Deferred.await(runtime.failure))
        const orFailure = <A, E extends { readonly _tag: string },>(stream: Stream.Stream<A, E>) =>
          LosslessQueue.merge(stream, failureStream)
        const events = (definition: Ephemeral.AnyEvent) =>
          Stream.unwrap(SequencedPubSub.subscribe(runtime.events)).pipe(
            Stream.filter((entry) => entry.channel === definition.name),
            Stream.mapEffect((entry) =>
              Schema.decodeUnknownEffect(definition.payloadSchema)(entry.value).pipe(
                Effect.catchTag("SchemaError", (cause) =>
                  Effect.fail(new Ephemeral.DecodeError({ definition: definition.name, cause }))),
                Effect.map((payload) => ({ member: entry.member, payload }))
              )
            ),
            orFailure
          )
        const state = (definition: Ephemeral.AnyState) =>
          Stream.fromPubSub(runtime.views).pipe(
            Stream.mapAccum(
              EphemeralView.noProjection,
              EphemeralView.projectSlice(
                (view) =>
                  EphemeralView.channelStates(view, definition.name),
                (view) => EphemeralView.channelSlice(view, definition.name)
              )
            ),
            Stream.mapEffect(Effect.forEach((entry) =>
              Effect.all({
                key: Schema.decodeUnknownEffect(definition.keySchema)(entry.key),
                value: Schema.decodeUnknownEffect(definition.payloadSchema)(entry.value)
              }).pipe(
                Effect.catchTag("SchemaError", (cause) =>
                  Effect.fail(new Ephemeral.DecodeError({ definition: definition.name, cause }))),
                Effect.map(({ key, value }) => ({
                  member: entry.member,
                  key,
                  value,
                  expiresAtMillis: entry.expiresAtMillis
                }))
              )
            )),
            orFailure
          )
        const members = Stream.fromPubSub(runtime.views).pipe(
          Stream.mapAccum(
            EphemeralView.noProjection,
            EphemeralView.projectSlice(
              (view) =>
                view.members,
              (view) =>
                EphemeralView.sortedValues(view.members)
            )
          ),
          Stream.mapEffect(Effect.forEach((entry) =>
            Schema.decodeUnknownEffect(profile.payloadSchema)(entry.value).pipe(
              Effect.catchTag("SchemaError", (cause) =>
                Effect.fail(new Ephemeral.DecodeError({ definition: "member", cause }))),
              Effect.map((value) => ({
                member: entry.member,
                value,
                expiresAtMillis: entry.expiresAtMillis
              }))
            )
          )),
          orFailure
        )
        const updateMember = (value: Ephemeral.Payload<M>) =>
          Schema.encodeEffect(profile.payloadSchema)(value).pipe(
            Effect.flatMap((encoded) => Schema.decodeUnknownEffect(Schema.Json)(encoded)),
            Effect.catchTag(
              "SchemaError",
              (cause) => Effect.fail(new Ephemeral.EncodeError({ definition: "member", cause }))
            ),
            Effect.flatMap((encoded) =>
              publishWire(Protocol.EphemeralUpdateMemberRequest.make({
                spaceId: target.spaceId,
                member: target.member,
                value: encoded
              })).pipe(
                Effect.andThen(Effect.sync(() => runtime.recordMemberValue(encoded))),
                Semaphore.withPermit(runtime.memberUpdates)
              )
            )
          )
        return {
          spaceId: target.spaceId,
          member: target.member,
          events,
          state,
          members,
          updateMember
        }
      }

      const openSession = Effect.fnUntraced(
        function*<M extends Ephemeral.AnyMember,>(profile: M, input: SessionOptions<M>) {
          const value = yield* Schema.encodeEffect(profile.payloadSchema)(input.value).pipe(
            Effect.flatMap((encoded) => Schema.decodeUnknownEffect(Schema.Json)(encoded)),
            Effect.catchTag("SchemaError", (cause) =>
              Effect.fail(new Ephemeral.EncodeError({ definition: "member", cause })))
          )
          const joinRequest = yield* normalizeJoinRequest({
            spaceId: input.spaceId,
            member: input.member,
            value,
            ttl: input.ttl
          })
          const holder = yield* Scope.fork(yield* Effect.scope)
          return yield* Effect.gen(function*() {
            const runtime = yield* RcMap.get(runtimes, new SessionIdentity(joinRequest)).pipe(
              Scope.provide(holder)
            )
            if (runtime.requestHash !== Canonical.hash(joinRequest)) {
              return yield* invalidConfiguration(
                "session",
                "An ephemeral session for this member is already active with a different value or ttl"
              )
            }
            yield* Deferred.await(runtime.ready)
            return makeSession(profile, { spaceId: input.spaceId, member: input.member }, runtime)
          }).pipe(Effect.onError((cause) =>
            Scope.close(holder, Exit.failCause(cause))
          ))
        }
      )

      const publish: Service["publish"] = Effect.fnUntraced(
        function*(
          definition: Ephemeral.Any,
          input: PublishTarget & {
            readonly payload?: unknown
            readonly ttl: Duration.Input
            readonly key?: unknown
          }
        ) {
          const value = yield* Schema.encodeEffect(definition.payloadSchema)(input.payload).pipe(
            Effect.flatMap((encoded) => Schema.decodeUnknownEffect(Schema.Json)(encoded)),
            Effect.catchTag("SchemaError", (cause) =>
              Effect.fail(new Ephemeral.EncodeError({ definition: definition.name, cause })))
          )
          if (definition.kind === "event") {
            const ttlMillis = yield* boundedTtlMillis(
              input.ttl,
              1,
              Protocol.maximumEphemeralEventTtlMillis
            )
            return yield* publishWire(Protocol.EphemeralEventRequest.make({
              spaceId: input.spaceId,
              member: input.member,
              channel: definition.name,
              value,
              ttlMillis
            }))
          }
          const key = yield* encodeStateKey(definition, input.key)
          const ttlMillis = yield* boundedTtlMillis(
            input.ttl,
            1,
            Protocol.maximumEphemeralStateTtlMillis
          )
          return yield* publishWire(Protocol.EphemeralSetStateRequest.make({
            spaceId: input.spaceId,
            member: input.member,
            channel: definition.name,
            key,
            value,
            ttlMillis
          }))
        }
      )

      const encodeStateKey = (definition: Ephemeral.AnyState, key: unknown) =>
        Schema.encodeEffect(definition.keySchema)(key).pipe(
          Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(new Ephemeral.EncodeError({ definition: definition.name, cause }))),
          Effect.flatMap((encoded) =>
            Schema.decodeUnknownEffect(Protocol.EphemeralKey)(encoded).pipe(
              Effect.catchTag("SchemaError", (cause) =>
                Effect.fail(new Ephemeral.EncodeError({ definition: definition.name, cause })))
            )
          )
        )

      const clear = (definition: Ephemeral.AnyEvent, target: PublishTarget) =>
        publishWire(Protocol.EphemeralClearEventRequest.make({
          spaceId: target.spaceId,
          member: target.member,
          channel: definition.name
        }))

      const remove = <D extends Ephemeral.AnyState,>(definition: D, input: StateRemoveOptions<D>) =>
        encodeStateKey(definition, input.key).pipe(
          Effect.flatMap((key) =>
            publishWire(Protocol.EphemeralRemoveStateRequest.make({
              spaceId: input.spaceId,
              member: input.member,
              channel: definition.name,
              key
            }))
          )
        )

      return EphemeralClient.of({
        session: openSession,
        publish,
        clear,
        remove
      })
    })
  )

export const layerWithOptions = (options?: Options): Layer.Layer<
  EphemeralClient,
  ReplicaError.InvalidConfiguration,
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication> | Transport.Transport
> => layerFromSession(options).pipe(Layer.provide(ProtocolSession.layerWithOptions(options)))

export const layer = layerFromSession().pipe(Layer.provide(ProtocolSession.layer))
