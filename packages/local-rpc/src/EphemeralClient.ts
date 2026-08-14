import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Fiber from "effect/Fiber"
import * as Hash from "effect/Hash"
import * as Layer from "effect/Layer"
import * as PubSub from "effect/PubSub"
import * as RcMap from "effect/RcMap"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import type * as RpcClient from "effect/unstable/rpc/RpcClient"
import type * as RpcMiddleware from "effect/unstable/rpc/RpcMiddleware"
import type * as Authentication from "./Authentication.js"
import { positiveFiniteDurationMillis } from "./internal/configuration.js"
import { invalidConfiguration } from "./internal/errors.js"
import * as ProtocolSessionRetry from "./internal/protocolSession.js"
import * as ProtocolSession from "./ProtocolSession.js"

export type JoinRequest = Omit<Protocol.EphemeralJoinRequest, "ttlMillis"> & {
  readonly ttl: Duration.Input
}

export type EventRequest = Omit<Protocol.EphemeralEventRequest, "ttlMillis"> & {
  readonly ttl: Duration.Input
}

export type SetStateRequest = Omit<Protocol.EphemeralSetStateRequest, "ttlMillis"> & {
  readonly ttl: Duration.Input
}

export type PublishRequest =
  | EventRequest
  | SetStateRequest
  | Protocol.EphemeralClearEventRequest
  | Protocol.EphemeralRemoveStateRequest
  | Protocol.EphemeralUpdateMemberRequest

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
  readonly join: (
    request: JoinRequest
  ) => Stream.Stream<Protocol.EphemeralMessage, ReplicaError.ReplicaError>
  readonly publishEncoded: (
    request: PublishRequest
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly heartbeat: (
    request: Protocol.EphemeralHeartbeatRequest
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
}

export class EphemeralClient extends Context.Service<EphemeralClient, Service>()(
  "@lucas-barake/effect-local-rpc/EphemeralClient"
) {}

export interface Options extends ProtocolSession.Options {
  readonly rpcTimeout?: Duration.Input
  readonly heartbeatInterval?: Duration.Input
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

const normalizeJoinRequest = Effect.fnUntraced(function*(request: JoinRequest) {
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

interface RawView {
  readonly members: ReadonlyArray<Protocol.EphemeralMemberEntry>
  readonly states: ReadonlyMap<string, Protocol.EphemeralStateEntry>
}

interface SessionRuntime {
  readonly requestHash: string
  readonly views: PubSub.PubSub<RawView>
  readonly events: PubSub.PubSub<Protocol.EphemeralEventEntry>
  readonly ready: Deferred.Deferred<void, ReplicaError.ReplicaError>
  readonly failure: Deferred.Deferred<never, ReplicaError.ReplicaError>
}

const sameMember = (left: Protocol.EphemeralMember, right: Protocol.EphemeralMember) =>
  left.clientId === right.clientId && left.membershipIncarnation === right.membershipIncarnation

const stateIdentity = (
  member: Protocol.EphemeralMember,
  channel: Protocol.EphemeralChannel,
  key: Protocol.EphemeralKey
) =>
  [member.clientId, member.membershipIncarnation, channel, key]
    .map((component) => `${component.length}:${component}`)
    .join("")

const reduceView = (
  current: RawView | undefined,
  message: Exclude<Protocol.EphemeralMessage, Protocol.EphemeralEvent | Protocol.EphemeralEventCleared>
): RawView | undefined => {
  if (message._tag === "Snapshot") {
    return {
      members: message.members,
      states: new Map(
        message.states.map((entry) => [stateIdentity(entry.member, entry.channel, entry.key), entry])
      )
    }
  }
  if (current === undefined) return undefined
  if (message._tag === "MemberUpserted") {
    return {
      ...current,
      members: [
        ...current.members.filter((entry) => !sameMember(entry.member, message.entry.member)),
        message.entry
      ]
    }
  }
  if (message._tag === "MemberLeft") {
    return {
      ...current,
      members: current.members.filter((entry) => !sameMember(entry.member, message.member))
    }
  }
  if (message._tag === "StateSet") {
    const states = new Map(current.states)
    states.set(stateIdentity(message.entry.member, message.entry.channel, message.entry.key), message.entry)
    return { ...current, states }
  }
  const states = new Map(current.states)
  states.delete(stateIdentity(message.member, message.channel, message.key))
  return { ...current, states }
}

const stateSlice = (view: RawView, channel: string): ReadonlyArray<Protocol.EphemeralStateEntry> => {
  const identities = [...view.states.entries()]
    .filter(([, entry]) => entry.channel === channel)
    .map(([identity]) => identity)
    .toSorted()
  return identities.flatMap((identity) => {
    const entry = view.states.get(identity)
    if (entry === undefined) return []
    return [entry]
  })
}

const memberSlice = (view: RawView): ReadonlyArray<Protocol.EphemeralMemberEntry> => {
  const keyed = new Map(
    view.members.map((entry) => [`${entry.member.clientId}:${entry.member.membershipIncarnation}`, entry])
  )
  return [...keyed.keys()].toSorted().flatMap((key) => {
    const entry = keyed.get(key)
    if (entry === undefined) return []
    return [entry]
  })
}

const changedSlice = <A,>(previous: string | undefined, slice: A) => {
  const fingerprint = Canonical.hash(slice)
  if (fingerprint === previous) return [previous, []] as const
  return [fingerprint, [slice]] as const
}

const normalizePublishRequest = Effect.fnUntraced(function*(request: PublishRequest) {
  if (request._tag === "Event") {
    const ttlMillis = yield* boundedTtlMillis(request.ttl, 1, Protocol.maximumEphemeralEventTtlMillis)
    return Protocol.EphemeralEventRequest.make({
      spaceId: request.spaceId,
      member: request.member,
      channel: request.channel,
      value: request.value,
      ttlMillis
    })
  }
  if (request._tag === "SetState") {
    const ttlMillis = yield* boundedTtlMillis(request.ttl, 1, Protocol.maximumEphemeralStateTtlMillis)
    return Protocol.EphemeralSetStateRequest.make({
      spaceId: request.spaceId,
      member: request.member,
      channel: request.channel,
      key: request.key,
      value: request.value,
      ttlMillis
    })
  }
  return request
})

export const layerFromSession = (
  options?: Pick<Options, "rpcTimeout" | "heartbeatInterval">
): Layer.Layer<EphemeralClient, ReplicaError.InvalidConfiguration, ProtocolSession.ProtocolSession> =>
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
      const session = yield* ProtocolSession.ProtocolSession
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
      ): Effect.Effect<ActiveSession, ReplicaError.EphemeralSessionUnavailable> => {
        const active = sessions.get(sessionKey(request))
        if (active !== undefined) return Effect.succeed(active)
        return Effect.fail(
          new ReplicaError.EphemeralSessionUnavailable({
            spaceId: request.spaceId,
            clientId: request.member.clientId,
            membershipIncarnation: request.member.membershipIncarnation
          })
        )
      }

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
          Effect.asVoid,
          Effect.withSpan("EphemeralClient.heartbeat", {
            attributes: {
              "space.id": request.spaceId,
              "client.id": request.member.clientId
            }
          })
        )

      const joinWire = (request: Protocol.EphemeralJoinRequest) =>
        ProtocolSessionRetry.runStream(
          session,
          (version) =>
            Stream.unwrap(Effect.gen(function*() {
              const owner = {}
              const started = yield* Deferred.make<Protocol.EphemeralSessionStarted>()
              const acquisition = yield* client.JoinEphemeral(
                { ...request, protocolVersion: version },
                { asQueue: true }
              ).pipe(Effect.forkScoped({ startImmediately: true }))
              const queue = yield* Fiber.join(acquisition).pipe(
                Effect.timeoutOrElse({
                  duration: rpcTimeoutMillis,
                  orElse: () =>
                    Effect.fail(
                      new ReplicaError.OperationTimeout({
                        operation: "JoinEphemeral",
                        timeoutMillis: rpcTimeoutMillis
                      })
                    )
                }),
                Effect.ensuring(Fiber.interrupt(acquisition))
              )
              const messages = Stream.fromQueue(queue).pipe(
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
                )
              )
              const visible = messages.pipe(
                Stream.tap((message) => {
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
              return visible.pipe(
                Stream.mergeEffect(heartbeatLoop),
                Stream.ensuring(Effect.sync(() => {
                  if (sessions.get(sessionKey(request))?.owner === owner) {
                    sessions.delete(sessionKey(request))
                  }
                }))
              )
            })).pipe(Stream.repeat(Schedule.forever))
        ).pipe(
          Stream.withSpan("EphemeralClient.join", {
            attributes: {
              "space.id": request.spaceId,
              "client.id": request.member.clientId
            }
          })
        )

      const runtimes = yield* RcMap.make({
        lookup: Effect.fnUntraced(function*(identity: SessionIdentity) {
          const views = yield* PubSub.unbounded<RawView>({ replay: 1 })
          const events = yield* PubSub.unbounded<Protocol.EphemeralEventEntry>()
          const ready = yield* Deferred.make<void, ReplicaError.ReplicaError>()
          const failure = yield* Deferred.make<never, ReplicaError.ReplicaError>()
          let view: RawView | undefined
          const consume = (message: Protocol.EphemeralMessage) => {
            if (message._tag === "Event") return PubSub.publish(events, message.entry).pipe(Effect.asVoid)
            if (message._tag === "EventCleared") return Effect.void
            const next = reduceView(view, message)
            if (next === undefined) return Effect.void
            view = next
            return PubSub.publish(views, next).pipe(
              Effect.andThen(Deferred.succeed(ready, undefined)),
              Effect.asVoid
            )
          }
          yield* joinWire(identity.request).pipe(
            Stream.runForEach(consume),
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
              Effect.andThen(PubSub.shutdown(events))
            )
          })
          const runtime: SessionRuntime = {
            requestHash: Canonical.hash(identity.request),
            views,
            events,
            ready,
            failure
          }
          return runtime
        })
      })

      const decodeFailure = (definition: string) => (cause: Schema.SchemaError) =>
        Effect.fail(new Ephemeral.DecodeError({ definition, cause }))

      const encodeFailure = (definition: string) => (cause: Schema.SchemaError) =>
        Effect.fail(new Ephemeral.EncodeError({ definition, cause }))

      const makeSession = <M extends Ephemeral.AnyMember,>(
        profile: M,
        target: PublishTarget,
        runtime: SessionRuntime
      ): Session<M> => {
        const failureStream = Stream.fromEffect(Deferred.await(runtime.failure))
        const events = (definition: Ephemeral.AnyEvent) =>
          Stream.fromPubSub(runtime.events).pipe(
            Stream.filter((entry) => entry.channel === definition.name),
            Stream.mapEffect((entry) =>
              Schema.decodeUnknownEffect(definition.payloadSchema)(entry.value).pipe(
                Effect.catchTag("SchemaError", decodeFailure(definition.name)),
                Effect.map((payload) => ({ member: entry.member, payload }))
              )
            ),
            Stream.merge(failureStream)
          )
        const state = (definition: Ephemeral.AnyState) =>
          Stream.fromPubSub(runtime.views).pipe(
            Stream.map((current) => stateSlice(current, definition.name)),
            Stream.mapAccum((): string | undefined => undefined, changedSlice),
            Stream.mapEffect(Effect.forEach((entry) =>
              Effect.all({
                key: Schema.decodeUnknownEffect(definition.keySchema)(entry.key),
                value: Schema.decodeUnknownEffect(definition.payloadSchema)(entry.value)
              }).pipe(
                Effect.catchTag("SchemaError", decodeFailure(definition.name)),
                Effect.map(({ key, value }) => ({
                  member: entry.member,
                  key,
                  value,
                  expiresAtMillis: entry.expiresAtMillis
                }))
              )
            )),
            Stream.merge(failureStream)
          )
        const members = Stream.fromPubSub(runtime.views).pipe(
          Stream.map(memberSlice),
          Stream.mapAccum((): string | undefined => undefined, changedSlice),
          Stream.mapEffect(Effect.forEach((entry) =>
            Schema.decodeUnknownEffect(profile.payloadSchema)(entry.value).pipe(
              Effect.catchTag("SchemaError", decodeFailure("member")),
              Effect.map((value) => ({
                member: entry.member,
                value,
                expiresAtMillis: entry.expiresAtMillis
              }))
            )
          )),
          Stream.merge(failureStream)
        )
        const updateMember = (value: Ephemeral.Payload<M>) =>
          Schema.encodeEffect(profile.payloadSchema)(value).pipe(
            Effect.catchTag("SchemaError", encodeFailure("member")),
            Effect.flatMap((encoded) =>
              publishWire(Protocol.EphemeralUpdateMemberRequest.make({
                spaceId: target.spaceId,
                member: target.member,
                value: encoded
              }))
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
            Effect.catchTag("SchemaError", encodeFailure("member"))
          )
          const joinRequest = yield* normalizeJoinRequest({
            spaceId: input.spaceId,
            member: input.member,
            value,
            ttl: input.ttl
          })
          const runtime = yield* RcMap.get(runtimes, new SessionIdentity(joinRequest))
          if (runtime.requestHash !== Canonical.hash(joinRequest)) {
            return yield* invalidConfiguration(
              "session",
              "An ephemeral session for this member is already active with a different value or ttl"
            )
          }
          yield* Deferred.await(runtime.ready)
          return makeSession(profile, { spaceId: input.spaceId, member: input.member }, runtime)
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
            Effect.catchTag("SchemaError", encodeFailure(definition.name))
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
          Effect.catchTag("SchemaError", encodeFailure(definition.name)),
          Effect.flatMap((encoded) =>
            Schema.decodeUnknownEffect(Protocol.EphemeralKey)(encoded).pipe(
              Effect.catchTag("SchemaError", encodeFailure(definition.name))
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
        remove,
        publishEncoded: (request) => normalizePublishRequest(request).pipe(Effect.flatMap(publishWire)),
        heartbeat,
        join: (request) => Stream.unwrap(normalizeJoinRequest(request).pipe(Effect.map(joinWire)))
      })
    })
  )

export const layerWithOptions = (options?: Options): Layer.Layer<
  EphemeralClient,
  ReplicaError.InvalidConfiguration,
  RpcClient.Protocol | RpcMiddleware.ForClient<Authentication.Authentication>
> => layerFromSession(options).pipe(Layer.provide(ProtocolSession.layerWithOptions(options)))

export const layer = layerFromSession().pipe(Layer.provide(ProtocolSession.layer))
