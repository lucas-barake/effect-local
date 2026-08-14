import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as FiberMap from "effect/FiberMap"
import * as Layer from "effect/Layer"
import * as Metric from "effect/Metric"
import * as PubSub from "effect/PubSub"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import { positiveFiniteDurationMillis } from "./internal/configuration.js"
import { failCapacityExceeded, failInvalidConfiguration } from "./internal/errors.js"

export const JoinAuthorization = Schema.TaggedStruct("Join", {
  spaceId: Identity.SpaceId,
  member: Protocol.EphemeralMember,
  principal: Schema.Json
})

export const PublishAuthorization = Schema.TaggedStruct("Publish", {
  spaceId: Identity.SpaceId,
  member: Protocol.EphemeralMember,
  principal: Schema.Json
})

export const HeartbeatAuthorization = Schema.TaggedStruct("Heartbeat", {
  spaceId: Identity.SpaceId,
  member: Protocol.EphemeralMember,
  principal: Schema.Json
})

export const AuthorizationInput = Schema.Union([
  JoinAuthorization,
  PublishAuthorization,
  HeartbeatAuthorization
])
export type AuthorizationInput = typeof AuthorizationInput.Type

export interface Service {
  readonly join: (
    request: Protocol.EphemeralJoinRequest,
    principal: typeof Schema.Json.Type
  ) => Stream.Stream<Protocol.EphemeralMessage, ReplicaError.ReplicaError>
  readonly publish: (
    request: Protocol.EphemeralPublishRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly heartbeat: (
    request: Protocol.EphemeralHeartbeatRequest,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
}

export class EphemeralHub extends Context.Service<EphemeralHub, Service>()(
  "@lucas-barake/effect-local-rpc/EphemeralHub"
) {}

export interface Options {
  readonly capacity?: number
  readonly maximumSpaces?: number
  readonly maximumWatchersPerSpace: number
  readonly maximumMembersPerSpace?: number
  readonly maximumEventKeysPerMember?: number
  readonly maximumEventKeysPerSpace?: number
  readonly maximumStateKeysPerMember?: number
  readonly maximumStateKeysPerSpace?: number
  readonly maximumBytesPerMember?: number
  readonly maximumBytesPerSpace?: number
  readonly memberTtl?: Duration.Input
  readonly maximumEventTtl?: Duration.Input
  readonly maximumStateTtl?: Duration.Input
  readonly spaceIdleTtl?: Duration.Input
}

interface ResolvedOptions {
  readonly capacity: number
  readonly maximumSpaces: number
  readonly maximumWatchersPerSpace: number
  readonly maximumMembersPerSpace: number
  readonly maximumEventKeysPerMember: number
  readonly maximumEventKeysPerSpace: number
  readonly maximumStateKeysPerMember: number
  readonly maximumStateKeysPerSpace: number
  readonly maximumBytesPerMember: number
  readonly maximumBytesPerSpace: number
  readonly memberTtlMillis: number
  readonly maximumEventTtlMillis: number
  readonly maximumStateTtlMillis: number
  readonly spaceIdleTtlMillis: number
}

interface MemberRecord {
  readonly entry: Protocol.EphemeralMemberEntry
  readonly bytes: number
  readonly token: object
}

interface StateRecord {
  readonly entry: Protocol.EphemeralStateEntry
  readonly bytes: number
  readonly token: object
}

interface EventRecord {
  readonly member: Protocol.EphemeralMember
  readonly channel: Protocol.EphemeralChannel
  readonly token: object
}

interface SpaceRuntime {
  readonly spaceId: Identity.SpaceId
  readonly gate: Semaphore.Semaphore
  readonly watcherPermits: Semaphore.Semaphore
  readonly members: Map<string, MemberRecord>
  readonly states: Map<string, StateRecord>
  readonly events: Map<string, EventRecord>
  readonly memberBytes: Map<string, number>
  readonly memberStateKeys: Map<string, number>
  readonly memberEventKeys: Map<string, number>
  readonly memberTimers: FiberMap.FiberMap<string>
  readonly stateTimers: FiberMap.FiberMap<string>
  readonly eventTimers: FiberMap.FiberMap<string>
  channel: PubSub.PubSub<Protocol.EphemeralMessage>
  revision: number
  spaceBytes: number
}

const memberKey = (member: Protocol.EphemeralMember): string => `${member.clientId}:${member.membershipIncarnation}`

const stateKey = (
  member: Protocol.EphemeralMember,
  channel: Protocol.EphemeralChannel,
  key: Protocol.EphemeralKey
): string =>
  [member.clientId, member.membershipIncarnation, channel, key]
    .map((component) => `${component.length}:${component}`)
    .join("")

const eventKey = (member: Protocol.EphemeralMember, channel: Protocol.EphemeralChannel): string =>
  [member.clientId, member.membershipIncarnation, channel]
    .map((component) => `${component.length}:${component}`)
    .join("")

const nextRevision = (runtime: SpaceRuntime): Identity.EphemeralRevision => {
  runtime.revision += 1
  return Identity.EphemeralRevision.make(runtime.revision)
}

const positiveSafeInteger = Effect.fnUntraced(function*(option: string, value: number) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    return yield* failInvalidConfiguration(option, `${option} must be a positive safe integer`)
  }
  return value
})

const resolveOptions = Effect.fnUntraced(function*(options: Options) {
  const counts = {
    capacity: options.capacity ?? 1_024,
    maximumSpaces: options.maximumSpaces ?? 1_024,
    maximumWatchersPerSpace: options.maximumWatchersPerSpace,
    maximumMembersPerSpace: options.maximumMembersPerSpace ?? 1_024,
    maximumEventKeysPerMember: options.maximumEventKeysPerMember ?? 64,
    maximumEventKeysPerSpace: options.maximumEventKeysPerSpace ?? 4_096,
    maximumStateKeysPerMember: options.maximumStateKeysPerMember ?? 256,
    maximumStateKeysPerSpace: options.maximumStateKeysPerSpace ?? 16_384,
    maximumBytesPerMember: options.maximumBytesPerMember ?? 1024 * 1024,
    maximumBytesPerSpace: options.maximumBytesPerSpace ?? 16 * 1024 * 1024
  }
  for (const [option, value] of Object.entries(counts)) {
    yield* positiveSafeInteger(option, value)
  }
  const resolved = {
    ...counts,
    memberTtlMillis: yield* positiveFiniteDurationMillis(
      "memberTtl",
      options.memberTtl ?? Protocol.maximumEphemeralMemberTtlMillis
    ),
    maximumEventTtlMillis: yield* positiveFiniteDurationMillis(
      "maximumEventTtl",
      options.maximumEventTtl ?? Protocol.maximumEphemeralEventTtlMillis
    ),
    maximumStateTtlMillis: yield* positiveFiniteDurationMillis(
      "maximumStateTtl",
      options.maximumStateTtl ?? Protocol.maximumEphemeralStateTtlMillis
    ),
    spaceIdleTtlMillis: yield* positiveFiniteDurationMillis(
      "spaceIdleTtl",
      options.spaceIdleTtl ?? Protocol.maximumEphemeralStateTtlMillis
    )
  } satisfies ResolvedOptions
  if (resolved.memberTtlMillis > Protocol.maximumEphemeralMemberTtlMillis) {
    return yield* failInvalidConfiguration(
      "memberTtl",
      `memberTtl must not exceed ${Protocol.maximumEphemeralMemberTtlMillis} milliseconds`
    )
  }
  if (resolved.maximumEventTtlMillis > Protocol.maximumEphemeralEventTtlMillis) {
    return yield* failInvalidConfiguration(
      "maximumEventTtl",
      `maximumEventTtl must not exceed ${Protocol.maximumEphemeralEventTtlMillis} milliseconds`
    )
  }
  if (resolved.maximumStateTtlMillis > Protocol.maximumEphemeralStateTtlMillis) {
    return yield* failInvalidConfiguration(
      "maximumStateTtl",
      `maximumStateTtl must not exceed ${Protocol.maximumEphemeralStateTtlMillis} milliseconds`
    )
  }
  if (resolved.spaceIdleTtlMillis < resolved.maximumStateTtlMillis) {
    return yield* failInvalidConfiguration(
      "spaceIdleTtl",
      "spaceIdleTtl must be at least maximumStateTtl"
    )
  }
  return resolved
})

const ensurePayloadSize = Effect.fnUntraced(function*(value: unknown) {
  if ((yield* Protocol.encodedBytesEffect(value)) > Protocol.maximumEphemeralPayloadBytes) {
    yield* failCapacityExceeded("ephemeral payload bytes", Protocol.maximumEphemeralPayloadBytes)
  }
})

const ensureSession = (runtime: SpaceRuntime, member: Protocol.EphemeralMember) => {
  if (runtime.members.has(memberKey(member))) return Effect.void
  return Effect.fail(
    new ReplicaError.EphemeralSessionUnavailable({
      spaceId: runtime.spaceId,
      clientId: member.clientId,
      membershipIncarnation: member.membershipIncarnation
    })
  )
}

const publishDelta = Effect.fnUntraced(function*(
  runtime: SpaceRuntime,
  options: ResolvedOptions,
  message: Protocol.EphemeralMessage
) {
  if (!(yield* PubSub.publish(runtime.channel, message))) {
    const previous = runtime.channel
    runtime.channel = yield* PubSub.dropping<Protocol.EphemeralMessage>(options.capacity)
    yield* PubSub.shutdown(previous)
  }
})

const withGate = <A, E extends { readonly _tag: string }, R,>(
  runtime: SpaceRuntime,
  effect: Effect.Effect<A, E, R>
) => Semaphore.withPermit(runtime.gate, Effect.uninterruptible(effect))

const removeMember = (
  runtime: SpaceRuntime,
  options: ResolvedOptions,
  member: Protocol.EphemeralMember,
  token: object,
  cancelTimer: boolean
) =>
  withGate(
    runtime,
    Effect.gen(function*() {
      const key = memberKey(member)
      const current = runtime.members.get(key)
      if (current?.token !== token) return
      runtime.members.delete(key)
      runtime.spaceBytes -= current.bytes
      runtime.memberBytes.set(key, (runtime.memberBytes.get(key) ?? 0) - current.bytes)
      if (cancelTimer) yield* FiberMap.remove(runtime.memberTimers, key)
      yield* publishDelta(
        runtime,
        options,
        Protocol.EphemeralMemberLeft.make({
          spaceId: runtime.spaceId,
          revision: nextRevision(runtime),
          member
        })
      )
    })
  )

const removeState = (
  runtime: SpaceRuntime,
  options: ResolvedOptions,
  member: Protocol.EphemeralMember,
  channel: Protocol.EphemeralChannel,
  key: Protocol.EphemeralKey,
  token: object | undefined,
  cancelTimer: boolean,
  requireSession: boolean
) =>
  withGate(
    runtime,
    Effect.gen(function*() {
      if (requireSession) yield* ensureSession(runtime, member)
      const identity = stateKey(member, channel, key)
      const current = runtime.states.get(identity)
      if (current === undefined || (token !== undefined && current.token !== token)) return
      runtime.states.delete(identity)
      runtime.spaceBytes -= current.bytes
      const owner = memberKey(member)
      runtime.memberBytes.set(owner, (runtime.memberBytes.get(owner) ?? 0) - current.bytes)
      runtime.memberStateKeys.set(owner, (runtime.memberStateKeys.get(owner) ?? 1) - 1)
      if (cancelTimer) yield* FiberMap.remove(runtime.stateTimers, identity)
      yield* publishDelta(
        runtime,
        options,
        Protocol.EphemeralStateRemoved.make({
          spaceId: runtime.spaceId,
          revision: nextRevision(runtime),
          member,
          channel,
          key
        })
      )
    })
  )

const clearEvent = (
  runtime: SpaceRuntime,
  options: ResolvedOptions,
  member: Protocol.EphemeralMember,
  channel: Protocol.EphemeralChannel,
  token: object | undefined,
  cancelTimer: boolean,
  requireSession: boolean
) =>
  withGate(
    runtime,
    Effect.gen(function*() {
      if (requireSession) yield* ensureSession(runtime, member)
      const identity = eventKey(member, channel)
      const current = runtime.events.get(identity)
      if (current === undefined || (token !== undefined && current.token !== token)) return
      runtime.events.delete(identity)
      const owner = memberKey(member)
      runtime.memberEventKeys.set(owner, (runtime.memberEventKeys.get(owner) ?? 1) - 1)
      if (cancelTimer) yield* FiberMap.remove(runtime.eventTimers, identity)
      yield* publishDelta(
        runtime,
        options,
        Protocol.EphemeralEventCleared.make({
          spaceId: runtime.spaceId,
          revision: nextRevision(runtime),
          member,
          channel
        })
      )
    })
  )

const makeRuntime = (spaceId: Identity.SpaceId, options: ResolvedOptions) =>
  Effect.acquireRelease(
    Effect.gen(function*() {
      const channel = yield* PubSub.dropping<Protocol.EphemeralMessage>(options.capacity)
      return {
        spaceId,
        gate: yield* Semaphore.make(1),
        watcherPermits: yield* Semaphore.make(options.maximumWatchersPerSpace),
        members: new Map(),
        states: new Map(),
        events: new Map(),
        memberBytes: new Map(),
        memberStateKeys: new Map(),
        memberEventKeys: new Map(),
        memberTimers: yield* FiberMap.make<string>(),
        stateTimers: yield* FiberMap.make<string>(),
        eventTimers: yield* FiberMap.make<string>(),
        channel,
        revision: 0,
        spaceBytes: 0
      } satisfies SpaceRuntime
    }),
    (runtime) => PubSub.shutdown(runtime.channel)
  )

const acquireWatcher = (runtime: SpaceRuntime, options: ResolvedOptions) =>
  Effect.acquireRelease(
    Effect.gen(function*() {
      if (!(yield* Semaphore.takeIfAvailable(runtime.watcherPermits, 1))) {
        yield* failCapacityExceeded("ephemeral watchers", options.maximumWatchersPerSpace)
      }
    }),
    () => Semaphore.release(runtime.watcherPermits, 1)
  )

export const layer = <R = never,>(
  options: Options & {
    readonly authorize: (
      input: AuthorizationInput
    ) => Effect.Effect<void, ReplicaError.AuthorizationDenied, R>
  }
): Layer.Layer<EphemeralHub, ReplicaError.InvalidConfiguration, R> =>
  Layer.effect(
    EphemeralHub,
    Effect.gen(function*() {
      const context = yield* Effect.context<R>()
      const resolved = yield* resolveOptions(options)
      const watcherCount = Metric.gauge("effect_local_server_ephemeral_watcher_count")
      const spaces = yield* RcMap.make({
        lookup: (spaceId: Identity.SpaceId) => makeRuntime(spaceId, resolved),
        capacity: resolved.maximumSpaces,
        idleTimeToLive: resolved.spaceIdleTtlMillis
      })
      const acquireSpace = (spaceId: Identity.SpaceId) =>
        RcMap.get(spaces, spaceId).pipe(
          Effect.mapError((error) => {
            if (Cause.isExceededCapacityError(error)) {
              return new ReplicaError.CapacityExceeded({
                resource: "ephemeral spaces",
                limit: resolved.maximumSpaces
              })
            }
            return error
          })
        )
      const authorize = (input: AuthorizationInput) => options.authorize(input).pipe(Effect.provide(context))

      const service = EphemeralHub.of({
        join: (request, principal) =>
          Stream.unwrap(Effect.gen(function*() {
            yield* authorize(JoinAuthorization.make({
              spaceId: request.spaceId,
              member: request.member,
              principal
            }))
            yield* ensurePayloadSize(request)
            const runtime = yield* acquireSpace(request.spaceId)
            yield* acquireWatcher(runtime, resolved)
            yield* Effect.acquireRelease(
              Metric.modify(watcherCount, 1),
              () => Metric.modify(watcherCount, -1)
            )
            const token = {}
            const acquireJoin = withGate(
              runtime,
              Effect.gen(function*() {
                const key = memberKey(request.member)
                const previous = runtime.members.get(key)
                if (previous === undefined && runtime.members.size >= resolved.maximumMembersPerSpace) {
                  return yield* failCapacityExceeded("ephemeral members", resolved.maximumMembersPerSpace)
                }
                const bytes = yield* Protocol.encodedBytesEffect(request.value)
                const previousBytes = previous?.bytes ?? 0
                const memberBytes = (runtime.memberBytes.get(key) ?? 0) - previousBytes + bytes
                if (memberBytes > resolved.maximumBytesPerMember) {
                  return yield* failCapacityExceeded("ephemeral bytes per member", resolved.maximumBytesPerMember)
                }
                const spaceBytes = runtime.spaceBytes - previousBytes + bytes
                if (spaceBytes > resolved.maximumBytesPerSpace) {
                  return yield* failCapacityExceeded("ephemeral bytes per space", resolved.maximumBytesPerSpace)
                }
                const subscription = yield* PubSub.subscribe(runtime.channel)
                const now = yield* Clock.currentTimeMillis
                const ttlMillis = Math.min(request.ttlMillis, resolved.memberTtlMillis)
                const entry = Protocol.EphemeralMemberEntry.make({
                  member: request.member,
                  value: request.value,
                  expiresAtMillis: now + ttlMillis
                })
                runtime.members.set(key, { entry, bytes, token })
                runtime.memberBytes.set(key, memberBytes)
                runtime.spaceBytes = spaceBytes
                const revision = nextRevision(runtime)
                const expireMember = removeMember(runtime, resolved, request.member, token, false)
                yield* FiberMap.run(
                  runtime.memberTimers,
                  key,
                  Effect.sleep(ttlMillis).pipe(Effect.andThen(expireMember))
                )
                yield* publishDelta(
                  runtime,
                  resolved,
                  Protocol.EphemeralMemberUpserted.make({
                    spaceId: request.spaceId,
                    revision,
                    entry
                  })
                )
                return {
                  snapshot: Protocol.EphemeralSnapshot.make({
                    spaceId: request.spaceId,
                    revision,
                    members: Array.from(runtime.members.values(), (record) => record.entry),
                    states: Array.from(runtime.states.values(), (record) => record.entry)
                  }),
                  subscription
                }
              })
            )
            const acquired = yield* Effect.acquireRelease(
              acquireJoin,
              () => removeMember(runtime, resolved, request.member, token, true)
            )
            return Stream.concat(
              Stream.make(acquired.snapshot),
              Stream.fromSubscription(acquired.subscription).pipe(
                Stream.filter((message) => message.revision > acquired.snapshot.revision)
              )
            )
          })).pipe(
            Stream.withSpan("EphemeralHub.join", {
              attributes: { "ephemeral.space_id": request.spaceId }
            })
          ),
        publish: Effect.fn("EphemeralHub.publish")(function*(request, principal) {
          yield* authorize(PublishAuthorization.make({
            spaceId: request.spaceId,
            member: request.member,
            principal
          }))
          yield* ensurePayloadSize(request)
          if (!(yield* RcMap.has(spaces, request.spaceId))) {
            yield* new ReplicaError.EphemeralSessionUnavailable({
              spaceId: request.spaceId,
              clientId: request.member.clientId,
              membershipIncarnation: request.member.membershipIncarnation
            })
          }
          yield* Effect.scoped(Effect.gen(function*() {
            const runtime = yield* acquireSpace(request.spaceId)
            yield* ensureSession(runtime, request.member)
            if (request._tag === "ClearEvent") {
              yield* clearEvent(runtime, resolved, request.member, request.channel, undefined, true, true)
            } else if (request._tag === "RemoveState") {
              yield* removeState(
                runtime,
                resolved,
                request.member,
                request.channel,
                request.key,
                undefined,
                true,
                true
              )
            } else {
              yield* withGate(
                runtime,
                Effect.gen(function*() {
                  yield* ensureSession(runtime, request.member)
                  const owner = memberKey(request.member)
                  if (request._tag === "UpdateMember") {
                    const current = runtime.members.get(owner)
                    if (current === undefined) return yield* ensureSession(runtime, request.member)
                    const bytes = yield* Protocol.encodedBytesEffect(request.value)
                    const ownerBytes = (runtime.memberBytes.get(owner) ?? 0) - current.bytes + bytes
                    if (ownerBytes > resolved.maximumBytesPerMember) {
                      return yield* failCapacityExceeded("ephemeral bytes per member", resolved.maximumBytesPerMember)
                    }
                    const spaceBytes = runtime.spaceBytes - current.bytes + bytes
                    if (spaceBytes > resolved.maximumBytesPerSpace) {
                      return yield* failCapacityExceeded("ephemeral bytes per space", resolved.maximumBytesPerSpace)
                    }
                    const entry = Protocol.EphemeralMemberEntry.make({
                      ...current.entry,
                      value: request.value
                    })
                    runtime.members.set(owner, { ...current, entry, bytes })
                    runtime.memberBytes.set(owner, ownerBytes)
                    runtime.spaceBytes = spaceBytes
                    return yield* publishDelta(
                      runtime,
                      resolved,
                      Protocol.EphemeralMemberUpserted.make({
                        spaceId: request.spaceId,
                        revision: nextRevision(runtime),
                        entry
                      })
                    )
                  }
                  if (request._tag === "Event") {
                    const now = yield* Clock.currentTimeMillis
                    const ttlMillis = Math.min(request.ttlMillis, resolved.maximumEventTtlMillis)
                    const token = {}
                    const identity = eventKey(request.member, request.channel)
                    const previous = runtime.events.get(identity)
                    const ownerEventKeys = runtime.memberEventKeys.get(owner) ?? 0
                    if (previous === undefined && ownerEventKeys >= resolved.maximumEventKeysPerMember) {
                      return yield* failCapacityExceeded(
                        "ephemeral event keys per member",
                        resolved.maximumEventKeysPerMember
                      )
                    }
                    if (previous === undefined && runtime.events.size >= resolved.maximumEventKeysPerSpace) {
                      return yield* failCapacityExceeded(
                        "ephemeral event keys per space",
                        resolved.maximumEventKeysPerSpace
                      )
                    }
                    runtime.events.set(identity, { member: request.member, channel: request.channel, token })
                    let nextOwnerEventKeys = ownerEventKeys
                    if (previous === undefined) nextOwnerEventKeys += 1
                    runtime.memberEventKeys.set(owner, nextOwnerEventKeys)
                    const expireEvent = clearEvent(
                      runtime,
                      resolved,
                      request.member,
                      request.channel,
                      token,
                      false,
                      false
                    )
                    yield* FiberMap.run(
                      runtime.eventTimers,
                      identity,
                      Effect.sleep(ttlMillis).pipe(Effect.andThen(expireEvent))
                    )
                    return yield* publishDelta(
                      runtime,
                      resolved,
                      Protocol.EphemeralEvent.make({
                        spaceId: request.spaceId,
                        revision: nextRevision(runtime),
                        entry: Protocol.EphemeralEventEntry.make({
                          member: request.member,
                          channel: request.channel,
                          value: request.value,
                          expiresAtMillis: now + ttlMillis
                        })
                      })
                    )
                  }
                  const identity = stateKey(request.member, request.channel, request.key)
                  const previous = runtime.states.get(identity)
                  const ownerStateKeys = runtime.memberStateKeys.get(owner) ?? 0
                  if (previous === undefined && ownerStateKeys >= resolved.maximumStateKeysPerMember) {
                    return yield* failCapacityExceeded(
                      "ephemeral state keys per member",
                      resolved.maximumStateKeysPerMember
                    )
                  }
                  if (previous === undefined && runtime.states.size >= resolved.maximumStateKeysPerSpace) {
                    return yield* failCapacityExceeded(
                      "ephemeral state keys per space",
                      resolved.maximumStateKeysPerSpace
                    )
                  }
                  const bytes = yield* Protocol.encodedBytesEffect(request.value)
                  const previousBytes = previous?.bytes ?? 0
                  const ownerBytes = (runtime.memberBytes.get(owner) ?? 0) - previousBytes + bytes
                  if (ownerBytes > resolved.maximumBytesPerMember) {
                    return yield* failCapacityExceeded("ephemeral bytes per member", resolved.maximumBytesPerMember)
                  }
                  const spaceBytes = runtime.spaceBytes - previousBytes + bytes
                  if (spaceBytes > resolved.maximumBytesPerSpace) {
                    return yield* failCapacityExceeded("ephemeral bytes per space", resolved.maximumBytesPerSpace)
                  }
                  const now = yield* Clock.currentTimeMillis
                  const ttlMillis = Math.min(request.ttlMillis, resolved.maximumStateTtlMillis)
                  const token = {}
                  const entry = Protocol.EphemeralStateEntry.make({
                    member: request.member,
                    channel: request.channel,
                    key: request.key,
                    value: request.value,
                    expiresAtMillis: now + ttlMillis
                  })
                  runtime.states.set(identity, { entry, bytes, token })
                  runtime.memberBytes.set(owner, ownerBytes)
                  let nextOwnerStateKeys = ownerStateKeys
                  if (previous === undefined) nextOwnerStateKeys += 1
                  runtime.memberStateKeys.set(owner, nextOwnerStateKeys)
                  runtime.spaceBytes = spaceBytes
                  const expireState = removeState(
                    runtime,
                    resolved,
                    request.member,
                    request.channel,
                    request.key,
                    token,
                    false,
                    false
                  )
                  yield* FiberMap.run(
                    runtime.stateTimers,
                    identity,
                    Effect.sleep(ttlMillis).pipe(Effect.andThen(expireState))
                  )
                  yield* publishDelta(
                    runtime,
                    resolved,
                    Protocol.EphemeralStateSet.make({
                      spaceId: request.spaceId,
                      revision: nextRevision(runtime),
                      entry
                    })
                  )
                  return yield* Effect.void
                })
              )
            }
          }))
        }),
        heartbeat: Effect.fn("EphemeralHub.heartbeat")(function*(request, principal) {
          yield* authorize(HeartbeatAuthorization.make({
            spaceId: request.spaceId,
            member: request.member,
            principal
          }))
          if (!(yield* RcMap.has(spaces, request.spaceId))) {
            yield* new ReplicaError.EphemeralSessionUnavailable({
              spaceId: request.spaceId,
              clientId: request.member.clientId,
              membershipIncarnation: request.member.membershipIncarnation
            })
          }
          yield* Effect.scoped(Effect.gen(function*() {
            const runtime = yield* acquireSpace(request.spaceId)
            yield* withGate(
              runtime,
              Effect.gen(function*() {
                const key = memberKey(request.member)
                const current = runtime.members.get(key)
                if (current === undefined) return yield* ensureSession(runtime, request.member)
                const now = yield* Clock.currentTimeMillis
                const entry = Protocol.EphemeralMemberEntry.make({
                  ...current.entry,
                  expiresAtMillis: now + resolved.memberTtlMillis
                })
                runtime.members.set(key, { ...current, entry })
                yield* FiberMap.run(
                  runtime.memberTimers,
                  key,
                  Effect.sleep(resolved.memberTtlMillis).pipe(
                    Effect.andThen(removeMember(runtime, resolved, request.member, current.token, false))
                  )
                )
                return yield* Effect.void
              })
            )
          }))
        })
      })
      return service
    })
  )

export const layerTrusted = (options: Options): Layer.Layer<EphemeralHub, ReplicaError.InvalidConfiguration> =>
  layer({ ...options, authorize: () => Effect.void })
