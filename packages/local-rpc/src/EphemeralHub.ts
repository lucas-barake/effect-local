import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
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
import { capacityExceeded, invalidConfiguration } from "./internal/errors.js"

export const JoinAuthorization = Schema.TaggedStruct("Join", {
  spaceId: Identity.SpaceId,
  member: Protocol.EphemeralMember,
  principal: Schema.Json
})

export const PublishAuthorization = Schema.TaggedStruct("Publish", {
  spaceId: Identity.SpaceId,
  member: Protocol.EphemeralMember,
  operation: Schema.Literals(["Event", "ClearEvent", "SetState", "RemoveState", "UpdateMember"]),
  channel: Schema.NullOr(Protocol.EphemeralChannel),
  key: Schema.NullOr(Protocol.EphemeralKey),
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
  ) => Stream.Stream<Protocol.EphemeralJoinMessage, ReplicaError.ReplicaError>
  readonly publish: (
    request: Protocol.EphemeralPublishRequest,
    sessionToken: Identity.EphemeralSessionToken,
    principal: typeof Schema.Json.Type
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly heartbeat: (
    request: Protocol.EphemeralHeartbeatRequest,
    sessionToken: Identity.EphemeralSessionToken,
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
  readonly maximumSnapshotBytes?: number
  readonly memberTtl?: Duration.Input
  readonly authorizationRefreshInterval?: Duration.Input
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
  readonly maximumSnapshotBytes: number
  readonly memberTtlMillis: number
  readonly authorizationRefreshIntervalMillis: number
  readonly maximumEventTtlMillis: number
  readonly maximumStateTtlMillis: number
  readonly spaceIdleTtlMillis: number
}

interface MemberRecord {
  readonly entry: Protocol.EphemeralMemberEntry
  readonly bytes: number
  readonly entryBytes: number
  readonly token: object
  readonly sessionToken: Identity.EphemeralSessionToken
  readonly departed: Deferred.Deferred<void, ReplicaError.EphemeralSessionUnavailable>
  readonly leaseMillis: number
}

interface StateRecord {
  readonly entry: Protocol.EphemeralStateEntry
  readonly bytes: number
  readonly entryBytes: number
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
  readonly channel: PubSub.PubSub<Protocol.EphemeralMessage>
  revision: number
  spaceBytes: number
  snapshotEntriesBytes: number
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
    return yield* invalidConfiguration(option, `${option} must be a positive safe integer`)
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
    maximumBytesPerSpace: options.maximumBytesPerSpace ?? 16 * 1024 * 1024,
    maximumSnapshotBytes: options.maximumSnapshotBytes ?? Protocol.maximumBatchBytes
  }
  for (const [option, value] of Object.entries(counts)) {
    yield* positiveSafeInteger(option, value)
  }
  const memberTtlMillis = yield* positiveFiniteDurationMillis(
    "memberTtl",
    options.memberTtl ?? Protocol.maximumEphemeralMemberTtlMillis
  )
  const resolved = {
    ...counts,
    memberTtlMillis,
    authorizationRefreshIntervalMillis: yield* positiveFiniteDurationMillis(
      "authorizationRefreshInterval",
      options.authorizationRefreshInterval ?? Math.max(1, Math.floor(memberTtlMillis / 2))
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
  if (resolved.memberTtlMillis < Protocol.minimumEphemeralMemberTtlMillis) {
    return yield* invalidConfiguration(
      "memberTtl",
      `memberTtl must be at least ${Protocol.minimumEphemeralMemberTtlMillis} milliseconds`
    )
  }
  if (resolved.memberTtlMillis > Protocol.maximumEphemeralMemberTtlMillis) {
    return yield* invalidConfiguration(
      "memberTtl",
      `memberTtl must not exceed ${Protocol.maximumEphemeralMemberTtlMillis} milliseconds`
    )
  }
  if (resolved.authorizationRefreshIntervalMillis > resolved.memberTtlMillis) {
    return yield* invalidConfiguration(
      "authorizationRefreshInterval",
      "authorizationRefreshInterval must not exceed memberTtl"
    )
  }
  if (resolved.maximumSnapshotBytes > Protocol.maximumBatchBytes) {
    return yield* invalidConfiguration(
      "maximumSnapshotBytes",
      `maximumSnapshotBytes must not exceed ${Protocol.maximumBatchBytes}`
    )
  }
  if (resolved.maximumEventTtlMillis > Protocol.maximumEphemeralEventTtlMillis) {
    return yield* invalidConfiguration(
      "maximumEventTtl",
      `maximumEventTtl must not exceed ${Protocol.maximumEphemeralEventTtlMillis} milliseconds`
    )
  }
  if (resolved.maximumStateTtlMillis > Protocol.maximumEphemeralStateTtlMillis) {
    return yield* invalidConfiguration(
      "maximumStateTtl",
      `maximumStateTtl must not exceed ${Protocol.maximumEphemeralStateTtlMillis} milliseconds`
    )
  }
  if (resolved.spaceIdleTtlMillis < resolved.maximumStateTtlMillis) {
    return yield* invalidConfiguration(
      "spaceIdleTtl",
      "spaceIdleTtl must be at least maximumStateTtl"
    )
  }
  return resolved
})

const ensurePayloadSize = Effect.fnUntraced(function*(value: unknown) {
  if ((yield* Protocol.encodedBytesEffect(value)) > Protocol.maximumEphemeralPayloadBytes) {
    yield* capacityExceeded("ephemeral payload bytes", Protocol.maximumEphemeralPayloadBytes)
  }
})

const ensureSession = (
  runtime: SpaceRuntime,
  member: Protocol.EphemeralMember,
  sessionToken: Identity.EphemeralSessionToken
) => {
  if (runtime.members.get(memberKey(member))?.sessionToken === sessionToken) return Effect.void
  return Effect.fail(
    new ReplicaError.EphemeralSessionUnavailable({
      spaceId: runtime.spaceId,
      clientId: member.clientId,
      membershipIncarnation: member.membershipIncarnation
    })
  )
}

const publishDelta = (runtime: SpaceRuntime, message: Protocol.EphemeralMessage) =>
  PubSub.publish(runtime.channel, message).pipe(Effect.asVoid)

const snapshotBaseBytes = 256

const ensureSnapshotSize = (
  options: ResolvedOptions,
  nextEntriesBytes: number,
  nextEntryCount: number
) => {
  const bytes = snapshotBaseBytes + nextEntriesBytes + Math.max(0, nextEntryCount - 1)
  if (bytes <= options.maximumSnapshotBytes) return Effect.void
  return Effect.fail(capacityExceeded("ephemeral snapshot bytes", options.maximumSnapshotBytes))
}

const cleanupOwnerAccounting = (runtime: SpaceRuntime, owner: string) => {
  if (runtime.members.has(owner)) return
  if ((runtime.memberBytes.get(owner) ?? 0) !== 0) return
  if ((runtime.memberStateKeys.get(owner) ?? 0) !== 0) return
  if ((runtime.memberEventKeys.get(owner) ?? 0) !== 0) return
  runtime.memberBytes.delete(owner)
  runtime.memberStateKeys.delete(owner)
  runtime.memberEventKeys.delete(owner)
}

const withGate = <A, E extends { readonly _tag: string }, R,>(
  runtime: SpaceRuntime,
  effect: Effect.Effect<A, E, R>
) => Semaphore.withPermit(runtime.gate, Effect.uninterruptible(effect))

const removeMemberEvents = Effect.fnUntraced(function*(
  runtime: SpaceRuntime,
  member: Protocol.EphemeralMember,
  publishClears: boolean
) {
  const owner = memberKey(member)
  for (const [identity, event] of runtime.events) {
    if (memberKey(event.member) !== owner) continue
    runtime.events.delete(identity)
    yield* FiberMap.remove(runtime.eventTimers, identity)
    if (publishClears) {
      yield* publishDelta(
        runtime,
        Protocol.EphemeralEventCleared.make({
          spaceId: runtime.spaceId,
          revision: nextRevision(runtime),
          member,
          channel: event.channel
        })
      )
    }
  }
  runtime.memberEventKeys.set(owner, 0)
})

const removeMember = (
  runtime: SpaceRuntime,
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
      runtime.snapshotEntriesBytes -= current.entryBytes
      runtime.memberBytes.set(key, (runtime.memberBytes.get(key) ?? 0) - current.bytes)
      if (cancelTimer) yield* FiberMap.remove(runtime.memberTimers, key)
      yield* removeMemberEvents(runtime, member, false)
      cleanupOwnerAccounting(runtime, key)
      yield* Deferred.succeed(current.departed, undefined)
      yield* publishDelta(
        runtime,
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
  member: Protocol.EphemeralMember,
  channel: Protocol.EphemeralChannel,
  key: Protocol.EphemeralKey,
  token: object | undefined,
  cancelTimer: boolean,
  sessionToken?: Identity.EphemeralSessionToken
) =>
  withGate(
    runtime,
    Effect.gen(function*() {
      if (sessionToken !== undefined) yield* ensureSession(runtime, member, sessionToken)
      const identity = stateKey(member, channel, key)
      const current = runtime.states.get(identity)
      if (current === undefined || (token !== undefined && current.token !== token)) return
      runtime.states.delete(identity)
      runtime.spaceBytes -= current.bytes
      runtime.snapshotEntriesBytes -= current.entryBytes
      const owner = memberKey(member)
      runtime.memberBytes.set(owner, (runtime.memberBytes.get(owner) ?? 0) - current.bytes)
      runtime.memberStateKeys.set(owner, (runtime.memberStateKeys.get(owner) ?? 1) - 1)
      cleanupOwnerAccounting(runtime, owner)
      if (cancelTimer) yield* FiberMap.remove(runtime.stateTimers, identity)
      yield* publishDelta(
        runtime,
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
  member: Protocol.EphemeralMember,
  channel: Protocol.EphemeralChannel,
  token: object | undefined,
  cancelTimer: boolean,
  sessionToken?: Identity.EphemeralSessionToken
) =>
  withGate(
    runtime,
    Effect.gen(function*() {
      if (sessionToken !== undefined) yield* ensureSession(runtime, member, sessionToken)
      const identity = eventKey(member, channel)
      const current = runtime.events.get(identity)
      if (current === undefined || (token !== undefined && current.token !== token)) return
      runtime.events.delete(identity)
      const owner = memberKey(member)
      runtime.memberEventKeys.set(owner, (runtime.memberEventKeys.get(owner) ?? 1) - 1)
      cleanupOwnerAccounting(runtime, owner)
      if (cancelTimer) yield* FiberMap.remove(runtime.eventTimers, identity)
      yield* publishDelta(
        runtime,
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
      const channel = yield* PubSub.sliding<Protocol.EphemeralMessage>(options.capacity)
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
        spaceBytes: 0,
        snapshotEntriesBytes: 0
      } satisfies SpaceRuntime
    }),
    (runtime) => PubSub.shutdown(runtime.channel)
  )

const acquireWatcher = (runtime: SpaceRuntime, options: ResolvedOptions) =>
  Effect.acquireRelease(
    Effect.gen(function*() {
      if (!(yield* Semaphore.takeIfAvailable(runtime.watcherPermits, 1))) {
        yield* capacityExceeded("ephemeral watchers", options.maximumWatchersPerSpace)
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
): Layer.Layer<EphemeralHub, ReplicaError.InvalidConfiguration, R | Crypto.Crypto> =>
  Layer.effect(
    EphemeralHub,
    Effect.gen(function*() {
      const context = yield* Effect.context<R>()
      const crypto = yield* Crypto.Crypto
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
              return capacityExceeded("ephemeral spaces", resolved.maximumSpaces)
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
            const sessionToken = yield* crypto.randomUUIDv4.pipe(
              Effect.map((uuid) => Identity.EphemeralSessionToken.make(`eps_${uuid}`)),
              Effect.catch(() => Effect.fail(new ReplicaError.ServerUnavailable()))
            )
            const departed = yield* Deferred.make<void, ReplicaError.EphemeralSessionUnavailable>()
            const acquireJoin = withGate(
              runtime,
              Effect.gen(function*() {
                const key = memberKey(request.member)
                const previous = runtime.members.get(key)
                if (previous === undefined && runtime.members.size >= resolved.maximumMembersPerSpace) {
                  return yield* capacityExceeded("ephemeral members", resolved.maximumMembersPerSpace)
                }
                const bytes = yield* Protocol.encodedBytesEffect(request.value)
                const previousBytes = previous?.bytes ?? 0
                const memberBytes = (runtime.memberBytes.get(key) ?? 0) - previousBytes + bytes
                if (memberBytes > resolved.maximumBytesPerMember) {
                  return yield* capacityExceeded("ephemeral bytes per member", resolved.maximumBytesPerMember)
                }
                const spaceBytes = runtime.spaceBytes - previousBytes + bytes
                if (spaceBytes > resolved.maximumBytesPerSpace) {
                  return yield* capacityExceeded("ephemeral bytes per space", resolved.maximumBytesPerSpace)
                }
                const subscription = yield* PubSub.subscribe(runtime.channel)
                const now = yield* Clock.currentTimeMillis
                const ttlMillis = Math.min(request.ttlMillis, resolved.memberTtlMillis)
                const entry = Protocol.EphemeralMemberEntry.make({
                  member: request.member,
                  value: request.value,
                  expiresAtMillis: now + ttlMillis
                })
                const entryBytes = yield* Protocol.encodedBytesEffect(entry)
                const previousEntryBytes = previous?.entryBytes ?? 0
                const nextSnapshotEntriesBytes = runtime.snapshotEntriesBytes - previousEntryBytes + entryBytes
                let nextEntryCount = runtime.members.size + runtime.states.size
                if (previous === undefined) nextEntryCount += 1
                yield* ensureSnapshotSize(resolved, nextSnapshotEntriesBytes, nextEntryCount)
                if (previous !== undefined) {
                  yield* removeMemberEvents(runtime, request.member, true)
                  yield* Deferred.fail(
                    previous.departed,
                    new ReplicaError.EphemeralSessionUnavailable({
                      spaceId: request.spaceId,
                      clientId: request.member.clientId,
                      membershipIncarnation: request.member.membershipIncarnation
                    })
                  )
                }
                runtime.members.set(key, {
                  entry,
                  bytes,
                  entryBytes,
                  token,
                  sessionToken,
                  departed,
                  leaseMillis: ttlMillis
                })
                runtime.memberBytes.set(key, memberBytes)
                runtime.spaceBytes = spaceBytes
                runtime.snapshotEntriesBytes = nextSnapshotEntriesBytes
                const revision = nextRevision(runtime)
                const expireMember = removeMember(runtime, request.member, token, false)
                yield* FiberMap.run(
                  runtime.memberTimers,
                  key,
                  Effect.sleep(ttlMillis).pipe(Effect.andThen(expireMember))
                )
                yield* publishDelta(
                  runtime,
                  Protocol.EphemeralMemberUpserted.make({
                    spaceId: request.spaceId,
                    revision,
                    entry
                  })
                )
                return {
                  started: Protocol.EphemeralSessionStarted.make({
                    spaceId: request.spaceId,
                    member: request.member,
                    sessionToken,
                    leaseMillis: ttlMillis
                  }),
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
              () => removeMember(runtime, request.member, token, true)
            )
            const refreshAuthorization = authorize(JoinAuthorization.make({
              spaceId: request.spaceId,
              member: request.member,
              principal
            }))
            return Stream.concat(
              Stream.make(acquired.started, acquired.snapshot),
              Stream.fromSubscription(acquired.subscription).pipe(
                Stream.filter((message) => message.revision > acquired.snapshot.revision),
                Stream.mapAccum(
                  () => acquired.snapshot.revision,
                  (revision: Identity.EphemeralRevision, message: Protocol.EphemeralMessage) =>
                    [message.revision, [{
                      contiguous: message.revision === revision + 1,
                      message
                    }]] as const
                ),
                Stream.takeUntil(({ contiguous }) => !contiguous, { excludeLast: true }),
                Stream.map(({ message }) => message)
              )
            ).pipe(
              Stream.interruptWhen(Deferred.await(departed)),
              Stream.mergeEffect(
                Effect.sleep(resolved.authorizationRefreshIntervalMillis).pipe(
                  Effect.andThen(refreshAuthorization),
                  Effect.forever
                )
              )
            )
          })).pipe(
            Stream.withSpan("EphemeralHub.join", {
              attributes: { "ephemeral.space_id": request.spaceId }
            })
          ),
        publish: Effect.fn("EphemeralHub.publish")(function*(request, sessionToken, principal) {
          let channel: Protocol.EphemeralChannel | null = null
          if (request._tag !== "UpdateMember") channel = request.channel
          let key: Protocol.EphemeralKey | null = null
          if (request._tag === "SetState" || request._tag === "RemoveState") key = request.key
          yield* authorize(PublishAuthorization.make({
            spaceId: request.spaceId,
            member: request.member,
            operation: request._tag,
            channel,
            key,
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
            yield* ensureSession(runtime, request.member, sessionToken)
            if (request._tag === "ClearEvent") {
              yield* clearEvent(runtime, request.member, request.channel, undefined, true, sessionToken)
            } else if (request._tag === "RemoveState") {
              yield* removeState(
                runtime,
                request.member,
                request.channel,
                request.key,
                undefined,
                true,
                sessionToken
              )
            } else {
              yield* withGate(
                runtime,
                Effect.gen(function*() {
                  yield* ensureSession(runtime, request.member, sessionToken)
                  const owner = memberKey(request.member)
                  if (request._tag === "UpdateMember") {
                    const current = runtime.members.get(owner)
                    if (current === undefined) return yield* ensureSession(runtime, request.member, sessionToken)
                    const bytes = yield* Protocol.encodedBytesEffect(request.value)
                    const ownerBytes = (runtime.memberBytes.get(owner) ?? 0) - current.bytes + bytes
                    if (ownerBytes > resolved.maximumBytesPerMember) {
                      return yield* capacityExceeded("ephemeral bytes per member", resolved.maximumBytesPerMember)
                    }
                    const spaceBytes = runtime.spaceBytes - current.bytes + bytes
                    if (spaceBytes > resolved.maximumBytesPerSpace) {
                      return yield* capacityExceeded("ephemeral bytes per space", resolved.maximumBytesPerSpace)
                    }
                    const entry = Protocol.EphemeralMemberEntry.make({
                      ...current.entry,
                      value: request.value
                    })
                    const entryBytes = yield* Protocol.encodedBytesEffect(entry)
                    const nextSnapshotEntriesBytes = runtime.snapshotEntriesBytes - current.entryBytes + entryBytes
                    yield* ensureSnapshotSize(
                      resolved,
                      nextSnapshotEntriesBytes,
                      runtime.members.size + runtime.states.size
                    )
                    runtime.members.set(owner, { ...current, entry, bytes, entryBytes })
                    runtime.memberBytes.set(owner, ownerBytes)
                    runtime.spaceBytes = spaceBytes
                    runtime.snapshotEntriesBytes = nextSnapshotEntriesBytes
                    return yield* publishDelta(
                      runtime,
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
                      return yield* capacityExceeded(
                        "ephemeral event keys per member",
                        resolved.maximumEventKeysPerMember
                      )
                    }
                    if (previous === undefined && runtime.events.size >= resolved.maximumEventKeysPerSpace) {
                      return yield* capacityExceeded(
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
                      request.member,
                      request.channel,
                      token,
                      false
                    )
                    yield* FiberMap.run(
                      runtime.eventTimers,
                      identity,
                      Effect.sleep(ttlMillis).pipe(Effect.andThen(expireEvent))
                    )
                    return yield* publishDelta(
                      runtime,
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
                    return yield* capacityExceeded(
                      "ephemeral state keys per member",
                      resolved.maximumStateKeysPerMember
                    )
                  }
                  if (previous === undefined && runtime.states.size >= resolved.maximumStateKeysPerSpace) {
                    return yield* capacityExceeded(
                      "ephemeral state keys per space",
                      resolved.maximumStateKeysPerSpace
                    )
                  }
                  const bytes = yield* Protocol.encodedBytesEffect(request.value)
                  const previousBytes = previous?.bytes ?? 0
                  const ownerBytes = (runtime.memberBytes.get(owner) ?? 0) - previousBytes + bytes
                  if (ownerBytes > resolved.maximumBytesPerMember) {
                    return yield* capacityExceeded("ephemeral bytes per member", resolved.maximumBytesPerMember)
                  }
                  const spaceBytes = runtime.spaceBytes - previousBytes + bytes
                  if (spaceBytes > resolved.maximumBytesPerSpace) {
                    return yield* capacityExceeded("ephemeral bytes per space", resolved.maximumBytesPerSpace)
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
                  const entryBytes = yield* Protocol.encodedBytesEffect(entry)
                  const nextSnapshotEntriesBytes = runtime.snapshotEntriesBytes - (previous?.entryBytes ?? 0) +
                    entryBytes
                  let nextEntryCount = runtime.members.size + runtime.states.size
                  if (previous === undefined) nextEntryCount += 1
                  yield* ensureSnapshotSize(resolved, nextSnapshotEntriesBytes, nextEntryCount)
                  runtime.states.set(identity, { entry, bytes, entryBytes, token })
                  runtime.memberBytes.set(owner, ownerBytes)
                  let nextOwnerStateKeys = ownerStateKeys
                  if (previous === undefined) nextOwnerStateKeys += 1
                  runtime.memberStateKeys.set(owner, nextOwnerStateKeys)
                  runtime.spaceBytes = spaceBytes
                  runtime.snapshotEntriesBytes = nextSnapshotEntriesBytes
                  const expireState = removeState(
                    runtime,
                    request.member,
                    request.channel,
                    request.key,
                    token,
                    false
                  )
                  yield* FiberMap.run(
                    runtime.stateTimers,
                    identity,
                    Effect.sleep(ttlMillis).pipe(Effect.andThen(expireState))
                  )
                  yield* publishDelta(
                    runtime,
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
        heartbeat: Effect.fn("EphemeralHub.heartbeat")(function*(request, sessionToken, principal) {
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
                if (current?.sessionToken !== sessionToken) {
                  return yield* ensureSession(runtime, request.member, sessionToken)
                }
                const now = yield* Clock.currentTimeMillis
                const entry = Protocol.EphemeralMemberEntry.make({
                  ...current.entry,
                  expiresAtMillis: now + current.leaseMillis
                })
                const entryBytes = yield* Protocol.encodedBytesEffect(entry)
                const nextSnapshotEntriesBytes = runtime.snapshotEntriesBytes + entryBytes - current.entryBytes
                yield* ensureSnapshotSize(
                  resolved,
                  nextSnapshotEntriesBytes,
                  runtime.members.size + runtime.states.size
                )
                runtime.snapshotEntriesBytes = nextSnapshotEntriesBytes
                runtime.members.set(key, { ...current, entry, entryBytes })
                yield* FiberMap.run(
                  runtime.memberTimers,
                  key,
                  Effect.sleep(current.leaseMillis).pipe(
                    Effect.andThen(removeMember(runtime, request.member, current.token, false))
                  )
                )
                return yield* publishDelta(
                  runtime,
                  Protocol.EphemeralMemberUpserted.make({
                    spaceId: request.spaceId,
                    revision: nextRevision(runtime),
                    entry
                  })
                )
              })
            )
          }))
        })
      })
      return service
    })
  )

export const layerTrusted = (
  options: Options
): Layer.Layer<EphemeralHub, ReplicaError.InvalidConfiguration, Crypto.Crypto> =>
  layer({ ...options, authorize: () => Effect.void })
