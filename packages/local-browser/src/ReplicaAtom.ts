import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Cause from "effect/Cause"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Hash from "effect/Hash"
import type * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Atom } from "effect/unstable/reactivity"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import type * as AtomRegistry from "effect/unstable/reactivity/AtomRegistry"
import type * as Reactivity from "effect/unstable/reactivity/Reactivity"

class QueryKey<P,> implements Equal.Equal {
  readonly value: string
  readonly payload: P
  constructor(value: string, payload: P) {
    this.value = value
    this.payload = payload
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof QueryKey && this.value === that.value
  }
  [Hash.symbol](): number {
    return Hash.string(this.value)
  }
}

class EphemeralSessionKey implements Equal.Equal {
  readonly value: string
  readonly profile: Ephemeral.AnyMember
  readonly options: EphemeralClient.SessionOptions<Ephemeral.AnyMember>
  constructor(profile: Ephemeral.AnyMember, options: EphemeralClient.SessionOptions<Ephemeral.AnyMember>) {
    this.profile = profile
    this.options = options
    // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Atom family keys are built synchronously; the session effect re-encodes the value through the Effect codec, so this encode only derives the cache identity.
    const encoded = Schema.encodeSync(profile.payloadSchema)(options.value)
    this.value = `${options.spaceId}:${options.member.clientId}:${options.member.membershipIncarnation}:${
      Duration.toMillis(options.ttl)
    }:${Canonical.hash(encoded)}`
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof EphemeralSessionKey && this.value === that.value && this.profile === that.profile
  }
  [Hash.symbol](): number {
    return Hash.string(this.value)
  }
}

class EphemeralEventKey implements Equal.Equal {
  readonly session: object
  readonly definition: Ephemeral.AnyEvent
  constructor(session: object, definition: Ephemeral.AnyEvent) {
    this.session = session
    this.definition = definition
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof EphemeralEventKey && this.session === that.session &&
      this.definition === that.definition
  }
  [Hash.symbol](): number {
    return Hash.hash(this.session) ^ Hash.hash(this.definition)
  }
}

class EphemeralStateKey implements Equal.Equal {
  readonly session: object
  readonly definition: Ephemeral.AnyState
  constructor(session: object, definition: Ephemeral.AnyState) {
    this.session = session
    this.definition = definition
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof EphemeralStateKey && this.session === that.session &&
      this.definition === that.definition
  }
  [Hash.symbol](): number {
    return Hash.hash(this.session) ^ Hash.hash(this.definition)
  }
}

class EphemeralPublishKey implements Equal.Equal {
  readonly value: string
  readonly definition: Ephemeral.Any
  readonly target: EphemeralClient.PublishTarget
  constructor(definition: Ephemeral.Any, target: EphemeralClient.PublishTarget) {
    this.definition = definition
    this.target = target
    this.value = `${target.spaceId}:${target.member.clientId}:${target.member.membershipIncarnation}`
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof EphemeralPublishKey && this.value === that.value &&
      this.definition === that.definition
  }
  [Hash.symbol](): number {
    return Hash.string(this.value) ^ Hash.hash(this.definition)
  }
}

class EphemeralRemoveKey implements Equal.Equal {
  readonly value: string
  readonly definition: Ephemeral.AnyState
  readonly target: EphemeralClient.PublishTarget
  constructor(definition: Ephemeral.AnyState, target: EphemeralClient.PublishTarget) {
    this.definition = definition
    this.target = target
    this.value = `${target.spaceId}:${target.member.clientId}:${target.member.membershipIncarnation}`
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof EphemeralRemoveKey && this.value === that.value &&
      this.definition === that.definition
  }
  [Hash.symbol](): number {
    return Hash.string(this.value) ^ Hash.hash(this.definition)
  }
}

class EphemeralMembersKey implements Equal.Equal {
  readonly session: object
  constructor(session: object) {
    this.session = session
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof EphemeralMembersKey && this.session === that.session
  }
  [Hash.symbol](): number {
    return Hash.hash(this.session)
  }
}

export const make = <E,>(
  layer: Layer.Layer<
    Replica.Replica | QueryReactivity.QueryReactivity | EphemeralClient.EphemeralClient,
    E,
    AtomRegistry.AtomRegistry | Reactivity.Reactivity
  >,
  options?: {
    readonly factory?: Atom.RuntimeFactory
    readonly idleTTL?: Duration.Input
  }
) => {
  const factory = options?.factory ?? Atom.runtime
  const runtime = factory(layer)
  const idleTTL = Duration.toMillis(options?.idleTTL ?? Duration.seconds(30))

  type SessionError = ReplicaError.ReplicaError | Ephemeral.EncodeError | E
  type SessionAtom<M extends Ephemeral.AnyMember,> = Atom.Atom<
    AsyncResult.AsyncResult<EphemeralClient.Session<M>, SessionError>
  >
  type SessionSource = Atom.Atom<
    AsyncResult.AsyncResult<
      EphemeralClient.Session<Ephemeral.AnyMember>,
      ReplicaError.ReplicaError | Ephemeral.EncodeError
    >
  >
  type ProjectionError = Ephemeral.DecodeError | SessionError | Cause.NoSuchElementError

  const ephemeralSessions = Atom.family((key: EphemeralSessionKey) =>
    runtime.atom(
      EphemeralClient.EphemeralClient.use((client) => client.session(key.profile, key.options))
    ).pipe(Atom.setIdleTTL(idleTTL))
  )
  const ephemeral = <M extends Ephemeral.AnyMember,>(
    profile: M,
    request: EphemeralClient.SessionOptions<M>
  ): SessionAtom<M> =>
    // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The session family erases the member schema type; the atom for this key was built from this exact profile, so the runtime values already match M.
    ephemeralSessions(new EphemeralSessionKey(profile, request)) as unknown as SessionAtom<M>

  const ephemeralEventsFamily = Atom.family((key: EphemeralEventKey) => {
    // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- Projection keys erase the session atom type; only session atoms from this graph construct these keys.
    const source = key.session as SessionSource
    return runtime.atom((get) =>
      Stream.unwrap(
        get.result(source).pipe(Effect.map((session) => session.events(key.definition)))
      )
    ).pipe(Atom.setIdleTTL(idleTTL))
  })
  const ephemeralEvents = <M extends Ephemeral.AnyMember, D extends Ephemeral.AnyEvent,>(
    session: SessionAtom<M>,
    definition: D
  ): Atom.Atom<AsyncResult.AsyncResult<EphemeralClient.EventEnvelope<D>, ProjectionError>> =>
    // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The projection family erases the definition type; the atom for this key decodes with this exact definition, so the runtime values already match D.
    ephemeralEventsFamily(new EphemeralEventKey(session, definition)) as unknown as Atom.Atom<
      AsyncResult.AsyncResult<EphemeralClient.EventEnvelope<D>, ProjectionError>
    >

  const ephemeralStateFamily = Atom.family((key: EphemeralStateKey) => {
    // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- Projection keys erase the session atom type; only session atoms from this graph construct these keys.
    const source = key.session as SessionSource
    return runtime.atom((get) =>
      Stream.unwrap(
        get.result(source).pipe(Effect.map((session) => session.state(key.definition)))
      )
    ).pipe(Atom.setIdleTTL(idleTTL))
  })
  const ephemeralState = <M extends Ephemeral.AnyMember, D extends Ephemeral.AnyState,>(
    session: SessionAtom<M>,
    definition: D
  ): Atom.Atom<
    AsyncResult.AsyncResult<ReadonlyArray<EphemeralClient.StateEntry<D>>, ProjectionError>
  > =>
    // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The projection family erases the definition type; the atom for this key decodes with this exact definition, so the runtime values already match D.
    ephemeralStateFamily(new EphemeralStateKey(session, definition)) as unknown as Atom.Atom<
      AsyncResult.AsyncResult<ReadonlyArray<EphemeralClient.StateEntry<D>>, ProjectionError>
    >

  const ephemeralMembersFamily = Atom.family((key: EphemeralMembersKey) => {
    // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- Projection keys erase the session atom type; only session atoms from this graph construct these keys.
    const source = key.session as SessionSource
    return runtime.atom((get) => Stream.unwrap(get.result(source).pipe(Effect.map((session) => session.members)))).pipe(
      Atom.setIdleTTL(idleTTL)
    )
  })
  const ephemeralMembers = <M extends Ephemeral.AnyMember,>(
    session: SessionAtom<M>
  ): Atom.Atom<
    AsyncResult.AsyncResult<ReadonlyArray<EphemeralClient.MemberEntry<M>>, ProjectionError>
  > =>
    // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion -- The members family erases the member schema type; the atom for this key decodes with the session's profile, so the runtime values already match M.
    ephemeralMembersFamily(new EphemeralMembersKey(session)) as unknown as Atom.Atom<
      AsyncResult.AsyncResult<ReadonlyArray<EphemeralClient.MemberEntry<M>>, ProjectionError>
    >

  const ephemeralPublishFamily = Atom.family((key: EphemeralPublishKey) =>
    runtime.fn<{
      readonly payload?: unknown
      readonly ttl: Duration.Input
      readonly key?: unknown
    }>()(
      (input) =>
        Effect.yieldNow.pipe(Effect.andThen(EphemeralClient.EphemeralClient.use((client) => {
          if (key.definition.kind === "event") {
            return client.publish(key.definition, {
              spaceId: key.target.spaceId,
              member: key.target.member,
              payload: input.payload,
              ttl: input.ttl
            })
          }
          return client.publish(key.definition, {
            spaceId: key.target.spaceId,
            member: key.target.member,
            key: input.key,
            payload: input.payload,
            ttl: input.ttl
          })
        }))),
      { concurrent: true }
    )
  )
  function publishEphemeral<D extends Ephemeral.AnyEvent,>(
    definition: D,
    target: EphemeralClient.PublishTarget
  ): Atom.AtomResultFn<
    Omit<EphemeralClient.EventPublishOptions<D>, "spaceId" | "member">,
    void,
    ReplicaError.ReplicaError | Ephemeral.EncodeError | E
  >
  function publishEphemeral<D extends Ephemeral.AnyState,>(
    definition: D,
    target: EphemeralClient.PublishTarget
  ): Atom.AtomResultFn<
    Omit<EphemeralClient.StatePublishOptions<D>, "spaceId" | "member">,
    void,
    ReplicaError.ReplicaError | Ephemeral.EncodeError | E
  >
  function publishEphemeral(
    definition: Ephemeral.Any,
    target: EphemeralClient.PublishTarget
  ): Atom.AtomResultFn<
    {
      readonly payload?: unknown
      readonly ttl: Duration.Input
      readonly key?: unknown
    },
    void,
    ReplicaError.ReplicaError | Ephemeral.EncodeError | E
  > {
    return ephemeralPublishFamily(new EphemeralPublishKey(definition, target))
  }

  const ephemeralRemoveFamily = Atom.family((key: EphemeralRemoveKey) =>
    runtime.fn<{ readonly key: unknown }>()(
      (input) =>
        Effect.yieldNow.pipe(Effect.andThen(EphemeralClient.EphemeralClient.use((client) =>
          client.remove(key.definition, {
            spaceId: key.target.spaceId,
            member: key.target.member,
            key: input.key
          })
        ))),
      { concurrent: true }
    )
  )
  function removeEphemeral<D extends Ephemeral.AnyState,>(
    definition: D,
    target: EphemeralClient.PublishTarget
  ): Atom.AtomResultFn<
    Omit<EphemeralClient.StateRemoveOptions<D>, "spaceId" | "member">,
    void,
    ReplicaError.ReplicaError | Ephemeral.EncodeError | E
  >
  function removeEphemeral(
    definition: Ephemeral.AnyState,
    target: EphemeralClient.PublishTarget
  ): Atom.AtomResultFn<{ readonly key: unknown }, void, ReplicaError.ReplicaError | Ephemeral.EncodeError | E> {
    return ephemeralRemoveFamily(new EphemeralRemoveKey(definition, target))
  }

  const entity = <M extends Model.Any,>(spaceId: Identity.SpaceId, model: M) =>
    Atom.family((key: Model.Key<M>) =>
      runtime.atom(
        Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.get(model, key))))
      ).pipe(
        factory.withReactivity([ReactivityKey.membership(spaceId), ReactivityKey.entity(spaceId, model.name, key)]),
        Atom.setIdleTTL(idleTTL)
      )
    )

  const query = <Q extends Query.Any,>(spaceId: Identity.SpaceId, definition: Q) => {
    const family = Atom.family((key: QueryKey<Q["payloadSchema"]["Type"]>) => {
      const token = ReactivityKey.query(spaceId, definition.name, key.payload)
      const retention = runtime.atom(
        QueryReactivity.QueryReactivity.use((service) =>
          Effect.acquireRelease(
            service.retain(token),
            (release) => release
          ).pipe(Effect.asVoid)
        )
      ).pipe(Atom.setIdleTTL(idleTTL))
      const target = runtime.atom(
        Replica.Replica.use((replica) =>
          replica.space(spaceId).pipe(Effect.flatMap((space) => space.query(definition, key.payload)))
        )
      ).pipe(
        factory.withReactivity([ReactivityKey.membership(spaceId), token])
      )
      return Atom.transform(target, (get, atom) => {
        if (!AsyncResult.isSuccess(get(retention))) return AsyncResult.initial(true)
        get.subscribe(atom, (value) => get.setSelf(value))
        return get.once(atom)
      }, { initialValueTarget: target }).pipe(Atom.setIdleTTL(idleTTL))
    })
    return (payload: Q["payloadSchema"]["Type"]) => {
      return family(new QueryKey(`${spaceId}:${definition.name}:${Canonical.hash(payload)}`, payload))
    }
  }

  const mutation = <M extends Mutation.Any,>(spaceId: Identity.SpaceId, definition: M) =>
    runtime.fn<Mutation.Payload<M>>()(
      (payload) =>
        Effect.yieldNow.pipe(
          Effect.andThen(
            Replica.Replica.use((replica) =>
              replica.space(spaceId).pipe(Effect.flatMap((space) => space.mutate(definition, payload)))
            )
          )
        ),
      { concurrent: true }
    )

  const receipt = <M extends Mutation.Any,>(
    spaceId: Identity.SpaceId,
    definition: M,
    mutationId: Identity.MutationId
  ) =>
    runtime.atom(
      Replica.Replica.use((replica) =>
        replica.space(spaceId).pipe(Effect.flatMap((space) => space.receipt(definition, mutationId)))
      )
    ).pipe(
      factory.withReactivity([
        ReactivityKey.membership(spaceId),
        ReactivityKey.receipt(spaceId, mutationId)
      ]),
      Atom.setIdleTTL(idleTTL)
    )

  const pending = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.atom(
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.pending)))
    ).pipe(
      factory.withReactivity([ReactivityKey.membership(spaceId), ReactivityKey.pending(spaceId)]),
      Atom.setIdleTTL(idleTTL)
    )
  )

  const pendingFor = <M extends Mutation.Any,>(spaceId: Identity.SpaceId, definition: M) =>
    runtime.atom(
      Replica.Replica.use((replica) =>
        replica.space(spaceId).pipe(Effect.flatMap((space) => space.pendingFor(definition)))
      )
    ).pipe(
      factory.withReactivity([ReactivityKey.membership(spaceId), ReactivityKey.pending(spaceId)]),
      Atom.setIdleTTL(idleTTL)
    )

  const settlements = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.atom(
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.map((space) => space.settlements())))
    ).pipe(
      factory.withReactivity([ReactivityKey.membership(spaceId)]),
      Atom.setIdleTTL(idleTTL)
    )
  )

  const settlementsFor = <M extends Mutation.Any,>(spaceId: Identity.SpaceId, definition: M) =>
    runtime.atom(
      Replica.Replica.use((replica) =>
        replica.space(spaceId).pipe(Effect.map((space) => space.settlementsFor(definition)))
      )
    ).pipe(
      factory.withReactivity([ReactivityKey.membership(spaceId)]),
      Atom.setIdleTTL(idleTTL)
    )

  const status = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.atom(
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.status)))
    ).pipe(factory.withReactivity([ReactivityKey.membership(spaceId), ReactivityKey.status(spaceId)]))
  )
  const scope = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.atom(
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.scope)))
    ).pipe(
      factory.withReactivity([ReactivityKey.membership(spaceId), ReactivityKey.scope(spaceId)])
    )
  )
  const activation = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.atom(
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.activation)))
    ).pipe(
      factory.withReactivity([ReactivityKey.membership(spaceId), ReactivityKey.activation(spaceId)])
    )
  )
  const setScope = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.fn<Protocol.ReplicationScope>()(
      (nextScope) =>
        Replica.Replica.use((replica) =>
          replica.space(spaceId).pipe(Effect.flatMap((space) => space.setScope(nextScope)))
        ),
      { concurrent: true }
    )
  )
  const activate = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.fn(
      () => Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.activate))),
      { concurrent: true }
    )
  )
  const deactivate = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.fn(
      () => Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.deactivate))),
      { concurrent: true }
    )
  )
  const spaces = runtime.atom(Replica.Replica.use((replica) => replica.spaces)).pipe(
    factory.withReactivity([ReactivityKey.spaces])
  )
  const aggregateStatus = runtime.atom(Replica.Replica.use((replica) => replica.status)).pipe(
    factory.withReactivity([ReactivityKey.aggregateStatus])
  )
  const join = runtime.fn<Identity.SpaceId>()(
    (spaceId) => Replica.Replica.use((replica) => replica.join(spaceId)),
    { concurrent: true }
  )
  const leave = runtime.fn<Identity.SpaceId>()(
    (spaceId) => Replica.Replica.use((replica) => replica.leave(spaceId)),
    { concurrent: true }
  )

  return {
    factory,
    runtime,
    entity,
    query,
    mutation,
    receipt,
    pending,
    pendingFor,
    settlements,
    settlementsFor,
    scope,
    setScope,
    activation,
    activate,
    deactivate,
    status,
    spaces,
    aggregateStatus,
    join,
    leave,
    ephemeral,
    ephemeralEvents,
    ephemeralState,
    ephemeralMembers,
    publishEphemeral,
    removeEphemeral
  } as const
}
