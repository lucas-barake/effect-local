import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
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
import type * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Atom } from "effect/unstable/reactivity"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import type * as AtomRegistry from "effect/unstable/reactivity/AtomRegistry"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"

class QueryKey implements Equal.Equal {
  readonly spaceId: Identity.SpaceId
  readonly definition: Query.Any
  readonly payload: unknown
  constructor(spaceId: Identity.SpaceId, definition: Query.Any, payload: unknown) {
    this.spaceId = spaceId
    this.definition = definition
    this.payload = payload
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof QueryKey && this.spaceId === that.spaceId && this.definition === that.definition &&
      Equal.equals(this.payload, that.payload)
  }
  [Hash.symbol](): number {
    return Hash.string(`${this.spaceId}:${this.definition.name}`) ^ Hash.hash(this.payload)
  }
}

class EntityKey implements Equal.Equal {
  readonly spaceId: Identity.SpaceId
  readonly model: Model.Any
  readonly key: unknown
  constructor(spaceId: Identity.SpaceId, model: Model.Any, key: unknown) {
    this.spaceId = spaceId
    this.model = model
    this.key = key
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof EntityKey && this.spaceId === that.spaceId && this.model === that.model &&
      Equal.equals(this.key, that.key)
  }
  [Hash.symbol](): number {
    return Hash.string(`${this.spaceId}:${this.model.name}`) ^ Hash.hash(this.key)
  }
}

class MutationKey implements Equal.Equal {
  readonly spaceId: Identity.SpaceId
  readonly definition: Mutation.Any
  readonly qualifier: string
  constructor(spaceId: Identity.SpaceId, definition: Mutation.Any, qualifier: string) {
    this.spaceId = spaceId
    this.definition = definition
    this.qualifier = qualifier
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof MutationKey && this.spaceId === that.spaceId && this.definition === that.definition &&
      this.qualifier === that.qualifier
  }
  [Hash.symbol](): number {
    return Hash.string(`${this.spaceId}:${this.definition.name}:${this.qualifier}`)
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

// Publish and remove keep separate `Atom.family` maps, so one key class is
// enough: the type parameter is what narrows the remove family to state.
class EphemeralTargetKey<D extends Ephemeral.Any,> implements Equal.Equal {
  readonly value: string
  readonly definition: D
  readonly target: EphemeralClient.PublishTarget
  constructor(definition: D, target: EphemeralClient.PublishTarget) {
    this.definition = definition
    this.target = target
    this.value = `${target.spaceId}:${target.member.clientId}:${target.member.membershipIncarnation}`
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof EphemeralTargetKey && this.value === that.value &&
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
  const reactivity = runtime.atom(Effect.service(Reactivity.Reactivity))

  const refreshOn = (keys: ReadonlyArray<string>) =>
  <A, EA,>(
    atom: Atom.Atom<AsyncResult.AsyncResult<A, EA>>
  ): Atom.Atom<AsyncResult.AsyncResult<A, EA>> =>
    Atom.transform(atom, (get) => {
      let stale = false
      get.subscribe(atom, (value) => {
        get.setSelf(value)
        if (!stale || value.waiting) return
        stale = false
        get.refresh(atom)
      })
      const service = get(reactivity)
      if (AsyncResult.isSuccess(service)) {
        get.addFinalizer(service.value.registerUnsafe(keys, () => {
          if (get.once(atom).waiting) stale = true
          else get.refresh(atom)
        }))
      }
      return get.once(atom)
    }, { initialValueTarget: atom })

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

  const ephemeralPublishFamily = Atom.family((key: EphemeralTargetKey<Ephemeral.Any>) =>
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
    return ephemeralPublishFamily(new EphemeralTargetKey(definition, target))
  }

  const ephemeralRemoveFamily = Atom.family((key: EphemeralTargetKey<Ephemeral.AnyState>) =>
    runtime.fn<{ readonly key: unknown }>()(
      (input) =>
        Effect.yieldNow.pipe(
          Effect.andThen(EphemeralClient.EphemeralClient.use((client) =>
            client.remove(key.definition, {
              spaceId: key.target.spaceId,
              member: key.target.member,
              key: input.key
            })
          ))
        ),
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
    return ephemeralRemoveFamily(new EphemeralTargetKey(definition, target))
  }

  type GraphError = ReplicaError.ReplicaError | E

  const entityFamily = Atom.family((key: EntityKey) =>
    runtime.atom(
      Replica.Replica.use((replica) =>
        replica.space(key.spaceId).pipe(Effect.flatMap((space) => space.get(key.model, key.key)))
      )
    ).pipe(
      refreshOn([
        ReactivityKey.membership(key.spaceId),
        ReactivityKey.entity(key.spaceId, key.model.name, key.key)
      ]),
      Atom.setIdleTTL(idleTTL)
    )
  )
  function entity<M extends Model.Any,>(
    spaceId: Identity.SpaceId,
    model: M
  ): (key: Model.Key<M>) => Atom.Atom<AsyncResult.AsyncResult<Option.Option<Model.Value<M>>, GraphError>>
  function entity(
    spaceId: Identity.SpaceId,
    model: Model.Any
  ): (key: unknown) => Atom.Atom<AsyncResult.AsyncResult<Option.Option<unknown>, GraphError>> {
    return (key) => entityFamily(new EntityKey(spaceId, model, key))
  }

  const queryFamily = Atom.family((key: QueryKey) => {
    const token = ReactivityKey.query(key.spaceId, key.definition.name, key.payload)
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
        replica.space(key.spaceId).pipe(Effect.flatMap((space) => space.query(key.definition, key.payload)))
      )
    ).pipe(
      refreshOn([ReactivityKey.membership(key.spaceId), token])
    )
    return Atom.transform(target, (get, atom) => {
      if (!AsyncResult.isSuccess(get(retention))) return AsyncResult.initial(true)
      get.subscribe(atom, (value) => get.setSelf(value))
      return get.once(atom)
    }, { initialValueTarget: target }).pipe(Atom.setIdleTTL(idleTTL))
  })
  function query<Q extends Query.Any,>(
    spaceId: Identity.SpaceId,
    definition: Q
  ): (payload: Q["payloadSchema"]["Type"]) => Atom.Atom<
    AsyncResult.AsyncResult<
      Q["successSchema"]["Type"],
      GraphError | ReplicaError.QueryFailed | Q["errorSchema"]["Type"]
    >
  >
  function query(
    spaceId: Identity.SpaceId,
    definition: Query.Any
  ): (payload: unknown) => Atom.Atom<AsyncResult.AsyncResult<unknown, unknown>> {
    return (payload) => queryFamily(new QueryKey(spaceId, definition, payload))
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

  const receiptFamily = Atom.family((key: MutationKey) =>
    runtime.atom(
      Replica.Replica.use((replica) =>
        replica.space(key.spaceId).pipe(
          Effect.flatMap((space) => space.receipt(key.definition, Identity.MutationId.make(key.qualifier)))
        )
      )
    ).pipe(
      refreshOn([
        ReactivityKey.membership(key.spaceId),
        ReactivityKey.receipt(key.spaceId, Identity.MutationId.make(key.qualifier))
      ]),
      Atom.setIdleTTL(idleTTL)
    )
  )
  function receipt<M extends Mutation.Any,>(
    spaceId: Identity.SpaceId,
    definition: M,
    mutationId: Identity.MutationId
  ): Atom.Atom<AsyncResult.AsyncResult<Option.Option<Replica.Receipt<M>>, GraphError>>
  function receipt(
    spaceId: Identity.SpaceId,
    definition: Mutation.Any,
    mutationId: Identity.MutationId
  ): Atom.Atom<AsyncResult.AsyncResult<Option.Option<unknown>, GraphError>> {
    return receiptFamily(new MutationKey(spaceId, definition, mutationId))
  }

  const pending = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.atom(
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.pending)))
    ).pipe(
      refreshOn([ReactivityKey.membership(spaceId), ReactivityKey.pending(spaceId)]),
      Atom.setIdleTTL(idleTTL)
    )
  )

  const pendingForFamily = Atom.family((key: MutationKey) =>
    runtime.atom(
      Replica.Replica.use((replica) =>
        replica.space(key.spaceId).pipe(Effect.flatMap((space) => space.pendingFor(key.definition)))
      )
    ).pipe(
      refreshOn([ReactivityKey.membership(key.spaceId), ReactivityKey.pending(key.spaceId)]),
      Atom.setIdleTTL(idleTTL)
    )
  )
  function pendingFor<M extends Mutation.Any,>(
    spaceId: Identity.SpaceId,
    definition: M
  ): Atom.Atom<AsyncResult.AsyncResult<ReadonlyArray<Replica.PendingMutation<M>>, GraphError>>
  function pendingFor(
    spaceId: Identity.SpaceId,
    definition: Mutation.Any
  ): Atom.Atom<AsyncResult.AsyncResult<ReadonlyArray<Replica.PendingMutation>, GraphError>> {
    return pendingForFamily(new MutationKey(spaceId, definition, ""))
  }

  const settlements = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.atom(
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.map((space) => space.settlements())))
    ).pipe(
      refreshOn([ReactivityKey.membership(spaceId)]),
      Atom.setIdleTTL(idleTTL)
    )
  )

  const settlementsForFamily = Atom.family((key: MutationKey) =>
    runtime.atom(
      Replica.Replica.use((replica) =>
        replica.space(key.spaceId).pipe(Effect.map((space) => space.settlementsFor(key.definition)))
      )
    ).pipe(
      refreshOn([ReactivityKey.membership(key.spaceId)]),
      Atom.setIdleTTL(idleTTL)
    )
  )
  function settlementsFor<M extends Mutation.Any,>(
    spaceId: Identity.SpaceId,
    definition: M
  ): Atom.Atom<
    AsyncResult.AsyncResult<Stream.Stream<Replica.SettledMutation<M>, ReplicaError.ReplicaError>, GraphError>
  >
  function settlementsFor(
    spaceId: Identity.SpaceId,
    definition: Mutation.Any
  ): Atom.Atom<AsyncResult.AsyncResult<Stream.Stream<Replica.SettledMutation, ReplicaError.ReplicaError>, GraphError>> {
    return settlementsForFamily(new MutationKey(spaceId, definition, ""))
  }

  const status = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.atom(
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.status)))
    ).pipe(refreshOn([ReactivityKey.membership(spaceId), ReactivityKey.status(spaceId)]))
  )
  const scope = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.atom(
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.scope)))
    ).pipe(
      refreshOn([ReactivityKey.membership(spaceId), ReactivityKey.scope(spaceId)])
    )
  )
  const activation = Atom.family((spaceId: Identity.SpaceId) =>
    runtime.atom(
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.flatMap((space) => space.activation)))
    ).pipe(
      refreshOn([ReactivityKey.membership(spaceId), ReactivityKey.activation(spaceId)])
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
    refreshOn([ReactivityKey.spaces])
  )
  const aggregateStatus = runtime.atom(Replica.Replica.use((replica) => replica.status)).pipe(
    refreshOn([ReactivityKey.aggregateStatus])
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
