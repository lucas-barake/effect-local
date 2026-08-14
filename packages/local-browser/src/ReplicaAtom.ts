import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Hash from "effect/Hash"
import * as HashMap from "effect/HashMap"
import type * as Layer from "effect/Layer"
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

class EphemeralJoinKey implements Equal.Equal {
  readonly value: string
  readonly request: EphemeralClient.JoinRequest
  constructor(request: EphemeralClient.JoinRequest) {
    this.value = `${request.spaceId}:${request.member.clientId}:${request.member.membershipIncarnation}:${
      Canonical.hash(request)
    }`
    this.request = request
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof EphemeralJoinKey && this.value === that.value
  }
  [Hash.symbol](): number {
    return Hash.string(this.value)
  }
}

export interface EphemeralView {
  readonly spaceId: Identity.SpaceId
  readonly revision: Identity.EphemeralRevision
  readonly members: ReadonlyArray<Protocol.EphemeralMemberEntry>
  readonly states: HashMap.HashMap<string, Protocol.EphemeralStateEntry>
  readonly events: ReadonlyArray<Protocol.EphemeralEventEntry>
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

const stateRecord = (
  entry: Protocol.EphemeralStateEntry
): readonly [string, Protocol.EphemeralStateEntry] => [
  stateIdentity(entry.member, entry.channel, entry.key),
  entry
]

const reduceEphemeral = (
  current: EphemeralView | undefined,
  message: Protocol.EphemeralMessage
): EphemeralView | undefined => {
  if (message._tag === "Snapshot") {
    return {
      spaceId: message.spaceId,
      revision: message.revision,
      members: message.members,
      states: HashMap.fromIterable(message.states.map(stateRecord)),
      events: []
    }
  }
  if (current === undefined) return undefined
  if (message._tag === "MemberUpserted") {
    return {
      ...current,
      revision: message.revision,
      members: [
        ...current.members.filter((entry) => !sameMember(entry.member, message.entry.member)),
        message.entry
      ]
    }
  }
  if (message._tag === "MemberLeft") {
    return {
      ...current,
      revision: message.revision,
      members: current.members.filter((entry) => !sameMember(entry.member, message.member)),
      events: current.events.filter((entry) => !sameMember(entry.member, message.member))
    }
  }
  if (message._tag === "StateSet") {
    return {
      ...current,
      revision: message.revision,
      states: HashMap.set(
        current.states,
        stateIdentity(message.entry.member, message.entry.channel, message.entry.key),
        message.entry
      )
    }
  }
  if (message._tag === "StateRemoved") {
    return {
      ...current,
      revision: message.revision,
      states: HashMap.remove(
        current.states,
        stateIdentity(message.member, message.channel, message.key)
      )
    }
  }
  if (message._tag === "Event") {
    return {
      ...current,
      revision: message.revision,
      events: [
        ...current.events.filter((entry) =>
          !sameMember(entry.member, message.entry.member) || entry.channel !== message.entry.channel
        ),
        message.entry
      ]
    }
  }
  return {
    ...current,
    revision: message.revision,
    events: current.events.filter((entry) =>
      !sameMember(entry.member, message.member) || entry.channel !== message.channel
    )
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

  const ephemeral = Atom.family((key: EphemeralJoinKey) => {
    const stream = Stream.unwrap(
      Effect.gen(function*() {
        const client = yield* EphemeralClient.EphemeralClient
        return client.join(key.request).pipe(
          Stream.mapAccum(
            (): EphemeralView | undefined => undefined,
            (current, message) => {
              const next = reduceEphemeral(current, message)
              if (next === undefined) return [current, []] as const
              return [next, [next]] as const
            }
          )
        )
      })
    )
    return runtime.atom(stream).pipe(Atom.setIdleTTL(idleTTL))
  })
  const ephemeralView = (request: EphemeralClient.JoinRequest) => ephemeral(new EphemeralJoinKey(request))
  const publishEphemeral = runtime.fn<EphemeralClient.PublishRequest>()(
    (request) => EphemeralClient.EphemeralClient.use((client) => client.publish(request)),
    { concurrent: true }
  )

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
      Replica.Replica.use((replica) => replica.space(spaceId).pipe(Effect.map((space) => space.settlements)))
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
    ephemeral: ephemeralView,
    publishEphemeral
  } as const
}
