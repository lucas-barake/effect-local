import * as QueryReactivity from "@lucas-barake/effect-local-sql/QueryReactivity"
import * as Attachment from "@lucas-barake/effect-local/Attachment"
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

class AttachmentKey implements Equal.Equal {
  readonly spaceId: Identity.SpaceId
  readonly reference: Attachment.Reference
  readonly value: string
  constructor(spaceId: Identity.SpaceId, reference: Attachment.Reference) {
    this.spaceId = spaceId
    this.reference = reference
    this.value = `${spaceId}:${reference.digest}:${reference.bytes}`
  }
  [Equal.symbol](that: unknown): boolean {
    return that instanceof AttachmentKey && this.value === that.value
  }
  [Hash.symbol](): number {
    return Hash.string(this.value)
  }
}

export const make = <E,>(
  layer: Layer.Layer<
    Replica.Replica | QueryReactivity.QueryReactivity,
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

  const spaceKey = (spaceId: Identity.SpaceId) => `effect-local:space:${spaceId}`
  const attachmentFamily = Atom.family((key: AttachmentKey) =>
    runtime.atom(
      Replica.Replica.use((replica) =>
        replica.space(key.spaceId).pipe(
          Effect.flatMap(
            Effect.fnUntraced(function*(space) {
              const bytes = new Uint8Array(key.reference.bytes)
              const actual = yield* space.readAttachment(key.reference).pipe(
                Stream.runFoldEffect(() => 0, (offset, chunk) => {
                  const nextOffset = offset + chunk.length
                  if (nextOffset > key.reference.bytes) {
                    return Effect.fail(
                      new Attachment.AttachmentLengthMismatch({
                        expected: key.reference.bytes,
                        actual: nextOffset
                      })
                    )
                  }
                  bytes.set(chunk, offset)
                  return Effect.succeed(nextOffset)
                })
              )
              if (actual !== key.reference.bytes) {
                return yield* new Attachment.AttachmentLengthMismatch({
                  expected: key.reference.bytes,
                  actual
                })
              }
              return bytes
            })
          )
        )
      )
    ).pipe(Atom.setIdleTTL(idleTTL))
  )
  const attachment = (spaceId: Identity.SpaceId, reference: Attachment.Reference) =>
    attachmentFamily(new AttachmentKey(spaceId, reference))
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
    attachment,
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
    leave
  } as const
}
