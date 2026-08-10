import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Hash from "effect/Hash"
import type * as Layer from "effect/Layer"
import { Atom } from "effect/unstable/reactivity"
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

export const make = <R, E,>(
  layer: Layer.Layer<Replica.Replica | R, E, AtomRegistry.AtomRegistry | Reactivity.Reactivity>,
  options?: {
    readonly factory?: Atom.RuntimeFactory
    readonly idleTTL?: Duration.Input
  }
) => {
  const factory = options?.factory ?? Atom.runtime
  const runtime = factory(layer)
  const idleTTL = Duration.toMillis(options?.idleTTL ?? Duration.seconds(30))

  const entity = <M extends Model.Any,>(model: M) =>
    Atom.family((key: Model.Key<M>) =>
      runtime.atom(Replica.Replica.use((replica) => replica.get(model, key))).pipe(
        factory.withReactivity({ "effect-local:entities": [[model.name, key]] }),
        Atom.setIdleTTL(idleTTL)
      )
    )

  const query = <Q extends Query.Any,>(definition: Q) => {
    const dependencyKeys = Array.from(
      new Set(definition.dependsOn.map((model) => model.name)),
      (name) => [name]
    )
    const family = Atom.family((key: QueryKey<Q["payloadSchema"]["Type"]>) =>
      runtime.atom(Replica.Replica.use((replica) => replica.query(definition, key.payload))).pipe(
        factory.withReactivity({
          "effect-local:entities": dependencyKeys
        }),
        Atom.setIdleTTL(idleTTL)
      )
    )
    return (payload: Q["payloadSchema"]["Type"]) => {
      return family(new QueryKey(`${definition.name}:${Canonical.hash(payload)}`, payload))
    }
  }

  const mutation = <M extends Mutation.Any,>(definition: M) =>
    runtime.fn<Mutation.Payload<M>>()(
      (payload) =>
        Effect.yieldNow.pipe(
          Effect.andThen(Replica.Replica.use((replica) => replica.mutate(definition, payload)))
        ),
      { concurrent: true }
    )

  const receipt = Atom.family((mutationId: Identity.MutationId) =>
    runtime.atom(Replica.Replica.use((replica) => replica.receipt(mutationId))).pipe(
      factory.withReactivity([`effect-local:receipt:${mutationId}`]),
      Atom.setIdleTTL(idleTTL)
    )
  )

  const status = runtime.atom(Replica.Replica.use((replica) => replica.status)).pipe(
    factory.withReactivity(["effect-local:status"])
  )

  return { factory, runtime, entity, query, mutation, receipt, status } as const
}
