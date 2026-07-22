import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Document from "@lucas-barake/effect-local/Document"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Hash from "effect/Hash"
import * as Layer from "effect/Layer"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Atom } from "effect/unstable/reactivity"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as ReplicaClient from "./ReplicaClient.js"

export const layerReactivity = Layer.effectDiscard(Effect.gen(function*() {
  const replica = yield* ReplicaClient.ReplicaClient
  const reactivity = yield* Reactivity.Reactivity
  yield* replica.invalidations.pipe(
    Stream.runForEach((event) => reactivity.invalidate(event.keys)),
    Effect.retry({
      schedule: Schedule.spaced(1_000),
      while: (error) => error.reason._tag === "StorageUnavailable"
    }),
    Effect.tapCause(Effect.logError),
    Effect.ignore,
    Effect.forkScoped
  )
}))

class QueryKey<P,> implements Equal.Equal {
  readonly key: string
  readonly payload: P

  constructor(key: string, payload: P) {
    this.key = key
    this.payload = payload
  }

  [Equal.symbol](that: unknown): boolean {
    return that instanceof QueryKey && this.key === that.key
  }

  [Hash.symbol](): number {
    return Hash.string(this.key)
  }
}

export const documentFamily = <R, E, D extends Document.Any,>(
  runtime: Atom.AtomRuntime<Replica.Replica | R, E>,
  document: D
) =>
  Atom.family((documentId: Identity.DocumentId) =>
    runtime.atom(Replica.Replica.use((replica) => replica.get(document, documentId))).pipe(
      runtime.factory.withReactivity([document.name])
    )
  )

export const queryFamily = <R, E, Q extends Query.Any,>(
  runtime: Atom.AtomRuntime<Replica.Replica | R, E>,
  query: Q
) => {
  const family = Atom.family((entry: QueryKey<Q["payloadSchema"]["Type"]>) =>
    runtime.atom(Replica.Replica.use((replica) => {
      const execute = replica.query as unknown as (
        query: Q,
        payload: Q["payloadSchema"]["Type"]
      ) => Effect.Effect<Q["successSchema"]["Type"], Q["errorSchema"]["Type"] | ReplicaError.ReplicaError>
      return execute(query, entry.payload)
    })).pipe(
      runtime.factory.withReactivity([
        ...new Set(query.dependsOn.flatMap((projection) => [projection.name, projection.document.name]))
      ])
    )
  )
  return (
    ...payload: [Q["payloadSchema"]["Type"]] extends [void] ? readonly []
      : readonly [payload: Q["payloadSchema"]["Type"]]
  ) => {
    const value = payload[0] as Q["payloadSchema"]["Type"]
    const encoded = Schema.encodeSync(query.payloadSchema)(value)
    const key = `${query.name}:${query.version}:${payload.length === 0 ? "void" : Canonical.hash(encoded)}`
    return family(new QueryKey(key, value))
  }
}

export const mutation = <R, E, M extends Mutation.Any,>(
  runtime: Atom.AtomRuntime<Replica.Replica | R, E>,
  definition: M
) =>
  runtime.fn<
    {
      readonly commandId: Identity.CommandId
      readonly documentId: Identity.DocumentId
    } & ([M["payloadSchema"]["Type"]] extends [void] ? object : { readonly payload: M["payloadSchema"]["Type"] })
  >()(
    (options) => Replica.Replica.use((replica) => replica.mutate(definition, options)),
    { concurrent: true, reactivityKeys: [definition.document.name] }
  )

export const status = <R, E,>(runtime: Atom.AtomRuntime<Replica.Replica | R, E>) =>
  runtime.atom(
    Replica.Replica.pipe(
      Effect.map((replica) => replica.status),
      Stream.unwrap
    )
  )
