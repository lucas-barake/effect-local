import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Document from "@lucas-barake/effect-local/Document"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import type * as Snapshot from "@lucas-barake/effect-local/Snapshot"
import type * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Hash from "effect/Hash"
import * as Layer from "effect/Layer"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { type AsyncResult, Atom, AtomRegistry } from "effect/unstable/reactivity"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as ReplicaClient from "./ReplicaClient.js"

export interface Service {
  readonly registry: AtomRegistry.AtomRegistry
  readonly status: Atom.Atom<
    AsyncResult.AsyncResult<
      ReplicaStatus.ReplicaStatus,
      ReplicaError.ReplicaError | Cause.NoSuchElementError
    >
  >
  readonly get: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Atom.Atom<AsyncResult.AsyncResult<Snapshot.FromDocument<D>, ReplicaError.ReplicaError>>
  readonly query: <Q extends Query.Any,>(
    query: Q,
    ...payload: [Q["payload"]["Type"]] extends [void] ? readonly [] : readonly [payload: Q["payload"]["Type"]]
  ) => Atom.Atom<AsyncResult.AsyncResult<Q["success"]["Type"], Q["error"]["Type"] | ReplicaError.ReplicaError>>
  readonly refresh: <A,>(atom: Atom.Atom<A>) => void
  readonly mount: <A,>(atom: Atom.Atom<A>) => () => void
}

export class ReplicaAtom extends Context.Service<ReplicaAtom, Service>()(
  "@lucas-barake/effect-local-browser/ReplicaAtom"
) {}

export const reactivityLayer = Layer.effectDiscard(Effect.gen(function*() {
  const replica = yield* ReplicaClient.ReplicaClient
  const reactivity = yield* Reactivity.Reactivity
  yield* replica.invalidations.pipe(
    Stream.runForEach((event) => reactivity.invalidate(event.keys)),
    Effect.retry({
      times: 1,
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

export const layer = Layer.effect(
  ReplicaAtom,
  Effect.gen(function*() {
    const registry = yield* AtomRegistry.AtomRegistry
    const replica = yield* Replica.Replica
    return {
      registry,
      status: Atom.make(replica.status),
      get: (document, documentId) => Atom.make(replica.get(document, documentId)),
      query: (query, ...payload) => Atom.make(replica.query(query, ...payload as never)),
      refresh: registry.refresh,
      mount: (atom) => registry.mount(atom)
    }
  })
)

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
  const family = Atom.family((entry: QueryKey<Q["payload"]["Type"]>) =>
    runtime.atom(Replica.Replica.use((replica) => {
      const execute = replica.query as unknown as (
        query: Q,
        payload: Q["payload"]["Type"]
      ) => Effect.Effect<Q["success"]["Type"], Q["error"]["Type"] | ReplicaError.ReplicaError>
      return execute(query, entry.payload)
    })).pipe(
      runtime.factory.withReactivity([
        ...new Set(query.dependsOn.flatMap((projection) => [projection.name, projection.document.name]))
      ])
    )
  )
  return (
    ...payload: [Q["payload"]["Type"]] extends [void] ? readonly []
      : readonly [payload: Q["payload"]["Type"]]
  ) => {
    const value = payload[0] as Q["payload"]["Type"]
    const encoded = Schema.encodeSync(query.payload)(value)
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
    } & ([M["payload"]["Type"]] extends [void] ? object : { readonly payload: M["payload"]["Type"] })
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
