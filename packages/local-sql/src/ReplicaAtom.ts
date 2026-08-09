/**
 * Atom bindings for an in-process replica.
 *
 * These factories are platform neutral: they depend only on the `Replica.Replica` service
 * and the status services owned by this package, so any runtime that composes `SqlReplica`
 * in-process (React Native, tests, Node scripts) can use them directly. Commit reactivity
 * needs no bridge here: `CommitPublisher` invalidates `Reactivity` keys in the same layer
 * graph, so atoms built with `runtime.factory.withReactivity` refresh on commit.
 *
 * The browser package re-exports the neutral factories and keeps its own retrying status
 * atoms, because a browser tab talks to a remote owner whose session can be replaced
 * mid-stream. In-process there is no owner session, so the status atoms here subscribe
 * without retry logic.
 */
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Conflict from "@lucas-barake/effect-local/Conflict"
import type * as Document from "@lucas-barake/effect-local/Document"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Hash from "effect/Hash"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Atom } from "effect/unstable/reactivity"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as PeerConnectionStatus from "./PeerConnectionStatus.js"
import * as RelayConnectionStatus from "./RelayConnectionStatus.js"

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
      runtime.factory.withReactivity([
        ReplicaDefinition.documentInstanceKey(document.name, documentId),
        ReplicaDefinition.documentTypeRefreshKey(document.name)
      ])
    )
  )

export const conflictFamily = <R, E, D extends Document.Any,>(
  runtime: Atom.AtomRuntime<Replica.Replica | R, E>,
  document: D
) =>
  Atom.family((documentId: Identity.DocumentId) =>
    runtime.atom(Replica.Replica.use((replica) => replica.inspectConflicts(document, documentId))).pipe(
      runtime.factory.withReactivity([
        ReplicaDefinition.documentInstanceKey(document.name, documentId),
        ReplicaDefinition.documentTypeRefreshKey(document.name)
      ])
    )
  )

export const commandDeliveryFamily = <R, E,>(
  runtime: Atom.AtomRuntime<Replica.Replica | R, E>
) =>
  Atom.family((commandId: Identity.CommandId) =>
    runtime.atom(
      Stream.unwrap(
        Replica.Replica.use((replica) => Effect.succeed(replica.commandDeliveryChanges(commandId)))
      )
    )
  )

export const queryFamily = <R, E, Q extends Query.Any,>(
  runtime: Atom.AtomRuntime<Replica.Replica | R, E>,
  query: Q
) => {
  const family = Atom.family((entry: QueryKey<Q["payloadSchema"]["Type"]>) =>
    runtime.atom(Replica.Replica.use((replica) => {
      const execute = replica.query as (
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
    const value = payload[0]
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
    (options) =>
      Replica.Replica.use((replica) => replica.mutate(definition, options)).pipe(
        Reactivity.mutation(ReplicaDefinition.documentCommitKeys(definition.document.name, options.documentId))
      ),
    { concurrent: true }
  )

export const resolveConflict = <R, E, D extends Document.Any,>(
  runtime: Atom.AtomRuntime<Replica.Replica | R, E>,
  document: D
) =>
  runtime.fn<{
    readonly commandId: Identity.CommandId
    readonly documentId: Identity.DocumentId
    readonly resolution: Conflict.Resolution
  }>()(
    (options) =>
      Replica.Replica.use((replica) => replica.resolveConflict(document, options)).pipe(
        Reactivity.mutation(ReplicaDefinition.documentCommitKeys(document.name, options.documentId))
      ),
    { concurrent: true }
  )

export const status = <R, E,>(runtime: Atom.AtomRuntime<Replica.Replica | R, E>) =>
  runtime.atom(
    Replica.Replica.pipe(
      Effect.map((replica) => replica.status),
      Stream.unwrap
    )
  )

export const peerConnectionStatus = <R, E,>(
  runtime: Atom.AtomRuntime<PeerConnectionStatus.PeerConnectionStatus | R, E>,
  peerId: Identity.PeerId
) =>
  runtime.atom(
    PeerConnectionStatus.PeerConnectionStatus.pipe(
      Effect.map((service) => service.status(peerId)),
      Stream.unwrap
    )
  )

/**
 * Whether this replica can reach its relay.
 *
 * Distinct from {@link peerConnectionStatus}, which reports one peer session. Every peer session
 * runs over one socket, so when the relay drops they all go quiet at once and per peer status alone
 * would render that as several peers vanishing rather than as one link failing. A replica with no
 * relay in its topology reports `NotConfigured` rather than a `Disconnected` that would imply a link
 * exists.
 */
export const relayConnectionStatus = <R, E,>(
  runtime: Atom.AtomRuntime<RelayConnectionStatus.RelayConnectionStatus | R, E>
) =>
  runtime.atom(
    RelayConnectionStatus.RelayConnectionStatus.pipe(
      Effect.map((service) => service.status),
      Stream.unwrap
    )
  )
