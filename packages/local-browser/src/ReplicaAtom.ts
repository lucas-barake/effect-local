import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Conflict from "@lucas-barake/effect-local/Conflict"
import type * as Document from "@lucas-barake/effect-local/Document"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
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
    // Restarting after a noninterrupt failure keeps remote invalidations live. Interruption remains
    // terminal so the owning scope controls shutdown.
    Effect.catchCause((cause) =>
      Cause.hasInterrupts(cause)
        ? Effect.failCause(cause)
        : Effect.logWarning("replica invalidation stream restarting", cause)
    ),
    Effect.andThen(Effect.sleep(1_000)),
    Effect.forever,
    Effect.forkScoped
  )
})).pipe(Layer.fresh)

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
      Stream.unwrap,
      Stream.retry(
        Schedule.spaced("1 second").pipe(
          Schedule.setInputType<ReplicaError.ReplicaError>(),
          // A session the owner no longer knows is replaced by `ReplicaClient` before the stream
          // goes terminal, so resubscribing lands on the new owner after a takeover or a lease
          // expiry instead of stranding the tab on the old owner's terminal mismatch. Any other
          // protocol mismatch stays terminal: it means deployment skew, and the tab must reload.
          Schedule.while(({ input }) =>
            input.reason._tag === "ProtocolMismatch" && input.reason.expected === "active session"
          )
        )
      )
    )
  )

export const peerConnectionStatus = <R, E,>(
  runtime: Atom.AtomRuntime<PeerConnectionStatus.PeerConnectionStatus | R, E>,
  peerId: Identity.PeerId
) =>
  runtime.atom(
    PeerConnectionStatus.PeerConnectionStatus.pipe(
      Effect.map((service) => service.status(peerId)),
      Stream.unwrap,
      Stream.retry(
        Schedule.spaced("1 second").pipe(
          Schedule.setInputType<ReplicaError.ReplicaError>(),
          Schedule.while(({ input }) =>
            input.reason._tag === "ProtocolMismatch" && input.reason.expected === "active session"
          )
        )
      )
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
      Stream.unwrap,
      // Same resubscribe rule as the other status atoms: a lapsed owner session is recoverable and
      // anything else is a deployment mismatch the page has to be told about.
      Stream.retry(
        Schedule.spaced("1 second").pipe(
          Schedule.setInputType<ReplicaError.ReplicaError>(),
          Schedule.while(({ input }) =>
            input.reason._tag === "ProtocolMismatch" && input.reason.expected === "active session"
          )
        )
      )
    )
  )
