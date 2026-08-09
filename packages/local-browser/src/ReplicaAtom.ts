/**
 * Atom bindings for a browser replica client.
 *
 * The neutral factories (documents, queries, mutations, conflicts) are shared with every
 * in-process runtime and re-exported from `@lucas-barake/effect-local-sql/ReplicaAtom`.
 * What stays here is browser specific: `layerReactivity` bridges the owner's invalidation
 * stream from `ReplicaClient` into `Reactivity`, and the status atoms retry on a replaced
 * owner session, because a tab talks to a remote owner that can be taken over or expire
 * mid-stream.
 */
import * as PeerConnectionStatus from "@lucas-barake/effect-local-sql/PeerConnectionStatus"
import * as RelayConnectionStatus from "@lucas-barake/effect-local-sql/RelayConnectionStatus"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schedule from "effect/Schedule"
import * as Stream from "effect/Stream"
import type { Atom } from "effect/unstable/reactivity"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as ReplicaClient from "./ReplicaClient.js"

export {
  commandDeliveryFamily,
  conflictFamily,
  documentFamily,
  mutation,
  queryFamily,
  resolveConflict
} from "@lucas-barake/effect-local-sql/ReplicaAtom"

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
    Effect.repeat(Schedule.spaced(1_000)),
    Effect.forkScoped
  )
})).pipe(Layer.fresh)

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
