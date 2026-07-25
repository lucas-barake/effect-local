import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import type * as Stream from "effect/Stream"
import type * as Identity from "./Identity.js"
import type * as ReplicaError from "./ReplicaError.js"

export interface Capabilities {
  readonly storeAndForward: boolean
  /**
   * Whether the peer compares document lineage before it merges a sync message.
   *
   * Optional rather than required: a peer that predates lineage advertises nothing, and an absent
   * value means exactly that. Such a peer has no cross lineage check, so it would union a rewritten
   * document and push the discarded history straight back. The send path treats an absent value as
   * "not lineage aware" and refuses to emit a rewritten document to it.
   */
  readonly lineageAware?: boolean
}

export interface Connection {
  readonly peerId: Identity.PeerId
  readonly capabilities: Capabilities
  readonly receive: Stream.Stream<Uint8Array, ReplicaError.ReplicaError>
  readonly send: (message: Uint8Array) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly close: Effect.Effect<void>
}

export interface ConnectOptions {
  readonly replicaId: Identity.ReplicaId
  readonly peerId: Identity.PeerId
}

export class PeerTransport extends Context.Service<PeerTransport, {
  readonly capabilities: Capabilities
  readonly connect: (options: ConnectOptions) => Effect.Effect<Connection, ReplicaError.ReplicaError, Scope.Scope>
}>()("@lucas-barake/effect-local/PeerTransport") {}
