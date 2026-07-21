import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import type * as Stream from "effect/Stream"
import type * as Identity from "./Identity.js"
import type * as ReplicaError from "./ReplicaError.js"

export interface Capabilities {
  readonly storeAndForward: boolean
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
