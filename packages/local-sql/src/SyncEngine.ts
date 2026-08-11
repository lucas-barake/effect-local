import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Stream from "effect/Stream"

export interface Service {
  readonly submit: (request: Protocol.SubmitRequest) => Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError>
  readonly pull: (request: Protocol.PullRequest) => Effect.Effect<Protocol.PullResult, ReplicaError.ReplicaError>
  readonly bootstrap: (
    request: Protocol.BootstrapRequest
  ) => Effect.Effect<Protocol.BootstrapPage, ReplicaError.ReplicaError>
  readonly watch: (request: Protocol.WatchRequest) => Stream.Stream<Protocol.Wake, ReplicaError.ReplicaError>
}

export class SyncEngine extends Context.Service<SyncEngine, Service>()(
  "@lucas-barake/effect-local-sql/SyncEngine"
) {}
