import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Option from "effect/Option"
import type * as Identity from "./Identity.js"
import type * as Model from "./Model.js"
import type * as Mutation from "./Mutation.js"
import type * as Protocol from "./Protocol.js"
import type * as Quarantine from "./Quarantine.js"
import type * as Query from "./Query.js"
import type * as ReplicaError from "./ReplicaError.js"
import type * as ReplicaStatus from "./ReplicaStatus.js"

export interface Space {
  readonly spaceId: Identity.SpaceId
  readonly mutate: <M extends Mutation.Any,>(
    mutation: M,
    payload: Mutation.Payload<M>
  ) => Effect.Effect<Protocol.PendingMutation, ReplicaError.ReplicaError | Mutation.Rejection<M>>
  readonly get: <M extends Model.Any,>(
    model: M,
    key: Model.Key<M>
  ) => Effect.Effect<Option.Option<Model.Value<M>>, ReplicaError.ReplicaError>
  readonly query: <Q extends Query.Any,>(
    query: Q,
    payload: Q["payloadSchema"]["Type"]
  ) => Effect.Effect<Q["successSchema"]["Type"], ReplicaError.ReplicaError | Q["errorSchema"]["Type"]>
  readonly receipt: (
    mutationId: Identity.MutationId
  ) => Effect.Effect<Option.Option<Protocol.Receipt>, ReplicaError.ReplicaError>
  readonly quarantine: Effect.Effect<ReadonlyArray<Quarantine.QuarantinedMutation>, ReplicaError.ReplicaError>
  readonly discardQuarantined: (
    mutationId: Identity.MutationId
  ) => Effect.Effect<Protocol.Receipt, ReplicaError.ReplicaError>
  readonly resubmitQuarantined: <M extends Mutation.Any,>(
    mutationId: Identity.MutationId,
    mutation: M,
    payload: Mutation.Payload<M>
  ) => Effect.Effect<Quarantine.ResubmitResult, ReplicaError.ReplicaError | Mutation.Rejection<M>>
  readonly status: Effect.Effect<ReplicaStatus.SpaceStatus, ReplicaError.ReplicaError>
}

export interface Service {
  readonly join: (spaceId: Identity.SpaceId) => Effect.Effect<Space, ReplicaError.ReplicaError>
  readonly leave: (spaceId: Identity.SpaceId) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly spaces: Effect.Effect<ReadonlyArray<Space>, ReplicaError.ReplicaError>
  readonly space: (
    spaceId: Identity.SpaceId
  ) => Effect.Effect<Space, ReplicaError.ReplicaError>
  readonly status: Effect.Effect<ReplicaStatus.Aggregate, ReplicaError.ReplicaError>
}

export class Replica extends Context.Service<Replica, Service>()("@lucas-barake/effect-local/Replica") {}
