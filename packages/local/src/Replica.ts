import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Option from "effect/Option"
import type * as Identity from "./Identity.js"
import type * as Model from "./Model.js"
import type * as Mutation from "./Mutation.js"
import type * as Protocol from "./Protocol.js"
import type * as Query from "./Query.js"
import type * as ReplicaError from "./ReplicaError.js"
import type * as ReplicaStatus from "./ReplicaStatus.js"

export interface Service {
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
  readonly status: Effect.Effect<ReplicaStatus.ReplicaStatus, ReplicaError.ReplicaError>
}

export class Replica extends Context.Service<Replica, Service>()("@lucas-barake/effect-local/Replica") {}
