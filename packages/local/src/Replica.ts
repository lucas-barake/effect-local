import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as Stream from "effect/Stream"
import type * as Attachment from "./Attachment.js"
import type * as Identity from "./Identity.js"
import type * as Model from "./Model.js"
import type * as Mutation from "./Mutation.js"
import type * as Protocol from "./Protocol.js"
import type * as Quarantine from "./Quarantine.js"
import type * as Query from "./Query.js"
import type * as ReplicaError from "./ReplicaError.js"
import type * as ReplicaStatus from "./ReplicaStatus.js"

export type PendingMutation<M extends Mutation.Any = Mutation.Any,> =
  & Omit<Protocol.PendingMutation, "optimisticResult">
  & {
    readonly payload: Mutation.Payload<M>
  }

export type AcceptedReceipt<M extends Mutation.Any,> = Omit<Protocol.AcceptedReceipt, "name" | "result"> & {
  readonly name: M["name"]
  readonly result: Mutation.Success<M>
}

type RawRejectedReceipt<M extends Mutation.Any, Origin extends Protocol.RejectionOrigin,> =
  & Omit<Protocol.RejectedReceipt, "name" | "origin" | "rejection">
  & {
    readonly name: M["name"]
    readonly origin: Origin
    readonly rejection: typeof Schema.Json.Type
  }

export type MutationRejectedReceipt<M extends Mutation.Any,> =
  & Omit<Protocol.RejectedReceipt, "name" | "origin" | "rejection">
  & {
    readonly name: M["name"]
    readonly origin: "Mutation"
    readonly rejection: Mutation.Rejection<M>
  }

export type RejectedReceipt<M extends Mutation.Any,> =
  | MutationRejectedReceipt<M>
  | RawRejectedReceipt<M, Exclude<Protocol.RejectionOrigin, "Mutation">>

export type Receipt<M extends Mutation.Any,> =
  | AcceptedReceipt<M>
  | RejectedReceipt<M>
  | Protocol.LegacyReceipt
  | Protocol.ExpiredReceipt

export type MutationSettlement<M extends Mutation.Any = Mutation.Any,> =
  | {
    readonly pending: PendingMutation<M>
    readonly receipt: Exclude<Receipt<M>, Protocol.LegacyReceipt>
  }
  | {
    readonly pending: Protocol.PendingMutation
    readonly receipt: Protocol.LegacyReceipt
  }

export const Activation = Schema.Literals(["Inactive", "Activating", "Active", "Deactivating"])
export type Activation = typeof Activation.Type

export interface Space {
  readonly spaceId: Identity.SpaceId
  readonly scope: Effect.Effect<Protocol.ReplicationScope, ReplicaError.ReplicaError>
  readonly setScope: (scope: Protocol.ReplicationScope) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly activation: Effect.Effect<Activation, ReplicaError.ReplicaError>
  readonly activate: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly deactivate: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly stageAttachment: <E extends { readonly _tag: string }, R,>(
    bytes: Stream.Stream<Uint8Array, E, R>
  ) => Effect.Effect<Attachment.Reference, E | ReplicaError.ReplicaError, R>
  readonly readAttachment: (
    reference: Attachment.Reference,
    range?: Attachment.Range
  ) => Stream.Stream<Uint8Array, ReplicaError.ReplicaError>
  readonly releaseAttachment: (
    reference: Attachment.Reference
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
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
  readonly receipt: <M extends Mutation.Any,>(
    mutation: M,
    mutationId: Identity.MutationId
  ) => Effect.Effect<Option.Option<Receipt<M>>, ReplicaError.ReplicaError>
  readonly pending: Effect.Effect<ReadonlyArray<PendingMutation>, ReplicaError.ReplicaError>
  readonly pendingFor: <M extends Mutation.Any,>(
    mutation: M
  ) => Effect.Effect<ReadonlyArray<PendingMutation<M>>, ReplicaError.ReplicaError>
  readonly settlements: Stream.Stream<MutationSettlement, ReplicaError.ReplicaError>
  readonly settlementsFor: <M extends Mutation.Any,>(
    mutation: M
  ) => Stream.Stream<MutationSettlement<M>, ReplicaError.ReplicaError>
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
