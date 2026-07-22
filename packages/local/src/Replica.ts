import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Stream from "effect/Stream"
import type * as Backup from "./Backup.js"
import type * as CommandOutcome from "./CommandOutcome.js"
import type * as Document from "./Document.js"
import type * as Identity from "./Identity.js"
import type * as Mutation from "./Mutation.js"
import type * as Query from "./Query.js"
import type * as ReplicaError from "./ReplicaError.js"
import type * as ReplicaStatus from "./ReplicaStatus.js"
import type * as Snapshot from "./Snapshot.js"

export class Replica extends Context.Service<Replica, {
  readonly create: <D extends Document.Any,>(
    document: D,
    options: {
      readonly commandId: Identity.CommandId
      readonly value: D["schema"]["Type"]
    }
  ) => Effect.Effect<CommandOutcome.CommandOutcome<Identity.DocumentId>, ReplicaError.ReplicaError>
  readonly get: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<Snapshot.FromDocument<D>, ReplicaError.ReplicaError>
  readonly mutate: <M extends Mutation.Any,>(
    mutation: M,
    options: {
      readonly commandId: Identity.CommandId
      readonly documentId: Identity.DocumentId
    } & ([M["payloadSchema"]["Type"]] extends [void] ? object : { readonly payload: M["payloadSchema"]["Type"] })
  ) => Effect.Effect<
    CommandOutcome.CommandOutcome<M["successSchema"]["Type"], M["errorSchema"]["Type"]>,
    ReplicaError.ReplicaError
  >
  readonly delete: <D extends Document.Any,>(
    document: D,
    options: {
      readonly commandId: Identity.CommandId
      readonly documentId: Identity.DocumentId
    }
  ) => Effect.Effect<CommandOutcome.CommandOutcome<void>, ReplicaError.ReplicaError>
  readonly query: <Q extends Query.Any,>(
    query: Q,
    ...payload: [Q["payloadSchema"]["Type"]] extends [void] ? readonly []
      : readonly [payload: Q["payloadSchema"]["Type"]]
  ) => Effect.Effect<Q["successSchema"]["Type"], Q["errorSchema"]["Type"] | ReplicaError.ReplicaError>
  readonly lookupMutation: <M extends Mutation.Any,>(
    mutation: M,
    commandId: Identity.CommandId
  ) => Effect.Effect<
    CommandOutcome.CommandOutcome<M["successSchema"]["Type"], M["errorSchema"]["Type"]>,
    ReplicaError.ReplicaError
  >
  readonly lookupCreate: <D extends Document.Any,>(
    document: D,
    commandId: Identity.CommandId
  ) => Effect.Effect<CommandOutcome.CommandOutcome<Identity.DocumentId>, ReplicaError.ReplicaError>
  readonly lookupDelete: <D extends Document.Any,>(
    document: D,
    commandId: Identity.CommandId
  ) => Effect.Effect<CommandOutcome.CommandOutcome<void>, ReplicaError.ReplicaError>
  readonly flush: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly status: Stream.Stream<ReplicaStatus.ReplicaStatus, ReplicaError.ReplicaError>
  readonly exportBackup: (options: Backup.ExportOptions) => Stream.Stream<Uint8Array, ReplicaError.ReplicaError>
  readonly restoreBackup: <R,>(
    options: Backup.RestoreOptions<R>
  ) => Effect.Effect<void, ReplicaError.ReplicaError, R>
  readonly exportDocument: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<Backup.ExportedDocument<D["schema"]["Encoded"]>, ReplicaError.ReplicaError>
  readonly importDocument: <D extends Document.Any,>(
    document: D,
    options: {
      readonly commandId: Identity.CommandId
      readonly value: Backup.ExportedDocument<D["schema"]["Encoded"]>
    }
  ) => Effect.Effect<CommandOutcome.CommandOutcome<Identity.DocumentId>, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local/Replica") {}
