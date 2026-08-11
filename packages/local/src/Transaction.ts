import type * as Effect from "effect/Effect"
import type * as Option from "effect/Option"
import type * as Schema from "effect/Schema"
import type * as Field from "./Field.js"
import type * as Model from "./Model.js"
import type * as ReplicaError from "./ReplicaError.js"

export interface Transaction {
  readonly get: <M extends Model.Any,>(
    model: M,
    key: Model.Key<M>
  ) => Effect.Effect<Option.Option<Model.Value<M>>, ReplicaError.StorageError>
  readonly set: <M extends Model.Any,>(
    model: M,
    key: Model.Key<M>,
    value: Model.Value<M>
  ) => Effect.Effect<void, ReplicaError.StorageError>
  readonly delete: <M extends Model.Any,>(model: M, key: Model.Key<M>) => Effect.Effect<void, ReplicaError.StorageError>
  readonly applyField: <Value extends Schema.Top, Operation extends Schema.Top, E,>(
    semantics: Field.Semantics<Value, Operation, E>,
    current: Value["Type"],
    operation: Operation["Type"]
  ) => Effect.Effect<Value["Type"], E>
}

export interface Query {
  readonly get: Transaction["get"]
  readonly all: <M extends Model.Any,>(
    model: M
  ) => Effect.Effect<ReadonlyArray<Model.Value<M>>, ReplicaError.StorageError>
}
