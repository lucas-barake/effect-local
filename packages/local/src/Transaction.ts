import type * as Effect from "effect/Effect"
import type * as Option from "effect/Option"
import type * as Schema from "effect/Schema"
import type * as Statement from "effect/unstable/sql/Statement"
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
  readonly applyField: <Value extends Schema.Top, Operation extends Schema.Top, E extends Field.TaggedError,>(
    semantics: Field.Semantics<Value, Operation, E>,
    current: Value["Type"],
    operation: Operation["Type"]
  ) => Effect.Effect<Value["Type"], E>
}

export interface Query {
  readonly get: Transaction["get"]
  /**
   * Runs one raw SQL statement over the declared models with the full power of SQLite: joins,
   * subqueries, aggregates, window functions, recursive CTE continuations.
   *
   * Each model in `models` becomes a CTE named after the model, scoped to the space and its
   * active generations, with a `key` column (the canonical entity key), a `value` column (the
   * encoded entity JSON), and one column per top-level field of the model schema extracted from
   * the JSON. The statement begins with the query body (`SELECT`/`VALUES`) or continues the CTE
   * list with a leading `, name AS (...)`; it must not open its own `WITH`.
   *
   * `models` also drives reactivity: the query re-runs when any entity of a declared model
   * changes. A model the statement reads but `models` omits yields stale results without an
   * error, so the list must be complete. Rows come back raw for the caller to decode - pass this
   * as the `execute` of `SqlSchema.findAll` and friends, or rely on the query's success schema.
   */
  readonly sql: (
    models: ReadonlyArray<Model.Any>,
    statement: (sql: Statement.Constructor) => Statement.Statement<unknown>
  ) => Effect.Effect<Array<unknown>, ReplicaError.QueryError>
}
