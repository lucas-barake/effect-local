import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Transaction from "@lucas-barake/effect-local/Transaction"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as SqlClient from "effect/sql/SqlClient"
import * as SqlError from "effect/sql/SqlError"
import * as SqlSchema from "effect/sql/SqlSchema"
import * as Codec from "./codec.js"
import * as Rows from "./rows.js"

interface EncodedEntityKey<M extends Model.Any,> {
  readonly encodedKey: M["key"]["Encoded"]
  readonly keyJson: string
}

interface EncodedEntity<M extends Model.Any,> extends EncodedEntityKey<M> {
  readonly encodedValue: M["schema"]["Encoded"]
  readonly valueJson: string
}

const encodeEntityKey = Effect.fnUntraced(function*<M extends Model.Any,>(
  model: M,
  key: Model.Key<M>
): Effect.fn.Return<EncodedEntityKey<M>, ReplicaError.StorageCorrupt> {
  const encodedKey = yield* Codec.encode(model.key, key)
  return { encodedKey, keyJson: yield* Codec.stringifyKey(encodedKey) }
})

const encodeEntity = Effect.fnUntraced(function*<M extends Model.Any,>(
  model: M,
  key: Model.Key<M>,
  value: Model.Value<M>
): Effect.fn.Return<EncodedEntity<M>, ReplicaError.StorageCorrupt> {
  const encodedKey = yield* encodeEntityKey(model, key)
  const encodedValue = yield* Codec.encode(model.schema, value)
  return { ...encodedKey, encodedValue, valueJson: yield* Codec.stringify(encodedValue) }
})

export interface Address {
  readonly spaceId: Identity.SpaceId
  readonly schemaGeneration: number
  readonly projectionGeneration: number
}

export const local = (
  options: Address & {
    readonly sql: SqlClient.SqlClient
    readonly table: "visible" | "canonical"
    readonly changes?: Array<Protocol.EntityChange>
  }
): Transaction.Transaction => {
  const find = SqlSchema.findOneOption({
    Request: Schema.Struct({ model: Schema.String, key: Schema.String }),
    Result: Rows.EntityRow,
    execute: ({ model, key }) => {
      if (options.table === "visible") {
        return options.sql`SELECT value_json FROM effect_local_client_visible_entities_data
          WHERE space_id = ${options.spaceId} AND schema_generation = ${options.schemaGeneration}
            AND projection_generation = ${options.projectionGeneration}
            AND model = ${model} AND entity_key = ${key}`
      }
      return options.sql`SELECT value_json FROM effect_local_client_canonical_entities_data
          WHERE space_id = ${options.spaceId} AND schema_generation = ${options.schemaGeneration}
            AND model = ${model} AND entity_key = ${key}`
    }
  })
  return {
    get: Effect.fnUntraced(function*(model, key) {
      const { keyJson } = yield* encodeEntityKey(model, key)
      const row = yield* find({ model: model.name, key: keyJson }).pipe(
        Effect.catchTags({
          SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
          SchemaError: (cause) =>
            Effect.fail(new ReplicaError.StorageCorrupt({ message: "Client entity row is corrupt", cause }))
        })
      )
      if (Option.isNone(row)) return Option.none()
      const value = yield* Codec.parse(row.value.value_json).pipe(
        Effect.flatMap((encoded) => Codec.decode(model.schema, encoded))
      )
      return Option.some(value)
    }),
    set: Effect.fnUntraced(function*(model, key, value) {
      const encoded = yield* encodeEntity(model, key, value)
      if (options.table === "visible") {
        yield* options.sql`INSERT INTO effect_local_client_visible_entities_data
          (space_id, schema_generation, projection_generation, model, entity_key, value_json, model_version)
          VALUES (${options.spaceId}, ${options.schemaGeneration}, ${options.projectionGeneration},
            ${model.name}, ${encoded.keyJson}, ${encoded.valueJson}, ${model.version})
          ON CONFLICT (space_id, schema_generation, projection_generation, model, entity_key) DO UPDATE SET
            value_json = excluded.value_json, model_version = excluded.model_version`
      } else {
        yield* options.sql`INSERT INTO effect_local_client_canonical_entities_data
          (space_id, schema_generation, model, entity_key, value_json, model_version)
          VALUES (${options.spaceId}, ${options.schemaGeneration}, ${model.name}, ${encoded.keyJson},
            ${encoded.valueJson}, ${model.version})
          ON CONFLICT (space_id, schema_generation, model, entity_key) DO UPDATE SET
            value_json = excluded.value_json, model_version = excluded.model_version`
      }
      options.changes?.push({
        _tag: "Upsert",
        entity: { model: model.name, modelVersion: model.version, key: encoded.encodedKey },
        value: encoded.encodedValue
      })
    }, Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))),
    delete: Effect.fnUntraced(function*(model, key) {
      const encoded = yield* encodeEntityKey(model, key)
      if (options.table === "visible") {
        yield* options.sql`DELETE FROM effect_local_client_visible_entities_data
          WHERE space_id = ${options.spaceId} AND schema_generation = ${options.schemaGeneration}
            AND projection_generation = ${options.projectionGeneration}
            AND model = ${model.name} AND entity_key = ${encoded.keyJson}`
      } else {
        yield* options.sql`DELETE FROM effect_local_client_canonical_entities_data
          WHERE space_id = ${options.spaceId} AND schema_generation = ${options.schemaGeneration}
            AND model = ${model.name} AND entity_key = ${encoded.keyJson}`
      }
      options.changes?.push({
        _tag: "Delete",
        entity: { model: model.name, modelVersion: model.version, key: encoded.encodedKey }
      })
    }, Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))),
    applyField: (semantics, current, operation) => semantics.apply(current, operation)
  }
}

export const server = (options: {
  readonly sql: SqlClient.SqlClient
  readonly definition: Definition.Any
  readonly spaceId: Identity.SpaceId
  readonly generation: number
  readonly changes: Array<Protocol.EntityChange>
}): Transaction.Transaction => {
  const find = SqlSchema.findOneOption({
    Request: Schema.Struct({ spaceId: Schema.String, model: Schema.String, key: Schema.String }),
    Result: Rows.EntityRow,
    execute: ({ spaceId, model, key }) =>
      options.sql`SELECT value_json FROM effect_local_server_entities_data
      WHERE space_id = ${spaceId} AND generation = ${options.generation}
        AND model = ${model} AND entity_key = ${key}`
  })
  return {
    get: Effect.fnUntraced(function*(model, key) {
      const { keyJson } = yield* encodeEntityKey(model, key)
      const row = yield* find({ spaceId: options.spaceId, model: model.name, key: keyJson }).pipe(
        Effect.catchTags({
          SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
          SchemaError: (cause) =>
            Effect.fail(new ReplicaError.StorageCorrupt({ message: "Server entity row is corrupt", cause }))
        })
      )
      if (Option.isNone(row)) return Option.none()
      const value = yield* Codec.parse(row.value.value_json).pipe(
        Effect.flatMap((encoded) => Codec.decode(model.schema, encoded))
      )
      return Option.some(value)
    }),
    set: Effect.fnUntraced(function*(model, key, value) {
      const encoded = yield* encodeEntity(model, key, value)
      const entityBytes = yield* Protocol.encodedBytesEffect({
        model: model.name,
        modelVersion: model.version,
        key: encoded.encodedKey,
        value: encoded.encodedValue
      })
      yield* options.sql`INSERT INTO effect_local_server_entities_data
          (space_id, generation, model, model_version, entity_key, value_json, entity_bytes)
        VALUES (${options.spaceId}, ${options.generation}, ${model.name}, ${model.version},
          ${encoded.keyJson}, ${encoded.valueJson}, ${entityBytes})
        ON CONFLICT (space_id, generation, model, entity_key) DO UPDATE
          SET model_version = excluded.model_version, value_json = excluded.value_json,
            entity_bytes = excluded.entity_bytes`
      options.changes.push({
        _tag: "Upsert",
        entity: { model: model.name, modelVersion: model.version, key: encoded.encodedKey },
        value: encoded.encodedValue
      })
    }, Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))),
    delete: Effect.fnUntraced(function*(model, key) {
      const encoded = yield* encodeEntityKey(model, key)
      yield* options.sql`DELETE FROM effect_local_server_entities_data
        WHERE space_id = ${options.spaceId} AND generation = ${options.generation}
          AND model = ${model.name} AND entity_key = ${encoded.keyJson}`
      options.changes.push({
        _tag: "Delete",
        entity: { model: model.name, modelVersion: model.version, key: encoded.encodedKey }
      })
    }, Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))),
    applyField: (semantics, current, operation) => semantics.apply(current, operation)
  }
}

export const applyCanonicalChange = Effect.fnUntraced(function*(
  sql: SqlClient.SqlClient,
  spaceId: Identity.SpaceId,
  schemaGeneration: number,
  change: Protocol.EntityChange
) {
  const keyJson = yield* Codec.stringifyKey(change.entity.key)
  if (change._tag === "Delete") {
    yield* sql`DELETE FROM effect_local_client_canonical_entities_data
        WHERE space_id = ${spaceId} AND schema_generation = ${schemaGeneration}
          AND model = ${change.entity.model} AND entity_key = ${keyJson}`
    return
  }
  const valueJson = yield* Codec.stringify(change.value)
  yield* sql`INSERT INTO effect_local_client_canonical_entities_data
        (space_id, schema_generation, model, entity_key, value_json, model_version)
        VALUES (${spaceId}, ${schemaGeneration}, ${change.entity.model}, ${keyJson}, ${valueJson},
          ${change.entity.modelVersion})
        ON CONFLICT (space_id, schema_generation, model, entity_key) DO UPDATE SET
          value_json = excluded.value_json, model_version = excluded.model_version`
}, Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause }))))

export const entityKey = (entity: Protocol.EntityKey) => Canonical.stringify([entity.model, entity.key])

const maximumTransactionAttempts = 8

type ServerTransactionFailure = ReplicaError.ReplicaError | SqlError.SqlError | Schema.SchemaError

const isTransientConflict = (error: SqlError.SqlError) =>
  error.reason._tag === "DeadlockError" || error.reason._tag === "SerializationError"

const transientConflict = (error: ReplicaError.StorageUnavailable | SqlError.SqlError) => {
  if (error._tag === "SqlError") return isTransientConflict(error)
  return SqlError.isSqlError(error.cause) && isTransientConflict(error.cause)
}

export function withServerTransaction<A, E extends ServerTransactionFailure, R,>(
  sql: SqlClient.SqlClient,
  effect: Effect.Effect<A, E, R>
): Effect.Effect<A, E | SqlError.SqlError, R>
export function withServerTransaction<R,>(
  sql: SqlClient.SqlClient,
  effect: Effect.Effect<unknown, ServerTransactionFailure, R>
): Effect.Effect<unknown, ServerTransactionFailure, R> {
  const attempt = (remaining: number): Effect.Effect<unknown, ServerTransactionFailure, R> =>
    Effect.suspend(() => {
      let committing = false
      const markCommitting = Effect.sync(() => {
        committing = true
      })
      return sql.withTransaction(Effect.tap(effect, markCommitting)).pipe(
        Effect.catchCause((cause) => {
          const commitErrors = cause.reasons.filter(Cause.isDieReason).map((reason) => reason.defect).filter(
            SqlError.isSqlError
          )
          if (!committing || commitErrors.length === 0 || commitErrors.length !== cause.reasons.length) {
            return Effect.failCause(cause)
          }
          if (commitErrors.length === 1) return Effect.fail(commitErrors[0])
          return Effect.fail(
            new SqlError.SqlError({
              reason: new SqlError.UnknownError({
                message: "COMMIT failed and its cleanup ROLLBACK failed",
                operation: "commit",
                cause
              })
            })
          )
        }),
        Effect.catchTag(["SqlError", "StorageUnavailable"], (error) => {
          if (remaining > 1 && transientConflict(error)) return attempt(remaining - 1)
          return Effect.fail(error)
        })
      )
    })
  return attempt(maximumTransactionAttempts)
}
