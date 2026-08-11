import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Transaction from "@lucas-barake/effect-local/Transaction"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Codec from "./codec.js"
import * as Rows from "./rows.js"
import * as StorageUnavailable from "./storageUnavailable.js"

interface EncodedEntityKey<M extends Model.Any,> {
  readonly encodedKey: M["key"]["Encoded"]
  readonly keyJson: string
}

interface EncodedEntity<M extends Model.Any,> extends EncodedEntityKey<M> {
  readonly encodedValue: M["schema"]["Encoded"]
  readonly valueJson: string
}

function encodeEntity<M extends Model.Any,>(
  model: M,
  key: Model.Key<M>
): Effect.Effect<EncodedEntityKey<M>, ReplicaError.StorageCorrupt>
function encodeEntity<M extends Model.Any,>(
  model: M,
  key: Model.Key<M>,
  value: Model.Value<M>
): Effect.Effect<EncodedEntity<M>, ReplicaError.StorageCorrupt>
function encodeEntity<M extends Model.Any,>(
  model: M,
  key: Model.Key<M>,
  value?: Model.Value<M>
): Effect.Effect<EncodedEntityKey<M> | EncodedEntity<M>, ReplicaError.StorageCorrupt> {
  return Effect.gen(function*() {
    const encodedKey = yield* Codec.encode(model.key, key)
    const keyJson = yield* Codec.stringify(encodedKey)
    if (value === undefined) return { encodedKey, keyJson }
    const encodedValue = yield* Codec.encode(model.schema, value)
    return { encodedKey, keyJson, encodedValue, valueJson: yield* Codec.stringify(encodedValue) }
  })
}

export const local = (options: {
  readonly sql: SqlClient.SqlClient
  readonly table: "visible" | "canonical"
  readonly spaceId: Identity.SpaceId
  readonly schemaGeneration: number
  readonly projectionGeneration: number
  readonly changes?: Array<Protocol.EntityChange>
  readonly onVisibleChange?: <M extends Model.Any,>(
    model: M,
    entityKey: string,
    value: Model.Value<M> | undefined
  ) => Effect.Effect<void, ReplicaError.StorageError>
}): Transaction.Transaction => {
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
    get: (model, key) =>
      Effect.gen(function*() {
        const { keyJson } = yield* encodeEntity(model, key)
        const row = yield* find({ model: model.name, key: keyJson }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        if (Option.isNone(row)) return Option.none()
        const value = yield* Codec.parse(row.value.value_json).pipe(
          Effect.flatMap((encoded) => Codec.decode(model.schema, encoded))
        )
        return Option.some(value)
      }),
    set: (model, key, value) =>
      Effect.gen(function*() {
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
        if (options.table === "visible") {
          yield* (options.onVisibleChange?.(model, encoded.keyJson, value) ?? Effect.void)
        }
        options.changes?.push({
          _tag: "Upsert",
          entity: { model: model.name, modelVersion: model.version, key: encoded.encodedKey },
          value: encoded.encodedValue
        })
      }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))),
    delete: (model, key) =>
      Effect.gen(function*() {
        const encoded = yield* encodeEntity(model, key)
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
        if (options.table === "visible") {
          yield* (options.onVisibleChange?.(model, encoded.keyJson, undefined) ?? Effect.void)
        }
        options.changes?.push({
          _tag: "Delete",
          entity: { model: model.name, modelVersion: model.version, key: encoded.encodedKey }
        })
      }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))),
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
    get: (model, key) =>
      Effect.gen(function*() {
        const { keyJson } = yield* encodeEntity(model, key)
        const row = yield* find({ spaceId: options.spaceId, model: model.name, key: keyJson }).pipe(
          Effect.mapError(StorageUnavailable.make)
        )
        if (Option.isNone(row)) return Option.none()
        const value = yield* Codec.parse(row.value.value_json).pipe(
          Effect.flatMap((encoded) => Codec.decode(model.schema, encoded))
        )
        return Option.some(value)
      }),
    set: (model, key, value) =>
      Effect.gen(function*() {
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
      }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))),
    delete: (model, key) =>
      Effect.gen(function*() {
        const encoded = yield* encodeEntity(model, key)
        yield* options.sql`DELETE FROM effect_local_server_entities_data
        WHERE space_id = ${options.spaceId} AND generation = ${options.generation}
          AND model = ${model.name} AND entity_key = ${encoded.keyJson}`
        options.changes.push({
          _tag: "Delete",
          entity: { model: model.name, modelVersion: model.version, key: encoded.encodedKey }
        })
      }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))),
    applyField: (semantics, current, operation) => semantics.apply(current, operation)
  }
}

export const applyCanonicalChange = (
  sql: SqlClient.SqlClient,
  spaceId: Identity.SpaceId,
  schemaGeneration: number,
  change: Protocol.EntityChange
) =>
  Effect.gen(function*() {
    const keyJson = yield* Codec.stringify(change.entity.key)
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
  }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))))

export const entityKey = (entity: Protocol.EntityKey) => `${entity.model}\u0000${Canonical.stringify(entity.key)}`
