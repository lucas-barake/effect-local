import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Transaction from "@lucas-barake/effect-local/Transaction"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Codec from "./codec.js"
import * as Rows from "./rows.js"

const storageError = (cause: unknown) => new ReplicaError.StorageUnavailable({ cause })

const encodeEntity = <M extends Model.Any,>(model: M, key: Model.Key<M>, value?: Model.Value<M>) =>
  Effect.gen(function*() {
    const encodedKey = yield* Codec.encode(model.key, key)
    const keyJson = yield* Codec.stringify(encodedKey)
    if (value === undefined) return { encodedKey, keyJson } as const
    const encodedValue = yield* Codec.encode(model.schema, value)
    return { encodedKey, keyJson, encodedValue, valueJson: yield* Codec.stringify(encodedValue) } as const
  })

export const local = (options: {
  readonly sql: SqlClient.SqlClient
  readonly definition: Definition.Any
  readonly table: "visible" | "canonical"
  readonly changes?: Array<Protocol.EntityChange>
}): Transaction.Transaction => {
  const find = SqlSchema.findOneOption({
    Request: Schema.Struct({ model: Schema.String, key: Schema.String }),
    Result: Rows.EntityRow,
    execute: ({ model, key }) =>
      options.table === "visible"
        ? options
          .sql`SELECT value_json FROM effect_local_visible_entities WHERE model = ${model} AND entity_key = ${key}`
        : options
          .sql`SELECT value_json FROM effect_local_canonical_entities WHERE model = ${model} AND entity_key = ${key}`
  })
  return {
    get: (model, key) =>
      Effect.gen(function*() {
        const { keyJson } = yield* encodeEntity(model, key)
        const row = yield* find({ model: model.name, key: keyJson }).pipe(
          Effect.mapError(storageError)
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
          yield* options.sql`INSERT INTO effect_local_visible_entities (model, entity_key, value_json)
          VALUES (${model.name}, ${encoded.keyJson}, ${encoded.valueJson})
          ON CONFLICT (model, entity_key) DO UPDATE SET value_json = excluded.value_json`
        } else {
          yield* options.sql`INSERT INTO effect_local_canonical_entities (model, entity_key, value_json)
          VALUES (${model.name}, ${encoded.keyJson}, ${encoded.valueJson})
          ON CONFLICT (model, entity_key) DO UPDATE SET value_json = excluded.value_json`
        }
        options.changes?.push({
          _tag: "Upsert",
          entity: { model: model.name, key: encoded.encodedKey as any },
          value: encoded.encodedValue as any
        })
      }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(storageError(cause)))),
    delete: (model, key) =>
      Effect.gen(function*() {
        const encoded = yield* encodeEntity(model, key)
        if (options.table === "visible") {
          yield* options
            .sql`DELETE FROM effect_local_visible_entities WHERE model = ${model.name} AND entity_key = ${encoded.keyJson}`
        } else {
          yield* options
            .sql`DELETE FROM effect_local_canonical_entities WHERE model = ${model.name} AND entity_key = ${encoded.keyJson}`
        }
        options.changes?.push({ _tag: "Delete", entity: { model: model.name, key: encoded.encodedKey as any } })
      }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(storageError(cause)))),
    applyField: (semantics, current, operation) => semantics.apply(current, operation)
  }
}

export const server = (options: {
  readonly sql: SqlClient.SqlClient
  readonly definition: Definition.Any
  readonly spaceId: Identity.SpaceId
  readonly changes: Array<Protocol.EntityChange>
}): Transaction.Transaction => {
  const find = SqlSchema.findOneOption({
    Request: Schema.Struct({ spaceId: Schema.String, model: Schema.String, key: Schema.String }),
    Result: Rows.EntityRow,
    execute: ({ spaceId, model, key }) =>
      options.sql`SELECT value_json FROM effect_local_server_entities
      WHERE space_id = ${spaceId} AND model = ${model} AND entity_key = ${key}`
  })
  return {
    get: (model, key) =>
      Effect.gen(function*() {
        const { keyJson } = yield* encodeEntity(model, key)
        const row = yield* find({ spaceId: options.spaceId, model: model.name, key: keyJson }).pipe(
          Effect.mapError(storageError)
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
        yield* options.sql`INSERT INTO effect_local_server_entities (space_id, model, entity_key, value_json)
        VALUES (${options.spaceId}, ${model.name}, ${encoded.keyJson}, ${encoded.valueJson})
        ON CONFLICT (space_id, model, entity_key) DO UPDATE SET value_json = excluded.value_json`
        options.changes.push({
          _tag: "Upsert",
          entity: { model: model.name, key: encoded.encodedKey as any },
          value: encoded.encodedValue as any
        })
      }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(storageError(cause)))),
    delete: (model, key) =>
      Effect.gen(function*() {
        const encoded = yield* encodeEntity(model, key)
        yield* options.sql`DELETE FROM effect_local_server_entities
        WHERE space_id = ${options.spaceId} AND model = ${model.name} AND entity_key = ${encoded.keyJson}`
        options.changes.push({ _tag: "Delete", entity: { model: model.name, key: encoded.encodedKey as any } })
      }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(storageError(cause)))),
    applyField: (semantics, current, operation) => semantics.apply(current, operation)
  }
}

export const applyLocalChange = (
  sql: SqlClient.SqlClient,
  table: "visible" | "canonical",
  change: Protocol.EntityChange
) =>
  Effect.gen(function*() {
    const keyJson = yield* Codec.stringify(change.entity.key)
    if (change._tag === "Delete") {
      if (
        table === "visible"
      ) {
        yield* sql`DELETE FROM effect_local_visible_entities WHERE model = ${change.entity.model} AND entity_key = ${keyJson}`
      } else {yield* sql`DELETE FROM effect_local_canonical_entities WHERE model = ${change.entity.model} AND entity_key = ${keyJson}`}
      return
    }
    const valueJson = yield* Codec.stringify(change.value)
    if (table === "visible") {
      yield* sql`INSERT INTO effect_local_visible_entities (model, entity_key, value_json)
        VALUES (${change.entity.model}, ${keyJson}, ${valueJson})
        ON CONFLICT (model, entity_key) DO UPDATE SET value_json = excluded.value_json`
    } else {
      yield* sql`INSERT INTO effect_local_canonical_entities (model, entity_key, value_json)
        VALUES (${change.entity.model}, ${keyJson}, ${valueJson})
        ON CONFLICT (model, entity_key) DO UPDATE SET value_json = excluded.value_json`
    }
  }).pipe(Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(storageError(cause))))

export const entityKey = (entity: Protocol.EntityKey) => `${entity.model}\u0000${Canonical.stringify(entity.key)}`
