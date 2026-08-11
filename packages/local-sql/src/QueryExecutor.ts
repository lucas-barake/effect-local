import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Codec from "./internal/codec.js"
import * as Rows from "./internal/rows.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as SqlTransaction from "./internal/transaction.js"

export interface Service {
  readonly execute: <Q extends Query.Any,>(
    query: Q,
    payload: Q["payloadSchema"]["Type"]
  ) => Effect.Effect<Q["successSchema"]["Type"], ReplicaError.ReplicaError | Q["errorSchema"]["Type"]>
}

export class QueryExecutor extends Context.Service<QueryExecutor, Service>()(
  "@lucas-barake/effect-local-sql/QueryExecutor"
) {}

export type Handlers<D extends Definition.Any,> = D["queries"][number] extends infer Q
  ? Q extends Query.Query<infer Name, infer P, infer A, infer E> ? Query.HandlerService<Name, P, A, E> : never
  : never

const SchemaFenceRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: Identity.SchemaVersion,
  schema_hash: Identity.SchemaHash,
  target_schema_version: Schema.NullOr(Identity.SchemaVersion),
  target_schema_hash: Schema.NullOr(Identity.SchemaHash),
  migration_hash: Schema.NullOr(Identity.SchemaHash)
})

export const layer = <D extends Definition.Any,>(
  definition: D
): Layer.Layer<QueryExecutor, never, SqlClient.SqlClient | Handlers<D>> =>
  Layer.effect(
    QueryExecutor,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const context = yield* Effect.context<Handlers<D>>()
      const handlers = new Map<string, Query.HandlerService<any, any, any, any>>()
      for (const query of definition.queries) {
        handlers.set(query.name, Context.get(context, query.handler as any) as any)
      }

      const allRows = SqlSchema.findAll({
        Request: Schema.String,
        Result: Rows.EntityRow,
        execute: (model) =>
          sql`SELECT value_json FROM effect_local_visible_entities WHERE model = ${model} ORDER BY entity_key`
      })
      const findFence = SqlSchema.findOne({
        Request: Schema.Void,
        Result: SchemaFenceRow,
        execute: () =>
          sql`SELECT definition_hash, schema_version, schema_hash,
          target_schema_version, target_schema_hash, migration_hash
          FROM effect_local_client_meta WHERE singleton = 1`
      })
      const queryCapability = {
        get: SqlTransaction.local({ sql, definition, table: "visible" }).get,
        all: <M extends Model.Any,>(model: M) =>
          allRows(model.name).pipe(
            Effect.mapError(StorageUnavailable.make),
            Effect.flatMap((rows) =>
              Effect.forEach(
                rows,
                (row) => Codec.parse(row.value_json).pipe(Effect.flatMap((value) => Codec.decode(model.schema, value)))
              )
            )
          )
      }
      return QueryExecutor.of({
        execute: (query, payload) =>
          sql.withTransaction(Effect.gen(function*() {
            const fence = yield* findFence(undefined).pipe(Effect.mapError(StorageUnavailable.make))
            if (
              fence.schema_version !== definition.schemaIdentity.version ||
              fence.schema_hash !== definition.schemaIdentity.hash ||
              fence.target_schema_version !== null || fence.target_schema_hash !== null ||
              fence.migration_hash !== null
            ) {
              return yield* new ReplicaError.StaleSchema({
                expectedVersion: definition.schemaIdentity.version,
                expectedHash: definition.schemaIdentity.hash,
                actualVersion: fence.schema_version,
                actualHash: fence.schema_hash
              })
            }
            if (fence.definition_hash !== definition.hash) {
              return yield* new ReplicaError.DefinitionMismatch({
                expected: definition.hash,
                actual: fence.definition_hash
              })
            }
            const handler = handlers.get(query.name)
            if (handler === undefined) {
              return yield* new ReplicaError.ProtocolInvalid({ message: `Unknown query: ${query.name}` })
            }
            const encodedPayload = yield* Codec.encode(query.payloadSchema, payload)
            const decodedPayload = yield* Codec.decode(query.payloadSchema, encodedPayload)
            const result = yield* handler.execute({ query: queryCapability, payload: decodedPayload })
            const encoded = yield* Codec.encode(query.successSchema, result)
            return yield* Codec.decode(query.successSchema, encoded)
          })).pipe(
            Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
            Effect.withSpan("QueryExecutor.execute", {
              attributes: { "query.name": query.name }
            })
          )
      })
    })
  )
