import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Transaction from "@lucas-barake/effect-local/Transaction"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as IndexStore from "./IndexStore.js"
import * as Codec from "./internal/codec.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as SqlTransaction from "./internal/transaction.js"
import * as QueryReactivity from "./QueryReactivity.js"

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

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

const SchemaFenceRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: Identity.SchemaVersion,
  schema_hash: Identity.SchemaHash,
  active_schema_generation: NonNegativeInt,
  active_projection_generation: NonNegativeInt,
  target_schema_version: Schema.NullOr(Identity.SchemaVersion),
  target_schema_hash: Schema.NullOr(Identity.SchemaHash),
  migration_hash: Schema.NullOr(Identity.SchemaHash)
})

export const layer = <D extends Definition.Any,>(
  definition: D,
  spaceId: Identity.SpaceId
): Layer.Layer<QueryExecutor, never, SqlClient.SqlClient | Handlers<D> | QueryReactivity.QueryReactivity> =>
  Layer.effect(
    QueryExecutor,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const queryReactivity = yield* QueryReactivity.QueryReactivity
      const context = yield* Effect.context<Handlers<D>>()
      for (const query of definition.queries) {
        Context.getUnsafe<Query.HandlerService<any, any, any, any>, any>(query.handler)(context)
      }

      const findFence = SqlSchema.findOne({
        Request: Schema.Void,
        Result: SchemaFenceRow,
        execute: () =>
          sql`SELECT definition_hash, schema_version, schema_hash, active_schema_generation,
          active_projection_generation,
          target_schema_version, target_schema_hash, migration_hash
          FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
      })
      const maximumPageReadsPerScan = 64
      const queryCapability = (
        address: IndexStore.Address,
        queryToken: string,
        reads: Array<QueryReactivity.Read>
      ): Transaction.Query => {
        const transaction = SqlTransaction.local({ sql, table: "visible", ...address })
        const record = Effect.fnUntraced(function*(read: QueryReactivity.Read) {
          const position = reads.length
          reads.push(read)
          yield* queryReactivity.record(queryToken, reads)
          return position
        })
        return {
          get: (model, key) =>
            record({ _tag: "Entity", spaceId, key: ReactivityKey.entity(spaceId, model.name, key) }).pipe(
              Effect.andThen(transaction.get(model, key))
            ),
          from: (model, index) => {
            let pageReads = 0
            const collapsed = new Set<string>()
            return IndexStore.query(
              sql,
              address,
              model,
              index,
              Effect.fnUntraced(function*(initial) {
                let partitionKey = ""
                for (const value of initial.partition) {
                  const text = String(value)
                  partitionKey += `${text.length}:${text}`
                }
                if (collapsed.has(partitionKey)) {
                  return () => Effect.void
                }
                if (pageReads >= maximumPageReadsPerScan) {
                  const covering: IndexStore.Footprint = {
                    ...initial,
                    lower: undefined,
                    upper: undefined,
                    cursor: undefined,
                    boundary: undefined,
                    hasMore: false,
                    full: true
                  }
                  collapsed.add(partitionKey)
                  yield* record({ _tag: "Index", footprint: covering })
                  return () => Effect.void
                }
                pageReads++
                const position = yield* record({ _tag: "Index", footprint: initial })
                return Effect.fnUntraced(function*(complete) {
                  reads[position] = { _tag: "Index", footprint: complete }
                  yield* queryReactivity.record(queryToken, reads)
                })
              })
            )
          }
        }
      }
      const execute = <Q extends Query.Any,>(
        query: Q,
        payload: Q["payloadSchema"]["Type"]
      ): Effect.Effect<
        Q["successSchema"]["Type"],
        ReplicaError.ReplicaError | Q["errorSchema"]["Type"]
      > => {
        const key = ReactivityKey.query(spaceId, query.name, payload)
        return sql.withTransaction(Effect.gen(function*() {
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
          const registered = definition.queryByName.get(query.name)
          if (registered === undefined || registered !== query) {
            return yield* new ReplicaError.ProtocolInvalid({ message: `Unknown query: ${query.name}` })
          }
          const handler = Context.getUnsafe<
            Query.HandlerService<
              Q["name"],
              Q["payloadSchema"],
              Q["successSchema"],
              Q["errorSchema"]
            >,
            any
          >(query.handler)(context)
          const payloadSchema = Schema.make<
            Schema.Codec<
              Q["payloadSchema"]["Type"],
              Q["payloadSchema"]["Encoded"]
            >
          >(query.payloadSchema.ast)
          const successSchema = Schema.make<
            Schema.Codec<
              Q["successSchema"]["Type"],
              Q["successSchema"]["Encoded"]
            >
          >(query.successSchema.ast)
          const encodedPayload = yield* Codec.encode(payloadSchema, payload)
          const decodedPayload = yield* Codec.decode(payloadSchema, encodedPayload)
          const reads: Array<QueryReactivity.Read> = []
          yield* queryReactivity.record(key, reads)
          const result = yield* handler.execute({
            query: queryCapability(
              {
                spaceId,
                schemaGeneration: fence.active_schema_generation,
                projectionGeneration: fence.active_projection_generation
              },
              key,
              reads
            ),
            payload: decodedPayload
          })
          const encoded = yield* Codec.encode(successSchema, result)
          const value = yield* Codec.decode(successSchema, encoded)
          yield* queryReactivity.record(key, reads)
          return value
        })).pipe(
          Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
          Effect.withSpan("QueryExecutor.execute", {
            attributes: { "query.name": query.name }
          })
        )
      }
      return QueryExecutor.of({ execute })
    })
  )
