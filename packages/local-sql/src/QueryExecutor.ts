import type * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Model from "@lucas-barake/effect-local/Model"
import type * as Query from "@lucas-barake/effect-local/Query"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Transaction from "@lucas-barake/effect-local/Transaction"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SchemaAST from "effect/SchemaAST"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import type * as Statement from "effect/unstable/sql/Statement"
import * as Codec from "./internal/codec.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as SqlTransaction from "./internal/transaction.js"
import * as QueryReactivity from "./QueryReactivity.js"

export interface Service {
  readonly execute: <Q extends Query.Any,>(
    query: Q,
    payload: Q["payloadSchema"]["Type"]
  ) => Effect.Effect<
    Q["successSchema"]["Type"],
    ReplicaError.ReplicaError | ReplicaError.QueryFailed | Q["errorSchema"]["Type"]
  >
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

const quoteIdentifier = (name: string): string => `"${name.replaceAll(`"`, `""`)}"`

const jsonPath = (field: string): string => `$."${field.replaceAll("\\", "\\\\").replaceAll(`"`, `\\"`)}"`

const modelColumns = (model: Model.Any): ReadonlyArray<string> => {
  const encoded = SchemaAST.toEncoded(model.schema.ast)
  if (!SchemaAST.isObjects(encoded)) return []
  const columns: Array<string> = []
  const seen = new Set<string>()
  for (const signature of encoded.propertySignatures) {
    const field = signature.name
    // `key` and `value` are the entity columns; a colliding field stays reachable through `value`.
    if (typeof field !== "string" || field === "key" || field === "value" || seen.has(field)) continue
    seen.add(field)
    columns.push(field)
  }
  return columns
}

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
      const modelCte = (address: SqlTransaction.Address, model: Model.Any): Statement.Fragment => {
        // The JSON path is bound, not inlined: a field name may contain a quote, which no SQL
        // string literal survives.
        let projected = sql.literal("")
        for (const field of modelColumns(model)) {
          projected = sql`${projected}, json_extract(value_json, ${jsonPath(field)}) AS ${
            sql.literal(quoteIdentifier(field))
          }`
        }
        return sql`${sql.literal(quoteIdentifier(model.name))} AS (
          SELECT entity_key AS "key", value_json AS "value"${projected}
          FROM effect_local_client_visible_entities_data
          WHERE space_id = ${address.spaceId}
            AND schema_generation = ${address.schemaGeneration}
            AND projection_generation = ${address.projectionGeneration}
            AND model = ${model.name})`
      }
      const findCorruptModel = SqlSchema.findOneOption({
        Request: Schema.Struct({
          spaceId: Schema.String,
          schemaGeneration: Schema.Number,
          projectionGeneration: Schema.Number,
          models: Schema.Array(Schema.String)
        }),
        Result: Schema.Struct({ model: Schema.String }),
        execute: (request) =>
          sql`SELECT model FROM effect_local_client_visible_entities_data
          WHERE space_id = ${request.spaceId}
            AND schema_generation = ${request.schemaGeneration}
            AND projection_generation = ${request.projectionGeneration}
            AND ${sql.in("model", request.models)}
            AND json_valid(value_json) = 0
          LIMIT 1`
      })
      // A single undecodable row makes SQLite reject the whole statement with the same reason a
      // typo does, so the storage is inspected before the failure is blamed on the statement.
      const classifyStatementFailure = (
        address: SqlTransaction.Address,
        models: ReadonlyArray<Model.Any>,
        error: SqlError.SqlError
      ): Effect.Effect<never, ReplicaError.QueryError> => {
        const statementFailed = new ReplicaError.QueryFailed({
          message: "The query statement failed",
          cause: error
        })
        return findCorruptModel({
          spaceId: address.spaceId,
          schemaGeneration: address.schemaGeneration,
          projectionGeneration: address.projectionGeneration,
          models: models.map((model) => model.name)
        }).pipe(
          Effect.matchEffect({
            onFailure: () => Effect.fail(statementFailed),
            onSuccess: Option.match({
              onNone: (): Effect.Effect<never, ReplicaError.QueryError> => Effect.fail(statementFailed),
              onSome: (corrupt): Effect.Effect<never, ReplicaError.QueryError> =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: `Stored ${corrupt.model} entity values are not valid JSON`,
                    cause: error
                  })
                )
            })
          })
        )
      }
      const queryCapability = (
        address: SqlTransaction.Address,
        queryToken: string,
        reads: Array<QueryReactivity.Read>
      ): Transaction.Query => {
        const transaction = SqlTransaction.local({ sql, table: "visible", ...address })
        const record = Effect.fnUntraced(function*(read: QueryReactivity.Read) {
          reads.push(read)
          yield* queryReactivity.record(queryToken, reads)
        })
        return {
          get: (model, key) =>
            record({ _tag: "Entity", spaceId, key: ReactivityKey.entity(spaceId, model.name, key) }).pipe(
              Effect.andThen(transaction.get(model, key))
            ),
          sql: Effect.fnUntraced(
            function*(models, statement) {
              if (models.length === 0) {
                return yield* Effect.die(
                  new Error("query.sql requires at least one model; an empty read set can never react to changes")
                )
              }
              const deduped: Array<Model.Any> = []
              const names = new Set<string>()
              for (const model of models) {
                if (names.has(model.name)) continue
                names.add(model.name)
                deduped.push(model)
              }
              for (const model of deduped) {
                yield* record({ _tag: "Model", spaceId, model: model.name })
              }
              const user = statement(sql)
              const [text] = user.compile()
              if (/^\s*with\b/i.test(text)) {
                return yield* new ReplicaError.QueryFailed({
                  message: "The statement must begin with the query body (SELECT/VALUES) or continue the " +
                    "generated CTE list with a leading `, name AS (...)`; it cannot open its own WITH clause"
                })
              }
              let ctes = modelCte(address, deduped[0])
              for (const model of deduped.slice(1)) {
                ctes = sql`${ctes}, ${modelCte(address, model)}`
              }
              // The transient engine reasons stay StorageUnavailable; the compiler holds this
              // list to the SqlError reason union, so a renamed reason fails to build instead of
              // silently reclassifying an outage as a bad statement.
              const rows: ReadonlyArray<unknown> = yield* sql`WITH RECURSIVE ${ctes} ${user}`.pipe(
                Effect.catchReasons("SqlError", {
                  ConnectionError: (reason) => Effect.fail(StorageUnavailable.make(reason)),
                  LockTimeoutError: (reason) => Effect.fail(StorageUnavailable.make(reason)),
                  StatementTimeoutError: (reason) => Effect.fail(StorageUnavailable.make(reason)),
                  DeadlockError: (reason) => Effect.fail(StorageUnavailable.make(reason)),
                  SerializationError: (reason) => Effect.fail(StorageUnavailable.make(reason))
                }),
                Effect.catchTag("SqlError", (error) => classifyStatementFailure(address, deduped, error))
              )
              return [...rows]
            }
          )
        }
      }
      const execute = <Q extends Query.Any,>(
        query: Q,
        payload: Q["payloadSchema"]["Type"]
      ): Effect.Effect<
        Q["successSchema"]["Type"],
        ReplicaError.ReplicaError | ReplicaError.QueryFailed | Q["errorSchema"]["Type"]
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
