import type * as Query from "@lucas-barake/effect-local/Query"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import type * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"

export class QueryExecutor extends Context.Service<QueryExecutor, {
  readonly execute: <Q extends Query.Any,>(
    query: Q,
    payload: Q["payloadSchema"]["Type"]
  ) => Effect.Effect<Q["successSchema"]["Type"], Q["errorSchema"]["Type"] | ReplicaError.ReplicaError>
  readonly reactive: <Q extends Query.Any,>(
    query: Q,
    payload: Q["payloadSchema"]["Type"]
  ) => Stream.Stream<Q["successSchema"]["Type"], Q["errorSchema"]["Type"] | ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/QueryExecutor") {}

export type QueryHandlers<D extends ReplicaDefinition.Any,> = D["queries"][number] extends infer Q
  ? Q extends Query.Query<infer Name, infer Payload, infer Success, infer Error, infer Dependencies>
    ? Query.HandlerService<Name, Payload, Success, Error, Dependencies>
  : never
  : never

export const layer = <D extends ReplicaDefinition.Any,>(
  definition: D
): Layer.Layer<QueryExecutor, never, Reactivity.Reactivity | SqlClient.SqlClient | QueryHandlers<D>> =>
  Layer.effect(
    QueryExecutor,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const reactivity = yield* Reactivity.Reactivity
      const context = yield* Effect.context<QueryHandlers<D>>()
      const handlers = new Map<string, Query.Handler<any, any, any, never>>()
      for (const query of definition.queries) {
        handlers.set(query.name, Context.get(context, query.handler))
      }
      const findProjectionStatus = SqlSchema.findOneOption({
        Request: Schema.String,
        Result: Schema.Struct({ status: Schema.String }),
        execute: (projectionName) =>
          sql`SELECT status FROM effect_local_projection_registry
            WHERE projection_name = ${projectionName}`
      })
      const findBlockedDocumentCount = SqlSchema.findOneOption({
        Request: Schema.String,
        Result: Schema.Struct({ count: Schema.Number }),
        execute: (documentType) =>
          sql`SELECT COUNT(*) AS count FROM effect_local_documents
            WHERE document_type = ${documentType} AND projection_status != 'Ready'`
      })
      const findBlockedProjectionCount = SqlSchema.findOneOption({
        Request: Schema.String,
        Result: Schema.Struct({ count: Schema.Number }),
        execute: (projectionName) =>
          sql`SELECT COUNT(*) AS count FROM effect_local_document_projections
            WHERE projection_name = ${projectionName} AND status != 'Ready'`
      })
      const execute = <Q extends Query.Any,>(query: Q, payload: Q["payloadSchema"]["Type"]) =>
        Effect.gen(function*() {
          for (const projection of query.dependsOn) {
            const registry = yield* findProjectionStatus(projection.name).pipe(
              Effect.mapError((cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProjectionBlocked({
                    projection: projection.name,
                    cause
                  })
                })
              )
            )
            if (registry._tag === "None" || registry.value.status !== "Ready") {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.ProjectionBlocked({
                  projection: projection.name,
                  cause: new Error("Projection is not ready")
                })
              })
            }
            const blockedDocuments = yield* findBlockedDocumentCount(projection.document.name).pipe(
              Effect.mapError((cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProjectionBlocked({
                    projection: projection.name,
                    cause
                  })
                })
              )
            )
            if ((blockedDocuments._tag === "Some" ? blockedDocuments.value.count : 0) > 0) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.ProjectionBlocked({
                  projection: projection.name,
                  cause: new Error("A source document projection is not ready")
                })
              })
            }
            const blocked = yield* findBlockedProjectionCount(projection.name).pipe(
              Effect.mapError((cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProjectionBlocked({
                    projection: projection.name,
                    cause
                  })
                })
              )
            )
            if ((blocked._tag === "Some" ? blocked.value.count : 0) > 0) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.ProjectionBlocked({
                  projection: projection.name,
                  cause: new Error("A document projection is not ready")
                })
              })
            }
          }
          const encoded = yield* Schema.encodeEffect(query.payloadSchema)(payload).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause
                })
              })
            )
          )
          const decoded = yield* Schema.decodeEffect(query.payloadSchema)(encoded).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause
                })
              })
            )
          )
          const handler = handlers.get(query.name)
          if (handler === undefined) return yield* Effect.die(new Error(`Missing query handler: ${query.name}`))
          const result = yield* handler(decoded)
          return yield* Schema.decodeUnknownEffect(Schema.toType(query.successSchema))(result).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause
                })
              })
            )
          )
        }) as Effect.Effect<Q["successSchema"]["Type"], Q["errorSchema"]["Type"] | ReplicaError.ReplicaError>
      const reactivityKeys = (query: Query.Any) =>
        [...new Set(query.dependsOn.flatMap((projection) => [projection.name, projection.document.name]))]
      return QueryExecutor.of({
        execute,
        reactive: (query, payload) => reactivity.stream(reactivityKeys(query), execute(query, payload))
      })
    })
  )
