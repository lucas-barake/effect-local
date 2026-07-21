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

export class QueryExecutor extends Context.Service<QueryExecutor, {
  readonly execute: <Q extends Query.Any,>(
    query: Q,
    payload: Q["payload"]["Type"]
  ) => Effect.Effect<Q["success"]["Type"], Q["error"]["Type"] | ReplicaError.ReplicaError>
  readonly reactive: <Q extends Query.Any,>(
    query: Q,
    payload: Q["payload"]["Type"]
  ) => Stream.Stream<Q["success"]["Type"], Q["error"]["Type"] | ReplicaError.ReplicaError>
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
      const execute = <Q extends Query.Any,>(query: Q, payload: Q["payload"]["Type"]) =>
        Effect.gen(function*() {
          for (const projection of query.dependsOn) {
            const registry = yield* sql<{ readonly status: string }>`
            SELECT status FROM effect_local_projection_registry
            WHERE projection_name = ${projection.name}`.pipe(
              Effect.mapError((cause) =>
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "ProjectionBlocked",
                    projection: projection.name,
                    cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                  }
                })
              )
            )
            if (registry[0]?.status !== "Ready") {
              return yield* new ReplicaError.ReplicaError({
                reason: {
                  _tag: "ProjectionBlocked",
                  projection: projection.name,
                  cause: { _tag: "SchemaCause", message: "Projection is not ready", path: [] }
                }
              })
            }
            const blockedDocuments = yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM effect_local_documents
            WHERE document_type = ${projection.document.name} AND projection_status != 'Ready'`.pipe(
              Effect.mapError((cause) =>
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "ProjectionBlocked",
                    projection: projection.name,
                    cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                  }
                })
              )
            )
            if ((blockedDocuments[0]?.count ?? 0) > 0) {
              return yield* new ReplicaError.ReplicaError({
                reason: {
                  _tag: "ProjectionBlocked",
                  projection: projection.name,
                  cause: { _tag: "SchemaCause", message: "A source document projection is not ready", path: [] }
                }
              })
            }
            const blocked = yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM effect_local_document_projections
            WHERE projection_name = ${projection.name} AND status != 'Ready'`.pipe(
              Effect.mapError((cause) =>
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "ProjectionBlocked",
                    projection: projection.name,
                    cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                  }
                })
              )
            )
            if ((blocked[0]?.count ?? 0) > 0) {
              return yield* new ReplicaError.ReplicaError({
                reason: {
                  _tag: "ProjectionBlocked",
                  projection: projection.name,
                  cause: { _tag: "SchemaCause", message: "A document projection is not ready", path: [] }
                }
              })
            }
          }
          const encoded = yield* Schema.encodeEffect(query.payload)(payload).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageCorrupt",
                  cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                }
              })
            )
          )
          const decoded = yield* Schema.decodeEffect(query.payload)(encoded).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageCorrupt",
                  cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                }
              })
            )
          )
          const handler = handlers.get(query.name)
          if (handler === undefined) return yield* Effect.die(new Error(`Missing query handler: ${query.name}`))
          const result = yield* handler(decoded)
          return yield* Schema.decodeUnknownEffect(Schema.toType(query.success))(result).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageCorrupt",
                  cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                }
              })
            )
          )
        }) as Effect.Effect<Q["success"]["Type"], Q["error"]["Type"] | ReplicaError.ReplicaError>
      const reactivityKeys = (query: Query.Any) =>
        query.dependsOn.flatMap((projection) => [projection.name, projection.document.name])
      return QueryExecutor.of({
        execute,
        reactive: (query, payload) => reactivity.stream(reactivityKeys(query), execute(query, payload))
      })
    })
  )
