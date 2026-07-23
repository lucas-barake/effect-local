import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Projection from "@lucas-barake/effect-local/Projection"
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"

export interface Migration {
  readonly id: number
  readonly name: string
  readonly run: (sql: SqlClient.SqlClient, destinationTable: string) => Effect.Effect<void, SqlError.SqlError>
}

export interface SqlProjection<P extends Projection.Any,> {
  readonly projection: P
  readonly table: string
  readonly migrations: ReadonlyArray<Migration>
  readonly deleteByDocument: (
    sql: SqlClient.SqlClient,
    destinationTable: string,
    documentId: Identity.DocumentId
  ) => Effect.Effect<void, SqlError.SqlError>
  readonly insert: (
    sql: SqlClient.SqlClient,
    destinationTable: string,
    row: P["Row"]["Type"]
  ) => Effect.Effect<void, SqlError.SqlError>
  readonly service: Context.Service<BindingService<P>, SqlProjection<P>>
  readonly layer: Layer.Layer<BindingService<P>>
}

export interface BindingService<P extends Projection.Any,> {
  readonly projection: P
}

export const make = <P extends Projection.Any,>(
  projection: P,
  options: {
    readonly table: string
    readonly migrations: ReadonlyArray<Migration>
    readonly deleteByDocument: SqlProjection<P>["deleteByDocument"]
    readonly insert: SqlProjection<P>["insert"]
  }
): SqlProjection<P> => {
  if (options.table.length === 0) throw new TypeError("Projection table must be nonempty")
  if (options.migrations.length === 0) throw new TypeError("Projection requires at least one migration")
  const ids = new Set<number>()
  for (const migration of options.migrations) {
    if (!Number.isSafeInteger(migration.id) || migration.id < 1) {
      throw new TypeError("Projection migration ID must be a positive integer")
    }
    if (migration.name.length === 0) throw new TypeError("Projection migration name must be nonempty")
    if (ids.has(migration.id)) throw new TypeError(`Duplicate projection migration: ${migration.id}`)
    ids.add(migration.id)
  }
  const service = Context.Service<BindingService<P>, SqlProjection<P>>(
    `@lucas-barake/effect-local-sql/SqlProjection/${projection.name}`
  )
  const binding = { projection, service, ...options } as SqlProjection<P>
  return Object.assign(binding, { layer: Layer.succeed(service, binding) })
}

export type Any = SqlProjection<any>
