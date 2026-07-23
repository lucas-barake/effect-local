import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Document from "@lucas-barake/effect-local/Document"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as SchemaDescriptor from "@lucas-barake/effect-local/SchemaDescriptor"
import type * as Snapshot from "@lucas-barake/effect-local/Snapshot"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Predicate from "effect/Predicate"
import * as Schema from "effect/Schema"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import type * as SqlProjection from "./SqlProjection.js"

const StringArrayJson = Schema.fromJsonString(Schema.Array(Schema.String))

const toProjectionBlocked = (projection: string) => (cause: unknown) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.ProjectionBlocked({
      projection,
      cause
    })
  })

export class ProjectionStore extends Context.Service<ProjectionStore, {
  readonly clear: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly replace: <P extends Projection.Any,>(
    binding: SqlProjection.SqlProjection<P>,
    snapshot: Snapshot.FromDocument<P["document"]>,
    destinationTable: string
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly replaceDocument: <D extends Document.Any,>(
    document: D,
    snapshot: Snapshot.FromDocument<D>,
    commitSequence: Identity.CommitSequence
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/ProjectionStore") {}

export type BindingServices<Bindings extends ReadonlyArray<SqlProjection.Any>,> = Bindings[number] extends infer Binding
  ? Binding extends SqlProjection.SqlProjection<infer P> ? SqlProjection.BindingService<P>
  : never
  : never

export const layer = <const Bindings extends ReadonlyArray<SqlProjection.Any>,>(
  bindings: Bindings
): Layer.Layer<
  ProjectionStore,
  ReplicaError.ReplicaError,
  Crypto.Crypto | SqlClient.SqlClient | BindingServices<Bindings>
> =>
  Layer.effect(
    ProjectionStore,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const crypto = yield* Crypto.Crypto
      const context = yield* Effect.context<BindingServices<Bindings>>()
      const resolved = bindings.map((binding) => Context.getUnsafe(context, binding.service))
      const findRegistry = SqlSchema.findOneOption({
        Request: Schema.String,
        Result: Schema.Struct({
          projection_version: Schema.Int,
          schema_checksum: Schema.String,
          table_name: Schema.String
        }),
        execute: (projectionName) =>
          sql`SELECT table_name, projection_version, schema_checksum
            FROM effect_local_projection_registry WHERE projection_name = ${projectionName}`
      })
      const tables = new Set<string>()
      // One transaction over every binding's migrations and registry reconciliation
      // so a rejected binding rolls back the earlier ones instead of leaving a
      // partially migrated projection schema.
      yield* sql.withTransaction(Effect.gen(function*() {
        for (const binding of resolved) {
          if (tables.has(binding.table)) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProjectionBlocked({
                projection: binding.projection.name,
                cause: new Error(`Duplicate projection table: ${binding.table}`)
              })
            })
          }
          tables.add(binding.table)
          yield* Migrator.make({})({
            loader: Migrator.fromRecord(Object.fromEntries(binding.migrations.map((migration) => [
              `${migration.id}_${migration.name}`,
              migration.run(sql, binding.table)
            ]))),
            table: `${binding.table}_effect_sql_migrations`
          }).pipe(
            Effect.catchDefect((cause) =>
              Predicate.isTagged("MigrationError")(cause)
                ? Effect.fail(toProjectionBlocked(binding.projection.name)(cause))
                : Effect.die(cause)
            ),
            Effect.catchTag(["SqlError", "MigrationError"], (cause) =>
              Effect.fail(toProjectionBlocked(binding.projection.name)(cause)))
          )
          const checksum = yield* Canonical.digest(SchemaDescriptor.make(binding.projection.Row)).pipe(
            Effect.provideService(Crypto.Crypto, crypto)
          )
          const registry = yield* findRegistry(binding.projection.name).pipe(
            Effect.mapError(toProjectionBlocked(binding.projection.name))
          )
          if (registry._tag === "Some") {
            const row = registry.value
            if (
              row.table_name !== binding.table ||
              row.projection_version !== binding.projection.version ||
              row.schema_checksum !== checksum
            ) {
              yield* Effect.gen(function*() {
                yield* sql`DELETE FROM ${sql(binding.table)}`
                yield* sql`DELETE FROM effect_local_document_projections
                  WHERE projection_name = ${binding.projection.name}`
                yield* sql`UPDATE effect_local_projection_registry SET
                  table_name = ${binding.table},
                  projection_version = ${binding.projection.version},
                  schema_checksum = ${checksum},
                  status = 'Rebuilding'
                  WHERE projection_name = ${binding.projection.name}`
              }).pipe(Effect.mapError(toProjectionBlocked(binding.projection.name)))
            }
          } else {
            const populated = yield* SqlSchema.findOne({
              Request: Schema.String,
              Result: Schema.Struct({ populated: Schema.Int }),
              execute: (documentType) =>
                sql`SELECT EXISTS (
                  SELECT 1 FROM effect_local_documents WHERE document_type = ${documentType}
                ) AS populated`
            })(binding.projection.document.name).pipe(
              Effect.mapError(toProjectionBlocked(binding.projection.name))
            )
            const status = populated.populated === 1 ? "Rebuilding" : "Ready"
            yield* sql`INSERT INTO effect_local_projection_registry (
            projection_name, table_name, projection_version, schema_checksum, status
          ) VALUES (
            ${binding.projection.name}, ${binding.table}, ${binding.projection.version}, ${checksum}, ${status}
          )`.pipe(Effect.mapError(toProjectionBlocked(binding.projection.name)))
          }
        }
      })).pipe(
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({ cause })
            })
          ))
      )
      const replace = <P extends Projection.Any,>(
        binding: SqlProjection.SqlProjection<P>,
        snapshot: Snapshot.FromDocument<P["document"]>,
        destinationTable: string
      ) =>
        sql.withTransaction(Effect.gen(function*() {
          const rows = snapshot.tombstone ? [] : yield* Projection.evaluate(binding.projection, snapshot)
          for (const row of rows) {
            if (
              typeof row !== "object" || row === null || !("sourceDocumentId" in row) ||
              row.sourceDocumentId !== snapshot.documentId
            ) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.ProjectionBlocked({
                  projection: binding.projection.name,
                  cause: new Error("Projection row must contain its source document ID")
                })
              })
            }
          }
          yield* binding.deleteByDocument(sql, destinationTable, snapshot.documentId)
          yield* Effect.forEach(rows, (row) =>
            binding.insert(sql, destinationTable, row), { discard: true })
          yield* sql`INSERT INTO effect_local_document_projections (
            document_id, projection_name, projected_heads, status
          ) VALUES (
            ${snapshot.documentId}, ${binding.projection.name}, ${
            Schema.encodeSync(StringArrayJson)(snapshot.heads)
          }, 'Ready'
          ) ON CONFLICT(document_id, projection_name) DO UPDATE SET
            projected_heads = excluded.projected_heads,
            status = excluded.status`
        })).pipe(
          Effect.catchTag("SqlError", (cause) => Effect.fail(toProjectionBlocked(binding.projection.name)(cause)))
        )
      return ProjectionStore.of({
        clear: sql.withTransaction(Effect.gen(function*() {
          for (const binding of resolved) {
            yield* sql`DELETE FROM ${sql(binding.table)}`
          }
          yield* sql`DELETE FROM effect_local_document_projections`
        })).pipe(
          Effect.mapError(toProjectionBlocked("$all"))
        ),
        replace,
        replaceDocument: (document, snapshot, commitSequence) =>
          sql.withTransaction(Effect.gen(function*() {
            const matching = resolved.filter((binding) => binding.projection.document.name === document.name)
            for (const binding of matching) {
              yield* replace(binding, snapshot, binding.table)
            }
            yield* sql`UPDATE effect_local_commit_outbox
              SET invalidation_keys = ${
              Schema.encodeSync(StringArrayJson)([document.name, ...matching.map((binding) => binding.projection.name)])
            }
              WHERE commit_sequence = ${commitSequence} AND document_id = ${snapshot.documentId}`
          })).pipe(
            Effect.catchTag("SqlError", (cause) => Effect.fail(toProjectionBlocked(document.name)(cause)))
          )
      })
    })
  )
