import type * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Projection from "@lucas-barake/effect-local/Projection"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as DocumentStore from "./DocumentStore.js"
import * as InternalAutomerge from "./internal/automerge.js"
import * as ProjectionStore from "./ProjectionStore.js"

export interface State {
  readonly migratedDocuments: ReadonlyArray<{ readonly documentType: string; readonly count: number }>
  readonly rebuiltProjections: ReadonlyArray<string>
}

export class ReplicaEvolution extends Context.Service<ReplicaEvolution, State>()(
  "@lucas-barake/effect-local-sql/ReplicaEvolution"
) {}

export const make = (
  definition: ReplicaDefinition.Any
): Effect.Effect<
  State,
  ReplicaError.ReplicaError,
  DocumentStore.DocumentStore | ProjectionStore.ProjectionStore | SqlClient.SqlClient
> =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const store = yield* DocumentStore.DocumentStore
    const projections = yield* ProjectionStore.ProjectionStore

    const ProjectionNameRow = Schema.Struct({ projection_name: Schema.String })
    const findRebuilding = SqlSchema.findAll({
      Request: Schema.Void,
      Result: ProjectionNameRow,
      execute: () => sql`SELECT projection_name FROM effect_local_projection_registry WHERE status = 'Rebuilding'`
    })
    const findRegisteredProjections = SqlSchema.findAll({
      Request: Schema.Void,
      Result: ProjectionNameRow,
      execute: () => sql`SELECT projection_name FROM effect_local_projection_registry`
    })
    const findDocuments = SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({
        document_id: Identity.DocumentId,
        document_type: Schema.String,
        schema_version: Schema.Int
      }),
      execute: () => sql`SELECT document_id, document_type, schema_version FROM effect_local_documents`
    })

    const evolveDocument = (document: Document.Any, documentId: Identity.DocumentId) =>
      sql.withTransaction(
        Effect.acquireUseRelease(
          store.materialize(document, documentId),
          (stored) => projections.replaceDocument(document, stored.snapshot, stored.commitSequence),
          (stored) => Effect.sync(() => InternalAutomerge.free(stored.automerge))
        )
      )

    return yield* Effect.gen(function*() {
      const registered = new Set(definition.projections.map((projection: Projection.Any) => projection.name))
      const orphans = yield* findRegisteredProjections(undefined)
      for (const orphan of orphans) {
        if (registered.has(orphan.projection_name)) continue
        yield* sql`DELETE FROM effect_local_document_projections WHERE projection_name = ${orphan.projection_name}`
        yield* sql`DELETE FROM effect_local_projection_registry WHERE projection_name = ${orphan.projection_name}`
      }

      const rebuilding = yield* findRebuilding(undefined)
      const rebuildNames = rebuilding
        .map((row) => row.projection_name)
        .filter((name) => registered.has(name))
      const rebuildTypes = new Set(
        definition.projections
          .filter((projection: Projection.Any) => rebuildNames.includes(projection.name))
          .map((projection: Projection.Any) => projection.document.name)
      )

      const rows = yield* findDocuments(undefined)
      const migrated = new Map<string, number>()
      for (const row of rows) {
        const document = DocumentSet.get(definition.documents, row.document_type)
        if (document === undefined) {
          return yield* Effect.die(
            new Error(`Stored document type is not part of the accepted definition: ${row.document_type}`)
          )
        }
        const stale = row.schema_version !== document.version
        if (!stale && !rebuildTypes.has(row.document_type)) continue
        const evolved = yield* evolveDocument(document, row.document_id).pipe(
          Effect.as(true),
          Effect.catchReason("ReplicaError", "StorageCorrupt", (reason) =>
            Effect.logWarning("Skipping unrecoverable document during replica evolution", reason).pipe(
              Effect.annotateLogs({ documentId: row.document_id, documentType: row.document_type }),
              Effect.as(false)
            ))
        )
        if (evolved && stale) {
          migrated.set(row.document_type, (migrated.get(row.document_type) ?? 0) + 1)
        }
      }

      for (const name of rebuildNames) {
        yield* sql`UPDATE effect_local_projection_registry SET status = 'Ready' WHERE projection_name = ${name}`
      }

      return {
        migratedDocuments: [...migrated].map(([documentType, count]) => ({ documentType, count })),
        rebuiltProjections: rebuildNames
      }
    }).pipe(
      Effect.catchTags({
        SchemaError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause
              })
            })
          ),
        SqlError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause
              })
            })
          )
      })
    )
  })

export const layer = (
  definition: ReplicaDefinition.Any
): Layer.Layer<
  ReplicaEvolution,
  ReplicaError.ReplicaError,
  DocumentStore.DocumentStore | ProjectionStore.ProjectionStore | SqlClient.SqlClient
> => Layer.effect(ReplicaEvolution, make(definition))
