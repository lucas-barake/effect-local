import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as Crypto from "effect/Crypto"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import type * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { storageFormatVersion } from "./internal/schema.js"
import * as Migrations from "./Migrations.js"

export interface State {
  readonly replicaId: Identity.ReplicaId
  readonly incarnation: Identity.ReplicaIncarnation
  readonly writerGeneration: Identity.WriterGeneration
  readonly definitionHash: string
}

export class ReplicaBootstrap extends Context.Service<ReplicaBootstrap, State>()(
  "@lucas-barake/effect-local-sql/ReplicaBootstrap"
) {}

// A function, not a shared instance, so each failure captures its own stack at the site that observed it.
const metadataMissing = () =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.StorageCorrupt({
      cause: new Error("Replica metadata is missing")
    })
  })

const unsupportedStorageFormat = (observedVersion: number) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.UnsupportedStorageFormatVersion({
      observedVersion,
      supportedVersion: storageFormatVersion
    })
  })

export const make = (definition: ReplicaDefinition.Any) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const findMetadataTable = SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ name: Schema.String }),
      execute: () => sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'effect_local_metadata'`
    })
    const findStorageFormat = SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ storage_format_version: Schema.Int }),
      execute: () => sql`SELECT storage_format_version FROM effect_local_metadata WHERE singleton = 1`
    })
    // Migration 1 creates the metadata table together with every canonical store table, so whenever the
    // metadata table exists these are readable too. A populated replica whose metadata singleton is gone is
    // corrupt, and must be rejected before the migrator touches it rather than after. The peer tables are
    // deliberately absent here because migration 2 creates them and they may not exist yet; findPopulated
    // below covers them and stays the authoritative check.
    const findPopulatedBeforeMigrating = SqlSchema.findOneOption({
      Request: Schema.Void,
      Result: Schema.Struct({ populated: Schema.Int }),
      execute: () =>
        sql`SELECT EXISTS (
          SELECT 1 FROM effect_local_writer_generations
          UNION ALL SELECT 1 FROM effect_local_documents
          UNION ALL SELECT 1 FROM effect_local_changes
          UNION ALL SELECT 1 FROM effect_local_checkpoints
          UNION ALL SELECT 1 FROM effect_local_command_receipts
          UNION ALL SELECT 1 FROM effect_local_projection_registry
          UNION ALL SELECT 1 FROM effect_local_document_projections
          UNION ALL SELECT 1 FROM effect_local_commit_outbox
          UNION ALL SELECT 1 FROM effect_local_quarantine
          UNION ALL SELECT 1 FROM effect_local_backup_installations
        ) AS populated`
    })
    // The storage format version decides whether this build may touch the database at all, so it has to be
    // checked before Migrations.run, which commits its own transaction. Checking afterwards would mean a build
    // that refuses to open a replica has already migrated it. A database with no tables yet is a fresh one and
    // is left to the migrator. This runs outside the bootstrap transaction below, so it maps its own SchemaError.
    yield* Effect.gen(function*() {
      const table = yield* findMetadataTable(undefined)
      if (table.length === 0) return
      const stored = (yield* findStorageFormat(undefined))[0]
      if (stored === undefined) {
        const populated = yield* findPopulatedBeforeMigrating(undefined)
        if (populated._tag === "Some" && populated.value.populated === 1) {
          return yield* metadataMissing()
        }
        return
      }
      if (stored.storage_format_version === storageFormatVersion) return
      return yield* unsupportedStorageFormat(stored.storage_format_version)
    }).pipe(Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.StorageCorrupt({ cause })
        })
      )))
    yield* Migrations.run
    const findMetadata = SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ singleton: Schema.Int }),
      execute: () => sql`SELECT singleton FROM effect_local_metadata WHERE singleton = 1`
    })
    const findPopulated = SqlSchema.findOneOption({
      Request: Schema.Void,
      Result: Schema.Struct({ populated: Schema.Int }),
      execute: () =>
        sql`SELECT EXISTS (
          SELECT 1 FROM effect_local_writer_generations
          UNION ALL SELECT 1 FROM effect_local_documents
          UNION ALL SELECT 1 FROM effect_local_changes
          UNION ALL SELECT 1 FROM effect_local_checkpoints
          UNION ALL SELECT 1 FROM effect_local_command_receipts
          UNION ALL SELECT 1 FROM effect_local_projection_registry
          UNION ALL SELECT 1 FROM effect_local_document_projections
          UNION ALL SELECT 1 FROM effect_local_commit_outbox
          UNION ALL SELECT 1 FROM effect_local_quarantine
          UNION ALL SELECT 1 FROM effect_local_backup_installations
          UNION ALL SELECT 1 FROM effect_local_peer_receipts
          UNION ALL SELECT 1 FROM effect_local_peer_outbox
        ) AS populated`
    })
    const findFormat = SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({ definition_hash: Schema.String, storage_format_version: Schema.Int }),
      execute: () => sql`SELECT definition_hash, storage_format_version FROM effect_local_metadata WHERE singleton = 1`
    })
    const findStoredVersions = SqlSchema.findAll({
      Request: Schema.Void,
      Result: Schema.Struct({
        document_type: Schema.String,
        min_version: Schema.Int,
        max_version: Schema.Int
      }),
      execute: () =>
        sql`SELECT document_type, MIN(schema_version) AS min_version, MAX(schema_version) AS max_version
          FROM effect_local_documents GROUP BY document_type`
    })
    const findPermit = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Schema.Struct({
        replica_id: Identity.ReplicaId,
        replica_incarnation: Identity.ReplicaIncarnation,
        writer_generation: Identity.WriterGeneration
      }),
      execute: () =>
        sql`SELECT replica_id, replica_incarnation, writer_generation
          FROM effect_local_metadata WHERE singleton = 1`
    })
    return yield* sql.withTransaction(Effect.gen(function*() {
      const metadata = yield* findMetadata(undefined)
      if (metadata.length === 0) {
        const populated = yield* findPopulated(undefined)
        if (populated._tag === "Some" && populated.value.populated === 1) {
          return yield* metadataMissing()
        }
        yield* sql`INSERT INTO effect_local_metadata (
          singleton,
          storage_format_version,
          replica_id,
          replica_incarnation,
          writer_generation,
          definition_hash,
          commit_sequence
        ) VALUES (1, ${storageFormatVersion}, ${yield* Identity.makeReplicaId.pipe(
          Effect.mapError((cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({
                cause
              })
            })
          )
        )}, 0, 0, ${definition.hash}, 0)`
      }
      const format = (yield* findFormat(undefined))[0]
      if (format === undefined) {
        return yield* metadataMissing()
      }
      if (format.storage_format_version !== storageFormatVersion) {
        return yield* unsupportedStorageFormat(format.storage_format_version)
      }
      if (format.definition_hash !== definition.hash) {
        const stored = yield* findStoredVersions(undefined)
        const migratable = stored.every((row) => {
          const document = DocumentSet.get(definition.documents, row.document_type)
          return document !== undefined &&
            Document.supportsStoredVersion(document, row.min_version) &&
            Document.supportsStoredVersion(document, row.max_version)
        })
        if (!migratable) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: format.definition_hash,
              observed: definition.hash
            })
          })
        }
        yield* sql`UPDATE effect_local_metadata SET definition_hash = ${definition.hash} WHERE singleton = 1`
      }
      yield* sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1`
      const row = yield* findPermit(undefined).pipe(
        Effect.catchTag("NoSuchElementError", () => metadataMissing())
      )
      yield* sql`INSERT INTO effect_local_writer_generations (generation, claimed_at)
        VALUES (${row.writer_generation}, ${DateTime.formatIso(yield* DateTime.now)})`
      return {
        replicaId: row.replica_id,
        incarnation: row.replica_incarnation,
        writerGeneration: row.writer_generation,
        definitionHash: definition.hash
      }
    })).pipe(Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.StorageCorrupt({ cause })
        })
      )))
  })

export const layer = (
  definition: ReplicaDefinition.Any
): Layer.Layer<
  ReplicaBootstrap,
  Migrator.MigrationError | SqlError.SqlError | ReplicaError.ReplicaError,
  Crypto.Crypto | SqlClient.SqlClient
> => Layer.effect(ReplicaBootstrap, make(definition))
