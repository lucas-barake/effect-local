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

export const make = (definition: ReplicaDefinition.Any) =>
  Effect.gen(function*() {
    yield* Migrations.run
    const sql = yield* SqlClient.SqlClient
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
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: new Error("Replica metadata is missing")
            })
          })
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
      if (format?.storage_format_version !== storageFormatVersion) {
        return yield* Effect.die(new Error("Unsupported storage format version"))
      }
      if (format.definition_hash !== definition.hash) {
        return yield* new ReplicaError.ReplicaError({
          reason: new ReplicaError.ProtocolMismatch({
            expected: format.definition_hash,
            observed: definition.hash
          })
        })
      }
      yield* sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1`
      const row = yield* findPermit(undefined).pipe(
        Effect.catchTag("NoSuchElementError", () => Effect.die(new Error("Replica metadata was not initialized")))
      )
      yield* sql`INSERT INTO effect_local_writer_generations (generation, claimed_at)
        VALUES (${row.writer_generation}, ${DateTime.formatIso(yield* DateTime.now)})`
      return {
        replicaId: row.replica_id,
        incarnation: row.replica_incarnation,
        writerGeneration: row.writer_generation,
        definitionHash: definition.hash
      }
    })).pipe(
      Effect.catchTag("SchemaError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause
            })
          })
        ))
    )
  })

export const layer = (
  definition: ReplicaDefinition.Any
): Layer.Layer<
  ReplicaBootstrap,
  Migrator.MigrationError | SqlError.SqlError | ReplicaError.ReplicaError,
  Crypto.Crypto | SqlClient.SqlClient
> => Layer.effect(ReplicaBootstrap, make(definition))
