import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as SqlClient from "effect/unstable/sql/SqlClient"
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
    return yield* sql.withTransaction(Effect.gen(function*() {
      const metadata = yield* sql`SELECT singleton FROM effect_local_metadata WHERE singleton = 1`
      if (metadata.length === 0) {
        const populated = yield* sql<{ readonly populated: number }>`SELECT EXISTS (
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
        if (populated[0]?.populated === 1) {
          return yield* new ReplicaError.ReplicaError({
            reason: {
              _tag: "StorageCorrupt",
              cause: { _tag: "SchemaCause", message: "Replica metadata is missing", path: [] }
            }
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
        ) VALUES (1, ${storageFormatVersion}, ${Identity.makeReplicaId()}, 0, 0, ${definition.hash}, 0)`
      }
      const formats = yield* sql<{ readonly definition_hash: string; readonly storage_format_version: number }>`
        SELECT definition_hash, storage_format_version FROM effect_local_metadata WHERE singleton = 1
      `
      if (formats[0]?.storage_format_version !== storageFormatVersion) {
        return yield* Effect.die(new Error("Unsupported storage format version"))
      }
      if (formats[0].definition_hash !== definition.hash) {
        return yield* new ReplicaError.ReplicaError({
          reason: {
            _tag: "ProtocolMismatch",
            expected: formats[0].definition_hash,
            observed: definition.hash
          }
        })
      }
      yield* sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1 WHERE singleton = 1`
      const rows = yield* sql<{
        readonly replica_id: string
        readonly replica_incarnation: number
        readonly writer_generation: number
      }>`SELECT replica_id, replica_incarnation, writer_generation
        FROM effect_local_metadata WHERE singleton = 1`
      const row = rows[0]
      if (row === undefined) return yield* Effect.die(new Error("Replica metadata was not initialized"))
      yield* sql`INSERT INTO effect_local_writer_generations (generation, claimed_at)
        VALUES (${row.writer_generation}, ${new Date().toISOString()})`
      return yield* Effect.try({
        try: (): State => ({
          replicaId: Identity.ReplicaId.make(row.replica_id),
          incarnation: Identity.ReplicaIncarnation.make(row.replica_incarnation),
          writerGeneration: Identity.WriterGeneration.make(row.writer_generation),
          definitionHash: definition.hash
        }),
        catch: (cause) =>
          new ReplicaError.ReplicaError({
            reason: {
              _tag: "StorageCorrupt",
              cause: { _tag: "SchemaCause", message: String(cause), path: [] }
            }
          })
      })
    }))
  })

export const layer = (definition: ReplicaDefinition.Any) => Layer.effect(ReplicaBootstrap, make(definition))
