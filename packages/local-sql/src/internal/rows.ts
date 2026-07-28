import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Schema from "effect/Schema"
import * as WriterProvenance from "./writerProvenance.js"

/**
 * Result schemas for the durable tables.
 *
 * These live under `src/internal` because they describe the physical storage
 * shape rather than a consumer API. `Recovery` and `DocumentStore` share them so
 * the canonical read and the write path's read back decode the same row shape
 * through `SqlSchema` rather than asserting it with a bare `sql<T>` type
 * parameter. `BackupStore` keeps its own archive record schemas, and
 * `Migrations` still reads these tables with bare `sql<T>` type parameters.
 */

export const DocumentRow = Schema.Struct({
  accepted_heads: Schema.String,
  checkpoint_hash: Schema.NullOr(Schema.String),
  document_id: Schema.String,
  document_type: Schema.String,
  lineage: Identity.DocumentLineage,
  materialized_heads: Schema.String,
  observed_versions: Schema.String,
  projection_status: Schema.Literals(["Ready", "Blocked", "Rebuilding"]),
  schema_version: Schema.Number,
  tombstone: Schema.Number
})

export const CheckpointRow = Schema.Struct({
  bytes: Schema.Uint8Array,
  checkpoint_hash: Schema.String,
  checksum: Schema.String,
  commit_sequence: Schema.Number,
  document_id: Schema.String,
  heads: Schema.String,
  lineage: Identity.DocumentLineage,
  verified: Schema.Number,
  writer_provenance: WriterProvenance.StoredChangeProvenances
})

export const ChangeRow = Schema.Struct({
  actor: Schema.String,
  accepted_at: Schema.String,
  applied: Schema.Number,
  bytes: Schema.Uint8Array,
  change_hash: WriterProvenance.ChangeHash,
  commit_sequence: Schema.Number,
  dependencies: Schema.String,
  document_id: Schema.String,
  document_type: Schema.String,
  peer_id: Schema.NullOr(Schema.String),
  sequence: Schema.Number,
  writer_definition_hash: WriterProvenance.WriterDefinitionHash,
  writer_schema_version: WriterProvenance.WriterSchemaVersion
})
