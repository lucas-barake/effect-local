import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Schema from "effect/Schema"
import * as CheckpointAuthority from "../CheckpointAuthority.js"
import * as WriterProvenance from "./writerProvenance.js"

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

/**
 * Result schemas for the durable tables.
 *
 * These live under `src/internal` because they describe the physical storage
 * shape rather than a consumer API. `Recovery` and `DocumentStore` share them so
 * the canonical read and the write path's read back decode the same row shape
 * through `SqlSchema` rather than asserting it with a bare `sql<T>` type
 * parameter. `BackupStore` keeps its own archive record schemas.
 */

export const HistoryCountersRow = Schema.Struct({
  history_bytes: Schema.NullOr(NonNegativeInt),
  history_changes: Schema.NullOr(NonNegativeInt),
  history_operations: Schema.NullOr(NonNegativeInt)
})

export const DocumentRow = Schema.Struct({
  accepted_heads: Schema.String,
  checkpoint_hash: Schema.NullOr(Schema.String),
  document_id: Schema.String,
  document_type: Schema.String,
  ...HistoryCountersRow.fields,
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
  writer_provenance: WriterProvenance.StoredCheckpointProvenance
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

export const CheckpointTransferColumn = Schema.Struct({
  checkpoint_transfer: Schema.NullOr(Schema.Uint8Array)
})

export const LineageTransitionRow = Schema.Struct({
  authorization: Schema.NullOr(CheckpointAuthority.AuthorizationToken),
  checkpoint_hash: Schema.String,
  created_at: Schema.String,
  document_id: Identity.DocumentId,
  heads: Schema.String,
  lineage: Identity.DocumentLineage,
  prior_checkpoint_hash: Schema.String,
  prior_heads: Schema.String,
  prior_lineage: Identity.DocumentLineage,
  prior_snapshot: Schema.Uint8Array,
  schema_version: WriterProvenance.WriterSchemaVersion,
  writer_definition_hash: WriterProvenance.WriterDefinitionHash
})
