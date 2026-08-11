import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Schema from "effect/Schema"

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

export const ClientMetaRow = Schema.Struct({
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  definition_hash: Schema.String,
  schema_version: Identity.SchemaVersion,
  schema_hash: Identity.SchemaHash,
  schema_generation: NonNegativeInt,
  active_schema_generation: NonNegativeInt,
  target_schema_version: Schema.NullOr(Identity.SchemaVersion),
  target_schema_hash: Schema.NullOr(Identity.SchemaHash),
  migration_hash: Schema.NullOr(Identity.SchemaHash),
  next_local_sequence: Identity.LocalSequence,
  server_cursor: Identity.ServerSequence,
  visible_revision: Identity.VisibleRevision,
  requested_generation: NonNegativeInt,
  completed_generation: NonNegativeInt,
  installed_snapshot_id: Schema.NullOr(Identity.SnapshotId),
  installed_snapshot_sequence: Identity.ServerSequence,
  installed_snapshot_terminal_sequence: Identity.TerminalSequence
})

export const ClientReplicationMetaRow = Schema.Struct({
  replication_view_id: Schema.NullOr(Identity.ReplicationViewId),
  replication_view_revision: Identity.ReplicationViewRevision,
  desired_scope_json: Schema.String,
  desired_scope_digest: Protocol.MutationDigest,
  scope_generation: Identity.ReplicationScopeGeneration
})

export const ClientRetractionRow = Schema.Struct({
  generation: NonNegativeInt,
  model: Schema.String,
  model_version: Identity.SchemaVersion,
  entity_key: Schema.String
})

export const EntityRow = Schema.Struct({ value_json: Schema.String })
export const SizedEntityRow = Schema.Struct({
  value_json: Schema.String,
  entity_bytes: NonNegativeInt
})

const PendingRowFields = {
  mutation_id: Identity.MutationId,
  local_sequence: Identity.LocalSequence,
  basis: Identity.ServerSequence,
  name: Schema.String,
  payload_json: Schema.String,
  digest: Protocol.MutationDigest,
  digest_version: Protocol.MutationDigestVersion,
  source_schema_version: Identity.SchemaVersion,
  source_schema_hash: Identity.SchemaHash,
  mutation_version: Identity.SchemaVersion,
  optimistic_result_json: Schema.String,
  changes_json: Schema.String
}

export const PendingRow = Schema.Struct(PendingRowFields)

export const PendingReceiptRow = Schema.Struct({
  ...PendingRowFields,
  receipt_json: Schema.String,
  server_sequence: Schema.NullOr(Identity.ServerSequence),
  entry_mutation_id: Schema.NullOr(Identity.MutationId),
  entry_json: Schema.NullOr(Schema.String)
})

export const PendingLogRow = Schema.Struct({
  ...PendingRowFields,
  server_sequence: Identity.ServerSequence,
  entry_mutation_id: Identity.MutationId,
  entry_json: Schema.String
})

export const ReceiptRow = Schema.Struct({
  receipt_json: Schema.String
})

export const ServerMetaRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: Identity.SchemaVersion,
  schema_hash: Identity.SchemaHash,
  schema_generation: NonNegativeInt,
  active_schema_generation: NonNegativeInt,
  target_schema_version: Schema.NullOr(Identity.SchemaVersion),
  target_schema_hash: Schema.NullOr(Identity.SchemaHash),
  migration_hash: Schema.NullOr(Identity.SchemaHash),
  next_server_sequence: PositiveInt,
  next_terminal_sequence: PositiveInt,
  history_floor: Identity.ServerSequence,
  receipt_floor: Identity.TerminalSequence,
  retained_history_count: NonNegativeInt,
  retained_receipt_count: NonNegativeInt,
  entity_count: NonNegativeInt,
  entity_bytes: NonNegativeInt,
  snapshot_id: Schema.NullOr(Identity.SnapshotId),
  snapshot_sequence: Identity.ServerSequence,
  snapshot_terminal_sequence: Identity.TerminalSequence,
  metadata_verified: Schema.Literals([0, 1])
})

export const ServerClientRow = Schema.Struct({
  last_local_sequence: NonNegativeInt,
  expired_local_sequence: NonNegativeInt
})

export const ServerCountRow = Schema.Struct({
  history_count: NonNegativeInt,
  receipt_count: NonNegativeInt
})

export const ServerReceiptRow = Schema.Struct({
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  local_sequence: Identity.LocalSequence,
  digest: Protocol.MutationDigest,
  digest_version: Protocol.MutationDigestVersion,
  source_schema_version: Identity.SchemaVersion,
  source_schema_hash: Identity.SchemaHash,
  mutation_version: Schema.NullOr(Identity.SchemaVersion),
  mutation_name: Schema.NullOr(Schema.String),
  rejection_origin: Schema.NullOr(Protocol.RejectionOrigin),
  mutation_id: Identity.MutationId,
  terminal_sequence: Identity.TerminalSequence,
  receipt_json: Schema.String,
  server_sequence: Schema.NullOr(Identity.ServerSequence)
})

export const ClientLogRow = Schema.Struct({
  server_sequence: Identity.ServerSequence,
  mutation_id: Identity.MutationId,
  entry_json: Schema.String
})

export const ServerLogMetadataRow = Schema.Struct({
  server_sequence: Identity.ServerSequence,
  entry_bytes: PositiveInt
})

export const ServerLogRow = Schema.Struct({
  space_id: Identity.SpaceId,
  server_sequence: Identity.ServerSequence,
  client_id: Identity.ClientId,
  local_sequence: Identity.LocalSequence,
  mutation_id: Identity.MutationId,
  digest: Protocol.MutationDigest,
  entry_bytes: PositiveInt,
  entry_json: Schema.String,
  source_schema_version: Identity.SchemaVersion,
  source_schema_hash: Identity.SchemaHash
})

export const ServerEntityRow = Schema.Struct({
  model: Schema.String,
  model_version: Identity.SchemaVersion,
  entity_key: Schema.String,
  value_json: Schema.String,
  entity_bytes: NonNegativeInt
})

export const SnapshotManifestRow = Schema.Struct({
  space_id: Identity.SpaceId,
  snapshot_id: Identity.SnapshotId,
  definition_hash: Schema.String,
  schema_version: Identity.SchemaVersion,
  schema_hash: Identity.SchemaHash,
  server_sequence: Identity.ServerSequence,
  terminal_sequence: Identity.TerminalSequence,
  entity_count: NonNegativeInt,
  content_bytes: NonNegativeInt,
  digest: Protocol.SnapshotDigest
})

export const SnapshotEntityRow = Schema.Struct({
  ordinal: NonNegativeInt,
  model: Schema.String,
  model_version: Identity.SchemaVersion,
  entity_key: Schema.String,
  value_json: Schema.String,
  entity_bytes: PositiveInt
})

export const SnapshotEntityMetadataRow = Schema.Struct({
  ordinal: NonNegativeInt,
  wire_bytes: PositiveInt
})

export const SnapshotEntityWireRow = Schema.Struct({
  ordinal: NonNegativeInt,
  wire_json: Schema.String,
  wire_bytes: PositiveInt
})

export const ReplicationViewRow = Schema.Struct({
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  principal_digest: Protocol.MutationDigest,
  view_id: Identity.ReplicationViewId,
  view_revision: Identity.ReplicationViewRevision,
  scope_generation: Identity.ReplicationScopeGeneration,
  scope_json: Schema.String,
  scope_digest: Protocol.MutationDigest,
  definition_hash: Schema.String,
  schema_version: Identity.SchemaVersion,
  schema_hash: Identity.SchemaHash,
  server_sequence: Identity.ServerSequence
})

export const ReplicationViewEntityRow = Schema.Struct({
  model: Schema.String,
  model_version: Identity.SchemaVersion,
  entity_key: Schema.String,
  disposition: Schema.Literals(["Upsert", "Delete", "Retract"]),
  value_json: Schema.NullOr(Schema.String)
})

export const ReplicationPageRow = Schema.Struct({
  principal_digest: Protocol.MutationDigest,
  view_id: Identity.ReplicationViewId,
  base_revision: Identity.ReplicationViewRevision,
  target_revision: Identity.ReplicationViewRevision,
  scope_generation: Identity.ReplicationScopeGeneration,
  scope_json: Schema.String,
  scope_digest: Protocol.MutationDigest,
  server_sequence: Identity.ServerSequence,
  changes_json: Schema.String,
  content_bytes: NonNegativeInt,
  digest: Protocol.MutationDigest,
  has_more: Schema.Literals([0, 1])
})

export const ScopedSnapshotManifestRow = Schema.Struct({
  snapshot_id: Identity.SnapshotId,
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  principal_digest: Protocol.MutationDigest,
  definition_hash: Schema.String,
  schema_version: Identity.SchemaVersion,
  schema_hash: Identity.SchemaHash,
  scope_json: Schema.String,
  scope_digest: Protocol.MutationDigest,
  scope_generation: Identity.ReplicationScopeGeneration,
  view_id: Identity.ReplicationViewId,
  view_revision: Identity.ReplicationViewRevision,
  server_sequence: Identity.ServerSequence,
  terminal_sequence: Identity.TerminalSequence,
  entry_count: NonNegativeInt,
  content_bytes: NonNegativeInt,
  digest: Protocol.SnapshotDigest
})

export const ScopedSnapshotEntryRow = Schema.Struct({
  ordinal: NonNegativeInt,
  change_json: Schema.String,
  entry_bytes: PositiveInt
})

export const BootstrapRow = Schema.Struct({
  snapshot_id: Identity.SnapshotId,
  space_id: Identity.SpaceId,
  definition_hash: Schema.String,
  schema_version: Identity.SchemaVersion,
  schema_hash: Identity.SchemaHash,
  server_sequence: Identity.ServerSequence,
  terminal_sequence: Identity.TerminalSequence,
  entity_count: NonNegativeInt,
  content_bytes: NonNegativeInt,
  digest: Protocol.SnapshotDigest,
  next_ordinal: NonNegativeInt,
  received_bytes: NonNegativeInt,
  rolling_digest: Protocol.SnapshotDigest
})

export const ChangeRow = Schema.Struct({
  mutation_id: Identity.MutationId,
  changes_json: Schema.String
})

export const CountRow = Schema.Struct({ count: NonNegativeInt })
export const SpaceIdRow = Schema.Struct({ space_id: Identity.SpaceId })
export const SequenceRow = Schema.Struct({ server_sequence: Identity.ServerSequence })
export const TerminalReceiptIdentityRow = Schema.Struct({
  terminal_sequence: Identity.TerminalSequence,
  client_id: Identity.ClientId,
  local_sequence: Identity.LocalSequence
})
export const SnapshotIdRow = Schema.Struct({ snapshot_id: Identity.SnapshotId })
export const MutationIdRow = Schema.Struct({ mutation_id: Identity.MutationId })
export const EntityIdentityRow = Schema.Struct({
  model: Schema.String,
  model_version: Identity.SchemaVersion,
  entity_key: Schema.String
})
export const OrdinalRow = Schema.Struct({ ordinal: NonNegativeInt })
