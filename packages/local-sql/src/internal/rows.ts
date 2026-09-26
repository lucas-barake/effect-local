import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Schema from "effect/Schema"
import * as SchemaTransformation from "effect/SchemaTransformation"

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

const SqlNumber = Schema.Union([
  Schema.Number,
  Schema.BigInt.check(Schema.isBetweenBigInt({
    minimum: BigInt(Number.MIN_SAFE_INTEGER),
    maximum: BigInt(Number.MAX_SAFE_INTEGER)
  }))
]).pipe(Schema.decodeTo(
  Schema.Number,
  SchemaTransformation.transform<number, number | bigint>({
    decode: (value) => Number(value),
    encode: (value) => value
  })
))

export const integer = <S extends Schema.Codec<number, number>,>(schema: S) => SqlNumber.pipe(Schema.decodeTo(schema))

export const ClientMetaRow = Schema.Struct({
  space_id: Identity.SpaceId,
  membership_incarnation: Identity.MembershipIncarnation,
  definition_hash: Schema.String,
  schema_version: Identity.SchemaVersion,
  schema_hash: Identity.SchemaHash,
  schema_generation: NonNegativeInt,
  active_schema_generation: NonNegativeInt,
  active_projection_generation: NonNegativeInt,
  projection_schema_generation: NonNegativeInt,
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
  installed_snapshot_terminal_sequence: Identity.TerminalSequence,
  replication_view_id: Schema.NullOr(Identity.ReplicationViewId),
  replication_view_revision: Identity.ReplicationViewRevision,
  desired_scope_json: Schema.String,
  desired_scope_digest: Protocol.MutationDigest,
  scope_generation: Identity.ReplicationScopeGeneration,
  projection_replay_generation: Schema.NullOr(NonNegativeInt),
  projection_replay_cursor: Schema.NullOr(Schema.String)
})

export const ClientScopedBootstrapRow = Schema.Struct({
  snapshot_id: Identity.SnapshotId,
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  definition_hash: Schema.String,
  schema_version: Identity.SchemaVersion,
  schema_hash: Identity.SchemaHash,
  scope_digest: Protocol.MutationDigest,
  scope_generation: Identity.ReplicationScopeGeneration,
  view_id: Identity.ReplicationViewId,
  view_revision: Identity.ReplicationViewRevision,
  server_sequence: Identity.ServerSequence,
  terminal_sequence: Identity.TerminalSequence,
  entry_count: NonNegativeInt,
  content_bytes: NonNegativeInt,
  digest: Protocol.SnapshotDigest,
  next_ordinal: NonNegativeInt,
  received_bytes: NonNegativeInt,
  rolling_digest: Protocol.SnapshotDigest
})

export const EntityRow = Schema.Struct({ value_json: Schema.String })
export const ProjectionEntityRow = Schema.Struct({
  model: Schema.String,
  model_version: Identity.SchemaVersion,
  entity_key: Schema.String,
  value_json: Schema.String
})
export const RowIdRow = Schema.Struct({ row_id: NonNegativeInt })
export const SizedEntityRow = Schema.Struct({
  value_json: Schema.String,
  entity_bytes: NonNegativeInt
})

const MutationRowFields = {
  membership_incarnation: Identity.MembershipIncarnation,
  mutation_id: Identity.MutationId,
  local_sequence: Identity.LocalSequence,
  basis: Identity.ServerSequence,
  name: Schema.String,
  payload_json: Schema.String,
  digest: Protocol.MutationDigest,
  digest_version: Protocol.MutationDigestVersion,
  source_schema_version: Identity.SchemaVersion,
  source_schema_hash: Identity.SchemaHash,
  mutation_version: Identity.SchemaVersion
}

const PendingRowFields = {
  ...MutationRowFields,
  optimistic_result_json: Schema.String,
  changes_json: Schema.String,
  submission_state: Protocol.SubmissionState,
  attempt_count: NonNegativeInt
}

export const PendingRow = Schema.Struct(PendingRowFields)

export const SettledReceiptRow = Schema.Struct({
  receipt_json: Schema.String,
  settled_pending_json: Schema.String,
  settled_sequence: Identity.SettlementSequence
})

export const SettlementStateRow = Schema.Struct({
  next_settled_sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  settlement_floor: NonNegativeInt,
  settlement_prune_sequence: NonNegativeInt
})

export const QuarantineRow = Schema.Struct({
  space_id: Identity.SpaceId,
  ...MutationRowFields,
  rejection_json: Schema.String,
  target_schema_version: Identity.SchemaVersion,
  target_schema_hash: Identity.SchemaHash
})

export const QuarantineResubmissionRow = Schema.Struct({
  space_id: Identity.SpaceId,
  original_mutation_id: Identity.MutationId,
  replacement_mutation_id: Identity.MutationId
})

export const QuarantineCancellationRow = Schema.Struct({
  space_id: Identity.SpaceId,
  root_mutation_id: Identity.MutationId,
  current_mutation_id: Identity.MutationId
})

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

export const SettledPendingRow = Schema.Struct({
  settled_pending_json: Schema.NullOr(Schema.String)
})

export const ServerMetaRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: integer(Identity.SchemaVersion),
  schema_hash: Identity.SchemaHash,
  schema_generation: integer(NonNegativeInt),
  active_schema_generation: integer(NonNegativeInt),
  target_schema_version: Schema.NullOr(integer(Identity.SchemaVersion)),
  target_schema_hash: Schema.NullOr(Identity.SchemaHash),
  migration_hash: Schema.NullOr(Identity.SchemaHash),
  next_server_sequence: integer(PositiveInt),
  next_terminal_sequence: integer(PositiveInt),
  history_floor: integer(Identity.ServerSequence),
  receipt_floor: integer(Identity.TerminalSequence),
  retained_history_count: integer(NonNegativeInt),
  retained_receipt_count: integer(NonNegativeInt),
  entity_count: integer(NonNegativeInt),
  entity_bytes: integer(NonNegativeInt),
  snapshot_id: Schema.NullOr(Identity.SnapshotId),
  snapshot_sequence: integer(Identity.ServerSequence),
  snapshot_terminal_sequence: integer(Identity.TerminalSequence),
  metadata_verified: Schema.Literals([0, 1])
})

export const ServerClientRow = Schema.Struct({
  last_local_sequence: integer(NonNegativeInt),
  expired_local_sequence: integer(NonNegativeInt)
})

export const ServerCountRow = Schema.Struct({
  history_count: integer(NonNegativeInt),
  receipt_count: integer(NonNegativeInt)
})

export const ReplicationSpaceRow = Schema.Struct({
  definition_hash: Schema.String,
  schema_version: integer(Identity.SchemaVersion),
  schema_hash: Identity.SchemaHash,
  schema_generation: integer(NonNegativeInt),
  active_schema_generation: integer(NonNegativeInt),
  target_schema_version: Schema.NullOr(integer(Identity.SchemaVersion)),
  target_schema_hash: Schema.NullOr(Identity.SchemaHash),
  migration_hash: Schema.NullOr(Identity.SchemaHash),
  next_server_sequence: integer(PositiveInt),
  next_terminal_sequence: integer(PositiveInt),
  read_auth_epoch: integer(NonNegativeInt)
})

export const ServerReceiptRow = Schema.Struct({
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  membership_incarnation: Identity.MembershipIncarnation,
  local_sequence: integer(Identity.LocalSequence),
  digest: Protocol.MutationDigest,
  digest_version: Protocol.MutationDigestVersion,
  source_schema_version: integer(Identity.SchemaVersion),
  source_schema_hash: Identity.SchemaHash,
  mutation_version: Schema.NullOr(integer(Identity.SchemaVersion)),
  mutation_name: Schema.NullOr(Schema.String),
  rejection_origin: Schema.NullOr(Protocol.RejectionOrigin),
  mutation_id: Identity.MutationId,
  terminal_sequence: integer(Identity.TerminalSequence),
  receipt_json: Schema.String,
  server_sequence: Schema.NullOr(integer(Identity.ServerSequence))
})

export const ClientLogRow = Schema.Struct({
  membership_incarnation: Identity.MembershipIncarnation,
  server_sequence: integer(Identity.ServerSequence),
  mutation_id: Identity.MutationId,
  entry_json: Schema.String
})

export const ServerLogMetadataRow = Schema.Struct({
  server_sequence: integer(Identity.ServerSequence),
  entry_bytes: integer(PositiveInt)
})

export const ServerLogRow = Schema.Struct({
  space_id: Identity.SpaceId,
  server_sequence: integer(Identity.ServerSequence),
  client_id: Identity.ClientId,
  membership_incarnation: Identity.MembershipIncarnation,
  local_sequence: integer(Identity.LocalSequence),
  mutation_id: Identity.MutationId,
  digest: Protocol.MutationDigest,
  entry_bytes: integer(PositiveInt),
  entry_json: Schema.String,
  source_schema_version: integer(Identity.SchemaVersion),
  source_schema_hash: Identity.SchemaHash
})

export const ServerEntityRow = Schema.Struct({
  model: Schema.String,
  model_version: integer(Identity.SchemaVersion),
  entity_key: Schema.String,
  value_json: Schema.String,
  entity_bytes: integer(NonNegativeInt)
})

export const SnapshotManifestRow = Schema.Struct({
  space_id: Identity.SpaceId,
  snapshot_id: Identity.SnapshotId,
  definition_hash: Schema.String,
  schema_version: integer(Identity.SchemaVersion),
  schema_hash: Identity.SchemaHash,
  server_sequence: integer(Identity.ServerSequence),
  terminal_sequence: integer(Identity.TerminalSequence),
  entity_count: integer(NonNegativeInt),
  content_bytes: integer(NonNegativeInt),
  digest: Protocol.SnapshotDigest
})

export const SnapshotEntityRow = Schema.Struct({
  ordinal: integer(NonNegativeInt),
  model: Schema.String,
  model_version: integer(Identity.SchemaVersion),
  entity_key: Schema.String,
  value_json: Schema.String,
  entity_bytes: integer(PositiveInt)
})

export const SnapshotEntityMetadataRow = Schema.Struct({
  ordinal: integer(NonNegativeInt),
  wire_bytes: integer(PositiveInt)
})

export const SnapshotEntityWireRow = Schema.Struct({
  ordinal: integer(NonNegativeInt),
  wire_json: Schema.String,
  wire_bytes: integer(PositiveInt)
})

export const SnapshotProjectionRow = Schema.Struct({
  space_id: Identity.SpaceId,
  snapshot_id: Identity.SnapshotId,
  target_schema_version: integer(Identity.SchemaVersion),
  target_schema_hash: Identity.SchemaHash,
  definition_hash: Schema.String,
  entity_count: integer(NonNegativeInt),
  content_bytes: integer(NonNegativeInt),
  digest: Protocol.SnapshotDigest
})

export const ReplicationViewRow = Schema.Struct({
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  principal_digest: Protocol.MutationDigest,
  view_id: Identity.ReplicationViewId,
  view_revision: integer(Identity.ReplicationViewRevision),
  scope_generation: integer(Identity.ReplicationScopeGeneration),
  scope_json: Schema.String,
  scope_digest: Protocol.MutationDigest,
  definition_hash: Schema.String,
  index_layout_hash: Schema.String,
  schema_version: integer(Identity.SchemaVersion),
  schema_hash: Identity.SchemaHash,
  server_sequence: integer(Identity.ServerSequence),
  delivered_sequence: integer(Identity.ServerSequence),
  read_auth_epoch: integer(NonNegativeInt)
})

export const ReplicationViewEntityRow = Schema.Struct({
  model: Schema.String,
  model_version: integer(Identity.SchemaVersion),
  entity_key: Schema.String,
  disposition: Schema.Literals(["Upsert", "Delete", "Retract"]),
  value_json: Schema.NullOr(Schema.String)
})

export const ReplicationPageRow = Schema.Struct({
  principal_digest: Protocol.MutationDigest,
  view_id: Identity.ReplicationViewId,
  base_revision: integer(Identity.ReplicationViewRevision),
  target_revision: integer(Identity.ReplicationViewRevision),
  scope_generation: integer(Identity.ReplicationScopeGeneration),
  scope_json: Schema.String,
  scope_digest: Protocol.MutationDigest,
  server_sequence: integer(Identity.ServerSequence),
  changes_json: Schema.String,
  content_bytes: integer(NonNegativeInt),
  digest: Protocol.MutationDigest,
  has_more: Schema.Literals([0, 1]),
  read_auth_epoch: integer(NonNegativeInt)
})

export const ScopedSnapshotManifestRow = Schema.Struct({
  snapshot_id: Identity.SnapshotId,
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  principal_digest: Protocol.MutationDigest,
  definition_hash: Schema.String,
  index_layout_hash: Schema.String,
  schema_version: integer(Identity.SchemaVersion),
  schema_hash: Identity.SchemaHash,
  scope_json: Schema.String,
  scope_digest: Protocol.MutationDigest,
  scope_generation: integer(Identity.ReplicationScopeGeneration),
  view_id: Identity.ReplicationViewId,
  view_revision: integer(Identity.ReplicationViewRevision),
  server_sequence: integer(Identity.ServerSequence),
  terminal_sequence: integer(Identity.TerminalSequence),
  entry_count: integer(NonNegativeInt),
  content_bytes: integer(NonNegativeInt),
  digest: Protocol.SnapshotDigest
})

export const ScopedSnapshotEntryRow = Schema.Struct({
  ordinal: integer(NonNegativeInt),
  change_json: Schema.String,
  entry_bytes: integer(PositiveInt)
})

export const ServerScopedSnapshotEntryRow = Schema.Struct({
  ordinal: integer(NonNegativeInt),
  change_json: Schema.String,
  entry_bytes: integer(PositiveInt),
  source_model: Schema.String,
  source_model_version: integer(Identity.SchemaVersion),
  source_entity_key: Schema.String,
  source_value_json: Schema.String
})

export const ChangeRow = Schema.Struct({
  mutation_id: Identity.MutationId,
  changes_json: Schema.String
})

export const CountRow = Schema.Struct({ count: integer(NonNegativeInt) })
export const SpaceIdRow = Schema.Struct({ space_id: Identity.SpaceId })
export const MutationIdRow = Schema.Struct({ mutation_id: Identity.MutationId })
export const SpacePendingCountRow = Schema.Struct({
  space_id: Identity.SpaceId,
  count: integer(NonNegativeInt)
})
export const SequenceRow = Schema.Struct({ server_sequence: integer(Identity.ServerSequence) })
export const TerminalReceiptIdentityRow = Schema.Struct({
  terminal_sequence: integer(Identity.TerminalSequence),
  client_id: Identity.ClientId,
  local_sequence: integer(Identity.LocalSequence)
})
export const SnapshotIdRow = Schema.Struct({ snapshot_id: Identity.SnapshotId })
export const EntityIdentityRow = Schema.Struct({
  model: Schema.String,
  model_version: integer(Identity.SchemaVersion),
  entity_key: Schema.String
})
export const OrdinalRow = Schema.Struct({ ordinal: integer(NonNegativeInt) })
