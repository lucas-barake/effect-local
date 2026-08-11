import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Schema from "effect/Schema"

const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

export const ClientMetaRow = Schema.Struct({
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  definition_hash: Schema.String,
  next_local_sequence: Identity.LocalSequence,
  server_cursor: Identity.ServerSequence,
  visible_revision: Identity.VisibleRevision,
  requested_generation: NonNegativeInt,
  completed_generation: NonNegativeInt
})

export const EntityRow = Schema.Struct({ value_json: Schema.String })

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

export const ReceiptRow = Schema.Struct({
  receipt_json: Schema.String
})

export const ServerMetaRow = Schema.Struct({
  definition_hash: Schema.String,
  next_server_sequence: PositiveInt
})

export const ServerClientRow = Schema.Struct({ last_local_sequence: NonNegativeInt })

export const ServerReceiptRow = Schema.Struct({
  space_id: Identity.SpaceId,
  client_id: Identity.ClientId,
  local_sequence: Identity.LocalSequence,
  digest: Protocol.MutationDigest,
  digest_version: Protocol.MutationDigestVersion,
  source_schema_version: Identity.SchemaVersion,
  source_schema_hash: Identity.SchemaHash,
  mutation_version: Identity.SchemaVersion,
  mutation_name: Schema.String,
  rejection_origin: Schema.NullOr(Protocol.RejectionOrigin),
  mutation_id: Identity.MutationId,
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
  mutation_id: Identity.MutationId,
  entry_bytes: PositiveInt,
  entry_json: Schema.String,
  receipt_client_id: Identity.ClientId,
  receipt_local_sequence: Identity.LocalSequence,
  digest: Protocol.MutationDigest,
  source_schema_version: Identity.SchemaVersion,
  source_schema_hash: Identity.SchemaHash
})

export const ChangeRow = Schema.Struct({
  mutation_id: Identity.MutationId,
  changes_json: Schema.String
})

export const CountRow = Schema.Struct({ count: NonNegativeInt })
