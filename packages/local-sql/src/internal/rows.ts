import * as Schema from "effect/Schema"

export const ClientMetaRow = Schema.Struct({
  space_id: Schema.String,
  client_id: Schema.String,
  definition_hash: Schema.String,
  next_local_sequence: Schema.Number,
  server_cursor: Schema.Number,
  visible_revision: Schema.Number
})

export const EntityRow = Schema.Struct({ value_json: Schema.String })

export const PendingRow = Schema.Struct({
  mutation_id: Schema.String,
  local_sequence: Schema.Number,
  basis: Schema.Number,
  name: Schema.String,
  payload_json: Schema.String,
  digest: Schema.String,
  optimistic_result_json: Schema.String,
  changes_json: Schema.String
})

export const ReceiptRow = Schema.Struct({
  receipt_json: Schema.String
})

export const ServerMetaRow = Schema.Struct({
  definition_hash: Schema.String,
  next_server_sequence: Schema.Number
})

export const ServerClientRow = Schema.Struct({ last_local_sequence: Schema.Number })

export const ServerReceiptRow = Schema.Struct({
  digest: Schema.String,
  mutation_id: Schema.String,
  receipt_json: Schema.String
})

export const ServerLogRow = Schema.Struct({ entry_json: Schema.String })

export const ChangeRow = Schema.Struct({
  mutation_id: Schema.String,
  changes_json: Schema.String
})

export const CountRow = Schema.Struct({ count: Schema.Number })
