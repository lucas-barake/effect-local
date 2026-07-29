export const storageFormatVersion = 2

export const tables = [
  "effect_local_metadata",
  "effect_local_migration_catalog",
  "effect_local_writer_generations",
  "effect_local_documents",
  "effect_local_changes",
  "effect_local_checkpoints",
  "effect_local_command_receipts",
  "effect_local_projection_registry",
  "effect_local_document_projections",
  "effect_local_commit_outbox",
  "effect_local_quarantine",
  "effect_local_backup_installations",
  "effect_local_peer_receipts",
  "effect_local_peer_outbox",
  "effect_local_history_rewrites",
  "effect_local_peer_relay_receipt_delete_tokens",
  "effect_local_peer_relay_receipt_usage",
  "effect_local_peer_relay_outbox",
  "effect_local_peer_relay_outbox_remote_usage",
  "effect_local_peer_relay_outbox_replica_usage"
] as const

/**
 * The tables whose contents mean "this replica holds durable state", used to decide whether a
 * database carrying no metadata singleton is a fresh one or a corrupt one.
 *
 * Deliberately not every table in `tables`. `effect_local_metadata` is the row being looked for, and
 * `effect_local_migration_catalog` gains a row from every migration, so a probe counting either would
 * report a brand new database as populated and no replica could ever be created. The migrator's own
 * `effect_local_migrations` is excluded for the same reason, and is not in `tables` at all because
 * the migrator owns it rather than this schema.
 */
export const populatedTables: ReadonlyArray<string> = tables.filter(
  (table) => table !== "effect_local_metadata" && table !== "effect_local_migration_catalog"
)
