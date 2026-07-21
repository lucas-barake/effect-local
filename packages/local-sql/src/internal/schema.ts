export const storageFormatVersion = 1

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
  "effect_local_peer_outbox"
] as const
