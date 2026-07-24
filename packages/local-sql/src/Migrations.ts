import * as Automerge from "@automerge/automerge"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Migrator from "effect/unstable/sql/Migrator"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as WriterProvenance from "./internal/writerProvenance.js"

export const canonicalStoreChecksum = "sha256:effect-local-canonical-store-v1"
export const peerSyncChecksum = "sha256:effect-local-peer-sync-v3"
export const durabilityIndexesChecksum = "sha256:effect-local-durability-indexes-v1"
export const projectionReadinessChecksum = "sha256:effect-local-projection-readiness-v1"
export const pendingReceiptIndexesChecksum = "sha256:effect-local-pending-receipt-indexes-v1"
export const peerWriterProvenanceChecksum = "sha256:effect-local-peer-writer-provenance-v1"

const migration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE effect_local_migration_catalog (
    migration_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    storage_format_version INTEGER NOT NULL,
    replica_id TEXT NOT NULL,
    replica_incarnation INTEGER NOT NULL,
    writer_generation INTEGER NOT NULL,
    definition_hash TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_writer_generations (
    generation INTEGER PRIMARY KEY,
    claimed_at TEXT NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_documents (
    document_id TEXT PRIMARY KEY,
    document_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    observed_versions TEXT NOT NULL,
    materialized_heads TEXT NOT NULL,
    accepted_heads TEXT NOT NULL,
    tombstone INTEGER NOT NULL,
    projection_status TEXT NOT NULL,
    checkpoint_hash TEXT
  )`
  yield* sql`CREATE TABLE effect_local_changes (
    change_hash TEXT PRIMARY KEY,
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    document_type TEXT NOT NULL,
    writer_schema_version INTEGER NOT NULL,
    writer_definition_hash TEXT NOT NULL,
    actor TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    dependencies TEXT NOT NULL,
    bytes BLOB NOT NULL,
    applied INTEGER NOT NULL,
    peer_id TEXT,
    accepted_at TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL,
    UNIQUE(document_id, actor, sequence)
  )`
  yield* sql`CREATE INDEX effect_local_changes_document_sequence
    ON effect_local_changes(document_id, commit_sequence)`
  yield* sql`CREATE TABLE effect_local_checkpoints (
    checkpoint_hash TEXT PRIMARY KEY,
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    heads TEXT NOT NULL,
    bytes BLOB NOT NULL,
    checksum TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL,
    verified INTEGER NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_command_receipts (
    replica_incarnation INTEGER NOT NULL,
    command_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    mutation_name TEXT NOT NULL,
    result BLOB NOT NULL,
    document_id TEXT NOT NULL,
    heads TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL,
    PRIMARY KEY(replica_incarnation, command_id)
  )`
  yield* sql`CREATE TABLE effect_local_projection_registry (
    projection_name TEXT PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE,
    projection_version INTEGER NOT NULL,
    schema_checksum TEXT NOT NULL,
    status TEXT NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_document_projections (
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    projection_name TEXT NOT NULL REFERENCES effect_local_projection_registry(projection_name),
    projected_heads TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(document_id, projection_name)
  )`
  yield* sql`CREATE TABLE effect_local_commit_outbox (
    commit_sequence INTEGER PRIMARY KEY,
    document_id TEXT NOT NULL,
    invalidation_keys TEXT NOT NULL,
    published INTEGER NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT,
    peer_id TEXT,
    reason TEXT NOT NULL,
    bytes BLOB,
    created_at TEXT NOT NULL
  )`
  yield* sql`CREATE TABLE effect_local_backup_installations (
    installation_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    manifest_checksum TEXT NOT NULL,
    installed_at TEXT NOT NULL,
    replica_incarnation INTEGER NOT NULL
  )`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (1, 'canonical_store', ${canonicalStoreChecksum})`
})

const peerSyncMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE TABLE effect_local_peer_receipts (
    replica_incarnation INTEGER NOT NULL,
    peer_id TEXT NOT NULL,
    connection_epoch TEXT NOT NULL,
    receive_sequence INTEGER NOT NULL,
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    message_hash TEXT NOT NULL,
    reply BLOB,
    reply_hash TEXT,
    pending_message BLOB,
    heads TEXT NOT NULL,
    accepted_heads TEXT NOT NULL,
    commit_sequence INTEGER NOT NULL,
    accepted_at TEXT NOT NULL,
    PRIMARY KEY(replica_incarnation, peer_id, connection_epoch, receive_sequence)
  )`
  yield* sql`CREATE INDEX effect_local_peer_receipts_document_sequence
    ON effect_local_peer_receipts(document_id, commit_sequence)`
  yield* sql`CREATE TABLE effect_local_peer_outbox (
    replica_incarnation INTEGER NOT NULL,
    peer_id TEXT NOT NULL,
    connection_epoch TEXT NOT NULL,
    document_id TEXT NOT NULL REFERENCES effect_local_documents(document_id) ON DELETE CASCADE,
    send_sequence INTEGER NOT NULL,
    message BLOB NOT NULL,
    message_hash TEXT NOT NULL,
    heads TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(replica_incarnation, peer_id, connection_epoch, send_sequence)
  )`
  yield* sql`CREATE INDEX effect_local_peer_outbox_connection_status
    ON effect_local_peer_outbox(replica_incarnation, peer_id, connection_epoch, status, send_sequence)`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (2, 'peer_sync', ${peerSyncChecksum})`
})

const durabilityIndexesMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`ALTER TABLE effect_local_peer_outbox ADD COLUMN created_at TEXT NOT NULL DEFAULT ''`
  yield* sql`UPDATE effect_local_peer_outbox
    SET created_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE created_at = ''`
  yield* sql`DELETE FROM effect_local_checkpoints WHERE rowid IN (
    SELECT rowid FROM (
      SELECT rowid, ROW_NUMBER() OVER (
        PARTITION BY document_id
        ORDER BY verified DESC, commit_sequence DESC, checkpoint_hash DESC
      ) AS checkpoint_rank
      FROM effect_local_checkpoints
    ) WHERE checkpoint_rank > 2
  )`
  yield* sql`CREATE INDEX effect_local_checkpoints_document_verified_sequence
    ON effect_local_checkpoints(document_id, verified, commit_sequence DESC, checkpoint_hash DESC)`
  yield* sql`CREATE INDEX effect_local_commit_outbox_published_sequence
    ON effect_local_commit_outbox(published, commit_sequence)`
  yield* sql`CREATE INDEX effect_local_documents_type_projection_status
    ON effect_local_documents(document_type, projection_status)`
  yield* sql`CREATE INDEX effect_local_projection_registry_name_status
    ON effect_local_projection_registry(projection_name, status)`
  yield* sql`CREATE INDEX effect_local_peer_outbox_incarnation_created
    ON effect_local_peer_outbox(replica_incarnation, created_at)`
  yield* sql`CREATE INDEX effect_local_peer_receipts_incarnation_accepted
    ON effect_local_peer_receipts(replica_incarnation, accepted_at)`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (3, 'durability_indexes', ${durabilityIndexesChecksum})`
})

const projectionReadinessMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE INDEX effect_local_document_projections_not_ready
    ON effect_local_document_projections(projection_name)
    WHERE status != 'Ready'`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (4, 'projection_readiness', ${projectionReadinessChecksum})`
})

const pendingReceiptIndexesMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`CREATE INDEX effect_local_peer_receipts_pending_document
    ON effect_local_peer_receipts(replica_incarnation, document_id)
    WHERE pending_message IS NOT NULL`
  yield* sql`CREATE INDEX effect_local_peer_receipts_pending_peer
    ON effect_local_peer_receipts(replica_incarnation, peer_id)
    WHERE pending_message IS NOT NULL`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (5, 'pending_receipt_indexes', ${pendingReceiptIndexesChecksum})`
})

const peerWriterProvenanceMigration = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql`ALTER TABLE effect_local_peer_receipts
    ADD COLUMN writer_provenance TEXT NOT NULL DEFAULT '[]'`
  yield* sql`ALTER TABLE effect_local_peer_outbox
    ADD COLUMN writer_provenance TEXT NOT NULL DEFAULT '[]'`
  yield* sql`ALTER TABLE effect_local_checkpoints
    ADD COLUMN writer_provenance TEXT NOT NULL DEFAULT '[]'`
  yield* sql`UPDATE effect_local_changes
    SET writer_definition_hash = (
      SELECT definition_hash FROM effect_local_metadata WHERE singleton = 1
    )
    WHERE writer_definition_hash = 'local'`
  const defaults = (yield* sql<{
    readonly definition_hash: string
  }>`SELECT definition_hash FROM effect_local_metadata WHERE singleton = 1`)[0]
  const documents = yield* sql<{
    readonly document_id: string
    readonly schema_version: number
  }>`SELECT document_id, schema_version FROM effect_local_documents`
  const schemaVersionByDocument = new Map(
    documents.map((document) => [document.document_id, document.schema_version])
  )
  const checkpoints = yield* sql<{
    readonly bytes: Uint8Array
    readonly checkpoint_hash: string
    readonly document_id: string
    readonly schema_version: number
  }>`SELECT checkpoint.bytes, checkpoint.checkpoint_hash, checkpoint.document_id, document.schema_version
    FROM effect_local_checkpoints AS checkpoint
    INNER JOIN effect_local_documents AS document ON document.document_id = checkpoint.document_id`
  const changes = yield* sql<{
    readonly change_hash: string
    readonly document_id: string
    readonly writer_definition_hash: string
    readonly writer_schema_version: number
  }>`SELECT change_hash, document_id, writer_definition_hash, writer_schema_version
    FROM effect_local_changes`
  const changesByDocument = new Map<string, Array<(typeof changes)[number]>>()
  for (const change of changes) {
    const existing = changesByDocument.get(change.document_id)
    if (existing === undefined) changesByDocument.set(change.document_id, [change])
    else existing.push(change)
  }
  const checkpointProvenanceByDocument = new Map<string, Array<WriterProvenance.ChangeProvenance>>()
  for (const checkpoint of checkpoints) {
    yield* Effect.acquireUseRelease(
      Effect.option(Effect.try({
        try: () => Automerge.load(checkpoint.bytes),
        catch: (cause) => cause
      })),
      (option) =>
        Option.match(option, {
          onNone: () => Effect.void,
          onSome: (document) =>
            Effect.gen(function*() {
              const encoded = yield* Effect.option(Effect.try({
                try: () => {
                  // Legacy pruning discarded provenance for some checkpointed changes. Preserve the
                  // old receiver attribution by using the stored document version and local definition.
                  const writerProvenance = WriterProvenance.backfill(
                    WriterProvenance.changeHashes(document),
                    (changesByDocument.get(checkpoint.document_id) ?? []).map((change) => ({
                      changeHash: change.change_hash,
                      writerSchemaVersion: change.writer_schema_version,
                      writerDefinitionHash: change.writer_definition_hash
                    })),
                    {
                      writerSchemaVersion: checkpoint.schema_version,
                      writerDefinitionHash: defaults!.definition_hash
                    }
                  )
                  return {
                    stored: Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(writerProvenance),
                    writerProvenance
                  }
                },
                catch: (cause) => cause
              }))
              if (Option.isNone(encoded)) return
              yield* sql`UPDATE effect_local_checkpoints
                SET writer_provenance = ${encoded.value.stored}
                WHERE checkpoint_hash = ${checkpoint.checkpoint_hash}`
              const existing = checkpointProvenanceByDocument.get(checkpoint.document_id)
              if (existing === undefined) {
                checkpointProvenanceByDocument.set(checkpoint.document_id, [...encoded.value.writerProvenance])
              } else {
                existing.push(...encoded.value.writerProvenance)
              }
            })
        }),
      (option) =>
        Option.match(option, {
          onNone: () => Effect.void,
          onSome: (document) => Effect.sync(() => Automerge.free(document))
        })
    )
  }
  const storedEntries = (documentId: string): ReadonlyArray<WriterProvenance.ChangeProvenance> => [
    ...(changesByDocument.get(documentId) ?? []).map((change) => ({
      changeHash: change.change_hash,
      writerSchemaVersion: change.writer_schema_version,
      writerDefinitionHash: change.writer_definition_hash
    })),
    ...(checkpointProvenanceByDocument.get(documentId) ?? [])
  ]
  const backfillMessage = (documentId: string, message: Uint8Array) =>
    WriterProvenance.backfill(
      WriterProvenance.syncMessageChangeHashes(message),
      storedEntries(documentId),
      {
        writerSchemaVersion: schemaVersionByDocument.get(documentId)!,
        writerDefinitionHash: defaults!.definition_hash
      }
    )
  const outbox = yield* sql<{
    readonly document_id: string
    readonly message: Uint8Array
    readonly row_id: number
  }>`SELECT rowid AS row_id, document_id, message FROM effect_local_peer_outbox`
  for (const row of outbox) {
    const writerProvenance = yield* Effect.option(Effect.try({
      try: () => backfillMessage(row.document_id, row.message),
      catch: (cause) => cause
    }))
    if (Option.isSome(writerProvenance)) {
      yield* sql`UPDATE effect_local_peer_outbox
        SET writer_provenance = ${Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(writerProvenance.value)}
        WHERE rowid = ${row.row_id}`
    }
  }
  const pendingReceipts = yield* sql<{
    readonly document_id: string
    readonly pending_message: Uint8Array
    readonly row_id: number
  }>`SELECT rowid AS row_id, document_id, pending_message
    FROM effect_local_peer_receipts WHERE pending_message IS NOT NULL`
  for (const row of pendingReceipts) {
    const writerProvenance = yield* Effect.option(Effect.try({
      try: () => backfillMessage(row.document_id, row.pending_message),
      catch: (cause) => cause
    }))
    if (Option.isSome(writerProvenance)) {
      yield* sql`UPDATE effect_local_peer_receipts
        SET writer_provenance = ${Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(writerProvenance.value)}
        WHERE rowid = ${row.row_id}`
    }
  }
  yield* sql`DELETE FROM effect_local_peer_receipts WHERE pending_message IS NULL`
  yield* sql`INSERT INTO effect_local_migration_catalog (migration_id, name, checksum)
    VALUES (6, 'peer_writer_provenance', ${peerWriterProvenanceChecksum})`
})

export const loader = Migrator.fromRecord({
  "1_canonical_store": migration,
  "2_peer_sync": peerSyncMigration,
  "3_durability_indexes": durabilityIndexesMigration,
  "4_projection_readiness": projectionReadinessMigration,
  "5_pending_receipt_indexes": pendingReceiptIndexesMigration,
  "6_peer_writer_provenance": peerWriterProvenanceMigration
})

const migrate = Migrator.make({})({ loader, table: "effect_local_migrations" })

export const run = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const findCatalog = SqlSchema.findAll({
    Request: Schema.Int,
    Result: Schema.Struct({ checksum: Schema.String, name: Schema.String }),
    execute: (migrationId) =>
      sql`SELECT name, checksum FROM effect_local_migration_catalog WHERE migration_id = ${migrationId}`
  })
  const expectedCatalog = [
    { id: 1, name: "canonical_store", checksum: canonicalStoreChecksum, label: "Canonical store" },
    { id: 2, name: "peer_sync", checksum: peerSyncChecksum, label: "Peer sync" },
    { id: 3, name: "durability_indexes", checksum: durabilityIndexesChecksum, label: "Durability indexes" },
    { id: 4, name: "projection_readiness", checksum: projectionReadinessChecksum, label: "Projection readiness" },
    {
      id: 5,
      name: "pending_receipt_indexes",
      checksum: pendingReceiptIndexesChecksum,
      label: "Pending receipt indexes"
    },
    {
      id: 6,
      name: "peer_writer_provenance",
      checksum: peerWriterProvenanceChecksum,
      label: "Peer writer provenance"
    }
  ] as const
  // One transaction over migrate + validation so a rejected catalog rolls back
  // the freshly applied migrations instead of leaving a partial schema.
  return yield* sql.withTransaction(Effect.gen(function*() {
    const applied = yield* migrate
    for (const expected of expectedCatalog) {
      const row = (yield* findCatalog(expected.id))[0]
      if (row?.name !== expected.name || row?.checksum !== expected.checksum) {
        return yield* new Migrator.MigrationError({
          kind: "BadState",
          message: `${expected.label} migration checksum mismatch`
        })
      }
    }
    return applied
  }))
}).pipe(
  Effect.catchTag("SchemaError", (cause) =>
    Effect.fail(
      new Migrator.MigrationError({
        kind: "BadState",
        message: `Invalid migration catalog: ${cause}`
      })
    ))
)

export const layer: Layer.Layer<never, Migrator.MigrationError | SqlError.SqlError, SqlClient.SqlClient> = Layer
  .effectDiscard(run)
