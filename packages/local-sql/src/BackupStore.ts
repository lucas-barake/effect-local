import * as Automerge from "@automerge/automerge"
import * as Backup from "@lucas-barake/effect-local/Backup"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Encoding from "effect/Encoding"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as CheckpointAuthority from "./CheckpointAuthority.js"
import * as InternalAutomerge from "./internal/automerge.js"
import * as ClusterStorage from "./internal/clusterStorage.js"
import * as HistoryCounters from "./internal/historyCounters.js"
import * as WriterProvenance from "./internal/writerProvenance.js"
import * as ProjectionStore from "./ProjectionStore.js"
import * as Recovery from "./Recovery.js"
import * as ReplicaBootstrap from "./ReplicaBootstrap.js"
import * as ReplicaGate from "./ReplicaGate.js"
import * as ReplicaHealth from "./ReplicaHealth.js"

const Manifest = Schema.Struct({
  formatVersion: Backup.FormatVersion,
  definitionHash: Schema.String,
  replicaId: Identity.ReplicaId,
  incarnation: Identity.ReplicaIncarnation,
  createdAt: Schema.String,
  recordCount: Schema.Int,
  declaredBytes: Schema.Int
})

const DocumentRecord = Schema.Struct({
  document_id: Identity.DocumentId,
  document_type: Schema.String,
  schema_version: Schema.Int,
  observed_versions: Schema.String,
  materialized_heads: Schema.String,
  accepted_heads: Schema.String,
  tombstone: Schema.Int,
  projection_status: Schema.String,
  checkpoint_hash: Schema.NullOr(Schema.String),
  lineage: Identity.DocumentLineage,
  history_changes: Schema.optionalKey(Schema.Unknown),
  history_operations: Schema.optionalKey(Schema.Unknown),
  history_bytes: Schema.optionalKey(Schema.Unknown)
})

const ChangeRecord = Schema.Struct({
  change_hash: Schema.String,
  document_id: Schema.String,
  document_type: Schema.String,
  writer_schema_version: Schema.Int,
  writer_definition_hash: Schema.String,
  actor: Schema.String,
  sequence: Schema.Int,
  dependencies: Schema.String,
  bytes: Schema.String,
  applied: Schema.Int,
  peer_id: Schema.NullOr(Schema.String),
  accepted_at: Schema.String,
  commit_sequence: Schema.Int
})

const BoundedCompactCheckpointProvenance = Schema.Struct({
  ...WriterProvenance.CompactCheckpointProvenance.fields,
  authorization: CheckpointAuthority.AuthorizationToken
})
const CheckpointProvenance = Schema.Union([
  WriterProvenance.ChangeProvenances,
  BoundedCompactCheckpointProvenance
])
const StoredCheckpointProvenance = Schema.fromJsonString(Schema.toCodecJson(CheckpointProvenance))
const EncodedCompactCheckpointProvenance = Schema.Struct({
  ...WriterProvenance.CompactCheckpointProvenance.fields,
  heads: Schema.Array(Schema.String),
  base: Schema.Union([
    Schema.TaggedStruct("Bootstrap", {}),
    Schema.TaggedStruct("Heads", { baseHeads: Schema.Array(Schema.String) })
  ]),
  authorization: Schema.String
})
const EncodedCheckpointProvenance = Schema.Union([
  WriterProvenance.ChangeProvenances,
  EncodedCompactCheckpointProvenance
])
const isEncodedCompactCheckpoint = (
  provenance: typeof EncodedCheckpointProvenance.Type
): provenance is typeof EncodedCompactCheckpointProvenance.Type => !Array.isArray(provenance)

const CheckpointRecord = Schema.Struct({
  checkpoint_hash: Schema.String,
  document_id: Identity.DocumentId,
  heads: Schema.String,
  bytes: Schema.String,
  checksum: Schema.String,
  commit_sequence: Schema.Int,
  verified: Schema.Int,
  writer_provenance: Schema.optionalKey(EncodedCheckpointProvenance),
  lineage: Identity.DocumentLineage
})

const TransitionRecord = Schema.Struct({
  authorization: Schema.NullOr(Schema.String),
  checkpoint_hash: WriterProvenance.ChangeHash,
  created_at: Schema.String,
  document_id: Identity.DocumentId,
  heads: Schema.String,
  lineage: Identity.DocumentLineage,
  prior_checkpoint_hash: WriterProvenance.ChangeHash,
  prior_heads: Schema.String,
  prior_lineage: Identity.DocumentLineage,
  prior_snapshot: Schema.String,
  schema_version: WriterProvenance.WriterSchemaVersion,
  writer_definition_hash: WriterProvenance.WriterDefinitionHash
})

const ReceiptRecord = Schema.Struct({
  replica_incarnation: Identity.ReplicaIncarnation,
  command_id: Schema.String,
  request_hash: Schema.String,
  mutation_name: Schema.String,
  result: Schema.String,
  document_id: Schema.String,
  heads: Schema.String,
  commit_sequence: Schema.Int
})

const StoredChangeRecord = Schema.Struct({ ...ChangeRecord.fields, bytes: Schema.Uint8Array })
const StoredCheckpointRecord = Schema.Struct({
  ...CheckpointRecord.fields,
  bytes: Schema.Uint8Array,
  writer_provenance: StoredCheckpointProvenance,
  lineage: Identity.DocumentLineage
})
const DecodedCheckpointRecord = Schema.Struct({
  ...CheckpointRecord.fields,
  bytes: Schema.Uint8Array,
  writer_provenance: CheckpointProvenance,
  lineage: Identity.DocumentLineage
})
const StoredTransitionRecord = Schema.Struct({
  ...TransitionRecord.fields,
  authorization: Schema.NullOr(CheckpointAuthority.AuthorizationToken),
  prior_snapshot: Schema.Uint8Array
})
const StoredReceiptRecord = Schema.Struct({ ...ReceiptRecord.fields, result: Schema.Uint8Array })

const EndRecord = Schema.Struct({ recordCount: Schema.Int, recordsChecksum: Schema.String })
const Envelope = Schema.Struct({ kind: Schema.String, checksum: Schema.String, value: Schema.Unknown })
const Heads = Schema.fromJsonString(Conflict.Heads)
const ObservedVersions = Schema.fromJsonString(Schema.Array(Schema.Int))
const BackupSizingRow = Schema.Struct({ raw_bytes: Schema.Int, record_count: Schema.Int })
const SqliteTableRow = Schema.Struct({ name: Schema.String })
const ForeignKeyViolationRow = Schema.Struct({
  table: Schema.String,
  rowid: Schema.NullOr(Schema.Int),
  parent: Schema.String,
  fkid: Schema.Int
})
const JsonString = Schema.fromJsonString(Schema.Unknown)
const EnvelopeJson = Schema.fromJsonString(Envelope)
const backupAlreadyInstalled = { _tag: "BackupAlreadyInstalled" } as const

type Envelope = typeof Envelope.Type
type RawDecodedRecord =
  | { readonly kind: "Document"; readonly value: typeof DocumentRecord.Type }
  | { readonly kind: "Change"; readonly value: typeof StoredChangeRecord.Type }
  | {
    readonly kind: "Checkpoint"
    readonly value: Omit<typeof DecodedCheckpointRecord.Type, "writer_provenance"> & {
      readonly writer_provenance?: typeof CheckpointProvenance.Type
    }
  }
  | { readonly kind: "Transition"; readonly value: typeof StoredTransitionRecord.Type }
  | { readonly kind: "Receipt"; readonly value: typeof StoredReceiptRecord.Type }

type DecodedRecord =
  | Exclude<RawDecodedRecord, { readonly kind: "Checkpoint" }>
  | { readonly kind: "Checkpoint"; readonly value: typeof DecodedCheckpointRecord.Type }

const encodeEnvelopeJson = (envelope: Envelope) =>
  Schema.encodeEffect(EnvelopeJson)(envelope).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.BackupInvalid({
          cause
        })
      })
    )
  )

const decodeBytes = (encoded: string) =>
  Effect.fromResult(Encoding.decodeBase64(encoded)).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: new ReplicaError.BackupInvalid({
          cause
        })
      })
    )
  )

const exceedsJsonDepth = (value: unknown, limit: number) => {
  const pending: Array<readonly [unknown, number]> = [[value, 1]]
  while (pending.length > 0) {
    const [current, depth] = pending.pop()!
    if (depth > limit) return true
    if (Array.isArray(current)) {
      for (const child of current) pending.push([child, depth + 1])
    } else if (current !== null && typeof current === "object") {
      for (const child of Object.values(current)) pending.push([child, depth + 1])
    }
  }
  return false
}

export class BackupStore extends Context.Service<BackupStore, {
  readonly export: (options: Backup.ExportOptions) => Stream.Stream<Uint8Array, ReplicaError.ReplicaError>
  readonly restore: <R,>(options: Backup.RestoreOptions<R>) => Effect.Effect<void, ReplicaError.ReplicaError, R>
  readonly installDocument: <D extends Document.Any, R,>(
    document: D,
    options: Backup.InstallDocumentOptions<R>
  ) => Effect.Effect<void, ReplicaError.ReplicaError, R>
}>()("@lucas-barake/effect-local-sql/BackupStore") {}

export const layer = (definition: ReplicaDefinition.Any): Layer.Layer<
  BackupStore,
  never,
  | ProjectionStore.ProjectionStore
  | ReplicaBootstrap.ReplicaBootstrap
  | ReplicaGate.ReplicaGate
  | ReplicaHealth.ReplicaHealth
  | ReplicaLimits.ReplicaLimits
  | CheckpointAuthority.CheckpointAuthority
  | Crypto.Crypto
  | SqlClient.SqlClient
> =>
  Layer.effect(
    BackupStore,
    Effect.gen(function*() {
      yield* ReplicaBootstrap.ReplicaBootstrap
      const gate = yield* ReplicaGate.ReplicaGate
      const health = yield* ReplicaHealth.ReplicaHealth
      const limits = yield* ReplicaLimits.ReplicaLimits
      const projections = yield* ProjectionStore.ProjectionStore
      const sql = yield* SqlClient.SqlClient
      const crypto = yield* Crypto.Crypto
      const checkpointAuthority = yield* CheckpointAuthority.CheckpointAuthority
      const findDocuments = SqlSchema.findAll({
        Request: Schema.Void,
        Result: DocumentRecord,
        execute: () => sql`SELECT * FROM effect_local_documents ORDER BY document_id`
      })
      const findChanges = SqlSchema.findAll({
        Request: Schema.Void,
        Result: StoredChangeRecord,
        execute: () => sql`SELECT * FROM effect_local_changes ORDER BY document_id, commit_sequence, sequence`
      })
      const findCheckpoints = SqlSchema.findAll({
        Request: Schema.Void,
        Result: StoredCheckpointRecord,
        execute: () => sql`SELECT * FROM effect_local_checkpoints ORDER BY document_id, commit_sequence`
      })
      const findTransitions = SqlSchema.findAll({
        Request: Schema.Void,
        Result: StoredTransitionRecord,
        execute: () => sql`SELECT * FROM effect_local_lineage_transitions ORDER BY document_id, created_at, lineage`
      })
      const findReceipts = SqlSchema.findAll({
        Request: Schema.Void,
        Result: StoredReceiptRecord,
        execute: () => sql`SELECT * FROM effect_local_command_receipts ORDER BY replica_incarnation, command_id`
      })
      const findBackupSizing = SqlSchema.findOneOption({
        Request: Schema.Void,
        Result: BackupSizingRow,
        execute: () =>
          sql`SELECT
            (SELECT COUNT(*) FROM effect_local_documents) +
            (SELECT COUNT(*) FROM effect_local_changes) +
            (SELECT COUNT(*) FROM effect_local_checkpoints) +
            (SELECT COUNT(*) FROM effect_local_lineage_transitions) +
            (SELECT COUNT(*) FROM effect_local_command_receipts) AS record_count,
            (SELECT COALESCE(SUM(
              length(document_id) + length(document_type) + length(observed_versions) +
              length(materialized_heads) + length(accepted_heads) + length(projection_status) +
              length(COALESCE(checkpoint_hash, '')) + length(lineage) +
              length(COALESCE(CAST(history_changes AS TEXT), '')) +
              length(COALESCE(CAST(history_operations AS TEXT), '')) +
              length(COALESCE(CAST(history_bytes AS TEXT), ''))
            ), 0) FROM effect_local_documents) +
            (SELECT COALESCE(SUM(
              length(change_hash) + length(document_id) + length(document_type) +
              length(writer_definition_hash) + length(actor) + length(dependencies) + length(bytes) +
              length(COALESCE(peer_id, '')) + length(accepted_at)
            ), 0) FROM effect_local_changes) +
            (SELECT COALESCE(SUM(
              length(checkpoint_hash) + length(document_id) + length(heads) + length(bytes) + length(checksum) +
              length(writer_provenance) + length(lineage)
            ), 0) FROM effect_local_checkpoints) +
            (SELECT COALESCE(SUM(
              length(document_id) + length(prior_lineage) + length(prior_checkpoint_hash) +
              length(prior_heads) + length(prior_snapshot) + length(lineage) + length(checkpoint_hash) +
              length(heads) + length(writer_definition_hash) + length(COALESCE(authorization, '')) +
              length(created_at)
            ), 0) FROM effect_local_lineage_transitions) +
            (SELECT COALESCE(SUM(
              length(command_id) + length(request_hash) + length(mutation_name) + length(result) +
              length(document_id) + length(heads)
            ), 0) FROM effect_local_command_receipts) AS raw_bytes`
      })
      const findClusterTables = SqlSchema.findAll({
        Request: Schema.Void,
        Result: SqliteTableRow,
        execute: () =>
          sql`SELECT name FROM sqlite_master
            WHERE type = 'table' AND name IN (
              ${`${ClusterStorage.messagePrefix}_messages`},
              ${`${ClusterStorage.messagePrefix}_replies`}
            )`
      })
      const findForeignKeyViolations = SqlSchema.findAll({
        Request: Schema.Void,
        Result: ForeignKeyViolationRow,
        execute: () => sql`PRAGMA foreign_key_check`
      })
      const findInstallation = SqlSchema.findOneOption({
        Request: Identity.BackupInstallationId,
        Result: Schema.Struct({
          document_id: Schema.NullOr(Identity.DocumentId),
          manifest_checksum: Schema.String,
          mode: Schema.Literals(["clone", "replace", "document"])
        }),
        execute: (installationId) =>
          sql`SELECT mode, manifest_checksum, document_id FROM effect_local_backup_installations
            WHERE installation_id = ${installationId}`
      })
      const findDocumentExists = SqlSchema.findOne({
        Request: Identity.DocumentId,
        Result: Schema.Struct({ present: Schema.Int }),
        execute: (documentId) =>
          sql`SELECT EXISTS (
            SELECT 1 FROM effect_local_documents WHERE document_id = ${documentId}
          ) AS present`
      })
      const incrementCommitSequence = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Schema.Struct({
          commit_sequence: Identity.CommitSequence,
          prior_sequence: Identity.CommitSequence
        }),
        execute: () =>
          sql`UPDATE effect_local_metadata SET commit_sequence = commit_sequence + 1
            WHERE singleton = 1
            RETURNING commit_sequence, commit_sequence - 1 AS prior_sequence`
      })
      const recovery = yield* Recovery.make
      const digest = (value: unknown) => Canonical.digest(value).pipe(Effect.provideService(Crypto.Crypto, crypto))
      const encodeEnvelope = (kind: string, value: unknown) =>
        digest(value).pipe(Effect.map((checksum) => ({ kind, checksum, value } satisfies Envelope)))

      const exportBackup = (options: Backup.ExportOptions) =>
        Stream.unwrap(
          Effect.scoped(Effect.gen(function*() {
            const maxBytes = yield* Backup.validateMaxBytes(options.maxBytes)
            if (maxBytes > limits.maxBackupBytes) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupTooLarge({
                  limit: limits.maxBackupBytes,
                  observed: maxBytes
                })
              })
            }
            const identity = yield* gate.admit
            const snapshot = yield* sql.withTransaction(Effect.gen(function*() {
              const sizing = yield* findBackupSizing(undefined)
              const { raw_bytes: rawBytes, record_count: recordCount } = Option.getOrElse(
                sizing,
                () => ({ raw_bytes: 0, record_count: 0 })
              )
              if (recordCount > limits.maxArchiveRecords) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.BackupTooLarge({
                    limit: limits.maxArchiveRecords,
                    observed: recordCount
                  })
                })
              }
              const estimatedBytes = rawBytes * 2 + recordCount * 512 + 4096
              if (estimatedBytes > maxBytes) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.BackupTooLarge({
                    limit: maxBytes,
                    observed: estimatedBytes
                  })
                })
              }
              const documents = yield* findDocuments(undefined)
              const changes = yield* findChanges(undefined)
              const checkpoints = yield* findCheckpoints(undefined)
              const transitions = yield* findTransitions(undefined)
              const receipts = yield* findReceipts(undefined)
              yield* gate.validate(identity)
              return { documents, changes, checkpoints, transitions, receipts }
            }))
            const records = yield* Effect.forEach([
              ...snapshot.documents.map((value) => ["Document", value] as const),
              ...snapshot.changes.map((row) =>
                ["Change", { ...row, bytes: Encoding.encodeBase64(row.bytes) }] as const
              ),
              ...snapshot.checkpoints.map((row) =>
                ["Checkpoint", {
                  ...row,
                  bytes: Encoding.encodeBase64(row.bytes),
                  writer_provenance: WriterProvenance.isCompactCheckpoint(row.writer_provenance)
                    ? {
                      ...row.writer_provenance,
                      authorization: Encoding.encodeBase64(row.writer_provenance.authorization)
                    }
                    : row.writer_provenance
                }] as const
              ),
              ...snapshot.transitions.map((row) =>
                ["Transition", {
                  ...row,
                  authorization: row.authorization === null ? null : Encoding.encodeBase64(row.authorization),
                  prior_snapshot: Encoding.encodeBase64(row.prior_snapshot)
                }] as const
              ),
              ...snapshot.receipts.map((row) =>
                ["Receipt", { ...row, result: Encoding.encodeBase64(row.result) }] as const
              )
            ], ([kind, value]) => encodeEnvelope(kind, value))
            const recordsChecksum = yield* digest(records.map((record) => record.checksum))
            const end = yield* encodeEnvelope("End", { recordCount: records.length, recordsChecksum })
            const recordLines = yield* Effect.forEach(records, encodeEnvelopeJson)
            const endLine = yield* encodeEnvelopeJson(end)
            const createdAt = DateTime.formatIso(yield* DateTime.now)
            const encoder = new TextEncoder()
            const recordBytes = recordLines.map((line) => encoder.encode(`${line}\n`))
            const endBytes = encoder.encode(`${endLine}\n`)
            const trailerBytes = recordBytes.reduce((total, bytes) => total + bytes.byteLength, 0) +
              endBytes.byteLength
            let declaredBytes = 0
            let manifestBytes = new Uint8Array()
            for (let attempt = 0; attempt < 8; attempt++) {
              const manifest = yield* encodeEnvelope("Manifest", {
                formatVersion: 1,
                definitionHash: definition.hash,
                replicaId: identity.replicaId,
                incarnation: identity.incarnation,
                createdAt,
                recordCount: records.length,
                declaredBytes
              })
              const manifestLine = yield* encodeEnvelopeJson(manifest)
              manifestBytes = encoder.encode(`${manifestLine}\n`)
              const next = manifestBytes.byteLength + trailerBytes
              if (next === declaredBytes) break
              declaredBytes = next
            }
            if (declaredBytes > maxBytes) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupTooLarge({
                  limit: maxBytes,
                  observed: declaredBytes
                })
              })
            }
            const chunks: Array<Uint8Array<ArrayBuffer>> = []
            for (const bytes of [manifestBytes, ...recordBytes, endBytes]) {
              for (let offset = 0; offset < bytes.byteLength; offset += limits.maxChunkBytes) {
                chunks.push(bytes.slice(offset, offset + limits.maxChunkBytes))
              }
            }
            return Stream.fromIterable(chunks)
          })).pipe(
            Effect.catchTags({
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({
                      cause
                    })
                  })
                ),
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({
                      cause
                    })
                  })
                )
            })
          )
        )

      type ArchiveOperation<R,> =
        | Backup.RestoreOptions<R>
        | (
          Backup.InstallDocumentOptions<R> & { readonly mode: "document"; readonly document: Document.Any }
        )

      const processArchive = <R,>(
        options: ArchiveOperation<R>
      ): Effect.Effect<void, ReplicaError.ReplicaError, R> =>
        Effect.gen(function*() {
          const reporter = yield* health.restoring
          const maxBytes = yield* Backup.validateMaxBytes(options.maxBytes)
          if (maxBytes > limits.maxBackupBytes) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.BackupTooLarge({
                limit: limits.maxBackupBytes,
                observed: maxBytes
              })
            })
          }
          const observedBytes = yield* Ref.make(0)
          const envelopes = yield* options.source.pipe(
            Stream.mapEffect((chunk) =>
              Ref.updateAndGet(observedBytes, (bytes) => bytes + chunk.byteLength).pipe(
                Effect.flatMap((bytes) => {
                  if (chunk.byteLength > limits.maxChunkBytes) {
                    return Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.BackupTooLarge({
                          limit: limits.maxChunkBytes,
                          observed: chunk.byteLength
                        })
                      })
                    )
                  }
                  return bytes > maxBytes
                    ? Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.BackupTooLarge({
                          limit: maxBytes,
                          observed: bytes
                        })
                      })
                    )
                    : reporter.progress(bytes).pipe(Effect.as(chunk))
                })
              )
            ),
            Stream.decodeText(),
            Stream.splitLines,
            Stream.mapEffect((line, index) =>
              index >= limits.maxArchiveRecords + 2
                ? Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupTooLarge({
                      limit: limits.maxArchiveRecords,
                      observed: index - 1
                    })
                  })
                )
                : Schema.decodeUnknownEffect(JsonString)(line).pipe(
                  Effect.filterOrFail(
                    (value) => !exceedsJsonDepth(value, limits.maxJsonDepth),
                    () =>
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.BackupInvalid({
                          cause: new Error(`Backup JSON exceeds maximum depth ${limits.maxJsonDepth}`)
                        })
                      })
                  ),
                  Effect.flatMap(Schema.decodeUnknownEffect(Envelope)),
                  Effect.catchTag("SchemaError", (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.BackupInvalid({
                          cause
                        })
                      })
                    ))
                )
            ),
            Stream.runCollect
          )
          const observedByteCount = yield* Ref.get(observedBytes)
          const manifestEnvelope = envelopes[0]
          const endEnvelope = envelopes.at(-1)
          if (manifestEnvelope?.kind !== "Manifest" || endEnvelope?.kind !== "End") {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.BackupInvalid({
                cause: new Error("Backup manifest or ending record is missing")
              })
            })
          }
          const manifest = yield* Schema.decodeUnknownEffect(Manifest)(manifestEnvelope.value).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause
                })
              })
            )
          )
          const end = yield* Schema.decodeUnknownEffect(EndRecord)(endEnvelope.value).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause
                })
              })
            )
          )
          if (
            manifest.definitionHash !== options.expectedDefinitionHash ||
            manifest.definitionHash !== definition.hash ||
            manifest.declaredBytes !== observedByteCount
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.BackupInvalid({
                cause: new Error("Backup manifest does not match this replica")
              })
            })
          }
          const records = envelopes.slice(1, -1)
          if (manifest.recordCount !== records.length || end.recordCount !== records.length) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.BackupInvalid({
                cause: new Error("Backup record count mismatch")
              })
            })
          }
          for (const envelope of envelopes) {
            if ((yield* digest(envelope.value)) !== envelope.checksum) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Backup checksum mismatch: ${envelope.kind}`)
                })
              })
            }
          }
          if ((yield* digest(records.map((record) => record.checksum))) !== end.recordsChecksum) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.BackupInvalid({
                cause: new Error("Backup records checksum mismatch")
              })
            })
          }
          const archiveChecksum = yield* digest(envelopes.map((envelope) => envelope.checksum))
          const rawDecoded = yield* Effect.gen(function*() {
            const decoded: Array<RawDecodedRecord> = []
            for (const record of records) {
              switch (record.kind) {
                case "Document": {
                  const value = yield* Schema.decodeUnknownEffect(DocumentRecord)(record.value)
                  decoded.push({ kind: "Document", value })
                  break
                }
                case "Change": {
                  const encoded = yield* Schema.decodeUnknownEffect(ChangeRecord)(record.value)
                  const bytes = yield* decodeBytes(encoded.bytes)
                  decoded.push({
                    kind: "Change",
                    value: {
                      ...encoded,
                      bytes,
                      writer_definition_hash: encoded.writer_definition_hash === "local"
                        ? manifest.definitionHash
                        : encoded.writer_definition_hash
                    }
                  })
                  break
                }
                case "Checkpoint": {
                  const encoded = yield* Schema.decodeUnknownEffect(CheckpointRecord)(record.value)
                  const bytes = yield* decodeBytes(encoded.bytes)
                  let writerProvenance: typeof CheckpointProvenance.Type | undefined
                  if (encoded.writer_provenance !== undefined) {
                    if (isEncodedCompactCheckpoint(encoded.writer_provenance)) {
                      const authorization = yield* decodeBytes(encoded.writer_provenance.authorization)
                      writerProvenance = yield* Schema.decodeUnknownEffect(BoundedCompactCheckpointProvenance)({
                        ...encoded.writer_provenance,
                        authorization
                      })
                      const canonicalHeads = Conflict.normalizeHeads(encoded.writer_provenance.heads)
                      const canonicalBaseHeads = encoded.writer_provenance.base._tag === "Heads"
                        ? Conflict.normalizeHeads(encoded.writer_provenance.base.baseHeads)
                        : undefined
                      if (
                        encoded.writer_provenance.heads.length !== canonicalHeads.length ||
                        encoded.writer_provenance.heads.some((head, index) => head !== canonicalHeads[index]) ||
                        (
                          encoded.writer_provenance.base._tag === "Heads" &&
                          (
                            encoded.writer_provenance.base.baseHeads.length !== canonicalBaseHeads!.length ||
                            encoded.writer_provenance.base.baseHeads.some(
                              (head, index) => head !== canonicalBaseHeads![index]
                            )
                          )
                        )
                      ) {
                        return yield* new ReplicaError.ReplicaError({
                          reason: new ReplicaError.BackupInvalid({
                            cause: new Error(`Compact checkpoint heads are not canonical: ${encoded.checkpoint_hash}`)
                          })
                        })
                      }
                    } else {
                      writerProvenance = encoded.writer_provenance
                    }
                  }
                  const { writer_provenance: _writerProvenance, ...checkpoint } = encoded
                  decoded.push({
                    kind: "Checkpoint",
                    value: writerProvenance === undefined
                      ? { ...checkpoint, bytes }
                      : { ...checkpoint, bytes, writer_provenance: writerProvenance }
                  })
                  break
                }
                case "Transition": {
                  const encoded = yield* Schema.decodeUnknownEffect(TransitionRecord)(record.value)
                  const priorSnapshot = yield* decodeBytes(encoded.prior_snapshot)
                  const authorization = encoded.authorization === null
                    ? null
                    : yield* decodeBytes(encoded.authorization).pipe(
                      Effect.flatMap(Schema.decodeUnknownEffect(CheckpointAuthority.AuthorizationToken))
                    )
                  decoded.push({
                    kind: "Transition",
                    value: { ...encoded, authorization, prior_snapshot: priorSnapshot }
                  })
                  break
                }
                case "Receipt": {
                  const encoded = yield* Schema.decodeUnknownEffect(ReceiptRecord)(record.value)
                  const result = yield* decodeBytes(encoded.result)
                  decoded.push({ kind: "Receipt", value: { ...encoded, result } })
                  break
                }
                default:
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error(`Unknown backup record: ${record.kind}`)
                    })
                  })
              }
            }
            return decoded
          }).pipe(
            Effect.catchTag("SchemaError", (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.BackupInvalid({
                    cause
                  })
                })
              ))
          )
          const documentById = new Map<string, typeof DocumentRecord.Type>(
            rawDecoded.flatMap((record) =>
              record.kind === "Document" ? [[record.value.document_id, record.value] as const] : []
            )
          )
          const documentIds = new Set<string>()
          const changeHashes = new Set<string>()
          const changeActors = new Set<string>()
          const checkpointHashes = new Set<string>()
          const transitionLineages = new Set<string>()
          const transitionPriorLineages = new Set<string>()
          const receiptIds = new Set<string>()
          for (const record of rawDecoded) {
            let key: string
            let keys: ReadonlyArray<Set<string>>
            switch (record.kind) {
              case "Document":
                key = record.value.document_id
                keys = [documentIds]
                break
              case "Change":
                key = record.value.change_hash
                keys = [changeHashes]
                if (changeActors.has(`${record.value.document_id}:${record.value.actor}:${record.value.sequence}`)) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error(`Duplicate archive change sequence: ${record.value.document_id}`)
                    })
                  })
                }
                changeActors.add(`${record.value.document_id}:${record.value.actor}:${record.value.sequence}`)
                break
              case "Checkpoint":
                key = record.value.checkpoint_hash
                keys = [checkpointHashes]
                break
              case "Transition": {
                key = `${record.value.document_id}:${record.value.lineage}`
                keys = [transitionLineages]
                const priorKey = `${record.value.document_id}:${record.value.prior_lineage}`
                if (transitionPriorLineages.has(priorKey)) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error(`Conflicting archive transition fork: ${priorKey}`)
                    })
                  })
                }
                transitionPriorLineages.add(priorKey)
                break
              }
              case "Receipt":
                key = `${record.value.replica_incarnation}:${record.value.command_id}`
                keys = [receiptIds]
                break
            }
            if (keys.some((entries) => entries.has(key))) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Duplicate archive ${record.kind.toLowerCase()} record: ${key}`)
                })
              })
            }
            for (const entries of keys) entries.add(key)
          }
          for (const record of rawDecoded) {
            if (record.kind !== "Document" && !documentIds.has(record.value.document_id)) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Archive ${record.kind.toLowerCase()} references an unknown document`)
                })
              })
            }
          }
          const changeProvenanceByDocument = new Map<string, Array<WriterProvenance.ChangeProvenance>>()
          for (const record of rawDecoded) {
            if (record.kind !== "Change") continue
            const entry = {
              changeHash: record.value.change_hash,
              writerSchemaVersion: record.value.writer_schema_version,
              writerDefinitionHash: record.value.writer_definition_hash
            }
            const existing = changeProvenanceByDocument.get(record.value.document_id)
            if (existing === undefined) changeProvenanceByDocument.set(record.value.document_id, [entry])
            else existing.push(entry)
          }
          const checkpointHistoryByHash = new Map<string, HistoryCounters.HistoryCounters>()
          const decoded = yield* Effect.forEach(rawDecoded, (record): Effect.Effect<
            DecodedRecord,
            ReplicaError.ReplicaError
          > => {
            if (record.kind !== "Checkpoint") return Effect.succeed(record)
            return Effect.gen(function*() {
              const decodedCheckpoint = yield* Effect.acquireUseRelease(
                Effect.try({
                  try: () => Automerge.load<InternalAutomerge.Root<unknown>>(record.value.bytes),
                  catch: (cause) =>
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.BackupInvalid({ cause })
                    })
                }),
                (document) =>
                  Effect.try({
                    try: () => {
                      const checkpointChanges = Automerge.getAllChanges(document).map((bytes) => ({
                        bytes,
                        decoded: Automerge.decodeChange(bytes)
                      }))
                      const declaredHeads = Schema.decodeUnknownSync(Heads)(record.value.heads)
                        .toSorted(Conflict.compareCodeUnits)
                      const actualHeads = Automerge.getHeads(document).toSorted(Conflict.compareCodeUnits)
                      if (
                        declaredHeads.length !== actualHeads.length ||
                        declaredHeads.some((head, index) => head !== actualHeads[index])
                      ) throw new TypeError(`Checkpoint heads mismatch: ${record.value.checkpoint_hash}`)
                      const changeHashes = checkpointChanges.map((change) => change.decoded.hash).toSorted()
                      const stored = changeProvenanceByDocument.get(record.value.document_id) ?? []
                      const storedDocument = documentById.get(record.value.document_id)
                      if (storedDocument === undefined) {
                        throw new TypeError(`Checkpoint references unknown document ${record.value.document_id}`)
                      }
                      const provenance = record.value.writer_provenance ??
                        WriterProvenance.backfill(
                          changeHashes,
                          stored,
                          {
                            writerSchemaVersion: storedDocument.schema_version,
                            writerDefinitionHash: manifest.definitionHash
                          }
                        )
                      if (WriterProvenance.isCompactCheckpoint(provenance)) {
                        if (
                          provenance.checkpointHash !== record.value.checkpoint_hash ||
                          provenance.lineage !== record.value.lineage ||
                          provenance.schemaVersion !== storedDocument.schema_version ||
                          provenance.heads.length !== actualHeads.length ||
                          provenance.heads.some((head, index) => head !== actualHeads[index]) ||
                          Automerge.getMissingDeps(document, []).length !== 0
                        ) throw new TypeError(`Invalid compact checkpoint proof: ${record.value.checkpoint_hash}`)
                      } else {
                        WriterProvenance.validateExact(changeHashes, provenance)
                        WriterProvenance.resolve(changeHashes, [...provenance, ...stored])
                      }
                      checkpointHistoryByHash.set(record.value.checkpoint_hash, {
                        changes: checkpointChanges.length,
                        operations: checkpointChanges.reduce(
                          (total, change) => total + change.decoded.ops.length,
                          0
                        ),
                        bytes: checkpointChanges.reduce((total, change) => total + change.bytes.byteLength, 0)
                      })
                      return {
                        kind: "Checkpoint" as const,
                        value: { ...record.value, writer_provenance: provenance }
                      }
                    },
                    catch: (cause) =>
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.BackupInvalid({ cause })
                      })
                  }),
                (document) => Effect.sync(() => InternalAutomerge.free(document))
              )
              if (WriterProvenance.isCompactCheckpoint(decodedCheckpoint.value.writer_provenance)) {
                const provenance = decodedCheckpoint.value.writer_provenance
                yield* checkpointAuthority.verifyManifest(
                  CheckpointAuthority.ManifestClaims.make({
                    purpose: CheckpointAuthority.manifestPurpose,
                    documentId: decodedCheckpoint.value.document_id,
                    lineage: provenance.lineage,
                    checkpointHash: provenance.checkpointHash,
                    heads: provenance.heads,
                    base: provenance.base,
                    schemaVersion: provenance.schemaVersion,
                    writerDefinitionHash: provenance.writerDefinitionHash
                  }),
                  provenance.authorization
                )
              }
              return decodedCheckpoint
            })
          })
          const checkpointByHash = new Map(
            decoded.flatMap((record) =>
              record.kind === "Checkpoint" ? [[record.value.checkpoint_hash, record.value] as const] : []
            )
          )
          const transitionsByDocument = new Map<string, Array<typeof StoredTransitionRecord.Type>>()
          for (const document of documentById.values()) {
            if (document.checkpoint_hash === null) continue
            const checkpoint = checkpointByHash.get(document.checkpoint_hash)
            if (
              checkpoint === undefined ||
              checkpoint.document_id !== document.document_id ||
              checkpoint.lineage !== document.lineage
            ) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Invalid active checkpoint for ${document.document_id}`)
                })
              })
            }
          }
          const metadataByChange = new Map<string, {
            readonly dependencies: ReadonlyArray<string>
            readonly operations: number
          }>()
          for (const record of decoded) {
            switch (record.kind) {
              case "Document": {
                const document = definition.documents.byName.get(record.value.document_type)
                if (
                  document === undefined ||
                  !Document.supportsStoredVersion(document, record.value.schema_version) ||
                  (record.value.tombstone !== 0 && record.value.tombstone !== 1)
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error(`Invalid document record: ${record.value.document_id}`)
                    })
                  })
                }
                yield* Effect.all([
                  Schema.decodeUnknownEffect(ObservedVersions)(record.value.observed_versions),
                  Schema.decodeUnknownEffect(Heads)(record.value.materialized_heads),
                  Schema.decodeUnknownEffect(Heads)(record.value.accepted_heads)
                ], { discard: true }).pipe(
                  Effect.mapError((cause) =>
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.BackupInvalid({ cause })
                    })
                  )
                )
                break
              }
              case "Change": {
                const decodedChange = yield* Effect.try({
                  try: () => Automerge.decodeChange(record.value.bytes),
                  catch: (cause) =>
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.BackupInvalid({
                        cause
                      })
                    })
                })
                if (
                  !definition.documents.byName.has(record.value.document_type) ||
                  (record.value.applied !== 0 && record.value.applied !== 1) ||
                  decodedChange.hash !== record.value.change_hash ||
                  decodedChange.actor !== record.value.actor ||
                  decodedChange.seq !== record.value.sequence ||
                  Schema.encodeSync(JsonString)(decodedChange.deps) !== record.value.dependencies
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error(`Invalid change record: ${record.value.change_hash}`)
                    })
                  })
                }
                metadataByChange.set(record.value.change_hash, {
                  dependencies: decodedChange.deps,
                  operations: decodedChange.ops.length
                })
                break
              }
              case "Checkpoint": {
                const [checksum, checkpointHash] = yield* Effect.all([
                  digest(record.value.bytes),
                  digest({ documentId: record.value.document_id, bytes: record.value.bytes })
                ])
                if (
                  (record.value.verified !== 0 && record.value.verified !== 1) ||
                  checksum !== record.value.checksum || checkpointHash !== record.value.checkpoint_hash
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error(`Invalid checkpoint record: ${record.value.checkpoint_hash}`)
                    })
                  })
                }
                yield* Effect.acquireUseRelease(
                  Effect.try({
                    try: () => Automerge.load<InternalAutomerge.Root<unknown>>(record.value.bytes),
                    catch: (cause) =>
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.BackupInvalid({ cause })
                      })
                  }),
                  (document) =>
                    Effect.try({
                      try: () =>
                        WriterProvenance.isCompactCheckpoint(record.value.writer_provenance)
                          ? undefined
                          : WriterProvenance.validateExact(
                            WriterProvenance.changeHashes(document),
                            record.value.writer_provenance
                          ),
                      catch: (cause) =>
                        new ReplicaError.ReplicaError({
                          reason: new ReplicaError.BackupInvalid({ cause })
                        })
                    }),
                  (document) => Effect.sync(() => InternalAutomerge.free(document))
                )
                break
              }
              case "Transition": {
                const [priorHeads, resultingHeads] = yield* Effect.try({
                  try: () => {
                    const priorHeads = Schema.decodeUnknownSync(Heads)(record.value.prior_heads)
                    const resultingHeads = Schema.decodeUnknownSync(Heads)(record.value.heads)
                    if (
                      Schema.encodeSync(JsonString)(priorHeads) !== record.value.prior_heads ||
                      Schema.encodeSync(JsonString)(resultingHeads) !== record.value.heads
                    ) throw new TypeError(`Transition heads are not canonical: ${record.value.lineage}`)
                    return [priorHeads, resultingHeads] as const
                  },
                  catch: (cause) =>
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.BackupInvalid({ cause })
                    })
                })
                if (record.value.prior_lineage === record.value.lineage) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error(`Transition does not advance lineage: ${record.value.lineage}`)
                    })
                  })
                }
                const priorCheckpointHash = yield* digest({
                  documentId: record.value.document_id,
                  bytes: record.value.prior_snapshot
                })
                yield* Effect.acquireUseRelease(
                  Effect.try({
                    try: () => Automerge.load<InternalAutomerge.Root<unknown>>(record.value.prior_snapshot),
                    catch: (cause) =>
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.BackupInvalid({ cause })
                      })
                  }),
                  (priorDocument) =>
                    Effect.try({
                      try: () => {
                        const actualHeads = Automerge.getHeads(priorDocument).toSorted(Conflict.compareCodeUnits)
                        if (
                          priorCheckpointHash !== record.value.prior_checkpoint_hash ||
                          priorHeads.length !== actualHeads.length ||
                          priorHeads.some((head, index) => head !== actualHeads[index]) ||
                          Automerge.getMissingDeps(priorDocument, []).length !== 0
                        ) throw new TypeError(`Invalid transition prior snapshot: ${record.value.lineage}`)
                      },
                      catch: (cause) =>
                        new ReplicaError.ReplicaError({
                          reason: new ReplicaError.BackupInvalid({ cause })
                        })
                    }),
                  (priorDocument) => Effect.sync(() => InternalAutomerge.free(priorDocument))
                )
                const anchor = checkpointByHash.get(record.value.checkpoint_hash)
                if (anchor !== undefined) {
                  const anchorHeads = yield* Schema.decodeUnknownEffect(Heads)(anchor.heads).pipe(
                    Effect.mapError((cause) =>
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.BackupInvalid({ cause })
                      })
                    )
                  )
                  if (
                    anchor.document_id !== record.value.document_id ||
                    anchor.lineage !== record.value.lineage ||
                    anchorHeads.length !== resultingHeads.length ||
                    anchorHeads.some((head, index) => head !== resultingHeads[index])
                  ) {
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.BackupInvalid({
                        cause: new Error(`Transition anchor checkpoint does not match: ${record.value.lineage}`)
                      })
                    })
                  }
                }
                if (record.value.authorization === null) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.CheckpointRejected({
                      documentId: record.value.document_id,
                      reason: "Lineage transition authorization is missing"
                    })
                  })
                }
                yield* checkpointAuthority.verifyTransition(
                  CheckpointAuthority.TransitionClaims.make({
                    purpose: CheckpointAuthority.transitionPurpose,
                    documentId: record.value.document_id,
                    priorLineage: record.value.prior_lineage,
                    priorCheckpointHash: record.value.prior_checkpoint_hash,
                    priorHeads,
                    resultingLineage: record.value.lineage,
                    anchorCheckpointHash: record.value.checkpoint_hash,
                    resultingHeads,
                    schemaVersion: record.value.schema_version,
                    writerDefinitionHash: record.value.writer_definition_hash
                  }),
                  record.value.authorization
                )
                const transitions = transitionsByDocument.get(record.value.document_id)
                if (transitions === undefined) {
                  transitionsByDocument.set(record.value.document_id, [record.value])
                } else {
                  transitions.push(record.value)
                }
                break
              }
              case "Receipt": {
                if (record.value.replica_incarnation > manifest.incarnation) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error(`Invalid receipt record: ${record.value.command_id}`)
                    })
                  })
                }
                break
              }
            }
          }
          for (const [documentId, transitions] of transitionsByDocument) {
            const document = documentById.get(documentId)!
            const byPriorLineage = new Map(transitions.map((transition) => [transition.prior_lineage, transition]))
            const resultingLineages = new Set(transitions.map((transition) => transition.lineage))
            const roots = transitions.filter((transition) => !resultingLineages.has(transition.prior_lineage))
            if (roots.length !== 1) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Lineage transition chain has no unique root: ${documentId}`)
                })
              })
            }
            let current = roots[0]!
            let visited = 1
            for (;;) {
              const next = byPriorLineage.get(current.lineage)
              if (next === undefined) break
              current = next
              visited += 1
              if (visited > transitions.length) break
            }
            if (visited !== transitions.length || current.lineage !== document.lineage) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Lineage transition chain is not adjacent to the document: ${documentId}`)
                })
              })
            }
          }
          const checkpointDocuments = new Set(
            decoded.flatMap((record) => record.kind === "Checkpoint" ? [record.value.document_id] : [])
          )
          const changesByDocument = new Map<string, Array<typeof StoredChangeRecord.Type>>()
          for (const record of decoded) {
            if (record.kind !== "Change") continue
            const changes = changesByDocument.get(record.value.document_id)
            if (changes === undefined) changesByDocument.set(record.value.document_id, [record.value])
            else changes.push(record.value)
          }
          const historyByDocument = new Map<string, HistoryCounters.HistoryCounters | null>()
          for (const document of documentById.values()) {
            const changes = changesByDocument.get(document.document_id) ?? []
            const complete = yield* Effect.try({
              try: () => {
                const appliedHashes = new Set(
                  changes.flatMap((change) => change.applied === 1 ? [change.change_hash] : [])
                )
                const dependencies = new Set<string>()
                for (const change of changes) {
                  if (change.applied !== 1) continue
                  for (const dependency of metadataByChange.get(change.change_hash)!.dependencies) {
                    if (!appliedHashes.has(dependency)) return false
                    dependencies.add(dependency)
                  }
                }
                const observedHeads = [...appliedHashes]
                  .filter((hash) => !dependencies.has(hash))
                  .toSorted(Conflict.compareCodeUnits)
                const expectedHeads = Schema.decodeSync(
                  Schema.fromJsonString(Schema.Array(Schema.String))
                )(document.materialized_heads).toSorted(Conflict.compareCodeUnits)
                return observedHeads.length === expectedHeads.length &&
                  observedHeads.every((head, index) => head === expectedHeads[index])
              },
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.BackupInvalid({ cause })
                })
            }).pipe(Effect.catch(() => Effect.succeed(false)))
            if (!complete) {
              if (checkpointDocuments.has(document.document_id)) {
                historyByDocument.set(document.document_id, null)
                continue
              }
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Incomplete retained history for ${document.document_id}`)
                })
              })
            }
            const counters = yield* HistoryCounters.check(
              HistoryCounters.measureDecoded(changes.map((change) => ({
                bytes: change.bytes,
                operations: metadataByChange.get(change.change_hash)!.operations
              }))),
              limits
            )
            historyByDocument.set(document.document_id, counters)
          }
          if (options.mode === "document") {
            const selectedDocuments = decoded.flatMap((record) =>
              record.kind === "Document" && record.value.document_id === options.documentId ? [record.value] : []
            )
            if (selectedDocuments.length !== 1) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Backup must contain exactly one document record for ${options.documentId}`)
                })
              })
            }
            const selectedDocument = selectedDocuments[0]!
            if (selectedDocument.document_type !== options.document.name) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Backup document type does not match ${options.document.name}`)
                })
              })
            }
            const selectedChanges = decoded.flatMap((record) =>
              record.kind === "Change" && record.value.document_id === options.documentId ? [record.value] : []
            )
            if (selectedChanges.some((change) => change.document_type !== selectedDocument.document_type)) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Backup changes do not match document ${options.documentId}`)
                })
              })
            }
            const selectedCheckpoints = decoded.flatMap((record) =>
              record.kind === "Checkpoint" && record.value.document_id === options.documentId ? [record.value] : []
            )
            const selectedTransitions = decoded.flatMap((record) =>
              record.kind === "Transition" && record.value.document_id === options.documentId ? [record.value] : []
            )
            const activeCheckpoints = selectedDocument.checkpoint_hash === null
              ? []
              : selectedCheckpoints.filter((checkpoint) =>
                checkpoint.checkpoint_hash === selectedDocument.checkpoint_hash
              )
            if (selectedDocument.checkpoint_hash !== null && activeCheckpoints.length !== 1) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Backup active checkpoint is missing or duplicated for ${options.documentId}`)
                })
              })
            }
            const activeCheckpoint = activeCheckpoints[0]
            const checkpointChanges = new Set(
              activeCheckpoint === undefined
                ? []
                : WriterProvenance.exactEntries(activeCheckpoint.writer_provenance).map((entry) => entry.changeHash)
            )
            if (selectedChanges.some((change) => checkpointChanges.has(change.change_hash) && change.applied !== 1)) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.BackupInvalid({
                  cause: new Error(`Checkpoint materialized change is not applied: ${options.documentId}`)
                })
              })
            }
            let selectedHistory = historyByDocument.get(options.documentId)
            if (selectedHistory === null || selectedHistory === undefined) {
              if (activeCheckpoint === undefined) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.BackupInvalid({
                    cause: new Error(`Backup history cannot be measured for ${options.documentId}`)
                  })
                })
              }
              const checkpointHistory = checkpointHistoryByHash.get(activeCheckpoint.checkpoint_hash)
              if (checkpointHistory === undefined) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.BackupInvalid({
                    cause: new Error(`Checkpoint history is missing: ${activeCheckpoint.checkpoint_hash}`)
                  })
                })
              }
              const retainedHistory = HistoryCounters.measureDecoded(
                selectedChanges.flatMap((change) =>
                  checkpointChanges.has(change.change_hash)
                    ? []
                    : [{ bytes: change.bytes, operations: metadataByChange.get(change.change_hash)!.operations }]
                )
              )
              selectedHistory = yield* HistoryCounters.check({
                changes: checkpointHistory.changes + retainedHistory.changes,
                operations: checkpointHistory.operations + retainedHistory.operations,
                bytes: checkpointHistory.bytes + retainedHistory.bytes
              }, limits)
            }
            const installed = yield* Effect.scoped(gate.admit.pipe(
              Effect.flatMap((permit) =>
                sql.withTransaction(Effect.gen(function*() {
                  const priorInstallation = yield* findInstallation(options.installationId)
                  if (priorInstallation._tag === "Some") {
                    if (
                      priorInstallation.value.manifest_checksum === archiveChecksum &&
                      priorInstallation.value.mode === "document" &&
                      priorInstallation.value.document_id === options.documentId
                    ) return false
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.BackupInvalid({
                        cause: new Error("Backup installation id was already used for a different request")
                      })
                    })
                  }
                  const existingDocument = yield* findDocumentExists(options.documentId).pipe(
                    Effect.catchTag("NoSuchElementError", () =>
                      Effect.fail(
                        new ReplicaError.ReplicaError({
                          reason: new ReplicaError.StorageCorrupt({
                            cause: new Error("Document existence query returned no row")
                          })
                        })
                      ))
                  )
                  if (existingDocument.present === 1) {
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.BackupInvalid({
                        cause: new Error(`Document already exists: ${options.documentId}`)
                      })
                    })
                  }
                  const sequence = yield* incrementCommitSequence(undefined).pipe(
                    Effect.catchTag("NoSuchElementError", () =>
                      Effect.fail(
                        new ReplicaError.ReplicaError({
                          reason: new ReplicaError.ReplicaMetadataMissing({
                            operation: "BackupStore.installDocument"
                          })
                        })
                      ))
                  )
                  yield* sql`INSERT INTO effect_local_documents (
                    document_id, document_type, schema_version, observed_versions,
                    materialized_heads, accepted_heads, tombstone, projection_status,
                    checkpoint_hash, lineage, history_changes, history_operations, history_bytes
                  ) VALUES (
                    ${selectedDocument.document_id}, ${selectedDocument.document_type},
                    ${selectedDocument.schema_version}, ${selectedDocument.observed_versions},
                    ${selectedDocument.materialized_heads}, ${selectedDocument.accepted_heads},
                    ${selectedDocument.tombstone}, 'Blocked', ${selectedDocument.checkpoint_hash},
                    ${selectedDocument.lineage}, ${selectedHistory.changes},
                    ${selectedHistory.operations}, ${selectedHistory.bytes}
                  )`
                  const installedChanges = selectedChanges.map((change) =>
                    Object.assign({}, change, {
                      commit_sequence: checkpointChanges.has(change.change_hash)
                        ? sequence.prior_sequence
                        : sequence.commit_sequence
                    })
                  )
                  for (let index = 0; index < installedChanges.length; index += 50) {
                    yield* sql`INSERT INTO effect_local_changes ${
                      sql.insert(
                        installedChanges.slice(index, index + 50)
                      )
                    }`
                  }
                  if (activeCheckpoint !== undefined) {
                    const writerProvenance = yield* Schema.encodeEffect(
                      StoredCheckpointProvenance
                    )(activeCheckpoint.writer_provenance)
                    yield* sql`INSERT INTO effect_local_checkpoints (
                      checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence,
                      verified, writer_provenance, lineage
                    ) VALUES (
                      ${activeCheckpoint.checkpoint_hash}, ${activeCheckpoint.document_id},
                      ${activeCheckpoint.heads}, ${activeCheckpoint.bytes}, ${activeCheckpoint.checksum},
                      ${sequence.prior_sequence}, ${activeCheckpoint.verified},
                      ${writerProvenance}, ${activeCheckpoint.lineage}
                    )`
                  }
                  for (let index = 0; index < selectedTransitions.length; index += 50) {
                    yield* sql`INSERT INTO effect_local_lineage_transitions ${
                      sql.insert(selectedTransitions.slice(index, index + 50))
                    }`
                  }
                  const invalidationKeys = yield* Schema.encodeEffect(
                    Schema.fromJsonString(Schema.Array(Schema.String))
                  )(ReplicaDefinition.documentCommitKeys(options.document.name, options.documentId))
                  yield* sql`INSERT INTO effect_local_commit_outbox (
                    commit_sequence, document_id, invalidation_keys, published
                  ) VALUES (
                    ${sequence.commit_sequence}, ${options.documentId},
                    ${invalidationKeys}, 0
                  )`
                  const stored = yield* recovery.recoverWithPermit(options.document, options.documentId, permit).pipe(
                    Effect.mapError((error) =>
                      error.reason._tag === "StorageCorrupt"
                        ? new ReplicaError.ReplicaError({
                          reason: new ReplicaError.BackupInvalid({ cause: error })
                        })
                        : error
                    )
                  )
                  yield* projections.replaceDocument(
                    options.document,
                    stored.snapshot,
                    sequence.commit_sequence,
                    "Fresh"
                  ).pipe(Effect.ensuring(Effect.sync(() => InternalAutomerge.free(stored.automerge))))
                  yield* sql`INSERT INTO effect_local_backup_installations (
                    installation_id, mode, manifest_checksum, installed_at,
                    replica_incarnation, document_id
                  ) VALUES (
                    ${options.installationId}, 'document', ${archiveChecksum},
                    ${DateTime.formatIso(yield* DateTime.now)}, ${permit.incarnation}, ${options.documentId}
                  )`
                  yield* gate.validate(permit)
                  return true
                }))
              )
            ))
            if (!installed) return
            return
          }
          const nextReplicaId = options.mode === "clone"
            ? yield* Identity.makeReplicaId.pipe(
              Effect.provideService(Crypto.Crypto, crypto),
              Effect.mapError((cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
                })
              )
            )
            : manifest.replicaId
          yield* Effect.scoped(Effect.gen(function*() {
            yield* reporter.installing
            yield* gate.claim((permit) =>
              Effect.gen(function*() {
                const restoredPermit: ReplicaGate.Permit = {
                  ...permit,
                  incarnation: Identity.ReplicaIncarnation.make(
                    Math.max(permit.incarnation, manifest.incarnation + 1)
                  )
                }
                const installed = yield* findInstallation(options.installationId)
                if (installed._tag === "Some") {
                  if (
                    installed.value.manifest_checksum !== manifestEnvelope.checksum ||
                    installed.value.mode !== options.mode ||
                    installed.value.document_id !== null
                  ) {
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.BackupInvalid({
                        cause: new Error("Backup installation id was already used for a different restore")
                      })
                    })
                  }
                  return yield* Effect.fail(backupAlreadyInstalled)
                }
                yield* sql`INSERT INTO effect_local_backup_installations (
                installation_id, mode, manifest_checksum, installed_at, replica_incarnation
              ) VALUES (
                ${options.installationId}, ${options.mode}, ${manifestEnvelope.checksum},
                ${DateTime.formatIso(yield* DateTime.now)}, ${restoredPermit.incarnation}
              )`
                yield* sql`DELETE FROM effect_local_peer_relay_outbox`
                yield* sql`DELETE FROM effect_local_peer_relay_outbox_remote_usage`
                yield* sql`DELETE FROM effect_local_peer_relay_outbox_replica_usage`
                yield* sql`DELETE FROM effect_local_command_delivery_events`
                yield* sql`DELETE FROM effect_local_peer_relay_delivery_changes`
                yield* sql`DELETE FROM effect_local_peer_relay_delivery_messages`
                yield* sql`DELETE FROM effect_local_command_delivery_changes`
                yield* sql`DELETE FROM effect_local_command_delivery_sources`
                yield* sql`UPDATE effect_local_command_delivery_control
                  SET refresh_epoch = refresh_epoch + 1
                  WHERE singleton = 1`
                yield* sql`INSERT INTO effect_local_peer_relay_receipt_delete_tokens (receipt_row_id)
                  SELECT row_id
                  FROM effect_local_peer_receipts
                  WHERE relay_message_id IS NOT NULL`
                yield* sql`DELETE FROM effect_local_peer_receipts
                  WHERE relay_message_id IS NOT NULL`
                yield* sql`DELETE FROM effect_local_peer_relay_receipt_usage`
                const clusterTables = yield* findClusterTables(undefined)
                if (clusterTables.some((table) => table.name === `${ClusterStorage.messagePrefix}_replies`)) {
                  yield* sql`DELETE FROM ${sql(`${ClusterStorage.messagePrefix}_replies`)}`
                }
                if (clusterTables.some((table) => table.name === `${ClusterStorage.messagePrefix}_messages`)) {
                  yield* sql`DELETE FROM ${sql(`${ClusterStorage.messagePrefix}_messages`)}`
                }
                yield* sql`DELETE FROM effect_local_commit_outbox`
                yield* sql`DELETE FROM effect_local_command_receipts`
                yield* sql`DELETE FROM effect_local_lineage_transitions`
                yield* sql`DELETE FROM effect_local_checkpoints`
                yield* sql`DELETE FROM effect_local_changes`
                yield* projections.clear
                yield* sql`DELETE FROM effect_local_documents`
                const documents = decoded.flatMap((record) =>
                  record.kind === "Document"
                    ? [{
                      ...record.value,
                      projection_status: "Ready",
                      history_changes: historyByDocument.get(record.value.document_id)?.changes ?? null,
                      history_operations: historyByDocument.get(record.value.document_id)?.operations ?? null,
                      history_bytes: historyByDocument.get(record.value.document_id)?.bytes ?? null
                    }]
                    : []
                )
                const changes = decoded.flatMap((record) => record.kind === "Change" ? [record.value] : [])
                const checkpoints = decoded.flatMap((record) =>
                  record.kind === "Checkpoint"
                    ? [{
                      ...record.value,
                      writer_provenance: Schema.encodeSync(StoredCheckpointProvenance)(
                        record.value.writer_provenance
                      )
                    }]
                    : []
                )
                const transitions = decoded.flatMap((record) => record.kind === "Transition" ? [record.value] : [])
                const receipts = decoded.flatMap((record) => record.kind === "Receipt" ? [record.value] : [])
                for (let index = 0; index < documents.length; index += 50) {
                  yield* sql`INSERT INTO effect_local_documents ${sql.insert(documents.slice(index, index + 50))}`
                }
                for (let index = 0; index < changes.length; index += 50) {
                  yield* sql`INSERT INTO effect_local_changes ${sql.insert(changes.slice(index, index + 50))}`
                }
                for (let index = 0; index < checkpoints.length; index += 50) {
                  yield* sql`INSERT INTO effect_local_checkpoints ${sql.insert(checkpoints.slice(index, index + 50))}`
                }
                for (let index = 0; index < transitions.length; index += 50) {
                  yield* sql`INSERT INTO effect_local_lineage_transitions ${
                    sql.insert(transitions.slice(index, index + 50))
                  }`
                }
                for (let index = 0; index < receipts.length; index += 50) {
                  yield* sql`INSERT INTO effect_local_command_receipts ${sql.insert(receipts.slice(index, index + 50))}`
                }
                const sequences = decoded.flatMap((record) =>
                  "commit_sequence" in record.value ? [record.value.commit_sequence] : []
                )
                const commitSequence = sequences.length === 0 ? 0 : Math.max(...sequences)
                yield* sql`UPDATE effect_local_metadata SET
            replica_id = ${nextReplicaId},
            replica_incarnation = ${restoredPermit.incarnation},
            definition_hash = ${definition.hash},
            commit_sequence = ${commitSequence}
            WHERE singleton = 1`
                for (const record of decoded) {
                  if (record.kind !== "Document") continue
                  const document = definition.documents.byName.get(record.value.document_type)!
                  const stored = yield* recovery.recoverWithPermit(
                    document,
                    record.value.document_id,
                    restoredPermit
                  ).pipe(
                    Effect.mapError((cause) =>
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.BackupInvalid({
                          cause
                        })
                      })
                    )
                  )
                  yield* projections.replaceDocument(document, stored.snapshot, stored.commitSequence, "Reused").pipe(
                    Effect.ensuring(Effect.sync(() => InternalAutomerge.free(stored.automerge)))
                  )
                }
                const foreignKeys = yield* findForeignKeyViolations(undefined)
                if (foreignKeys.length > 0) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error("Backup violates SQLite foreign keys")
                    })
                  })
                }
                yield* gate.validate(restoredPermit)
              })
            ).pipe(
              Effect.catchTag("BackupAlreadyInstalled", () => gate.refresh.pipe(Effect.asVoid))
            )
          }))
        }).pipe(
          Effect.scoped,
          Effect.catchTags({
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
              ),
            SqlError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: !cause.isRetryable &&
                      (cause.reason._tag === "UniqueViolation" || cause.reason._tag === "ConstraintError")
                    ? new ReplicaError.BackupInvalid({
                      cause
                    })
                    : new ReplicaError.StorageUnavailable({
                      cause
                    })
                })
              )
          }),
          Effect.asVoid
        )

      return BackupStore.of({
        export: exportBackup,
        restore: processArchive,
        installDocument: (document, options) => processArchive({ ...options, document, mode: "document" })
      })
    })
  )

export const layerRejectAll = (definition: ReplicaDefinition.Any) =>
  layer(definition).pipe(Layer.provide(CheckpointAuthority.layerRejectAll))
