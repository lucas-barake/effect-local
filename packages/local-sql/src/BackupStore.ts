import * as Backup from "@lucas-barake/effect-local/Backup"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
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
import * as InternalAutomerge from "./internal/automerge.js"
import * as ClusterStorage from "./internal/clusterStorage.js"
import * as ProjectionStore from "./ProjectionStore.js"
import * as Recovery from "./Recovery.js"
import * as ReplicaBootstrap from "./ReplicaBootstrap.js"
import * as ReplicaGate from "./ReplicaGate.js"

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
  checkpoint_hash: Schema.NullOr(Schema.String)
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

const CheckpointRecord = Schema.Struct({
  checkpoint_hash: Schema.String,
  document_id: Schema.String,
  heads: Schema.String,
  bytes: Schema.String,
  checksum: Schema.String,
  commit_sequence: Schema.Int,
  verified: Schema.Int
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
const StoredCheckpointRecord = Schema.Struct({ ...CheckpointRecord.fields, bytes: Schema.Uint8Array })
const StoredReceiptRecord = Schema.Struct({ ...ReceiptRecord.fields, result: Schema.Uint8Array })

const EndRecord = Schema.Struct({ recordCount: Schema.Int, recordsChecksum: Schema.String })
const Envelope = Schema.Struct({ kind: Schema.String, checksum: Schema.String, value: Schema.Unknown })
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

type Envelope = typeof Envelope.Type
type DecodedRecord =
  | { readonly kind: "Document"; readonly value: typeof DocumentRecord.Type }
  | { readonly kind: "Change"; readonly value: typeof StoredChangeRecord.Type }
  | { readonly kind: "Checkpoint"; readonly value: typeof StoredCheckpointRecord.Type }
  | { readonly kind: "Receipt"; readonly value: typeof StoredReceiptRecord.Type }

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
}>()("@lucas-barake/effect-local-sql/BackupStore") {}

export const layer = (definition: ReplicaDefinition.Any): Layer.Layer<
  BackupStore,
  never,
  | ProjectionStore.ProjectionStore
  | ReplicaBootstrap.ReplicaBootstrap
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
  | Crypto.Crypto
  | SqlClient.SqlClient
> =>
  Layer.effect(
    BackupStore,
    Effect.gen(function*() {
      yield* ReplicaBootstrap.ReplicaBootstrap
      const gate = yield* ReplicaGate.ReplicaGate
      const limits = yield* ReplicaLimits.ReplicaLimits
      const projections = yield* ProjectionStore.ProjectionStore
      const sql = yield* SqlClient.SqlClient
      const crypto = yield* Crypto.Crypto
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
            (SELECT COUNT(*) FROM effect_local_command_receipts) AS record_count,
            (SELECT COALESCE(SUM(
              length(document_id) + length(document_type) + length(observed_versions) +
              length(materialized_heads) + length(accepted_heads) + length(projection_status) +
              length(COALESCE(checkpoint_hash, ''))
            ), 0) FROM effect_local_documents) +
            (SELECT COALESCE(SUM(
              length(change_hash) + length(document_id) + length(document_type) +
              length(writer_definition_hash) + length(actor) + length(dependencies) + length(bytes) +
              length(COALESCE(peer_id, '')) + length(accepted_at)
            ), 0) FROM effect_local_changes) +
            (SELECT COALESCE(SUM(
              length(checkpoint_hash) + length(document_id) + length(heads) + length(bytes) + length(checksum)
            ), 0) FROM effect_local_checkpoints) +
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
          manifest_checksum: Schema.String,
          mode: Schema.Literals(["clone", "replace"])
        }),
        execute: (installationId) =>
          sql`SELECT mode, manifest_checksum FROM effect_local_backup_installations
            WHERE installation_id = ${installationId}`
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
            const identity = yield* gate.shared
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
              const receipts = yield* findReceipts(undefined)
              yield* gate.validate(identity)
              return { documents, changes, checkpoints, receipts }
            }))
            const records = yield* Effect.forEach([
              ...snapshot.documents.map((value) => ["Document", value] as const),
              ...snapshot.changes.map((row) =>
                ["Change", { ...row, bytes: Encoding.encodeBase64(row.bytes) }] as const
              ),
              ...snapshot.checkpoints.map((row) =>
                ["Checkpoint", { ...row, bytes: Encoding.encodeBase64(row.bytes) }] as const
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

      const restore = <R,>(
        options: Backup.RestoreOptions<R>
      ): Effect.Effect<void, ReplicaError.ReplicaError, R> =>
        Effect.gen(function*() {
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
                    : Effect.succeed(chunk)
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
          const decoded = yield* Effect.gen(function*() {
            const decoded: Array<DecodedRecord> = []
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
                  decoded.push({ kind: "Change", value: { ...encoded, bytes } })
                  break
                }
                case "Checkpoint": {
                  const encoded = yield* Schema.decodeUnknownEffect(CheckpointRecord)(record.value)
                  const bytes = yield* decodeBytes(encoded.bytes)
                  decoded.push({ kind: "Checkpoint", value: { ...encoded, bytes } })
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
          for (const record of decoded) {
            switch (record.kind) {
              case "Document": {
                if (
                  !definition.documents.byName.has(record.value.document_type) ||
                  (record.value.tombstone !== 0 && record.value.tombstone !== 1)
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error(`Invalid document record: ${record.value.document_id}`)
                    })
                  })
                }
                break
              }
              case "Change": {
                const decodedChange = yield* Effect.try({
                  try: () => InternalAutomerge.decode(record.value.bytes),
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
                  decodedChange.sequence !== record.value.sequence ||
                  Schema.encodeSync(JsonString)(decodedChange.dependencies) !== record.value.dependencies
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error(`Invalid change record: ${record.value.change_hash}`)
                    })
                  })
                }
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
                  installed.value.mode !== options.mode
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.BackupInvalid({
                      cause: new Error("Backup installation id was already used for a different restore")
                    })
                  })
                }
                return
              }
              yield* sql`INSERT INTO effect_local_backup_installations (
                installation_id, mode, manifest_checksum, installed_at, replica_incarnation
              ) VALUES (
                ${options.installationId}, ${options.mode}, ${manifestEnvelope.checksum},
                ${DateTime.formatIso(yield* DateTime.now)}, ${restoredPermit.incarnation}
              )`
              const clusterTables = yield* findClusterTables(undefined)
              if (clusterTables.some((table) => table.name === `${ClusterStorage.messagePrefix}_replies`)) {
                yield* sql`DELETE FROM ${sql(`${ClusterStorage.messagePrefix}_replies`)}`
              }
              if (clusterTables.some((table) => table.name === `${ClusterStorage.messagePrefix}_messages`)) {
                yield* sql`DELETE FROM ${sql(`${ClusterStorage.messagePrefix}_messages`)}`
              }
              yield* sql`DELETE FROM effect_local_commit_outbox`
              yield* sql`DELETE FROM effect_local_command_receipts`
              yield* sql`DELETE FROM effect_local_checkpoints`
              yield* sql`DELETE FROM effect_local_changes`
              yield* projections.clear
              yield* sql`DELETE FROM effect_local_documents`
              const documents = decoded.flatMap((record) =>
                record.kind === "Document" ? [{ ...record.value, projection_status: "Ready" }] : []
              )
              const changes = decoded.flatMap((record) => record.kind === "Change" ? [record.value] : [])
              const checkpoints = decoded.flatMap((record) => record.kind === "Checkpoint" ? [record.value] : [])
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
                yield* projections.replaceDocument(document, stored.snapshot, stored.commitSequence).pipe(
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
          )
        }).pipe(
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

      return BackupStore.of({ export: exportBackup, restore })
    })
  )
