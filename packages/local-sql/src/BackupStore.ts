import * as Backup from "@lucas-barake/effect-local/Backup"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Encoding from "effect/Encoding"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SqlClient from "effect/unstable/sql/SqlClient"
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
  document_id: Schema.String,
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
  replica_incarnation: Schema.Int,
  command_id: Schema.String,
  request_hash: Schema.String,
  mutation_name: Schema.String,
  result: Schema.String,
  document_id: Schema.String,
  heads: Schema.String,
  commit_sequence: Schema.Int
})

const EndRecord = Schema.Struct({ recordCount: Schema.Int, recordsChecksum: Schema.String })
const Envelope = Schema.Struct({ kind: Schema.String, checksum: Schema.String, value: Schema.Unknown })
const JsonString = Schema.fromJsonString(Schema.Unknown)
const EnvelopeJson = Schema.fromJsonString(Envelope)

type Envelope = typeof Envelope.Type
type DecodedRecord =
  | { readonly kind: "Document"; readonly value: typeof DocumentRecord.Type }
  | {
    readonly kind: "Change"
    readonly value: Omit<typeof ChangeRecord.Type, "bytes"> & { readonly bytes: Uint8Array }
  }
  | {
    readonly kind: "Checkpoint"
    readonly value: Omit<typeof CheckpointRecord.Type, "bytes"> & { readonly bytes: Uint8Array }
  }
  | {
    readonly kind: "Receipt"
    readonly value: Omit<typeof ReceiptRecord.Type, "result"> & { readonly result: Uint8Array }
  }

const encodeEnvelope = (kind: string, value: unknown) =>
  Canonical.digest(value).pipe(Effect.map((checksum) => ({ kind, checksum, value } satisfies Envelope)))

const encodeEnvelopeJson = (envelope: Envelope) =>
  Schema.encodeEffect(EnvelopeJson)(envelope).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: {
          _tag: "BackupInvalid",
          cause: { _tag: "SchemaCause", message: String(cause), path: [] }
        }
      })
    )
  )

const decodeBytes = (encoded: string) =>
  Effect.fromResult(Encoding.decodeBase64(encoded)).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.ReplicaError({
        reason: {
          _tag: "BackupInvalid",
          cause: { _tag: "SchemaCause", message: String(cause), path: [] }
        }
      })
    )
  )

const byteLength = (value: string) => new TextEncoder().encode(value).byteLength

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
      const recovery = yield* Recovery.make

      const exportBackup = (options: Backup.ExportOptions) =>
        Stream.unwrap(
          Effect.scoped(Effect.gen(function*() {
            if (options.maxBytes > limits.maxBackupBytes) {
              return yield* new ReplicaError.ReplicaError({
                reason: { _tag: "BackupTooLarge", limit: limits.maxBackupBytes, observed: options.maxBytes }
              })
            }
            const identity = yield* gate.shared
            const snapshot = yield* sql.withTransaction(Effect.gen(function*() {
              const sizing = yield* sql<{ readonly raw_bytes: number; readonly record_count: number }>`
              SELECT
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
                ), 0) FROM effect_local_command_receipts) AS raw_bytes
            `
              const recordCount = sizing[0]?.record_count ?? 0
              if (recordCount > limits.maxArchiveRecords) {
                return yield* new ReplicaError.ReplicaError({
                  reason: { _tag: "BackupTooLarge", limit: limits.maxArchiveRecords, observed: recordCount }
                })
              }
              const estimatedBytes = (sizing[0]?.raw_bytes ?? 0) * 2 + recordCount * 512 + 4096
              if (estimatedBytes > options.maxBytes) {
                return yield* new ReplicaError.ReplicaError({
                  reason: { _tag: "BackupTooLarge", limit: options.maxBytes, observed: estimatedBytes }
                })
              }
              const documents = yield* sql<
                Record<string, unknown>
              >`SELECT * FROM effect_local_documents ORDER BY document_id`
              const changes = yield* sql<ArrayBufferView & Record<string, unknown>>`
            SELECT * FROM effect_local_changes ORDER BY document_id, commit_sequence, sequence`
              const checkpoints = yield* sql<ArrayBufferView & Record<string, unknown>>`
            SELECT * FROM effect_local_checkpoints ORDER BY document_id, commit_sequence`
              const receipts = yield* sql<ArrayBufferView & Record<string, unknown>>`
            SELECT * FROM effect_local_command_receipts ORDER BY replica_incarnation, command_id`
              return { documents, changes, checkpoints, receipts }
            }))
            const records = yield* Effect.forEach([
              ...snapshot.documents.map((value) => ["Document", value] as const),
              ...snapshot.changes.map((row) =>
                ["Change", { ...row, bytes: Encoding.encodeBase64(row.bytes as Uint8Array) }] as const
              ),
              ...snapshot.checkpoints.map((row) =>
                ["Checkpoint", { ...row, bytes: Encoding.encodeBase64(row.bytes as Uint8Array) }] as const
              ),
              ...snapshot.receipts.map((row) =>
                ["Receipt", { ...row, result: Encoding.encodeBase64(row.result as Uint8Array) }] as const
              )
            ], ([kind, value]) => encodeEnvelope(kind, value))
            const recordsChecksum = yield* Canonical.digest(records.map((record) => record.checksum))
            const end = yield* encodeEnvelope("End", { recordCount: records.length, recordsChecksum })
            const recordLines = yield* Effect.forEach(records, encodeEnvelopeJson)
            const endLine = yield* encodeEnvelopeJson(end)
            const createdAt = new Date().toISOString()
            let declaredBytes = 0
            let manifestLine = ""
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
              manifestLine = yield* encodeEnvelopeJson(manifest)
              const next = byteLength(`${manifestLine}\n`) +
                recordLines.reduce((total, line) => total + byteLength(`${line}\n`), 0) +
                byteLength(`${endLine}\n`)
              if (next === declaredBytes) break
              declaredBytes = next
            }
            if (declaredBytes > options.maxBytes) {
              return yield* new ReplicaError.ReplicaError({
                reason: { _tag: "BackupTooLarge", limit: options.maxBytes, observed: declaredBytes }
              })
            }
            return Stream.fromIterable([manifestLine, ...recordLines, endLine]).pipe(
              Stream.map((line) => `${line}\n`),
              Stream.encodeText
            )
          })).pipe(
            Effect.catchTag("SqlError", (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "BackupInvalid",
                    cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                  }
                })
              ))
          )
        )

      const restore = <R,>(
        options: Backup.RestoreOptions<R>
      ): Effect.Effect<void, ReplicaError.ReplicaError, R> =>
        Effect.gen(function*() {
          if (options.maxBytes > limits.maxBackupBytes) {
            return yield* new ReplicaError.ReplicaError({
              reason: { _tag: "BackupTooLarge", limit: limits.maxBackupBytes, observed: options.maxBytes }
            })
          }
          let observedBytes = 0
          const envelopes = (yield* options.source.pipe(
            Stream.mapEffect((chunk) => {
              observedBytes += chunk.byteLength
              if (chunk.byteLength > limits.maxChunkBytes) {
                return Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: { _tag: "BackupTooLarge", limit: limits.maxChunkBytes, observed: chunk.byteLength }
                  })
                )
              }
              return observedBytes > options.maxBytes
                ? Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: { _tag: "BackupTooLarge", limit: options.maxBytes, observed: observedBytes }
                  })
                )
                : Effect.succeed(chunk)
            }),
            Stream.decodeText(),
            Stream.splitLines,
            Stream.mapEffect((line) =>
              Schema.decodeUnknownEffect(JsonString)(line).pipe(
                Effect.filterOrFail(
                  (value) => !exceedsJsonDepth(value, limits.maxJsonDepth),
                  () =>
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "BackupInvalid",
                        cause: {
                          _tag: "SchemaCause",
                          message: `Backup JSON exceeds maximum depth ${limits.maxJsonDepth}`,
                          path: []
                        }
                      }
                    })
                ),
                Effect.flatMap(Schema.decodeUnknownEffect(Envelope)),
                Effect.catchTag("SchemaError", (cause) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "BackupInvalid",
                        cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                      }
                    })
                  ))
              )
            ),
            Stream.runCollect
          )) as Array<Envelope>
          if (envelopes.length > limits.maxArchiveRecords + 2) {
            return yield* new ReplicaError.ReplicaError({
              reason: {
                _tag: "BackupTooLarge",
                limit: limits.maxArchiveRecords,
                observed: envelopes.length - 2
              }
            })
          }
          const manifestEnvelope = envelopes[0]
          const endEnvelope = envelopes.at(-1)
          if (manifestEnvelope?.kind !== "Manifest" || endEnvelope?.kind !== "End") {
            return yield* new ReplicaError.ReplicaError({
              reason: {
                _tag: "BackupInvalid",
                cause: { _tag: "SchemaCause", message: "Backup manifest or ending record is missing", path: [] }
              }
            })
          }
          const manifest = yield* Schema.decodeUnknownEffect(Manifest)(manifestEnvelope.value).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "BackupInvalid",
                  cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                }
              })
            )
          )
          const end = yield* Schema.decodeUnknownEffect(EndRecord)(endEnvelope.value).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "BackupInvalid",
                  cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                }
              })
            )
          )
          if (
            manifest.definitionHash !== options.expectedDefinitionHash ||
            manifest.definitionHash !== definition.hash ||
            manifest.declaredBytes !== observedBytes
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: {
                _tag: "BackupInvalid",
                cause: { _tag: "SchemaCause", message: "Backup manifest does not match this replica", path: [] }
              }
            })
          }
          const records = envelopes.slice(1, -1)
          if (manifest.recordCount !== records.length || end.recordCount !== records.length) {
            return yield* new ReplicaError.ReplicaError({
              reason: {
                _tag: "BackupInvalid",
                cause: { _tag: "SchemaCause", message: "Backup record count mismatch", path: [] }
              }
            })
          }
          for (const envelope of envelopes) {
            if ((yield* Canonical.digest(envelope.value)) !== envelope.checksum) {
              return yield* new ReplicaError.ReplicaError({
                reason: {
                  _tag: "BackupInvalid",
                  cause: { _tag: "SchemaCause", message: `Backup checksum mismatch: ${envelope.kind}`, path: [] }
                }
              })
            }
          }
          if ((yield* Canonical.digest(records.map((record) => record.checksum))) !== end.recordsChecksum) {
            return yield* new ReplicaError.ReplicaError({
              reason: {
                _tag: "BackupInvalid",
                cause: { _tag: "SchemaCause", message: "Backup records checksum mismatch", path: [] }
              }
            })
          }
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
                  reason: {
                    _tag: "BackupInvalid",
                    cause: { _tag: "SchemaCause", message: `Unknown backup record: ${record.kind}`, path: [] }
                  }
                })
            }
          }
          for (const record of decoded) {
            switch (record.kind) {
              case "Document": {
                if (
                  !definition.documents.byName.has(record.value.document_type) ||
                  (record.value.tombstone !== 0 && record.value.tombstone !== 1)
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "BackupInvalid",
                      cause: {
                        _tag: "SchemaCause",
                        message: `Invalid document record: ${record.value.document_id}`,
                        path: []
                      }
                    }
                  })
                }
                break
              }
              case "Change": {
                const decodedChange = yield* Effect.try({
                  try: () => InternalAutomerge.decode(record.value.bytes),
                  catch: (cause) =>
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "BackupInvalid",
                        cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                      }
                    })
                })
                if (
                  !definition.documents.byName.has(record.value.document_type) ||
                  (record.value.applied !== 0 && record.value.applied !== 1) ||
                  decodedChange.hash !== record.value.change_hash ||
                  decodedChange.actor !== record.value.actor ||
                  decodedChange.sequence !== record.value.sequence ||
                  JSON.stringify(decodedChange.dependencies) !== record.value.dependencies
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "BackupInvalid",
                      cause: {
                        _tag: "SchemaCause",
                        message: `Invalid change record: ${record.value.change_hash}`,
                        path: []
                      }
                    }
                  })
                }
                break
              }
              case "Checkpoint": {
                const [checksum, checkpointHash] = yield* Effect.all([
                  Canonical.digest(record.value.bytes),
                  Canonical.digest({ documentId: record.value.document_id, bytes: record.value.bytes })
                ])
                if (
                  (record.value.verified !== 0 && record.value.verified !== 1) ||
                  checksum !== record.value.checksum || checkpointHash !== record.value.checkpoint_hash
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "BackupInvalid",
                      cause: {
                        _tag: "SchemaCause",
                        message: `Invalid checkpoint record: ${record.value.checkpoint_hash}`,
                        path: []
                      }
                    }
                  })
                }
                break
              }
            }
          }
          yield* Effect.scoped(Effect.gen(function*() {
            const permit = yield* gate.exclusive
            const nextReplicaId = options.mode === "clone" ? Identity.makeReplicaId() : manifest.replicaId
            yield* sql.withTransaction(Effect.gen(function*() {
              const clusterTables = yield* sql<{ readonly name: string }>`
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name IN (
                  ${`${ClusterStorage.messagePrefix}_messages`},
                  ${`${ClusterStorage.messagePrefix}_replies`}
                )
              `
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
            replica_incarnation = ${permit.incarnation},
            definition_hash = ${definition.hash},
            commit_sequence = ${commitSequence}
            WHERE singleton = 1`
              for (const record of decoded) {
                if (record.kind !== "Document") continue
                const document = definition.documents.byName.get(record.value.document_type)!
                const stored = yield* recovery.recover(
                  document,
                  Identity.DocumentId.make(record.value.document_id)
                ).pipe(
                  Effect.mapError((cause) =>
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "BackupInvalid",
                        cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                      }
                    })
                  )
                )
                yield* projections.replaceDocument(document, stored.snapshot, stored.commitSequence).pipe(
                  Effect.ensuring(Effect.sync(() => InternalAutomerge.free(stored.automerge)))
                )
              }
              const foreignKeys = yield* sql`PRAGMA foreign_key_check`
              if (foreignKeys.length > 0) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "BackupInvalid",
                    cause: { _tag: "SchemaCause", message: "Backup violates SQLite foreign keys", path: [] }
                  }
                })
              }
              yield* gate.validate(permit)
            }))
            yield* gate.refresh
          }))
        }).pipe(
          Effect.catchTags({
            SqlError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "BackupInvalid",
                    cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                  }
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "BackupInvalid",
                    cause: { _tag: "SchemaCause", message: String(cause), path: [] }
                  }
                })
              )
          }),
          Effect.asVoid
        ) as Effect.Effect<void, ReplicaError.ReplicaError, R>

      return BackupStore.of({ export: exportBackup, restore })
    })
  )
