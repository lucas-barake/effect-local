import * as Automerge from "@automerge/automerge"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as InternalAutomerge from "./internal/automerge.js"
import * as Recovery from "./Recovery.js"
import * as ReplicaGate from "./ReplicaGate.js"

export interface PreparedCheckpoint {
  readonly bytes: Uint8Array
  readonly checkpointHash: string
  readonly checksum: string
  readonly commitSequence: Identity.CommitSequence
  readonly documentId: Identity.DocumentId
  readonly documentType: string
  readonly heads: ReadonlyArray<string>
}

export interface CompactResult {
  readonly checkpoint: PreparedCheckpoint
  readonly published: boolean
}

const CheckpointRow = Schema.Struct({
  bytes: Schema.Uint8Array,
  checkpoint_hash: Schema.String,
  checksum: Schema.String,
  commit_sequence: Schema.Number,
  heads: Schema.String
})

const ChangeHashRow = Schema.Struct({ change_hash: Schema.String })

const decodeHeads = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Array(Schema.String)))

export class Compaction extends Context.Service<Compaction, {
  readonly prepare: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<PreparedCheckpoint, ReplicaError.ReplicaError>
  readonly publish: (checkpoint: PreparedCheckpoint) => Effect.Effect<boolean, ReplicaError.ReplicaError>
  readonly compact: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId
  ) => Effect.Effect<CompactResult, ReplicaError.ReplicaError>
  readonly prune: (documentId: Identity.DocumentId) => Effect.Effect<number, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/Compaction") {}

export const layer: Layer.Layer<Compaction, never, Recovery.Recovery | ReplicaGate.ReplicaGate | SqlClient.SqlClient> =
  Layer.effect(
    Compaction,
    Effect.gen(function*() {
      const recovery = yield* Recovery.Recovery
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient

      const pendingCount = SqlSchema.findOne({
        Request: Identity.DocumentId,
        Result: Schema.Struct({ count: Schema.Number }),
        execute: (documentId) =>
          sql`SELECT COUNT(*) AS count FROM effect_local_changes
          WHERE document_id = ${documentId} AND applied = 0`
      })
      const verifiedCheckpoints = SqlSchema.findAll({
        Request: Identity.DocumentId,
        Result: CheckpointRow,
        execute: (documentId) =>
          sql`SELECT bytes, checkpoint_hash, checksum, commit_sequence, heads
        FROM effect_local_checkpoints
        WHERE document_id = ${documentId} AND verified = 1
        ORDER BY commit_sequence DESC, checkpoint_hash DESC`
      })
      const appliedChanges = SqlSchema.findAll({
        Request: Identity.DocumentId,
        Result: ChangeHashRow,
        execute: (documentId) =>
          sql`SELECT change_hash FROM effect_local_changes
        WHERE document_id = ${documentId} AND applied = 1
        ORDER BY commit_sequence, sequence, change_hash`
      })

      const prepare = <D extends Document.Any,>(document: D, documentId: Identity.DocumentId) =>
        Effect.gen(function*() {
          const stored = yield* recovery.recover(document, documentId)
          return yield* Effect.gen(function*() {
            if (
              JSON.stringify(stored.materializedHeads) !== JSON.stringify(stored.acceptedHeads) ||
              (yield* pendingCount(documentId).pipe(
                  Effect.mapError((cause) =>
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "StorageUnavailable",
                        cause: { _tag: "SqlCause", message: String(cause), code: null }
                      }
                    })
                  )
                )).count !== 0
            ) {
              return yield* new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageCorrupt",
                  cause: { _tag: "AutomergeCause", message: "Cannot compact an incomplete canonical history" }
                }
              })
            }
            const bytes = InternalAutomerge.save(stored.automerge)
            const checksum = yield* Canonical.digest(bytes)
            const checkpointHash = yield* Canonical.digest({ documentId, bytes })
            return {
              bytes,
              checkpointHash,
              checksum,
              commitSequence: stored.commitSequence,
              documentId,
              documentType: document.name,
              heads: stored.materializedHeads
            }
          }).pipe(Effect.ensuring(Effect.sync(() => InternalAutomerge.free(stored.automerge))))
        })

      const publish = (checkpoint: PreparedCheckpoint) =>
        Effect.gen(function*() {
          const permit = yield* gate.current
          const checksum = yield* Canonical.digest(checkpoint.bytes)
          const checkpointHash = yield* Canonical.digest({ documentId: checkpoint.documentId, bytes: checkpoint.bytes })
          if (checkpoint.checkpointHash !== checkpointHash || checkpoint.checksum !== checksum) {
            return yield* new ReplicaError.ReplicaError({
              reason: {
                _tag: "StorageCorrupt",
                cause: { _tag: "AutomergeCause", message: "Prepared checkpoint checksum mismatch" }
              }
            })
          }
          const verified = yield* Effect.acquireUseRelease(
            Effect.try({
              try: () => Automerge.load<InternalAutomerge.Root<unknown>>(checkpoint.bytes),
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "StorageCorrupt",
                    cause: { _tag: "AutomergeCause", message: String(cause) }
                  }
                })
            }),
            (automerge) =>
              Effect.sync(() => JSON.stringify(Automerge.getHeads(automerge)) === JSON.stringify(checkpoint.heads)),
            (automerge) => Effect.sync(() => InternalAutomerge.free(automerge))
          )
          if (!verified) {
            return yield* new ReplicaError.ReplicaError({
              reason: {
                _tag: "StorageCorrupt",
                cause: { _tag: "AutomergeCause", message: "Prepared checkpoint heads mismatch" }
              }
            })
          }
          return yield* sql.withTransaction(Effect.gen(function*() {
            const heads = JSON.stringify(checkpoint.heads)
            const rows = yield* sql<{ readonly document_id: string }>`UPDATE effect_local_documents SET
          checkpoint_hash = ${checkpoint.checkpointHash}
          WHERE document_id = ${checkpoint.documentId}
            AND document_type = ${checkpoint.documentType}
            AND materialized_heads = ${heads}
            AND accepted_heads = ${heads}
            AND (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) = ${checkpoint.commitSequence}
          RETURNING document_id`
            if (rows.length !== 1) {
              yield* gate.validate(permit)
              return false
            }
            yield* sql`INSERT INTO effect_local_checkpoints (
          checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified
        ) VALUES (
          ${checkpoint.checkpointHash}, ${checkpoint.documentId}, ${heads}, ${checkpoint.bytes},
          ${checkpoint.checksum}, ${checkpoint.commitSequence}, 1
        ) ON CONFLICT(checkpoint_hash) DO NOTHING`
            const installed = yield* sql`SELECT checkpoint_hash FROM effect_local_checkpoints
          WHERE checkpoint_hash = ${checkpoint.checkpointHash}
            AND document_id = ${checkpoint.documentId}
            AND heads = ${heads}
            AND bytes = ${checkpoint.bytes}
            AND checksum = ${checkpoint.checksum}
            AND commit_sequence = ${checkpoint.commitSequence}
            AND verified = 1`
            if (installed.length !== 1) {
              return yield* new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageCorrupt",
                  cause: { _tag: "AutomergeCause", message: "Checkpoint identity collision" }
                }
              })
            }
            const retained = yield* verifiedCheckpoints(checkpoint.documentId)
            for (const stale of retained.slice(2)) {
              yield* sql`DELETE FROM effect_local_checkpoints WHERE checkpoint_hash = ${stale.checkpoint_hash}`
            }
            yield* gate.validate(permit)
            return true
          })).pipe(
            Effect.catchTags({
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "StorageUnavailable",
                      cause: { _tag: "SqlCause", message: String(cause), code: null }
                    }
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "StorageUnavailable",
                      cause: { _tag: "SqlCause", message: String(cause), code: null }
                    }
                  })
                )
            })
          )
        })

      const compact = <D extends Document.Any,>(document: D, documentId: Identity.DocumentId) =>
        Effect.gen(function*() {
          const checkpoint = yield* prepare(document, documentId)
          return { checkpoint, published: yield* publish(checkpoint) }
        })

      const prune = (documentId: Identity.DocumentId) =>
        Effect.gen(function*() {
          const permit = yield* gate.current
          const checkpoints = yield* verifiedCheckpoints(documentId).pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageUnavailable",
                  cause: { _tag: "SqlCause", message: String(cause), code: null }
                }
              })
            )
          )
          if (checkpoints.length < 2) return 0
          const newest = checkpoints[0]
          const oldest = checkpoints[checkpoints.length - 1]
          return yield* Effect.scoped(Effect.gen(function*() {
            const [newestChecksum, oldestChecksum, newestHash, oldestHash] = yield* Effect.all([
              Canonical.digest(newest.bytes),
              Canonical.digest(oldest.bytes),
              Canonical.digest({ documentId, bytes: newest.bytes }),
              Canonical.digest({ documentId, bytes: oldest.bytes })
            ])
            if (
              newestChecksum !== newest.checksum || oldestChecksum !== oldest.checksum ||
              newest.checkpoint_hash !== newestHash || oldest.checkpoint_hash !== oldestHash
            ) {
              return yield* new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageCorrupt",
                  cause: { _tag: "AutomergeCause", message: "Cannot prune from a corrupt checkpoint" }
                }
              })
            }
            const newestDocument = yield* Effect.acquireRelease(
              Effect.try({
                try: () => Automerge.load<InternalAutomerge.Root<unknown>>(newest.bytes),
                catch: (cause) =>
                  new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "StorageCorrupt",
                      cause: { _tag: "AutomergeCause", message: String(cause) }
                    }
                  })
              }),
              (document) => Effect.sync(() => InternalAutomerge.free(document))
            )
            const oldestDocument = yield* Effect.acquireRelease(
              Effect.try({
                try: () => Automerge.load<InternalAutomerge.Root<unknown>>(oldest.bytes),
                catch: (cause) =>
                  new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "StorageCorrupt",
                      cause: { _tag: "AutomergeCause", message: String(cause) }
                    }
                  })
              }),
              (document) => Effect.sync(() => InternalAutomerge.free(document))
            )
            const [newestHeads, oldestHeads] = yield* Effect.try({
              try: () => [decodeHeads(newest.heads), decodeHeads(oldest.heads)] as const,
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "StorageCorrupt",
                    cause: { _tag: "AutomergeCause", message: String(cause) }
                  }
                })
            })
            if (
              JSON.stringify(Automerge.getHeads(newestDocument)) !== JSON.stringify(newestHeads) ||
              JSON.stringify(Automerge.getHeads(oldestDocument)) !== JSON.stringify(oldestHeads)
            ) {
              return yield* new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageCorrupt",
                  cause: { _tag: "AutomergeCause", message: "Cannot prune from checkpoint head metadata mismatch" }
                }
              })
            }
            const changes = yield* appliedChanges(documentId).pipe(
              Effect.mapError((cause) =>
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "StorageUnavailable",
                    cause: { _tag: "SqlCause", message: String(cause), code: null }
                  }
                })
              )
            )
            const dominated = changes.filter((change) =>
              Automerge.hasHeads(newestDocument, [change.change_hash]) &&
              Automerge.hasHeads(oldestDocument, [change.change_hash])
            )
            return yield* sql.withTransaction(Effect.gen(function*() {
              const rows = yield* sql<{ readonly document_id: string }>`UPDATE effect_local_documents SET
              checkpoint_hash = checkpoint_hash
              WHERE document_id = ${documentId}
                AND checkpoint_hash = ${newest.checkpoint_hash}
                AND materialized_heads = ${newest.heads}
                AND accepted_heads = ${newest.heads}
                AND (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) = ${newest.commit_sequence}
                AND EXISTS (
                  SELECT 1 FROM effect_local_checkpoints
                  WHERE checkpoint_hash = ${newest.checkpoint_hash}
                    AND bytes = ${newest.bytes}
                    AND checksum = ${newest.checksum}
                    AND heads = ${newest.heads}
                    AND verified = 1
                )
                AND EXISTS (
                  SELECT 1 FROM effect_local_checkpoints
                  WHERE checkpoint_hash = ${oldest.checkpoint_hash}
                    AND bytes = ${oldest.bytes}
                    AND checksum = ${oldest.checksum}
                    AND heads = ${oldest.heads}
                    AND verified = 1
                )
              RETURNING document_id`
              if (rows.length !== 1) {
                yield* gate.validate(permit)
                return 0
              }
              let deleted = 0
              for (const change of dominated) {
                const removed = yield* sql`DELETE FROM effect_local_changes
                WHERE document_id = ${documentId} AND change_hash = ${change.change_hash} AND applied = 1
                RETURNING change_hash`
                deleted += removed.length
              }
              yield* gate.validate(permit)
              return deleted
            })).pipe(
              Effect.catchTag("SqlError", (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "StorageUnavailable",
                      cause: { _tag: "SqlCause", message: String(cause), code: null }
                    }
                  })
                ))
            )
          }))
        })

      return Compaction.of({ compact, prepare, prune, publish })
    })
  )
