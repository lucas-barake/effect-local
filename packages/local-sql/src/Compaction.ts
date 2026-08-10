import * as Automerge from "@automerge/automerge"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as CheckpointAuthority from "./CheckpointAuthority.js"
import * as InternalAutomerge from "./internal/automerge.js"
import * as HistoryCounters from "./internal/historyCounters.js"
import * as NativeError from "./internal/nativeError.js"
import * as WriterProvenance from "./internal/writerProvenance.js"
import * as Recovery from "./Recovery.js"
import * as ReplicaGate from "./ReplicaGate.js"

/**
 * The operator's identity for one maintenance request.
 *
 * Declared here rather than in `ReplicaWorkflow`, which re-exports it, because it is what keys
 * `rewriteHistory`'s durable marker: the rewrite has to name the request it is serving, and
 * `ReplicaWorkflow` already depends on this module. The brand identifier is unchanged from where
 * this used to live, so no persisted or serialized value moves.
 */
export const OperationId = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand("@lucas-barake/effect-local-sql/OperationId")
)
export type OperationId = typeof OperationId.Type

export interface PreparedCheckpoint {
  readonly bytes: Uint8Array
  readonly checkpointHash: string
  readonly checksum: string
  readonly commitSequence: Identity.CommitSequence
  readonly documentId: Identity.DocumentId
  readonly documentType: string
  readonly heads: ReadonlyArray<string>
  readonly writerProvenance: ReadonlyArray<WriterProvenance.ChangeProvenance>
}

export interface CompactResult {
  readonly checkpoint: PreparedCheckpoint
  readonly published: boolean
}

const Heads = Schema.fromJsonString(Schema.Array(Schema.String))

const CheckpointRow = Schema.Struct({
  bytes: Schema.Uint8Array,
  checkpoint_hash: Schema.String,
  checksum: Schema.String,
  commit_sequence: Identity.CommitSequence,
  heads: Heads,
  lineage: Identity.DocumentLineage,
  writer_provenance: WriterProvenance.StoredChangeProvenances
})

const ChangeHashRow = Schema.Struct({ change_hash: Schema.String })
// Plain string, matching `ChangeHashRow` above and the archive's own `command_id` decoding: the
// value is only counted, never used as an identifier, and branding it would make reclaiming a
// malformed legacy row fail instead of removing it.
const CommandIdRow = Schema.Struct({ command_id: Schema.String })
const AppliedChangeRow = Schema.Struct({
  change_hash: WriterProvenance.ChangeHash,
  writer_definition_hash: WriterProvenance.WriterDefinitionHash,
  writer_schema_version: WriterProvenance.WriterSchemaVersion
})
const ChangeProvenanceRow = Schema.Struct({
  change_hash: WriterProvenance.ChangeHash,
  writer_definition_hash: WriterProvenance.WriterDefinitionHash,
  writer_schema_version: WriterProvenance.WriterSchemaVersion
})
const encodeHeads = Schema.encodeSync(Heads)

const DocumentRow = Schema.Struct({ document_id: Identity.DocumentId })
/**
 * What `installDocumentCheckpoint` returns.
 *
 * The lineage comes back from the compare and swap itself rather than from a second read. The swap
 * does not touch the column, so `RETURNING` reports the value the row holds at the moment the
 * checkpoint is installed, on the row the swap matched, inside the transaction that writes the
 * checkpoint. A separate `SELECT` would prove less for one more statement.
 */
const InstalledDocumentRow = Schema.Struct({
  document_id: Identity.DocumentId,
  lineage: Identity.DocumentLineage
})
const CheckpointHashRow = Schema.Struct({ checkpoint_hash: Schema.String })
const CountRow = Schema.Struct({ count: Schema.Int })
const RelayReceiptRewriteRow = Schema.Struct({
  encoded_size: Schema.Int.check(Schema.isGreaterThan(0)),
  replica_incarnation: Identity.ReplicaIncarnation,
  row_id: Schema.Int.check(Schema.isGreaterThan(0)),
  sender_peer_id: Identity.PeerId,
  sender_subject_id: Schema.String,
  sender_tenant_id: Schema.String
})
const RelayReceiptUsageRow = Schema.Struct({
  encoded_bytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  receipt_count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
})
/** Only the columns `rewriteHistory`'s in-transaction guard and compare-and-swap read. */
const RewriteGuardRow = Schema.Struct({
  accepted_heads: Schema.String,
  checkpoint_hash: Schema.NullOr(Schema.String),
  document_type: Schema.String,
  lineage: Identity.DocumentLineage,
  materialized_heads: Schema.String,
  tombstone: Schema.Int
})
/** The `(replica_incarnation, operation_id)` marker a completed history rewrite left behind. */
const RewriteMarkerRow = Schema.Struct({
  document_id: Identity.DocumentId,
  lineage: Identity.DocumentLineage
})
const DocumentLineageRow = Schema.Struct({ lineage: Identity.DocumentLineage })
const CheckpointIdentity = Schema.Struct({
  bytes: Schema.Uint8Array,
  checkpointHash: Schema.String,
  checksum: Schema.String,
  commitSequence: Identity.CommitSequence,
  heads: Heads,
  writerProvenance: WriterProvenance.ChangeProvenances
})

export class Compaction extends Context.Service<Compaction, {
  readonly prepare: (
    document: Document.Any,
    documentId: Identity.DocumentId
  ) => Effect.Effect<PreparedCheckpoint, ReplicaError.ReplicaError>
  readonly publish: (checkpoint: PreparedCheckpoint) => Effect.Effect<boolean, ReplicaError.ReplicaError>
  readonly compact: (
    document: Document.Any,
    documentId: Identity.DocumentId
  ) => Effect.Effect<CompactResult, ReplicaError.ReplicaError>
  readonly prune: (documentId: Identity.DocumentId) => Effect.Effect<number, ReplicaError.ReplicaError>
  /**
   * Reclaims command receipts left behind by superseded replica incarnations and returns how many
   * rows were deleted.
   *
   * Receipts are keyed `(replica_incarnation, command_id)` and are only ever read at the current
   * incarnation, so rows below it can never satisfy a lookup again. The metadata incarnation only
   * ever increases, so those rows are unreachable permanently rather than temporarily.
   *
   * Acquires no gate lock. The caller owns the lock, exactly as `prune` does, so this must be
   * entered under an already held shared or write permit.
   */
  readonly pruneCommandReceipts: Effect.Effect<number, ReplicaError.ReplicaError>
  /**
   * Re-roots a document from its current value and returns the new lineage.
   *
   * Automerge exposes no way to prune a document's change graph, so the only way to bound a
   * high-churn document's checkpoint is to rebuild it as a fresh document holding the value the
   * current one materializes to. This is destructive and is not a compaction: it permanently
   * discards every prior change, every prior checkpoint, and with them the writer provenance of
   * every dropped change. Nothing that was rewritten can be recovered, and no peer can ever
   * reconcile its own history against the result -- the new lineage is what tells a peer its view
   * is unreachable rather than merely behind.
   *
   * Register conflict alternatives are collapsed. The rebuilt document carries exactly the value
   * `Automerge.toJS` exposes, which is the winner of each register; the losing alternatives that
   * `Automerge.getConflicts` would still report on the old document are gone.
   *
   * Refuses unless the document's canonical history is complete and settled: no unapplied change
   * rows, no peer receipt holding an undecoded pending message, no pending peer outbox row, no relay
   * outbox row, no unexpired relay receipt, and materialized heads equal to accepted heads. An orphan
   * peer change leaves no change row at all, so the pending message is its only durable record and
   * dropping it would lose an accepted write. An unexpired relay receipt is retained because it is
   * the duplicate evidence promised through its receipt horizon.
   *
   * Acquires no gate lock. The caller owns the lock, exactly as `prune` and `pruneCommandReceipts`
   * do, so this must be entered under an already held shared or write permit. The permit is only
   * sampled, never claimed: claiming would bump the replica incarnation and writer generation and
   * invalidate every live workflow handle and peer session for what is a single document's
   * maintenance.
   *
   * Idempotent per `operationId`. The rewrite records the lineage it minted against
   * `(replica_incarnation, operationId)` inside its own transaction, and a later call carrying the
   * same operation id returns that recorded lineage having performed no destructive work at all.
   * This is what makes the rewrite safe to re-run: the workflow that drives it dedupes operator
   * REQUESTS, but a crash between this transaction's commit and the journaling of the activity
   * result re-runs the ACTIVITY, and a second lineage would force every peer that already resynced
   * onto the first one to resync again.
   *
   * An operation id is bound to the first document it rewrote. Reusing it for another document fails
   * with `ProtocolMismatch` rather than performing a second destructive rewrite under an identity
   * that already names one.
   */
  readonly rewriteHistory: (
    document: Document.Any,
    documentId: Identity.DocumentId,
    operationId: OperationId
  ) => Effect.Effect<Identity.DocumentLineage, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/Compaction") {}

/**
 * Rows deleted per transaction when reclaiming superseded command receipts. The SQL client owns a
 * single connection, and a transaction holds it for its whole duration, so one unbounded delete over
 * the backlog this reclaims would stall every other command, query, and cluster write in the
 * process. Batching bounds that stall, and bounds the `RETURNING` set the count is derived from.
 */
const receiptPruneBatchSize = 512

export const layer: Layer.Layer<
  Compaction,
  never,
  Crypto.Crypto | Recovery.Recovery | ReplicaGate.ReplicaGate | ReplicaLimits.ReplicaLimits | SqlClient.SqlClient
> = Layer.effect(
  Compaction,
  Effect.gen(function*() {
    const crypto = yield* Crypto.Crypto
    const checkpointAuthority = Option.getOrElse(
      yield* Effect.serviceOption(CheckpointAuthority.CheckpointAuthority),
      () => CheckpointAuthority.rejectAll
    )
    const digest = (value: unknown) => Canonical.digest(value).pipe(Effect.provideService(Crypto.Crypto, crypto))
    const recovery = yield* Recovery.Recovery
    const gate = yield* ReplicaGate.ReplicaGate
    const limits = yield* ReplicaLimits.ReplicaLimits
    const sql = yield* SqlClient.SqlClient

    const pendingCount = SqlSchema.findOneOption({
      Request: Identity.DocumentId,
      Result: Schema.Struct({ count: Schema.Int }),
      execute: (documentId) =>
        sql`SELECT COUNT(*) AS count FROM effect_local_changes
          WHERE document_id = ${documentId} AND applied = 0`
    })
    const verifiedCheckpoints = SqlSchema.findAll({
      Request: Identity.DocumentId,
      Result: CheckpointRow,
      execute: (documentId) =>
        sql`SELECT bytes, checkpoint_hash, checksum, commit_sequence, heads, lineage, writer_provenance
        FROM effect_local_checkpoints
        WHERE document_id = ${documentId} AND verified = 1
        ORDER BY commit_sequence DESC, checkpoint_hash DESC`
    })
    const appliedChanges = SqlSchema.findAll({
      Request: Identity.DocumentId,
      Result: AppliedChangeRow,
      execute: (documentId) =>
        sql`SELECT change_hash, writer_definition_hash, writer_schema_version FROM effect_local_changes
        WHERE document_id = ${documentId} AND applied = 1
        ORDER BY commit_sequence, sequence, change_hash`
    })
    const changeProvenance = SqlSchema.findAll({
      Request: Identity.DocumentId,
      Result: ChangeProvenanceRow,
      execute: (documentId) =>
        sql`SELECT change_hash, writer_definition_hash, writer_schema_version
          FROM effect_local_changes
          WHERE document_id = ${documentId}`
    })
    const installDocumentCheckpoint = SqlSchema.findAll({
      Request: Schema.Struct({
        checkpointHash: Schema.String,
        commitSequence: Identity.CommitSequence,
        documentId: Identity.DocumentId,
        documentType: Schema.String,
        heads: Heads
      }),
      Result: InstalledDocumentRow,
      execute: ({ checkpointHash, commitSequence, documentId, documentType, heads }) =>
        sql`UPDATE effect_local_documents SET checkpoint_hash = ${checkpointHash}
          WHERE document_id = ${documentId}
            AND document_type = ${documentType}
            AND materialized_heads = ${heads}
            AND accepted_heads = ${heads}
            AND (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) = ${commitSequence}
          RETURNING document_id, lineage`
    })
    const installedCheckpoint = SqlSchema.findAll({
      Request: Schema.Struct({
        bytes: Schema.Uint8Array,
        checkpointHash: Schema.String,
        checksum: Schema.String,
        documentId: Identity.DocumentId,
        heads: Heads,
        writerProvenance: WriterProvenance.ChangeProvenances
      }),
      Result: CheckpointHashRow,
      execute: ({ bytes, checkpointHash, checksum, documentId, heads, writerProvenance }) =>
        sql`SELECT checkpoint_hash FROM effect_local_checkpoints
          WHERE checkpoint_hash = ${checkpointHash}
            AND document_id = ${documentId}
            AND heads = ${heads}
            AND bytes = ${bytes}
            AND checksum = ${checksum}
            AND writer_provenance = ${Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(writerProvenance)}
            AND verified = 1`
    })
    const retainDocumentForPrune = SqlSchema.findAll({
      Request: Schema.Struct({
        documentId: Identity.DocumentId,
        newest: CheckpointIdentity,
        oldest: CheckpointIdentity
      }),
      Result: DocumentRow,
      execute: ({ documentId, newest, oldest }) =>
        sql`UPDATE effect_local_documents SET checkpoint_hash = checkpoint_hash
          WHERE document_id = ${documentId}
            AND checkpoint_hash = ${newest.checkpointHash}
            AND materialized_heads = ${newest.heads}
            AND accepted_heads = ${newest.heads}
            AND (SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1) = ${newest.commitSequence}
            AND EXISTS (
              SELECT 1 FROM effect_local_checkpoints
              WHERE checkpoint_hash = ${newest.checkpointHash}
                AND bytes = ${newest.bytes}
                AND checksum = ${newest.checksum}
                AND heads = ${newest.heads}
                AND writer_provenance = ${
          Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(newest.writerProvenance)
        }
                AND verified = 1
            )
            AND EXISTS (
              SELECT 1 FROM effect_local_checkpoints
              WHERE checkpoint_hash = ${oldest.checkpointHash}
                AND bytes = ${oldest.bytes}
                AND checksum = ${oldest.checksum}
                AND heads = ${oldest.heads}
                AND writer_provenance = ${
          Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(oldest.writerProvenance)
        }
                AND verified = 1
            )
          RETURNING document_id`
    })
    const deleteAppliedChange = SqlSchema.findAll({
      Request: Schema.Struct({
        changeHash: Schema.String,
        documentId: Identity.DocumentId
      }),
      Result: ChangeHashRow,
      execute: ({ changeHash, documentId }) =>
        sql`DELETE FROM effect_local_changes
          WHERE document_id = ${documentId} AND change_hash = ${changeHash} AND applied = 1
          RETURNING change_hash`
    })

    // A platform that cannot produce randomness cannot mint a lineage, and a rewrite without one
    // would restart the same actor chain. Reported as unavailable rather than corrupt: nothing
    // durable is wrong, the operation simply could not start.
    const makeLineage = Identity.makeDocumentLineage.pipe(
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.catchTag("PlatformError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({ cause })
          })
        ))
    )
    const findDefinitionHash = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Schema.Struct({ definition_hash: WriterProvenance.WriterDefinitionHash }),
      execute: () => sql`SELECT definition_hash FROM effect_local_metadata WHERE singleton = 1`
    })
    // Same allocator `DocumentStore` uses, so a rewrite takes a slot in the one global commit
    // sequence rather than reusing the sequence the discarded history was written under.
    const nextCommitSequence = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Schema.Struct({ commit_sequence: Identity.CommitSequence }),
      execute: () =>
        sql`UPDATE effect_local_metadata SET commit_sequence = commit_sequence + 1
          WHERE singleton = 1 RETURNING commit_sequence`
    })
    const findRewriteGuard = SqlSchema.findOneOption({
      Request: Identity.DocumentId,
      Result: RewriteGuardRow,
      execute: (documentId) =>
        sql`SELECT accepted_heads, checkpoint_hash, document_type, lineage, materialized_heads, tombstone
          FROM effect_local_documents WHERE document_id = ${documentId}`
    })
    const findRewriteMarker = SqlSchema.findOneOption({
      Request: Schema.Struct({ incarnation: Identity.ReplicaIncarnation, operationId: OperationId }),
      Result: RewriteMarkerRow,
      execute: ({ incarnation, operationId }) =>
        sql`SELECT document_id, lineage FROM effect_local_history_rewrites
          WHERE replica_incarnation = ${incarnation} AND operation_id = ${operationId}`
    })
    const findDocumentLineage = SqlSchema.findOneOption({
      Request: Identity.DocumentId,
      Result: DocumentLineageRow,
      execute: (documentId) => sql`SELECT lineage FROM effect_local_documents WHERE document_id = ${documentId}`
    })
    const changeCount = SqlSchema.findOneOption({
      Request: Identity.DocumentId,
      Result: CountRow,
      execute: (documentId) => sql`SELECT COUNT(*) AS count FROM effect_local_changes WHERE document_id = ${documentId}`
    })
    // An orphan peer change never becomes a change row, so `pendingCount` cannot see it. Its only
    // durable record is the receipt's `pending_message` blob, which the rewrite would delete.
    const pendingReceiptCount = SqlSchema.findOneOption({
      Request: Identity.DocumentId,
      Result: CountRow,
      execute: (documentId) =>
        sql`SELECT COUNT(*) AS count FROM effect_local_peer_receipts
          WHERE document_id = ${documentId} AND pending_message IS NOT NULL`
    })
    const pendingOutboxCount = SqlSchema.findOneOption({
      Request: Identity.DocumentId,
      Result: CountRow,
      execute: (documentId) =>
        sql`SELECT COUNT(*) AS count FROM effect_local_peer_outbox
          WHERE document_id = ${documentId} AND status = 'Pending'`
    })
    // Every relay outbox row is still sender custody. Unlike the direct outbox there is no settled
    // state to ignore, so any row carrying this document blocks a lineage change.
    const relayOutboxCount = SqlSchema.findOneOption({
      Request: Identity.DocumentId,
      Result: CountRow,
      execute: (documentId) =>
        sql`SELECT COUNT(*) AS count FROM effect_local_peer_relay_outbox
          WHERE document_id = ${documentId}`
    })
    const unexpiredRelayReceiptCount = SqlSchema.findOneOption({
      Request: Schema.Struct({
        documentId: Identity.DocumentId,
        expiresAt: Schema.String
      }),
      Result: CountRow,
      execute: (request) =>
        sql`SELECT COUNT(*) AS count FROM effect_local_peer_receipts
          WHERE document_id = ${request.documentId}
            AND relay_message_id IS NOT NULL
            AND relay_receipt_expires_at > ${request.expiresAt}`
    })
    const expiredRelayReceipts = SqlSchema.findAll({
      Request: Schema.Struct({
        documentId: Identity.DocumentId,
        expiresAt: Schema.String
      }),
      Result: RelayReceiptRewriteRow,
      execute: (request) =>
        sql`SELECT relay_encoded_size AS encoded_size, replica_incarnation, row_id,
          relay_sender_peer_id AS sender_peer_id,
          relay_sender_subject_id AS sender_subject_id,
          relay_sender_tenant_id AS sender_tenant_id
          FROM effect_local_peer_receipts
          WHERE document_id = ${request.documentId}
            AND relay_message_id IS NOT NULL
            AND pending_message IS NULL
            AND relay_receipt_expires_at <= ${request.expiresAt}
          ORDER BY replica_incarnation, relay_sender_tenant_id, relay_sender_subject_id,
            relay_sender_peer_id, relay_message_id, row_id`
    })
    const decrementRelayReceiptUsage = SqlSchema.findAll({
      Request: Schema.Struct({
        encodedBytes: Schema.Int.check(Schema.isGreaterThan(0)),
        receiptCount: Schema.Int.check(Schema.isGreaterThan(0)),
        replicaIncarnation: Identity.ReplicaIncarnation,
        senderPeerId: Identity.PeerId,
        senderSubjectId: Schema.String,
        senderTenantId: Schema.String
      }),
      Result: RelayReceiptUsageRow,
      execute: (request) =>
        sql`UPDATE effect_local_peer_relay_receipt_usage
          SET receipt_count = receipt_count - ${request.receiptCount},
            encoded_bytes = encoded_bytes - ${request.encodedBytes}
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND sender_tenant_id = ${request.senderTenantId}
            AND sender_subject_id = ${request.senderSubjectId}
            AND sender_peer_id = ${request.senderPeerId}
            AND receipt_count >= ${request.receiptCount}
            AND encoded_bytes >= ${request.encodedBytes}
          RETURNING receipt_count, encoded_bytes`
    })
    const deleteZeroRelayReceiptUsage = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        senderPeerId: Identity.PeerId,
        senderSubjectId: Schema.String,
        senderTenantId: Schema.String
      }),
      Result: Schema.Struct({ sender_peer_id: Identity.PeerId }),
      execute: (request) =>
        sql`DELETE FROM effect_local_peer_relay_receipt_usage
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND sender_tenant_id = ${request.senderTenantId}
            AND sender_subject_id = ${request.senderSubjectId}
            AND sender_peer_id = ${request.senderPeerId}
            AND receipt_count = 0
            AND encoded_bytes = 0
          RETURNING sender_peer_id`
    })
    const authorizeRelayReceiptDelete = SqlSchema.findAll({
      Request: Schema.Int.check(Schema.isGreaterThan(0)),
      Result: Schema.Struct({ receipt_row_id: Schema.Int.check(Schema.isGreaterThan(0)) }),
      execute: (rowId) =>
        sql`INSERT INTO effect_local_peer_relay_receipt_delete_tokens (receipt_row_id)
          VALUES (${rowId})
          RETURNING receipt_row_id`
    })
    const deleteExpiredRelayReceipt = SqlSchema.findAll({
      Request: Schema.Struct({
        documentId: Identity.DocumentId,
        expiresAt: Schema.String,
        rowId: Schema.Int.check(Schema.isGreaterThan(0))
      }),
      Result: Schema.Struct({ row_id: Schema.Int.check(Schema.isGreaterThan(0)) }),
      execute: (request) =>
        sql`DELETE FROM effect_local_peer_receipts
          WHERE row_id = ${request.rowId}
            AND document_id = ${request.documentId}
            AND relay_message_id IS NOT NULL
            AND pending_message IS NULL
            AND relay_receipt_expires_at <= ${request.expiresAt}
          RETURNING row_id`
    })
    /**
     * The fail closed fence for a history rewrite.
     *
     * `gate.current` is a bare `Ref.get` and takes no lock, so the permit alone proves nothing about
     * concurrent writers. This statement is the real fence: it matches only while the document still
     * carries exactly the heads and checkpoint the rebuilt document was derived from, and every
     * destructive statement of the rewrite runs after it inside the same transaction. `IS` rather
     * than `=` on `checkpoint_hash` so a document that has never been compacted, whose hash is NULL,
     * is matched rather than silently excluded.
     */
    const rewriteDocumentRoot = SqlSchema.findAll({
      Request: Schema.Struct({
        checkpointHash: Schema.String,
        documentId: Identity.DocumentId,
        documentType: Schema.String,
        heads: Heads,
        historyBytes: Schema.Int,
        historyChanges: Schema.Int,
        historyOperations: Schema.Int,
        lineage: Identity.DocumentLineage,
        priorAcceptedHeads: Schema.String,
        priorCheckpointHash: Schema.NullOr(Schema.String),
        priorMaterializedHeads: Schema.String,
        tombstone: Schema.Int
      }),
      Result: DocumentRow,
      execute: (request) =>
        sql`UPDATE effect_local_documents SET
            materialized_heads = ${request.heads},
            accepted_heads = ${request.heads},
            checkpoint_hash = ${request.checkpointHash},
            history_bytes = ${request.historyBytes},
            history_changes = ${request.historyChanges},
            history_operations = ${request.historyOperations},
            tombstone = ${request.tombstone},
            projection_status = 'Blocked',
            lineage = ${request.lineage}
          WHERE document_id = ${request.documentId}
            AND document_type = ${request.documentType}
            AND materialized_heads = ${request.priorMaterializedHeads}
            AND accepted_heads = ${request.priorAcceptedHeads}
            AND checkpoint_hash IS ${request.priorCheckpointHash}
          RETURNING document_id`
    })
    /**
     * The lineage an already completed attempt of this same operator request minted, or `None` when
     * no attempt has committed yet.
     *
     * Modelled on the backup installation guard: look the request up by its own id, reject a
     * recorded row that does not describe the request being served, and otherwise report that the
     * work is already done. Two inconsistencies are rejected rather than worked around. A marker
     * naming a different document means one operation id was reused for two rewrites, and serving it
     * would destroy a second document's history under an identity that already names one. A marker
     * whose lineage is not the document's current lineage means the document was rewritten again
     * after this request committed, so the recorded lineage is one no peer can still reach; reporting
     * it would tell the operator the document sits at a lineage it does not have.
     */
    const replayedRewrite = (
      documentId: Identity.DocumentId,
      operationId: OperationId,
      incarnation: Identity.ReplicaIncarnation
    ) =>
      Effect.gen(function*() {
        const marker = yield* findRewriteMarker({ incarnation, operationId })
        if (Option.isNone(marker)) return Option.none<Identity.DocumentLineage>()
        if (marker.value.document_id !== documentId) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: `history rewrite of ${marker.value.document_id}`,
              observed: `history rewrite of ${documentId}`
            })
          })
        }
        const current = yield* findDocumentLineage(documentId)
        if (!Option.exists(current, (row) => row.lineage === marker.value.lineage)) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: NativeError.nativeError("Recorded history rewrite does not match the document's current lineage")
            })
          })
        }
        return Option.some(marker.value.lineage)
      })

    const prepare = (document: Document.Any, documentId: Identity.DocumentId) =>
      Effect.gen(function*() {
        const stored = yield* recovery.recover(document, documentId)
        return yield* Effect.gen(function*() {
          const pending = yield* pendingCount(documentId).pipe(
            Effect.catchTags({
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({
                      cause
                    })
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({
                      cause
                    })
                  })
                )
            })
          )
          if (
            !Equal.equals(stored.materializedHeads, stored.acceptedHeads) ||
            Option.exists(pending, (row) => row.count !== 0)
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError("Cannot compact an incomplete canonical history")
              })
            })
          }
          const bytes = InternalAutomerge.save(stored.automerge)
          const checksum = yield* digest(bytes)
          const checkpointHash = yield* digest({ documentId, bytes })
          const provenanceRows = yield* changeProvenance(documentId).pipe(
            Effect.catchTags({
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({ cause })
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({ cause })
                  })
                )
            })
          )
          const checkpoints = yield* verifiedCheckpoints(documentId).pipe(
            Effect.catchTags({
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({ cause })
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({ cause })
                  })
                )
            })
          )
          const writerProvenance = yield* Effect.try({
            try: () =>
              WriterProvenance.resolve(
                WriterProvenance.changeHashes(stored.automerge),
                [
                  ...provenanceRows.map((row) => ({
                    changeHash: row.change_hash,
                    writerSchemaVersion: row.writer_schema_version,
                    writerDefinitionHash: row.writer_definition_hash
                  })),
                  ...checkpoints.flatMap((checkpoint) => checkpoint.writer_provenance)
                ]
              ),
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              })
          })
          return {
            bytes,
            checkpointHash,
            checksum,
            commitSequence: stored.commitSequence,
            documentId,
            documentType: document.name,
            heads: stored.materializedHeads,
            writerProvenance
          }
        }).pipe(Effect.ensuring(Effect.sync(() => InternalAutomerge.free(stored.automerge))))
      })

    const publish = (checkpoint: PreparedCheckpoint) =>
      Effect.gen(function*() {
        const permit = yield* gate.current
        const checksum = yield* digest(checkpoint.bytes)
        const checkpointHash = yield* digest({ documentId: checkpoint.documentId, bytes: checkpoint.bytes })
        if (checkpoint.checkpointHash !== checkpointHash || checkpoint.checksum !== checksum) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: NativeError.nativeError("Prepared checkpoint checksum mismatch")
            })
          })
        }
        const checkpointContent = yield* Effect.acquireUseRelease(
          Effect.try({
            try: () => Automerge.load<InternalAutomerge.Root<unknown>>(checkpoint.bytes),
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause
                })
              })
          }),
          (automerge) =>
            Effect.try({
              try: () => {
                const changeHashes = WriterProvenance.changeHashes(automerge)
                WriterProvenance.validateExact(changeHashes, checkpoint.writerProvenance)
                return {
                  changeHashes,
                  headsMatch: Equal.equals(Automerge.getHeads(automerge), checkpoint.heads)
                }
              },
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({ cause })
                })
            }),
          (automerge) => Effect.sync(() => InternalAutomerge.free(automerge))
        )
        if (!checkpointContent.headsMatch) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: NativeError.nativeError("Prepared checkpoint heads mismatch")
            })
          })
        }
        return yield* sql.withTransaction(Effect.gen(function*() {
          const heads = encodeHeads(checkpoint.heads)
          const rows = yield* installDocumentCheckpoint({
            checkpointHash: checkpoint.checkpointHash,
            commitSequence: checkpoint.commitSequence,
            documentId: checkpoint.documentId,
            documentType: checkpoint.documentType,
            heads: checkpoint.heads
          })
          const installedDocument = rows[0]
          if (rows.length !== 1 || installedDocument === undefined) {
            yield* gate.validate(permit)
            return false
          }
          const [provenanceRows, checkpoints] = yield* Effect.all([
            changeProvenance(checkpoint.documentId),
            verifiedCheckpoints(checkpoint.documentId)
          ])
          const durableWriterProvenance = yield* Effect.try({
            try: () =>
              WriterProvenance.resolve(
                checkpointContent.changeHashes,
                [
                  ...provenanceRows.map((row) => ({
                    changeHash: row.change_hash,
                    writerSchemaVersion: row.writer_schema_version,
                    writerDefinitionHash: row.writer_definition_hash
                  })),
                  ...checkpoints.flatMap((stored) => stored.writer_provenance)
                ]
              ),
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              })
          })
          if (!WriterProvenance.equals(durableWriterProvenance, checkpoint.writerProvenance)) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError("Prepared checkpoint writer provenance does not match durable history")
              })
            })
          }
          // Stamped with the lineage the swap above read off this document, exactly as `PeerSync`
          // stamps the lineage it admitted a message under. Leaving it at the column default would
          // label an ordinary compaction of a rewritten document as genesis, so the document and its
          // own checkpoints would disagree about which history they belong to and `Recovery` would
          // reject the checkpoint it just wrote.
          yield* sql`INSERT INTO effect_local_checkpoints (
          checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified, writer_provenance,
          lineage
        ) VALUES (
          ${checkpoint.checkpointHash}, ${checkpoint.documentId}, ${heads}, ${checkpoint.bytes},
          ${checkpoint.checksum}, ${checkpoint.commitSequence}, 1,
          ${Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(checkpoint.writerProvenance)},
          ${installedDocument.lineage}
        ) ON CONFLICT(checkpoint_hash) DO NOTHING`
          const installed = yield* installedCheckpoint({
            bytes: checkpoint.bytes,
            checkpointHash: checkpoint.checkpointHash,
            checksum: checkpoint.checksum,
            documentId: checkpoint.documentId,
            heads: checkpoint.heads,
            writerProvenance: checkpoint.writerProvenance
          })
          if (installed.length !== 1) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError("Checkpoint identity collision")
              })
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
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
              )
          })
        )
      })

    // `<` rather than PeerSync's `!=` idiom for the equivalent peer receipt sweep: no row can hold
    // an incarnation above the current one, because receipts are only written under a validated
    // permit and restore rejects archived rows above the manifest before raising metadata past it.
    // The two predicates therefore select the same rows, but `replica_incarnation` leads the primary
    // key, so `<` is an index range scan where `!=` forces a full scan of the table this reclaims.
    const deleteSupersededReceipts = SqlSchema.findAll({
      Request: Schema.Struct({ incarnation: Identity.ReplicaIncarnation, limit: Schema.Int }),
      Result: CommandIdRow,
      execute: ({ incarnation, limit }) =>
        sql`DELETE FROM effect_local_command_receipts
          WHERE rowid IN (
            SELECT rowid FROM effect_local_command_receipts
            WHERE replica_incarnation < ${incarnation}
            LIMIT ${limit}
          )
          RETURNING command_id`
    })

    const pruneCommandReceipts = Effect.gen(function*() {
      const permit = yield* gate.current
      // The incarnation is a sequence seeded at zero and only ever advanced by a writer re-claim, so
      // a replica still on its first incarnation cannot own a superseded receipt. Returning before
      // opening a transaction keeps compaction free of storage work on every such replica, which is
      // every replica that has never restored a backup.
      if (permit.incarnation === 0) return 0
      let deleted = 0
      while (true) {
        // One transaction per batch. Spanning transactions does not weaken the fence: each batch
        // revalidates the permit before and after its own delete, and the predicate is monotone
        // because the metadata incarnation only increases. A partially completed sweep is a correct
        // partial reclaim that the next run finishes, and a fenced batch leaves earlier ones
        // committed. Lock order stays gate-then-SQL; nothing here acquires the gate.
        const removed = yield* sql.withTransaction(Effect.gen(function*() {
          yield* gate.validate(permit)
          const rows = yield* deleteSupersededReceipts({
            incarnation: permit.incarnation,
            limit: receiptPruneBatchSize
          })
          yield* gate.validate(permit)
          return rows.length
        }))
        deleted += removed
        if (removed < receiptPruneBatchSize) return deleted
      }
    }).pipe(
      Effect.catchTags({
        SqlError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({ cause })
            })
          ),
        SchemaError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause })
            })
          )
      }),
      Effect.tap((deleted) => Effect.annotateCurrentSpan({ "receipts.pruned": deleted })),
      Effect.withSpan("Compaction.pruneCommandReceipts", {
        attributes: { "receipts.batch_size": receiptPruneBatchSize }
      })
    )

    const compact = (document: Document.Any, documentId: Identity.DocumentId) =>
      Effect.gen(function*() {
        const checkpoint = yield* prepare(document, documentId)
        return { checkpoint, published: yield* publish(checkpoint) }
      })

    const prune = (documentId: Identity.DocumentId) =>
      Effect.gen(function*() {
        const permit = yield* gate.current
        const checkpoints = yield* verifiedCheckpoints(documentId).pipe(
          Effect.catchTags({
            SqlError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
              )
          })
        )
        const documentLineage = yield* findDocumentLineage(documentId).pipe(
          Effect.catchTags({
            SqlError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
                })
              ),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
              )
          })
        )
        if (Option.isNone(documentLineage)) return 0
        // The retained pair is chosen from one lineage only, the document's own. A checkpoint left
        // over from a discarded history dominates none of the current history's changes, so a pair
        // that straddled a rewrite would compute domination against a change graph that no longer
        // exists. The document row is the authority here, not the newest checkpoint: a checkpoint
        // that disagrees with it is the inconsistency, and `Recovery` demotes it.
        const retained = checkpoints.filter((checkpoint) => checkpoint.lineage === documentLineage.value.lineage)
        if (retained.length < 2) return 0
        const newest = retained[0]
        const oldest = retained[retained.length - 1]
        return yield* Effect.scoped(Effect.gen(function*() {
          const [newestChecksum, oldestChecksum, newestHash, oldestHash] = yield* Effect.all([
            digest(newest.bytes),
            digest(oldest.bytes),
            digest({ documentId, bytes: newest.bytes }),
            digest({ documentId, bytes: oldest.bytes })
          ])
          if (
            newestChecksum !== newest.checksum || oldestChecksum !== oldest.checksum ||
            newest.checkpoint_hash !== newestHash || oldest.checkpoint_hash !== oldestHash
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError("Cannot prune from a corrupt checkpoint")
              })
            })
          }
          const newestDocument = yield* Effect.acquireRelease(
            Effect.try({
              try: () => Automerge.load<InternalAutomerge.Root<unknown>>(newest.bytes),
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
            }),
            (document) => Effect.sync(() => InternalAutomerge.free(document))
          )
          const oldestDocument = yield* Effect.acquireRelease(
            Effect.try({
              try: () => Automerge.load<InternalAutomerge.Root<unknown>>(oldest.bytes),
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
            }),
            (document) => Effect.sync(() => InternalAutomerge.free(document))
          )
          if (
            !Equal.equals(Automerge.getHeads(newestDocument), newest.heads) ||
            !Equal.equals(Automerge.getHeads(oldestDocument), oldest.heads)
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError("Cannot prune from checkpoint head metadata mismatch")
              })
            })
          }
          const checkpointHashes = yield* Effect.try({
            try: () => {
              const newestHashes = WriterProvenance.changeHashes(newestDocument)
              const oldestHashes = WriterProvenance.changeHashes(oldestDocument)
              WriterProvenance.validateExact(newestHashes, newest.writer_provenance)
              WriterProvenance.validateExact(oldestHashes, oldest.writer_provenance)
              WriterProvenance.resolve(
                [...new Set([...newestHashes, ...oldestHashes])],
                [...newest.writer_provenance, ...oldest.writer_provenance]
              )
              return [...new Set([...newestHashes, ...oldestHashes])]
            },
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              })
          })
          const changes = yield* appliedChanges(documentId).pipe(
            Effect.catchTags({
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({
                      cause
                    })
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({
                      cause
                    })
                  })
                )
            })
          )
          yield* Effect.try({
            try: () =>
              WriterProvenance.resolve(
                checkpointHashes,
                [
                  ...newest.writer_provenance,
                  ...oldest.writer_provenance,
                  ...changes.map((change) => ({
                    changeHash: change.change_hash,
                    writerSchemaVersion: change.writer_schema_version,
                    writerDefinitionHash: change.writer_definition_hash
                  }))
                ]
              ),
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              })
          })
          const dominated = changes.filter((change) =>
            Automerge.hasHeads(newestDocument, [change.change_hash]) &&
            Automerge.hasHeads(oldestDocument, [change.change_hash])
          )
          return yield* sql.withTransaction(Effect.gen(function*() {
            const rows = yield* retainDocumentForPrune({
              documentId,
              newest: {
                bytes: newest.bytes,
                checkpointHash: newest.checkpoint_hash,
                checksum: newest.checksum,
                commitSequence: newest.commit_sequence,
                heads: newest.heads,
                writerProvenance: newest.writer_provenance
              },
              oldest: {
                bytes: oldest.bytes,
                checkpointHash: oldest.checkpoint_hash,
                checksum: oldest.checksum,
                commitSequence: oldest.commit_sequence,
                heads: oldest.heads,
                writerProvenance: oldest.writer_provenance
              }
            })
            if (rows.length !== 1) {
              yield* gate.validate(permit)
              return 0
            }
            let deleted = 0
            for (const change of dominated) {
              const removed = yield* deleteAppliedChange({ changeHash: change.change_hash, documentId })
              deleted += removed.length
            }
            yield* gate.validate(permit)
            return deleted
          })).pipe(
            Effect.catchTags({
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({
                      cause
                    })
                  })
                ),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({
                      cause
                    })
                  })
                )
            })
          )
        }))
      })

    const rewriteHistory = (
      document: Document.Any,
      documentId: Identity.DocumentId,
      operationId: OperationId
    ) =>
      Effect.scoped(Effect.gen(function*() {
        // Sampled, never claimed. `gate.current` is a bare `Ref.get` and takes no lock, so this
        // permit is only the value `gate.validate` fences against; `rewriteDocumentRoot` is what
        // actually serialises the rewrite against a concurrent writer.
        const permit = yield* gate.current
        // Before any of the work below, not only inside the transaction that rechecks it. A replay
        // reaches this after its own rewrite already committed, so the document it would recover and
        // re-root is the rewritten one: redoing that work would discard the whole point of the
        // marker, and the settled-history guard inside the transaction would reject the replay
        // outright once a peer has resynced onto the new lineage and left an outbox row behind.
        const replayed = yield* replayedRewrite(documentId, operationId, permit.incarnation)
        if (Option.isSome(replayed)) {
          yield* Effect.annotateCurrentSpan({ "rewrite.replayed": true })
          return replayed.value
        }
        // Everything up to the transaction runs outside it on purpose. The SQL client owns a single
        // connection and a transaction holds it for its whole duration, so recovering a high-churn
        // document, re-rooting it and hashing the result inside one would stall every other command
        // and query in the process for as long as that takes. `prepare` and `prune` read the same
        // way. Both handles are acquired with their own release: the transaction body below is
        // interruptible and SQL rolls back, but wasm handles do not.
        const stored = yield* Effect.acquireRelease(
          recovery.recover(document, documentId),
          (storedHandle) => Effect.sync(() => InternalAutomerge.free(storedHandle.automerge))
        )
        const priorSnapshot = InternalAutomerge.save(stored.automerge)
        const priorCheckpointHash = yield* digest({ documentId, bytes: priorSnapshot })
        // The heads the rebuilt document was actually derived from, captured here rather than
        // re-read below. `recover` ran in its own transaction and released the connection, so a
        // writer can advance the document before this one opens. Feeding the swap the re-read heads
        // would make it match that advanced row and commit a document rebuilt from a stale value,
        // silently discarding every write in between. These are what make the swap a real fence.
        const priorMaterializedHeads = encodeHeads(stored.materializedHeads)
        const priorAcceptedHeads = encodeHeads(stored.acceptedHeads)
        const preparedGuard = yield* findRewriteGuard(documentId)
        if (Option.isNone(preparedGuard)) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.DocumentNotFound({ documentId })
          })
        }
        const priorLineage = preparedGuard.value.lineage
        const lineage = yield* makeLineage
        const rebuilt = yield* Effect.acquireRelease(
          Effect.try({
            try: () =>
              InternalAutomerge.reroot(
                InternalAutomerge.value(stored.automerge),
                InternalAutomerge.tombstone(stored.automerge),
                InternalAutomerge.rewriteActorId(permit.replicaId, permit.writerGeneration, documentId, lineage)
              ),
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              })
          }),
          (rebuiltHandle) => Effect.sync(() => InternalAutomerge.free(rebuiltHandle))
        )
        const bytes = InternalAutomerge.save(rebuilt)
        yield* Effect.annotateCurrentSpan({ "rewrite.checkpoint_bytes": bytes.byteLength })
        const checksum = yield* digest(bytes)
        const checkpointHash = yield* digest({ documentId, bytes })
        const heads = InternalAutomerge.heads(rebuilt)
        const rootChanges = InternalAutomerge.changesSince(rebuilt, [])
        const rootChange = rootChanges[0]
        if (rootChanges.length !== 1 || rootChange === undefined) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: NativeError.nativeError(`Re-rooted document produced ${rootChanges.length} changes instead of one`)
            })
          })
        }
        const history = yield* HistoryCounters.check(HistoryCounters.measureDecoded(rootChanges), limits)
        const definitionHash = yield* findDefinitionHash(undefined).pipe(
          Effect.map((row) => row.definition_hash),
          Effect.catchTag("NoSuchElementError", () =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.ReplicaMetadataMissing({ operation: "Compaction.rewriteHistory" })
              })
            ))
        )
        const transitionClaims = yield* Schema.decodeUnknownEffect(CheckpointAuthority.TransitionClaims)({
          purpose: CheckpointAuthority.transitionPurpose,
          documentId,
          priorLineage,
          priorCheckpointHash,
          priorHeads: stored.materializedHeads,
          resultingLineage: lineage,
          anchorCheckpointHash: checkpointHash,
          resultingHeads: heads,
          schemaVersion: stored.snapshot.version,
          writerDefinitionHash: definitionHash
        }).pipe(
          Effect.catchTag("SchemaError", (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              })
            ))
        )
        const authorization = yield* checkpointAuthority.signTransition(transitionClaims)
        // The stored schema version, not `document.version`: `recover` decodes at the version the
        // row records and does not migrate, so the value the re-rooted change carries is encoded at
        // exactly that version. `document.version` would attribute a newer encoding to bytes that do
        // not use it. The document row's own `schema_version` is left untouched, so a later
        // `DocumentStore.materialize` still upgrades the rewritten document the ordinary way.
        const writerSchemaVersion = stored.snapshot.version
        const provenanceEntries = yield* Schema.decodeUnknownEffect(WriterProvenance.ChangeProvenances)(
          WriterProvenance.changeHashes(rebuilt).map((changeHash) => ({
            changeHash,
            writerSchemaVersion,
            writerDefinitionHash: definitionHash
          }))
        )
        const writerProvenance = yield* Effect.try({
          try: () => WriterProvenance.resolve(WriterProvenance.changeHashes(rebuilt), provenanceEntries),
          catch: (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause })
            })
        })
        const encodedHeads = encodeHeads(heads)
        const storedProvenance = Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(writerProvenance)
        return yield* sql.withTransaction(Effect.gen(function*() {
          yield* gate.validate(permit)
          // Rechecked inside the transaction that performs the write, and before every guard and
          // every destructive statement below. The read above committed its own transaction and
          // released the connection, so only this one proves no attempt of this request landed in
          // between; and a replay must return the recorded lineage rather than be rejected by the
          // settled-history guard for state the rewrite it already performed produced.
          const recorded = yield* replayedRewrite(documentId, operationId, permit.incarnation)
          if (Option.isSome(recorded)) {
            yield* gate.validate(permit)
            yield* Effect.annotateCurrentSpan({ "rewrite.replayed": true })
            return recorded.value
          }
          const guard = yield* findRewriteGuard(documentId)
          if (Option.isNone(guard)) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.DocumentNotFound({ documentId })
            })
          }
          const row = guard.value
          const currentDefinitionHash = yield* findDefinitionHash(undefined).pipe(
            Effect.map((definitionRow) => definitionRow.definition_hash),
            Effect.catchTag("NoSuchElementError", () =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ReplicaMetadataMissing({ operation: "Compaction.rewriteHistory" })
                })
              ))
          )
          // Re-read from SQL rather than reused from the `recover` above: that read committed its
          // own transaction and released the connection, so anything it observed is stale here.
          const receiptExpiryCutoff = DateTime.formatIso(yield* DateTime.now)
          const [unapplied, receipts, outbox, relayOutbox, unexpiredRelayReceipts, priorChanges] = yield* Effect.all([
            pendingCount(documentId),
            pendingReceiptCount(documentId),
            pendingOutboxCount(documentId),
            relayOutboxCount(documentId),
            unexpiredRelayReceiptCount({ documentId, expiresAt: receiptExpiryCutoff }),
            changeCount(documentId)
          ])
          const nonZero = (count: Option.Option<typeof CountRow.Type>) =>
            Option.exists(count, (countRow) => countRow.count !== 0)
          if (
            row.document_type !== document.name || row.lineage !== transitionClaims.priorLineage ||
            row.materialized_heads !== priorMaterializedHeads || row.accepted_heads !== priorAcceptedHeads ||
            row.materialized_heads !== row.accepted_heads || currentDefinitionHash !== definitionHash ||
            nonZero(unapplied) || nonZero(receipts) || nonZero(outbox) || nonZero(relayOutbox) ||
            nonZero(unexpiredRelayReceipts)
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError("Cannot rewrite the history of an incomplete or unsettled document")
              })
            })
          }
          const tombstone = InternalAutomerge.tombstone(rebuilt)
          if ((row.tombstone === 1) !== tombstone) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError("Re-rooted tombstone does not match the stored document")
              })
            })
          }
          // Compare and swap first, before anything destructive. On zero rows this FAILS rather than
          // reporting a no-op the way `publish` and `prune` do: those commit, and a commit after the
          // deletes below would leave a document with no checkpoints, no change rows and a
          // `checkpoint_hash` pointing at nothing, which no recovery path can repair.
          const swapped = yield* rewriteDocumentRoot({
            checkpointHash,
            documentId,
            documentType: document.name,
            heads,
            historyBytes: history.bytes,
            historyChanges: history.changes,
            historyOperations: history.operations,
            lineage,
            priorAcceptedHeads,
            // The one prior read in transaction. A checkpoint hash is derived from the heads, so at
            // unchanged heads it can only have been moved by a concurrent `publish` of an equivalent
            // checkpoint, which this rewrite supersedes anyway; carrying a stale one would fail the
            // rewrite for no reason. It stays in the predicate so the swap still targets exactly the
            // row the guard above read.
            priorCheckpointHash: row.checkpoint_hash,
            priorMaterializedHeads,
            tombstone: (() => {
              if (tombstone) return (1)
              return (0)
            })()
          })
          if (swapped.length !== 1) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError("Document advanced while its history rewrite was being prepared")
              })
            })
          }
          const commitSequence = yield* nextCommitSequence(undefined).pipe(
            Effect.map((sequenceRow) => sequenceRow.commit_sequence),
            Effect.catchTag("NoSuchElementError", () =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ReplicaMetadataMissing({ operation: "Compaction.rewriteHistory" })
                })
              ))
          )
          const settledRelayReceipts = yield* expiredRelayReceipts({
            documentId,
            expiresAt: receiptExpiryCutoff
          })
          yield* sql`DELETE FROM effect_local_changes WHERE document_id = ${documentId}`
          yield* sql`DELETE FROM effect_local_checkpoints WHERE document_id = ${documentId}`
          yield* sql`DELETE FROM effect_local_peer_outbox WHERE document_id = ${documentId}`
          yield* sql`DELETE FROM effect_local_peer_receipt_replies WHERE document_id = ${documentId}`
          // Relay receipts are quota reservations as well as duplicate evidence. Decrement each
          // exact sender reservation before deleting its expired receipts. Any mismatch fails the
          // transaction, which restores the document root, receipts, and usage together.
          const usage = new Map<string, {
            readonly encodedBytes: number
            readonly receiptCount: number
            readonly replicaIncarnation: Identity.ReplicaIncarnation
            readonly senderPeerId: Identity.PeerId
            readonly senderSubjectId: string
            readonly senderTenantId: string
          }>()
          for (const receipt of settledRelayReceipts) {
            const key = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))([
              receipt.replica_incarnation,
              receipt.sender_tenant_id,
              receipt.sender_subject_id,
              receipt.sender_peer_id
            ])
            const current = usage.get(key)
            usage.set(key, {
              encodedBytes: (current?.encodedBytes ?? 0) + receipt.encoded_size,
              receiptCount: (current?.receiptCount ?? 0) + 1,
              replicaIncarnation: receipt.replica_incarnation,
              senderPeerId: receipt.sender_peer_id,
              senderSubjectId: receipt.sender_subject_id,
              senderTenantId: receipt.sender_tenant_id
            })
          }
          const zeroUsage: Array<{
            readonly replicaIncarnation: Identity.ReplicaIncarnation
            readonly senderPeerId: Identity.PeerId
            readonly senderSubjectId: string
            readonly senderTenantId: string
          }> = []
          for (const entry of usage.values()) {
            const updated = yield* decrementRelayReceiptUsage(entry)
            if (updated.length !== 1) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause: NativeError.nativeError("Relay receipt usage is inconsistent")
                })
              })
            }
            if (updated[0].receipt_count === 0) {
              zeroUsage.push(entry)
            }
          }
          // Remove zero reservations before the receipts they accounted for. Both statements are
          // still invisible outside this transaction, and a later delete failure rolls them back.
          for (const entry of zeroUsage) {
            const deleted = yield* deleteZeroRelayReceiptUsage(entry)
            if (deleted.length !== 1) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause: NativeError.nativeError("Zero relay receipt usage disappeared")
                })
              })
            }
          }
          for (const receipt of settledRelayReceipts) {
            const authorized = yield* authorizeRelayReceiptDelete(receipt.row_id)
            if (authorized.length !== 1) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause: NativeError.nativeError("Relay receipt deletion was not authorized")
                })
              })
            }
            const deleted = yield* deleteExpiredRelayReceipt({
              documentId,
              expiresAt: receiptExpiryCutoff,
              rowId: receipt.row_id
            })
            if (deleted.length !== 1) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause: NativeError.nativeError("Relay receipt disappeared during history rewrite")
                })
              })
            }
          }
          yield* sql`DELETE FROM effect_local_peer_receipts
            WHERE document_id = ${documentId}
              AND relay_message_id IS NULL`
          // `effect_local_command_receipts` is deliberately retained. Its `heads` column goes stale,
          // but nothing reads it: `CommandExecutor` selects only `request_hash`, `mutation_name` and
          // `result`. Deleting the rows would break command idempotency, so a retried command that
          // already committed would run a second time against the rewritten document.
          yield* sql`INSERT INTO effect_local_checkpoints (
            checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified,
            writer_provenance, lineage
          ) VALUES (
            ${checkpointHash}, ${documentId}, ${encodedHeads}, ${bytes}, ${checksum}, ${commitSequence}, 1,
            ${storedProvenance}, ${lineage}
          )`
          // The re-rooted change is written as a change row too. Without it the rewritten document
          // would exist only as a checkpoint, and `Recovery`'s rows-only fallback -- the tail it
          // walks when every checkpoint fails to verify -- would have nothing to rebuild from.
          yield* sql`INSERT INTO effect_local_changes (
            change_hash, document_id, document_type, writer_schema_version, writer_definition_hash,
            actor, sequence, dependencies, bytes, applied, peer_id, accepted_at, commit_sequence
          ) VALUES (
            ${rootChange.hash}, ${documentId}, ${document.name}, ${writerSchemaVersion}, ${definitionHash},
            ${rootChange.actor}, ${rootChange.sequence}, ${encodeHeads(rootChange.dependencies)},
            ${rootChange.bytes}, 1, NULL, ${DateTime.formatIso(yield* DateTime.now)}, ${commitSequence}
          )`
          // Without this row `CommitPublisher` never invalidates the rewritten document, and its gap
          // detector fires on the next commit because the sequence this rewrite consumed is missing.
          yield* sql`INSERT INTO effect_local_commit_outbox (
            commit_sequence, document_id, invalidation_keys, published
          ) VALUES (
            ${commitSequence}, ${documentId},
            ${encodeHeads(ReplicaDefinition.documentCommitKeys(document.name, documentId))}, 0
          )`
          yield* sql`INSERT INTO effect_local_lineage_transitions (
            document_id, prior_lineage, prior_checkpoint_hash, prior_heads, prior_snapshot,
            lineage, checkpoint_hash, heads, schema_version, writer_definition_hash,
            authorization, created_at
          ) VALUES (
            ${documentId}, ${transitionClaims.priorLineage}, ${transitionClaims.priorCheckpointHash},
            ${encodeHeads(transitionClaims.priorHeads)}, ${priorSnapshot}, ${transitionClaims.resultingLineage},
            ${transitionClaims.anchorCheckpointHash}, ${encodeHeads(transitionClaims.resultingHeads)},
            ${transitionClaims.schemaVersion}, ${transitionClaims.writerDefinitionHash},
            ${Option.getOrNull(authorization)}, ${DateTime.formatIso(yield* DateTime.now)}
          )`
          // The marker the replay above consults, written by the same transaction as the rewrite it
          // records so it can never be observed apart from it. A plain insert rather than an upsert:
          // the recheck at the top of this transaction already proved no row exists, and a conflict
          // here would mean the durable state contradicts a read taken on the same connection inside
          // the same transaction, which must surface rather than be absorbed.
          yield* sql`INSERT INTO effect_local_history_rewrites (
            replica_incarnation, operation_id, document_id, lineage, rewritten_at
          ) VALUES (
            ${permit.incarnation}, ${operationId}, ${documentId}, ${lineage},
            ${DateTime.formatIso(yield* DateTime.now)}
          )`

          // Read back before returning. Every other checkpoint write in this file is verified by the
          // recovery or publication that follows it; this one destroys the history that would let a
          // later recovery notice, so it verifies itself here while the transaction can still roll
          // back.
          const installed = yield* verifiedCheckpoints(documentId)
          const persisted = installed[0]
          if (installed.length !== 1 || persisted === undefined) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError(`Rewritten document retained ${installed.length} verified checkpoints`)
              })
            })
          }
          const [persistedChecksum, persistedHash] = yield* Effect.all([
            digest(persisted.bytes),
            digest({ documentId, bytes: persisted.bytes })
          ])
          if (
            persisted.checkpoint_hash !== checkpointHash || persisted.lineage !== lineage ||
            persisted.commit_sequence !== commitSequence || persisted.checksum !== persistedChecksum ||
            persisted.checkpoint_hash !== persistedHash
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError("Rewritten checkpoint does not match what was written")
              })
            })
          }
          yield* Effect.acquireUseRelease(
            Effect.try({
              try: () => Automerge.load<InternalAutomerge.Root<unknown>>(persisted.bytes),
              catch: (cause) =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({ cause })
                })
            }),
            (automerge) =>
              Effect.try({
                try: () => {
                  if (!Equal.equals(Automerge.getHeads(automerge), persisted.heads)) {
                    return NativeError.throwTypeError("Rewritten checkpoint heads do not match its stored heads")
                  }
                  WriterProvenance.validateExact(
                    WriterProvenance.changeHashes(automerge),
                    persisted.writer_provenance
                  )
                  return undefined
                },
                catch: (cause) =>
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({ cause })
                  })
              }),
            (automerge) => Effect.sync(() => InternalAutomerge.free(automerge))
          )
          const confirmed = yield* installedCheckpoint({
            bytes,
            checkpointHash,
            checksum,
            documentId,
            heads,
            writerProvenance
          })
          if (confirmed.length !== 1) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: NativeError.nativeError("Rewritten checkpoint identity collision")
              })
            })
          }
          yield* gate.validate(permit)
          yield* Effect.annotateCurrentSpan({
            "rewrite.changes_removed": Option.match(priorChanges, {
              onNone: () => 0,
              onSome: (changeCountRow) => changeCountRow.count
            })
          })
          return lineage
        }))
      })).pipe(
        Effect.catchTags({
          SqlError: (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({ cause })
              })
            ),
          SchemaError: (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              })
            )
        }),
        // Byte and row counts only, annotated from inside. The document's value, its heads and the
        // lineage itself never reach the span.
        Effect.withSpan("Compaction.rewriteHistory")
      )

    return Compaction.of({ compact, prepare, prune, pruneCommandReceipts, publish, rewriteHistory })
  })
)
