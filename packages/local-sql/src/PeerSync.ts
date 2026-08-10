import * as Automerge from "@automerge/automerge"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Arr from "effect/Array"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as RcMap from "effect/RcMap"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as CheckpointAuthority from "./CheckpointAuthority.js"
import * as DocumentStore from "./DocumentStore.js"
import * as InternalAutomerge from "./internal/automerge.js"
import * as HistoryCounters from "./internal/historyCounters.js"
import * as SyncChunks from "./internal/syncChunks.js"
import * as WriterProvenance from "./internal/writerProvenance.js"
import * as PeerRelayReceiptLimits from "./PeerRelayReceiptLimits.js"
import * as PeerSyncEnvelope from "./PeerSyncEnvelope.js"
import * as ProjectionStore from "./ProjectionStore.js"
import * as ReplicaBootstrap from "./ReplicaBootstrap.js"
import * as ReplicaGate from "./ReplicaGate.js"

export interface Session {
  readonly peerId: Identity.PeerId
  readonly connectionEpoch: string
  readonly replicaIncarnation: Identity.ReplicaIncarnation
}

export interface Outbound {
  readonly sendSequence: number
  readonly documentId: Identity.DocumentId
  readonly message: Uint8Array
  readonly messageHash: string
  readonly heads: ReadonlyArray<string>
  /**
   * The document's lineage at the moment this message was generated, not at the moment it is sent.
   *
   * A queued message describes the history it was generated from. Re-reading the document's lineage
   * at send time would relabel a message generated before a rewrite with the lineage that replaced
   * it, and the peer would then accept history the rewrite discarded.
   */
  readonly lineage: Identity.DocumentLineage
  readonly writerProvenance: ReadonlyArray<WriterProvenance.ChangeProvenance>
  readonly checkpointTransfer?: Uint8Array
  readonly receiptReplyId?: number
}

export interface ReplyFragment {
  readonly receiptReplyId: number
  readonly replyIndex: number
  readonly message: Uint8Array
  readonly messageHash: string
  readonly heads: ReadonlyArray<string>
}

export interface Reply {
  readonly documentId: Identity.DocumentId
  readonly message: Uint8Array
  readonly messageHash: string
  readonly heads: ReadonlyArray<string>
  readonly checkpointTransfer?: Uint8Array
  readonly fragments?: ReadonlyArray<ReplyFragment>
}

export interface Generated {
  readonly outbound: Outbound | null
  readonly observedByPeer: boolean
  readonly dirty: boolean
}

export interface Received {
  readonly reply: Reply | null
  readonly heads: ReadonlyArray<string>
  readonly acceptedHeads: ReadonlyArray<string>
  readonly commitSequence: Identity.CommitSequence
  readonly observedByPeer: boolean
  readonly duplicate: boolean
}

export interface RelayReceipt extends PeerTransport.RelayDeliveryIdentity {
  readonly receiptExpiresAt: string
  readonly encodedSize: number
}

const Heads = Schema.fromJsonString(Schema.Array(Schema.String))
const Versions = Schema.fromJsonString(Schema.Array(Schema.Int))

class ConcurrentDocumentWrite extends Schema.TaggedErrorClass<ConcurrentDocumentWrite>(
  "@lucas-barake/effect-local-sql/ConcurrentDocumentWrite"
)("ConcurrentDocumentWrite", {}) {}

const ReceiptRow = Schema.Struct({
  commit_sequence: Schema.Number,
  accepted_heads: Heads,
  heads: Heads,
  message_hash: Schema.String,
  reply: Schema.NullOr(Schema.Uint8Array),
  reply_hash: Schema.NullOr(Schema.String),
  document_id: Schema.String,
  row_id: Schema.Int,
  writer_provenance: WriterProvenance.StoredChangeProvenances,
  checkpoint_transfer: Schema.NullOr(Schema.Uint8Array)
})

const ReceiptReplyRow = Schema.Struct({
  heads: Heads,
  message: Schema.Uint8Array,
  message_hash: Schema.String,
  receipt_row_id: Schema.NullOr(Schema.Int),
  reply_index: Schema.Int,
  row_id: Schema.Int,
  status: Schema.Literals(["Pending", "Sent"])
})

const RelayReceiptRow = Schema.Struct({
  ...ReceiptRow.fields,
  relay_encoded_size: Schema.Int,
  relay_outer_envelope_digest: Schema.String,
  relay_receipt_expires_at: Schema.String
})

const RelayReceiptPruneRow = Schema.Struct({
  encoded_size: Schema.Int,
  relay_message_id: Identity.RelayMessageId,
  row_id: Schema.Int,
  sender_peer_id: Identity.PeerId,
  sender_subject_id: Schema.String,
  sender_tenant_id: Schema.String
})

const RelayReceiptUsageRow = Schema.Struct({
  encoded_bytes: Schema.Int,
  receipt_count: Schema.Int
})

const PendingRow = Schema.Struct({
  actor: Schema.String,
  bytes: Schema.Uint8Array,
  change_hash: Schema.String,
  dependencies: Schema.String,
  sequence: Schema.Int,
  writer_definition_hash: WriterProvenance.WriterDefinitionHash,
  writer_schema_version: WriterProvenance.WriterSchemaVersion
})

const PendingReceiptRow = Schema.Struct({
  accepted_heads: Heads,
  row_id: Schema.Int,
  writer_provenance: WriterProvenance.StoredChangeProvenances
})

const ExistingChangeRow = Schema.Struct({
  actor: Schema.String,
  change_hash: Schema.String,
  document_id: Schema.String,
  sequence: Schema.Number,
  writer_definition_hash: WriterProvenance.WriterDefinitionHash,
  writer_schema_version: WriterProvenance.WriterSchemaVersion
})

const OutboxRow = Schema.Struct({
  checkpoint_transfer: Schema.NullOr(Schema.Uint8Array),
  document_id: Schema.String,
  heads: Heads,
  lineage: Identity.DocumentLineage,
  message: Schema.Uint8Array,
  message_hash: Schema.String,
  receipt_reply_id: Schema.NullOr(Schema.Int),
  send_sequence: Schema.Number,
  writer_provenance: WriterProvenance.StoredChangeProvenances
})

const DocumentLineageRow = Schema.Struct({
  lineage: Identity.DocumentLineage
})

const CheckpointDocumentRow = Schema.Struct({
  accepted_heads: Heads,
  checkpoint_hash: Schema.NullOr(Schema.String),
  document_type: Schema.String,
  lineage: Identity.DocumentLineage,
  materialized_heads: Heads,
  projection_status: Schema.Literals(["Ready", "Blocked", "Rebuilding"]),
  tombstone: Schema.Int
})

const DefinitionHashRow = Schema.Struct({
  definition_hash: WriterProvenance.WriterDefinitionHash
})

const LineageTransitionRow = Schema.Struct({
  authorization: Schema.NullOr(WriterProvenance.AuthorizationToken),
  checkpoint_hash: Schema.String,
  heads: Heads,
  lineage: Identity.DocumentLineage,
  prior_checkpoint_hash: Schema.String,
  prior_heads: Heads,
  prior_lineage: Identity.DocumentLineage,
  prior_snapshot: Schema.Uint8Array,
  schema_version: WriterProvenance.WriterSchemaVersion,
  writer_definition_hash: WriterProvenance.WriterDefinitionHash
})

const ChangeProvenanceRow = Schema.Struct({
  change_hash: WriterProvenance.ChangeHash,
  writer_definition_hash: WriterProvenance.WriterDefinitionHash,
  writer_schema_version: WriterProvenance.WriterSchemaVersion
})

const CheckpointProvenanceRow = Schema.Struct({
  writer_provenance: WriterProvenance.StoredCheckpointProvenance
})

const CheckpointHashRow = Schema.Struct({
  checkpoint_hash: Schema.String
})

const CommitSequenceRow = Schema.Struct({
  commit_sequence: Schema.Number
})

const CountRow = Schema.Struct({
  count: Schema.Number
})

const TotalsRow = Schema.Struct({
  bytes: Schema.Number,
  count: Schema.Number
})

const PendingTotalsRow = Schema.Struct({
  bytes: Schema.Number,
  count: Schema.Number,
  dependencies: Schema.Number
})

const ReceiptTotalsRow = Schema.Struct({
  document_count: Schema.Number,
  peer_count: Schema.Number,
  replica_count: Schema.Number
})

const MarkedOutboxRow = Schema.Struct({
  receipt_reply_id: Schema.NullOr(Schema.Int),
  send_sequence: Schema.Number
})

const MarkedReceiptReplyRow = Schema.Struct({
  receipt_row_id: Schema.NullOr(Schema.Int)
})

const InsertedReceiptReplyRow = Schema.Struct({
  reply_index: Schema.Int,
  row_id: Schema.Int
})

const SequenceRow = Schema.Struct({
  sequence: Schema.Number
})

const sessionKey = (session: Session) => `${session.replicaIncarnation}:${session.peerId}:${session.connectionEpoch}`

const syncStateKey = (session: Session, documentId: Identity.DocumentId) => `${sessionKey(session)}:${documentId}`

const receivedFromReceipt = (
  documentId: Identity.DocumentId,
  receipt: typeof ReceiptRow.Type,
  rows: ReadonlyArray<typeof ReceiptReplyRow.Type>
): Received => ({
  reply: rows[0] === undefined ? null : {
    documentId,
    message: rows[0].message,
    messageHash: rows[0].message_hash,
    heads: rows[0].heads,
    ...(receipt.checkpoint_transfer === null ? {} : { checkpointTransfer: receipt.checkpoint_transfer }),
    fragments: rows.map((row) => ({
      receiptReplyId: row.row_id,
      replyIndex: row.reply_index,
      message: row.message,
      messageHash: row.message_hash,
      heads: row.heads
    }))
  },
  heads: receipt.heads,
  acceptedHeads: receipt.accepted_heads,
  commitSequence: Identity.CommitSequence.make(receipt.commit_sequence),
  observedByPeer: false,
  duplicate: true
})

const sameHeads = (left: ReadonlyArray<string>, right: ReadonlyArray<string>) =>
  Equal.equals(left.toSorted(), right.toSorted())

const syncStateAtHeads = (
  heads: ReadonlyArray<string>,
  sentHashes: ReadonlyArray<string>
): Automerge.SyncState => ({
  ...Automerge.initSyncState(),
  sharedHeads: [...heads],
  lastSentHeads: [...heads],
  theirHeads: [...heads],
  sentHashes: [...sentHashes]
})

export class PeerSync extends Context.Service<PeerSync, {
  readonly open: (peerId: Identity.PeerId) => Effect.Effect<Session, ReplicaError.ReplicaError>
  readonly reset: (session: Session) => Effect.Effect<void, ReplicaError.ReplicaError>
  /**
   * `peer.lineageAware` is what the connected peer advertised, and it gates the send direction.
   *
   * A peer that does not compare lineage unions whatever it is given, so emitting a rewritten
   * document to it would push the discarded history back onto this replica through the peer's own
   * reply. The refusal on the receive side cannot cover that direction, because it is the peer, not
   * this replica, that would be doing the merging.
   */
  readonly generate: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId,
    session: Session,
    peer: { readonly lineageAware: boolean; readonly checkpointTransfer?: boolean }
  ) => Effect.Effect<Generated, ReplicaError.ReplicaError>
  readonly receive: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId,
    session: Session,
    input: {
      readonly remoteConnectionEpoch: string
      readonly receiveSequence: number
      /**
       * Absent for persisted requests and relay envelopes created before lineage was introduced.
       * Those inputs describe the genesis lineage, matching the wire compatibility behavior.
       */
      readonly lineage?: Identity.DocumentLineage
      readonly message: Uint8Array
      readonly writerProvenance: ReadonlyArray<WriterProvenance.ChangeProvenance>
      readonly checkpointTransfer?: Uint8Array
      readonly relay?: RelayReceipt
    }
  ) => Effect.Effect<Received, ReplicaError.ReplicaError>
  readonly enqueue: (session: Session, reply: Reply) => Effect.Effect<Outbound | null, ReplicaError.ReplicaError>
  readonly pending: (session: Session) => Effect.Effect<ReadonlyArray<Outbound>, ReplicaError.ReplicaError>
  readonly markSent: (
    session: Session,
    sendSequence: number,
    messageHash: string
  ) => Effect.Effect<boolean, ReplicaError.ReplicaError>
  /**
   * Runs one document maintenance operation under the same lock as `generate` and `receive`, then
   * clears that document's sync state before releasing the lock.
   *
   * The lock is acquired before `effect` starts, so a capacity failure cannot happen after the
   * maintenance operation commits. The handoff from a successful interruptible operation to state
   * invalidation is uninterruptible.
   */
  readonly withDocumentInvalidation: <A, E, R,>(
    documentId: Identity.DocumentId,
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E | ReplicaError.ReplicaError, R>
  /**
   * Drops the in-memory Automerge sync state every live session holds for one document.
   *
   * A history rewrite replaces the document's change graph without touching the replica
   * incarnation or any session generation, which are the only two things that evict a sync state
   * today. A state kept across a rewrite still describes the discarded history, so `generate` would
   * keep answering from it. Taken under the same per document lock the sync paths use, so it cannot
   * interleave with a `generate` or `receive` for that document.
   */
  readonly invalidateDocument: (documentId: Identity.DocumentId) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly pruneRelayReceipts?: Effect.Effect<number, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/PeerSync") {}

type Requirements =
  | DocumentStore.DocumentStore
  | ReplicaBootstrap.ReplicaBootstrap
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
  | ProjectionStore.ProjectionStore
  | Crypto.Crypto
  | SqlClient.SqlClient

const make = (
  relayReceiptLimits: PeerRelayReceiptLimits.Values | null
) =>
  Effect.gen(function*() {
    void relayReceiptLimits
    const sql = yield* SqlClient.SqlClient
    const store = yield* DocumentStore.DocumentStore
    const bootstrap = yield* ReplicaBootstrap.ReplicaBootstrap
    const gate = yield* ReplicaGate.ReplicaGate
    const limits = yield* ReplicaLimits.ReplicaLimits
    const projections = yield* ProjectionStore.ProjectionStore
    const checkpointAuthority = Option.getOrElse(
      yield* Effect.serviceOption(CheckpointAuthority.CheckpointAuthority),
      () => CheckpointAuthority.rejectAll
    )
    const crypto = yield* Crypto.Crypto
    const digest = (value: unknown) => Canonical.digest(value).pipe(Effect.provideService(Crypto.Crypto, crypto))
    const states = yield* Ref.make(new Map<string, Automerge.SyncState>())
    const sessionGenerations = yield* RcMap.make({
      capacity: limits.maxQueuedRpc,
      lookup: () => Ref.make(0)
    })
    const documentLocks = yield* RcMap.make({
      capacity: limits.maxQueuedRpc,
      lookup: () => Semaphore.make(1)
    })
    const quotaLock = yield* Semaphore.make(1)
    const startupMillis = yield* Clock.currentTimeMillis
    const startupAt = new Date(startupMillis).toISOString()
    const startupCutoff = new Date(startupMillis - limits.maxPendingAgeMillis).toISOString()
    const findReceipts = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId,
        connectionEpoch: Schema.String,
        receiveSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
      }),
      Result: ReceiptRow,
      execute: (request) =>
        sql`SELECT accepted_heads, checkpoint_transfer, commit_sequence, document_id, heads, message_hash, reply,
          reply_hash, row_id, writer_provenance
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND receive_sequence = ${request.receiveSequence}
            AND relay_message_id IS NULL`
    })
    const findRelayReceipts = SqlSchema.findAll({
      Request: Schema.Struct({
        relayMessageId: Identity.RelayMessageId,
        replicaIncarnation: Identity.ReplicaIncarnation,
        senderPeerId: Identity.PeerId,
        senderSubjectId: Schema.String,
        senderTenantId: Schema.String
      }),
      Result: RelayReceiptRow,
      execute: (request) =>
        sql`SELECT accepted_heads, checkpoint_transfer, commit_sequence, document_id, heads, message_hash, reply,
          reply_hash, row_id, writer_provenance, relay_encoded_size, relay_outer_envelope_digest,
          relay_receipt_expires_at
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND relay_sender_tenant_id = ${request.senderTenantId}
            AND relay_sender_subject_id = ${request.senderSubjectId}
            AND relay_sender_peer_id = ${request.senderPeerId}
            AND relay_message_id = ${request.relayMessageId}
          LIMIT 1`
    })
    const findRelayReceiptsToPrune = SqlSchema.findAll({
      Request: Schema.Struct({
        expiresAt: Schema.String,
        limit: Schema.Int.check(Schema.isGreaterThan(0)),
        replicaIncarnation: Identity.ReplicaIncarnation
      }),
      Result: RelayReceiptPruneRow,
      execute: (request) =>
        sql`SELECT relay_encoded_size AS encoded_size, relay_message_id, row_id,
          relay_sender_peer_id AS sender_peer_id,
          relay_sender_subject_id AS sender_subject_id,
          relay_sender_tenant_id AS sender_tenant_id
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND relay_message_id IS NOT NULL
            AND relay_receipt_expires_at <= ${request.expiresAt}
          ORDER BY relay_receipt_expires_at, relay_sender_tenant_id, relay_sender_subject_id,
            relay_sender_peer_id, relay_message_id, row_id
          LIMIT ${request.limit}`
    })
    const findReceiptReplies = SqlSchema.findAll({
      Request: Schema.Int,
      Result: ReceiptReplyRow,
      execute: (receiptRowId) =>
        sql`SELECT heads, message, message_hash, receipt_row_id, reply_index, row_id, status
          FROM effect_local_peer_receipt_replies
          WHERE receipt_row_id = ${receiptRowId}
          ORDER BY reply_index`
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
    const deleteRelayReceipt = SqlSchema.findAll({
      Request: Schema.Struct({
        rowId: Schema.Int
      }),
      Result: Schema.Struct({ row_id: Schema.Int }),
      execute: (request) =>
        sql`DELETE FROM effect_local_peer_receipts
          WHERE row_id = ${request.rowId}
            AND relay_message_id IS NOT NULL
          RETURNING row_id`
    })
    const findRelayReceiptUsage = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        senderPeerId: Identity.PeerId,
        senderSubjectId: Schema.String,
        senderTenantId: Schema.String
      }),
      Result: RelayReceiptUsageRow,
      execute: (request) =>
        sql`SELECT receipt_count, encoded_bytes
          FROM effect_local_peer_relay_receipt_usage
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND sender_tenant_id = ${request.senderTenantId}
            AND sender_subject_id = ${request.senderSubjectId}
            AND sender_peer_id = ${request.senderPeerId}`
    })
    const findRelayReplicaReceiptUsage = SqlSchema.findAll({
      Request: Identity.ReplicaIncarnation,
      Result: TotalsRow,
      execute: (replicaIncarnation) =>
        sql`SELECT COALESCE(SUM(receipt_count), 0) AS count,
          COALESCE(SUM(encoded_bytes), 0) AS bytes
          FROM effect_local_peer_relay_receipt_usage
          WHERE replica_incarnation = ${replicaIncarnation}`
    })
    const recordRelayReceiptUsage = (
      replicaIncarnation: Identity.ReplicaIncarnation,
      relay: RelayReceipt,
      encodedBytes: number
    ) =>
      Effect.gen(function*() {
        yield* sql`INSERT INTO effect_local_peer_relay_receipt_usage (
          replica_incarnation, sender_tenant_id, sender_subject_id, sender_peer_id,
          receipt_count, encoded_bytes
        ) VALUES (
          ${replicaIncarnation}, ${relay.senderTenantId}, ${relay.senderSubjectId},
          ${relay.senderPeerId}, 1, ${encodedBytes}
        ) ON CONFLICT(replica_incarnation, sender_tenant_id, sender_subject_id, sender_peer_id)
        DO UPDATE SET
          receipt_count = effect_local_peer_relay_receipt_usage.receipt_count + 1,
          encoded_bytes = effect_local_peer_relay_receipt_usage.encoded_bytes + excluded.encoded_bytes`
        const remote = (yield* findRelayReceiptUsage({
          replicaIncarnation,
          senderPeerId: relay.senderPeerId,
          senderSubjectId: relay.senderSubjectId,
          senderTenantId: relay.senderTenantId
        }))[0]
        const replica = (yield* findRelayReplicaReceiptUsage(replicaIncarnation))[0]
        if ((remote?.receipt_count ?? 0) > relayReceiptLimits!.maxReceiptsPerRemote) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.QuotaExceeded({
              resource: "relay receipts per remote",
              limit: relayReceiptLimits!.maxReceiptsPerRemote
            })
          })
        }
        if ((remote?.encoded_bytes ?? 0) > relayReceiptLimits!.maxEncodedBytesPerRemote) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.QuotaExceeded({
              resource: "relay receipt bytes per remote",
              limit: relayReceiptLimits!.maxEncodedBytesPerRemote
            })
          })
        }
        if ((replica?.count ?? 0) > relayReceiptLimits!.maxReceiptsPerReplica) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.QuotaExceeded({
              resource: "relay receipts per replica",
              limit: relayReceiptLimits!.maxReceiptsPerReplica
            })
          })
        }
        if ((replica?.bytes ?? 0) > relayReceiptLimits!.maxEncodedBytesPerReplica) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.QuotaExceeded({
              resource: "relay receipt bytes per replica",
              limit: relayReceiptLimits!.maxEncodedBytesPerReplica
            })
          })
        }
      })
    const findExistingChanges = SqlSchema.findAll({
      Request: Schema.Struct({
        documentId: Identity.DocumentId,
        changes: Schema.Array(Schema.Struct({
          actor: Schema.String,
          changeHash: Schema.String,
          sequence: Schema.Int
        }))
      }),
      Result: ExistingChangeRow,
      execute: (request) =>
        sql`SELECT actor, change_hash, document_id, sequence, writer_definition_hash, writer_schema_version
          FROM effect_local_changes
          WHERE ${sql.in("change_hash", request.changes.map((change) => change.changeHash))}
            OR (document_id = ${request.documentId} AND ${
          sql.or(request.changes.map((change) => sql`(actor = ${change.actor} AND sequence = ${change.sequence})`))
        })`
    })
    const findPendingChanges = SqlSchema.findAll({
      Request: Identity.DocumentId,
      Result: PendingRow,
      execute: (documentId) =>
        sql`SELECT actor, bytes, change_hash, dependencies, sequence,
          writer_definition_hash, writer_schema_version
          FROM effect_local_changes
          WHERE document_id = ${documentId} AND applied = 0 ORDER BY accepted_at, change_hash`
    })
    const findPendingReceipts = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        documentId: Identity.DocumentId
      }),
      Result: PendingReceiptRow,
      execute: (request) =>
        sql`SELECT accepted_heads, row_id, writer_provenance
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND document_id = ${request.documentId}
            AND pending_message IS NOT NULL`
    })
    // Deliberately narrow: one column, one row, one index lookup on the primary key. Both lineage
    // gates run before any of the expensive work they protect -- before the pending sweep writes,
    // before a message is decoded, and before the document is rebuilt from storage -- so the read
    // that decides a refusal must cost less than the work it refuses.
    const findDocumentLineage = SqlSchema.findOne({
      Request: Identity.DocumentId,
      Result: DocumentLineageRow,
      execute: (documentId) => sql`SELECT lineage FROM effect_local_documents WHERE document_id = ${documentId}`
    })
    const findCheckpointDocument = SqlSchema.findOneOption({
      Request: Identity.DocumentId,
      Result: CheckpointDocumentRow,
      execute: (documentId) =>
        sql`SELECT accepted_heads, checkpoint_hash, document_type, lineage, materialized_heads,
          projection_status, tombstone
          FROM effect_local_documents WHERE document_id = ${documentId}`
    })
    const checkpointInstallCounts = SqlSchema.findOne({
      Request: Schema.Struct({ documentId: Identity.DocumentId, now: Schema.String }),
      Result: Schema.Struct({
        direct_outbox: Schema.Int,
        pending_changes: Schema.Int,
        pending_receipts: Schema.Int,
        relay_outbox: Schema.Int,
        unexpired_relay_receipts: Schema.Int
      }),
      execute: ({ documentId, now }) =>
        sql`SELECT
          (SELECT COUNT(*) FROM effect_local_changes
            WHERE document_id = ${documentId} AND applied = 0) AS pending_changes,
          (SELECT COUNT(*) FROM effect_local_peer_receipts
            WHERE document_id = ${documentId} AND pending_message IS NOT NULL) AS pending_receipts,
          (SELECT COUNT(*) FROM effect_local_peer_outbox
            WHERE document_id = ${documentId} AND status = 'Pending') AS direct_outbox,
          (SELECT COUNT(*) FROM effect_local_peer_relay_outbox
            WHERE document_id = ${documentId}) AS relay_outbox,
          (SELECT COUNT(*) FROM effect_local_peer_receipts
            WHERE document_id = ${documentId} AND relay_message_id IS NOT NULL
              AND relay_receipt_expires_at > ${now}) AS unexpired_relay_receipts`
    })
    const findDefinitionHash = SqlSchema.findOne({
      Request: Schema.Void,
      Result: DefinitionHashRow,
      execute: () => sql`SELECT definition_hash FROM effect_local_metadata WHERE singleton = 1`
    })
    const findGenerationLineageTransitions = SqlSchema.findAll({
      Request: Schema.Struct({
        documentId: Identity.DocumentId,
        lineage: Identity.DocumentLineage
      }),
      Result: LineageTransitionRow,
      execute: ({ documentId, lineage }) =>
        sql`WITH RECURSIVE transition_chain (
          authorization, checkpoint_hash, heads, lineage, prior_checkpoint_hash,
          prior_heads, prior_lineage, prior_snapshot, schema_version, writer_definition_hash, depth
        ) AS (
          SELECT authorization, checkpoint_hash, heads, lineage, prior_checkpoint_hash,
            prior_heads, prior_lineage, prior_snapshot, schema_version, writer_definition_hash, 1
          FROM effect_local_lineage_transitions
          WHERE document_id = ${documentId} AND lineage = ${lineage}
          UNION ALL
          SELECT transition.authorization, transition.checkpoint_hash, transition.heads,
            transition.lineage, transition.prior_checkpoint_hash, transition.prior_heads,
            transition.prior_lineage, transition.prior_snapshot, transition.schema_version,
            transition.writer_definition_hash, chain.depth + 1
          FROM effect_local_lineage_transitions AS transition
          INNER JOIN transition_chain AS chain ON transition.lineage = chain.prior_lineage
          WHERE transition.document_id = ${documentId} AND chain.depth <= ${PeerSyncEnvelope.maximumCheckpointTransitions}
        )
        SELECT authorization, checkpoint_hash, heads, lineage, prior_checkpoint_hash,
          prior_heads, prior_lineage, prior_snapshot, schema_version, writer_definition_hash
        FROM transition_chain ORDER BY depth
        LIMIT ${PeerSyncEnvelope.maximumCheckpointTransitions + 1}`
    })
    const findRelevantLineageTransitions = SqlSchema.findAll({
      Request: Schema.Struct({
        documentId: Identity.DocumentId,
        lineages: Schema.Array(Identity.DocumentLineage),
        priorLineages: Schema.Array(Identity.DocumentLineage)
      }),
      Result: LineageTransitionRow,
      execute: ({ documentId, lineages, priorLineages }) =>
        sql`SELECT authorization, checkpoint_hash, heads, lineage, prior_checkpoint_hash,
          prior_heads, prior_lineage, prior_snapshot, schema_version, writer_definition_hash
          FROM effect_local_lineage_transitions
          WHERE document_id = ${documentId}
            AND (${sql.in("lineage", lineages)} OR ${sql.in("prior_lineage", priorLineages)})`
    })
    const documentLineage = (documentId: Identity.DocumentId) =>
      findDocumentLineage(documentId).pipe(
        Effect.map((row) => row.lineage),
        Effect.catchTags({
          // A document this replica does not hold has no history for a rewrite to have discarded,
          // so it is on the genesis lineage exactly as a never rewritten document is.
          NoSuchElementError: () => Effect.succeed(Identity.genesisLineage),
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
    const findPendingOutbox = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId,
        connectionEpoch: Schema.String
      }),
      Result: OutboxRow,
      execute: (request) =>
        sql`SELECT checkpoint_transfer, document_id, heads, lineage, message, message_hash, receipt_reply_id,
          send_sequence, writer_provenance
          FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND status = 'Pending'
          ORDER BY send_sequence`
    })
    const findOutboxReply = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId,
        connectionEpoch: Schema.String,
        documentId: Identity.DocumentId,
        messageHash: Schema.String
      }),
      Result: OutboxRow,
      execute: (request) =>
        sql`SELECT checkpoint_transfer, document_id, heads, lineage, message, message_hash, receipt_reply_id,
          send_sequence, writer_provenance
          FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND document_id = ${request.documentId}
            AND message_hash = ${request.messageHash}
          ORDER BY send_sequence
          LIMIT 1`
    })
    const findReceiptRepliesById = SqlSchema.findAll({
      Request: Schema.Array(Schema.Int),
      Result: ReceiptReplyRow,
      execute: (rowIds) =>
        sql`SELECT heads, message, message_hash, receipt_row_id, reply_index, row_id, status
          FROM effect_local_peer_receipt_replies
          WHERE ${sql.in("row_id", rowIds)}
          ORDER BY reply_index`
    })
    const insertReceiptReplies = SqlSchema.findAll({
      Request: Schema.Array(Schema.Struct({
        receipt_row_id: Schema.Int,
        reply_index: Schema.Int,
        document_id: Identity.DocumentId,
        message: Schema.Uint8Array,
        message_hash: Schema.String,
        heads: Schema.String,
        status: Schema.Literal("Pending")
      })),
      Result: InsertedReceiptReplyRow,
      execute: (rows) =>
        sql`INSERT INTO effect_local_peer_receipt_replies ${sql.insert(rows)}
          RETURNING reply_index, row_id`
    })
    const findOutboxReceiptReplies = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId,
        connectionEpoch: Schema.String,
        receiptReplyIds: Schema.Array(Schema.Int)
      }),
      Result: OutboxRow,
      execute: (request) =>
        sql`SELECT checkpoint_transfer, document_id, heads, lineage, message, message_hash, receipt_reply_id,
          send_sequence, writer_provenance
          FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND status = 'Pending'
            AND ${sql.in("receipt_reply_id", request.receiptReplyIds)}
          ORDER BY send_sequence`
    })
    const findChangeProvenance = SqlSchema.findAll({
      Request: Schema.Array(WriterProvenance.ChangeHash),
      Result: ChangeProvenanceRow,
      execute: (changeHashes) =>
        sql`SELECT change_hash, writer_definition_hash, writer_schema_version
          FROM effect_local_changes
          WHERE ${sql.in("change_hash", changeHashes)}`
    })
    const findDocumentChangeProvenance = SqlSchema.findAll({
      Request: Identity.DocumentId,
      Result: ChangeProvenanceRow,
      execute: (documentId) =>
        sql`SELECT change_hash, writer_definition_hash, writer_schema_version
          FROM effect_local_changes
          WHERE document_id = ${documentId}`
    })
    const findCheckpointProvenance = SqlSchema.findAll({
      Request: Identity.DocumentId,
      Result: CheckpointProvenanceRow,
      execute: (documentId) =>
        sql`SELECT writer_provenance
          FROM effect_local_checkpoints
          WHERE document_id = ${documentId} AND verified = 1
          ORDER BY commit_sequence DESC, checkpoint_hash DESC
          LIMIT 2`
    })
    const findCheckpointIdentity = SqlSchema.findAll({
      Request: Schema.Struct({
        bytes: Schema.Uint8Array,
        checkpointHash: Schema.String,
        checksum: Schema.String,
        documentId: Identity.DocumentId,
        heads: Heads,
        writerProvenance: WriterProvenance.ChangeProvenances
      }),
      Result: CheckpointHashRow,
      execute: (request) =>
        sql`SELECT checkpoint_hash FROM effect_local_checkpoints
          WHERE checkpoint_hash = ${request.checkpointHash}
            AND document_id = ${request.documentId}
            AND heads = ${request.heads}
            AND bytes = ${request.bytes}
            AND checksum = ${request.checksum}
            AND verified = 1
            AND writer_provenance = ${
          Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(request.writerProvenance)
        }`
    })
    const findCommitSequence = SqlSchema.findAll({
      Request: Schema.Void,
      Result: CommitSequenceRow,
      execute: () => sql`SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1`
    })
    const incrementCommitSequence = SqlSchema.findAll({
      Request: Schema.Void,
      Result: CommitSequenceRow,
      execute: () =>
        sql`UPDATE effect_local_metadata SET commit_sequence = commit_sequence + 1
          WHERE singleton = 1 RETURNING commit_sequence`
    })
    const updateDocument = SqlSchema.findAll({
      Request: Schema.Struct({
        acceptedHeads: Schema.String,
        checkpointHash: Schema.NullOr(Schema.String),
        documentId: Identity.DocumentId,
        expectedAcceptedHeads: Schema.String,
        expectedHistoryBytes: Schema.NullOr(Schema.Int),
        expectedHistoryChanges: Schema.NullOr(Schema.Int),
        expectedHistoryOperations: Schema.NullOr(Schema.Int),
        expectedMaterializedHeads: Schema.String,
        expectedProjectionStatus: Schema.Literals(["Ready", "Blocked", "Rebuilding"]),
        historyBytes: Schema.NullOr(Schema.Int),
        historyChanges: Schema.NullOr(Schema.Int),
        historyOperations: Schema.NullOr(Schema.Int),
        materializedHeads: Schema.String,
        projectionStatus: Schema.Literals(["Ready", "Blocked", "Rebuilding"]),
        tombstone: Schema.Int
      }),
      Result: Schema.Struct({ document_id: Identity.DocumentId }),
      execute: (request) =>
        sql`UPDATE effect_local_documents SET
          materialized_heads = ${request.materializedHeads},
          accepted_heads = ${request.acceptedHeads},
          history_bytes = ${request.historyBytes},
          history_changes = ${request.historyChanges},
          history_operations = ${request.historyOperations},
          tombstone = ${request.tombstone},
          projection_status = ${request.projectionStatus},
          checkpoint_hash = COALESCE(${request.checkpointHash}, checkpoint_hash)
          WHERE document_id = ${request.documentId}
            AND materialized_heads = ${request.expectedMaterializedHeads}
            AND accepted_heads = ${request.expectedAcceptedHeads}
            AND history_bytes IS ${request.expectedHistoryBytes}
            AND history_changes IS ${request.expectedHistoryChanges}
            AND history_operations IS ${request.expectedHistoryOperations}
            AND projection_status = ${request.expectedProjectionStatus}
          RETURNING document_id`
    })
    const findOutboxTotals = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId,
        connectionEpoch: Schema.String
      }),
      Result: TotalsRow,
      execute: (request) =>
        sql`SELECT COALESCE(SUM(LENGTH(message) + COALESCE(LENGTH(checkpoint_transfer), 0)), 0) AS bytes,
          COUNT(*) AS count
          FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND status = 'Pending'`
    })
    const findNextOutboxSequence = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId,
        connectionEpoch: Schema.String
      }),
      Result: SequenceRow,
      execute: (request) =>
        sql`SELECT COALESCE(MAX(send_sequence), -1) + 1 AS sequence
          FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}`
    })
    const findPendingOutboxCount = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId,
        connectionEpoch: Schema.String,
        documentId: Identity.DocumentId
      }),
      Result: CountRow,
      execute: (request) =>
        sql`SELECT COUNT(*) AS count FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND document_id = ${request.documentId}
            AND status = 'Pending'`
    })
    const findReceiptTotals = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId,
        documentId: Identity.DocumentId
      }),
      Result: ReceiptTotalsRow,
      execute: (request) =>
        sql`SELECT
          (SELECT COUNT(*) FROM effect_local_peer_receipts
            WHERE replica_incarnation = ${request.replicaIncarnation}
              AND document_id = ${request.documentId}
              AND pending_message IS NOT NULL) AS document_count,
          (SELECT COUNT(*) FROM effect_local_peer_receipts
            WHERE replica_incarnation = ${request.replicaIncarnation}
              AND peer_id = ${request.peerId}
              AND pending_message IS NOT NULL) AS peer_count,
          (SELECT COUNT(*) FROM effect_local_peer_receipts
            WHERE replica_incarnation = ${request.replicaIncarnation}
              AND pending_message IS NOT NULL) AS replica_count`
    })
    const findDocumentPendingChangeTotals = SqlSchema.findAll({
      Request: Identity.DocumentId,
      Result: PendingTotalsRow,
      execute: (documentId) =>
        sql`SELECT
          COALESCE(SUM(LENGTH(bytes)), 0) AS bytes,
          COUNT(*) AS count,
          COALESCE(SUM(json_array_length(dependencies)), 0) AS dependencies
          FROM effect_local_changes WHERE document_id = ${documentId} AND applied = 0`
    })
    const findDocumentPendingReceiptTotals = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        documentId: Identity.DocumentId
      }),
      Result: TotalsRow,
      execute: (request) =>
        sql`SELECT COALESCE(SUM(LENGTH(pending_message)), 0) AS bytes, COUNT(pending_message) AS count
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND document_id = ${request.documentId}
            AND pending_message IS NOT NULL`
    })
    const findPeerPendingChangeTotals = SqlSchema.findAll({
      Request: Identity.PeerId,
      Result: PendingTotalsRow,
      execute: (peerId) =>
        sql`SELECT COALESCE(SUM(LENGTH(bytes)), 0) AS bytes, COUNT(*) AS count,
          COALESCE(SUM(json_array_length(dependencies)), 0) AS dependencies
          FROM effect_local_changes WHERE peer_id = ${peerId} AND applied = 0`
    })
    const findPeerPendingReceiptTotals = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId
      }),
      Result: TotalsRow,
      execute: (request) =>
        sql`SELECT COALESCE(SUM(LENGTH(pending_message)), 0) AS bytes, COUNT(pending_message) AS count
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND pending_message IS NOT NULL`
    })
    const findReplicaPendingChangeTotals = SqlSchema.findAll({
      Request: Schema.Void,
      Result: PendingTotalsRow,
      execute: () =>
        sql`SELECT COALESCE(SUM(LENGTH(bytes)), 0) AS bytes, COUNT(*) AS count,
          COALESCE(SUM(json_array_length(dependencies)), 0) AS dependencies
          FROM effect_local_changes WHERE applied = 0`
    })
    const findReplicaPendingReceiptTotals = SqlSchema.findAll({
      Request: Identity.ReplicaIncarnation,
      Result: TotalsRow,
      execute: (replicaIncarnation) =>
        sql`SELECT COALESCE(SUM(LENGTH(pending_message)), 0) AS bytes, COUNT(pending_message) AS count
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${replicaIncarnation}
            AND pending_message IS NOT NULL`
    })
    const markOutboxSent = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId,
        connectionEpoch: Schema.String,
        sendSequence: Schema.Number,
        messageHash: Schema.String
      }),
      Result: MarkedOutboxRow,
      execute: (request) =>
        sql`UPDATE effect_local_peer_outbox
          SET status = 'Sent'
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND send_sequence = ${request.sendSequence}
            AND message_hash = ${request.messageHash}
            AND status = 'Pending'
          RETURNING receipt_reply_id, send_sequence`
    })
    const markReceiptReplySent = SqlSchema.findAll({
      Request: Schema.Int,
      Result: MarkedReceiptReplyRow,
      execute: (rowId) =>
        sql`UPDATE effect_local_peer_receipt_replies
          SET status = 'Sent'
          WHERE row_id = ${rowId}
          RETURNING receipt_row_id`
    })
    const pruneRelayReceiptsInTransaction = (
      replicaIncarnation: Identity.ReplicaIncarnation,
      expiresAt: string
    ) =>
      relayReceiptLimits === null
        ? Effect.succeed(0)
        : Effect.gen(function*() {
          const rows = yield* findRelayReceiptsToPrune({
            expiresAt,
            limit: relayReceiptLimits.pruneBatchSize,
            replicaIncarnation
          })
          const usage = new Map<string, {
            readonly encodedBytes: number
            readonly receiptCount: number
            readonly senderPeerId: Identity.PeerId
            readonly senderSubjectId: string
            readonly senderTenantId: string
          }>()
          for (const row of rows) {
            const key = JSON.stringify([row.sender_tenant_id, row.sender_subject_id, row.sender_peer_id])
            const current = usage.get(key)
            usage.set(key, {
              encodedBytes: (current?.encodedBytes ?? 0) + row.encoded_size,
              receiptCount: (current?.receiptCount ?? 0) + 1,
              senderPeerId: row.sender_peer_id,
              senderSubjectId: row.sender_subject_id,
              senderTenantId: row.sender_tenant_id
            })
          }
          for (const entry of usage.values()) {
            const updated = yield* decrementRelayReceiptUsage({
              ...entry,
              replicaIncarnation
            })
            if (updated.length !== 1) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause: new Error("Relay receipt usage is inconsistent")
                })
              })
            }
          }
          for (const row of rows) {
            yield* sql`INSERT INTO effect_local_peer_relay_receipt_delete_tokens (receipt_row_id)
              VALUES (${row.row_id})`
            const deleted = yield* deleteRelayReceipt({ rowId: row.row_id })
            if (deleted.length !== 1) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({
                  cause: new Error("Relay receipt disappeared during pruning")
                })
              })
            }
          }
          yield* sql`DELETE FROM effect_local_peer_receipt_replies AS reply
            WHERE receipt_row_id IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM effect_local_peer_outbox AS outbox
                WHERE outbox.receipt_reply_id = reply.row_id AND outbox.status = 'Pending'
              )`
          yield* sql`DELETE FROM effect_local_peer_relay_receipt_usage
            WHERE replica_incarnation = ${replicaIncarnation}
              AND receipt_count = 0
              AND encoded_bytes = 0`
          return rows.length
        })
    yield* sql.withTransaction(Effect.gen(function*() {
      yield* sql`INSERT INTO effect_local_quarantine (document_id, peer_id, reason, bytes, created_at)
        SELECT document_id, peer_id, 'Expired pending sync change', bytes, ${startupAt}
        FROM effect_local_changes
        WHERE applied = 0 AND accepted_at < ${startupCutoff}`
      yield* sql`DELETE FROM effect_local_changes
        WHERE applied = 0 AND accepted_at < ${startupCutoff}`
      yield* sql`INSERT INTO effect_local_quarantine (document_id, peer_id, reason, bytes, created_at)
        SELECT document_id, peer_id, 'Expired pending sync message', pending_message, ${startupAt}
        FROM effect_local_peer_receipts
        WHERE replica_incarnation = ${bootstrap.incarnation}
          AND relay_message_id IS NULL
          AND pending_message IS NOT NULL
          AND accepted_at < ${startupCutoff}`
      yield* sql`INSERT INTO effect_local_quarantine (document_id, peer_id, reason, bytes, created_at)
        SELECT document_id, peer_id, 'Expired pending sync outbox', message, ${startupAt}
        FROM effect_local_peer_outbox
        WHERE replica_incarnation = ${bootstrap.incarnation}
          AND status = 'Pending'
          AND created_at < ${startupCutoff}`
      yield* sql`DELETE FROM effect_local_peer_outbox
        WHERE replica_incarnation != ${bootstrap.incarnation} OR created_at < ${startupCutoff}`
      yield* sql`DELETE FROM effect_local_peer_receipts
        WHERE relay_message_id IS NULL
          AND (replica_incarnation != ${bootstrap.incarnation} OR accepted_at < ${startupCutoff})`
      yield* sql`DELETE FROM effect_local_peer_receipt_replies AS reply
        WHERE receipt_row_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM effect_local_peer_outbox AS outbox
            WHERE outbox.receipt_reply_id = reply.row_id AND outbox.status = 'Pending'
          )`
      if (relayReceiptLimits !== null) {
        yield* pruneRelayReceiptsInTransaction(bootstrap.incarnation, startupAt)
      }
    })).pipe(
      Effect.catchTags({
        SchemaError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause })
            })
          ),
        SqlError: (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({ cause })
            })
          )
      })
    )

    const readState = (session: Session, documentId: Identity.DocumentId) =>
      Ref.get(states).pipe(
        Effect.map((current) => current.get(syncStateKey(session, documentId)) ?? Automerge.initSyncState())
      )

    const writeState = (session: Session, documentId: Identity.DocumentId, state: Automerge.SyncState) =>
      Ref.update(states, (current) => {
        const next = new Map(current)
        next.set(syncStateKey(session, documentId), state)
        return next
      })

    const removeState = (session: Session) =>
      Ref.update(states, (current) => {
        const prefix = `${sessionKey(session)}:`
        return new Map([...current].filter(([key]) => !key.startsWith(prefix)))
      })

    const removeDocumentState = (documentId: Identity.DocumentId) =>
      Ref.update(states, (current) => {
        const suffix = `:${documentId}`
        return new Map([...current].filter(([key]) => !key.endsWith(suffix)))
      })

    const withStateLock = <A, E, R,>(
      documentId: Identity.DocumentId,
      effect: Effect.Effect<A, E, R>
    ) =>
      RcMap.get(documentLocks, documentId).pipe(
        Effect.mapError(() =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.QuotaExceeded({
              resource: "in-flight sync documents",
              limit: limits.maxQueuedRpc
            })
          })
        ),
        Effect.flatMap((lock) => lock.withPermit(effect)),
        Effect.scoped
      )

    const validateSession = (permit: ReplicaGate.Permit, session: Session) =>
      Effect.gen(function*() {
        if (permit.incarnation !== session.replicaIncarnation) {
          yield* removeState(session)
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.ProtocolMismatch({
              expected: String(permit.incarnation),
              observed: String(session.replicaIncarnation)
            })
          })
        }
      })

    const withSessionGeneration = <A, E, R,>(
      session: Session,
      use: (generation: Ref.Ref<number>) => Effect.Effect<A, E, R>
    ) =>
      RcMap.get(sessionGenerations, sessionKey(session)).pipe(
        Effect.mapError(() =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.QuotaExceeded({
              resource: "in-flight peer sessions",
              limit: limits.maxQueuedRpc
            })
          })
        ),
        Effect.flatMap(use),
        Effect.scoped
      )

    const validateSessionGeneration = (generation: Ref.Ref<number>, expected: number) =>
      Ref.get(generation).pipe(
        Effect.flatMap((current) =>
          current === expected ? Effect.void : Effect.fail(
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: `session generation ${current}`,
                observed: `session generation ${expected}`
              })
            })
          )
        )
      )

    const expirePending = (
      session: Session,
      documentId: Identity.DocumentId,
      now: string,
      cutoff: string
    ) =>
      sql.withTransaction(Effect.gen(function*() {
        yield* sql`INSERT INTO effect_local_quarantine (document_id, peer_id, reason, bytes, created_at)
          SELECT document_id, peer_id, 'Expired pending sync change', bytes, ${now}
          FROM effect_local_changes
          WHERE document_id = ${documentId} AND applied = 0 AND accepted_at < ${cutoff}`
        yield* sql`DELETE FROM effect_local_changes
          WHERE document_id = ${documentId} AND applied = 0 AND accepted_at < ${cutoff}`
        yield* sql`INSERT INTO effect_local_quarantine (document_id, peer_id, reason, bytes, created_at)
          SELECT document_id, peer_id, 'Expired pending sync message', pending_message, ${now}
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${session.replicaIncarnation}
            AND document_id = ${documentId}
            AND relay_message_id IS NULL
            AND pending_message IS NOT NULL
            AND accepted_at < ${cutoff}`
        yield* sql`DELETE FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${session.replicaIncarnation}
            AND document_id = ${documentId}
            AND relay_message_id IS NULL
            AND pending_message IS NOT NULL
            AND accepted_at < ${cutoff}`
        yield* sql`DELETE FROM effect_local_peer_receipt_replies AS reply
          WHERE receipt_row_id IS NULL
            AND NOT EXISTS (
              SELECT 1 FROM effect_local_peer_outbox AS outbox
              WHERE outbox.receipt_reply_id = reply.row_id AND outbox.status = 'Pending'
            )`
      }))

    // Dominated by `gate.validate` on every path that reaches them, so these are defensive: they keep
    // the one-condition-one-answer invariant if the statement order ever changes.
    const nextSequence = incrementCommitSequence(undefined).pipe(Effect.flatMap((rows) =>
      rows[0] === undefined
        ? Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ReplicaMetadataMissing({ operation: "PeerSync.nextSequence" })
          })
        )
        : Effect.succeed(Identity.CommitSequence.make(rows[0].commit_sequence))
    ))

    const currentSequence = findCommitSequence(undefined).pipe(Effect.flatMap((rows) =>
      rows[0] === undefined
        ? Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.ReplicaMetadataMissing({ operation: "PeerSync.currentSequence" })
          })
        )
        : Effect.succeed(Identity.CommitSequence.make(rows[0].commit_sequence))
    ))

    const decodeWriterProvenanceHashes = (message: Uint8Array) =>
      Effect.gen(function*() {
        const changes = yield* Effect.try({
          try: () => SyncChunks.decodeSyncChanges(Automerge.decodeSyncMessage(message).changes),
          catch: (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause })
            })
        })
        if (changes.length > limits.maxSyncChangesPerMessage) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.QuotaExceeded({
              resource: "sync message writer provenance",
              limit: limits.maxSyncChangesPerMessage
            })
          })
        }
        return changes.map((change) => change.hash)
      })

    const loadWriterProvenanceBatch = (
      documentId: Identity.DocumentId,
      messages: ReadonlyArray<Uint8Array>
    ) =>
      Effect.gen(function*() {
        const messageHashes = yield* Effect.forEach(messages, decodeWriterProvenanceHashes)
        const uniqueHashes = [...new Set(messageHashes.flat())]
        if (uniqueHashes.length === 0) return messages.map(() => [])
        const [rowGroups, checkpoints] = yield* Effect.all([
          Effect.forEach(Arr.chunksOf(uniqueHashes, 500), findChangeProvenance),
          findCheckpointProvenance(documentId)
        ]).pipe(
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
        return yield* Effect.try({
          try: () =>
            messageHashes.map((hashes) =>
              WriterProvenance.resolve(
                hashes,
                [
                  ...rowGroups.flat().map((row) => ({
                    changeHash: row.change_hash,
                    writerSchemaVersion: row.writer_schema_version,
                    writerDefinitionHash: row.writer_definition_hash
                  })),
                  ...checkpoints.flatMap((checkpoint) => WriterProvenance.exactEntries(checkpoint.writer_provenance))
                ]
              )
            ),
          catch: (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause })
            })
        })
      })

    const loadWriterProvenance = (documentId: Identity.DocumentId, message: Uint8Array) =>
      loadWriterProvenanceBatch(documentId, [message]).pipe(
        Effect.map((provenance) => provenance[0]!)
      )

    const persistOutbound = (
      session: Session,
      documentId: Identity.DocumentId,
      message: Uint8Array,
      heads: ReadonlyArray<string>,
      checkpointTransfer?: Uint8Array
    ) =>
      Effect.gen(function*() {
        const writerProvenance = checkpointTransfer === undefined
          ? yield* loadWriterProvenance(documentId, message)
          : []
        // Read here and stored on the row, never re-read when the row is finally sent. This is the
        // generation time the message describes: a message queued before a rewrite must stay
        // labelled with the lineage it was generated from, or the peer would apply pre-rewrite
        // history under the post-rewrite label.
        const lineage = yield* documentLineage(documentId)
        const totals = yield* findOutboxTotals({
          replicaIncarnation: session.replicaIncarnation,
          peerId: session.peerId,
          connectionEpoch: session.connectionEpoch
        })
        if (
          (totals[0]?.bytes ?? 0) + message.byteLength + (checkpointTransfer?.byteLength ?? 0) >
            limits.maxPendingBytesPerPeer
        ) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.QuotaExceeded({
              resource: "peer sync outbox bytes",
              limit: limits.maxPendingBytesPerPeer
            })
          })
        }
        if ((totals[0]?.count ?? 0) >= limits.maxPendingChangesPerPeer) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.QuotaExceeded({
              resource: "peer sync outbox messages",
              limit: limits.maxPendingChangesPerPeer
            })
          })
        }
        const rows = yield* findNextOutboxSequence({
          replicaIncarnation: session.replicaIncarnation,
          peerId: session.peerId,
          connectionEpoch: session.connectionEpoch
        })
        const sendSequence = rows[0]?.sequence ?? 0
        const messageHash = yield* digest(message)
        const createdAt = new Date(yield* Clock.currentTimeMillis).toISOString()
        yield* sql`INSERT INTO effect_local_peer_outbox (
          replica_incarnation, peer_id, connection_epoch, document_id, send_sequence,
          message, message_hash, heads, status, created_at, writer_provenance, lineage, checkpoint_transfer
        ) VALUES (
          ${session.replicaIncarnation}, ${session.peerId}, ${session.connectionEpoch}, ${documentId}, ${sendSequence},
          ${message}, ${messageHash}, ${Schema.encodeSync(Heads)(heads)}, 'Pending', ${createdAt},
          ${Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(writerProvenance)}, ${lineage},
          ${checkpointTransfer ?? null}
        )`
        return {
          sendSequence,
          documentId,
          message,
          messageHash,
          heads,
          lineage,
          writerProvenance,
          ...(checkpointTransfer === undefined ? {} : { checkpointTransfer })
        } satisfies Outbound
      })

    const enqueue = (session: Session, reply: Reply) =>
      Effect.scoped(Effect.gen(function*() {
        const permit = yield* gate.shared
        yield* validateSession(permit, session)
        // The cluster's ApplySync handler holds the client's only transaction permit when it takes
        // quotaLock, so holding quotaLock while touching the database deadlocks the worker. One
        // order everywhere: gate, then transaction permit, then quotaLock.
        return yield* sql.withTransaction(quotaLock.withPermit(Effect.gen(function*() {
          const messageHash = yield* digest(reply.message)
          if (messageHash !== reply.messageHash) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.ProtocolMismatch({
                expected: messageHash,
                observed: reply.messageHash
              })
            })
          }
          if (reply.fragments === undefined) {
            const rows = yield* findOutboxReply({
              replicaIncarnation: session.replicaIncarnation,
              peerId: session.peerId,
              connectionEpoch: session.connectionEpoch,
              documentId: reply.documentId,
              messageHash: reply.messageHash
            })
            const existing = rows[0]
            if (existing !== undefined) {
              if (!Equal.equals(existing.checkpoint_transfer, reply.checkpointTransfer ?? null)) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ProtocolMismatch({
                    expected: "matching queued checkpoint transfer",
                    observed: "conflicting queued checkpoint transfer"
                  })
                })
              }
              return {
                sendSequence: existing.send_sequence,
                documentId: reply.documentId,
                message: existing.message,
                messageHash: existing.message_hash,
                heads: existing.heads,
                lineage: existing.lineage,
                writerProvenance: existing.writer_provenance,
                ...(existing.checkpoint_transfer === null
                  ? {}
                  : { checkpointTransfer: existing.checkpoint_transfer }),
                ...(existing.receipt_reply_id === null ? {} : { receiptReplyId: existing.receipt_reply_id })
              }
            }
            return yield* persistOutbound(
              session,
              reply.documentId,
              reply.message,
              reply.heads,
              reply.checkpointTransfer
            )
          }

          const fragments = [...reply.fragments].toSorted((left, right) => left.replyIndex - right.replyIndex)
          if (
            fragments.length === 0 ||
            fragments[0]!.messageHash !== reply.messageHash ||
            !Equal.equals(fragments[0]!.message, reply.message) ||
            fragments.some((fragment, index) => fragment.replyIndex !== index)
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause: new Error("Invalid receipt reply batch") })
            })
          }
          const stored = yield* findReceiptRepliesById(fragments.map((fragment) => fragment.receiptReplyId))
          if (stored.length !== fragments.length) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause: new Error("Missing receipt reply") })
            })
          }
          for (let index = 0; index < fragments.length; index++) {
            const fragment = fragments[index]!
            const row = stored[index]!
            const observedHash = yield* digest(fragment.message)
            if (
              row.row_id !== fragment.receiptReplyId || row.reply_index !== fragment.replyIndex ||
              row.message_hash !== fragment.messageHash || observedHash !== fragment.messageHash ||
              !Equal.equals(row.message, fragment.message) || !Equal.equals(row.heads, fragment.heads)
            ) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause: new Error("Conflicting receipt reply") })
              })
            }
          }
          const pendingRows = stored.filter((row) => row.status === "Pending")
          if (pendingRows.length === 0) return null
          const existing = yield* findOutboxReceiptReplies({
            replicaIncarnation: session.replicaIncarnation,
            peerId: session.peerId,
            connectionEpoch: session.connectionEpoch,
            receiptReplyIds: pendingRows.map((row) => row.row_id)
          })
          if (existing.length !== 0 && existing.length !== pendingRows.length) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause: new Error("Partial receipt reply outbox batch") })
            })
          }
          if (existing.length !== 0) {
            for (let index = 0; index < existing.length; index++) {
              const row = existing[index]!
              const expected = pendingRows[index]!
              if (
                row.receipt_reply_id !== expected.row_id || row.message_hash !== expected.message_hash ||
                !Equal.equals(row.message, expected.message)
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({ cause: new Error("Conflicting receipt reply outbox") })
                })
              }
            }
            const first = existing[0]!
            return {
              sendSequence: first.send_sequence,
              documentId: reply.documentId,
              message: first.message,
              messageHash: first.message_hash,
              heads: first.heads,
              lineage: first.lineage,
              writerProvenance: first.writer_provenance,
              receiptReplyId: first.receipt_reply_id!
            }
          }
          const totals = (yield* findOutboxTotals({
            replicaIncarnation: session.replicaIncarnation,
            peerId: session.peerId,
            connectionEpoch: session.connectionEpoch
          }))[0]
          const batchBytes = pendingRows.reduce((total, row) => total + row.message.byteLength, 0)
          if ((totals?.bytes ?? 0) + batchBytes > limits.maxPendingBytesPerPeer) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.QuotaExceeded({
                resource: "peer sync outbox bytes",
                limit: limits.maxPendingBytesPerPeer
              })
            })
          }
          if ((totals?.count ?? 0) + pendingRows.length > limits.maxPendingChangesPerPeer) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.QuotaExceeded({
                resource: "peer sync outbox messages",
                limit: limits.maxPendingChangesPerPeer
              })
            })
          }
          const sendSequence = (yield* findNextOutboxSequence({
            replicaIncarnation: session.replicaIncarnation,
            peerId: session.peerId,
            connectionEpoch: session.connectionEpoch
          }))[0]?.sequence ?? 0
          const lineage = yield* documentLineage(reply.documentId)
          const createdAt = new Date(yield* Clock.currentTimeMillis).toISOString()
          const provenances = yield* loadWriterProvenanceBatch(
            reply.documentId,
            pendingRows.map((row) => row.message)
          )
          const outbounds = pendingRows.map((row, index) => {
            const writerProvenance = provenances[index]!
            return {
              outbound: {
                sendSequence: sendSequence + index,
                documentId: reply.documentId,
                message: row.message,
                messageHash: row.message_hash,
                heads: row.heads,
                lineage,
                writerProvenance,
                receiptReplyId: row.row_id
              } satisfies Outbound,
              record: {
                replica_incarnation: session.replicaIncarnation,
                peer_id: session.peerId,
                connection_epoch: session.connectionEpoch,
                document_id: reply.documentId,
                send_sequence: sendSequence + index,
                message: row.message,
                message_hash: row.message_hash,
                heads: Schema.encodeSync(Heads)(row.heads),
                status: "Pending",
                created_at: createdAt,
                writer_provenance: Schema.encodeSync(
                  WriterProvenance.StoredChangeProvenances
                )(writerProvenance),
                lineage,
                receipt_reply_id: row.row_id
              }
            }
          })
          yield* sql`INSERT INTO effect_local_peer_outbox ${sql.insert(outbounds.map((item) => item.record))}`
          return outbounds[0]?.outbound ?? null
        }))).pipe(
          Effect.catchTags({
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({ cause })
                })
              ),
            SqlError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({ cause })
                })
              )
          })
        )
      }))

    const generate = <D extends Document.Any,>(
      document: D,
      documentId: Identity.DocumentId,
      session: Session,
      peer: { readonly lineageAware: boolean; readonly checkpointTransfer?: boolean }
    ) =>
      withSessionGeneration(session, (generation) =>
        Effect.scoped(Effect.gen(function*() {
          // Claims and history rewrites take the gate before any document lock. Keep the same order
          // here so a queued exclusive claim cannot leave generation holding the document lock while
          // it waits for a gate permit that the rewrite cannot release until it acquires that lock.
          const permit = yield* gate.shared
          yield* validateSession(permit, session)
          return yield* withStateLock(
            documentId,
            Effect.gen(function*() {
              // The one direction the receive side refusal cannot cover. A peer that does not compare
              // lineage merges whatever it is handed, so handing it a rewritten document makes it
              // resurrect the discarded history and push it back here as its own reply. Fail locally
              // instead of emitting: the peer is not at fault and has nothing to reject.
              const lineage = yield* documentLineage(documentId)
              if (lineage !== Identity.genesisLineage && !peer.lineageAware) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.DocumentLineageChanged({
                    documentId,
                    localLineage: lineage,
                    remoteLineage: Identity.genesisLineage
                  })
                })
              }
              const sessionGeneration = yield* Ref.get(generation)
              const existing = yield* findPendingOutboxCount({
                replicaIncarnation: session.replicaIncarnation,
                peerId: session.peerId,
                connectionEpoch: session.connectionEpoch,
                documentId
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
                })
              )
              if ((existing[0]?.count ?? 0) > 0) {
                return { outbound: null, observedByPeer: false, dirty: true }
              }
              return yield* Effect.acquireUseRelease(
                store.load(document, documentId),
                (durable) =>
                  Effect.gen(function*() {
                    const state = yield* readState(session, documentId)
                    const generated = yield* Effect.try({
                      try: () => Automerge.generateSyncMessage(durable.automerge, state),
                      catch: (cause) =>
                        new ReplicaError.ReplicaError({
                          reason: new ReplicaError.ProtocolMismatch({
                            expected: "valid local Automerge sync state",
                            observed: String(cause)
                          })
                        })
                    })
                    const observedByPeer = Automerge.hasOurChanges(durable.automerge, generated[0])
                    if (generated[1] === null) {
                      yield* quotaLock.withPermit(
                        validateSessionGeneration(generation, sessionGeneration).pipe(
                          Effect.andThen(writeState(session, documentId, generated[0]))
                        )
                      )
                      return { outbound: null, observedByPeer, dirty: false }
                    }
                    const generatedChangeCount = yield* Effect.try({
                      try: () =>
                        SyncChunks.decodeSyncChanges(Automerge.decodeSyncMessage(generated[1]!).changes).length,
                      catch: (cause) =>
                        new ReplicaError.ReplicaError({
                          reason: new ReplicaError.ProtocolMismatch({
                            expected: "valid local Automerge sync message",
                            observed: String(cause)
                          })
                        })
                    })
                    const oversizedBytes = generated[1].byteLength > limits.maxSyncMessageBytes
                    const oversizedChanges = generatedChangeCount > limits.maxSyncChangesPerMessage
                    // The first v2 cold-sync frame is only an announcement. Its follow-up carries
                    // the history after the peer answers, so use the snapshot's decoded count while
                    // there are no shared heads or the quota failure would be deferred until a reply
                    // path that has no capability negotiation input.
                    const proactiveColdCheckpoint = peer.checkpointTransfer === true &&
                      state.sharedHeads.length === 0 &&
                      durable.historyChanges !== null &&
                      durable.historyChanges > limits.maxSyncChangesPerMessage
                    if ((oversizedBytes || oversizedChanges) && !peer.checkpointTransfer) {
                      return yield* new ReplicaError.ReplicaError({
                        reason: new ReplicaError.QuotaExceeded({
                          resource: oversizedBytes ? "sync message bytes" : "sync message writer provenance",
                          limit: oversizedBytes
                            ? limits.maxSyncMessageBytes
                            : limits.maxSyncChangesPerMessage
                        })
                      })
                    }
                    let checkpointTransfer: Uint8Array | undefined
                    let outboundMessage = generated[1]
                    if (oversizedBytes || oversizedChanges || proactiveColdCheckpoint) {
                      const snapshot = InternalAutomerge.save(durable.automerge)
                      if (Automerge.getMissingDeps(durable.automerge, []).length !== 0) {
                        return yield* new ReplicaError.ReplicaError({
                          reason: new ReplicaError.StorageCorrupt({
                            cause: new Error("Cannot transfer an incomplete checkpoint snapshot")
                          })
                        })
                      }
                      const checkpointHash = yield* digest({ documentId, bytes: snapshot })
                      const [definition, retained] = yield* Effect.all([
                        findDefinitionHash(undefined),
                        findGenerationLineageTransitions({ documentId, lineage })
                      ]).pipe(
                        Effect.catchTags({
                          NoSuchElementError: () =>
                            Effect.fail(
                              new ReplicaError.ReplicaError({
                                reason: new ReplicaError.ReplicaMetadataMissing({
                                  operation: "PeerSync.generate checkpoint"
                                })
                              })
                            ),
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
                      if (retained.length > PeerSyncEnvelope.maximumCheckpointTransitions) {
                        return yield* new ReplicaError.ReplicaError({
                          reason: new ReplicaError.QuotaExceeded({
                            resource: "checkpoint lineage transitions",
                            limit: PeerSyncEnvelope.maximumCheckpointTransitions
                          })
                        })
                      }
                      const definitionHash = definition.definition_hash
                      const baseHeads = state.sharedHeads
                      const base = baseHeads.length === 0
                        ? { _tag: "Bootstrap" as const }
                        : { _tag: "Heads" as const, baseHeads }
                      const manifestClaims = CheckpointAuthority.ManifestClaims.make({
                        purpose: CheckpointAuthority.manifestPurpose,
                        documentId,
                        lineage,
                        checkpointHash,
                        heads: durable.materializedHeads,
                        base,
                        schemaVersion: durable.snapshot.version,
                        writerDefinitionHash: definitionHash
                      })
                      const authorization = yield* checkpointAuthority.signManifest(manifestClaims)
                      if (Option.isNone(authorization)) {
                        if (oversizedBytes || oversizedChanges) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.QuotaExceeded({
                              resource: oversizedBytes ? "sync message bytes" : "sync message writer provenance",
                              limit: oversizedBytes
                                ? limits.maxSyncMessageBytes
                                : limits.maxSyncChangesPerMessage
                            })
                          })
                        }
                      } else {
                        const byResultingLineage = new Map(retained.map((row) => [row.lineage, row]))
                        const reversed: Array<typeof LineageTransitionRow.Type> = []
                        let cursor = lineage
                        const seen = new Set<Identity.DocumentLineage>()
                        while (cursor !== Identity.genesisLineage) {
                          if (seen.has(cursor)) {
                            return yield* new ReplicaError.ReplicaError({
                              reason: new ReplicaError.StorageCorrupt({
                                cause: new Error("Lineage transition chain contains a cycle")
                              })
                            })
                          }
                          seen.add(cursor)
                          const transition = byResultingLineage.get(cursor)
                          if (transition === undefined) break
                          if (transition.authorization === null) break
                          reversed.push(transition)
                          cursor = transition.prior_lineage
                        }
                        checkpointTransfer = yield* PeerSyncEnvelope.encodeCheckpointTransfer({
                          snapshot,
                          manifest: { ...manifestClaims, authorization: authorization.value },
                          transitions: reversed.toReversed().map((transition) => ({
                            purpose: CheckpointAuthority.transitionPurpose,
                            documentId,
                            priorLineage: transition.prior_lineage,
                            priorCheckpointHash: transition.prior_checkpoint_hash,
                            priorHeads: transition.prior_heads,
                            priorSnapshot: transition.prior_snapshot,
                            resultingLineage: transition.lineage,
                            anchorCheckpointHash: transition.checkpoint_hash,
                            resultingHeads: transition.heads,
                            schemaVersion: transition.schema_version,
                            writerDefinitionHash: transition.writer_definition_hash,
                            authorization: transition.authorization!
                          }))
                        }, limits.maxSyncMessageBytes)
                        outboundMessage = new Uint8Array()
                      }
                    }
                    const outbound = yield* sql.withTransaction(quotaLock.withPermit(Effect.gen(function*() {
                      yield* validateSessionGeneration(generation, sessionGeneration)
                      const existing = yield* findPendingOutboxCount({
                        replicaIncarnation: session.replicaIncarnation,
                        peerId: session.peerId,
                        connectionEpoch: session.connectionEpoch,
                        documentId
                      })
                      if ((existing[0]?.count ?? 0) > 0) return null
                      const outbound = yield* persistOutbound(
                        session,
                        documentId,
                        outboundMessage!,
                        durable.materializedHeads,
                        checkpointTransfer
                      )
                      yield* writeState(
                        session,
                        documentId,
                        checkpointTransfer === undefined
                          ? generated[0]
                          : syncStateAtHeads(
                            durable.materializedHeads,
                            WriterProvenance.changeHashes(durable.automerge)
                          )
                      )
                      return outbound
                    }))).pipe(
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
                    return outbound === null
                      ? { outbound: null, observedByPeer: false, dirty: true }
                      : { outbound, observedByPeer, dirty: false }
                  }),
                (durable) => Effect.sync(() => InternalAutomerge.free(durable.automerge))
              )
            })
          )
        })))

    const installCheckpoint = <D extends Document.Any,>(
      document: D,
      documentId: Identity.DocumentId,
      session: Session,
      receiptSession: Session,
      receiveSequence: number,
      remoteLineage: Identity.DocumentLineage,
      checkpointTransferBytes: Uint8Array,
      messageHash: string,
      acceptedAt: string,
      relay: RelayReceipt | undefined,
      generation: Ref.Ref<number>,
      sessionGeneration: number,
      permit: ReplicaGate.Permit
    ) =>
      Effect.acquireUseRelease(
        Effect.gen(function*() {
          const transfer = yield* PeerSyncEnvelope.decodeCheckpointTransfer(
            checkpointTransferBytes,
            limits.maxSyncMessageBytes
          )
          const manifest = transfer.manifest
          if (
            manifest.documentId !== documentId || manifest.lineage !== remoteLineage ||
            manifest.schemaVersion !== document.version
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.CheckpointRejected({
                documentId,
                reason: "Checkpoint manifest does not match the requested document"
              })
            })
          }
          const definitionHash = (yield* findDefinitionHash(undefined)).definition_hash
          if (manifest.writerDefinitionHash !== definitionHash) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.CheckpointRejected({
                documentId,
                reason: "Checkpoint definition does not match this replica"
              })
            })
          }
          const checkpointHash = yield* digest({ documentId, bytes: transfer.snapshot })
          if (checkpointHash !== manifest.checkpointHash) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.CheckpointRejected({
                documentId,
                reason: "Checkpoint snapshot hash does not match its manifest"
              })
            })
          }
          yield* checkpointAuthority.verifyManifest(manifest, manifest.authorization)
          const priorChangeHashes = new Map<Identity.DocumentLineage, ReadonlySet<string>>()
          let preceding: PeerSyncEnvelope.CheckpointTransfer["transitions"][number] | undefined
          for (const transition of transfer.transitions) {
            if (
              transition.documentId !== documentId ||
              transition.schemaVersion !== manifest.schemaVersion ||
              transition.writerDefinitionHash !== manifest.writerDefinitionHash ||
              (preceding !== undefined &&
                (
                  preceding.resultingLineage !== transition.priorLineage ||
                  !sameHeads(preceding.resultingHeads, transition.priorHeads)
                ))
            ) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.CheckpointRejected({
                  documentId,
                  reason: "Checkpoint lineage transition claims are discontinuous"
                })
              })
            }
            yield* checkpointAuthority.verifyTransition(transition, transition.authorization)
            const priorHash = yield* digest({ documentId, bytes: transition.priorSnapshot })
            if (priorHash !== transition.priorCheckpointHash) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.CheckpointRejected({
                  documentId,
                  reason: "Checkpoint lineage transition prior snapshot hash is invalid"
                })
              })
            }
            const prior = yield* Effect.try({
              try: () => Automerge.load(transition.priorSnapshot),
              catch: () =>
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CheckpointRejected({
                    documentId,
                    reason: "Checkpoint lineage transition prior snapshot is invalid"
                  })
                })
            })
            const validPrior = sameHeads(Automerge.getHeads(prior), transition.priorHeads) &&
              Automerge.getMissingDeps(prior, []).length === 0
            priorChangeHashes.set(
              transition.resultingLineage,
              new Set(WriterProvenance.changeHashes(prior))
            )
            Automerge.free(prior)
            if (!validPrior) {
              return yield* new ReplicaError.ReplicaError({
                reason: new ReplicaError.CheckpointRejected({
                  documentId,
                  reason: "Checkpoint lineage transition prior snapshot heads are invalid"
                })
              })
            }
            preceding = transition
          }
          const automerge = yield* Effect.try({
            try: () => Automerge.load<InternalAutomerge.Root<D["schema"]["Encoded"]>>(transfer.snapshot),
            catch: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.CheckpointRejected({
                  documentId,
                  reason: `Checkpoint snapshot cannot be decoded: ${String(cause)}`
                })
              })
          })
          const heads = InternalAutomerge.heads(automerge)
          if (!sameHeads(heads, manifest.heads) || Automerge.getMissingDeps(automerge, []).length !== 0) {
            InternalAutomerge.free(automerge)
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.CheckpointRejected({
                documentId,
                reason: "Checkpoint snapshot heads are incomplete or do not match its manifest"
              })
            })
          }
          const encoded = InternalAutomerge.value(automerge)
          const value = yield* Document.decodeStored(
            document,
            documentId,
            manifest.schemaVersion,
            encoded
          ).pipe(
            Effect.mapError(() =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.CheckpointRejected({
                  documentId,
                  reason: "Checkpoint snapshot does not materialize as the requested document"
                })
              })
            ),
            Effect.onError(() => Effect.sync(() => InternalAutomerge.free(automerge)))
          )
          return { automerge, heads, manifest, priorChangeHashes, transfer, value }
        }),
        (prepared) =>
          Effect.gen(function*() {
            const { automerge, heads, manifest, priorChangeHashes, transfer, value } = prepared
            const existing = Option.getOrUndefined(yield* findCheckpointDocument(documentId))
            if (existing === undefined) {
              if (manifest.base._tag !== "Bootstrap") {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CheckpointRejected({
                    documentId,
                    reason: "A missing document requires a bootstrap checkpoint"
                  })
                })
              }
            } else if (existing.lineage === manifest.lineage) {
              if (
                (manifest.base._tag === "Heads" &&
                  !sameHeads(manifest.base.baseHeads, existing.materialized_heads)) ||
                !Automerge.hasHeads(automerge, [...existing.materialized_heads])
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CheckpointRejected({
                    documentId,
                    reason: "Checkpoint does not extend the current document heads"
                  })
                })
              }
            } else {
              const start = transfer.transitions.findIndex((transition) => transition.priorLineage === existing.lineage)
              if (start < 0) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CheckpointRejected({
                    documentId,
                    reason: "Checkpoint has no transition from the current lineage"
                  })
                })
              }
              const chain = transfer.transitions.slice(start)
              let anchorHeads = existing.materialized_heads
              let anchorLineage = existing.lineage
              for (const [index, transition] of chain.entries()) {
                if (
                  transition.documentId !== documentId || transition.priorLineage !== anchorLineage ||
                  (index !== 0 && !sameHeads(transition.priorHeads, anchorHeads))
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.CheckpointRejected({
                      documentId,
                      reason: "Checkpoint lineage transition chain is discontinuous"
                    })
                  })
                }
                const priorHashes = priorChangeHashes.get(transition.resultingLineage)
                if (
                  priorHashes === undefined ||
                  (index === 0 && existing.materialized_heads.some((head) => !priorHashes.has(head)))
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.CheckpointRejected({
                      documentId,
                      reason: "Checkpoint lineage transition prior snapshot does not contain the local heads"
                    })
                  })
                }
                anchorLineage = transition.resultingLineage
                anchorHeads = transition.resultingHeads
              }
              if (
                anchorLineage !== manifest.lineage ||
                !Automerge.hasHeads(automerge, [...anchorHeads])
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CheckpointRejected({
                    documentId,
                    reason: "Checkpoint does not contain the final lineage anchor"
                  })
                })
              }
            }
            const checksum = yield* digest(transfer.snapshot)
            const history = yield* HistoryCounters.check(
              HistoryCounters.measureDecoded(InternalAutomerge.changesSince(automerge, [])),
              limits
            )
            const checkpointChangeHashes = WriterProvenance.changeHashes(automerge)
            return yield* sql.withTransaction(quotaLock.withPermit(Effect.gen(function*() {
              yield* validateSessionGeneration(generation, sessionGeneration)
              yield* gate.validate(permit)
              const current = Option.getOrUndefined(yield* findCheckpointDocument(documentId))
              if (
                (existing === undefined) !== (current === undefined) ||
                (existing !== undefined && current !== undefined &&
                  (
                    current.lineage !== existing.lineage ||
                    !sameHeads(current.materialized_heads, existing.materialized_heads) ||
                    !sameHeads(current.accepted_heads, existing.accepted_heads) ||
                    current.checkpoint_hash !== existing.checkpoint_hash
                  ))
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.CheckpointRejected({
                    documentId,
                    reason: "Document advanced while checkpoint installation was prepared"
                  })
                })
              }
              if (current !== undefined) {
                const counts = yield* checkpointInstallCounts({ documentId, now: acceptedAt })
                if (
                  !sameHeads(current.materialized_heads, current.accepted_heads) ||
                  counts.pending_changes !== 0 || counts.pending_receipts !== 0 ||
                  counts.direct_outbox !== 0 || counts.relay_outbox !== 0 ||
                  counts.unexpired_relay_receipts !== 0
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.CheckpointRejected({
                      documentId,
                      reason: "Cannot replace an incomplete or unsettled document"
                    })
                  })
                }
              }
              const commitSequence = (yield* incrementCommitSequence(undefined))[0]?.commit_sequence
              if (commitSequence === undefined) {
                return yield* new ReplicaError.ReplicaError({
                  reason: new ReplicaError.ReplicaMetadataMissing({
                    operation: "PeerSync.receive checkpoint commit"
                  })
                })
              }
              if (current === undefined) {
                yield* sql`INSERT INTO effect_local_documents (
                  document_id, document_type, schema_version, observed_versions, materialized_heads,
                  accepted_heads, tombstone, projection_status, checkpoint_hash, history_changes,
                  history_operations, history_bytes, lineage
                ) VALUES (
                  ${documentId}, ${document.name}, ${manifest.schemaVersion},
                  ${Schema.encodeSync(Versions)([manifest.schemaVersion])}, ${Schema.encodeSync(Heads)(heads)},
                  ${Schema.encodeSync(Heads)(heads)}, ${InternalAutomerge.tombstone(automerge) ? 1 : 0},
                  'Ready', ${manifest.checkpointHash}, ${history.changes}, ${history.operations},
                  ${history.bytes}, ${manifest.lineage}
                )`
              } else {
                yield* sql`UPDATE effect_local_documents SET
                  schema_version = ${manifest.schemaVersion},
                  observed_versions = ${Schema.encodeSync(Versions)([manifest.schemaVersion])},
                  materialized_heads = ${Schema.encodeSync(Heads)(heads)},
                  accepted_heads = ${Schema.encodeSync(Heads)(heads)},
                  tombstone = ${InternalAutomerge.tombstone(automerge) ? 1 : 0},
                  projection_status = 'Ready', checkpoint_hash = ${manifest.checkpointHash},
                  history_changes = ${history.changes}, history_operations = ${history.operations},
                  history_bytes = ${history.bytes}, lineage = ${manifest.lineage}
                  WHERE document_id = ${documentId}`
                yield* sql`DELETE FROM effect_local_changes WHERE document_id = ${documentId}`
                yield* sql`DELETE FROM effect_local_checkpoints WHERE document_id = ${documentId}`
                yield* sql`DELETE FROM effect_local_peer_outbox WHERE document_id = ${documentId}`
                yield* sql`DELETE FROM effect_local_peer_receipts
                  WHERE document_id = ${documentId} AND relay_message_id IS NULL`
              }
              const compactProvenance = WriterProvenance.CompactCheckpointProvenance.make({
                checkpointHash: manifest.checkpointHash,
                lineage: manifest.lineage,
                heads: manifest.heads,
                base: manifest.base,
                schemaVersion: manifest.schemaVersion,
                writerDefinitionHash: manifest.writerDefinitionHash,
                authorization: manifest.authorization
              })
              yield* sql`INSERT INTO effect_local_checkpoints (
                checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified,
                writer_provenance, lineage
              ) VALUES (
                ${manifest.checkpointHash}, ${documentId}, ${Schema.encodeSync(Heads)(heads)},
                ${transfer.snapshot}, ${checksum}, ${commitSequence}, 1,
                ${Schema.encodeSync(WriterProvenance.StoredCheckpointProvenance)(compactProvenance)},
                ${manifest.lineage}
              )`
              const storedTransitions = transfer.transitions.length === 0
                ? []
                : yield* findRelevantLineageTransitions({
                  documentId,
                  lineages: transfer.transitions.map((transition) => transition.resultingLineage),
                  priorLineages: transfer.transitions.map((transition) => transition.priorLineage)
                })
              for (const transition of transfer.transitions) {
                const conflict = storedTransitions.find((stored) =>
                  stored.lineage === transition.resultingLineage ||
                  stored.prior_lineage === transition.priorLineage
                )
                if (conflict !== undefined) {
                  if (
                    conflict.lineage !== transition.resultingLineage ||
                    conflict.prior_lineage !== transition.priorLineage ||
                    conflict.checkpoint_hash !== transition.anchorCheckpointHash ||
                    conflict.prior_checkpoint_hash !== transition.priorCheckpointHash ||
                    !sameHeads(conflict.heads, transition.resultingHeads) ||
                    !sameHeads(conflict.prior_heads, transition.priorHeads) ||
                    !Equal.equals(conflict.prior_snapshot, transition.priorSnapshot) ||
                    !Equal.equals(conflict.authorization, transition.authorization)
                  ) {
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.CheckpointRejected({
                        documentId,
                        reason: "Stored lineage transition conflicts with checkpoint transfer"
                      })
                    })
                  }
                  continue
                }
                yield* sql`INSERT INTO effect_local_lineage_transitions (
                  document_id, prior_lineage, prior_checkpoint_hash, prior_heads, prior_snapshot,
                  lineage, checkpoint_hash, heads, schema_version, writer_definition_hash,
                  authorization, created_at
                ) VALUES (
                  ${documentId}, ${transition.priorLineage}, ${transition.priorCheckpointHash},
                  ${Schema.encodeSync(Heads)(transition.priorHeads)}, ${transition.priorSnapshot},
                  ${transition.resultingLineage}, ${transition.anchorCheckpointHash},
                  ${Schema.encodeSync(Heads)(transition.resultingHeads)}, ${transition.schemaVersion},
                  ${transition.writerDefinitionHash}, ${transition.authorization}, ${acceptedAt}
                )`
              }
              yield* sql`INSERT INTO effect_local_commit_outbox (
                commit_sequence, document_id, invalidation_keys, published
              ) VALUES (
                ${commitSequence}, ${documentId},
                ${Schema.encodeSync(Heads)(ReplicaDefinition.documentCommitKeys(document.name, documentId))}, 0
              )`
              yield* projections.replaceDocument(
                document,
                {
                  documentId,
                  value,
                  version: manifest.schemaVersion,
                  heads,
                  tombstone: InternalAutomerge.tombstone(automerge),
                  projection: "Ready"
                },
                Identity.CommitSequence.make(commitSequence),
                "Fresh"
              )
              const encodedWriterProvenance = Schema.encodeSync(
                WriterProvenance.StoredChangeProvenances
              )([])
              const relayRetainedSize = relay === undefined
                ? null
                : relay.encodedSize + checkpointTransferBytes.byteLength +
                  new TextEncoder().encode(encodedWriterProvenance).byteLength
              yield* sql`INSERT INTO effect_local_peer_receipts (
                replica_incarnation, peer_id, connection_epoch, receive_sequence, document_id,
                message_hash, reply, reply_hash, pending_message, heads, accepted_heads,
                commit_sequence, accepted_at, writer_provenance, checkpoint_transfer,
                relay_sender_tenant_id, relay_sender_subject_id, relay_sender_peer_id,
                relay_message_id, relay_outer_envelope_digest, relay_receipt_expires_at,
                relay_encoded_size
              ) VALUES (
                ${receiptSession.replicaIncarnation}, ${receiptSession.peerId},
                ${receiptSession.connectionEpoch}, ${receiveSequence}, ${documentId}, ${messageHash},
                NULL, NULL, NULL, ${Schema.encodeSync(Heads)(heads)}, ${Schema.encodeSync(Heads)(heads)},
                ${commitSequence}, ${acceptedAt}, ${encodedWriterProvenance},
                ${checkpointTransferBytes}, ${relay?.senderTenantId ?? null},
                ${relay?.senderSubjectId ?? null}, ${relay?.senderPeerId ?? null},
                ${relay?.relayMessageId ?? null}, ${relay?.outerEnvelopeDigest ?? null},
                ${relay?.receiptExpiresAt ?? null}, ${relayRetainedSize}
              )`
              if (relay !== undefined) {
                yield* recordRelayReceiptUsage(receiptSession.replicaIncarnation, relay, relayRetainedSize!)
              }
              yield* gate.validate(permit)
              return {
                reply: null,
                heads,
                acceptedHeads: heads,
                commitSequence: Identity.CommitSequence.make(commitSequence),
                observedByPeer: false,
                duplicate: false
              } satisfies Received
            }))).pipe(
              Effect.tap((received) =>
                writeState(
                  session,
                  documentId,
                  syncStateAtHeads(received.heads, checkpointChangeHashes)
                )
              ),
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
          }),
        (prepared) => Effect.sync(() => InternalAutomerge.free(prepared.automerge))
      ).pipe(
        Effect.catchTags({
          NoSuchElementError: () =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.ReplicaMetadataMissing({
                  operation: "PeerSync.receive checkpoint"
                })
              })
            ),
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
        Effect.onError(() => removeDocumentState(documentId))
      )

    const receive = <D extends Document.Any,>(
      document: D,
      documentId: Identity.DocumentId,
      session: Session,
      input: {
        readonly remoteConnectionEpoch: string
        readonly receiveSequence: number
        readonly lineage?: Identity.DocumentLineage
        readonly message: Uint8Array
        readonly writerProvenance: ReadonlyArray<WriterProvenance.ChangeProvenance>
        readonly checkpointTransfer?: Uint8Array
        readonly relay?: RelayReceipt
      }
    ) =>
      withSessionGeneration(session, (generation) =>
        Ref.get(generation).pipe(
          Effect.flatMap((sessionGeneration) =>
            withStateLock(
              documentId,
              Effect.scoped(Effect.gen(function*() {
                const receiptSession = { ...session, connectionEpoch: input.remoteConnectionEpoch }
                const { message, receiveSequence } = input
                const checkpointTransfer = input.checkpointTransfer
                const relay = input.relay
                if (relay !== undefined && relayReceiptLimits === null) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: "direct peer receipt",
                      observed: "relay peer receipt"
                    })
                  })
                }
                const writerProvenance = yield* Schema.decodeUnknownEffect(
                  WriterProvenance.ChangeProvenances
                )(input.writerProvenance).pipe(
                  Effect.map(WriterProvenance.canonicalize),
                  Effect.mapError(() =>
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.ProtocolMismatch({
                        expected: "valid writer provenance",
                        observed: "invalid writer provenance"
                      })
                    })
                  )
                )
                // Re-decoded here for the same reason the writer provenance above is: the value is
                // peer controlled, and a direct caller of `receive` has not necessarily passed it
                // through the wire schema that already checks it.
                const remoteLineage = yield* Schema.decodeUnknownEffect(Identity.DocumentLineage)(
                  input.lineage ?? Identity.genesisLineage
                ).pipe(
                  Effect.mapError(() =>
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.ProtocolMismatch({
                        expected: "valid document lineage",
                        observed: "invalid document lineage"
                      })
                    })
                  )
                )
                if (writerProvenance.length > limits.maxSyncChangesPerMessage) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: `at most ${limits.maxSyncChangesPerMessage} writer provenance entries`,
                      observed: String(writerProvenance.length)
                    })
                  })
                }
                if (!Number.isSafeInteger(receiveSequence) || receiveSequence < 0) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: "nonnegative safe receive sequence",
                      observed: String(receiveSequence)
                    })
                  })
                }
                if (message.byteLength > limits.maxSyncMessageBytes) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: `sync message at most ${limits.maxSyncMessageBytes} bytes`,
                      observed: String(message.byteLength)
                    })
                  })
                }
                if (
                  checkpointTransfer !== undefined &&
                  (message.byteLength !== 0 || writerProvenance.length !== 0)
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: "empty sync message and writer provenance for checkpoint transfer",
                      observed: "checkpoint transfer mixed with ordinary sync content"
                    })
                  })
                }
                // Refuse before anything else touches storage. Every Automerge ingestion path is a
                // union, so a message from a superseded lineage cannot be merged at all: applying
                // it restores exactly the history the rewrite discarded, and the rewritten value
                // then loses to the surviving lineage's higher operation counters.
                //
                // This must stay ahead of three specific things below. `expirePending` already
                // writes -- it quarantines and deletes rows -- so a refusal after it is not free of
                // durable effect. The duplicate receipt short circuit replays a receipt cached
                // before the rewrite and never compares lineage, so a retransmission would slip
                // past a check placed after it. And `decodeSyncMessage` plus `store.load` are the
                // expensive part of the whole path, which would make the cheapest hostile message
                // the most expensive one to reject.
                const localLineage = yield* documentLineage(documentId)
                if (checkpointTransfer === undefined && localLineage !== remoteLineage) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.DocumentLineageChanged({
                      documentId,
                      localLineage,
                      remoteLineage
                    })
                  })
                }
                // Use current, not shared: the cluster serves ApplySync inside sql.withTransaction,
                // so acquiring the gate here inverts claim's gate-then-SQL lock order (restore-vs-
                // ApplySync deadlock). Fencing still holds via gate.validate in the write tx below.
                const permit = yield* gate.current
                yield* validateSession(permit, session)
                const nowMillis = yield* Clock.currentTimeMillis
                const acceptedAt = new Date(nowMillis).toISOString()
                if (relay !== undefined) {
                  if (relay.senderPeerId !== session.peerId) {
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.ProtocolMismatch({
                        expected: session.peerId,
                        observed: relay.senderPeerId
                      })
                    })
                  }
                  if (
                    !Number.isSafeInteger(relay.encodedSize) ||
                    relay.encodedSize <= 0
                  ) {
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.ProtocolMismatch({
                        expected: "positive safe relay receipt encoded size",
                        observed: String(relay.encodedSize)
                      })
                    })
                  }
                  const receiptExpiresAtMillis = Date.parse(relay.receiptExpiresAt)
                  if (
                    !Number.isFinite(receiptExpiresAtMillis) ||
                    receiptExpiresAtMillis <= nowMillis ||
                    receiptExpiresAtMillis - nowMillis > relayReceiptLimits!.receiptRetentionMillis
                  ) {
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.ProtocolMismatch({
                        expected: "bounded future relay receipt expiry",
                        observed: "invalid relay receipt expiry"
                      })
                    })
                  }
                }
                yield* quotaLock.withPermit(Effect.gen(function*() {
                  yield* validateSessionGeneration(generation, sessionGeneration)
                  yield* expirePending(
                    receiptSession,
                    documentId,
                    acceptedAt,
                    new Date(nowMillis - limits.maxPendingAgeMillis).toISOString()
                  ).pipe(Effect.catchTag("SqlError", (cause) =>
                    Effect.fail(
                      new ReplicaError.ReplicaError({
                        reason: new ReplicaError.StorageUnavailable({ cause })
                      })
                    )))
                  if (relayReceiptLimits !== null) {
                    yield* sql.withTransaction(Effect.gen(function*() {
                      yield* pruneRelayReceiptsInTransaction(permit.incarnation, acceptedAt)
                      yield* gate.validate(permit)
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
                      })
                    )
                  }
                }))
                const messageHash = yield* digest(message)
                if (relay !== undefined && relay.messageHash !== messageHash) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: messageHash,
                      observed: relay.messageHash
                    })
                  })
                }
                const validateReceipt = (receipt: typeof ReceiptRow.Type) =>
                  Effect.gen(function*() {
                    if (receipt.document_id !== documentId) {
                      return yield* new ReplicaError.ReplicaError({
                        reason: new ReplicaError.ProtocolMismatch({
                          expected: receipt.document_id,
                          observed: documentId
                        })
                      })
                    }
                    if (receipt.message_hash !== messageHash) {
                      return yield* new ReplicaError.ReplicaError({
                        reason: new ReplicaError.ProtocolMismatch({
                          expected: receipt.message_hash,
                          observed: messageHash
                        })
                      })
                    }
                    if (!WriterProvenance.equals(receipt.writer_provenance, writerProvenance)) {
                      return yield* new ReplicaError.ReplicaError({
                        reason: new ReplicaError.ProtocolMismatch({
                          expected: "matching writer provenance",
                          observed: "conflicting writer provenance"
                        })
                      })
                    }
                    if (
                      !Equal.equals(
                        receipt.checkpoint_transfer,
                        checkpointTransfer ?? null
                      )
                    ) {
                      return yield* new ReplicaError.ReplicaError({
                        reason: new ReplicaError.ProtocolMismatch({
                          expected: "matching checkpoint transfer",
                          observed: "conflicting checkpoint transfer"
                        })
                      })
                    }
                  })
                const loadReceipt = () =>
                  relay === undefined
                    ? findReceipts({
                      replicaIncarnation: receiptSession.replicaIncarnation,
                      peerId: receiptSession.peerId,
                      connectionEpoch: receiptSession.connectionEpoch,
                      receiveSequence
                    })
                    : findRelayReceipts({
                      relayMessageId: relay.relayMessageId,
                      replicaIncarnation: receiptSession.replicaIncarnation,
                      senderPeerId: relay.senderPeerId,
                      senderSubjectId: relay.senderSubjectId,
                      senderTenantId: relay.senderTenantId
                    })
                const validateStoredReceipt = (
                  receipt: typeof ReceiptRow.Type | typeof RelayReceiptRow.Type
                ) =>
                  Effect.gen(function*() {
                    yield* validateReceipt(receipt)
                    if (
                      relay !== undefined &&
                      (
                        !("relay_outer_envelope_digest" in receipt) ||
                        receipt.relay_outer_envelope_digest !== relay.outerEnvelopeDigest
                      )
                    ) {
                      return yield* new ReplicaError.ReplicaError({
                        reason: new ReplicaError.ProtocolMismatch({
                          expected: "matching relay receipt identity",
                          observed: "conflicting relay receipt identity"
                        })
                      })
                    }
                  })
                const storedReceipt = yield* sql.withTransaction(Effect.gen(function*() {
                  const receipt = (yield* loadReceipt())[0]
                  if (receipt === undefined) return null
                  return {
                    receipt,
                    replies: yield* findReceiptReplies(receipt.row_id)
                  }
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
                  })
                )
                if (storedReceipt !== null) {
                  yield* validateStoredReceipt(storedReceipt.receipt)
                  yield* quotaLock.withPermit(validateSessionGeneration(generation, sessionGeneration))
                  if (storedReceipt.receipt.checkpoint_transfer !== null) {
                    yield* Effect.uninterruptible(removeDocumentState(documentId))
                  }
                  return receivedFromReceipt(documentId, storedReceipt.receipt, storedReceipt.replies)
                }
                if (checkpointTransfer !== undefined) {
                  return yield* installCheckpoint(
                    document,
                    documentId,
                    session,
                    receiptSession,
                    receiveSequence,
                    remoteLineage,
                    checkpointTransfer,
                    messageHash,
                    acceptedAt,
                    relay,
                    generation,
                    sessionGeneration,
                    permit
                  )
                }
                const validateReceiptQuota = Effect.gen(function*() {
                  const receiptTotals = yield* findReceiptTotals({
                    replicaIncarnation: receiptSession.replicaIncarnation,
                    peerId: receiptSession.peerId,
                    documentId
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
                    })
                  )
                  const receiptTotal = receiptTotals[0]
                  if ((receiptTotal?.document_count ?? 0) > limits.maxPendingChangesPerDocument) {
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.QuotaExceeded({
                        resource: "document sync receipts",
                        limit: limits.maxPendingChangesPerDocument
                      })
                    })
                  }
                  if ((receiptTotal?.peer_count ?? 0) > limits.maxPendingChangesPerPeer) {
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.QuotaExceeded({
                        resource: "peer sync receipts",
                        limit: limits.maxPendingChangesPerPeer
                      })
                    })
                  }
                  if ((receiptTotal?.replica_count ?? 0) > limits.maxPendingChangesPerReplica) {
                    return yield* new ReplicaError.ReplicaError({
                      reason: new ReplicaError.QuotaExceeded({
                        resource: "replica sync receipts",
                        limit: limits.maxPendingChangesPerReplica
                      })
                    })
                  }
                })
                const decoded = yield* Effect.try({
                  try: () => Automerge.decodeSyncMessage(message),
                  catch: (cause) =>
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.ProtocolMismatch({
                        expected: "valid Automerge sync message",
                        observed: String(cause)
                      })
                    })
                })
                const incomingChanges = yield* Effect.try({
                  try: () => SyncChunks.decodeSyncChanges(decoded.changes),
                  catch: (cause) =>
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.ProtocolMismatch({
                        expected: "valid Automerge change chunks",
                        observed: String(cause)
                      })
                    })
                })
                if (incomingChanges.length !== writerProvenance.length) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: `writer provenance for ${incomingChanges.length} sync changes`,
                      observed: String(writerProvenance.length)
                    })
                  })
                }
                const provenanceByHash = new Map(writerProvenance.map((entry) => [entry.changeHash, entry]))
                if (
                  provenanceByHash.size !== writerProvenance.length ||
                  incomingChanges.some((change) => !provenanceByHash.has(change.hash))
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.ProtocolMismatch({
                      expected: "one writer provenance entry per sync change",
                      observed: "missing, duplicate, or unrelated writer provenance"
                    })
                  })
                }
                return yield* Effect.acquireUseRelease(
                  store.load(document, documentId),
                  (durable) =>
                    Effect.gen(function*() {
                      const { changeBytes, changes, unresolvedBytes } = yield* Effect.try({
                        try: () => {
                          let current = Automerge.clone(durable.automerge)
                          try {
                            for (const chunk of decoded.changes) current = Automerge.loadIncremental(current, chunk)
                            const changeBytes = Automerge.getChangesSince(current, [...durable.materializedHeads])
                            return {
                              changeBytes,
                              changes: changeBytes.map((bytes) => Automerge.decodeChange(bytes)),
                              unresolvedBytes: Automerge.hasHeads(current, decoded.heads)
                                ? 0
                                : decoded.changes.reduce((total, bytes) => total + bytes.byteLength, 0)
                            }
                          } finally {
                            InternalAutomerge.free(current)
                          }
                        },
                        catch: (cause) =>
                          new ReplicaError.ReplicaError({
                            reason: new ReplicaError.ProtocolMismatch({
                              expected: "valid Automerge change chunks",
                              observed: String(cause)
                            })
                          })
                      })
                      if (changes.length > limits.maxSyncChangesPerMessage) {
                        return yield* new ReplicaError.ReplicaError({
                          reason: new ReplicaError.ProtocolMismatch({
                            expected: `at most ${limits.maxSyncChangesPerMessage} sync changes`,
                            observed: String(changes.length)
                          })
                        })
                      }
                      const dependencyEdges = changes.reduce((total, change) => total + change.deps.length, 0)
                      const operations = changes.reduce((total, change) => total + change.ops.length, 0)
                      if (dependencyEdges > limits.maxSyncDependencyEdgesPerMessage) {
                        return yield* new ReplicaError.ReplicaError({
                          reason: new ReplicaError.ProtocolMismatch({
                            expected: `at most ${limits.maxSyncDependencyEdgesPerMessage} dependency edges`,
                            observed: String(dependencyEdges)
                          })
                        })
                      }
                      if (operations > limits.maxSyncOperationsPerMessage) {
                        return yield* new ReplicaError.ReplicaError({
                          reason: new ReplicaError.ProtocolMismatch({
                            expected: `at most ${limits.maxSyncOperationsPerMessage} operations`,
                            observed: String(operations)
                          })
                        })
                      }
                      const identities = new Map<string, string>()
                      const validationChanges = [
                        ...new Map(
                          [...changes, ...incomingChanges].map((change) => [change.hash, change])
                        ).values()
                      ]
                      for (const change of validationChanges) {
                        const key = `${change.actor}:${change.seq}`
                        const existing = identities.get(key)
                        if (existing !== undefined && existing !== change.hash) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.ProtocolMismatch({
                              expected: existing,
                              observed: change.hash
                            })
                          })
                        }
                        identities.set(key, change.hash)
                      }
                      const validateExistingChanges = (rows: ReadonlyArray<typeof ExistingChangeRow.Type>) =>
                        Effect.gen(function*() {
                          const hashes = new Map(rows.map((row) => [row.change_hash, row]))
                          const storedIdentities = new Map(rows.map((row) => [`${row.actor}:${row.sequence}`, row]))
                          for (const change of validationChanges) {
                            const hash = hashes.get(change.hash)
                            if (
                              hash !== undefined &&
                              (hash.document_id !== documentId || hash.actor !== change.actor ||
                                hash.sequence !== change.seq)
                            ) {
                              return yield* new ReplicaError.ReplicaError({
                                reason: new ReplicaError.ProtocolMismatch({
                                  expected: `${hash.document_id}:${hash.actor}:${hash.sequence}`,
                                  observed: `${documentId}:${change.actor}:${change.seq}`
                                })
                              })
                            }
                            const identity = storedIdentities.get(`${change.actor}:${change.seq}`)
                            if (identity !== undefined && identity.change_hash !== change.hash) {
                              return yield* new ReplicaError.ReplicaError({
                                reason: new ReplicaError.ProtocolMismatch({
                                  expected: identity.change_hash,
                                  observed: change.hash
                                })
                              })
                            }
                            const provenance = provenanceByHash.get(change.hash)
                            if (
                              hash !== undefined && provenance !== undefined &&
                              (
                                hash.writer_schema_version !== provenance.writerSchemaVersion ||
                                hash.writer_definition_hash !== provenance.writerDefinitionHash
                              )
                            ) {
                              return yield* new ReplicaError.ReplicaError({
                                reason: new ReplicaError.ProtocolMismatch({
                                  expected: "matching stored writer provenance",
                                  observed: "conflicting writer provenance"
                                })
                              })
                            }
                          }
                          return hashes
                        })
                      const existingChanges = validationChanges.length === 0 ? [] : yield* findExistingChanges({
                        documentId,
                        changes: validationChanges.map((change) => ({
                          actor: change.actor,
                          changeHash: change.hash,
                          sequence: change.seq
                        }))
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
                        })
                      )
                      yield* validateExistingChanges(existingChanges)
                      const validatePendingQuota = Effect.gen(function*() {
                        const pendingTotals = yield* findDocumentPendingChangeTotals(documentId).pipe(
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
                        const receiptPending = yield* findDocumentPendingReceiptTotals({
                          replicaIncarnation: receiptSession.replicaIncarnation,
                          documentId
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
                          })
                        )
                        if (
                          (pendingTotals[0]?.bytes ?? 0) + (receiptPending[0]?.bytes ?? 0) >
                            limits.maxPendingBytesPerDocument
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.QuotaExceeded({
                              resource: "pending document bytes",
                              limit: limits.maxPendingBytesPerDocument
                            })
                          })
                        }
                        if (
                          (pendingTotals[0]?.count ?? 0) + (receiptPending[0]?.count ?? 0) >
                            limits.maxPendingChangesPerDocument
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.QuotaExceeded({
                              resource: "pending document changes",
                              limit: limits.maxPendingChangesPerDocument
                            })
                          })
                        }
                        if (
                          (pendingTotals[0]?.dependencies ?? 0) > limits.maxPendingDependencyEdgesPerDocument
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.QuotaExceeded({
                              resource: "pending document dependency edges",
                              limit: limits.maxPendingDependencyEdgesPerDocument
                            })
                          })
                        }
                        const peerTotals = yield* findPeerPendingChangeTotals(receiptSession.peerId).pipe(
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
                        const peerReceiptPending = yield* findPeerPendingReceiptTotals({
                          replicaIncarnation: receiptSession.replicaIncarnation,
                          peerId: receiptSession.peerId
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
                          })
                        )
                        if (
                          (peerTotals[0]?.bytes ?? 0) + (peerReceiptPending[0]?.bytes ?? 0) >
                            limits.maxPendingBytesPerPeer
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.QuotaExceeded({
                              resource: "pending peer bytes",
                              limit: limits.maxPendingBytesPerPeer
                            })
                          })
                        }
                        if (
                          (peerTotals[0]?.count ?? 0) + (peerReceiptPending[0]?.count ?? 0) >
                            limits.maxPendingChangesPerPeer
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.QuotaExceeded({
                              resource: "pending peer changes",
                              limit: limits.maxPendingChangesPerPeer
                            })
                          })
                        }
                        if (
                          (peerTotals[0]?.dependencies ?? 0) > limits.maxPendingDependencyEdgesPerPeer
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.QuotaExceeded({
                              resource: "pending peer dependency edges",
                              limit: limits.maxPendingDependencyEdgesPerPeer
                            })
                          })
                        }
                        const replicaTotals = yield* findReplicaPendingChangeTotals(undefined).pipe(
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
                        const replicaReceiptPending = yield* findReplicaPendingReceiptTotals(
                          receiptSession.replicaIncarnation
                        ).pipe(
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
                        if (
                          (replicaTotals[0]?.bytes ?? 0) + (replicaReceiptPending[0]?.bytes ?? 0) >
                            limits.maxPendingBytesPerReplica
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.QuotaExceeded({
                              resource: "pending replica bytes",
                              limit: limits.maxPendingBytesPerReplica
                            })
                          })
                        }
                        if (
                          (replicaTotals[0]?.count ?? 0) + (replicaReceiptPending[0]?.count ?? 0) >
                            limits.maxPendingChangesPerReplica
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.QuotaExceeded({
                              resource: "pending replica changes",
                              limit: limits.maxPendingChangesPerReplica
                            })
                          })
                        }
                        if (
                          (replicaTotals[0]?.dependencies ?? 0) > limits.maxPendingDependencyEdgesPerReplica
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.QuotaExceeded({
                              resource: "pending replica dependency edges",
                              limit: limits.maxPendingDependencyEdgesPerReplica
                            })
                          })
                        }
                      })
                      const state = yield* readState(session, documentId)
                      const received = yield* Effect.try({
                        try: () => Automerge.receiveSyncMessage(durable.automerge, state, message),
                        catch: (cause) =>
                          new ReplicaError.ReplicaError({
                            reason: new ReplicaError.ProtocolMismatch({
                              expected: "applicable Automerge sync message",
                              observed: String(cause)
                            })
                          })
                      })
                      const pendingRows = yield* findPendingChanges(documentId).pipe(
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
                      const pendingReceiptRows = yield* findPendingReceipts({
                        replicaIncarnation: receiptSession.replicaIncarnation,
                        documentId
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
                        })
                      )
                      const checkpointProvenanceRows = yield* findCheckpointProvenance(documentId).pipe(
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
                      const checkpointProvenanceByHash = new Map<
                        string,
                        WriterProvenance.ChangeProvenance
                      >()
                      const durableChangeHashes = WriterProvenance.changeHashes(durable.automerge)
                      for (const row of checkpointProvenanceRows) {
                        if (!WriterProvenance.isCompactCheckpoint(row.writer_provenance)) continue
                        const compact = row.writer_provenance
                        if (!Automerge.hasHeads(durable.automerge, [...compact.heads])) continue
                        const changesAfterCheckpoint = new Set(
                          Automerge.getChangesSince(durable.automerge, [...compact.heads])
                            .map((bytes) => Automerge.decodeChange(bytes).hash)
                        )
                        for (const changeHash of durableChangeHashes) {
                          if (changesAfterCheckpoint.has(changeHash)) continue
                          checkpointProvenanceByHash.set(changeHash, {
                            changeHash,
                            writerSchemaVersion: compact.schemaVersion,
                            writerDefinitionHash: compact.writerDefinitionHash
                          })
                        }
                      }
                      for (
                        const entry of checkpointProvenanceRows.flatMap((row) =>
                          WriterProvenance.exactEntries(row.writer_provenance)
                        )
                      ) {
                        const existing = checkpointProvenanceByHash.get(entry.changeHash)
                        if (
                          existing !== undefined &&
                          (
                            existing.writerSchemaVersion !== entry.writerSchemaVersion ||
                            existing.writerDefinitionHash !== entry.writerDefinitionHash
                          )
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.StorageCorrupt({
                              cause: new Error(
                                `Conflicting checkpoint writer provenance for change ${entry.changeHash}`
                              )
                            })
                          })
                        }
                        checkpointProvenanceByHash.set(entry.changeHash, entry)
                      }
                      for (const entry of writerProvenance) {
                        const checkpointEntry = checkpointProvenanceByHash.get(entry.changeHash)
                        if (
                          checkpointEntry !== undefined &&
                          (
                            checkpointEntry.writerSchemaVersion !== entry.writerSchemaVersion ||
                            checkpointEntry.writerDefinitionHash !== entry.writerDefinitionHash
                          )
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.ProtocolMismatch({
                              expected: "matching checkpoint writer provenance",
                              observed: "conflicting writer provenance"
                            })
                          })
                        }
                      }
                      const pendingProvenanceByHash = new Map<string, WriterProvenance.ChangeProvenance>()
                      for (
                        const entry of [
                          ...pendingRows.map((row) => ({
                            changeHash: row.change_hash,
                            writerSchemaVersion: row.writer_schema_version,
                            writerDefinitionHash: row.writer_definition_hash
                          })),
                          ...pendingReceiptRows.flatMap((row) => row.writer_provenance)
                        ]
                      ) {
                        const existing = pendingProvenanceByHash.get(entry.changeHash)
                        if (
                          existing !== undefined &&
                          (
                            existing.writerSchemaVersion !== entry.writerSchemaVersion ||
                            existing.writerDefinitionHash !== entry.writerDefinitionHash
                          )
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.StorageCorrupt({
                              cause: new Error(
                                `Conflicting pending writer provenance for change ${entry.changeHash}`
                              )
                            })
                          })
                        }
                        pendingProvenanceByHash.set(entry.changeHash, entry)
                      }
                      for (const entry of writerProvenance) {
                        const pending = pendingProvenanceByHash.get(entry.changeHash)
                        if (
                          pending !== undefined &&
                          (
                            pending.writerSchemaVersion !== entry.writerSchemaVersion ||
                            pending.writerDefinitionHash !== entry.writerDefinitionHash
                          )
                        ) {
                          return yield* new ReplicaError.ReplicaError({
                            reason: new ReplicaError.ProtocolMismatch({
                              expected: "matching pending writer provenance",
                              observed: "conflicting writer provenance"
                            })
                          })
                        }
                      }
                      const staged = pendingRows.length === 0
                        ? received[0]
                        : yield* Effect.try({
                          try: () => {
                            for (const row of pendingRows) {
                              const pending = InternalAutomerge.decode(row.bytes)
                              if (
                                pending.hash !== row.change_hash || pending.actor !== row.actor ||
                                pending.sequence !== row.sequence ||
                                Schema.encodeSync(Heads)(pending.dependencies) !== row.dependencies
                              ) {
                                throw new TypeError(`Invalid stored change: ${row.change_hash}`)
                              }
                            }
                            return InternalAutomerge.replay(received[0], pendingRows.map((row) => row.bytes))
                          },
                          catch: (cause) =>
                            new ReplicaError.ReplicaError({
                              reason: new ReplicaError.StorageCorrupt({ cause })
                            })
                        })
                      const generated = yield* Effect.try({
                        try: () => Automerge.generateSyncMessage(staged, received[1]),
                        catch: (cause) =>
                          new ReplicaError.ReplicaError({
                            reason: new ReplicaError.ProtocolMismatch({
                              expected: "valid Automerge sync response",
                              observed: String(cause)
                            })
                          })
                      })
                      const replyMessages = generated[1] === null
                        ? []
                        : yield* Effect.try({
                          try: () =>
                            SyncChunks.batchSyncMessage(generated[1]!, {
                              maxChanges: Math.min(
                                limits.maxSyncChangesPerMessage,
                                PeerSyncEnvelope.maximumWriterProvenanceEntries
                              ),
                              maxBytes: limits.maxSyncMessageBytes,
                              maxMessages: limits.maxPendingChangesPerPeer,
                              maxTotalBytes: limits.maxPendingBytesPerPeer
                            }),
                          catch: (cause) =>
                            new ReplicaError.ReplicaError({
                              reason: new ReplicaError.ProtocolMismatch({
                                expected: "batchable Automerge sync response",
                                observed: String(cause)
                              })
                            })
                        })
                      if (replyMessages === null) {
                        return yield* new ReplicaError.ReplicaError({
                          reason: new ReplicaError.QuotaExceeded({
                            resource: "sync response bytes",
                            limit: limits.maxSyncMessageBytes
                          })
                        })
                      }
                      const materializedHeads = InternalAutomerge.heads(staged)
                      const acceptedHeads = Automerge.hasHeads(staged, decoded.heads)
                        ? materializedHeads
                        : [...new Set([...durable.acceptedHeads, ...materializedHeads, ...decoded.heads])].toSorted()
                      const transition = !sameHeads(materializedHeads, durable.materializedHeads)
                      const value = transition
                        ? yield* Document.decode(document, documentId, InternalAutomerge.value(staged))
                        : durable.snapshot.value
                      // A chunk whose dependencies are not satisfied yet stays queued inside the Automerge
                      // document instead of joining its history, so `getChangesSince` above never reports it
                      // and it never becomes an `effect_local_changes` row. The saved checkpoint is the only
                      // durable carrier for such a change, so a message that leaves any incoming change
                      // unmaterialized must still checkpoint even when the canonical heads did not move.
                      const unmaterialized = incomingChanges.some((change) =>
                        !Automerge.hasHeads(staged, [change.hash])
                      )
                      const checkpoint = !transition && !unmaterialized
                        ? null
                        : yield* Effect.gen(function*() {
                          const bytes = InternalAutomerge.save(staged)
                          const durableRows = yield* findDocumentChangeProvenance(documentId).pipe(
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
                          const checkpointWriterProvenance = yield* Effect.try({
                            try: () =>
                              WriterProvenance.resolve(
                                WriterProvenance.changeHashes(staged),
                                [
                                  ...durableRows.map((row) => ({
                                    changeHash: row.change_hash,
                                    writerSchemaVersion: row.writer_schema_version,
                                    writerDefinitionHash: row.writer_definition_hash
                                  })),
                                  ...checkpointProvenanceByHash.values(),
                                  ...pendingProvenanceByHash.values(),
                                  ...writerProvenance
                                ]
                              ),
                            catch: (cause) =>
                              new ReplicaError.ReplicaError({
                                reason: new ReplicaError.StorageCorrupt({ cause })
                              })
                          })
                          return {
                            bytes,
                            checksum: yield* digest(bytes),
                            checkpointHash: yield* digest({ documentId, bytes }),
                            writerProvenance: checkpointWriterProvenance
                          }
                        })
                      const result = yield* quotaLock.withPermit(Effect.gen(function*() {
                        const result = yield* sql.withTransaction(Effect.gen(function*() {
                          yield* validateSessionGeneration(generation, sessionGeneration)
                          if (relayReceiptLimits !== null) {
                            yield* pruneRelayReceiptsInTransaction(permit.incarnation, acceptedAt)
                          }
                          const receiptRows = yield* loadReceipt()
                          const receipt = receiptRows[0]
                          if (receipt !== undefined) {
                            yield* validateStoredReceipt(receipt)
                            const replies = yield* findReceiptReplies(receipt.row_id)
                            return {
                              _tag: "Duplicate" as const,
                              received: receivedFromReceipt(documentId, receipt, replies)
                            }
                          }
                          const committedChanges = validationChanges.length === 0 ? [] : yield* findExistingChanges({
                            documentId,
                            changes: validationChanges.map((change) => ({
                              actor: change.actor,
                              changeHash: change.hash,
                              sequence: change.seq
                            }))
                          })
                          const committedChangeMap = yield* validateExistingChanges(committedChanges)
                          const newChanges = changes.flatMap((change, index) =>
                            committedChangeMap.has(change.hash)
                              ? []
                              : [{
                                bytes: changeBytes[index]!,
                                operations: change.ops.length
                              }]
                          )
                          const history = newChanges.length === 0
                            ? {
                              bytes: durable.historyBytes,
                              changes: durable.historyChanges,
                              operations: durable.historyOperations
                            }
                            : yield* HistoryCounters.add(
                              {
                                bytes: durable.historyBytes,
                                changes: durable.historyChanges,
                                operations: durable.historyOperations
                              },
                              HistoryCounters.measureDecoded(newChanges),
                              limits
                            )
                          yield* gate.validate(permit)
                          const commitSequence = transition ? yield* nextSequence : yield* currentSequence
                          for (let index = 0; index < changes.length; index++) {
                            const change = changes[index]!
                            if (committedChangeMap.has(change.hash)) continue
                            const bytes = changeBytes[index]!
                            const applied = Automerge.hasHeads(staged, [change.hash]) ? 1 : 0
                            const provenance = provenanceByHash.get(change.hash) ??
                              pendingProvenanceByHash.get(change.hash)
                            if (provenance === undefined) {
                              return yield* new ReplicaError.ReplicaError({
                                reason: new ReplicaError.ProtocolMismatch({
                                  expected: `writer provenance for change ${change.hash}`,
                                  observed: `missing writer provenance (incoming=${
                                    provenanceByHash.has(change.hash)
                                  }, pending=${pendingProvenanceByHash.has(change.hash)}, committed=${
                                    committedChangeMap.has(change.hash)
                                  })`
                                })
                              })
                            }
                            yield* sql`INSERT INTO effect_local_changes (
              change_hash, document_id, document_type, writer_schema_version, writer_definition_hash,
              actor, sequence, dependencies, bytes, applied, peer_id, accepted_at, commit_sequence
            ) VALUES (
              ${change.hash}, ${documentId}, ${document.name}, ${provenance.writerSchemaVersion},
              ${provenance.writerDefinitionHash},
              ${change.actor}, ${change.seq}, ${Schema.encodeSync(Heads)(change.deps)}, ${bytes}, ${applied},
              ${receiptSession.peerId}, ${acceptedAt}, ${commitSequence}
            ) ON CONFLICT(change_hash) DO NOTHING`
                          }
                          if (validationChanges.length > 0) {
                            yield* findExistingChanges({
                              documentId,
                              changes: validationChanges.map((change) => ({
                                actor: change.actor,
                                changeHash: change.hash,
                                sequence: change.seq
                              }))
                            }).pipe(Effect.flatMap(validateExistingChanges))
                          }
                          for (const row of pendingRows) {
                            if (Automerge.hasHeads(staged, [row.change_hash])) {
                              yield* sql`UPDATE effect_local_changes SET applied = 1, commit_sequence = ${commitSequence}
                WHERE change_hash = ${row.change_hash}`
                            }
                          }
                          for (const row of pendingReceiptRows) {
                            if (Automerge.hasHeads(staged, [...row.accepted_heads])) {
                              yield* sql`UPDATE effect_local_peer_receipts SET pending_message = NULL
                WHERE row_id = ${row.row_id}`
                            }
                          }
                          if (checkpoint !== null) {
                            // Stamped with the lineage this message was admitted under, not left on
                            // the column default. A checkpoint written for a rewritten document has
                            // to name the lineage it belongs to, or it would read back as a
                            // pre rewrite blob. The refusal above proves the two agree, and a
                            // rewrite that lands in between moves the document's heads, which makes
                            // `updateDocument` below match no row and roll this transaction back.
                            yield* sql`INSERT INTO effect_local_checkpoints (
              checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified, writer_provenance,
              lineage
            ) VALUES (
              ${checkpoint.checkpointHash}, ${documentId}, ${Schema.encodeSync(Heads)(materializedHeads)},
              ${checkpoint.bytes}, ${checkpoint.checksum}, ${commitSequence}, 1,
              ${Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(checkpoint.writerProvenance)},
              ${localLineage}
            ) ON CONFLICT(checkpoint_hash) DO NOTHING`
                            const installed = yield* findCheckpointIdentity({
                              bytes: checkpoint.bytes,
                              checkpointHash: checkpoint.checkpointHash,
                              checksum: checkpoint.checksum,
                              documentId,
                              heads: materializedHeads,
                              writerProvenance: checkpoint.writerProvenance
                            })
                            if (installed.length !== 1) {
                              return yield* new ReplicaError.ReplicaError({
                                reason: new ReplicaError.StorageCorrupt({
                                  cause: new Error("Checkpoint identity collision")
                                })
                              })
                            }
                            yield* sql`DELETE FROM effect_local_checkpoints
                WHERE document_id = ${documentId}
                  AND checkpoint_hash NOT IN (
                    SELECT checkpoint_hash FROM effect_local_checkpoints
                    WHERE document_id = ${documentId}
                    ORDER BY verified DESC, commit_sequence DESC, checkpoint_hash DESC
                    LIMIT 2
                  )`
                          }
                          const updated = yield* updateDocument({
                            acceptedHeads: Schema.encodeSync(Heads)(acceptedHeads),
                            checkpointHash: checkpoint?.checkpointHash ?? null,
                            documentId,
                            expectedAcceptedHeads: Schema.encodeSync(Heads)(durable.acceptedHeads),
                            expectedHistoryBytes: durable.historyBytes,
                            expectedHistoryChanges: durable.historyChanges,
                            expectedHistoryOperations: durable.historyOperations,
                            expectedMaterializedHeads: Schema.encodeSync(Heads)(durable.materializedHeads),
                            expectedProjectionStatus: durable.snapshot.projection,
                            historyBytes: history.bytes,
                            historyChanges: history.changes,
                            historyOperations: history.operations,
                            materializedHeads: Schema.encodeSync(Heads)(materializedHeads),
                            projectionStatus: transition ? "Blocked" : durable.snapshot.projection,
                            tombstone: InternalAutomerge.tombstone(staged) ? 1 : 0
                          })
                          if (updated.length === 0) return yield* new ConcurrentDocumentWrite()
                          if (transition) {
                            yield* sql`INSERT INTO effect_local_commit_outbox (
              commit_sequence, document_id, invalidation_keys, published
            ) VALUES (
              ${commitSequence}, ${documentId},
              ${Schema.encodeSync(Heads)(ReplicaDefinition.documentCommitKeys(document.name, documentId))}, 0
            )`
                            yield* projections.replaceDocument(
                              document,
                              {
                                ...durable.snapshot,
                                heads: materializedHeads,
                                tombstone: InternalAutomerge.tombstone(staged),
                                value
                              },
                              commitSequence,
                              "Fresh"
                            )
                          }
                          const replyBytes = replyMessages.reduce(
                            (total, replyMessage) => total + replyMessage.byteLength,
                            0
                          )
                          if (replyMessages.length > limits.maxPendingChangesPerPeer) {
                            return yield* new ReplicaError.ReplicaError({
                              reason: new ReplicaError.QuotaExceeded({
                                resource: "peer sync outbox messages",
                                limit: limits.maxPendingChangesPerPeer
                              })
                            })
                          }
                          if (replyBytes > limits.maxPendingBytesPerPeer) {
                            return yield* new ReplicaError.ReplicaError({
                              reason: new ReplicaError.QuotaExceeded({
                                resource: "peer sync outbox bytes",
                                limit: limits.maxPendingBytesPerPeer
                              })
                            })
                          }
                          const replyParts = yield* Effect.forEach(
                            replyMessages,
                            (replyMessage, replyIndex) =>
                              Effect.gen(function*() {
                                return {
                                  replyIndex,
                                  message: replyMessage,
                                  messageHash: yield* digest(replyMessage),
                                  heads: materializedHeads
                                }
                              })
                          )
                          const firstReply = replyParts[0]
                          const pendingMessage = unresolvedBytes === 0 ? null : message
                          const encodedWriterProvenance = Schema.encodeSync(
                            WriterProvenance.StoredChangeProvenances
                          )(writerProvenance)
                          const relayRetainedSize = relay === undefined
                            ? null
                            : relay.encodedSize + replyBytes +
                              (pendingMessage?.byteLength ?? 0) +
                              new TextEncoder().encode(encodedWriterProvenance).byteLength
                          yield* sql`INSERT INTO effect_local_peer_receipts (
            replica_incarnation, peer_id, connection_epoch, receive_sequence,
            document_id, message_hash, reply, reply_hash, pending_message,
            heads, accepted_heads, commit_sequence, accepted_at, writer_provenance,
            relay_sender_tenant_id, relay_sender_subject_id, relay_sender_peer_id,
            relay_message_id, relay_outer_envelope_digest, relay_receipt_expires_at,
            relay_encoded_size, checkpoint_transfer
          ) VALUES (
            ${receiptSession.replicaIncarnation}, ${receiptSession.peerId}, ${receiptSession.connectionEpoch},
            ${receiveSequence},
            ${documentId}, ${messageHash}, ${firstReply?.message ?? null}, ${firstReply?.messageHash ?? null},
            ${pendingMessage}, ${Schema.encodeSync(Heads)(materializedHeads)},
            ${Schema.encodeSync(Heads)(acceptedHeads)}, ${commitSequence}, ${acceptedAt},
            ${encodedWriterProvenance},
            ${relay?.senderTenantId ?? null}, ${relay?.senderSubjectId ?? null},
            ${relay?.senderPeerId ?? null}, ${relay?.relayMessageId ?? null},
            ${relay?.outerEnvelopeDigest ?? null}, ${relay?.receiptExpiresAt ?? null},
            ${relayRetainedSize}, ${checkpointTransfer ?? null}
          )`
                          const insertedReceipt = (yield* loadReceipt())[0]
                          if (insertedReceipt === undefined) {
                            return yield* new ReplicaError.ReplicaError({
                              reason: new ReplicaError.StorageCorrupt({
                                cause: new Error("Inserted sync receipt is missing")
                              })
                            })
                          }
                          const insertedReplies = replyParts.length === 0
                            ? []
                            : yield* insertReceiptReplies(replyParts.map((part) => ({
                              receipt_row_id: insertedReceipt.row_id,
                              reply_index: part.replyIndex,
                              document_id: documentId,
                              message: part.message,
                              message_hash: part.messageHash,
                              heads: Schema.encodeSync(Heads)(part.heads),
                              status: "Pending"
                            })))
                          if (insertedReplies.length !== replyParts.length) {
                            return yield* new ReplicaError.ReplicaError({
                              reason: new ReplicaError.StorageCorrupt({
                                cause: new Error("Inserted sync receipt replies are missing")
                              })
                            })
                          }
                          const replyIds = new Map(
                            insertedReplies.map((inserted) => [inserted.reply_index, inserted.row_id])
                          )
                          const fragments = replyParts.map((part) => ({
                            receiptReplyId: replyIds.get(part.replyIndex)!,
                            replyIndex: part.replyIndex,
                            message: part.message,
                            messageHash: part.messageHash,
                            heads: part.heads
                          }))
                          const reply: Reply | null = firstReply === undefined
                            ? null
                            : {
                              documentId,
                              message: firstReply.message,
                              messageHash: firstReply.messageHash,
                              heads: firstReply.heads,
                              fragments
                            }
                          if (relay !== undefined) {
                            yield* recordRelayReceiptUsage(
                              receiptSession.replicaIncarnation,
                              relay,
                              relayRetainedSize!
                            )
                          }
                          if (unresolvedBytes !== 0) {
                            yield* validateReceiptQuota
                            yield* validatePendingQuota
                          }
                          return { _tag: "Committed" as const, commitSequence, reply }
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
                          })
                        )
                        if (result._tag === "Committed") {
                          yield* writeState(session, documentId, generated[0])
                        }
                        return result
                      }))
                      if (result._tag === "Duplicate") return result.received
                      return {
                        reply: result.reply,
                        heads: materializedHeads,
                        acceptedHeads,
                        commitSequence: result.commitSequence,
                        observedByPeer: Automerge.hasOurChanges(staged, generated[0]),
                        duplicate: false
                      }
                    }),
                  (durable) => Effect.sync(() => InternalAutomerge.free(durable.automerge))
                )
              })).pipe(
                Effect.retry({
                  times: 8,
                  while: (error) => error._tag === "ConcurrentDocumentWrite"
                }),
                Effect.catchTag("ConcurrentDocumentWrite", () =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.StorageUnavailable({
                        cause: new Error("Document remained busy while applying peer sync")
                      })
                    })
                  ))
              )
            )
          )
        ))

    const pruneRelayReceipts = relayReceiptLimits === null
      ? {}
      : {
        pruneRelayReceipts: Effect.scoped(Effect.gen(function*() {
          const permit = yield* gate.shared
          const expiresAt = new Date(yield* Clock.currentTimeMillis).toISOString()
          return yield* sql.withTransaction(
            quotaLock.withPermit(Effect.gen(function*() {
              const pruned = yield* pruneRelayReceiptsInTransaction(permit.incarnation, expiresAt)
              yield* gate.validate(permit)
              return pruned
            }))
          ).pipe(
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
        }))
      }
    return PeerSync.of({
      open: (peerId) =>
        Effect.scoped(Effect.gen(function*() {
          const permit = yield* gate.shared
          const connectionEpoch = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError((cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({
                  cause
                })
              })
            )
          )
          return { peerId, connectionEpoch, replicaIncarnation: permit.incarnation }
        })),
      reset: (session) =>
        withSessionGeneration(session, (generation) =>
          Effect.scoped(Effect.gen(function*() {
            yield* gate.shared
            yield* sql.withTransaction(quotaLock.withPermit(Effect.gen(function*() {
              yield* sql`DELETE FROM effect_local_peer_outbox
              WHERE replica_incarnation = ${session.replicaIncarnation}
                AND peer_id = ${session.peerId}
                AND connection_epoch = ${session.connectionEpoch}`
              yield* sql`DELETE FROM effect_local_peer_receipts
              WHERE replica_incarnation = ${session.replicaIncarnation}
                AND peer_id = ${session.peerId}
                AND connection_epoch = ${session.connectionEpoch}
                AND relay_message_id IS NULL`
              yield* sql`DELETE FROM effect_local_peer_receipt_replies AS reply
              WHERE receipt_row_id IS NULL
                AND NOT EXISTS (
                  SELECT 1 FROM effect_local_peer_outbox AS outbox
                  WHERE outbox.receipt_reply_id = reply.row_id AND outbox.status = 'Pending'
                )`
              yield* Ref.update(generation, (current) => current + 1)
              yield* removeState(session)
            }))).pipe(Effect.catchTag("SqlError", (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({ cause })
                })
              )))
          }))),
      generate,
      receive,
      enqueue,
      withDocumentInvalidation: (documentId, effect) =>
        withStateLock(
          documentId,
          Effect.uninterruptibleMask((restore) =>
            restore(effect).pipe(Effect.ensuring(removeDocumentState(documentId)))
          )
        ),
      invalidateDocument: (documentId) =>
        // Under the per document lock rather than the session lock: a rewrite is scoped to one
        // document but crosses every session, and taking the same lock `generate` and `receive`
        // take is what stops a state being rewritten back in by a call already in flight.
        withStateLock(documentId, removeDocumentState(documentId)),
      pending: (session) =>
        Effect.scoped(Effect.gen(function*() {
          const permit = yield* gate.shared
          yield* validateSession(permit, session)
          return yield* findPendingOutbox({
            replicaIncarnation: session.replicaIncarnation,
            peerId: session.peerId,
            connectionEpoch: session.connectionEpoch
          }).pipe(
            Effect.map((rows) =>
              rows.map((row) => ({
                sendSequence: row.send_sequence,
                documentId: Identity.DocumentId.make(row.document_id),
                message: row.message,
                messageHash: row.message_hash,
                heads: row.heads,
                lineage: row.lineage,
                writerProvenance: row.writer_provenance,
                ...(row.checkpoint_transfer === null
                  ? {}
                  : { checkpointTransfer: row.checkpoint_transfer }),
                ...(row.receipt_reply_id === null ? {} : { receiptReplyId: row.receipt_reply_id })
              }))
            ),
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
        })),
      ...pruneRelayReceipts,
      markSent: (session, sendSequence, messageHash) =>
        Effect.scoped(Effect.gen(function*() {
          const permit = yield* gate.shared
          yield* validateSession(permit, session)
          return yield* sql.withTransaction(
            quotaLock.withPermit(Effect.gen(function*() {
              const rows = yield* markOutboxSent({
                replicaIncarnation: session.replicaIncarnation,
                peerId: session.peerId,
                connectionEpoch: session.connectionEpoch,
                sendSequence,
                messageHash
              })
              if (rows.length === 0) return false
              const receiptReplyId = rows[0]!.receipt_reply_id
              if (receiptReplyId !== null) {
                const marked = yield* markReceiptReplySent(receiptReplyId)
                if (marked.length === 0) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({
                      cause: new Error(`Missing pending receipt reply ${receiptReplyId}`)
                    })
                  })
                }
                if (marked[0]!.receipt_row_id === null) {
                  yield* sql`DELETE FROM effect_local_peer_receipt_replies AS reply
                    WHERE row_id = ${receiptReplyId} AND receipt_row_id IS NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM effect_local_peer_outbox AS outbox
                        WHERE outbox.receipt_reply_id = reply.row_id AND outbox.status = 'Pending'
                      )`
                }
              }
              yield* sql`DELETE FROM effect_local_peer_outbox
              WHERE replica_incarnation = ${session.replicaIncarnation}
                AND peer_id = ${session.peerId}
                AND connection_epoch = ${session.connectionEpoch}
                AND status = 'Sent'
                AND send_sequence < ${sendSequence}`
              return true
            }))
          ).pipe(
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
        }))
    })
  })

export const layer: Layer.Layer<
  PeerSync,
  ReplicaError.ReplicaError,
  Requirements
> = Layer.effect(PeerSync, make(null))

export const layerRelay: Layer.Layer<
  PeerSync,
  ReplicaError.ReplicaError,
  Requirements | PeerRelayReceiptLimits.PeerRelayReceiptLimits
> = Layer.effect(
  PeerSync,
  Effect.gen(function*() {
    const relayReceiptLimits = yield* PeerRelayReceiptLimits.PeerRelayReceiptLimits
    return yield* make(relayReceiptLimits)
  })
)
