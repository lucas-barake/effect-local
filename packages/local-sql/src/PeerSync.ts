import * as Automerge from "@automerge/automerge"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as PeerTransport from "@lucas-barake/effect-local/PeerTransport"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Equal from "effect/Equal"
import * as Layer from "effect/Layer"
import * as RcMap from "effect/RcMap"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as DocumentStore from "./DocumentStore.js"
import * as InternalAutomerge from "./internal/automerge.js"
import * as HistoryCounters from "./internal/historyCounters.js"
import * as SyncChunks from "./internal/syncChunks.js"
import * as WriterProvenance from "./internal/writerProvenance.js"
import * as PeerRelayReceiptLimits from "./PeerRelayReceiptLimits.js"
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
}

export interface Reply {
  readonly documentId: Identity.DocumentId
  readonly message: Uint8Array
  readonly messageHash: string
  readonly heads: ReadonlyArray<string>
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
  writer_provenance: WriterProvenance.StoredChangeProvenances
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
  document_id: Schema.String,
  heads: Heads,
  lineage: Identity.DocumentLineage,
  message: Schema.Uint8Array,
  message_hash: Schema.String,
  send_sequence: Schema.Number,
  writer_provenance: WriterProvenance.StoredChangeProvenances
})

const DocumentLineageRow = Schema.Struct({
  lineage: Identity.DocumentLineage
})

const ChangeProvenanceRow = Schema.Struct({
  change_hash: WriterProvenance.ChangeHash,
  writer_definition_hash: WriterProvenance.WriterDefinitionHash,
  writer_schema_version: WriterProvenance.WriterSchemaVersion
})

const CheckpointProvenanceRow = Schema.Struct({
  writer_provenance: WriterProvenance.StoredChangeProvenances
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

const SendSequenceRow = Schema.Struct({
  send_sequence: Schema.Number
})

const SequenceRow = Schema.Struct({
  sequence: Schema.Number
})

const sessionKey = (session: Session) => `${session.replicaIncarnation}:${session.peerId}:${session.connectionEpoch}`

const syncStateKey = (session: Session, documentId: Identity.DocumentId) => `${sessionKey(session)}:${documentId}`

const receivedFromReceipt = (documentId: Identity.DocumentId, receipt: typeof ReceiptRow.Type): Received => ({
  reply: receipt.reply === null || receipt.reply_hash === null ? null : {
    documentId,
    message: receipt.reply,
    messageHash: receipt.reply_hash,
    heads: receipt.heads
  },
  heads: receipt.heads,
  acceptedHeads: receipt.accepted_heads,
  commitSequence: Identity.CommitSequence.make(receipt.commit_sequence),
  observedByPeer: false,
  duplicate: true
})

const sameHeads = (left: ReadonlyArray<string>, right: ReadonlyArray<string>) =>
  Equal.equals(left.toSorted(), right.toSorted())

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
    peer: { readonly lineageAware: boolean }
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
      readonly relay?: RelayReceipt
    }
  ) => Effect.Effect<Received, ReplicaError.ReplicaError>
  readonly enqueue: (session: Session, reply: Reply) => Effect.Effect<Outbound, ReplicaError.ReplicaError>
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
        sql`SELECT accepted_heads, commit_sequence, document_id, heads, message_hash, reply, reply_hash,
          writer_provenance
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
        sql`SELECT accepted_heads, commit_sequence, document_id, heads, message_hash, reply, reply_hash,
          writer_provenance, relay_encoded_size, relay_outer_envelope_digest, relay_receipt_expires_at
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
        sql`SELECT document_id, heads, lineage, message, message_hash, send_sequence, writer_provenance
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
        sql`SELECT document_id, heads, lineage, message, message_hash, send_sequence, writer_provenance
          FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND document_id = ${request.documentId}
            AND message_hash = ${request.messageHash}
          ORDER BY send_sequence
          LIMIT 1`
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
        sql`SELECT COALESCE(SUM(LENGTH(message)), 0) AS bytes, COUNT(*) AS count
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
      Result: SendSequenceRow,
      execute: (request) =>
        sql`UPDATE effect_local_peer_outbox
          SET status = 'Sent'
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND send_sequence = ${request.sendSequence}
            AND message_hash = ${request.messageHash}
            AND status = 'Pending'
          RETURNING send_sequence`
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
      yield* sql`DELETE FROM effect_local_peer_receipts
        WHERE relay_message_id IS NULL
          AND (replica_incarnation != ${bootstrap.incarnation} OR accepted_at < ${startupCutoff})`
      yield* sql`DELETE FROM effect_local_peer_outbox
        WHERE replica_incarnation != ${bootstrap.incarnation} OR created_at < ${startupCutoff}`
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

    const loadWriterProvenance = (documentId: Identity.DocumentId, message: Uint8Array) =>
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
        if (changes.length === 0) return []
        const [rows, checkpoints] = yield* Effect.all([
          findChangeProvenance(changes.map((change) => change.hash)),
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
            WriterProvenance.resolve(
              changes.map((change) => change.hash),
              [
                ...rows.map((row) => ({
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
      })

    const persistOutbound = (
      session: Session,
      documentId: Identity.DocumentId,
      message: Uint8Array,
      heads: ReadonlyArray<string>
    ) =>
      Effect.gen(function*() {
        const writerProvenance = yield* loadWriterProvenance(documentId, message)
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
        if ((totals[0]?.bytes ?? 0) + message.byteLength > limits.maxPendingBytesPerPeer) {
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
          message, message_hash, heads, status, created_at, writer_provenance, lineage
        ) VALUES (
          ${session.replicaIncarnation}, ${session.peerId}, ${session.connectionEpoch}, ${documentId}, ${sendSequence},
          ${message}, ${messageHash}, ${Schema.encodeSync(Heads)(heads)}, 'Pending', ${createdAt},
          ${Schema.encodeSync(WriterProvenance.StoredChangeProvenances)(writerProvenance)}, ${lineage}
        )`
        return { sendSequence, documentId, message, messageHash, heads, lineage, writerProvenance } satisfies Outbound
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
          const rows = yield* findOutboxReply({
            replicaIncarnation: session.replicaIncarnation,
            peerId: session.peerId,
            connectionEpoch: session.connectionEpoch,
            documentId: reply.documentId,
            messageHash: reply.messageHash
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
          const existing = rows[0]
          if (existing !== undefined) {
            return {
              sendSequence: existing.send_sequence,
              documentId: reply.documentId,
              message: existing.message,
              messageHash: existing.message_hash,
              heads: existing.heads,
              lineage: existing.lineage,
              writerProvenance: existing.writer_provenance
            }
          }
          return yield* persistOutbound(session, reply.documentId, reply.message, reply.heads).pipe(
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
        }))).pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({ cause })
              })
            ))
        )
      }))

    const generate = <D extends Document.Any,>(
      document: D,
      documentId: Identity.DocumentId,
      session: Session,
      peer: { readonly lineageAware: boolean }
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
                    if (generated[1].byteLength > limits.maxSyncMessageBytes) {
                      return yield* new ReplicaError.ReplicaError({
                        reason: new ReplicaError.QuotaExceeded({
                          resource: "sync message bytes",
                          limit: limits.maxSyncMessageBytes
                        })
                      })
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
                        generated[1]!,
                        durable.materializedHeads
                      )
                      yield* writeState(session, documentId, generated[0])
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
                if (localLineage !== remoteLineage) {
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
                const receiptRows = yield* loadReceipt().pipe(
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
                const receipt = receiptRows[0]
                if (receipt !== undefined) {
                  yield* validateStoredReceipt(receipt)
                  yield* quotaLock.withPermit(validateSessionGeneration(generation, sessionGeneration))
                  return receivedFromReceipt(documentId, receipt)
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
                const validateRelayReceiptQuota = relay === undefined
                  ? Effect.void
                  : Effect.gen(function*() {
                    const remote = (yield* findRelayReceiptUsage({
                      replicaIncarnation: receiptSession.replicaIncarnation,
                      senderPeerId: relay.senderPeerId,
                      senderSubjectId: relay.senderSubjectId,
                      senderTenantId: relay.senderTenantId
                    }))[0]
                    const replica = (yield* findRelayReplicaReceiptUsage(
                      receiptSession.replicaIncarnation
                    ))[0]
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
                      for (const entry of checkpointProvenanceRows.flatMap((row) => row.writer_provenance)) {
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
                      if (generated[1] !== null && generated[1].byteLength > limits.maxSyncMessageBytes) {
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
                            return { _tag: "Duplicate" as const, received: receivedFromReceipt(documentId, receipt) }
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
                          const reply = generated[1] === null
                            ? null
                            : {
                              documentId,
                              message: generated[1],
                              messageHash: yield* digest(generated[1]),
                              heads: materializedHeads
                            }
                          const pendingMessage = unresolvedBytes === 0 ? null : message
                          const encodedWriterProvenance = Schema.encodeSync(
                            WriterProvenance.StoredChangeProvenances
                          )(writerProvenance)
                          const relayRetainedSize = relay === undefined
                            ? null
                            : relay.encodedSize +
                              (reply?.message.byteLength ?? 0) +
                              (pendingMessage?.byteLength ?? 0) +
                              new TextEncoder().encode(encodedWriterProvenance).byteLength
                          yield* sql`INSERT INTO effect_local_peer_receipts (
            replica_incarnation, peer_id, connection_epoch, receive_sequence,
            document_id, message_hash, reply, reply_hash, pending_message,
            heads, accepted_heads, commit_sequence, accepted_at, writer_provenance,
            relay_sender_tenant_id, relay_sender_subject_id, relay_sender_peer_id,
            relay_message_id, relay_outer_envelope_digest, relay_receipt_expires_at,
            relay_encoded_size
          ) VALUES (
            ${receiptSession.replicaIncarnation}, ${receiptSession.peerId}, ${receiptSession.connectionEpoch},
            ${receiveSequence},
            ${documentId}, ${messageHash}, ${reply?.message ?? null}, ${reply?.messageHash ?? null},
            ${pendingMessage}, ${Schema.encodeSync(Heads)(materializedHeads)},
            ${Schema.encodeSync(Heads)(acceptedHeads)}, ${commitSequence}, ${acceptedAt},
            ${encodedWriterProvenance},
            ${relay?.senderTenantId ?? null}, ${relay?.senderSubjectId ?? null},
            ${relay?.senderPeerId ?? null}, ${relay?.relayMessageId ?? null},
            ${relay?.outerEnvelopeDigest ?? null}, ${relay?.receiptExpiresAt ?? null},
            ${relayRetainedSize}
          )`
                          if (relay !== undefined) {
                            yield* sql`INSERT INTO effect_local_peer_relay_receipt_usage (
              replica_incarnation, sender_tenant_id, sender_subject_id, sender_peer_id,
              receipt_count, encoded_bytes
            ) VALUES (
              ${receiptSession.replicaIncarnation}, ${relay.senderTenantId}, ${relay.senderSubjectId},
              ${relay.senderPeerId}, 1, ${relayRetainedSize!}
            ) ON CONFLICT(replica_incarnation, sender_tenant_id, sender_subject_id, sender_peer_id)
            DO UPDATE SET
              receipt_count = effect_local_peer_relay_receipt_usage.receipt_count + 1,
              encoded_bytes = effect_local_peer_relay_receipt_usage.encoded_bytes + excluded.encoded_bytes`
                            yield* validateRelayReceiptQuota
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
                writerProvenance: row.writer_provenance
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
