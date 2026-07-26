import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import type * as SchemaError from "effect/SchemaError"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as WriterProvenance from "./internal/writerProvenance.js"
import * as PeerRelayOutboxLimits from "./PeerRelayOutboxLimits.js"
import * as PeerSyncEnvelope from "./PeerSyncEnvelope.js"
import * as ReplicaGate from "./ReplicaGate.js"

export interface Endpoint {
  readonly expectedLocal: PeerSyncEnvelope.RelayPeerPrincipal
  readonly remote: PeerSyncEnvelope.RelayPeerPrincipal
  readonly relayPeerId: Identity.PeerId
}

export interface AdmitInput extends Endpoint {
  readonly payload: Uint8Array
  readonly retryHorizonMillis: number
}

export interface ReplayInput extends Endpoint {
  readonly maximum: number
}

export interface CustodyInput {
  readonly relayMessageId: Identity.RelayMessageId
  readonly outerEnvelopeDigest: string
}

export interface Entry extends Endpoint {
  readonly rowId: number
  readonly replicaId: Identity.ReplicaId
  readonly replicaIncarnation: Identity.ReplicaIncarnation
  readonly writerGeneration: Identity.WriterGeneration
  readonly relayMessageId: Identity.RelayMessageId
  readonly outerEnvelopeDigest: string
  readonly protocolVersion: typeof PeerSyncEnvelope.relayProtocolVersion
  readonly payloadVersion: typeof PeerSyncEnvelope.syncEnvelopeVersion
  readonly senderConnectionEpoch: string
  readonly senderSequence: number
  readonly document: {
    readonly documentId: Identity.DocumentId
    readonly documentType: string
  }
  readonly writerProvenance: PeerSyncEnvelope.RelayOuterEnvelope["writerProvenance"]
  readonly messageHash: string
  readonly payload: Uint8Array
  readonly encodedSize: number
  readonly createdAt: string
  readonly retryDeadline: string
  readonly nextAttemptAt: string
}

export interface Usage {
  readonly remote: {
    readonly messageCount: number
    readonly encodedBytes: number
  }
  readonly replica: {
    readonly messageCount: number
    readonly encodedBytes: number
  }
}

const RelayDigest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/))
const IsoDate = Schema.String
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

const EndpointSchema = Schema.Struct({
  expectedLocal: PeerSyncEnvelope.RelayPeerPrincipal,
  remote: PeerSyncEnvelope.RelayPeerPrincipal,
  relayPeerId: Identity.PeerId
})

const Row = Schema.Struct({
  row_id: PositiveInt,
  replica_id: Identity.ReplicaId,
  replica_incarnation: Identity.ReplicaIncarnation,
  writer_generation: Identity.WriterGeneration,
  expected_local_tenant_id: Schema.String,
  expected_local_subject_id: Schema.String,
  expected_local_peer_id: Identity.PeerId,
  remote_tenant_id: Schema.String,
  remote_subject_id: Schema.String,
  remote_peer_id: Identity.PeerId,
  relay_peer_id: Identity.PeerId,
  relay_message_id: Identity.RelayMessageId,
  outer_envelope_digest: RelayDigest,
  protocol_version: Schema.Literal(PeerSyncEnvelope.relayProtocolVersion),
  payload_version: Schema.Literal(PeerSyncEnvelope.syncEnvelopeVersion),
  sender_connection_epoch: Schema.String,
  sender_sequence: NonNegativeInt,
  document_id: Identity.DocumentId,
  document_type: Schema.String,
  writer_provenance: WriterProvenance.StoredChangeProvenances,
  message_hash: RelayDigest,
  payload: Schema.Uint8Array,
  encoded_size: PositiveInt,
  created_at: IsoDate,
  retry_deadline: IsoDate,
  next_attempt_at: IsoDate,
  custody_state: Schema.Literals(["Pending", "InFlight"])
})

const RowMetadata = Schema.Struct({
  row_id: PositiveInt,
  encoded_size: PositiveInt,
  actual_size: PositiveInt
})

const InsertedRow = Schema.Struct({ row_id: PositiveInt })
const UsageRow = Schema.Struct({
  message_count: NonNegativeInt,
  encoded_bytes: NonNegativeInt
})
const HorizonRow = Schema.Struct({ horizon_millis: Schema.NullOr(Schema.Number) })
const replayRowBatchSize = 500

const replicaFailure = (reason: ReplicaError.Reason): ReplicaError.ReplicaError =>
  new ReplicaError.ReplicaError({ reason })

const storageUnavailable = (cause: unknown) =>
  Effect.fail(replicaFailure(new ReplicaError.StorageUnavailable({ cause })))

const storageCorrupt = (cause: unknown) => Effect.fail(replicaFailure(new ReplicaError.StorageCorrupt({ cause })))

const protocolMismatch = (expected: string, observed: string) =>
  replicaFailure(new ReplicaError.ProtocolMismatch({ expected, observed }))

const quotaExceeded = (resource: string, limit: number) =>
  replicaFailure(new ReplicaError.QuotaExceeded({ resource, limit }))

const parseIso = (value: string): number | null => {
  const millis = Date.parse(value)
  return Number.isFinite(millis) && new Date(millis).toISOString() === value ? millis : null
}

export class PeerRelayOutbox extends Context.Service<PeerRelayOutbox, {
  readonly admit: (input: AdmitInput) => Effect.Effect<Entry, ReplicaError.ReplicaError>
  readonly dueForEndpoint: (
    input: ReplayInput
  ) => Effect.Effect<ReadonlyArray<Entry>, ReplicaError.ReplicaError>
  readonly maximumPendingHorizon: (
    endpoint: Endpoint
  ) => Effect.Effect<number | null, ReplicaError.ReplicaError>
  readonly markCustody: (input: CustodyInput) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly pruneExpired: Effect.Effect<number, ReplicaError.ReplicaError>
  readonly usage: (endpoint: Endpoint) => Effect.Effect<Usage, ReplicaError.ReplicaError>
  readonly validateReplicaIncarnation: (
    expected: Identity.ReplicaIncarnation
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/PeerRelayOutbox") {}

type Requirements =
  | SqlClient.SqlClient
  | Crypto.Crypto
  | ReplicaLimits.ReplicaLimits
  | ReplicaGate.ReplicaGate
  | PeerRelayOutboxLimits.PeerRelayOutboxLimits

const make = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  const crypto = yield* Crypto.Crypto
  const replicaLimits = yield* ReplicaLimits.ReplicaLimits
  const gate = yield* ReplicaGate.ReplicaGate
  const limits = yield* PeerRelayOutboxLimits.PeerRelayOutboxLimits

  const decodeEndpoint = (input: Endpoint) =>
    Schema.decodeUnknownEffect(EndpointSchema)(input).pipe(
      Effect.mapError(() => protocolMismatch("valid relay endpoint", "invalid relay endpoint"))
    )

  const findSource = SqlSchema.findAll({
    Request: Schema.Struct({
      replicaId: Identity.ReplicaId,
      replicaIncarnation: Identity.ReplicaIncarnation,
      relayPeerId: Identity.PeerId,
      remoteTenantId: Schema.String,
      remoteSubjectId: Schema.String,
      remotePeerId: Identity.PeerId,
      senderConnectionEpoch: Schema.String,
      senderSequence: NonNegativeInt
    }),
    Result: Row,
    execute: (request) =>
      sql`SELECT *
        FROM effect_local_peer_relay_outbox
        WHERE replica_id = ${request.replicaId}
          AND replica_incarnation = ${request.replicaIncarnation}
          AND relay_peer_id = ${request.relayPeerId}
          AND remote_tenant_id = ${request.remoteTenantId}
          AND remote_subject_id = ${request.remoteSubjectId}
          AND remote_peer_id = ${request.remotePeerId}
          AND sender_connection_epoch = ${request.senderConnectionEpoch}
          AND sender_sequence = ${request.senderSequence}`
  })

  const findRow = SqlSchema.findAll({
    Request: Schema.Struct({
      replicaId: Identity.ReplicaId,
      replicaIncarnation: Identity.ReplicaIncarnation,
      relayMessageId: Identity.RelayMessageId
    }),
    Result: Row,
    execute: (request) =>
      sql`SELECT *
        FROM effect_local_peer_relay_outbox
        WHERE replica_id = ${request.replicaId}
          AND replica_incarnation = ${request.replicaIncarnation}
          AND relay_message_id = ${request.relayMessageId}`
  })

  const findDueMetadata = SqlSchema.findAll({
    Request: Schema.Struct({
      replicaId: Identity.ReplicaId,
      replicaIncarnation: Identity.ReplicaIncarnation,
      relayPeerId: Identity.PeerId,
      expectedLocalTenantId: Schema.String,
      expectedLocalSubjectId: Schema.String,
      expectedLocalPeerId: Identity.PeerId,
      remoteTenantId: Schema.String,
      remoteSubjectId: Schema.String,
      remotePeerId: Identity.PeerId,
      now: Schema.String,
      maximum: PositiveInt
    }),
    Result: RowMetadata,
    execute: (request) =>
      sql`SELECT row_id, encoded_size, length(payload) AS actual_size
        FROM effect_local_peer_relay_outbox
        WHERE replica_id = ${request.replicaId}
          AND replica_incarnation = ${request.replicaIncarnation}
          AND relay_peer_id = ${request.relayPeerId}
          AND expected_local_tenant_id = ${request.expectedLocalTenantId}
          AND expected_local_subject_id = ${request.expectedLocalSubjectId}
          AND expected_local_peer_id = ${request.expectedLocalPeerId}
          AND remote_tenant_id = ${request.remoteTenantId}
          AND remote_subject_id = ${request.remoteSubjectId}
          AND remote_peer_id = ${request.remotePeerId}
          AND custody_state = 'Pending'
          AND next_attempt_at <= ${request.now}
          AND retry_deadline > ${request.now}
        ORDER BY next_attempt_at, row_id
        LIMIT ${request.maximum}`
  })

  const findByRowIds = SqlSchema.findAll({
    Request: Schema.Struct({
      replicaId: Identity.ReplicaId,
      replicaIncarnation: Identity.ReplicaIncarnation,
      rowIds: Schema.Array(PositiveInt).check(Schema.isMinLength(1))
    }),
    Result: Row,
    execute: (request) =>
      sql`SELECT *
        FROM effect_local_peer_relay_outbox
        WHERE replica_id = ${request.replicaId}
          AND replica_incarnation = ${request.replicaIncarnation}
          AND ${sql.in("row_id", request.rowIds)}`
  })

  const insertRow = SqlSchema.findAll({
    Request: Schema.Struct({
      replicaId: Identity.ReplicaId,
      replicaIncarnation: Identity.ReplicaIncarnation,
      writerGeneration: Identity.WriterGeneration,
      expectedLocalTenantId: Schema.String,
      expectedLocalSubjectId: Schema.String,
      expectedLocalPeerId: Identity.PeerId,
      remoteTenantId: Schema.String,
      remoteSubjectId: Schema.String,
      remotePeerId: Identity.PeerId,
      relayPeerId: Identity.PeerId,
      relayMessageId: Identity.RelayMessageId,
      outerEnvelopeDigest: RelayDigest,
      senderConnectionEpoch: Schema.String,
      senderSequence: NonNegativeInt,
      documentId: Identity.DocumentId,
      documentType: Schema.String,
      writerProvenance: WriterProvenance.StoredChangeProvenances,
      messageHash: RelayDigest,
      payload: Schema.Uint8Array,
      encodedSize: PositiveInt,
      createdAt: Schema.String,
      retryDeadline: Schema.String,
      nextAttemptAt: Schema.String
    }),
    Result: InsertedRow,
    execute: (request) =>
      sql`INSERT INTO effect_local_peer_relay_outbox (
        replica_id, replica_incarnation, writer_generation,
        expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
        remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
        relay_message_id, outer_envelope_digest, protocol_version, payload_version,
        sender_connection_epoch, sender_sequence, document_id, document_type,
        writer_provenance, message_hash, payload, encoded_size,
        created_at, retry_deadline, next_attempt_at, custody_state
      ) VALUES (
        ${request.replicaId}, ${request.replicaIncarnation}, ${request.writerGeneration},
        ${request.expectedLocalTenantId}, ${request.expectedLocalSubjectId}, ${request.expectedLocalPeerId},
        ${request.remoteTenantId}, ${request.remoteSubjectId}, ${request.remotePeerId}, ${request.relayPeerId},
        ${request.relayMessageId}, ${request.outerEnvelopeDigest},
        ${PeerSyncEnvelope.relayProtocolVersion}, ${PeerSyncEnvelope.syncEnvelopeVersion},
        ${request.senderConnectionEpoch}, ${request.senderSequence}, ${request.documentId},
        ${request.documentType}, ${request.writerProvenance}, ${request.messageHash},
        ${request.payload}, ${request.encodedSize}, ${request.createdAt}, ${request.retryDeadline},
        ${request.nextAttemptAt}, 'Pending'
      ) RETURNING row_id`
  })

  const reserveRemote = SqlSchema.findAll({
    Request: Schema.Struct({
      replicaIncarnation: Identity.ReplicaIncarnation,
      remoteTenantId: Schema.String,
      remoteSubjectId: Schema.String,
      remotePeerId: Identity.PeerId,
      encodedSize: PositiveInt,
      maxMessages: PositiveInt,
      maxBytes: PositiveInt
    }),
    Result: UsageRow,
    execute: (request) =>
      sql`INSERT INTO effect_local_peer_relay_outbox_remote_usage (
        replica_incarnation, remote_tenant_id, remote_subject_id, remote_peer_id,
        message_count, encoded_bytes
      ) VALUES (
        ${request.replicaIncarnation}, ${request.remoteTenantId}, ${request.remoteSubjectId},
        ${request.remotePeerId}, 1, ${request.encodedSize}
      )
      ON CONFLICT (replica_incarnation, remote_tenant_id, remote_subject_id, remote_peer_id)
      DO UPDATE SET
        message_count = message_count + 1,
        encoded_bytes = encoded_bytes + excluded.encoded_bytes
      WHERE message_count + 1 <= ${request.maxMessages}
        AND encoded_bytes + excluded.encoded_bytes <= ${request.maxBytes}
      RETURNING message_count, encoded_bytes`
  })

  const reserveReplica = SqlSchema.findAll({
    Request: Schema.Struct({
      replicaIncarnation: Identity.ReplicaIncarnation,
      encodedSize: PositiveInt,
      maxMessages: PositiveInt,
      maxBytes: PositiveInt
    }),
    Result: UsageRow,
    execute: (request) =>
      sql`INSERT INTO effect_local_peer_relay_outbox_replica_usage (
        replica_incarnation, message_count, encoded_bytes
      ) VALUES (${request.replicaIncarnation}, 1, ${request.encodedSize})
      ON CONFLICT (replica_incarnation)
      DO UPDATE SET
        message_count = message_count + 1,
        encoded_bytes = encoded_bytes + excluded.encoded_bytes
      WHERE message_count + 1 <= ${request.maxMessages}
        AND encoded_bytes + excluded.encoded_bytes <= ${request.maxBytes}
      RETURNING message_count, encoded_bytes`
  })

  const decrementRemote = SqlSchema.findAll({
    Request: Schema.Struct({
      replicaIncarnation: Identity.ReplicaIncarnation,
      remoteTenantId: Schema.String,
      remoteSubjectId: Schema.String,
      remotePeerId: Identity.PeerId,
      encodedSize: PositiveInt
    }),
    Result: UsageRow,
    execute: (request) =>
      sql`UPDATE effect_local_peer_relay_outbox_remote_usage
        SET message_count = message_count - 1,
          encoded_bytes = encoded_bytes - ${request.encodedSize}
        WHERE replica_incarnation = ${request.replicaIncarnation}
          AND remote_tenant_id = ${request.remoteTenantId}
          AND remote_subject_id = ${request.remoteSubjectId}
          AND remote_peer_id = ${request.remotePeerId}
          AND message_count >= 1
          AND encoded_bytes >= ${request.encodedSize}
        RETURNING message_count, encoded_bytes`
  })

  const decrementReplica = SqlSchema.findAll({
    Request: Schema.Struct({
      replicaIncarnation: Identity.ReplicaIncarnation,
      encodedSize: PositiveInt
    }),
    Result: UsageRow,
    execute: (request) =>
      sql`UPDATE effect_local_peer_relay_outbox_replica_usage
        SET message_count = message_count - 1,
          encoded_bytes = encoded_bytes - ${request.encodedSize}
        WHERE replica_incarnation = ${request.replicaIncarnation}
          AND message_count >= 1
          AND encoded_bytes >= ${request.encodedSize}
        RETURNING message_count, encoded_bytes`
  })

  const decrementUsage = (row: typeof Row.Type) =>
    Effect.gen(function*() {
      const remote = yield* decrementRemote({
        replicaIncarnation: row.replica_incarnation,
        remoteTenantId: row.remote_tenant_id,
        remoteSubjectId: row.remote_subject_id,
        remotePeerId: row.remote_peer_id,
        encodedSize: row.encoded_size
      })
      const replica = yield* decrementReplica({
        replicaIncarnation: row.replica_incarnation,
        encodedSize: row.encoded_size
      })
      if (remote.length !== 1 || replica.length !== 1) {
        return yield* storageCorrupt(new Error("Relay outbox usage reservation mismatch"))
      }
      yield* sql`DELETE FROM effect_local_peer_relay_outbox_remote_usage
        WHERE replica_incarnation = ${row.replica_incarnation}
          AND remote_tenant_id = ${row.remote_tenant_id}
          AND remote_subject_id = ${row.remote_subject_id}
          AND remote_peer_id = ${row.remote_peer_id}
          AND message_count = 0
          AND encoded_bytes = 0`
      yield* sql`DELETE FROM effect_local_peer_relay_outbox_replica_usage
        WHERE replica_incarnation = ${row.replica_incarnation}
          AND message_count = 0
          AND encoded_bytes = 0`
    })

  const validateRow = (
    row: typeof Row.Type,
    permit: ReplicaGate.Permit
  ): Effect.Effect<Entry, ReplicaError.ReplicaError> =>
    Effect.gen(function*() {
      if (
        row.replica_incarnation !== permit.incarnation ||
        row.replica_id !== permit.replicaId ||
        row.encoded_size !== row.payload.byteLength
      ) {
        return yield* storageCorrupt(new Error("Relay outbox metadata mismatch"))
      }
      const createdAt = parseIso(row.created_at)
      const retryDeadline = parseIso(row.retry_deadline)
      const nextAttemptAt = parseIso(row.next_attempt_at)
      if (
        createdAt === null ||
        retryDeadline === null ||
        nextAttemptAt === null ||
        retryDeadline <= createdAt ||
        retryDeadline - createdAt > limits.maxRetryHorizonMillis ||
        nextAttemptAt < createdAt ||
        nextAttemptAt >= retryDeadline
      ) {
        return yield* storageCorrupt(new Error("Relay outbox deadline mismatch"))
      }
      const syncEnvelope = yield* PeerSyncEnvelope.decodeSyncEnvelope(row.payload, replicaLimits).pipe(
        Effect.provideService(Crypto.Crypto, crypto)
      )
      if (
        syncEnvelope.connectionEpoch !== row.sender_connection_epoch ||
        syncEnvelope.sequence !== row.sender_sequence ||
        syncEnvelope.documentId !== row.document_id ||
        syncEnvelope.documentType !== row.document_type ||
        syncEnvelope.messageHash !== row.message_hash
      ) {
        return yield* storageCorrupt(new Error("Relay outbox payload metadata mismatch"))
      }
      const outerEnvelope: PeerSyncEnvelope.RelayOuterEnvelope = {
        domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
        version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
        expectedLocal: {
          tenantId: row.expected_local_tenant_id,
          subjectId: row.expected_local_subject_id,
          peerId: row.expected_local_peer_id
        },
        remote: {
          tenantId: row.remote_tenant_id,
          subjectId: row.remote_subject_id,
          peerId: row.remote_peer_id
        },
        relayPeerId: row.relay_peer_id,
        relayMessageId: row.relay_message_id,
        protocolVersion: row.protocol_version,
        payloadVersion: row.payload_version,
        senderReplicaIncarnation: row.replica_incarnation,
        senderConnectionEpoch: row.sender_connection_epoch,
        senderSequence: row.sender_sequence,
        document: {
          documentId: row.document_id,
          documentType: row.document_type
        },
        lineage: syncEnvelope.lineage,
        writerProvenance: row.writer_provenance,
        messageHash: row.message_hash,
        payload: row.payload
      }
      const digest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope(outerEnvelope).pipe(
        Effect.provideService(Crypto.Crypto, crypto)
      )
      if (digest !== row.outer_envelope_digest) {
        return yield* storageCorrupt(new Error("Relay outbox digest mismatch"))
      }
      return {
        rowId: row.row_id,
        replicaId: row.replica_id,
        replicaIncarnation: row.replica_incarnation,
        writerGeneration: row.writer_generation,
        expectedLocal: outerEnvelope.expectedLocal,
        remote: outerEnvelope.remote,
        relayPeerId: row.relay_peer_id,
        relayMessageId: row.relay_message_id,
        outerEnvelopeDigest: row.outer_envelope_digest,
        protocolVersion: row.protocol_version,
        payloadVersion: row.payload_version,
        senderConnectionEpoch: row.sender_connection_epoch,
        senderSequence: row.sender_sequence,
        document: outerEnvelope.document,
        writerProvenance: outerEnvelope.writerProvenance,
        messageHash: row.message_hash,
        payload: row.payload,
        encodedSize: row.encoded_size,
        createdAt: row.created_at,
        retryDeadline: row.retry_deadline,
        nextAttemptAt: row.next_attempt_at
      }
    })

  const mapStorageFailures = <A, R,>(
    effect: Effect.Effect<
      A,
      ReplicaError.ReplicaError | SchemaError.SchemaError | SqlError.SqlError,
      R
    >
  ): Effect.Effect<A, ReplicaError.ReplicaError, R> =>
    effect.pipe(
      Effect.catchTags({
        SqlError: storageUnavailable,
        SchemaError: storageCorrupt
      })
    )

  const validateReplicaIncarnation = (expected: Identity.ReplicaIncarnation) =>
    gate.current.pipe(
      Effect.flatMap((permit) =>
        permit.incarnation === expected
          ? Effect.void
          : Effect.fail(protocolMismatch("current replica incarnation", "stale replica incarnation"))
      )
    )

  const admit = (input: AdmitInput): Effect.Effect<Entry, ReplicaError.ReplicaError> =>
    Effect.scoped(Effect.gen(function*() {
      const endpoint = yield* decodeEndpoint(input)
      if (
        !Number.isSafeInteger(input.retryHorizonMillis) ||
        input.retryHorizonMillis <= 0 ||
        input.retryHorizonMillis > limits.maxRetryHorizonMillis
      ) {
        return yield* Effect.fail(
          protocolMismatch(
            `retry horizon from 1 through ${limits.maxRetryHorizonMillis}`,
            "invalid retry horizon"
          )
        )
      }
      const permit = yield* gate.shared
      const syncEnvelope = yield* PeerSyncEnvelope.decodeSyncEnvelope(input.payload, replicaLimits).pipe(
        Effect.provideService(Crypto.Crypto, crypto)
      )
      const relayMessageId = yield* Identity.makeRelayMessageId.pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.mapError((cause) => replicaFailure(new ReplicaError.StorageUnavailable({ cause })))
      )
      const outerEnvelope: PeerSyncEnvelope.RelayOuterEnvelope = {
        domain: PeerSyncEnvelope.relayOuterEnvelopeDomain,
        version: PeerSyncEnvelope.relayOuterEnvelopeVersion,
        expectedLocal: endpoint.expectedLocal,
        remote: endpoint.remote,
        relayPeerId: endpoint.relayPeerId,
        relayMessageId,
        protocolVersion: PeerSyncEnvelope.relayProtocolVersion,
        payloadVersion: PeerSyncEnvelope.syncEnvelopeVersion,
        senderReplicaIncarnation: permit.incarnation,
        senderConnectionEpoch: syncEnvelope.connectionEpoch,
        senderSequence: syncEnvelope.sequence,
        document: PeerSyncEnvelope.syncEnvelopeDocument(syncEnvelope),
        lineage: syncEnvelope.lineage,
        writerProvenance: syncEnvelope.writerProvenance,
        messageHash: syncEnvelope.messageHash,
        payload: input.payload
      }
      const outerEnvelopeDigest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope(outerEnvelope).pipe(
        Effect.provideService(Crypto.Crypto, crypto)
      )
      const nowMillis = yield* Clock.currentTimeMillis
      const createdAt = new Date(nowMillis).toISOString()
      const retryDeadline = new Date(nowMillis + input.retryHorizonMillis).toISOString()
      const request = {
        replicaId: permit.replicaId,
        replicaIncarnation: permit.incarnation,
        relayPeerId: endpoint.relayPeerId,
        remoteTenantId: endpoint.remote.tenantId,
        remoteSubjectId: endpoint.remote.subjectId,
        remotePeerId: endpoint.remote.peerId,
        senderConnectionEpoch: syncEnvelope.connectionEpoch,
        senderSequence: syncEnvelope.sequence
      }
      return yield* mapStorageFailures(sql.withTransaction(Effect.gen(function*() {
        const existing = yield* findSource(request)
        if (existing.length > 1) {
          return yield* storageCorrupt(new Error("Duplicate relay source operation"))
        }
        if (existing.length === 1) {
          const entry = yield* validateRow(existing[0]!, permit)
          const existingEnvelope: PeerSyncEnvelope.RelayOuterEnvelope = {
            ...outerEnvelope,
            relayMessageId: entry.relayMessageId
          }
          const existingDigest = yield* PeerSyncEnvelope.digestRelayOuterEnvelope(existingEnvelope).pipe(
            Effect.provideService(Crypto.Crypto, crypto)
          )
          const existingCreatedAt = parseIso(entry.createdAt)
          const existingRetryDeadline = parseIso(entry.retryDeadline)
          if (
            entry.outerEnvelopeDigest !== existingDigest ||
            existingCreatedAt === null ||
            existingRetryDeadline === null ||
            existingRetryDeadline - existingCreatedAt !== input.retryHorizonMillis
          ) {
            return yield* Effect.fail(
              protocolMismatch("stable relay source operation", "conflicting relay source operation")
            )
          }
          yield* gate.validate(permit)
          return entry
        }
        const remoteUsage = yield* reserveRemote({
          replicaIncarnation: permit.incarnation,
          remoteTenantId: endpoint.remote.tenantId,
          remoteSubjectId: endpoint.remote.subjectId,
          remotePeerId: endpoint.remote.peerId,
          encodedSize: input.payload.byteLength,
          maxMessages: limits.maxMessagesPerRemote,
          maxBytes: limits.maxEncodedBytesPerRemote
        })
        if (remoteUsage.length !== 1) {
          return yield* Effect.fail(
            quotaExceeded("relay outbox remote quota", limits.maxMessagesPerRemote)
          )
        }
        const replicaUsage = yield* reserveReplica({
          replicaIncarnation: permit.incarnation,
          encodedSize: input.payload.byteLength,
          maxMessages: limits.maxMessagesPerReplica,
          maxBytes: limits.maxEncodedBytesPerReplica
        })
        if (replicaUsage.length !== 1) {
          return yield* Effect.fail(
            quotaExceeded("relay outbox replica quota", limits.maxMessagesPerReplica)
          )
        }
        const inserted = yield* insertRow({
          replicaIncarnation: permit.incarnation,
          replicaId: permit.replicaId,
          writerGeneration: permit.writerGeneration,
          expectedLocalTenantId: endpoint.expectedLocal.tenantId,
          expectedLocalSubjectId: endpoint.expectedLocal.subjectId,
          expectedLocalPeerId: endpoint.expectedLocal.peerId,
          remoteTenantId: endpoint.remote.tenantId,
          remoteSubjectId: endpoint.remote.subjectId,
          remotePeerId: endpoint.remote.peerId,
          relayPeerId: endpoint.relayPeerId,
          relayMessageId,
          outerEnvelopeDigest,
          senderConnectionEpoch: syncEnvelope.connectionEpoch,
          senderSequence: syncEnvelope.sequence,
          documentId: syncEnvelope.documentId,
          documentType: syncEnvelope.documentType,
          writerProvenance: syncEnvelope.writerProvenance,
          messageHash: syncEnvelope.messageHash,
          payload: input.payload,
          encodedSize: input.payload.byteLength,
          createdAt,
          retryDeadline,
          nextAttemptAt: createdAt
        })
        if (inserted.length !== 1) {
          return yield* storageCorrupt(new Error("Relay outbox insert did not return one row"))
        }
        yield* gate.validate(permit)
        return {
          rowId: inserted[0]!.row_id,
          replicaId: permit.replicaId,
          replicaIncarnation: permit.incarnation,
          writerGeneration: permit.writerGeneration,
          expectedLocal: endpoint.expectedLocal,
          remote: endpoint.remote,
          relayPeerId: endpoint.relayPeerId,
          relayMessageId,
          outerEnvelopeDigest,
          protocolVersion: PeerSyncEnvelope.relayProtocolVersion as typeof PeerSyncEnvelope.relayProtocolVersion,
          payloadVersion: PeerSyncEnvelope.syncEnvelopeVersion as typeof PeerSyncEnvelope.syncEnvelopeVersion,
          senderConnectionEpoch: syncEnvelope.connectionEpoch,
          senderSequence: syncEnvelope.sequence,
          document: PeerSyncEnvelope.syncEnvelopeDocument(syncEnvelope),
          writerProvenance: syncEnvelope.writerProvenance,
          messageHash: syncEnvelope.messageHash,
          payload: input.payload,
          encodedSize: input.payload.byteLength,
          createdAt,
          retryDeadline,
          nextAttemptAt: createdAt
        }
      })))
    }))

  const dueForEndpoint = (input: ReplayInput) =>
    Effect.scoped(Effect.gen(function*() {
      const endpoint = yield* decodeEndpoint(input)
      if (
        !Number.isSafeInteger(input.maximum) ||
        input.maximum <= 0 ||
        input.maximum > limits.maxMessagesPerRemote
      ) {
        return yield* Effect.fail(
          protocolMismatch(
            `replay maximum from 1 through ${limits.maxMessagesPerRemote}`,
            "invalid replay maximum"
          )
        )
      }
      const permit = yield* gate.shared
      const now = new Date(yield* Clock.currentTimeMillis).toISOString()
      return yield* mapStorageFailures(sql.withTransaction(Effect.gen(function*() {
        const metadata = yield* findDueMetadata({
          replicaId: permit.replicaId,
          replicaIncarnation: permit.incarnation,
          relayPeerId: endpoint.relayPeerId,
          expectedLocalTenantId: endpoint.expectedLocal.tenantId,
          expectedLocalSubjectId: endpoint.expectedLocal.subjectId,
          expectedLocalPeerId: endpoint.expectedLocal.peerId,
          remoteTenantId: endpoint.remote.tenantId,
          remoteSubjectId: endpoint.remote.subjectId,
          remotePeerId: endpoint.remote.peerId,
          now,
          maximum: input.maximum
        })
        if (metadata.length === 0) return []
        const rows: Array<typeof Row.Type> = []
        for (let offset = 0; offset < metadata.length; offset += replayRowBatchSize) {
          rows.push(
            ...yield* findByRowIds({
              replicaId: permit.replicaId,
              replicaIncarnation: permit.incarnation,
              rowIds: metadata.slice(offset, offset + replayRowBatchSize).map((item) => item.row_id)
            })
          )
        }
        const rowsById = new Map(rows.map((row) => [row.row_id, row]))
        const entries: Array<Entry> = []
        for (const item of metadata) {
          if (
            item.actual_size !== item.encoded_size ||
            item.actual_size > PeerSyncEnvelope.maximumSyncEnvelopeBytes(
                replicaLimits.maxSyncMessageBytes,
                replicaLimits.maxSyncChangesPerMessage
              )
          ) {
            return yield* storageCorrupt(new Error("Relay outbox payload length mismatch"))
          }
          const row = rowsById.get(item.row_id)
          if (row === undefined) {
            return yield* storageCorrupt(new Error("Relay outbox row disappeared during replay"))
          }
          const entry = yield* validateRow(row, permit)
          if (
            entry.expectedLocal.tenantId !== endpoint.expectedLocal.tenantId ||
            entry.expectedLocal.subjectId !== endpoint.expectedLocal.subjectId ||
            entry.expectedLocal.peerId !== endpoint.expectedLocal.peerId
          ) {
            return yield* storageCorrupt(new Error("Relay outbox local endpoint mismatch"))
          }
          entries.push(entry)
        }
        return entries
      })))
    }))

  const maximumPendingHorizon = (input: Endpoint) =>
    Effect.scoped(Effect.gen(function*() {
      const endpoint = yield* decodeEndpoint(input)
      const permit = yield* gate.shared
      const query = SqlSchema.findAll({
        Request: Schema.Void,
        Result: HorizonRow,
        execute: () =>
          sql`SELECT MAX(
            (julianday(retry_deadline) - julianday(created_at)) * 86400000.0
          ) AS horizon_millis
          FROM effect_local_peer_relay_outbox
          WHERE replica_id = ${permit.replicaId}
            AND replica_incarnation = ${permit.incarnation}
            AND expected_local_tenant_id = ${endpoint.expectedLocal.tenantId}
            AND expected_local_subject_id = ${endpoint.expectedLocal.subjectId}
            AND expected_local_peer_id = ${endpoint.expectedLocal.peerId}
            AND relay_peer_id = ${endpoint.relayPeerId}
            AND remote_tenant_id = ${endpoint.remote.tenantId}
            AND remote_subject_id = ${endpoint.remote.subjectId}
            AND remote_peer_id = ${endpoint.remote.peerId}`
      })
      const rows = yield* mapStorageFailures(query(undefined))
      const horizon = rows[0]?.horizon_millis ?? null
      if (horizon === null) return null
      const rounded = Math.round(horizon)
      if (rounded <= 0 || rounded > limits.maxRetryHorizonMillis) {
        return yield* storageCorrupt(new Error("Relay outbox horizon mismatch"))
      }
      return rounded
    }))

  const markCustody = (input: CustodyInput) =>
    Effect.scoped(Effect.gen(function*() {
      const permit = yield* gate.shared
      yield* mapStorageFailures(sql.withTransaction(Effect.gen(function*() {
        const rows = yield* findRow({
          replicaId: permit.replicaId,
          replicaIncarnation: permit.incarnation,
          relayMessageId: input.relayMessageId
        })
        if (rows.length === 0) {
          yield* gate.validate(permit)
          return
        }
        if (rows.length !== 1) {
          return yield* storageCorrupt(new Error("Duplicate relay message identity"))
        }
        const row = rows[0]!
        yield* validateRow(row, permit)
        if (row.outer_envelope_digest !== input.outerEnvelopeDigest) {
          return yield* Effect.fail(
            protocolMismatch("matching relay outer envelope digest", "conflicting custody digest")
          )
        }
        yield* decrementUsage(row)
        yield* sql`DELETE FROM effect_local_peer_relay_outbox
          WHERE row_id = ${row.row_id}
            AND replica_id = ${permit.replicaId}
            AND replica_incarnation = ${permit.incarnation}
            AND relay_message_id = ${input.relayMessageId}
            AND outer_envelope_digest = ${input.outerEnvelopeDigest}`
        yield* gate.validate(permit)
      })))
    }))

  const pruneExpired = Effect.scoped(Effect.gen(function*() {
    const permit = yield* gate.shared
    const now = new Date(yield* Clock.currentTimeMillis).toISOString()
    return yield* mapStorageFailures(sql.withTransaction(Effect.gen(function*() {
      const query = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Row,
        execute: () =>
          sql`SELECT *
            FROM effect_local_peer_relay_outbox
            WHERE replica_id = ${permit.replicaId}
              AND replica_incarnation = ${permit.incarnation}
              AND retry_deadline <= ${now}
            ORDER BY row_id
            LIMIT ${limits.pruneBatchSize}`
      })
      const rows = yield* query(undefined)
      for (const row of rows) {
        yield* validateRow(row, permit)
        yield* decrementUsage(row)
        yield* sql`DELETE FROM effect_local_peer_relay_outbox
          WHERE row_id = ${row.row_id}
            AND replica_id = ${permit.replicaId}
            AND replica_incarnation = ${permit.incarnation}`
      }
      yield* gate.validate(permit)
      return rows.length
    })))
  }))

  const usage = (input: Endpoint) =>
    Effect.scoped(Effect.gen(function*() {
      const endpoint = yield* decodeEndpoint(input)
      const permit = yield* gate.shared
      const remoteQuery = SqlSchema.findAll({
        Request: Schema.Void,
        Result: UsageRow,
        execute: () =>
          sql`SELECT message_count, encoded_bytes
            FROM effect_local_peer_relay_outbox_remote_usage
            WHERE replica_incarnation = ${permit.incarnation}
              AND remote_tenant_id = ${endpoint.remote.tenantId}
              AND remote_subject_id = ${endpoint.remote.subjectId}
              AND remote_peer_id = ${endpoint.remote.peerId}`
      })
      const replicaQuery = SqlSchema.findAll({
        Request: Schema.Void,
        Result: UsageRow,
        execute: () =>
          sql`SELECT message_count, encoded_bytes
            FROM effect_local_peer_relay_outbox_replica_usage
            WHERE replica_incarnation = ${permit.incarnation}`
      })
      const [remoteRows, replicaRows] = yield* mapStorageFailures(
        sql.withTransaction(Effect.all([remoteQuery(undefined), replicaQuery(undefined)]))
      )
      const remote = remoteRows[0] ?? { message_count: 0, encoded_bytes: 0 }
      const replica = replicaRows[0] ?? { message_count: 0, encoded_bytes: 0 }
      return {
        remote: {
          messageCount: remote.message_count,
          encodedBytes: remote.encoded_bytes
        },
        replica: {
          messageCount: replica.message_count,
          encodedBytes: replica.encoded_bytes
        }
      }
    }))

  return {
    admit,
    dueForEndpoint,
    maximumPendingHorizon,
    markCustody,
    pruneExpired,
    usage,
    validateReplicaIncarnation
  }
})

export const layerSql: Layer.Layer<
  PeerRelayOutbox,
  never,
  Requirements
> = Layer.effect(PeerRelayOutbox, make)
