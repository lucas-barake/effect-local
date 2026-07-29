import * as CommandDelivery from "@lucas-barake/effect-local/CommandDelivery"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import type * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as WriterProvenance from "./internal/writerProvenance.js"
import type * as PeerSyncEnvelope from "./PeerSyncEnvelope.js"
import * as ReplicaGate from "./ReplicaGate.js"

const IsoDate = Schema.DateTimeUtcFromString

const ReceiptRow = Schema.Struct({
  document_id: Identity.DocumentId,
  tracked: Schema.Int
})

const DeliveryRow = Schema.Struct({
  expected_local_tenant_id: Schema.String,
  expected_local_subject_id: Schema.String,
  expected_local_peer_id: Identity.PeerId,
  remote_tenant_id: Schema.String,
  remote_subject_id: Schema.String,
  remote_peer_id: Identity.PeerId,
  relay_peer_id: Identity.PeerId,
  relay_message_id: Identity.RelayMessageId,
  change_hash: WriterProvenance.ChangeHash,
  created_at: IsoDate,
  retry_deadline: IsoDate,
  relay_custody_accepted_at: Schema.NullOr(IsoDate),
  sender_custody_unconfirmed_at: Schema.NullOr(IsoDate)
})

const EventRow = Schema.Struct({
  event_sequence: Schema.Int.check(Schema.isGreaterThan(0)),
  replica_incarnation: Identity.ReplicaIncarnation,
  command_id: Schema.NullOr(Identity.CommandId),
  document_id: Identity.DocumentId
})

const CursorRow = Schema.Struct({
  event_sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  refresh_epoch: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
})

const StoredMessageRow = Schema.Struct({
  replica_id: Identity.ReplicaId,
  expected_local_tenant_id: Schema.String,
  expected_local_subject_id: Schema.String,
  expected_local_peer_id: Identity.PeerId,
  remote_tenant_id: Schema.String,
  remote_subject_id: Schema.String,
  remote_peer_id: Identity.PeerId,
  relay_peer_id: Identity.PeerId,
  outer_envelope_digest: Schema.String,
  sender_connection_epoch: Schema.String,
  sender_sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  document_id: Identity.DocumentId,
  created_at: Schema.String,
  retry_deadline: Schema.String
})

export interface Endpoint {
  readonly expectedLocal: PeerSyncEnvelope.RelayPeerPrincipal
  readonly remote: PeerSyncEnvelope.RelayPeerPrincipal
  readonly relayPeerId: Identity.PeerId
}

export interface MessageInput extends Endpoint {
  readonly replicaId: Identity.ReplicaId
  readonly replicaIncarnation: Identity.ReplicaIncarnation
  readonly relayMessageId: Identity.RelayMessageId
  readonly outerEnvelopeDigest: string
  readonly senderConnectionEpoch: string
  readonly senderSequence: number
  readonly documentId: Identity.DocumentId
  readonly createdAt: string
  readonly retryDeadline: string
  readonly changeHashes: ReadonlyArray<string>
}

export interface Event {
  readonly sequence: number
  readonly replicaIncarnation: Identity.ReplicaIncarnation
  readonly commandId: Identity.CommandId | null
  readonly documentId: Identity.DocumentId
}

export interface Cursor {
  readonly sequence: number
  readonly refreshEpoch: number
}

const endpointKey = (endpoint: Endpoint): string =>
  JSON.stringify([
    endpoint.expectedLocal.tenantId,
    endpoint.expectedLocal.subjectId,
    endpoint.expectedLocal.peerId,
    endpoint.remote.tenantId,
    endpoint.remote.subjectId,
    endpoint.remote.peerId,
    endpoint.relayPeerId
  ])

export class CommandDeliveryStore extends Context.Service<CommandDeliveryStore, {
  readonly lookup: (
    commandId: Identity.CommandId
  ) => Effect.Effect<CommandDelivery.CommandDelivery, ReplicaError.ReplicaError>
  readonly snapshotWithCursor: (
    commandId: Identity.CommandId
  ) => Effect.Effect<
    readonly [CommandDelivery.CommandDelivery, Cursor],
    ReplicaError.ReplicaError
  >
  readonly recordMessage: (input: MessageInput) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly markAccepted: (
    replicaIncarnation: Identity.ReplicaIncarnation,
    relayMessageId: Identity.RelayMessageId,
    outerEnvelopeDigest: string,
    acceptedAt: string
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly markUnconfirmed: (
    replicaIncarnation: Identity.ReplicaIncarnation,
    relayMessageId: Identity.RelayMessageId,
    observedAt: string
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly documentConfirmed: (
    documentId: Identity.DocumentId,
    endpoint: Endpoint | undefined
  ) => Effect.Effect<boolean, ReplicaError.ReplicaError>
  readonly pendingEvents: Effect.Effect<ReadonlyArray<Event>, ReplicaError.ReplicaError>
  readonly markEventsPublished: (
    sequences: ReadonlyArray<number>
  ) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly cursor: Effect.Effect<Cursor, ReplicaError.ReplicaError>
  readonly advanceRefreshEpoch: Effect.Effect<void, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/CommandDeliveryStore") {}

export const layer: Layer.Layer<
  CommandDeliveryStore,
  never,
  SqlClient.SqlClient | ReplicaGate.ReplicaGate
> = Layer.effect(
  CommandDeliveryStore,
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const gate = yield* ReplicaGate.ReplicaGate

    const findReceipt = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        commandId: Identity.CommandId
      }),
      Result: ReceiptRow,
      execute: (request) =>
        sql`SELECT
          receipts.document_id,
          CASE WHEN sources.command_id IS NULL THEN 0 ELSE 1 END AS tracked
        FROM effect_local_command_receipts receipts
        LEFT JOIN effect_local_command_delivery_sources sources
          ON sources.replica_incarnation = receipts.replica_incarnation
          AND sources.command_id = receipts.command_id
        WHERE receipts.replica_incarnation = ${request.replicaIncarnation}
          AND receipts.command_id = ${request.commandId}`
    })

    const findSourceChanges = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        commandId: Identity.CommandId
      }),
      Result: Schema.Struct({ change_hash: WriterProvenance.ChangeHash }),
      execute: (request) =>
        sql`SELECT change_hash
        FROM effect_local_command_delivery_changes
        WHERE replica_incarnation = ${request.replicaIncarnation}
          AND command_id = ${request.commandId}
        ORDER BY change_hash`
    })

    const findDeliveryRows = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        commandId: Identity.CommandId
      }),
      Result: DeliveryRow,
      execute: (request) =>
        sql`SELECT
          messages.expected_local_tenant_id,
          messages.expected_local_subject_id,
          messages.expected_local_peer_id,
          messages.remote_tenant_id,
          messages.remote_subject_id,
          messages.remote_peer_id,
          messages.relay_peer_id,
          messages.relay_message_id,
          message_changes.change_hash,
          messages.created_at,
          messages.retry_deadline,
          messages.relay_custody_accepted_at,
          messages.sender_custody_unconfirmed_at
        FROM effect_local_command_delivery_changes command_changes
        INNER JOIN effect_local_peer_relay_delivery_changes message_changes
          ON message_changes.replica_incarnation = command_changes.replica_incarnation
          AND message_changes.change_hash = command_changes.change_hash
        INNER JOIN effect_local_peer_relay_delivery_messages messages
          ON messages.replica_incarnation = message_changes.replica_incarnation
          AND messages.relay_message_id = message_changes.relay_message_id
        WHERE command_changes.replica_incarnation = ${request.replicaIncarnation}
          AND command_changes.command_id = ${request.commandId}
        ORDER BY
          messages.relay_peer_id,
          messages.remote_peer_id,
          messages.relay_message_id,
          message_changes.change_hash`
    })

    const currentCursor = SqlSchema.findOne({
      Request: Schema.Void,
      Result: CursorRow,
      execute: () =>
        sql`SELECT
          COALESCE(MAX(events.event_sequence), 0) AS event_sequence,
          control.refresh_epoch
        FROM effect_local_command_delivery_control control
        LEFT JOIN effect_local_command_delivery_events events ON 1 = 1
        WHERE control.singleton = 1
        GROUP BY control.refresh_epoch`
    })(undefined).pipe(
      Effect.map((row) => ({
        sequence: row.event_sequence,
        refreshEpoch: row.refresh_epoch
      })),
      Effect.catchTag("NoSuchElementError", (cause) =>
        Effect.fail(
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({ cause })
          })
        ))
    )

    const lookupWithPermit = (
      commandId: Identity.CommandId,
      permit: ReplicaGate.Permit
    ): Effect.Effect<CommandDelivery.CommandDelivery, ReplicaError.ReplicaError> =>
      Effect.gen(function*() {
        const receiptRows = yield* findReceipt({
          replicaIncarnation: permit.incarnation,
          commandId
        })
        if (receiptRows.length === 0) return CommandDelivery.unknown(commandId)
        if (receiptRows.length !== 1) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: new Error("Duplicate command receipt")
            })
          })
        }
        const receipt = receiptRows[0]!
        if (receipt.tracked !== 1) return CommandDelivery.untracked(commandId, receipt.document_id)
        const sourceChanges = yield* findSourceChanges({
          replicaIncarnation: permit.incarnation,
          commandId
        })
        if (sourceChanges.length === 0) return CommandDelivery.noChanges(commandId, receipt.document_id)
        const rows = yield* findDeliveryRows({
          replicaIncarnation: permit.incarnation,
          commandId
        })
        const groups = new Map<string, {
          readonly relayPeerId: Identity.PeerId
          readonly remotePeerId: Identity.PeerId
          readonly accepted: Set<string>
          readonly pending: Set<string>
          readonly unconfirmed: Set<string>
          acceptedAt: DateTime.Utc | undefined
          pendingAt: DateTime.Utc | undefined
          retryDeadline: DateTime.Utc | undefined
          unconfirmedAt: DateTime.Utc | undefined
        }>()
        for (const row of rows) {
          const key = endpointKey({
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
            relayPeerId: row.relay_peer_id
          })
          let group = groups.get(key)
          if (group === undefined) {
            group = {
              relayPeerId: row.relay_peer_id,
              remotePeerId: row.remote_peer_id,
              accepted: new Set(),
              pending: new Set(),
              unconfirmed: new Set(),
              acceptedAt: undefined,
              pendingAt: undefined,
              retryDeadline: undefined,
              unconfirmedAt: undefined
            }
            groups.set(key, group)
          }
          if (row.relay_custody_accepted_at !== null) {
            group.accepted.add(row.change_hash)
            if (
              group.acceptedAt === undefined ||
              row.relay_custody_accepted_at.epochMilliseconds > group.acceptedAt.epochMilliseconds
            ) {
              group.acceptedAt = row.relay_custody_accepted_at
            }
          } else if (row.sender_custody_unconfirmed_at !== null) {
            group.unconfirmed.add(row.change_hash)
            if (
              group.unconfirmedAt === undefined ||
              row.sender_custody_unconfirmed_at.epochMilliseconds > group.unconfirmedAt.epochMilliseconds
            ) {
              group.unconfirmedAt = row.sender_custody_unconfirmed_at
              group.retryDeadline = row.retry_deadline
            }
          } else {
            group.pending.add(row.change_hash)
            if (
              group.pendingAt === undefined ||
              row.created_at.epochMilliseconds < group.pendingAt.epochMilliseconds
            ) {
              group.pendingAt = row.created_at
            }
            if (
              group.retryDeadline === undefined ||
              row.retry_deadline.epochMilliseconds > group.retryDeadline.epochMilliseconds
            ) {
              group.retryDeadline = row.retry_deadline
            }
          }
        }
        const localChangeCount = sourceChanges.length
        const destinations = [...groups.values()].slice(0, 256).map((group): CommandDelivery.Destination => {
          if (group.accepted.size === localChangeCount) {
            return {
              relayPeerId: group.relayPeerId,
              remotePeerId: group.remotePeerId,
              state: {
                _tag: "RelayCustodyAccepted",
                acceptedChangeCount: group.accepted.size,
                acceptedAt: group.acceptedAt!,
                ...(group.unconfirmedAt === undefined
                  ? {}
                  : { senderCustodyUnconfirmedAt: group.unconfirmedAt })
              }
            }
          }
          if (group.pending.size > 0) {
            return {
              relayPeerId: group.relayPeerId,
              remotePeerId: group.remotePeerId,
              state: {
                _tag: "PendingRelayCustody",
                acceptedChangeCount: group.accepted.size,
                pendingChangeCount: group.pending.size,
                firstPendingAt: group.pendingAt!,
                retryDeadline: group.retryDeadline!
              }
            }
          }
          return {
            relayPeerId: group.relayPeerId,
            remotePeerId: group.remotePeerId,
            state: {
              _tag: "RelayCustodyUnconfirmedAtDeadline",
              acceptedChangeCount: group.accepted.size,
              unconfirmedChangeCount: localChangeCount - group.accepted.size,
              deadline: group.retryDeadline!,
              observedAt: group.unconfirmedAt!
            }
          }
        })
        yield* gate.validate(permit)
        return {
          _tag: "TrackedCommand",
          commandId,
          documentId: receipt.document_id,
          localChangeCount,
          destinations
        } satisfies CommandDelivery.TrackedCommand
      }).pipe(
        Effect.catchTags({
          SqlError: (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({ cause })
            }),
          SchemaError: (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause })
            })
        })
      )

    const lookup = (commandId: Identity.CommandId) =>
      Effect.scoped(Effect.gen(function*() {
        const permit = yield* gate.shared
        return yield* lookupWithPermit(commandId, permit)
      }))

    const snapshotWithCursor = (commandId: Identity.CommandId) =>
      Effect.scoped(Effect.gen(function*() {
        const permit = yield* gate.shared
        return yield* sql.withTransaction(Effect.gen(function*() {
          const snapshot = yield* lookupWithPermit(commandId, permit)
          const cursor = yield* currentCursor
          yield* gate.validate(permit)
          return [snapshot, cursor] as const
        }))
      })).pipe(
        Effect.catchTags({
          SqlError: (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({ cause })
            }),
          SchemaError: (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause })
            })
        })
      )

    const insertEvents = (
      replicaIncarnation: Identity.ReplicaIncarnation,
      documentId: Identity.DocumentId,
      changeHashes: ReadonlyArray<string>
    ) =>
      changeHashes.length === 0
        ? Effect.void
        : sql`INSERT INTO effect_local_command_delivery_events (
            replica_incarnation, command_id, document_id, published
          )
          SELECT DISTINCT
            sources.replica_incarnation,
            sources.command_id,
            sources.document_id,
            0
          FROM effect_local_command_delivery_sources sources
          INNER JOIN effect_local_command_delivery_changes changes
            ON changes.replica_incarnation = sources.replica_incarnation
            AND changes.command_id = sources.command_id
          WHERE sources.replica_incarnation = ${replicaIncarnation}
            AND sources.document_id = ${documentId}
            AND ${sql.in("changes.change_hash", changeHashes)}`

    const findStoredMessage = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        relayMessageId: Identity.RelayMessageId
      }),
      Result: StoredMessageRow,
      execute: (request) =>
        sql`SELECT
          replica_id,
          expected_local_tenant_id,
          expected_local_subject_id,
          expected_local_peer_id,
          remote_tenant_id,
          remote_subject_id,
          remote_peer_id,
          relay_peer_id,
          outer_envelope_digest,
          sender_connection_epoch,
          sender_sequence,
          document_id,
          created_at,
          retry_deadline
        FROM effect_local_peer_relay_delivery_messages
        WHERE replica_incarnation = ${request.replicaIncarnation}
          AND relay_message_id = ${request.relayMessageId}`
    })

    const findStoredMessageChanges = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        relayMessageId: Identity.RelayMessageId
      }),
      Result: Schema.Struct({
        change_hash: WriterProvenance.ChangeHash
      }),
      execute: (request) =>
        sql`SELECT change_hash
        FROM effect_local_peer_relay_delivery_changes
        WHERE replica_incarnation = ${request.replicaIncarnation}
          AND relay_message_id = ${request.relayMessageId}
        ORDER BY change_hash`
    })

    const recordMessage = (input: MessageInput) =>
      Effect.gen(function*() {
        const uniqueChangeHashes = [...new Set(input.changeHashes)].toSorted()
        if (uniqueChangeHashes.length !== input.changeHashes.length) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: new Error("Relay delivery message contains duplicate change hashes")
            })
          })
        }
        const insertedMessages = yield* sql<{ readonly relay_message_id: string }>`
        INSERT INTO effect_local_peer_relay_delivery_messages (
          replica_id, replica_incarnation,
          expected_local_tenant_id, expected_local_subject_id, expected_local_peer_id,
          remote_tenant_id, remote_subject_id, remote_peer_id, relay_peer_id,
          relay_message_id, outer_envelope_digest, sender_connection_epoch, sender_sequence,
          document_id, created_at, retry_deadline,
          relay_custody_accepted_at, sender_custody_unconfirmed_at
        ) VALUES (
          ${input.replicaId}, ${input.replicaIncarnation},
          ${input.expectedLocal.tenantId}, ${input.expectedLocal.subjectId}, ${input.expectedLocal.peerId},
          ${input.remote.tenantId}, ${input.remote.subjectId}, ${input.remote.peerId}, ${input.relayPeerId},
          ${input.relayMessageId}, ${input.outerEnvelopeDigest},
          ${input.senderConnectionEpoch}, ${input.senderSequence},
          ${input.documentId}, ${input.createdAt}, ${input.retryDeadline}, NULL, NULL
        )
        ON CONFLICT(replica_incarnation, relay_message_id) DO NOTHING
        RETURNING relay_message_id`
        if (insertedMessages.length === 0) {
          const existing = yield* findStoredMessage({
            replicaIncarnation: input.replicaIncarnation,
            relayMessageId: input.relayMessageId
          })
          const existingChanges = yield* findStoredMessageChanges({
            replicaIncarnation: input.replicaIncarnation,
            relayMessageId: input.relayMessageId
          })
          const row = existing[0]
          if (
            existing.length !== 1 ||
            row === undefined ||
            row.replica_id !== input.replicaId ||
            row.expected_local_tenant_id !== input.expectedLocal.tenantId ||
            row.expected_local_subject_id !== input.expectedLocal.subjectId ||
            row.expected_local_peer_id !== input.expectedLocal.peerId ||
            row.remote_tenant_id !== input.remote.tenantId ||
            row.remote_subject_id !== input.remote.subjectId ||
            row.remote_peer_id !== input.remote.peerId ||
            row.relay_peer_id !== input.relayPeerId ||
            row.outer_envelope_digest !== input.outerEnvelopeDigest ||
            row.sender_connection_epoch !== input.senderConnectionEpoch ||
            row.sender_sequence !== input.senderSequence ||
            row.document_id !== input.documentId ||
            row.created_at !== input.createdAt ||
            row.retry_deadline !== input.retryDeadline ||
            existingChanges.length !== uniqueChangeHashes.length ||
            existingChanges.some((change, index) => change.change_hash !== uniqueChangeHashes[index])
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: new Error("Relay delivery message identity is conflicting")
              })
            })
          }
          return
        }
        if (insertedMessages.length !== 1) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: new Error("Duplicate relay delivery message identity")
            })
          })
        }
        for (const changeHash of uniqueChangeHashes) {
          const insertedChanges = yield* sql<{ readonly change_hash: string }>`
          INSERT INTO effect_local_peer_relay_delivery_changes (
            replica_incarnation, relay_message_id, change_hash
          ) VALUES (
            ${input.replicaIncarnation}, ${input.relayMessageId}, ${changeHash}
          )
          ON CONFLICT(replica_incarnation, relay_message_id, change_hash) DO NOTHING
          RETURNING change_hash`
          if (insertedChanges.length !== 1) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: new Error("Duplicate relay delivery change identity")
              })
            })
          }
        }
        yield* insertEvents(input.replicaIncarnation, input.documentId, uniqueChangeHashes)
      }).pipe(
        Effect.catchTags({
          SqlError: (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({ cause })
            }),
          SchemaError: (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({ cause })
            })
        })
      )

    const matchingCommands = (
      replicaIncarnation: Identity.ReplicaIncarnation,
      relayMessageId: Identity.RelayMessageId
    ) =>
      sql`INSERT INTO effect_local_command_delivery_events (
        replica_incarnation, command_id, document_id, published
      )
      SELECT DISTINCT
        sources.replica_incarnation,
        sources.command_id,
        sources.document_id,
        0
      FROM effect_local_peer_relay_delivery_changes message_changes
      INNER JOIN effect_local_command_delivery_changes command_changes
        ON command_changes.replica_incarnation = message_changes.replica_incarnation
        AND command_changes.change_hash = message_changes.change_hash
      INNER JOIN effect_local_command_delivery_sources sources
        ON sources.replica_incarnation = command_changes.replica_incarnation
        AND sources.command_id = command_changes.command_id
      WHERE message_changes.replica_incarnation = ${replicaIncarnation}
        AND message_changes.relay_message_id = ${relayMessageId}`

    const markAccepted = (
      replicaIncarnation: Identity.ReplicaIncarnation,
      relayMessageId: Identity.RelayMessageId,
      outerEnvelopeDigest: string,
      acceptedAt: string
    ) =>
      Effect.gen(function*() {
        const rows = yield* sql<{ readonly relay_message_id: string }>`
        UPDATE effect_local_peer_relay_delivery_messages
          SET relay_custody_accepted_at = ${acceptedAt}
          WHERE replica_incarnation = ${replicaIncarnation}
            AND relay_message_id = ${relayMessageId}
            AND outer_envelope_digest = ${outerEnvelopeDigest}
            AND relay_custody_accepted_at IS NULL
          RETURNING relay_message_id`
        if (rows.length === 0) {
          const existing = yield* sql<{ readonly outer_envelope_digest: string; readonly accepted: string | null }>`
            SELECT outer_envelope_digest, relay_custody_accepted_at AS accepted
            FROM effect_local_peer_relay_delivery_messages
            WHERE replica_incarnation = ${replicaIncarnation}
              AND relay_message_id = ${relayMessageId}`
          if (
            existing.length !== 1 ||
            existing[0]!.outer_envelope_digest !== outerEnvelopeDigest ||
            existing[0]!.accepted === null
          ) {
            return yield* new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageCorrupt({
                cause: new Error("Relay custody identity is missing or conflicting")
              })
            })
          }
          return
        }
        if (rows.length !== 1) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: new Error("Duplicate relay custody identity")
            })
          })
        }
        yield* matchingCommands(replicaIncarnation, relayMessageId)
      }).pipe(
        Effect.catchTag("SqlError", (cause) =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({ cause })
          }))
      )

    const markUnconfirmed = (
      replicaIncarnation: Identity.ReplicaIncarnation,
      relayMessageId: Identity.RelayMessageId,
      observedAt: string
    ) =>
      Effect.gen(function*() {
        const rows = yield* sql<{ readonly relay_message_id: string }>`
        UPDATE effect_local_peer_relay_delivery_messages
          SET sender_custody_unconfirmed_at = ${observedAt}
          WHERE replica_incarnation = ${replicaIncarnation}
            AND relay_message_id = ${relayMessageId}
            AND relay_custody_accepted_at IS NULL
            AND sender_custody_unconfirmed_at IS NULL
          RETURNING relay_message_id`
        if (rows.length > 1) {
          return yield* new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({
              cause: new Error("Duplicate relay deadline identity")
            })
          })
        }
        if (rows.length === 1) {
          yield* matchingCommands(replicaIncarnation, relayMessageId)
        }
      }).pipe(
        Effect.catchTag("SqlError", (cause) =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({ cause })
          }))
      )

    const documentConfirmed = (
      documentId: Identity.DocumentId,
      endpoint: Endpoint | undefined
    ) =>
      endpoint === undefined
        ? Effect.succeed(false)
        : Effect.scoped(Effect.gen(function*() {
          const permit = yield* gate.shared
          const rows = yield* SqlSchema.findOne({
            Request: Schema.Void,
            Result: Schema.Struct({
              local_count: Schema.Int,
              accepted_count: Schema.Int
            }),
            execute: () =>
              sql`SELECT
                COUNT(DISTINCT local.change_hash) AS local_count,
                COUNT(DISTINCT accepted.change_hash) AS accepted_count
              FROM effect_local_changes local
              LEFT JOIN (
                SELECT message_changes.change_hash
                FROM effect_local_peer_relay_delivery_messages messages
                INNER JOIN effect_local_peer_relay_delivery_changes message_changes
                  ON message_changes.replica_incarnation = messages.replica_incarnation
                  AND message_changes.relay_message_id = messages.relay_message_id
                WHERE messages.replica_incarnation = ${permit.incarnation}
                  AND messages.document_id = ${documentId}
                  AND messages.expected_local_tenant_id = ${endpoint.expectedLocal.tenantId}
                  AND messages.expected_local_subject_id = ${endpoint.expectedLocal.subjectId}
                  AND messages.expected_local_peer_id = ${endpoint.expectedLocal.peerId}
                  AND messages.remote_tenant_id = ${endpoint.remote.tenantId}
                  AND messages.remote_subject_id = ${endpoint.remote.subjectId}
                  AND messages.remote_peer_id = ${endpoint.remote.peerId}
                  AND messages.relay_peer_id = ${endpoint.relayPeerId}
                  AND messages.relay_custody_accepted_at IS NOT NULL
              ) accepted ON accepted.change_hash = local.change_hash
              WHERE local.document_id = ${documentId}
                AND local.peer_id IS NULL`
          })(undefined)
          yield* gate.validate(permit)
          return rows.local_count > 0 && rows.accepted_count === rows.local_count
        })).pipe(
          Effect.catchTags({
            SqlError: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageUnavailable({ cause })
              }),
            SchemaError: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              }),
            NoSuchElementError: (cause) =>
              new ReplicaError.ReplicaError({
                reason: new ReplicaError.StorageCorrupt({ cause })
              })
          })
        )

    const pendingEvents = SqlSchema.findAll({
      Request: Schema.Void,
      Result: EventRow,
      execute: () =>
        sql`SELECT event_sequence, replica_incarnation, command_id, document_id
        FROM effect_local_command_delivery_events
        WHERE published = 0
        ORDER BY event_sequence
        LIMIT 512`
    })(undefined).pipe(
      Effect.map((rows) =>
        rows.map((row) => ({
          sequence: row.event_sequence,
          replicaIncarnation: row.replica_incarnation,
          commandId: row.command_id,
          documentId: row.document_id
        }))
      ),
      Effect.catchTags({
        SqlError: (cause) =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({ cause })
          }),
        SchemaError: (cause) =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({ cause })
          })
      })
    )

    const markEventsPublished = (sequences: ReadonlyArray<number>) =>
      sequences.length === 0
        ? Effect.void
        : sql.withTransaction(Effect.gen(function*() {
          yield* sql`UPDATE effect_local_command_delivery_events
            SET published = 1
            WHERE ${sql.in("event_sequence", sequences)}`
          yield* sql`DELETE FROM effect_local_command_delivery_events
            WHERE published = 1
              AND event_sequence < (
                SELECT MAX(event_sequence)
                FROM effect_local_command_delivery_events
              )`
        })).pipe(
          Effect.catchTag("SqlError", (cause) =>
            new ReplicaError.ReplicaError({
              reason: new ReplicaError.StorageUnavailable({ cause })
            })),
          Effect.asVoid
        )

    const cursor = currentCursor.pipe(
      Effect.catchTags({
        SqlError: (cause) =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageUnavailable({ cause })
          }),
        SchemaError: (cause) =>
          new ReplicaError.ReplicaError({
            reason: new ReplicaError.StorageCorrupt({ cause })
          })
      })
    )

    const advanceRefreshEpoch = sql`UPDATE effect_local_command_delivery_control
      SET refresh_epoch = refresh_epoch + 1
      WHERE singleton = 1`.pipe(
      Effect.catchTag("SqlError", (cause) =>
        new ReplicaError.ReplicaError({
          reason: new ReplicaError.StorageUnavailable({ cause })
        })),
      Effect.asVoid
    )

    return CommandDeliveryStore.of({
      lookup,
      snapshotWithCursor,
      recordMessage,
      markAccepted,
      markUnconfirmed,
      documentConfirmed,
      pendingEvents,
      markEventsPublished,
      cursor,
      advanceRefreshEpoch
    })
  })
)
