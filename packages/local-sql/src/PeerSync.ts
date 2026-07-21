import * as Automerge from "@automerge/automerge"
import * as Canonical from "@lucas-barake/effect-local/Canonical"
import type * as Document from "@lucas-barake/effect-local/Document"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as DocumentStore from "./DocumentStore.js"
import * as InternalAutomerge from "./internal/automerge.js"
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
  readonly durableConfirmation: false
  readonly duplicate: boolean
}

const Heads = Schema.fromJsonString(Schema.Array(Schema.String))

const ReceiptRow = Schema.Struct({
  commit_sequence: Schema.Number,
  accepted_heads: Heads,
  heads: Heads,
  message_hash: Schema.String,
  reply: Schema.NullOr(Schema.Uint8Array),
  reply_hash: Schema.NullOr(Schema.String),
  document_id: Schema.String
})

const PendingRow = Schema.Struct({
  bytes: Schema.Uint8Array,
  change_hash: Schema.String,
  dependencies: Schema.String
})

const PendingReceiptRow = Schema.Struct({
  accepted_heads: Heads,
  connection_epoch: Schema.String,
  peer_id: Schema.String,
  receive_sequence: Schema.Number
})

const ExistingChangeRow = Schema.Struct({
  actor: Schema.String,
  change_hash: Schema.String,
  document_id: Schema.String,
  sequence: Schema.Number
})

const OutboxRow = Schema.Struct({
  document_id: Schema.String,
  heads: Heads,
  message: Schema.Uint8Array,
  message_hash: Schema.String,
  send_sequence: Schema.Number
})

const syncStateKey = (session: Session, documentId: Identity.DocumentId) =>
  `${session.replicaIncarnation}:${session.peerId}:${session.connectionEpoch}:${documentId}`

const sameHeads = (left: ReadonlyArray<string>, right: ReadonlyArray<string>) =>
  JSON.stringify([...left].toSorted()) === JSON.stringify([...right].toSorted())

export class PeerSync extends Context.Service<PeerSync, {
  readonly open: (peerId: Identity.PeerId) => Effect.Effect<Session, ReplicaError.ReplicaError>
  readonly reset: (session: Session) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly generate: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId,
    session: Session
  ) => Effect.Effect<Generated, ReplicaError.ReplicaError>
  readonly receive: <D extends Document.Any,>(
    document: D,
    documentId: Identity.DocumentId,
    session: Session,
    input: {
      readonly remoteConnectionEpoch: string
      readonly receiveSequence: number
      readonly message: Uint8Array
    }
  ) => Effect.Effect<Received, ReplicaError.ReplicaError>
  readonly enqueue: (session: Session, reply: Reply) => Effect.Effect<Outbound, ReplicaError.ReplicaError>
  readonly pending: (session: Session) => Effect.Effect<ReadonlyArray<Outbound>, ReplicaError.ReplicaError>
  readonly markSent: (
    session: Session,
    sendSequence: number,
    messageHash: string
  ) => Effect.Effect<boolean, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/PeerSync") {}

export const layer: Layer.Layer<
  PeerSync,
  ReplicaError.ReplicaError,
  | DocumentStore.DocumentStore
  | ReplicaBootstrap.ReplicaBootstrap
  | ReplicaGate.ReplicaGate
  | ReplicaLimits.ReplicaLimits
  | SqlClient.SqlClient
> = Layer.effect(
  PeerSync,
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const store = yield* DocumentStore.DocumentStore
    const bootstrap = yield* ReplicaBootstrap.ReplicaBootstrap
    const gate = yield* ReplicaGate.ReplicaGate
    const limits = yield* ReplicaLimits.ReplicaLimits
    const states = yield* Ref.make(new Map<string, Automerge.SyncState>())
    const lock = yield* Semaphore.make(1)
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
        sql`SELECT accepted_heads, commit_sequence, document_id, heads, message_hash, reply, reply_hash
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND receive_sequence = ${request.receiveSequence}`
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
        sql`SELECT actor, change_hash, document_id, sequence FROM effect_local_changes
          WHERE ${sql.in("change_hash", request.changes.map((change) => change.changeHash))}
            OR (document_id = ${request.documentId} AND ${
          sql.or(request.changes.map((change) => sql`(actor = ${change.actor} AND sequence = ${change.sequence})`))
        })`
    })
    const findPendingChanges = SqlSchema.findAll({
      Request: Identity.DocumentId,
      Result: PendingRow,
      execute: (documentId) =>
        sql`SELECT bytes, change_hash, dependencies FROM effect_local_changes
          WHERE document_id = ${documentId} AND applied = 0 ORDER BY accepted_at, change_hash`
    })
    const findPendingReceipts = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        documentId: Identity.DocumentId
      }),
      Result: PendingReceiptRow,
      execute: (request) =>
        sql`SELECT accepted_heads, connection_epoch, peer_id, receive_sequence
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND document_id = ${request.documentId}
            AND pending_message IS NOT NULL`
    })
    const findPendingOutbox = SqlSchema.findAll({
      Request: Schema.Struct({
        replicaIncarnation: Identity.ReplicaIncarnation,
        peerId: Identity.PeerId,
        connectionEpoch: Schema.String
      }),
      Result: OutboxRow,
      execute: (request) =>
        sql`SELECT document_id, heads, message, message_hash, send_sequence
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
        sql`SELECT document_id, heads, message, message_hash, send_sequence
          FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${request.replicaIncarnation}
            AND peer_id = ${request.peerId}
            AND connection_epoch = ${request.connectionEpoch}
            AND document_id = ${request.documentId}
            AND message_hash = ${request.messageHash}
          ORDER BY send_sequence
          LIMIT 1`
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
          AND pending_message IS NOT NULL
          AND accepted_at < ${startupCutoff}`
      yield* sql`INSERT INTO effect_local_quarantine (document_id, peer_id, reason, bytes, created_at)
        SELECT document_id, peer_id, 'Expired pending sync outbox', message, ${startupAt}
        FROM effect_local_peer_outbox
        WHERE replica_incarnation = ${bootstrap.incarnation}
          AND status = 'Pending'
          AND created_at < ${startupCutoff}`
      yield* sql`DELETE FROM effect_local_peer_receipts
        WHERE replica_incarnation != ${bootstrap.incarnation} OR accepted_at < ${startupCutoff}`
      yield* sql`DELETE FROM effect_local_peer_outbox
        WHERE replica_incarnation != ${bootstrap.incarnation} OR created_at < ${startupCutoff}`
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
        const prefix = `${session.replicaIncarnation}:${session.peerId}:${session.connectionEpoch}:`
        return new Map([...current].filter(([key]) => !key.startsWith(prefix)))
      })

    const validateSession = (permit: ReplicaGate.Permit, session: Session) =>
      permit.incarnation === session.replicaIncarnation
        ? Effect.void
        : removeState(session).pipe(
          Effect.andThen(Effect.fail(
            new ReplicaError.ReplicaError({
              reason: {
                _tag: "ProtocolMismatch",
                expected: String(permit.incarnation),
                observed: String(session.replicaIncarnation)
              }
            })
          ))
        )

    const expirePending = (session: Session, now: string, cutoff: string) =>
      sql.withTransaction(Effect.gen(function*() {
        yield* sql`INSERT INTO effect_local_quarantine (document_id, peer_id, reason, bytes, created_at)
          SELECT document_id, peer_id, 'Expired pending sync change', bytes, ${now}
          FROM effect_local_changes
          WHERE applied = 0 AND accepted_at < ${cutoff}`
        yield* sql`DELETE FROM effect_local_changes
          WHERE applied = 0 AND accepted_at < ${cutoff}`
        yield* sql`INSERT INTO effect_local_quarantine (document_id, peer_id, reason, bytes, created_at)
          SELECT document_id, peer_id, 'Expired pending sync message', pending_message, ${now}
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${session.replicaIncarnation}
            AND pending_message IS NOT NULL
            AND accepted_at < ${cutoff}`
        yield* sql`DELETE FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${session.replicaIncarnation}
            AND pending_message IS NOT NULL
            AND accepted_at < ${cutoff}`
      }))

    const nextSequence = sql<{ readonly commit_sequence: number }>`
      UPDATE effect_local_metadata SET commit_sequence = commit_sequence + 1
      WHERE singleton = 1 RETURNING commit_sequence
    `.pipe(Effect.flatMap((rows) =>
      rows[0] === undefined
        ? Effect.die(new Error("Replica metadata was not initialized"))
        : Effect.succeed(Identity.CommitSequence.make(rows[0].commit_sequence))
    ))

    const currentSequence = sql<{ readonly commit_sequence: number }>`
      SELECT commit_sequence FROM effect_local_metadata WHERE singleton = 1
    `.pipe(Effect.flatMap((rows) =>
      rows[0] === undefined
        ? Effect.die(new Error("Replica metadata was not initialized"))
        : Effect.succeed(Identity.CommitSequence.make(rows[0].commit_sequence))
    ))

    const persistOutbound = (
      session: Session,
      documentId: Identity.DocumentId,
      message: Uint8Array,
      heads: ReadonlyArray<string>
    ) =>
      Effect.gen(function*() {
        const totals = yield* sql<{ readonly bytes: number; readonly count: number }>`
          SELECT COALESCE(SUM(LENGTH(message)), 0) AS bytes, COUNT(*) AS count
          FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${session.replicaIncarnation}
            AND peer_id = ${session.peerId}
            AND connection_epoch = ${session.connectionEpoch}
            AND status = 'Pending'
        `
        if ((totals[0]?.bytes ?? 0) + message.byteLength > limits.maxPendingBytesPerPeer) {
          return yield* new ReplicaError.ReplicaError({
            reason: { _tag: "QuotaExceeded", resource: "peer sync outbox bytes", limit: limits.maxPendingBytesPerPeer }
          })
        }
        if ((totals[0]?.count ?? 0) >= limits.maxPendingChangesPerPeer) {
          return yield* new ReplicaError.ReplicaError({
            reason: {
              _tag: "QuotaExceeded",
              resource: "peer sync outbox messages",
              limit: limits.maxPendingChangesPerPeer
            }
          })
        }
        const rows = yield* sql<{ readonly sequence: number }>`
          SELECT COALESCE(MAX(send_sequence), -1) + 1 AS sequence
          FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${session.replicaIncarnation}
            AND peer_id = ${session.peerId}
            AND connection_epoch = ${session.connectionEpoch}
        `
        const sendSequence = rows[0]?.sequence ?? 0
        const messageHash = yield* Canonical.digest(message)
        const createdAt = new Date(yield* Clock.currentTimeMillis).toISOString()
        yield* sql`INSERT INTO effect_local_peer_outbox (
          replica_incarnation, peer_id, connection_epoch, document_id, send_sequence,
          message, message_hash, heads, status, created_at
        ) VALUES (
          ${session.replicaIncarnation}, ${session.peerId}, ${session.connectionEpoch}, ${documentId}, ${sendSequence},
          ${message}, ${messageHash}, ${JSON.stringify(heads)}, 'Pending', ${createdAt}
        )`
        return { sendSequence, documentId, message, messageHash, heads } satisfies Outbound
      })

    const enqueue = (session: Session, reply: Reply) =>
      lock.withPermit(Effect.scoped(Effect.gen(function*() {
        const permit = yield* gate.shared
        yield* validateSession(permit, session)
        const messageHash = yield* Canonical.digest(reply.message)
        if (messageHash !== reply.messageHash) {
          return yield* new ReplicaError.ReplicaError({
            reason: { _tag: "ProtocolMismatch", expected: messageHash, observed: reply.messageHash }
          })
        }
        const rows = yield* findOutboxReply({
          replicaIncarnation: session.replicaIncarnation,
          peerId: session.peerId,
          connectionEpoch: session.connectionEpoch,
          documentId: reply.documentId,
          messageHash: reply.messageHash
        }).pipe(
          Effect.catchTag(["SqlError", "SchemaError"], (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageUnavailable",
                  cause: { _tag: "SqlCause", message: String(cause), code: null }
                }
              })
            ))
        )
        const existing = rows[0]
        if (existing !== undefined) {
          return {
            sendSequence: existing.send_sequence,
            documentId: reply.documentId,
            message: existing.message,
            messageHash: existing.message_hash,
            heads: existing.heads
          }
        }
        return yield* sql.withTransaction(
          persistOutbound(session, reply.documentId, reply.message, reply.heads)
        ).pipe(
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
      })))

    const generate = <D extends Document.Any,>(
      document: D,
      documentId: Identity.DocumentId,
      session: Session
    ) =>
      lock.withPermit(Effect.scoped(Effect.gen(function*() {
        const permit = yield* gate.shared
        yield* validateSession(permit, session)
        const existing = yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_local_peer_outbox
          WHERE replica_incarnation = ${session.replicaIncarnation}
            AND peer_id = ${session.peerId}
            AND connection_epoch = ${session.connectionEpoch}
            AND document_id = ${documentId}
            AND status = 'Pending'
        `.pipe(
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
                    reason: {
                      _tag: "ProtocolMismatch",
                      expected: "valid local Automerge sync state",
                      observed: String(cause)
                    }
                  })
              })
              const observedByPeer = Automerge.hasOurChanges(durable.automerge, generated[0])
              if (generated[1] === null) {
                yield* writeState(session, documentId, generated[0])
                return { outbound: null, observedByPeer, dirty: false }
              }
              if (generated[1].byteLength > limits.maxSyncMessageBytes) {
                return yield* new ReplicaError.ReplicaError({
                  reason: { _tag: "QuotaExceeded", resource: "sync message bytes", limit: limits.maxSyncMessageBytes }
                })
              }
              const outbound = yield* sql.withTransaction(
                persistOutbound(session, documentId, generated[1], durable.materializedHeads)
              ).pipe(
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
              yield* writeState(session, documentId, generated[0])
              return { outbound, observedByPeer, dirty: false }
            }),
          (durable) => Effect.sync(() => InternalAutomerge.free(durable.automerge))
        )
      })))

    const receive = <D extends Document.Any,>(
      document: D,
      documentId: Identity.DocumentId,
      session: Session,
      input: {
        readonly remoteConnectionEpoch: string
        readonly receiveSequence: number
        readonly message: Uint8Array
      }
    ) =>
      lock.withPermit(Effect.scoped(Effect.gen(function*() {
        const receiptSession = { ...session, connectionEpoch: input.remoteConnectionEpoch }
        const { message, receiveSequence } = input
        if (!Number.isSafeInteger(receiveSequence) || receiveSequence < 0) {
          return yield* new ReplicaError.ReplicaError({
            reason: {
              _tag: "ProtocolMismatch",
              expected: "nonnegative safe receive sequence",
              observed: String(receiveSequence)
            }
          })
        }
        if (message.byteLength > limits.maxSyncMessageBytes) {
          return yield* new ReplicaError.ReplicaError({
            reason: {
              _tag: "ProtocolMismatch",
              expected: `sync message at most ${limits.maxSyncMessageBytes} bytes`,
              observed: String(message.byteLength)
            }
          })
        }
        const permit = yield* gate.shared
        yield* validateSession(permit, receiptSession)
        const nowMillis = yield* Clock.currentTimeMillis
        const acceptedAt = new Date(nowMillis).toISOString()
        yield* expirePending(
          receiptSession,
          acceptedAt,
          new Date(nowMillis - limits.maxPendingAgeMillis).toISOString()
        ).pipe(
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
        const messageHash = yield* Canonical.digest(message)
        const receiptRows = yield* findReceipts({
          replicaIncarnation: receiptSession.replicaIncarnation,
          peerId: receiptSession.peerId,
          connectionEpoch: receiptSession.connectionEpoch,
          receiveSequence
        }).pipe(
          Effect.catchTag(["SqlError", "SchemaError"], (cause) =>
            Effect.fail(
              new ReplicaError.ReplicaError({
                reason: {
                  _tag: "StorageUnavailable",
                  cause: { _tag: "SqlCause", message: String(cause), code: null }
                }
              })
            ))
        )
        const receipt = receiptRows[0]
        if (receipt !== undefined) {
          if (receipt.document_id !== documentId) {
            return yield* new ReplicaError.ReplicaError({
              reason: { _tag: "ProtocolMismatch", expected: receipt.document_id, observed: documentId }
            })
          }
          if (receipt.message_hash !== messageHash) {
            return yield* new ReplicaError.ReplicaError({
              reason: { _tag: "ProtocolMismatch", expected: receipt.message_hash, observed: messageHash }
            })
          }
          return {
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
            durableConfirmation: false as const,
            duplicate: true
          }
        }
        const receiptTotals = yield* sql<{
          readonly document_count: number
          readonly peer_count: number
          readonly replica_count: number
        }>`SELECT
            (SELECT COUNT(*) FROM effect_local_peer_receipts
              WHERE replica_incarnation = ${receiptSession.replicaIncarnation}
                AND document_id = ${documentId}) AS document_count,
            (SELECT COUNT(*) FROM effect_local_peer_receipts
              WHERE replica_incarnation = ${receiptSession.replicaIncarnation}
                AND peer_id = ${receiptSession.peerId}) AS peer_count,
            (SELECT COUNT(*) FROM effect_local_peer_receipts
              WHERE replica_incarnation = ${receiptSession.replicaIncarnation}) AS replica_count`
          .pipe(
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
        const receiptTotal = receiptTotals[0]
        if ((receiptTotal?.document_count ?? 0) >= limits.maxPendingChangesPerDocument) {
          return yield* new ReplicaError.ReplicaError({
            reason: {
              _tag: "QuotaExceeded",
              resource: "document sync receipts",
              limit: limits.maxPendingChangesPerDocument
            }
          })
        }
        if ((receiptTotal?.peer_count ?? 0) >= limits.maxPendingChangesPerPeer) {
          return yield* new ReplicaError.ReplicaError({
            reason: { _tag: "QuotaExceeded", resource: "peer sync receipts", limit: limits.maxPendingChangesPerPeer }
          })
        }
        if ((receiptTotal?.replica_count ?? 0) >= limits.maxPendingChangesPerReplica) {
          return yield* new ReplicaError.ReplicaError({
            reason: {
              _tag: "QuotaExceeded",
              resource: "replica sync receipts",
              limit: limits.maxPendingChangesPerReplica
            }
          })
        }
        const decoded = yield* Effect.try({
          try: () => Automerge.decodeSyncMessage(message),
          catch: (cause) =>
            new ReplicaError.ReplicaError({
              reason: {
                _tag: "ProtocolMismatch",
                expected: "valid Automerge sync message",
                observed: String(cause)
              }
            })
        })
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
                    reason: {
                      _tag: "ProtocolMismatch",
                      expected: "valid Automerge change chunks",
                      observed: String(cause)
                    }
                  })
              })
              if (changes.length > limits.maxSyncChangesPerMessage) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "ProtocolMismatch",
                    expected: `at most ${limits.maxSyncChangesPerMessage} sync changes`,
                    observed: String(changes.length)
                  }
                })
              }
              const dependencyEdges = changes.reduce((total, change) => total + change.deps.length, 0)
              const operations = changes.reduce((total, change) => total + change.ops.length, 0)
              if (dependencyEdges > limits.maxSyncDependencyEdgesPerMessage) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "ProtocolMismatch",
                    expected: `at most ${limits.maxSyncDependencyEdgesPerMessage} dependency edges`,
                    observed: String(dependencyEdges)
                  }
                })
              }
              if (operations > limits.maxSyncOperationsPerMessage) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "ProtocolMismatch",
                    expected: `at most ${limits.maxSyncOperationsPerMessage} operations`,
                    observed: String(operations)
                  }
                })
              }
              const identities = new Map<string, string>()
              for (const change of changes) {
                const key = `${change.actor}:${change.seq}`
                const existing = identities.get(key)
                if (existing !== undefined && existing !== change.hash) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: { _tag: "ProtocolMismatch", expected: existing, observed: change.hash }
                  })
                }
                identities.set(key, change.hash)
              }
              const existingChanges = changes.length === 0 ? [] : yield* findExistingChanges({
                documentId,
                changes: changes.map((change) => ({
                  actor: change.actor,
                  changeHash: change.hash,
                  sequence: change.seq
                }))
              }).pipe(
                Effect.catchTag(["SqlError", "SchemaError"], (cause) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "StorageUnavailable",
                        cause: { _tag: "SqlCause", message: String(cause), code: null }
                      }
                    })
                  ))
              )
              const hashes = new Map(existingChanges.map((row) => [row.change_hash, row]))
              const storedIdentities = new Map(existingChanges.map((row) => [`${row.actor}:${row.sequence}`, row]))
              for (const change of changes) {
                const hash = hashes.get(change.hash)
                if (
                  hash !== undefined &&
                  (hash.document_id !== documentId || hash.actor !== change.actor || hash.sequence !== change.seq)
                ) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "ProtocolMismatch",
                      expected: `${hash.document_id}:${hash.actor}:${hash.sequence}`,
                      observed: `${documentId}:${change.actor}:${change.seq}`
                    }
                  })
                }
                const identity = storedIdentities.get(`${change.actor}:${change.seq}`)
                if (identity !== undefined && identity.change_hash !== change.hash) {
                  return yield* new ReplicaError.ReplicaError({
                    reason: { _tag: "ProtocolMismatch", expected: identity.change_hash, observed: change.hash }
                  })
                }
              }
              const incoming = changeBytes.filter((_, index) => !hashes.has(changes[index]!.hash))
              const incomingBytes = incoming.reduce((total, bytes) => total + bytes.byteLength, 0)
              const incomingDependencies = changes.filter((change) => !hashes.has(change.hash)).reduce(
                (total, change) => total + change.deps.length,
                0
              )
              const pendingTotals = yield* sql<{
                readonly bytes: number
                readonly count: number
                readonly dependencies: number
              }>`SELECT
            COALESCE(SUM(LENGTH(bytes)), 0) AS bytes,
            COUNT(*) AS count,
            COALESCE(SUM(json_array_length(dependencies)), 0) AS dependencies
          FROM effect_local_changes WHERE document_id = ${documentId} AND applied = 0
        `.pipe(
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
              const receiptPending = yield* sql<{ readonly bytes: number; readonly count: number }>`
          SELECT COALESCE(SUM(LENGTH(pending_message)), 0) AS bytes, COUNT(pending_message) AS count
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${receiptSession.replicaIncarnation}
            AND document_id = ${documentId}
            AND pending_message IS NOT NULL
        `.pipe(
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
              if (
                (pendingTotals[0]?.bytes ?? 0) + (receiptPending[0]?.bytes ?? 0) +
                    incomingBytes + unresolvedBytes >
                  limits.maxPendingBytesPerDocument
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "QuotaExceeded",
                    resource: "pending document bytes",
                    limit: limits.maxPendingBytesPerDocument
                  }
                })
              }
              if (
                (pendingTotals[0]?.count ?? 0) + (receiptPending[0]?.count ?? 0) + incoming.length +
                    (unresolvedBytes === 0 ? 0 : 1) > limits.maxPendingChangesPerDocument
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "QuotaExceeded",
                    resource: "pending document changes",
                    limit: limits.maxPendingChangesPerDocument
                  }
                })
              }
              if (
                (pendingTotals[0]?.dependencies ?? 0) + incomingDependencies >
                  limits.maxPendingDependencyEdgesPerDocument
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "QuotaExceeded",
                    resource: "pending document dependency edges",
                    limit: limits.maxPendingDependencyEdgesPerDocument
                  }
                })
              }
              const peerTotals = yield* sql<
                { readonly bytes: number; readonly count: number; readonly dependencies: number }
              >`
          SELECT COALESCE(SUM(LENGTH(bytes)), 0) AS bytes, COUNT(*) AS count,
            COALESCE(SUM(json_array_length(dependencies)), 0) AS dependencies
          FROM effect_local_changes WHERE peer_id = ${receiptSession.peerId} AND applied = 0
        `.pipe(
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
              const peerReceiptPending = yield* sql<{ readonly bytes: number; readonly count: number }>`
          SELECT COALESCE(SUM(LENGTH(pending_message)), 0) AS bytes, COUNT(pending_message) AS count
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${receiptSession.replicaIncarnation}
            AND peer_id = ${receiptSession.peerId}
            AND pending_message IS NOT NULL
        `.pipe(
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
              if (
                (peerTotals[0]?.bytes ?? 0) + (peerReceiptPending[0]?.bytes ?? 0) +
                    incomingBytes + unresolvedBytes >
                  limits.maxPendingBytesPerPeer
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "QuotaExceeded",
                    resource: "pending peer bytes",
                    limit: limits.maxPendingBytesPerPeer
                  }
                })
              }
              if (
                (peerTotals[0]?.count ?? 0) + (peerReceiptPending[0]?.count ?? 0) + incoming.length +
                    (unresolvedBytes === 0 ? 0 : 1) > limits.maxPendingChangesPerPeer
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "QuotaExceeded",
                    resource: "pending peer changes",
                    limit: limits.maxPendingChangesPerPeer
                  }
                })
              }
              if (
                (peerTotals[0]?.dependencies ?? 0) + incomingDependencies > limits.maxPendingDependencyEdgesPerPeer
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "QuotaExceeded",
                    resource: "pending peer dependency edges",
                    limit: limits.maxPendingDependencyEdgesPerPeer
                  }
                })
              }
              const replicaTotals = yield* sql<{
                readonly bytes: number
                readonly count: number
                readonly dependencies: number
              }>`SELECT COALESCE(SUM(LENGTH(bytes)), 0) AS bytes, COUNT(*) AS count,
            COALESCE(SUM(json_array_length(dependencies)), 0) AS dependencies
          FROM effect_local_changes WHERE applied = 0
        `.pipe(
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
              const replicaReceiptPending = yield* sql<{ readonly bytes: number; readonly count: number }>`
          SELECT COALESCE(SUM(LENGTH(pending_message)), 0) AS bytes, COUNT(pending_message) AS count
          FROM effect_local_peer_receipts
          WHERE replica_incarnation = ${receiptSession.replicaIncarnation}
            AND pending_message IS NOT NULL
        `.pipe(
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
              if (
                (replicaTotals[0]?.bytes ?? 0) + (replicaReceiptPending[0]?.bytes ?? 0) +
                    incomingBytes + unresolvedBytes >
                  limits.maxPendingBytesPerReplica
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "QuotaExceeded",
                    resource: "pending replica bytes",
                    limit: limits.maxPendingBytesPerReplica
                  }
                })
              }
              if (
                (replicaTotals[0]?.count ?? 0) + (replicaReceiptPending[0]?.count ?? 0) + incoming.length +
                    (unresolvedBytes === 0 ? 0 : 1) > limits.maxPendingChangesPerReplica
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "QuotaExceeded",
                    resource: "pending replica changes",
                    limit: limits.maxPendingChangesPerReplica
                  }
                })
              }
              if (
                (replicaTotals[0]?.dependencies ?? 0) + incomingDependencies >
                  limits.maxPendingDependencyEdgesPerReplica
              ) {
                return yield* new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "QuotaExceeded",
                    resource: "pending replica dependency edges",
                    limit: limits.maxPendingDependencyEdgesPerReplica
                  }
                })
              }
              const state = yield* readState(session, documentId)
              const received = yield* Effect.try({
                try: () => Automerge.receiveSyncMessage(durable.automerge, state, message),
                catch: (cause) =>
                  new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "ProtocolMismatch",
                      expected: "applicable Automerge sync message",
                      observed: String(cause)
                    }
                  })
              })
              const pendingRows = yield* findPendingChanges(documentId).pipe(
                Effect.catchTag(["SqlError", "SchemaError"], (cause) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "StorageUnavailable",
                        cause: { _tag: "SqlCause", message: String(cause), code: null }
                      }
                    })
                  ))
              )
              let staged = received[0]
              if (pendingRows.length > 0) {
                staged = yield* Effect.try({
                  try: () => InternalAutomerge.replay(staged, pendingRows.map((row) => row.bytes)),
                  catch: (cause) =>
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "ProtocolMismatch",
                        expected: "applicable pending Automerge changes",
                        observed: String(cause)
                      }
                    })
                })
              }
              const generated = yield* Effect.try({
                try: () => Automerge.generateSyncMessage(staged, received[1]),
                catch: (cause) =>
                  new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "ProtocolMismatch",
                      expected: "valid Automerge sync response",
                      observed: String(cause)
                    }
                  })
              })
              if (generated[1] !== null && generated[1].byteLength > limits.maxSyncMessageBytes) {
                return yield* new ReplicaError.ReplicaError({
                  reason: { _tag: "QuotaExceeded", resource: "sync response bytes", limit: limits.maxSyncMessageBytes }
                })
              }
              const materializedHeads = InternalAutomerge.heads(staged)
              const acceptedHeads = Automerge.hasHeads(staged, decoded.heads)
                ? materializedHeads
                : [...new Set([...durable.acceptedHeads, ...materializedHeads, ...decoded.heads])].toSorted()
              const transition = !sameHeads(materializedHeads, durable.materializedHeads)
              const checkpoint = decoded.changes.length === 0 ?
                null :
                yield* Effect.sync(() => InternalAutomerge.save(staged)).pipe(
                  Effect.flatMap((bytes) =>
                    Effect.all({
                      bytes: Effect.succeed(bytes),
                      checksum: Canonical.digest(bytes),
                      checkpointHash: Canonical.digest({ documentId, bytes })
                    })
                  )
                )
              const result = yield* sql.withTransaction(Effect.gen(function*() {
                yield* gate.validate(permit)
                const commitSequence = transition ? yield* nextSequence : yield* currentSequence
                for (let index = 0; index < changes.length; index++) {
                  const change = changes[index]!
                  const bytes = changeBytes[index]!
                  const applied = Automerge.hasHeads(staged, [change.hash]) ? 1 : 0
                  yield* sql`INSERT INTO effect_local_changes (
              change_hash, document_id, document_type, writer_schema_version, writer_definition_hash,
              actor, sequence, dependencies, bytes, applied, peer_id, accepted_at, commit_sequence
            ) VALUES (
              ${change.hash}, ${documentId}, ${document.name}, ${document.version}, ${bootstrap.definitionHash},
              ${change.actor}, ${change.seq}, ${JSON.stringify(change.deps)}, ${bytes}, ${applied},
              ${receiptSession.peerId}, ${acceptedAt}, ${commitSequence}
            ) ON CONFLICT(change_hash) DO NOTHING`
                }
                for (const row of pendingRows) {
                  if (Automerge.hasHeads(staged, [row.change_hash])) {
                    yield* sql`UPDATE effect_local_changes SET applied = 1, commit_sequence = ${commitSequence}
                WHERE change_hash = ${row.change_hash}`
                  }
                }
                const pendingReceipts = yield* findPendingReceipts({
                  replicaIncarnation: receiptSession.replicaIncarnation,
                  documentId
                })
                for (const row of pendingReceipts) {
                  if (Automerge.hasHeads(staged, [...row.accepted_heads])) {
                    yield* sql`UPDATE effect_local_peer_receipts SET pending_message = NULL
                WHERE replica_incarnation = ${receiptSession.replicaIncarnation}
                  AND peer_id = ${row.peer_id}
                  AND connection_epoch = ${row.connection_epoch}
                  AND receive_sequence = ${row.receive_sequence}`
                  }
                }
                if (checkpoint !== null) {
                  yield* sql`INSERT INTO effect_local_checkpoints (
              checkpoint_hash, document_id, heads, bytes, checksum, commit_sequence, verified
            ) VALUES (
              ${checkpoint.checkpointHash}, ${documentId}, ${JSON.stringify(materializedHeads)},
              ${checkpoint.bytes}, ${checkpoint.checksum}, ${commitSequence}, 1
            ) ON CONFLICT(checkpoint_hash) DO NOTHING`
                  yield* sql`DELETE FROM effect_local_checkpoints
                WHERE document_id = ${documentId}
                  AND checkpoint_hash NOT IN (
                    SELECT checkpoint_hash FROM effect_local_checkpoints
                    WHERE document_id = ${documentId}
                    ORDER BY verified DESC, commit_sequence DESC, checkpoint_hash DESC
                    LIMIT 2
                  )`
                }
                yield* sql`UPDATE effect_local_documents SET
            materialized_heads = ${JSON.stringify(materializedHeads)},
            accepted_heads = ${JSON.stringify(acceptedHeads)},
            tombstone = ${InternalAutomerge.tombstone(staged) ? 1 : 0},
            projection_status = ${transition ? "Blocked" : durable.snapshot.projection},
            checkpoint_hash = COALESCE(${checkpoint?.checkpointHash ?? null}, checkpoint_hash)
            WHERE document_id = ${documentId}`
                if (transition) {
                  yield* sql`INSERT INTO effect_local_commit_outbox (
              commit_sequence, document_id, invalidation_keys, published
            ) VALUES (${commitSequence}, ${documentId}, ${JSON.stringify([document.name])}, 0)`
                }
                const reply = generated[1] === null
                  ? null
                  : {
                    documentId,
                    message: generated[1],
                    messageHash: yield* Canonical.digest(generated[1]),
                    heads: materializedHeads
                  }
                yield* sql`INSERT INTO effect_local_peer_receipts (
            replica_incarnation, peer_id, connection_epoch, receive_sequence,
            document_id, message_hash, reply, reply_hash, pending_message,
            heads, accepted_heads, commit_sequence, accepted_at
          ) VALUES (
            ${receiptSession.replicaIncarnation}, ${receiptSession.peerId}, ${receiptSession.connectionEpoch},
            ${receiveSequence},
            ${documentId}, ${messageHash}, ${reply?.message ?? null}, ${reply?.messageHash ?? null},
            ${unresolvedBytes === 0 ? null : message}, ${JSON.stringify(materializedHeads)},
            ${JSON.stringify(acceptedHeads)}, ${commitSequence}, ${acceptedAt}
          )`
                return { commitSequence, reply }
              })).pipe(
                Effect.catchTag(["SqlError", "SchemaError"], (cause) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "StorageUnavailable",
                        cause: { _tag: "SqlCause", message: String(cause), code: null }
                      }
                    })
                  ))
              )
              yield* writeState(session, documentId, generated[0])
              return {
                reply: result.reply,
                heads: materializedHeads,
                acceptedHeads,
                commitSequence: result.commitSequence,
                observedByPeer: Automerge.hasOurChanges(staged, generated[0]),
                durableConfirmation: false as const,
                duplicate: false
              }
            }),
          (durable) => Effect.sync(() => InternalAutomerge.free(durable.automerge))
        )
      })))

    return PeerSync.of({
      open: (peerId) =>
        gate.current.pipe(Effect.map((current) => ({
          peerId,
          connectionEpoch: globalThis.crypto.randomUUID(),
          replicaIncarnation: current.incarnation
        }))),
      reset: (session) =>
        lock.withPermit(Effect.scoped(Effect.gen(function*() {
          yield* gate.shared
          yield* sql.withTransaction(Effect.gen(function*() {
            yield* sql`DELETE FROM effect_local_peer_outbox
              WHERE replica_incarnation = ${session.replicaIncarnation}
                AND peer_id = ${session.peerId}
                AND connection_epoch = ${session.connectionEpoch}`
            yield* sql`DELETE FROM effect_local_peer_receipts
              WHERE replica_incarnation = ${session.replicaIncarnation}
                AND peer_id = ${session.peerId}
                AND connection_epoch = ${session.connectionEpoch}`
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
          yield* removeState(session)
        }))),
      generate,
      receive,
      enqueue,
      pending: (session) =>
        lock.withPermit(Effect.scoped(Effect.gen(function*() {
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
                heads: row.heads
              }))
            ),
            Effect.catchTag(["SqlError", "SchemaError"], (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "StorageUnavailable",
                    cause: { _tag: "SqlCause", message: String(cause), code: null }
                  }
                })
              ))
          )
        }))),
      markSent: (session, sendSequence, messageHash) =>
        lock.withPermit(Effect.scoped(Effect.gen(function*() {
          const permit = yield* gate.shared
          yield* validateSession(permit, session)
          return yield* sql.withTransaction(Effect.gen(function*() {
            const rows = yield* sql<{ readonly send_sequence: number }>`UPDATE effect_local_peer_outbox
              SET status = 'Sent'
              WHERE replica_incarnation = ${session.replicaIncarnation}
                AND peer_id = ${session.peerId}
                AND connection_epoch = ${session.connectionEpoch}
                AND send_sequence = ${sendSequence}
                AND message_hash = ${messageHash}
                AND status = 'Pending'
              RETURNING send_sequence`
            if (rows.length === 0) return false
            yield* sql`DELETE FROM effect_local_peer_outbox
              WHERE replica_incarnation = ${session.replicaIncarnation}
                AND peer_id = ${session.peerId}
                AND connection_epoch = ${session.connectionEpoch}
                AND status = 'Sent'
                AND send_sequence < ${sendSequence}`
            return true
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
        })))
    })
  })
)
