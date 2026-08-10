import * as SqlProjection from "@lucas-barake/effect-local-sql/SqlProjection"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Projection from "@lucas-barake/effect-local/Projection"
import * as Query from "@lucas-barake/effect-local/Query"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import type * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Transient from "@lucas-barake/effect-local/Transient"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"

/**
 * A conversation is one flat record keyed by message id, so every key has exactly one creator: the
 * sender creates the entry, and the recipient only assigns scalar fields (`deliveredAtMillis`,
 * `readAtMillis`) inside an entry the sender created. That keeps every concurrent edit
 * register-level and single-writer, so no merge can orphan a container.
 *
 * Only state that must survive a disconnect lives here. Presence and typing are transient (see
 * `Activity` below) and never touch the document, its history, or the relay's durable inboxes.
 */

const MessageBody = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4000))

const MessageEntry = Schema.Struct({
  author: Schema.String,
  body: MessageBody,
  sentAtMillis: Schema.Number,
  deliveredAtMillis: Schema.NullOr(Schema.Number),
  readAtMillis: Schema.NullOr(Schema.Number)
})

export const Conversation = Document.make("Conversation", {
  schema: Schema.Record(Schema.String, MessageEntry),
  version: 1
})

/**
 * Awareness the counterpart should stop believing the moment the sender goes away. Both variants
 * are fieldless on purpose: a transient message carries the authenticated sender peer id from the
 * live relay session, so the payload never has to claim who sent it and could not be trusted if it
 * did. Expiry is the receiver's job.
 */
export class Present extends Schema.TaggedClass<Present>()("Present", {}) {}
export class Typing extends Schema.TaggedClass<Typing>()("Typing", {}) {}

export const Activity = Transient.make("Activity", {
  document: Conversation,
  payload: Schema.Union([Present, Typing])
})

export const SendMessage = Mutation.make("SendMessage", {
  document: Conversation,
  payload: {
    messageId: Schema.String,
    author: Schema.String,
    body: MessageBody
  }
})

export const MarkDelivered = Mutation.make("MarkDelivered", {
  document: Conversation,
  payload: { messageId: Schema.String }
})

export const MarkRead = Mutation.make("MarkRead", {
  document: Conversation,
  payload: { messageId: Schema.String }
})

const MessageRow = Schema.Struct({
  messageId: Schema.String,
  sourceDocumentId: Identity.DocumentId,
  author: Schema.String,
  body: MessageBody,
  sentAtMillis: Schema.Number,
  deliveredAtMillis: Schema.NullOr(Schema.Number),
  readAtMillis: Schema.NullOr(Schema.Number)
})

export const Messages = Projection.make("Messages", {
  document: Conversation,
  version: 1,
  Row: MessageRow,
  key: (row) => row.messageId,
  project: (snapshot) =>
    Object.entries(snapshot.value).map(([messageId, entry]) => ({
      messageId,
      sourceDocumentId: snapshot.documentId,
      author: entry.author,
      body: entry.body,
      sentAtMillis: entry.sentAtMillis,
      deliveredAtMillis: entry.deliveredAtMillis,
      readAtMillis: entry.readAtMillis
    }))
})

export const ListMessages = Query.make("ListMessages", {
  payload: { conversationId: Identity.DocumentId },
  success: Schema.Array(MessageRow),
  dependsOn: [Messages]
})

const ConversationSummary = Schema.Struct({
  conversationId: Identity.DocumentId,
  lastAuthor: Schema.NullOr(Schema.String),
  lastBody: Schema.NullOr(Schema.String),
  lastSentAtMillis: Schema.NullOr(Schema.Number),
  unreadCount: Schema.Number
})

export const ConversationSummaries = Query.make("ConversationSummaries", {
  payload: { me: Schema.String },
  success: Schema.Array(ConversationSummary),
  dependsOn: [Messages]
})

const UndeliveredRow = Schema.Struct({
  messageId: Schema.String,
  conversationId: Identity.DocumentId
})

export const UndeliveredInbound = Query.make("UndeliveredInbound", {
  payload: { me: Schema.String },
  success: Schema.Array(UndeliveredRow),
  dependsOn: [Messages]
})

export const definition = ReplicaDefinition.make({
  name: "effect-local-chat",
  documents: DocumentSet.make(Conversation),
  mutations: [SendMessage, MarkDelivered, MarkRead],
  projections: [Messages],
  queries: [ListMessages, ConversationSummaries, UndeliveredInbound],
  transients: [Activity]
})

const MessagesSql = SqlProjection.make(Messages, {
  table: "chat_messages_v1",
  migrations: [{
    id: 1,
    name: "chat_messages_v1",
    run: (sql, table) =>
      sql`CREATE TABLE IF NOT EXISTS ${sql(table)} (
        message_id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL,
        author TEXT NOT NULL,
        body TEXT NOT NULL,
        sent_at REAL NOT NULL,
        delivered_at REAL,
        read_at REAL
      )`.pipe(Effect.asVoid)
  }],
  deleteByDocument: (sql, table, documentId) =>
    sql`DELETE FROM ${sql(table)} WHERE conversation_id = ${documentId}`.pipe(Effect.asVoid),
  insert: (sql, table, row) =>
    sql`INSERT INTO ${sql(table)} (
      message_id, conversation_id, author, body, sent_at, delivered_at, read_at
    ) VALUES (
      ${row.messageId}, ${row.sourceDocumentId}, ${row.author}, ${row.body},
      ${row.sentAtMillis}, ${row.deliveredAtMillis}, ${row.readAtMillis}
    )`.pipe(Effect.asVoid)
})

export const sqlProjections = [MessagesSql]

const ListMessagesSql = SqlSchema.findAll({
  Request: ListMessages.payloadSchema,
  Result: MessageRow,
  execute: (payload) =>
    SqlClient.SqlClient.use((sql) =>
      sql`SELECT
        message_id AS messageId,
        conversation_id AS sourceDocumentId,
        author,
        body,
        sent_at AS sentAtMillis,
        delivered_at AS deliveredAtMillis,
        read_at AS readAtMillis
      FROM chat_messages_v1
      WHERE conversation_id = ${payload.conversationId}
      ORDER BY sent_at ASC, message_id ASC`
    )
})

const ConversationSummariesSql = SqlSchema.findAll({
  Request: ConversationSummaries.payloadSchema,
  Result: ConversationSummary,
  execute: (payload) =>
    SqlClient.SqlClient.use((sql) =>
      sql`SELECT
        latest.conversation_id AS conversationId,
        latest.author AS lastAuthor,
        latest.body AS lastBody,
        latest.sent_at AS lastSentAtMillis,
        (SELECT COUNT(*) FROM chat_messages_v1 unread
          WHERE unread.conversation_id = latest.conversation_id
            AND unread.author <> ${payload.me}
            AND unread.read_at IS NULL) AS unreadCount
      FROM chat_messages_v1 latest
      WHERE latest.message_id = (SELECT newest.message_id FROM chat_messages_v1 newest
        WHERE newest.conversation_id = latest.conversation_id
        ORDER BY newest.sent_at DESC, newest.message_id DESC LIMIT 1)`
    )
})

const UndeliveredInboundSql = SqlSchema.findAll({
  Request: UndeliveredInbound.payloadSchema,
  Result: UndeliveredRow,
  execute: (payload) =>
    SqlClient.SqlClient.use((sql) =>
      sql`SELECT message_id AS messageId, conversation_id AS conversationId
      FROM chat_messages_v1
      WHERE author <> ${payload.me} AND delivered_at IS NULL
      ORDER BY sent_at ASC`
    )
})

export const DomainLive = Layer.mergeAll(
  SendMessage.toLayer(({ draft, payload }) => {
    if (draft[payload.messageId] !== undefined) return undefined
    draft[payload.messageId] = {
      author: payload.author,
      body: payload.body,
      sentAtMillis: Date.now(),
      deliveredAtMillis: null,
      readAtMillis: null
    }
    return undefined
  }),
  MarkDelivered.toLayer(({ draft, payload }) => {
    const entry = draft[payload.messageId]
    if (entry === undefined || entry.deliveredAtMillis !== null) return undefined
    entry.deliveredAtMillis = Date.now()
    return undefined
  }),
  MarkRead.toLayer(({ draft, payload }) => {
    const entry = draft[payload.messageId]
    if (entry === undefined) return undefined
    const now = Date.now()
    if (entry.deliveredAtMillis === null) entry.deliveredAtMillis = now
    if (entry.readAtMillis === null) entry.readAtMillis = now
    return undefined
  }),
  ListMessages.toLayer((payload) => ListMessagesSql(payload).pipe(Effect.orDie)),
  ConversationSummaries.toLayer((payload) => ConversationSummariesSql(payload).pipe(Effect.orDie)),
  UndeliveredInbound.toLayer((payload) => UndeliveredInboundSql(payload).pipe(Effect.orDie))
)

export const limits: ReplicaLimits.Values = {
  maxBackupBytes: 16 * 1024 * 1024,
  maxChunkBytes: 64 * 1024,
  maxArchiveRecords: 10_000,
  maxJsonDepth: 64,
  maxConflictDepth: 64,
  maxConflictNodes: 100_000,
  maxConflictAlternatives: 10_000,
  maxConflictPathSegments: 128,
  maxConflictValueBytes: 16 * 1024 * 1024,
  maxConflictSourceChanges: 100_000,
  maxConflictSourceOperations: 100_000,
  maxConflictSourceBytes: 64 * 1024 * 1024,
  maxSyncMessageBytes: 1024 * 1024,
  maxPeerSendMillis: 30_000,
  maxSyncChangesPerMessage: 1_000,
  maxSyncDependencyEdgesPerMessage: 10_000,
  maxSyncOperationsPerMessage: 100_000,
  maxPendingBytesPerDocument: 16 * 1024 * 1024,
  maxPendingBytesPerPeer: 32 * 1024 * 1024,
  maxPendingBytesPerReplica: 64 * 1024 * 1024,
  maxPendingAgeMillis: 7 * 24 * 60 * 60 * 1000,
  maxPendingChangesPerDocument: 10_000,
  maxPendingChangesPerPeer: 20_000,
  maxPendingChangesPerReplica: 50_000,
  maxPendingDependencyEdgesPerDocument: 100_000,
  maxPendingDependencyEdgesPerPeer: 200_000,
  maxPendingDependencyEdgesPerReplica: 500_000,
  maxSessions: 32,
  // Page to owner streams, not relay sessions. A tab holds the invalidation stream, one activity
  // stream per conversation (each transient client opens its own), and one delivery stream per
  // rendered outbound message — so this has to clear the longest conversation on screen.
  maxStreamsPerSession: 256,
  maxInFlightPerSession: 512,
  maxQueuedRpc: 1_024,
  maxQueuedPermits: 1_024,
  maxActiveRestores: 1_024,
  maxRestoresPerSession: 128,
  maxRestoreMillis: 30_000,
  maxRestorePullMillis: 10_000,
  maxRestoreCoalesceMillis: 25,
  maxRestoreErrorBytes: 4_096
}
