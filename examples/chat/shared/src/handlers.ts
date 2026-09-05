import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import {
  AdvanceDelivery,
  AdvanceRead,
  Conversation,
  type ConversationId,
  ConversationReadState,
  ConversationSummaries,
  dmConversationId,
  groupConversationId,
  Message,
  MessagesWindow,
  readStateKey,
  ReadStates,
  SendMessage,
  SendMessageRejection,
  StartConversation,
  StartConversationRejection,
  type UserId,
  users
} from "./domain.js"

/**
 * Mutation and query handlers. The same layer feeds the browser replica and
 * the server store, so every handler is deterministic: timestamps, ids, and
 * user intent arrive in the payload, never from the handler body.
 */

const layerSendMessage = SendMessage.toLayer(Effect.fnUntraced(function*({ payload, transaction }) {
  const conversation = yield* transaction.get(Conversation, payload.conversationId)
  if (Option.isNone(conversation)) {
    return yield* new SendMessageRejection({ reason: "ConversationNotFound" })
  }
  if (!conversation.value.memberIds.includes(payload.senderId)) {
    return yield* new SendMessageRejection({ reason: "NotAMember" })
  }
  yield* transaction.set(Message, payload.id, payload)
  return payload
}))

const layerStartConversation = StartConversation.toLayer(Effect.fnUntraced(function*({ payload, transaction }) {
  let expectedId: ConversationId | undefined = undefined
  if (payload.kind === "group") expectedId = groupConversationId
  else if (payload.memberIds.length === 2) expectedId = dmConversationId(payload.memberIds[0], payload.memberIds[1])
  if (expectedId === undefined || expectedId !== payload.id) {
    return yield* new StartConversationRejection({ reason: "IdMismatch" })
  }
  if (!payload.memberIds.every((memberId) => users.some((user) => user.id === memberId))) {
    return yield* new StartConversationRejection({ reason: "UnknownMember" })
  }
  // The group id is deterministic and create-if-absent, so the first write
  // pins the roster forever; only the complete roster is a valid group.
  if (payload.kind === "group") {
    const roster = users.map((user) => user.id)
    // Set equality both ways: length + memberIds ⊆ roster alone would let a
    // duplicate substitute for a missing member and pin the group without
    // them forever (the deterministic id is create-if-absent).
    const complete = payload.memberIds.length === roster.length &&
      roster.every((memberId) => payload.memberIds.includes(memberId))
    if (!complete) {
      return yield* new StartConversationRejection({ reason: "InvalidGroupRoster" })
    }
  }
  const existing = yield* transaction.get(Conversation, payload.id)
  if (Option.isSome(existing)) return existing.value
  yield* transaction.set(Conversation, payload.id, payload)
  return payload
}))

const emptyReadState = (conversationId: ConversationId, userId: UserId) =>
  ConversationReadState.schema.make({ conversationId, userId, deliveredUpTo: 0, readUpTo: 0 })

const layerAdvanceDelivery = AdvanceDelivery.toLayer(Effect.fnUntraced(function*({ payload, transaction }) {
  const current = yield* transaction.get(ConversationReadState, readStateKey(payload.conversationId, payload.userId))
  const next = {
    ...Option.getOrElse(current, () => emptyReadState(payload.conversationId, payload.userId)),
    deliveredUpTo: Math.max(
      Option.match(current, { onNone: () => 0, onSome: (state) => state.deliveredUpTo }),
      payload.upTo
    )
  }
  if (Option.isSome(current) && current.value.deliveredUpTo >= payload.upTo) return current.value
  yield* transaction.set(ConversationReadState, readStateKey(payload.conversationId, payload.userId), next)
  return next
}))

const layerAdvanceRead = AdvanceRead.toLayer(Effect.fnUntraced(function*({ payload, transaction }) {
  const current = yield* transaction.get(ConversationReadState, readStateKey(payload.conversationId, payload.userId))
  const base = Option.getOrElse(current, () => emptyReadState(payload.conversationId, payload.userId))
  // Reading implies delivery: readUpTo can never trail deliveredUpTo.
  const next = {
    ...base,
    deliveredUpTo: Math.max(base.deliveredUpTo, payload.upTo),
    readUpTo: Math.max(base.readUpTo, payload.upTo)
  }
  if (
    Option.isSome(current) && current.value.readUpTo >= payload.upTo && current.value.deliveredUpTo >= payload.upTo
  ) {
    return current.value
  }
  yield* transaction.set(ConversationReadState, readStateKey(payload.conversationId, payload.userId), next)
  return next
}))

const layerConversationSummaries = ConversationSummaries.toLayer(({ payload, query }) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: Schema.Struct({
      conversation: Schema.fromJsonString(Conversation.schema),
      lastMessage: Schema.NullOr(Schema.fromJsonString(Message.schema)),
      lastIncomingMessage: Schema.NullOr(Schema.fromJsonString(Message.schema)),
      unreadCount: Schema.Number,
      myDeliveredUpTo: Schema.Number,
      myReadUpTo: Schema.Number
    }),
    execute: () =>
      query.sql([Conversation, Message, ConversationReadState], (sql) =>
        sql`SELECT
          c."value" AS "conversation",
          (SELECT m."value" FROM "Message" m WHERE m."conversationId" = c."key"
            ORDER BY m."createdAt" DESC, m."key" DESC LIMIT 1) AS "lastMessage",
          (SELECT i."value" FROM "Message" i WHERE i."conversationId" = c."key" AND i."senderId" != ${payload.userId}
            ORDER BY i."createdAt" DESC, i."key" DESC LIMIT 1) AS "lastIncomingMessage",
          (SELECT COUNT(*) FROM "Message" u WHERE u."conversationId" = c."key" AND u."senderId" != ${payload.userId}
            AND u."createdAt" > COALESCE(
              (SELECT r."readUpTo" FROM "ConversationReadState" r
                WHERE r."conversationId" = c."key" AND r."userId" = ${payload.userId}), 0)) AS "unreadCount",
          COALESCE((SELECT d."deliveredUpTo" FROM "ConversationReadState" d
            WHERE d."conversationId" = c."key" AND d."userId" = ${payload.userId}), 0) AS "myDeliveredUpTo",
          COALESCE((SELECT s."readUpTo" FROM "ConversationReadState" s
            WHERE s."conversationId" = c."key" AND s."userId" = ${payload.userId}), 0) AS "myReadUpTo"
        FROM "Conversation" c`)
  })(undefined).pipe(
    Effect.map((rows) =>
      rows.map((row) => ({
        conversation: row.conversation,
        lastMessage: row.lastMessage,
        lastIncomingMessage: row.lastIncomingMessage,
        unreadCount: row.unreadCount,
        myDeliveredUpTo: row.myDeliveredUpTo,
        myReadUpTo: row.myReadUpTo
      }))
    ),
    Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(new ReplicaError.StorageCorrupt({ message: "Conversation summary rows are undecodable", cause })))
  )
)

const layerMessagesWindow = MessagesWindow.toLayer(({ payload, query }) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: Schema.Struct({ value: Schema.fromJsonString(Message.schema) }),
    execute: () =>
      query.sql([Message], (sql) =>
        sql`SELECT "value" FROM "Message" WHERE "conversationId" = ${payload.conversationId}
          ORDER BY "createdAt" DESC, "key" DESC LIMIT ${payload.limit + 1}`)
  })(undefined).pipe(
    Effect.map((rows) => ({
      // Newest first from SQL; the UI renders chronologically.
      items: rows.slice(0, payload.limit).map((row) => row.value).toReversed(),
      hasMore: rows.length > payload.limit
    })),
    Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(new ReplicaError.StorageCorrupt({ message: "Message rows are undecodable", cause })))
  )
)

const layerReadStates = ReadStates.toLayer(({ payload, query }) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: Schema.Struct({ value: Schema.fromJsonString(ConversationReadState.schema) }),
    execute: () =>
      query.sql([ConversationReadState], (sql) =>
        sql`SELECT "value" FROM "ConversationReadState" WHERE "conversationId" = ${payload.conversationId}
          ORDER BY "key" ASC`)
  })(undefined).pipe(
    Effect.map((rows) =>
      rows.map((row) =>
        row.value
      )
    ),
    Effect.catchTag(
      "SchemaError",
      (cause) => Effect.fail(new ReplicaError.StorageCorrupt({ message: "Read state rows are undecodable", cause }))
    )
  )
)

/** Mutation handlers - required by both the browser replica and the server store. */
export const layerMutations = Layer.mergeAll(
  layerSendMessage,
  layerStartConversation,
  layerAdvanceDelivery,
  layerAdvanceRead
)

/** Query handlers - queries always run against the local replica, so only the client needs these. */
export const layerQueries = Layer.mergeAll(
  layerConversationSummaries,
  layerMessagesWindow,
  layerReadStates
)

export const layerDomain = Layer.mergeAll(layerMutations, layerQueries)
