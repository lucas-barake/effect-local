import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Ephemeral from "@lucas-barake/effect-local/Ephemeral"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Model from "@lucas-barake/effect-local/Model"
import * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Query from "@lucas-barake/effect-local/Query"
import * as Schema from "effect/Schema"

// ---------------------------------------------------------------------------
// Branded identities
//
// Every id the domain passes around is nominal: a message id can never be
// passed where a conversation id is expected, and a user id never mixes with
// either. Brands carry no runtime checks beyond the underlying string.
// ---------------------------------------------------------------------------

export const UserId = Schema.String.pipe(Schema.brand("@effect-local/example-chat/UserId"))
export type UserId = typeof UserId.Type

export const ConversationId = Schema.String.pipe(Schema.brand("@effect-local/example-chat/ConversationId"))
export type ConversationId = typeof ConversationId.Type

export const MessageId = Schema.String.pipe(Schema.brand("@effect-local/example-chat/MessageId"))
export type MessageId = typeof MessageId.Type

// ---------------------------------------------------------------------------
// Users
//
// The roster is hard-coded: this is an example, not an identity provider. The
// server still runs a real authentication and authorization flow over these
// users - login issues a bearer token, the RPC middleware sends it, and the
// server checks every access, mutation, and read against the principal.
// ---------------------------------------------------------------------------

export interface ChatUser {
  readonly id: UserId
  readonly name: string
  readonly password: string
  readonly color: string
}

export const users: ReadonlyArray<ChatUser> = [
  { id: UserId.make("alice"), name: "Alice", password: "alice123", color: "#00a884" },
  { id: UserId.make("bob"), name: "Bob", password: "bob123", color: "#53bdeb" },
  { id: UserId.make("carol"), name: "Carol", password: "carol123", color: "#d4a72c" },
  { id: UserId.make("dave"), name: "Dave", password: "dave123", color: "#e05c7a" }
]

export const findUser = (id: UserId): ChatUser | undefined => users.find((user) => user.id === id)

/** The token a successful login returns. Stable so a reload can resume a session. */
export const tokenFor = (userId: UserId): string => `chat-token-${userId}`

// ---------------------------------------------------------------------------
// Space and conversations
//
// The whole demo lives in one shared space; every user joins it. Conversation
// ids are deterministic, so either participant can create the conversation
// on demand and both sides compute the same id without a server round trip.
// ---------------------------------------------------------------------------

export const spaceId = Identity.SpaceId.make("spc_5f1c2a3e-7b4d-4f1a-9c2e-3d8a6b5e4f01")

export const groupConversationId = ConversationId.make("group:everyone")

export const dmConversationId = (firstUserId: UserId, secondUserId: UserId): ConversationId =>
  ConversationId.make(`dm:${[firstUserId, secondUserId].toSorted().join(":")}`)

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

export const Conversation = Model.make("Conversation", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({
    id: ConversationId,
    kind: Schema.Literals(["dm", "group"]),
    memberIds: Schema.Array(UserId),
    createdBy: UserId,
    createdAt: Schema.Number
  })
})
export type Conversation = typeof Conversation.schema.Type

const MessageSchema = Schema.Struct({
  id: MessageId,
  conversationId: ConversationId,
  senderId: UserId,
  text: Schema.String,
  createdAt: Schema.Number
})

export const Message = Model.make("Message", {
  version: 1,
  key: Schema.String,
  schema: MessageSchema,
  indexes: {
    byConversation: {
      version: 1,
      partition: [{
        name: "conversation",
        affinity: "text",
        schema: ConversationId,
        extract: (message: typeof MessageSchema.Type) => message.conversationId
      }],
      sort: [{
        name: "created",
        affinity: "real",
        schema: Schema.Number,
        extract: (message: typeof MessageSchema.Type) => message.createdAt
      }]
    }
  }
})
export type Message = typeof MessageSchema.Type

/**
 * Delivery and read positions, one row per (conversation, user). Both
 * counters are message `createdAt` millis and only ever move forward, so
 * concurrent advances merge by max and replay is idempotent.
 */
export const ConversationReadState = Model.make("ConversationReadState", {
  version: 1,
  key: Schema.String,
  schema: Schema.Struct({
    conversationId: ConversationId,
    userId: UserId,
    deliveredUpTo: Schema.Number,
    readUpTo: Schema.Number
  })
})
export type ConversationReadState = typeof ConversationReadState.schema.Type

// Unambiguous durable key: a JSON tuple, so a `:` inside either component
// (conversation ids always contain one) can never make two pairs collide.
const readStateKeyCodec = Schema.fromJsonString(Schema.Tuple([ConversationId, UserId]))
export const readStateKey = (conversationId: ConversationId, userId: UserId): string =>
  // oxlint-disable-next-line effect-local/noManualEffectBoundary -- Encoding two branded strings is total; called as plain sync code inside Effect generators per repo rules.
  Schema.encodeSync(readStateKeyCodec)([conversationId, userId])

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

export class SendMessageRejection extends Schema.TaggedErrorClass<SendMessageRejection>(
  "@effect-local/example-chat/SendMessageRejection"
)("SendMessageRejection", {
  reason: Schema.Literals(["ConversationNotFound", "NotAMember"])
}) {}

export const SendMessage = Mutation.make("SendMessage", {
  version: 1,
  payload: Message.schema,
  success: Message.schema,
  rejection: SendMessageRejection
})

export class StartConversationRejection extends Schema.TaggedErrorClass<StartConversationRejection>(
  "@effect-local/example-chat/StartConversationRejection"
)("StartConversationRejection", {
  reason: Schema.Literals(["UnknownMember", "IdMismatch", "InvalidGroupRoster"])
}) {}

export const StartConversation = Mutation.make("StartConversation", {
  version: 1,
  payload: Conversation.schema,
  success: Conversation.schema,
  rejection: StartConversationRejection
})

export const AdvanceDelivery = Mutation.make("AdvanceDelivery", {
  version: 1,
  payload: {
    conversationId: ConversationId,
    userId: UserId,
    upTo: Schema.Number
  },
  success: ConversationReadState.schema
})

export const AdvanceRead = Mutation.make("AdvanceRead", {
  version: 1,
  payload: {
    conversationId: ConversationId,
    userId: UserId,
    upTo: Schema.Number
  },
  success: ConversationReadState.schema
})

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

export const ConversationSummary = Schema.Struct({
  conversation: Conversation.schema,
  lastMessage: Schema.NullOr(Message.schema),
  lastIncomingMessage: Schema.NullOr(Message.schema),
  unreadCount: Schema.Number,
  myDeliveredUpTo: Schema.Number,
  myReadUpTo: Schema.Number
})
export type ConversationSummary = typeof ConversationSummary.Type

export const ConversationSummaries = Query.make("ConversationSummaries", {
  payload: { userId: UserId },
  success: Schema.Array(ConversationSummary)
})

export const MessagesWindow = Query.make("MessagesWindow", {
  payload: { conversationId: ConversationId, limit: Schema.Number },
  success: Schema.Struct({
    items: Schema.Array(Message.schema),
    hasMore: Schema.Boolean
  })
})

export const ReadStates = Query.make("ReadStates", {
  payload: { conversationId: ConversationId },
  success: Schema.Array(ConversationReadState.schema)
})

// ---------------------------------------------------------------------------
// Ephemeral channels: typing state and presence roster. These never touch the
// durable log - the server expires them by TTL.
// ---------------------------------------------------------------------------

export const Typing = Ephemeral.make("typing", {
  kind: "state",
  key: ConversationId,
  payload: { userId: UserId }
})

export const PresenceProfile = Ephemeral.member({
  userId: UserId,
  name: Schema.String
})

export const ephemerals = [Typing]
export const profiles = { presence: PresenceProfile }

// ---------------------------------------------------------------------------
// Definition
// ---------------------------------------------------------------------------

export const definition = Definition.make({
  version: 1,
  models: [Conversation, Message, ConversationReadState],
  mutations: [SendMessage, StartConversation, AdvanceDelivery, AdvanceRead],
  queries: [ConversationSummaries, MessagesWindow, ReadStates]
})

// ---------------------------------------------------------------------------
// Receipt ticks (pure derivation, shared by UI and tests)
// ---------------------------------------------------------------------------

export type TickState = "failed" | "pending" | "sent" | "delivered" | "read"

/**
 * WhatsApp tick semantics for one outgoing message: failed (terminal rejection
 * or local failure, retryable), pending (in the local outbox), sent (server
 * accepted), delivered (every other member's deliveredUpTo covers it), read
 * (every other member's readUpTo covers it).
 */
export const tickState = (options: {
  readonly failed: boolean
  readonly pending: boolean
  readonly message: Message
  readonly senderId: UserId
  readonly readStates: ReadonlyArray<ConversationReadState>
  readonly memberIds: ReadonlyArray<UserId>
}): TickState => {
  if (options.failed) return "failed"
  if (options.pending) return "pending"
  const others = options.memberIds.filter((memberId) => memberId !== options.senderId)
  const covered = (upTo: (state: ConversationReadState) => number) =>
    others.every((memberId) => {
      const state = options.readStates.find((entry) =>
        entry.conversationId === options.message.conversationId && entry.userId === memberId
      )
      return state !== undefined && upTo(state) >= options.message.createdAt
    })
  if (others.length > 0 && covered((state) => state.readUpTo)) return "read"
  if (others.length > 0 && covered((state) => state.deliveredUpTo)) return "delivered"
  return "sent"
}
