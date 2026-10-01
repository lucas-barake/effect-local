import type * as ReplicaAtom from "@lucas-barake/effect-local-rpc/ReplicaAtom"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Clock from "effect/Clock"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import * as Atom from "effect/reactivity/Atom"
import { connecting, connectionChanges } from "./connection.js"
import {
  AdvanceDelivery,
  AdvanceRead,
  type Conversation,
  type ConversationId,
  ConversationSummaries,
  dmConversationId,
  findUser,
  groupConversationId,
  type Message,
  MessageId,
  MessagesWindow,
  PresenceProfile,
  ReadStates,
  SendMessage,
  spaceId,
  StartConversation,
  Typing,
  type UserId,
  users
} from "./domain.js"
import { makeFailedMessages, makeSettlementDaemonBody } from "./settlementDaemon.js"

export type ChatClient<E extends { readonly _tag: string },> = ReturnType<typeof makeChatClient<E>>

const windowPage = 50
const windowLimit = 1_000

const findUserName = (userId: UserId): string => findUser(userId)?.name ?? userId

const mintMessageId = Crypto.Crypto.use((crypto) => crypto.randomUUIDv4).pipe(
  Effect.map((uuid) => MessageId.make(uuid))
)

export const makeChatClient = <E extends { readonly _tag: string },>(graph: ReplicaAtom.Graph<E>, userId: UserId) => {
  const target = { spaceId }

  const failedMessages = makeFailedMessages()

  const presenceAtom = graph.ephemeral(PresenceProfile, {
    ...target,
    value: { userId, name: findUserName(userId) },
    ttl: "30 seconds"
  })
  const membersAtom = graph.ephemeralMembers(presenceAtom)
  const typingEntries = graph.ephemeralState(presenceAtom, Typing)

  const summariesAtom = graph.query(spaceId, ConversationSummaries)({ userId })
  const pendingSendsAtom = graph.pendingFor(spaceId, SendMessage)
  const statusAtom = graph.status(spaceId)
  const syncedAtom = Atom.make((get) => {
    const status = get(statusAtom)
    return AsyncResult.isSuccess(status) && status.value.synced
  })

  const connectionAtom = Atom.make((get) => connectionChanges(get.stream(statusAtom)), { initialValue: connecting })

  const windowSize = Atom.family((_conversationId: ConversationId) => Atom.make(windowPage))
  const loadEarlier = Atom.family((conversationId: ConversationId) =>
    Atom.writable(
      (get) => get(windowSize(conversationId)),
      (context) =>
        context.set(
          windowSize(conversationId),
          Math.min(context.get(windowSize(conversationId)) + windowPage, windowLimit)
        )
    )
  )

  const messagesWindow = Atom.family((conversationId: ConversationId) =>
    Atom.readable((get) => {
      const result = get(
        graph.query(spaceId, MessagesWindow)({ conversationId, limit: get(windowSize(conversationId)) })
      )
      if (!AsyncResult.isInitial(result)) return result
      return Option.match(get.self<typeof result>(), {
        onNone: () => result,
        onSome: (previous) => AsyncResult.waiting(previous)
      })
    })
  )
  const readStates = (conversationId: ConversationId) => graph.query(spaceId, ReadStates)({ conversationId })

  const sendMessage = graph.runtime.fn<{ readonly conversationId: ConversationId; readonly text: string }>()(
    Effect.fn("chat.sendMessage")(function*(input, get) {
      const replica = yield* Replica.Replica
      const space = yield* replica.space(spaceId)
      const createdAt = yield* Clock.currentTimeMillis
      const id = yield* mintMessageId
      const message: Message = {
        id,
        conversationId: input.conversationId,
        senderId: userId,
        text: input.text,
        createdAt
      }
      return yield* space.mutate(SendMessage, message).pipe(
        Effect.onError(() =>
          Effect.sync(() => {
            get.set(failedMessages, new Map(get(failedMessages)).set(message.id, message))
          })
        )
      )
    }),
    { concurrent: true }
  )

  const retryMessage = graph.runtime.fn<Message>()(
    Effect.fn("chat.retryMessage")(function*(message, get) {
      const replica = yield* Replica.Replica
      const space = yield* replica.space(spaceId)
      yield* space.mutate(SendMessage, message).pipe(
        Effect.onError((cause) =>
          Effect.logWarning("chat: retry failed").pipe(
            Effect.annotateLogs({ messageId: message.id, cause: String(cause) })
          )
        )
      )
      const next = new Map(get(failedMessages))
      next.delete(message.id)
      get.set(failedMessages, next)
    }),
    { concurrent: true }
  )

  const discardMessage = graph.runtime.fn<Message>()(
    Effect.fnUntraced(function*(message, get) {
      const next = new Map(get(failedMessages))
      next.delete(message.id)
      get.set(failedMessages, next)
    }),
    { concurrent: true }
  )

  const startConversation = graph.runtime.fn<
    { readonly kind: "dm"; readonly userId: UserId } | { readonly kind: "group" }
  >()(
    Effect.fn("chat.startConversation")(function*(input) {
      const createdAt = yield* Clock.currentTimeMillis
      let conversation: Conversation
      if (input.kind === "group") {
        conversation = {
          id: groupConversationId,
          kind: "group",
          memberIds: users.map((user) => user.id),
          createdBy: userId,
          createdAt
        }
      } else {
        conversation = {
          id: dmConversationId(userId, input.userId),
          kind: "dm",
          memberIds: [userId, input.userId].sort(),
          createdBy: userId,
          createdAt
        }
      }
      const replica = yield* Replica.Replica
      const space = yield* replica.space(spaceId)
      return yield* space.mutate(StartConversation, conversation).pipe(
        Effect.onError((cause) =>
          Effect.logWarning("chat: could not start conversation").pipe(
            Effect.annotateLogs({ kind: input.kind, cause: String(cause) })
          )
        )
      )
    }),
    { concurrent: true }
  )

  const markRead = graph.mutation(spaceId, AdvanceRead)
  const publishTyping = graph.publishEphemeral(Typing, target)
  const clearTyping = graph.removeEphemeral(Typing, target)

  const deliveryDaemon = graph.runtime.atom(
    Effect.fnUntraced(function*(get) {
      const summaries = yield* get.result(summariesAtom, { suspendOnWaiting: true })
      const replica = yield* Replica.Replica
      const space = yield* replica.space(spaceId)
      yield* Effect.forEach(
        summaries,
        (summary) => {
          if (!summary.conversation.memberIds.includes(userId)) return Effect.void
          const incoming = summary.lastIncomingMessage
          if (incoming === null || incoming.createdAt <= summary.myDeliveredUpTo) return Effect.void
          return space.mutate(AdvanceDelivery, {
            conversationId: summary.conversation.id,
            userId,
            upTo: incoming.createdAt
          }).pipe(
            Effect.catch((error) =>
              Effect.logWarning("chat: could not advance delivery").pipe(
                Effect.annotateLogs({ conversationId: summary.conversation.id, error: String(error) })
              )
            )
          )
        },
        { discard: true }
      )
    })
  )

  const settlementDaemon = graph.runtime.atom(makeSettlementDaemonBody(failedMessages))

  return {
    presenceAtom,
    membersAtom,
    summariesAtom,
    pendingSendsAtom,
    connectionAtom,
    syncedAtom,
    sendMessage,
    retryMessage,
    discardMessage,
    startConversation,
    markRead,
    publishTyping,
    clearTyping,
    deliveryDaemon,
    settlementDaemon,
    messagesWindow,
    loadEarlier,
    readStates,
    typingEntries,
    failedMessages
  }
}
