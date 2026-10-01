import {
  type ConversationId,
  findUser,
  type Message,
  type TickState,
  tickState,
  type UserId
} from "@effect-local/example-chat-shared/domain"
import { formatTime } from "@effect-local/example-chat-shared/time"
import { useAtomSet, useAtomValue } from "@effect/atom-react"
import * as Duration from "effect/Duration"
import * as Option from "effect/Option"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import { useEffect, useRef, useState } from "react"
import { AppState, FlatList, Pressable, StyleSheet, Text, TextInput, View } from "react-native"
import type { ChatClient } from "./runtime.js"
import { Avatar, colors } from "./theme.js"

const typingTtl = Duration.seconds(3)
const typingRefreshMillis = Duration.toMillis(Duration.seconds(1))

interface Row {
  readonly message: Message
  readonly failed: boolean
}

const tickLabels: Record<TickState, string> = {
  failed: "!",
  pending: "…",
  sent: "✓",
  delivered: "✓✓",
  read: "✓✓"
}

const senderName = (userId: UserId) => findUser(userId)?.name ?? userId

const Composer = ({ client, me, conversationId }: {
  readonly client: ChatClient
  readonly me: UserId
  readonly conversationId: ConversationId
}) => {
  const publishTyping = useAtomSet(client.publishTyping)
  const clearTyping = useAtomSet(client.clearTyping)
  const sendMessage = useAtomSet(client.sendMessage, { mode: "promiseExit" })
  const [draft, setDraft] = useState("")
  const typingPublishedAt = useRef<number | null>(null)

  const stopTyping = () => {
    if (typingPublishedAt.current === null) return
    typingPublishedAt.current = null
    clearTyping({ key: conversationId })
  }

  useEffect(() => {
    const publishedAt = typingPublishedAt
    return () => {
      if (publishedAt.current !== null) clearTyping({ key: conversationId })
    }
  }, [clearTyping, conversationId])

  const edit = (value: string) => {
    setDraft(value)
    if (value.trim().length === 0) {
      stopTyping()
      return
    }
    const now = Date.now()
    const publishedAt = typingPublishedAt.current
    if (publishedAt !== null && now - publishedAt < typingRefreshMillis) return
    typingPublishedAt.current = now
    publishTyping({ key: conversationId, payload: { userId: me }, ttl: typingTtl })
  }

  const send = () => {
    const text = draft.trim()
    if (text.length === 0) return
    setDraft("")
    stopTyping()
    void sendMessage({ conversationId, text })
  }

  return (
    <View style={styles.composer}>
      <TextInput
        style={styles.input}
        value={draft}
        placeholder="Type a message"
        onChangeText={edit}
        onBlur={stopTyping}
        onSubmitEditing={send}
        returnKeyType="send"
      />
      <Pressable
        accessibilityRole="button"
        accessibilityLabel="Send message"
        style={[styles.send, draft.trim().length === 0 && styles.sendDisabled]}
        disabled={draft.trim().length === 0}
        onPress={send}
      >
        <Text style={styles.sendText}>➤</Text>
      </Pressable>
    </View>
  )
}

export const Chat = ({ client, me, conversationId, onBack }: {
  readonly client: ChatClient
  readonly me: UserId
  readonly conversationId: ConversationId
  readonly onBack: () => void
}) => {
  const summaries = AsyncResult.getOrElse(useAtomValue(client.summariesAtom), () => [])
  const presence = AsyncResult.value(useAtomValue(client.membersAtom))
  const typing = AsyncResult.getOrElse(useAtomValue(client.typingEntries), () => [])
  const readStatesAtom = client.readStates(conversationId)
  const readStates = AsyncResult.value(useAtomValue(readStatesAtom))
  const windowResult = useAtomValue(client.messagesWindow(conversationId))
  const pendingResult = useAtomValue(client.pendingSendsAtom)
  const failed = useAtomValue(client.failedMessages)
  const loadEarlier = useAtomSet(client.loadEarlier(conversationId))
  const markRead = useAtomSet(client.markRead)
  const retryMessage = useAtomSet(client.retryMessage, { mode: "promiseExit" })
  const discardMessage = useAtomSet(client.discardMessage)

  const summary = summaries.find((entry) => entry.conversation.id === conversationId)
  const conversation = summary?.conversation
  let peer: UserId | undefined
  if (conversation !== undefined && conversation.kind === "dm") {
    peer = conversation.memberIds.find((memberId) => memberId !== me)
  }
  let title = "…"
  let color = colors.muted
  if (conversation?.kind === "group") title = "Everyone"
  if (peer !== undefined) {
    title = findUser(peer)?.name ?? "Unknown"
    color = findUser(peer)?.color ?? colors.muted
  }

  const typingHere = typing.filter((entry) => entry.key === conversationId && entry.value.userId !== me)
  let subtitle = ""
  if (typingHere.length > 0) {
    subtitle = "typing…"
    if (conversation?.kind === "group") {
      subtitle = `${typingHere.map((entry) => senderName(entry.value.userId)).join(", ")} typing…`
    }
  } else if (conversation?.kind === "group") {
    subtitle = `${conversation.memberIds.length} members`
  } else if (peer !== undefined && Option.isSome(presence)) {
    subtitle = "offline"
    if (presence.value.some((entry) => entry.value.userId === peer)) subtitle = "online"
  }

  const window = AsyncResult.value(windowResult)
  const items = Option.match(window, { onNone: () => [], onSome: (value) => value.items })
  const failedHere = [...failed.values()].filter((message) =>
    message.conversationId === conversationId && !items.some((item) => item.id === message.id)
  )
  const rows: Array<Row> = [
    ...items.map((message): Row => ({ message, failed: failed.has(message.id) })),
    ...failedHere.map((message): Row => ({ message, failed: true }))
  ].sort((left, right) => right.message.createdAt - left.message.createdAt)

  const pending = AsyncResult.value(pendingResult)
  const pendingIds = new Set(Option.getOrElse(pending, () => []).map((entry) => entry.payload.id))
  const stateOf = (row: Row): TickState | undefined => {
    if (conversation === undefined || Option.isNone(readStates) || Option.isNone(pending)) return undefined
    return tickState({
      failed: row.failed,
      pending: pendingIds.has(row.message.id),
      message: row.message,
      senderId: row.message.senderId,
      readStates: readStates.value,
      memberIds: conversation.memberIds
    })
  }

  const lastIncoming = summary?.lastIncomingMessage ?? null
  const myReadUpTo = summary?.myReadUpTo ?? 0
  useEffect(() => {
    const advance = () => {
      if (AppState.currentState === "active" && lastIncoming !== null && lastIncoming.createdAt > myReadUpTo) {
        markRead({ conversationId, userId: me, upTo: lastIncoming.createdAt })
      }
    }
    advance()
    const subscription = AppState.addEventListener("change", advance)
    return () => subscription.remove()
  }, [conversationId, lastIncoming, myReadUpTo, markRead, me])

  return (
    <View style={styles.screen}>
      <View style={styles.header}>
        <Pressable accessibilityRole="button" accessibilityLabel="Back to conversations" onPress={onBack}>
          <Text style={styles.back}>‹</Text>
        </Pressable>
        <Avatar name={title} color={color} size={36} />
        <View style={styles.headerText}>
          <Text style={styles.title}>{title}</Text>
          <Text accessibilityLiveRegion="polite" style={styles.subtitle}>{subtitle}</Text>
        </View>
      </View>
      <FlatList
        style={styles.messages}
        inverted
        data={rows}
        keyExtractor={(row) => row.message.id}
        onEndReached={() => {
          if (Option.isSome(window) && window.value.hasMore) loadEarlier(undefined)
        }}
        renderItem={({ item }) => {
          const mine = item.message.senderId === me
          const state = mine ? stateOf(item) : undefined
          return (
            <View style={[styles.bubble, mine ? styles.outgoing : styles.incoming]}>
              {conversation?.kind === "group" && !mine && (
                <Text style={styles.sender}>{senderName(item.message.senderId)}</Text>
              )}
              <Text style={styles.text}>{item.message.text}</Text>
              <View style={styles.meta}>
                <Text style={styles.time}>{formatTime(item.message.createdAt)}</Text>
                {state !== undefined && (
                  <Text
                    accessibilityLabel={state}
                    style={[styles.tick, state === "read" && styles.tickRead]}
                  >
                    {tickLabels[state]}
                  </Text>
                )}
              </View>
              {item.failed && (
                <View style={styles.failedActions}>
                  <Pressable accessibilityRole="button" onPress={() => void retryMessage(item.message)}>
                    <Text style={styles.action}>Retry</Text>
                  </Pressable>
                  <Pressable accessibilityRole="button" onPress={() => discardMessage(item.message)}>
                    <Text style={styles.action}>Discard</Text>
                  </Pressable>
                </View>
              )}
            </View>
          )
        }}
      />
      <Composer client={client} me={me} conversationId={conversationId} />
    </View>
  )
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: colors.background },
  header: { flexDirection: "row", alignItems: "center", gap: 10, padding: 12, backgroundColor: colors.accent },
  back: { color: "#ffffff", fontSize: 32, paddingHorizontal: 4 },
  headerText: { flex: 1 },
  title: { color: "#ffffff", fontSize: 17, fontWeight: "600" },
  subtitle: { color: "#d9fdd3", fontSize: 13 },
  messages: { flex: 1, paddingHorizontal: 10 },
  bubble: { maxWidth: "80%", marginVertical: 3, paddingHorizontal: 10, paddingVertical: 6, borderRadius: 10 },
  outgoing: { alignSelf: "flex-end", backgroundColor: colors.outgoing },
  incoming: { alignSelf: "flex-start", backgroundColor: colors.surface },
  sender: { fontSize: 12, fontWeight: "600", color: colors.accent },
  text: { fontSize: 16 },
  meta: { flexDirection: "row", justifyContent: "flex-end", alignItems: "center", gap: 4 },
  time: { fontSize: 11, color: colors.muted },
  tick: { fontSize: 12, color: colors.muted },
  tickRead: { color: colors.read },
  failedActions: { flexDirection: "row", gap: 16, marginTop: 4 },
  action: { color: "#c0392b", fontWeight: "600" },
  composer: { flexDirection: "row", alignItems: "center", gap: 8, padding: 8 },
  input: {
    flex: 1,
    backgroundColor: colors.surface,
    borderRadius: 22,
    paddingHorizontal: 16,
    paddingVertical: 10,
    fontSize: 16
  },
  send: {
    width: 44,
    height: 44,
    borderRadius: 22,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: colors.accent
  },
  sendDisabled: { opacity: 0.5 },
  sendText: { color: "#ffffff", fontSize: 18 }
})
