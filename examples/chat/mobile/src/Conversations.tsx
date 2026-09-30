import type { LoginResponse } from "@effect-local/example-chat-shared/auth"
import {
  type ConversationId,
  type ConversationSummary,
  dmConversationId,
  findUser,
  groupConversationId,
  type UserId,
  users
} from "@effect-local/example-chat-shared/domain"
import { formatTime } from "@effect-local/example-chat-shared/time"
import { useAtomSet, useAtomValue } from "@effect/atom-react"
import * as Option from "effect/Option"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import { FlatList, Pressable, StyleSheet, Text, View } from "react-native"
import { type ChatClient, logoutAtom } from "./runtime.js"
import { Avatar, colors } from "./theme.js"

const peerOf = (summary: ConversationSummary, me: UserId) => {
  if (summary.conversation.kind === "group") return undefined
  const peer = summary.conversation.memberIds.find((memberId) => memberId !== me)
  if (peer === undefined) return undefined
  return findUser(peer)
}

const titleOf = (summary: ConversationSummary, me: UserId) => {
  if (summary.conversation.kind === "group") return "Everyone"
  return peerOf(summary, me)?.name ?? "Unknown"
}

const colorOf = (summary: ConversationSummary, me: UserId) => peerOf(summary, me)?.color ?? colors.muted

const lastActivity = (summary: ConversationSummary) => summary.lastMessage?.createdAt ?? summary.conversation.createdAt

export const Conversations = ({ client, session, onOpen }: {
  readonly client: ChatClient
  readonly session: LoginResponse
  readonly onOpen: (conversationId: ConversationId) => void
}) => {
  const me = session.userId
  const logout = useAtomSet(logoutAtom)
  const summariesResult = useAtomValue(client.summariesAtom)
  const synced = useAtomValue(client.syncedAtom)
  const members = AsyncResult.getOrElse(useAtomValue(client.membersAtom), () => [])
  const onlineIds = new Set(members.map((entry) => entry.value.userId))
  const summaries = Option.getOrElse(AsyncResult.value(summariesResult), () => [])
  const mine = summaries
    .filter((summary) => summary.conversation.memberIds.includes(me))
    .sort((left, right) => lastActivity(right) - lastActivity(left))
  const knownIds = new Set(summaries.map((summary) => summary.conversation.id))
  const newDmUsers = users.filter((user) => user.id !== me && !knownIds.has(dmConversationId(me, user.id)))
  const settled = synced && !summariesResult.waiting

  return (
    <View style={styles.screen}>
      <View style={styles.header}>
        <Avatar name={session.name} color={session.color} size={36} />
        <Text style={styles.me}>{session.name}</Text>
        <Pressable accessibilityRole="button" onPress={() => logout(undefined)}>
          <Text style={styles.logout}>Log out</Text>
        </Pressable>
      </View>
      <FlatList
        data={mine}
        keyExtractor={(summary) => summary.conversation.id}
        renderItem={({ item }) => {
          const peer = peerOf(item, me)
          let preview = "No messages yet"
          if (item.lastMessage !== null) {
            preview = `${item.lastMessage.senderId === me ? "You: " : ""}${item.lastMessage.text}`
          }
          return (
            <Pressable
              accessibilityRole="button"
              style={styles.row}
              onPress={() => onOpen(item.conversation.id)}
            >
              <View>
                <Avatar name={titleOf(item, me)} color={colorOf(item, me)} />
                {peer !== undefined && onlineIds.has(peer.id) && <View style={styles.presence} />}
              </View>
              <View style={styles.rowBody}>
                <View style={styles.rowTop}>
                  <Text style={styles.rowTitle}>{titleOf(item, me)}</Text>
                  {item.lastMessage !== null && (
                    <Text style={styles.rowTime}>{formatTime(item.lastMessage.createdAt)}</Text>
                  )}
                </View>
                <View style={styles.rowTop}>
                  <Text style={styles.preview} numberOfLines={1}>{preview}</Text>
                  {item.unreadCount > 0 && (
                    <Text accessibilityLabel={`${item.unreadCount} unread`} style={styles.badge}>
                      {item.unreadCount}
                    </Text>
                  )}
                </View>
              </View>
            </Pressable>
          )
        }}
        ListEmptyComponent={settled
          ? <Text style={styles.empty}>No conversations yet. Start one below.</Text>
          : null}
        ListFooterComponent={settled && (newDmUsers.length > 0 || !knownIds.has(groupConversationId))
          ? (
            <View style={styles.start}>
              <Text style={styles.startTitle}>Start a chat</Text>
              {newDmUsers.map((user) => (
                <Pressable
                  key={user.id}
                  accessibilityRole="button"
                  style={styles.startRow}
                  onPress={() => onOpen(dmConversationId(me, user.id))}
                >
                  <Avatar name={user.name} color={user.color} size={32} />
                  <Text>{user.name}</Text>
                </Pressable>
              ))}
              {!knownIds.has(groupConversationId) && (
                <Pressable
                  accessibilityRole="button"
                  style={styles.startRow}
                  onPress={() => onOpen(groupConversationId)}
                >
                  <Avatar name="Everyone" color={colors.muted} size={32} />
                  <Text>Everyone (group)</Text>
                </Pressable>
              )}
            </View>
          )
          : null}
      />
    </View>
  )
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: colors.surface },
  header: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
    padding: 16,
    backgroundColor: colors.accent
  },
  me: { flex: 1, color: "#ffffff", fontSize: 18, fontWeight: "600" },
  logout: { color: "#ffffff" },
  row: {
    flexDirection: "row",
    gap: 12,
    padding: 14,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderColor: colors.border
  },
  presence: {
    position: "absolute",
    right: 0,
    bottom: 0,
    width: 12,
    height: 12,
    borderRadius: 6,
    backgroundColor: "#25d366",
    borderWidth: 2,
    borderColor: colors.surface
  },
  rowBody: { flex: 1, gap: 4 },
  rowTop: { flexDirection: "row", justifyContent: "space-between", alignItems: "center", gap: 8 },
  rowTitle: { fontSize: 16, fontWeight: "600" },
  rowTime: { color: colors.muted, fontSize: 12 },
  preview: { flex: 1, color: colors.muted },
  badge: {
    minWidth: 20,
    paddingHorizontal: 6,
    borderRadius: 10,
    overflow: "hidden",
    textAlign: "center",
    color: "#ffffff",
    backgroundColor: "#25d366",
    fontSize: 12
  },
  empty: { padding: 24, textAlign: "center", color: colors.muted },
  start: { padding: 16, gap: 8 },
  startTitle: { color: colors.muted, fontWeight: "600" },
  startRow: { flexDirection: "row", alignItems: "center", gap: 12, paddingVertical: 6 }
})
