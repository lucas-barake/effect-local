import type { LoginResponse } from "@effect-local/example-chat-shared/auth"
import { connecting, type Connection } from "@effect-local/example-chat-shared/connection"
import {
  type ChatUser,
  type Conversation,
  type ConversationId,
  type ConversationSummary,
  dmConversationId,
  dmPeer,
  findUser,
  groupConversationId,
  type UserId,
  users
} from "@effect-local/example-chat-shared/domain"
import { formatTime } from "@effect-local/example-chat-shared/time"
import { useAtomMount, useAtomSet, useAtomValue } from "@effect/atom-react"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import * as AsyncResult from "effect/reactivity/AsyncResult"
import { useState } from "react"
import { Avatar } from "./avatar.js"
import { ChatView } from "./chat.js"
import type { ChatClient } from "./replica.js"
import { clientFor, logoutAtom, reloadAtom } from "./replica.js"

const conversationPeer = (conversation: Conversation, me: UserId): ChatUser | undefined => {
  if (conversation.kind === "group") return undefined
  const other = conversation.memberIds.find((memberId) => memberId !== me)
  return other === undefined ? undefined : findUser(other)
}

const conversationTitle = (conversation: Conversation, me: UserId): string =>
  conversation.kind === "group" ? "Everyone" : conversationPeer(conversation, me)?.name ?? "Unknown"

const conversationColor = (conversation: Conversation, me: UserId): string =>
  conversation.kind === "group" ? "#667781" : conversationPeer(conversation, me)?.color ?? "#667781"

const StatusBanner = ({ connection }: { readonly connection: Connection }) => {
  const logout = useAtomSet(logoutAtom)
  const reload = useAtomSet(reloadAtom)
  return Match.value(connection).pipe(
    Match.when("online", () => null),
    Match.when("connecting", () => null),
    Match.when("idle", () => null),
    Match.when("needsAuthentication", () => (
      <div className="banner banner-warning" role="status">
        Session expired.{" "}
        <button type="button" className="banner-action" onClick={() => logout(undefined)}>
          Sign in again
        </button>
      </div>
    )),
    Match.when(
      "failed",
      () => <div className="banner banner-error" role="status">Sync failed — local data remains available.</div>
    ),
    Match.when("superseded", () => (
      <div className="banner banner-warning" role="status">
        This app was updated in another tab.{" "}
        <button type="button" className="banner-action" onClick={() => reload(undefined)}>
          Reload
        </button>
      </div>
    )),
    Match.when("offline", () => (
      <div className="banner banner-warning" role="status">
        Offline — messages send and receipts advance when the connection returns.
      </div>
    )),
    Match.exhaustive
  )
}

const Sidebar = ({ client, me, openId, onOpen }: {
  readonly client: ChatClient
  readonly me: UserId
  readonly openId: ConversationId | null
  readonly onOpen: (conversationId: ConversationId) => void
}) => {
  const summariesResult = useAtomValue(client.summariesAtom)
  const synced = useAtomValue(client.syncedAtom)
  const members = AsyncResult.getOrElse(useAtomValue(client.membersAtom), () => [])
  const onlineIds = new Set(members.map((entry) => entry.value.userId))

  const summaries = AsyncResult.value(summariesResult)
  if (Option.isNone(summaries)) {
    return (
      <aside className="sidebar" aria-label="Conversations">
        {AsyncResult.isFailure(summariesResult) && (
          <p className="sidebar-empty" role="alert">Could not load conversations.</p>
        )}
      </aside>
    )
  }

  const mine = summaries.value
    .filter((summary) => summary.conversation.memberIds.includes(me))
    .toSorted((left, right) =>
      (right.lastMessage?.createdAt ?? right.conversation.createdAt) -
      (left.lastMessage?.createdAt ?? left.conversation.createdAt)
    )
  const knownIds = new Set(summaries.value.map((summary) => summary.conversation.id))
  const newDmUsers = users.filter((user) => user.id !== me && !knownIds.has(dmConversationId(me, user.id)))
  const settled = synced && !summariesResult.waiting

  return (
    <aside className="sidebar" aria-label="Conversations">
      <div className="sidebar-list">
        {mine.map((summary) => (
          <ConversationRow
            key={summary.conversation.id}
            summary={summary}
            me={me}
            online={summary.conversation.kind === "dm" && summary.conversation.memberIds
              .filter((memberId) => memberId !== me)
              .some((memberId) => onlineIds.has(memberId))}
            active={openId === summary.conversation.id}
            onOpen={onOpen}
          />
        ))}
        {settled && mine.length === 0 && <p className="sidebar-empty">No conversations yet. Start one below.</p>}
      </div>
      {settled && (newDmUsers.length > 0 || !knownIds.has(groupConversationId)) && (
        <div className="sidebar-new">
          <p className="sidebar-new-title">Start a chat</p>
          {newDmUsers.map((user) => (
            <button
              key={user.id}
              type="button"
              className="sidebar-new-row"
              onClick={() => onOpen(dmConversationId(me, user.id))}
            >
              <Avatar name={user.name} color={user.color} size={32} />
              <span>{user.name}</span>
              {onlineIds.has(user.id) && <span className="presence-dot" role="img" aria-label="online" />}
            </button>
          ))}
          {!knownIds.has(groupConversationId) && (
            <button
              type="button"
              className="sidebar-new-row"
              onClick={() => onOpen(groupConversationId)}
            >
              <Avatar name="Everyone" color="#667781" size={32} />
              <span>Everyone (group)</span>
            </button>
          )}
        </div>
      )}
    </aside>
  )
}

const ConversationRow = ({ summary, me, online, active, onOpen }: {
  readonly summary: ConversationSummary
  readonly me: UserId
  readonly online: boolean
  readonly active: boolean
  readonly onOpen: (conversationId: ConversationId) => void
}) => {
  const title = conversationTitle(summary.conversation, me)
  const preview = summary.lastMessage === null
    ? "No messages yet"
    : `${summary.lastMessage.senderId === me ? "You: " : ""}${summary.lastMessage.text}`
  return (
    <button
      type="button"
      className={active ? "conversation conversation-active" : "conversation"}
      aria-current={active ? "true" : undefined}
      onClick={() => onOpen(summary.conversation.id)}
    >
      <span className="conversation-avatar">
        <Avatar name={title} color={conversationColor(summary.conversation, me)} />
        {online && <span className="presence-dot presence-dot-inline" role="img" aria-label="online" />}
      </span>
      <span className="conversation-body">
        <span className="conversation-top">
          <span className="conversation-title">{title}</span>
          {summary.lastMessage !== null && (
            <span className="conversation-time">{formatTime(summary.lastMessage.createdAt)}</span>
          )}
        </span>
        <span className="conversation-bottom">
          <span className="conversation-preview">{preview}</span>
          {summary.unreadCount > 0 && (
            <span className="unread-badge" aria-label={`${summary.unreadCount} unread`}>{summary.unreadCount}</span>
          )}
        </span>
      </span>
    </button>
  )
}

export const App = ({ session }: { readonly session: LoginResponse }) => {
  const client = clientFor(session)
  useAtomMount(client.presenceAtom)
  useAtomMount(client.deliveryDaemon)
  useAtomMount(client.settlementDaemon)
  const connection = AsyncResult.getOrElse(useAtomValue(client.connectionAtom), () => connecting)
  const logout = useAtomSet(logoutAtom)
  const startConversation = useAtomSet(client.startConversation)
  const [openId, setOpenId] = useState<ConversationId | null>(null)

  const me = session.userId
  const open = (conversationId: ConversationId) => {
    if (conversationId === groupConversationId) {
      startConversation({ kind: "group" })
    } else {
      const peer = dmPeer(conversationId, me)
      if (peer !== undefined) startConversation({ kind: "dm", userId: peer })
    }
    setOpenId(conversationId)
  }

  return (
    <div className="app">
      <StatusBanner connection={connection} />
      <div className={openId === null ? "app-panes" : "app-panes app-panes-chat-open"}>
        <div className="app-sidebar">
          <header className="sidebar-header">
            <Avatar name={session.name} color={session.color} />
            <span className="sidebar-me">{session.name}</span>
            <button type="button" className="sidebar-logout" onClick={() => logout(undefined)}>
              Log out
            </button>
          </header>
          <Sidebar client={client} me={me} openId={openId} onOpen={open} />
        </div>
        {openId === null
          ? (
            <div className="chat-placeholder">
              <p>Effect Chat — local-first, multi-tab, offline-capable.</p>
              <p>Pick a conversation to start messaging.</p>
            </div>
          )
          : (
            <ChatView
              key={openId}
              client={client}
              me={me}
              conversationId={openId}
              onBack={() => setOpenId(null)}
            />
          )}
      </div>
    </div>
  )
}
