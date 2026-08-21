import {
  type ChatUser,
  type Conversation,
  type ConversationId,
  type ConversationSummary,
  dmConversationId,
  findUser,
  groupConversationId,
  UserId,
  users
} from "@effect-local/example-chat-shared/domain"
import { useAtomMount, useAtomSet, useAtomValue } from "@effect/atom-react"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import { useState } from "react"
import { ChatView } from "./chat.js"
import { clientFor, logoutAtom, type StoredSession } from "./replica.js"

const latest = <A, E,>(result: AsyncResult.AsyncResult<A, E>, fallback: A): A =>
  AsyncResult.getOrElse(result, () => fallback)

const timeFormatter = new Intl.DateTimeFormat([], { hour: "2-digit", minute: "2-digit" })

export const formatTime = (millis: number): string => timeFormatter.format(millis)

export const Avatar = ({ name, color, size = 40 }: {
  readonly name: string
  readonly color: string
  readonly size?: number
}) => (
  <span
    className="avatar"
    style={{ backgroundColor: color, width: size, height: size, fontSize: size * 0.42 }}
  >
    {name[0]}
  </span>
)

const conversationPeer = (conversation: Conversation, me: UserId): ChatUser | undefined => {
  if (conversation.kind === "group") return undefined
  const other = conversation.memberIds.find((memberId) => memberId !== me)
  return other === undefined ? undefined : findUser(other)
}

const conversationTitle = (conversation: Conversation, me: UserId): string =>
  conversation.kind === "group" ? "Everyone" : conversationPeer(conversation, me)?.name ?? "Unknown"

const conversationColor = (conversation: Conversation, me: UserId): string =>
  conversation.kind === "group" ? "#667781" : conversationPeer(conversation, me)?.color ?? "#667781"

const StatusBanner = ({ status }: {
  readonly status: AsyncResult.AsyncResult<{ readonly _tag: string }, unknown>
}) => {
  const logout = useAtomSet(logoutAtom)
  if (!AsyncResult.isSuccess(status) || status.value._tag === "Online") return null
  if (status.value._tag === "NeedsAuthentication") {
    return (
      <div className="banner banner-warning">
        Session expired.{" "}
        <button type="button" className="banner-action" onClick={() => logout(undefined)}>
          Sign in again
        </button>
      </div>
    )
  }
  if (status.value._tag === "Failed") {
    return <div className="banner banner-error">Sync failed — local data remains available.</div>
  }
  return (
    <div className="banner banner-warning">
      Offline — messages send and receipts advance when the connection returns.
    </div>
  )
}

const Sidebar = ({ me, openId, onOpen }: {
  readonly me: UserId
  readonly openId: ConversationId | null
  readonly onOpen: (conversationId: ConversationId) => void
}) => {
  const client = clientFor(me)
  const summaries = latest(useAtomValue(client.summariesAtom), [])
  const members = latest(useAtomValue(client.membersAtom), [])
  const onlineIds = new Set(members.map((entry) => entry.value.userId))

  const mine = summaries
    .filter((summary) => summary.conversation.memberIds.includes(me))
    .toSorted((left, right) =>
      (right.lastMessage?.createdAt ?? right.conversation.createdAt) -
      (left.lastMessage?.createdAt ?? left.conversation.createdAt)
    )
  const knownIds = new Set(summaries.map((summary) => summary.conversation.id))
  const newDmUsers = users.filter((user) => user.id !== me && !knownIds.has(dmConversationId(me, user.id)))

  return (
    <aside className="sidebar">
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
        {mine.length === 0 && <p className="sidebar-empty">No conversations yet. Start one below.</p>}
      </div>
      {(newDmUsers.length > 0 || !knownIds.has(groupConversationId)) && (
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
              {onlineIds.has(user.id) && <span className="presence-dot" />}
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
      onClick={() => onOpen(summary.conversation.id)}
    >
      <span className="conversation-avatar">
        <Avatar name={title} color={conversationColor(summary.conversation, me)} />
        {online && <span className="presence-dot presence-dot-inline" />}
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
          {summary.unreadCount > 0 && <span className="unread-badge">{summary.unreadCount}</span>}
        </span>
      </span>
    </button>
  )
}

export const App = ({ session }: { readonly session: StoredSession }) => {
  const client = clientFor(session.userId)
  useAtomMount(client.presenceAtom)
  useAtomMount(client.deliveryDaemon)
  useAtomMount(client.settlementDaemon)
  const status = useAtomValue(client.statusAtom)
  const logout = useAtomSet(logoutAtom)
  const startConversation = useAtomSet(client.startConversation)
  const [openId, setOpenId] = useState<ConversationId | null>(null)

  const me = session.userId
  const open = (conversationId: ConversationId) => {
    if (conversationId === groupConversationId) {
      startConversation({ kind: "group" })
    } else {
      const peer = conversationId.slice("dm:".length).split(":").find((memberId) => memberId !== me)
      if (peer !== undefined) startConversation({ kind: "dm", userId: UserId.make(peer) })
    }
    setOpenId(conversationId)
  }

  return (
    <div className="app">
      <div className="app-sidebar">
        <header className="sidebar-header">
          <Avatar name={session.name} color={session.color} />
          <span className="sidebar-me">{session.name}</span>
          <button type="button" className="sidebar-logout" onClick={() => logout(undefined)}>
            Log out
          </button>
        </header>
        <StatusBanner status={status} />
        <Sidebar me={me} openId={openId} onOpen={open} />
      </div>
      {openId === null
        ? (
          <div className="chat-placeholder">
            <p>Effect Chat — local-first, multi-tab, offline-capable.</p>
            <p>Pick a conversation to start messaging.</p>
          </div>
        )
        : <ChatView key={openId} me={me} conversationId={openId} />}
    </div>
  )
}
