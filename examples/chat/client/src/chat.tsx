import {
  type Conversation,
  type ConversationId,
  findUser,
  type Message,
  type TickState,
  tickState,
  type UserId
} from "@effect-local/example-chat-shared/domain"
import { useAtomSet, useAtomValue } from "@effect/atom-react"
import * as Match from "effect/Match"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import { useEffect, useRef, useState } from "react"
import { Avatar, formatTime } from "./app.js"
import { type ChatClient, loadMoreAtom } from "./replica.js"

const latest = <A, E,>(result: AsyncResult.AsyncResult<A, E>, fallback: A): A =>
  AsyncResult.getOrElse(result, () => fallback)

// ---------------------------------------------------------------------------
// Tick icons (WhatsApp semantics): clock = in the outbox, one check = server
// accepted, two checks = delivered to every member, two blue = read by every
// member, triangle = terminally failed and retryable.
// ---------------------------------------------------------------------------

const Check = ({ className }: { readonly className: string }) => (
  <svg viewBox="0 0 16 15" width="16" height="15" className={className} aria-hidden>
    <path
      fill="currentColor"
      d="M10.91 3.316l-.478-.372a.365.365 0 0 0-.51.063L4.566 9.879a.32.32 0 0 1-.484.033L1.891 7.769a.366.366 0 0 0-.515.006l-.423.433a.364.364 0 0 0 .006.514l3.258 3.185c.143.14.361.125.484-.033l6.272-8.048a.365.365 0 0 0-.063-.51z"
    />
  </svg>
)

const CheckCheck = ({ className }: { readonly className: string }) => (
  <svg viewBox="0 0 16 15" width="16" height="15" className={className} aria-hidden>
    <path
      fill="currentColor"
      d="M15.01 3.316l-.478-.372a.365.365 0 0 0-.51.063L8.666 9.879a.32.32 0 0 1-.484.033l-.358-.325a.319.319 0 0 0-.484.032l-.378.483a.418.418 0 0 0 .036.541l1.32 1.266c.143.14.361.125.484-.033l6.272-8.048a.366.366 0 0 0-.064-.512zm-4.1 0l-.478-.372a.365.365 0 0 0-.51.063L4.566 9.879a.32.32 0 0 1-.484.033L1.891 7.769a.366.366 0 0 0-.515.006l-.423.433a.364.364 0 0 0 .006.514l3.258 3.185c.143.14.361.125.484-.033l6.272-8.048a.365.365 0 0 0-.063-.51z"
    />
  </svg>
)

const ClockIcon = () => (
  <svg viewBox="0 0 16 16" width="14" height="14" className="tick tick-pending" aria-hidden>
    <path
      fill="currentColor"
      d="M8 1a7 7 0 1 0 0 14A7 7 0 0 0 8 1zm0 12.6A5.6 5.6 0 1 1 8 2.4a5.6 5.6 0 0 1 0 11.2zM8.9 4H7.4v4.6l3.5 2.1.7-1.2-2.7-1.6V4z"
    />
  </svg>
)

const FailedIcon = () => (
  <svg viewBox="0 0 16 16" width="14" height="14" className="tick tick-failed" aria-hidden>
    <path
      fill="currentColor"
      d="M8 1.2 15.5 14H.5L8 1.2zm0 3.3L2.4 12.8h11.2L8 4.5zM7.3 7h1.4v3.2H7.3V7zm0 4h1.4v1.4H7.3V11z"
    />
  </svg>
)

const Ticks = ({ state }: { readonly state: TickState }) =>
  Match.value(state).pipe(
    Match.when("failed", () => <FailedIcon />),
    Match.when("pending", () => <ClockIcon />),
    Match.when("sent", () => <Check className="tick tick-sent" />),
    Match.when("delivered", () => <CheckCheck className="tick tick-delivered" />),
    Match.when("read", () => <CheckCheck className="tick tick-read" />),
    Match.exhaustive
  )

// ---------------------------------------------------------------------------
// Message list
// ---------------------------------------------------------------------------

const dayFormatter = new Intl.DateTimeFormat([], { day: "numeric", month: "short", year: "numeric" })

const dayLabel = (millis: number): string => dayFormatter.format(millis)

const sameDay = (left: number, right: number): boolean => dayFormatter.format(left) === dayFormatter.format(right)

interface Row {
  readonly message: Message
  readonly failed: boolean
}

const MessageRow = ({ row, me, state, conversation, onRetry, onDiscard }: {
  readonly row: Row
  readonly me: string
  readonly state: TickState
  readonly conversation: Conversation | undefined
  readonly onRetry: (message: Message) => void
  readonly onDiscard: (message: Message) => void
}) => {
  const outgoing = row.message.senderId === me
  const sender = findUser(row.message.senderId)
  return (
    <div className={outgoing ? "bubble-row bubble-row-out" : "bubble-row bubble-row-in"}>
      <div className={outgoing ? "bubble bubble-out" : "bubble bubble-in"}>
        {!outgoing && conversation?.kind === "group" && (
          <span className="bubble-sender" style={{ color: sender?.color ?? "#667781" }}>
            {sender?.name ?? row.message.senderId}
          </span>
        )}
        <span className="bubble-text">{row.message.text}</span>
        <span className="bubble-meta">
          <span className="bubble-time">{formatTime(row.message.createdAt)}</span>
          {outgoing && <Ticks state={state} />}
        </span>
        {row.failed && (
          <div className="bubble-failed">
            <span>Not delivered</span>
            <button type="button" className="bubble-failed-action" onClick={() => onRetry(row.message)}>
              Retry
            </button>
            <button type="button" className="bubble-failed-action" onClick={() => onDiscard(row.message)}>
              Delete
            </button>
          </div>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chat view
// ---------------------------------------------------------------------------

export const ChatView = ({ client, me, conversationId }: {
  readonly client: ChatClient
  readonly me: UserId
  readonly conversationId: ConversationId
}) => {
  const summaries = latest(useAtomValue(client.summariesAtom), [])
  const members = latest(useAtomValue(client.membersAtom), [])
  const typing = latest(useAtomValue(client.typingEntries), [])
  const readStatesAtom = client.readStates(conversationId)
  const windowAtom = client.messagesWindow(conversationId)
  const readStates = latest(useAtomValue(readStatesAtom), [])
  const window_ = latest(useAtomValue(windowAtom), { items: [], hasMore: false })
  const pending = latest(useAtomValue(client.pendingSendsAtom), [])
  const failed = useAtomValue(client.failedMessages)
  const discardMessage = useAtomSet(client.discardMessage)
  const loadMore = useAtomSet(loadMoreAtom)
  const markRead = useAtomSet(client.markRead)
  // Mutations are invoked with promiseExit so the failure channel stays
  // observable at the call site (the failed overlay also records it).
  const retryMessage = useAtomSet(client.retryMessage, { mode: "promiseExit" })
  const publishTyping = useAtomSet(client.publishTyping)
  const clearTyping = useAtomSet(client.clearTyping)
  const sendMessage = useAtomSet(client.sendMessage, { mode: "promiseExit" })

  const summary = summaries.find((entry) => entry.conversation.id === conversationId)
  const conversation = summary?.conversation
  const memberIds = conversation?.memberIds ?? []
  const peer = conversation === undefined || conversation.kind === "group"
    ? undefined
    : conversation.memberIds.find((memberId) => memberId !== me)
  const title = conversation === undefined
    ? "…"
    : conversation.kind === "group"
    ? "Everyone"
    : (peer !== undefined ? findUser(peer)?.name : undefined) ?? "Unknown"
  const color = conversation === undefined || conversation.kind === "group"
    ? "#667781"
    : (peer !== undefined ? findUser(peer)?.color : undefined) ?? "#667781"

  const onlineIds = new Set(members.map((entry) => entry.value.userId))
  const typingHere = typing.filter((entry) => entry.key === conversationId && entry.value.userId !== me)
  const subtitle = typingHere.length > 0
    ? conversation?.kind === "group"
      ? `${typingHere.map((entry) => findUser(entry.value.userId)?.name ?? entry.value.userId).join(", ")} typing…`
      : "typing…"
    : conversation?.kind === "group"
    ? `${memberIds.length} members`
    : peer !== undefined && onlineIds.has(peer)
    ? "online"
    : "offline"

  // Merge the durable window with the local failed overlay; the overlay holds
  // messages whose optimistic write was rolled back after a terminal failure.
  const pendingIds = new Set(pending.map((entry) => entry.payload.id))
  const failedHere = [...failed.values()].filter((message) => message.conversationId === conversationId)
  const rows: Array<Row> = [
    ...window_.items.map((message): Row => ({ message, failed: failed.has(message.id) })),
    ...failedHere
      .filter((message) => !window_.items.some((item) => item.id === message.id))
      .map((message): Row => ({ message, failed: true }))
  ].toSorted((left, right) => left.message.createdAt - right.message.createdAt)

  const lastIncoming = summary?.lastIncomingMessage ?? null
  const myReadUpTo = summary?.myReadUpTo ?? 0

  // Read daemon: advance my read position whenever the conversation is open,
  // the tab is visible, and something incoming arrived beyond it.
  useEffect(() => {
    const advance = () => {
      if (
        document.visibilityState === "visible" && lastIncoming !== null && lastIncoming.createdAt > myReadUpTo
      ) {
        markRead({ conversationId, userId: me, upTo: lastIncoming.createdAt })
      }
    }
    advance()
    document.addEventListener("visibilitychange", advance)
    return () => document.removeEventListener("visibilitychange", advance)
  }, [conversationId, lastIncoming, myReadUpTo, markRead])

  const listRef = useRef<HTMLDivElement | null>(null)
  const rowCount = rows.length
  useEffect(() => {
    const list = listRef.current
    if (list !== null) list.scrollTop = list.scrollHeight
  }, [conversationId, rowCount])

  const discard = (message: Message) => discardMessage(message)

  const [draft, setDraft] = useState("")
  const send = () => {
    const text = draft.trim()
    if (text.length === 0) return
    setDraft("")
    clearTyping({ key: conversationId })
    void sendMessage({ conversationId, text })
  }

  return (
    <main className="chat">
      <header className="chat-header">
        <Avatar name={title} color={color} />
        <div className="chat-header-text">
          <span className="chat-title">{title}</span>
          <span className={typingHere.length > 0 ? "chat-subtitle chat-subtitle-typing" : "chat-subtitle"}>
            {subtitle}
          </span>
        </div>
      </header>
      <div className="chat-messages" ref={listRef}>
        {window_.hasMore && (
          <button type="button" className="chat-load-more" onClick={() => loadMore(undefined)}>
            Load earlier messages
          </button>
        )}
        {rows.map((row, index) => (
          <div key={row.message.id}>
            {(index === 0 || !sameDay(rows[index - 1].message.createdAt, row.message.createdAt)) && (
              <div className="day-separator">
                <span>{dayLabel(row.message.createdAt)}</span>
              </div>
            )}
            <MessageRow
              row={row}
              me={me}
              conversation={conversation}
              state={tickState({
                failed: row.failed,
                pending: pendingIds.has(row.message.id),
                message: row.message,
                senderId: row.message.senderId,
                readStates,
                memberIds
              })}
              onRetry={(message) => void retryMessage(message)}
              onDiscard={discard}
            />
          </div>
        ))}
        {rows.length === 0 && <p className="chat-empty">No messages yet. Say hello!</p>}
      </div>
      <footer className="chat-composer">
        <input
          className="chat-input"
          value={draft}
          placeholder="Type a message"
          onChange={(event) => {
            setDraft(event.target.value)
            if (event.target.value.trim().length > 0) {
              publishTyping({ key: conversationId, payload: { userId: me }, ttl: "6 seconds" })
            }
          }}
          onBlur={() => clearTyping({ key: conversationId })}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault()
              send()
            }
          }}
        />
        <button type="button" className="chat-send" onClick={send} disabled={draft.trim().length === 0}>
          <svg viewBox="0 0 24 24" width="24" height="24" aria-hidden>
            <path
              fill="currentColor"
              d="M3.4 20.4l17.4-7.5c.8-.3.8-1.4 0-1.8L3.4 3.6c-.7-.3-1.4.3-1.4 1.1v4.4c0 .5.4.9.9 1L13.7 12 2.9 13.9c-.5.1-.9.5-.9 1v4.4c0 .8.7 1.4 1.4 1.1z"
            />
          </svg>
        </button>
      </footer>
    </main>
  )
}
