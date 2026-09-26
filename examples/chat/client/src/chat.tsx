import {
  type Conversation,
  type ConversationId,
  findUser,
  type Message,
  type MessageId,
  type TickState,
  tickState,
  type UserId
} from "@effect-local/example-chat-shared/domain"
import { useAtomSet, useAtomValue } from "@effect/atom-react"
import * as Duration from "effect/Duration"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import { useEffect, useLayoutEffect, useRef, useState } from "react"
import { Avatar } from "./avatar.js"
import type { ChatClient } from "./replica.js"
import { formatTime } from "./time.js"

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

const tickLabels: Record<TickState, string> = {
  failed: "Failed",
  pending: "Sending",
  sent: "Sent",
  delivered: "Delivered",
  read: "Read"
}

const Ticks = ({ state }: { readonly state: TickState }) => (
  <span className="tick-status" role="img" aria-label={tickLabels[state]}>
    {Match.value(state).pipe(
      Match.when("failed", () => <FailedIcon />),
      Match.when("pending", () => <ClockIcon />),
      Match.when("sent", () => <Check className="tick tick-sent" />),
      Match.when("delivered", () => <CheckCheck className="tick tick-delivered" />),
      Match.when("read", () => <CheckCheck className="tick tick-read" />),
      Match.exhaustive
    )}
  </span>
)

// ---------------------------------------------------------------------------
// Message list
// ---------------------------------------------------------------------------

const dayFormatter = new Intl.DateTimeFormat([], { day: "numeric", month: "short", year: "numeric" })

const dayLabel = (millis: number): string => dayFormatter.format(millis)

const sameDay = (left: number, right: number): boolean => dayFormatter.format(left) === dayFormatter.format(right)

const senderName = (userId: UserId): string => findUser(userId)?.name ?? userId

interface Row {
  readonly message: Message
  readonly failed: boolean
}

const MessageRow = ({ row, me, state, conversation, onRetry, onDiscard }: {
  readonly row: Row
  readonly me: string
  readonly state: TickState | undefined
  readonly conversation: Conversation | undefined
  readonly onRetry: (message: Message) => void
  readonly onDiscard: (message: Message) => void
}) => {
  const outgoing = row.message.senderId === me
  const sender = findUser(row.message.senderId)
  return (
    <div className={outgoing ? "bubble-row bubble-row-out" : "bubble-row bubble-row-in"} data-message-row>
      <div className={outgoing ? "bubble bubble-out" : "bubble bubble-in"}>
        <span className="visually-hidden">{outgoing ? "You:" : `${senderName(row.message.senderId)}:`}</span>
        {!outgoing && conversation?.kind === "group" && (
          <span className="bubble-sender" style={{ color: sender?.color ?? "#667781" }} aria-hidden>
            {sender?.name ?? row.message.senderId}
          </span>
        )}
        <span className="bubble-text">{row.message.text}</span>
        <span className="bubble-meta">
          <span className="bubble-time">{formatTime(row.message.createdAt)}</span>
          {outgoing && state !== undefined && <Ticks state={state} />}
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

const bottomSlack = 48

interface ScrollAnchor {
  readonly element: Element
  readonly offset: number
}

const firstVisibleRow = (list: HTMLElement): Element | undefined => {
  const elements = list.querySelectorAll("[data-message-row]")
  const top = list.getBoundingClientRect().top
  let low = 0
  let high = elements.length
  while (low < high) {
    const middle = (low + high) >>> 1
    if (elements.item(middle).getBoundingClientRect().bottom <= top) low = middle + 1
    else high = middle
  }
  return low < elements.length ? elements.item(low) : undefined
}

const useScrollAnchor = (rows: ReadonlyArray<Row>, me: UserId) => {
  const listRef = useRef<HTMLDivElement | null>(null)
  const pinned = useRef(true)
  const anchor = useRef<ScrollAnchor | null>(null)
  const edges = useRef<{ readonly first: MessageId; readonly last: MessageId } | null>(null)

  const capture = () => {
    const list = listRef.current
    if (list === null) return
    pinned.current = list.scrollHeight - list.scrollTop - list.clientHeight <= bottomSlack
    const element = firstVisibleRow(list)
    anchor.current = element === undefined
      ? null
      : { element, offset: element.getBoundingClientRect().top - list.getBoundingClientRect().top }
  }

  useLayoutEffect(() => {
    const list = listRef.current
    if (list === null) return
    const first = rows.at(0)?.message
    const last = rows.at(-1)?.message
    const previous = edges.current
    const prepended = previous !== null && first !== undefined && last !== undefined &&
      first.id !== previous.first && last.id === previous.last
    const sentByMe = previous !== null && last !== undefined && last.id !== previous.last && last.senderId === me
    edges.current = first === undefined || last === undefined ? null : { first: first.id, last: last.id }
    const current = anchor.current
    if (!prepended && (pinned.current || sentByMe)) {
      list.scrollTop = list.scrollHeight
    } else if (current !== null && current.element.isConnected) {
      list.scrollTop += current.element.getBoundingClientRect().top - list.getBoundingClientRect().top -
        current.offset
    }
    capture()
  })

  return { listRef, onScroll: capture }
}

const typingTtl = Duration.seconds(3)
const typingRefreshInterval = Duration.toMillis(Duration.seconds(1))

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

  const edit = (value: string, at: number) => {
    setDraft(value)
    if (value.trim().length === 0) {
      stopTyping()
      return
    }
    const publishedAt = typingPublishedAt.current
    if (publishedAt !== null && at - publishedAt < typingRefreshInterval) return
    typingPublishedAt.current = at
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
    <footer className="chat-composer">
      <input
        className="chat-input"
        aria-label="Message"
        value={draft}
        placeholder="Type a message"
        onChange={(event) => edit(event.target.value, event.timeStamp)}
        onBlur={stopTyping}
        onKeyDown={(event) => {
          if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault()
            send()
          }
        }}
      />
      <button
        type="button"
        className="chat-send"
        aria-label="Send message"
        onClick={send}
        disabled={draft.trim().length === 0}
      >
        <svg viewBox="0 0 24 24" width="24" height="24" aria-hidden>
          <path
            fill="currentColor"
            d="M3.4 20.4l17.4-7.5c.8-.3.8-1.4 0-1.8L3.4 3.6c-.7-.3-1.4.3-1.4 1.1v4.4c0 .5.4.9.9 1L13.7 12 2.9 13.9c-.5.1-.9.5-.9 1v4.4c0 .8.7 1.4 1.4 1.1z"
          />
        </svg>
      </button>
    </footer>
  )
}

// ---------------------------------------------------------------------------
// Chat view
// ---------------------------------------------------------------------------

export const ChatView = ({ client, me, conversationId, onBack }: {
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
  const pending = AsyncResult.value(useAtomValue(client.pendingSendsAtom))
  const failed = useAtomValue(client.failedMessages)
  const synced = useAtomValue(client.syncedAtom)
  const discardMessage = useAtomSet(client.discardMessage)
  const loadEarlier = useAtomSet(client.loadEarlier(conversationId))
  const markRead = useAtomSet(client.markRead)
  // Mutations are invoked with promiseExit so the failure channel stays
  // observable at the call site (the failed overlay also records it).
  const retryMessage = useAtomSet(client.retryMessage, { mode: "promiseExit" })

  const summary = summaries.find((entry) => entry.conversation.id === conversationId)
  const conversation = summary?.conversation
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

  const typingHere = typing.filter((entry) => entry.key === conversationId && entry.value.userId !== me)
  const presenceSubtitle = () => {
    if (conversation === undefined) return ""
    if (conversation.kind === "group") return `${conversation.memberIds.length} members`
    if (peer === undefined || Option.isNone(presence)) return ""
    return presence.value.some((entry) => entry.value.userId === peer) ? "online" : "offline"
  }
  const subtitle = typingHere.length > 0
    ? conversation?.kind === "group"
      ? `${typingHere.map((entry) => senderName(entry.value.userId)).join(", ")} typing…`
      : "typing…"
    : presenceSubtitle()

  // Merge the durable window with the local failed overlay; the overlay holds
  // messages whose optimistic write was rolled back after a terminal failure.
  const window_ = AsyncResult.value(windowResult)
  const items = Option.match(window_, { onNone: () => [], onSome: (value) => value.items })
  const failedHere = [...failed.values()].filter((message) => message.conversationId === conversationId)
  const rows: Array<Row> = [
    ...items.map((message): Row => ({ message, failed: failed.has(message.id) })),
    ...failedHere
      .filter((message) => !items.some((item) => item.id === message.id))
      .map((message): Row => ({ message, failed: true }))
  ].toSorted((left, right) => left.message.createdAt - right.message.createdAt)

  const receiptsKnown = conversation !== undefined && Option.isSome(readStates) && Option.isSome(pending)
  const pendingIds = new Set(Option.getOrElse(pending, () => []).map((entry) => entry.payload.id))
  const receiptState = (row: Row): TickState | undefined =>
    receiptsKnown
      ? tickState({
        failed: row.failed,
        pending: pendingIds.has(row.message.id),
        message: row.message,
        senderId: row.message.senderId,
        readStates: Option.getOrElse(readStates, () => []),
        memberIds: conversation.memberIds
      })
      : undefined

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

  const [announcement, setAnnouncement] = useState("")
  const announcedIncoming = useRef<MessageId | null | undefined>(undefined)
  const summaryKnown = summary !== undefined
  useEffect(() => {
    if (!summaryKnown) return
    const previous = announcedIncoming.current
    announcedIncoming.current = lastIncoming?.id ?? null
    if (previous === undefined || lastIncoming === null || lastIncoming.id === previous) return
    setAnnouncement(`${senderName(lastIncoming.senderId)}: ${lastIncoming.text}`)
  }, [summaryKnown, lastIncoming])

  const { listRef, onScroll } = useScrollAnchor(rows, me)

  return (
    <main className="chat">
      <header className="chat-header">
        <button type="button" className="chat-back" aria-label="Back to conversations" onClick={onBack}>
          <svg viewBox="0 0 24 24" width="24" height="24" aria-hidden>
            <path fill="currentColor" d="M20 11H7.83l5.59-5.59L12 4l-8 8 8 8 1.41-1.41L7.83 13H20v-2z" />
          </svg>
        </button>
        <Avatar name={title} color={color} />
        <div className="chat-header-text">
          <span className="chat-title">{title}</span>
          <span
            className={typingHere.length > 0 ? "chat-subtitle chat-subtitle-typing" : "chat-subtitle"}
            aria-live="polite"
          >
            {subtitle}
          </span>
        </div>
      </header>
      <div className="chat-messages" ref={listRef} onScroll={onScroll}>
        {Option.isSome(window_) && window_.value.hasMore && (
          <button
            type="button"
            className="chat-load-more"
            onClick={() => loadEarlier(undefined)}
          >
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
              state={receiptState(row)}
              onRetry={(message) => void retryMessage(message)}
              onDiscard={(message) => discardMessage(message)}
            />
          </div>
        ))}
        {synced && !windowResult.waiting && Option.isSome(window_) && rows.length === 0 && (
          <p className="chat-empty">No messages yet. Say hello!</p>
        )}
        {Option.isNone(window_) && AsyncResult.isFailure(windowResult) && (
          <p className="chat-empty" role="alert">Could not load messages.</p>
        )}
      </div>
      <div className="visually-hidden" aria-live="polite">{announcement}</div>
      <Composer client={client} me={me} conversationId={conversationId} />
    </main>
  )
}
