import { Avatar } from "@base-ui/react/avatar"
import { Button } from "@base-ui/react/button"
import { Input } from "@base-ui/react/input"
import { ScrollArea } from "@base-ui/react/scroll-area"
import { Select } from "@base-ui/react/select"
import { Tooltip } from "@base-ui/react/tooltip"
import { useAtomMount, useAtomSet, useAtomValue } from "@effect/atom-react"
import * as CommandDelivery from "@lucas-barake/effect-local/CommandDelivery"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Cause from "effect/Cause"
import * as Exit from "effect/Exit"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import { Check, CheckCheck, ChevronDown, Clock, Send } from "lucide-react"
import { type FormEvent, useEffect, useMemo, useRef, useState } from "react"
import * as Client from "./replica-client.ts"
import { type ChatUser, counterpartsOf, endpointFor, relayPeerId, users } from "./shared/identities.ts"

const me = Client.me
const counterparts = counterpartsOf(me.id)

interface MessageRowUi {
  readonly messageId: string
  readonly sourceDocumentId: Identity.DocumentId
  readonly author: string
  readonly body: string
  readonly sentAtMillis: number
  readonly deliveredAtMillis: number | null
  readonly readAtMillis: number | null
}

const ONLINE_WINDOW_MILLIS = 45_000

/**
 * Inbound sync blocks a document's projections until the next local command (here: the presence
 * heartbeat) rebuilds them, so a query can fail for up to one heartbeat interval; the previous
 * successful rows stay on screen meanwhile.
 */
const latest = <A, E,>(result: AsyncResult.AsyncResult<A, E>, fallback: A): A =>
  AsyncResult.getOrElse(result, () => fallback)

const timeOf = (millis: number) => new Date(millis).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })

const lastSeenText = (lastSeenAtMillis: number | undefined, now: number) => {
  if (lastSeenAtMillis === undefined) return "offline"
  if (now - lastSeenAtMillis < ONLINE_WINDOW_MILLIS) return "online"
  return `last seen ${timeOf(lastSeenAtMillis)}`
}

const UserAvatar = ({ size, user }: { readonly size: number; readonly user: ChatUser }) => (
  <Avatar.Root
    className="avatar"
    style={{ backgroundColor: user.color, width: size, height: size, fontSize: size * 0.42 }}
  >
    <Avatar.Fallback>{user.displayName[0]}</Avatar.Fallback>
  </Avatar.Root>
)

const Ticks = ({ counterpart, message }: {
  readonly counterpart: ChatUser
  readonly message: MessageRowUi
}) => {
  const delivery = useAtomValue(Client.commandDelivery(Client.commandIdOfMessage(message.messageId)))
  const custodyAccepted = delivery._tag === "Success" &&
    CommandDelivery.isRelayCustodyAccepted(
      delivery.value,
      relayPeerId,
      endpointFor(counterpart.id, me.id).principal.peerId
    )
  const [label, icon] = message.readAtMillis !== null
    ? ["Read", <CheckCheck key="read" className="tick tick-read" size={16} />]
    : message.deliveredAtMillis !== null
    ? ["Delivered", <CheckCheck key="delivered" className="tick" size={16} />]
    : custodyAccepted
    ? ["Received by server", <Check key="server" className="tick" size={16} />]
    : ["Waiting for server", <Clock key="pending" className="tick" size={14} />]
  return (
    <Tooltip.Root>
      <Tooltip.Trigger render={<span className="tick-wrap" data-status={label} />}>
        {icon}
      </Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Positioner sideOffset={6}>
          <Tooltip.Popup className="tooltip">{label}</Tooltip.Popup>
        </Tooltip.Positioner>
      </Tooltip.Portal>
    </Tooltip.Root>
  )
}

const ChatView = ({ conversationId, counterpart, presenceText }: {
  readonly conversationId: Identity.DocumentId
  readonly counterpart: ChatUser
  readonly presenceText: string
}) => {
  const result = useAtomValue(Client.messages({ conversationId }))
  const rows: ReadonlyArray<MessageRowUi> = latest(result, [])
  const runSend = useAtomSet(Client.sendMessage, { mode: "promiseExit" })
  const [draft, setDraft] = useState("")
  // Mirrors the input synchronously: a click on a submit button can reach the handler twice in one
  // dispatch (component wrapper + native form submission), and React state has not flushed yet.
  const draftRef = useRef("")
  const viewportRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const viewport = viewportRef.current
    if (viewport !== null) viewport.scrollTop = viewport.scrollHeight
  }, [rows.length])

  const submit = (event: FormEvent) => {
    event.preventDefault()
    const body = draftRef.current.trim()
    if (body.length === 0) return
    draftRef.current = ""
    setDraft("")
    void runSend({ conversationId, body }).then((exit) => {
      // eslint-disable-next-line no-console
      if (Exit.isFailure(exit)) console.error("[send]", Cause.pretty(exit.cause))
    })
  }

  return (
    <section className="chat">
      <header className="chat-header">
        <UserAvatar user={counterpart} size={38} />
        <div>
          <div className="chat-title">{counterpart.displayName}</div>
          <div className="chat-presence" data-online={presenceText === "online"}>{presenceText}</div>
        </div>
      </header>
      <ScrollArea.Root className="messages">
        <ScrollArea.Viewport className="messages-viewport" ref={viewportRef}>
          <div className="messages-list">
            {rows.map((row) => (
              <div key={row.messageId} className="bubble" data-mine={row.author === me.id}>
                <span className="bubble-body">{row.body}</span>
                <span className="bubble-meta">
                  {timeOf(row.sentAtMillis)}
                  {row.author === me.id && <Ticks message={row} counterpart={counterpart} />}
                </span>
              </div>
            ))}
          </div>
        </ScrollArea.Viewport>
        <ScrollArea.Scrollbar className="scrollbar" orientation="vertical">
          <ScrollArea.Thumb className="scrollbar-thumb" />
        </ScrollArea.Scrollbar>
      </ScrollArea.Root>
      <form className="composer" onSubmit={submit}>
        <Input
          className="composer-input"
          placeholder={`Message ${counterpart.displayName}`}
          value={draft}
          onValueChange={(value) => {
            draftRef.current = value
            setDraft(value)
          }}
        />
        <Button type="submit" className="composer-send" aria-label="Send" disabled={draft.trim().length === 0}>
          <Send size={18} />
        </Button>
      </form>
    </section>
  )
}

const relayStatusText = {
  NotConfigured: "no relay",
  Disconnected: "offline",
  Connecting: "connecting…",
  Connected: "connected"
} as const

export const App = () => {
  // The daemons live in the atom graph; mounting is the only involvement React has in them.
  useAtomMount(Client.presenceHeartbeat)
  useAtomMount(Client.deliveredReceipts)
  useAtomMount(Client.readReceipts)

  const relayStatus = useAtomValue(Client.relayConnectionStatus)
  const summariesResult = useAtomValue(Client.conversationSummaries({ me: me.id }))
  const presenceResult = useAtomValue(Client.presenceSnapshot())
  const conversationIds = latest(useAtomValue(Client.conversationIds), undefined)
  const now = latest(useAtomValue(Client.nowMillis), Date.now())
  const selectedId = useAtomValue(Client.selectedCounterpartId)
  const setSelectedId = useAtomSet(Client.selectedCounterpartId)
  const selected = useAtomValue(Client.selectedConversation)

  useEffect(() => {
    for (
      const [name, result] of [
        ["summaries", summariesResult],
        ["presence", presenceResult]
      ] as const
    ) {
      // eslint-disable-next-line no-console
      if (result._tag === "Failure") console.error(`[query:${name}]`, Cause.pretty(result.cause))
    }
  }, [summariesResult, presenceResult])

  const summaries = latest(summariesResult, [])
  const presenceRows = latest(presenceResult, [])

  const presenceOf = (counterpart: ChatUser) => {
    const conversationId = conversationIds?.get(counterpart.id)
    const row = presenceRows.find((candidate) =>
      candidate.sourceDocumentId === conversationId && candidate.userId === counterpart.id
    )
    return lastSeenText(row?.lastSeenAtMillis, now)
  }

  const rosterRows = useMemo(() => {
    const byConversation = new Map(summaries.map((summary) => [summary.conversationId, summary]))
    return counterparts
      .map((counterpart) => ({
        counterpart,
        conversationId: conversationIds?.get(counterpart.id),
        summary: conversationIds === undefined
          ? undefined
          : byConversation.get(conversationIds.get(counterpart.id)!)
      }))
      .toSorted((left, right) => (right.summary?.lastSentAtMillis ?? 0) - (left.summary?.lastSentAtMillis ?? 0))
  }, [summaries, conversationIds])

  const relayTag = relayStatus._tag === "Success" ? relayStatus.value._tag : "Connecting"

  return (
    <Tooltip.Provider>
      <div className="app">
        <aside className="sidebar">
          <header className="sidebar-header">
            <UserAvatar user={me} size={38} />
            <div className="sidebar-title">
              <div className="sidebar-name">{me.displayName}</div>
              <div className="relay-status" data-status={relayTag}>{relayStatusText[relayTag]}</div>
            </div>
            <Select.Root
              items={users.map((user) => ({ label: user.displayName, value: user.id }))}
              value={me.id}
              onValueChange={(value) => {
                const url = new URL(window.location.href)
                url.searchParams.set("user", String(value))
                window.location.assign(url)
              }}
            >
              <Select.Trigger className="user-switch" aria-label="Switch user">
                <Select.Value />
                <ChevronDown size={14} />
              </Select.Trigger>
              <Select.Portal>
                <Select.Positioner sideOffset={6}>
                  <Select.Popup className="menu">
                    {users.map((user) => (
                      <Select.Item key={user.id} className="menu-item" value={user.id}>
                        <Select.ItemText>{user.displayName}</Select.ItemText>
                      </Select.Item>
                    ))}
                  </Select.Popup>
                </Select.Positioner>
              </Select.Portal>
            </Select.Root>
          </header>
          <ScrollArea.Root className="roster">
            <ScrollArea.Viewport className="roster-viewport">
              {rosterRows.map(({ conversationId, counterpart, summary }) => {
                const presenceText = presenceOf(counterpart)
                return (
                  <button
                    key={counterpart.id}
                    type="button"
                    data-testid={`conversation-${counterpart.id}`}
                    className="roster-row"
                    data-selected={selectedId === counterpart.id}
                    onClick={() => setSelectedId(counterpart.id)}
                    disabled={conversationId === undefined}
                  >
                    <span className="roster-avatar">
                      <UserAvatar user={counterpart} size={44} />
                      <span className="presence-dot" data-online={presenceText === "online"} />
                    </span>
                    <span className="roster-copy">
                      <span className="roster-name">{counterpart.displayName}</span>
                      <span className="roster-preview">
                        {summary?.lastBody === undefined || summary.lastBody === null
                          ? "No messages yet"
                          : `${summary.lastAuthor === me.id ? "You: " : ""}${summary.lastBody}`}
                      </span>
                    </span>
                    <span className="roster-side">
                      {summary?.lastSentAtMillis != null && (
                        <span className="roster-time">{timeOf(summary.lastSentAtMillis)}</span>
                      )}
                      {summary !== undefined && summary.unreadCount > 0 && (
                        <span className="unread-badge">{summary.unreadCount}</span>
                      )}
                    </span>
                  </button>
                )
              })}
            </ScrollArea.Viewport>
            <ScrollArea.Scrollbar className="scrollbar" orientation="vertical">
              <ScrollArea.Thumb className="scrollbar-thumb" />
            </ScrollArea.Scrollbar>
          </ScrollArea.Root>
        </aside>
        {selected !== undefined
          ? (
            <ChatView
              conversationId={selected.conversationId}
              counterpart={selected.counterpart}
              presenceText={presenceOf(selected.counterpart)}
            />
          )
          : (
            <section className="chat chat-empty">
              <div>
                <h2>Effect Local Chat</h2>
                <p>Select a conversation. Everything you see is served from a local SQLite replica.</p>
              </div>
            </section>
          )}
      </div>
    </Tooltip.Provider>
  )
}
