import { type LoginRequest, LoginResponse } from "@effect-local/example-chat-shared/auth"
import {
  AdvanceDelivery,
  AdvanceRead,
  type Conversation,
  type ConversationId,
  ConversationSummaries,
  definition,
  dmConversationId,
  ephemerals,
  findUser,
  groupConversationId,
  type Message,
  MessageId,
  MessagesWindow,
  PresenceProfile,
  profiles,
  ReadStates,
  SendMessage,
  spaceId,
  StartConversation,
  Typing,
  type UserId,
  users
} from "@effect-local/example-chat-shared/domain"
import { layerDomain } from "@effect-local/example-chat-shared/handlers"
import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import * as BrowserKeyValueStore from "@effect/platform-browser/BrowserKeyValueStore"
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as BrowserSqlite from "@lucas-barake/effect-local-browser/BrowserSqlite"
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Clock from "effect/Clock"
import * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse"
import * as KeyValueStore from "effect/unstable/persistence/KeyValueStore"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import * as Atom from "effect/unstable/reactivity/Atom"
import * as Socket from "effect/unstable/socket/Socket"
import { makeFailedMessages, makeSettlementDaemonBody } from "./settlementDaemon.js"

/**
 * Browser client composition: MultiTab-owned SqlReplica over an OPFS SQLite
 * worker, one WebSocket carrying sync and ephemera, and the atom graph the
 * React UI subscribes to.
 *
 * Everything replica-related is per logged-in session (`clientFor`): the OPFS
 * database, the MultiTab client identity, the bearer token, and the atom graph
 * are all keyed by user id so switching accounts on one browser profile never
 * mixes local state.
 */

// ---------------------------------------------------------------------------
// Page runtime: services that outlive any one login (stored session, HTTP).
// ---------------------------------------------------------------------------

const pageRuntime = Atom.runtime(Layer.merge(BrowserKeyValueStore.layerLocalStorage, FetchHttpClient.layer))

const sessionKey = "effect-local-chat:session"

export const sessionAtom = Atom.kvs({
  runtime: pageRuntime,
  key: sessionKey,
  schema: Schema.NullOr(LoginResponse),
  defaultValue: () => null,
  mode: "async"
})

class LoginFailed extends Schema.TaggedError<LoginFailed>(
  "@effect-local/example-chat/LoginFailed"
)("LoginFailed", { message: Schema.String }) {}

const login = Effect.fn("chat.login")(
  function*(credentials: LoginRequest, get: Atom.FnContext) {
    const client = yield* HttpClient.HttpClient
    const request = yield* HttpClientRequest.post(`${location.origin}/login`).pipe(
      HttpClientRequest.bodyJson(credentials)
    )
    const response = yield* client.execute(request)
    if (response.status === 401) {
      return yield* new LoginFailed({ message: "Invalid username or password" })
    }
    const ok = yield* HttpClientResponse.filterStatusOk(response)
    const session = yield* HttpClientResponse.schemaBodyJson(LoginResponse)(ok)
    get.set(sessionAtom, session)
    return session
  },
  Effect.catchTags({
    HttpBodyError: () => new LoginFailed({ message: "Could not encode the login request" }),
    HttpClientError: () => new LoginFailed({ message: "Login service unavailable" }),
    SchemaError: () => new LoginFailed({ message: "Malformed login response" })
  })
)

export const loginAtom = pageRuntime.fn<LoginRequest>()(login)

// The stored session is removed before the reload so the next load lands on
// the login screen instead of resuming the signed out account.
export const logoutAtom = pageRuntime.fn<void>()(() =>
  KeyValueStore.KeyValueStore.use((store) => store.remove(sessionKey)).pipe(
    Effect.andThen(Effect.sync(() => location.reload()))
  )
)

export const followStoredSessionAtom = Atom.make(
  Stream.fromEventListener<StorageEvent>(window, "storage").pipe(
    Stream.filter((event) => event.storageArea === localStorage && (event.key === sessionKey || event.key === null)),
    Stream.runForEach(() => Effect.sync(() => location.reload()))
  )
)

// ---------------------------------------------------------------------------
// Ephemeral identity: deliberately decoupled from the replica client id.
// MultiTab owns the replica identity and never exposes it outside the owner
// callback, so presence/typing use an identity minted once per page load.
// ---------------------------------------------------------------------------

const member = Effect.runSync(
  Effect.all({
    clientId: Identity.makeClientId,
    membershipIncarnation: Identity.makeMembershipIncarnation
  }).pipe(
    Effect.map((fields) => Protocol.EphemeralMember.make(fields)),
    Effect.provide(BrowserCrypto.layer)
  )
)

// ---------------------------------------------------------------------------
// Replica stack (per session)
// ---------------------------------------------------------------------------

const syncUrl = () => {
  let scheme = "ws"
  if (location.protocol === "https:") scheme = "wss"
  return `${scheme}://${location.host}/sync`
}

const makeGraph = (session: LoginResponse) => {
  // A rejected bearer parks the space at NeedsAuthentication; the banner then
  // signs out and reloads, so the token never rotates inside one page load.
  const bearer = Redacted.make(session.token)
  const layerSync = SyncClient.layerWebSocket({ url: syncUrl() }).pipe(
    Layer.provide(Socket.layerWebSocketConstructorGlobal),
    Layer.provide(Authentication.layerCredentialProviderStatic(bearer))
  )
  const layerDatabase = BrowserSqlite.layerWorker(() =>
    new Worker(new URL("./sqlite.worker.ts", import.meta.url), { type: "module", name: session.userId })
  )
  return ReplicaAtom.make(
    BrowserReplica.layer({
      name: `chat-${session.userId}`,
      definition,
      layerDatabase,
      layerSync,
      spaces: [spaceId],
      ephemerals,
      profiles
    }).pipe(Layer.provide(layerDomain))
  )
}

// ---------------------------------------------------------------------------
// Atoms (per session)
// ---------------------------------------------------------------------------

export type ChatClient = ReturnType<typeof makeClient>

const windowPage = 50
const windowLimit = 1_000

export type Connection = "online" | "connecting" | "offline" | "needsAuthentication" | "failed"

const offlineGrace = Duration.seconds(2)
const online: Connection = "online"
const connecting: Connection = "connecting"
const offline: Connection = "offline"

const connectionOf = (
  status: AsyncResult.AsyncResult<ReplicaStatus.SpaceStatus, unknown>
): Connection => {
  if (!AsyncResult.isSuccess(status)) return connecting
  return Match.value(status.value).pipe(
    Match.tagsExhaustive({
      Online: () => online,
      SchemaUpdateAvailable: () => online,
      NeedsAuthentication: (): Connection => "needsAuthentication",
      Failed: (): Connection => "failed",
      Offline: () => connecting,
      Connecting: () => connecting
    })
  )
}

const findUserName = (userId: UserId): string => findUser(userId)?.name ?? userId

const mintMessageId = Crypto.Crypto.use((crypto) => crypto.randomUUIDv4).pipe(
  Effect.map((uuid) => MessageId.make(uuid)),
  Effect.provide(BrowserCrypto.layer)
)

const clients = new Map<UserId, ChatClient>()

export const clientFor = (session: LoginResponse): ChatClient => {
  const existing = clients.get(session.userId)
  if (existing !== undefined) return existing
  const client = makeClient(session)
  clients.set(session.userId, client)
  return client
}

const makeClient = (session: LoginResponse) => {
  const userId = session.userId
  const graph = makeGraph(session)
  const target = { spaceId, member }

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

  const connectionAtom = Atom.make(
    (get) =>
      get.stream(statusAtom).pipe(
        Stream.map(connectionOf),
        Stream.changes,
        Stream.switchMap((connection): Stream.Stream<Connection> => {
          if (connection !== connecting) return Stream.succeed(connection)
          return Stream.concat(
            Stream.succeed(connection),
            Stream.fromEffect(Effect.sleep(offlineGrace)).pipe(Stream.as(offline))
          )
        })
      ),
    { initialValue: connecting }
  )

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
        // Surface the failure to the caller AND record it in the local
        // failed-message overlay so the bubble can render and retry.
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

  // Discard reads the overlay at effect time: writing from a render-time
  // snapshot would race concurrent inserts by the settlement daemon.
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
          memberIds: [userId, input.userId].toSorted(),
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

  // Delivery daemon: whenever a conversation summary shows an incoming message
  // beyond my delivered position, advance it. Monotonic-max on the server makes
  // this converge after one write.
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
            // A single failing advance must not kill the daemon; the error is
            // deliberately discarded here after logging.
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

  // Settlement daemon: the body lives in ./settlementDaemon.ts (platform
  // neutral) so the smoke tests mount the exact production atom.
  const settlementDaemon = graph.runtime.atom(makeSettlementDaemonBody(failedMessages))

  return {
    graph,
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
