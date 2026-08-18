import { LoginRequest, LoginResponse } from "@effect-local/example-chat-shared/auth"
import {
  AdvanceDelivery,
  AdvanceRead,
  Conversation,
  type ConversationId,
  ConversationReadState,
  ConversationSummaries,
  definition,
  dmConversationId,
  ephemerals,
  findUser,
  groupConversationId,
  Message,
  MessageId,
  MessagesWindow,
  PresenceProfile,
  profiles,
  ReadStates,
  SendMessage,
  spaceId,
  StartConversation,
  Typing,
  UserId,
  users
} from "@effect-local/example-chat-shared/domain"
import { layerDomain } from "@effect-local/example-chat-shared/handlers"
import * as BrowserCrypto from "@effect/platform-browser/BrowserCrypto"
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as BrowserSqlite from "@lucas-barake/effect-local-browser/BrowserSqlite"
import * as MultiTab from "@lucas-barake/effect-local-browser/MultiTab"
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as EphemeralClient from "@lucas-barake/effect-local-rpc/EphemeralClient"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as SyncRpc from "@lucas-barake/effect-local-rpc/SyncRpc"
import * as SqlReplica from "@lucas-barake/effect-local-sql/SqlReplica"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Clock from "effect/Clock"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import * as Atom from "effect/unstable/reactivity/Atom"
import * as Socket from "effect/unstable/socket/Socket"
import { makeFailedMessages, makeSettlementDaemonBody } from "./settlementDaemon.js"

/**
 * Browser client composition: MultiTab-owned SqlReplica over OPFS SQLite,
 * WebSocket sync with credential rotation, ephemeral presence and typing, and
 * the atom graph the React UI subscribes to.
 *
 * Everything replica-related is per logged-in user (`clientFor`): the OPFS
 * database, the MultiTab client identity, and the atom graph are all keyed by
 * user id so switching accounts on one browser profile never mixes local
 * state.
 */

// ---------------------------------------------------------------------------
// Session (stored login)
//
// localStorage access and the initial decode happen at module bootstrap - a
// genuine non-Effect host boundary, covered by the scoped lint override in
// the client package. Everything past login runs inside Effect.
// ---------------------------------------------------------------------------

const StoredSession = Schema.Struct({
  token: Schema.String,
  userId: UserId,
  name: Schema.String,
  color: Schema.String,
  generation: Schema.Number
})
export type StoredSession = typeof StoredSession.Type

const sessionCodec = Schema.fromJsonString(StoredSession)
const sessionStorageKey = "effect-local-chat:session"

// localStorage itself can throw in storage-restricted contexts (private
// browsing, disabled cookies); every access degrades to "no stored value"
// rather than taking down module evaluation.
const storageGet = (key: string): string | null =>
  Effect.runSync(
    Effect.try(() => localStorage.getItem(key)).pipe(
      Effect.option,
      Effect.map(Option.getOrNull)
    )
  )

const storageSet = (key: string, value: string): void => {
  // On failure the session simply does not survive a reload.
  Effect.runSync(Effect.try(() => localStorage.setItem(key, value)).pipe(Effect.option))
}

const storageRemove = (key: string): void => {
  Effect.runSync(Effect.try(() => localStorage.removeItem(key)).pipe(Effect.option))
}

const loadSession = (): StoredSession | undefined => {
  const raw = storageGet(sessionStorageKey)
  if (raw === null) return undefined
  return Effect.runSync(
    Schema.decodeUnknownEffect(sessionCodec)(raw).pipe(
      Effect.option,
      Effect.map(Option.getOrUndefined)
    )
  )
}

class LoginFailed extends Schema.TaggedErrorClass<LoginFailed>(
  "@effect-local/example-chat/LoginFailed"
)("LoginFailed", { reason: Schema.String }) {}

const loginRequestCodec = Schema.fromJsonString(LoginRequest)

export const loginAtom = Atom.fn<LoginRequest>()(
  Effect.fn("chat.login")(function*(credentials: LoginRequest, get: Atom.FnContext) {
    const body = yield* Schema.encodeUnknownEffect(loginRequestCodec)(credentials)
    const response = yield* Effect.promise(() =>
      fetch("/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body
      })
    )
    if (response.status === 401) {
      return yield* new LoginFailed({ reason: "Invalid username or password" })
    }
    if (!response.ok) {
      return yield* new LoginFailed({ reason: "Login service unavailable" })
    }
    const raw: unknown = yield* Effect.promise(() => response.json())
    const session = yield* Schema.decodeUnknownEffect(LoginResponse)(raw).pipe(
      Effect.mapError(() => new LoginFailed({ reason: "Malformed login response" }))
    )
    const current = yield* SubscriptionRef.get(credentialRef)
    const generation = current.generation + 1
    const stored: StoredSession = { ...session, generation }
    storageSet(sessionStorageKey, yield* Schema.encodeUnknownEffect(sessionCodec)(stored))
    yield* SubscriptionRef.set(credentialRef, {
      generation,
      bearer: Redacted.make(session.token)
    })
    get.set(sessionAtom, stored)
    return stored
  })
)

export const logoutAtom = Atom.fn<void>()(() =>
  Effect.sync(() => {
    storageRemove(sessionStorageKey)
    location.reload()
  })
)

export const sessionAtom = Atom.make<StoredSession | undefined>(loadSession())

const credentialRef = Effect.runSync(
  SubscriptionRef.make<Authentication.Credential>({
    generation: 0,
    bearer: Redacted.make(loadSession()?.token ?? "anonymous")
  })
)

const credentialProvider = Authentication.CredentialProvider.of({
  acquire: SubscriptionRef.get(credentialRef),
  awaitChange: (rejectedGeneration) =>
    SubscriptionRef.changes(credentialRef).pipe(
      Stream.filter((credential) => credential.generation !== rejectedGeneration),
      Stream.runHead,
      Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
    )
})

// ---------------------------------------------------------------------------
// Ephemeral identity: deliberately decoupled from the replica client id.
// MultiTab owns the replica identity and never exposes it outside the owner
// callback, so presence/typing use an app-minted identity instead.
// ---------------------------------------------------------------------------

const mintUuid = (): string =>
  Effect.runSync(Crypto.Crypto.use((crypto) => crypto.randomUUIDv4).pipe(Effect.provide(BrowserCrypto.layer)))

const ephemeralClientId = (() => {
  const key = "effect-local-chat:ephemeral-client-id"
  const stored = storageGet(key)
  if (stored !== null) return Identity.ClientId.make(stored)
  const generated = Identity.ClientId.make(`cli_${mintUuid()}`)
  storageSet(key, generated)
  return generated
})()

const member = Protocol.EphemeralMember.make({
  clientId: ephemeralClientId,
  membershipIncarnation: Identity.MembershipIncarnation.make(`inc_${mintUuid()}`)
})

// ---------------------------------------------------------------------------
// Replica stack (per user)
// ---------------------------------------------------------------------------

// The Worker handle must stay owned by the layer: on leadership loss the owner
// scope closes, and without terminate() each grant would leak a live WASM worker.
const spawnDatabaseWorker = (userId: UserId) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const worker = new Worker(new URL("./sqlite.worker.ts", import.meta.url), {
        type: "module",
        name: userId
      })
      const channel = new MessageChannel()
      worker.postMessage({ port: channel.port2 }, [channel.port2])
      return { port: channel.port1, worker }
    }),
    ({ worker }) => Effect.sync(() => worker.terminate())
  )

const makeOwner = (userId: UserId) => (context: MultiTab.OwnerContext) => {
  let wsScheme = "ws"
  if (location.protocol === "https:") wsScheme = "wss"
  const layerSocket = Socket.layerWebSocket(`${wsScheme}://${location.host}/sync`).pipe(
    Layer.provide(Socket.layerWebSocketConstructorGlobal)
  )
  const layerProtocol = SyncClient.layerProtocolSocket().pipe(
    Layer.provide(layerSocket),
    Layer.provide(SyncRpc.layerJson())
  )
  const layerAuthentication = Layer.fresh(Authentication.layerClient).pipe(
    Layer.provide(Layer.succeed(Authentication.CredentialProvider, credentialProvider))
  )
  const layerSync = Layer.merge(SyncClient.layer, EphemeralClient.layer).pipe(
    Layer.provide(layerProtocol),
    Layer.provide(layerAuthentication)
  )
  const layerDatabase = Layer.unwrap(
    spawnDatabaseWorker(userId).pipe(Effect.map(({ port }) => BrowserSqlite.layerMessagePort(port)))
  )
  return SqlReplica.layer({
    definition,
    clientId: context.clientId,
    defaultScope: Protocol.ReplicationScope.make({
      models: [Conversation.name, Message.name, ConversationReadState.name]
    }),
    initialSpaces: [spaceId],
    maximumActiveSpaces: 4,
    foregroundActiveSpaces: 2,
    retainedReceipts: 256,
    maximumReceipts: 10_000,
    retainedHistoryEntries: 256,
    maximumBootstrapEntities: 10_000,
    maximumBootstrapBytes: 64 * 1024 * 1024,
    maximumBootstrapPageBytes: 4 * 1024 * 1024,
    migration: { retryDelay: "100 millis", maximumAttempts: 8 }
  }).pipe(
    Layer.provide(layerDomain),
    Layer.provideMerge(layerDatabase),
    Layer.provide(BrowserCrypto.layer),
    Layer.provideMerge(layerSync)
  )
}

const makeGraph = (userId: UserId) =>
  BrowserReplica.make(
    MultiTab.layer({
      name: `chat-${userId}`,
      definition,
      owner: makeOwner(userId),
      ephemerals,
      profiles,
      requestPersistence: true
    })
  )

// ---------------------------------------------------------------------------
// Atoms (per user)
// ---------------------------------------------------------------------------

export type ChatClient = ReturnType<typeof makeClient>

const windowSizeAtom = Atom.make(50)
export const loadMoreAtom = Atom.writable(
  (get) => get(windowSizeAtom),
  (context) => context.set(windowSizeAtom, Math.min(context.get(windowSizeAtom) + 50, 1_000))
)

const findUserName = (userId: UserId): string => findUser(userId)?.name ?? userId

const clients = new Map<UserId, ChatClient>()

export const clientFor = (userId: UserId): ChatClient => {
  const existing = clients.get(userId)
  if (existing !== undefined) return existing
  const client = makeClient(userId)
  clients.set(userId, client)
  return client
}

const makeClient = (userId: UserId) => {
  const graph = makeGraph(userId)

  const failedMessages = makeFailedMessages()

  const presenceAtom = graph.ephemeral(PresenceProfile, {
    spaceId,
    member,
    value: { userId, name: findUserName(userId) },
    ttl: "30 seconds"
  })
  const membersAtom = graph.ephemeralMembers(presenceAtom)
  const typingEntries = graph.ephemeralState(presenceAtom, Typing)

  const summariesAtom = graph.query(spaceId, ConversationSummaries)({ userId })
  const pendingSendsAtom = graph.pendingFor(spaceId, SendMessage)
  const statusAtom = graph.status(spaceId)

  // Memoized per conversation: a fresh Atom.readable wrapper per render would
  // force a useSyncExternalStore resubscribe on every keystroke.
  const makeWindowAtom = (conversationId: ConversationId) =>
    Atom.readable((get) => get(graph.query(spaceId, MessagesWindow)({ conversationId, limit: get(windowSizeAtom) })))
  const windowAtoms = new Map<ConversationId, ReturnType<typeof makeWindowAtom>>()
  const messagesWindow = (conversationId: ConversationId) => {
    const cached = windowAtoms.get(conversationId)
    if (cached !== undefined) return cached
    const atom = makeWindowAtom(conversationId)
    windowAtoms.set(conversationId, atom)
    return atom
  }
  const readStates = (conversationId: ConversationId) => graph.query(spaceId, ReadStates)({ conversationId })

  const sendMessage = graph.runtime.fn<{ readonly conversationId: ConversationId; readonly text: string }>()(
    Effect.fn("chat.sendMessage")(function*(input, get) {
      const replica = yield* Replica.Replica
      const space = yield* replica.space(spaceId)
      const createdAt = yield* Clock.currentTimeMillis
      const id = MessageId.make(
        yield* Crypto.Crypto.use((crypto) => crypto.randomUUIDv4).pipe(Effect.provide(BrowserCrypto.layer))
      )
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

  const markRead = graph.runtime.fn<{ readonly conversationId: ConversationId; readonly upTo: number }>()(
    Effect.fn("chat.markRead")(function*(input) {
      const replica = yield* Replica.Replica
      const space = yield* replica.space(spaceId)
      return yield* space.mutate(AdvanceRead, {
        conversationId: input.conversationId,
        userId,
        upTo: input.upTo
      }).pipe(
        Effect.onError((cause) =>
          Effect.logWarning("chat: could not advance read position").pipe(
            Effect.annotateLogs({ conversationId: input.conversationId, cause: String(cause) })
          )
        )
      )
    }),
    { concurrent: true }
  )

  const publishTyping = graph.runtime.fn<{ readonly conversationId: ConversationId }>()(
    Effect.fnUntraced(function*(input) {
      const at = yield* Clock.currentTimeMillis
      yield* EphemeralClient.EphemeralClient.use((client) =>
        client.publish(Typing, {
          spaceId,
          member,
          key: input.conversationId,
          payload: { userId, at },
          ttl: "6 seconds"
        })
      ).pipe(
        Effect.onError((cause) =>
          Effect.logWarning("chat: could not publish typing state").pipe(Effect.annotateLogs({ cause: String(cause) }))
        )
      )
    }),
    { concurrent: true }
  )

  const clearTyping = graph.runtime.fn<{ readonly conversationId: ConversationId }>()(
    (input) =>
      EphemeralClient.EphemeralClient.use((client) =>
        client.remove(Typing, { spaceId, member, key: input.conversationId })
      ).pipe(
        Effect.onError((cause) =>
          Effect.logWarning("chat: could not clear typing state").pipe(Effect.annotateLogs({ cause: String(cause) }))
        )
      ),
    { concurrent: true }
  )

  // Delivery daemon: whenever a conversation summary shows an incoming message
  // beyond my delivered position, advance it. Monotonic-max on the server makes
  // this converge after one write.
  const deliveryDaemon = graph.runtime.atom(
    Effect.fnUntraced(function*(get) {
      const summaries = yield* get.result(summariesAtom)
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
    statusAtom,
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
    readStates,
    typingEntries,
    failedMessages
  }
}
