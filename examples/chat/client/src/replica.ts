import type { LoginRequest, LoginResponse } from "@effect-local/example-chat-shared/auth"
import { type ChatClient as SharedChatClient, makeChatClient } from "@effect-local/example-chat-shared/client"
import { definition, ephemerals, profiles, spaceId, type UserId } from "@effect-local/example-chat-shared/domain"
import { layerDomain } from "@effect-local/example-chat-shared/handlers"
import { requestLogin, sessionKey, StoredSession } from "@effect-local/example-chat-shared/session"
import * as BrowserKeyValueStore from "@effect/platform-browser/BrowserKeyValueStore"
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as BrowserSqlite from "@lucas-barake/effect-local-browser/BrowserSqlite"
import type * as BrowserStorageError from "@lucas-barake/effect-local-browser/BrowserStorageError"
import * as Authentication from "@lucas-barake/effect-local-rpc/Authentication"
import * as ReplicaAtom from "@lucas-barake/effect-local-rpc/ReplicaAtom"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import * as Effect from "effect/Effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as Layer from "effect/Layer"
import * as KeyValueStore from "effect/persistence/KeyValueStore"
import * as Atom from "effect/reactivity/Atom"
import * as Redacted from "effect/Redacted"
import * as Socket from "effect/socket/Socket"
import * as Stream from "effect/Stream"

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

export const sessionAtom = Atom.kvs({
  runtime: pageRuntime,
  key: sessionKey,
  schema: StoredSession,
  defaultValue: () => null,
  mode: "async"
})

const login = Effect.fnUntraced(function*(credentials: LoginRequest, get: Atom.FnContext) {
  const session = yield* requestLogin(location.origin, credentials)
  get.set(sessionAtom, session)
  return session
})

export const loginAtom = pageRuntime.fn<LoginRequest>()(login)

// The stored session is removed before the reload so the next load lands on
// the login screen instead of resuming the signed out account.
export const logoutAtom = pageRuntime.fn<void>()(() =>
  KeyValueStore.KeyValueStore.use((store) => store.remove(sessionKey)).pipe(
    Effect.andThen(Effect.sync(() => location.reload()))
  )
)

export const reloadAtom = pageRuntime.fn<void>()(() => Effect.sync(() => location.reload()))

export const followStoredSessionAtom = Atom.make(
  Stream.fromEventListener<StorageEvent>(window, "storage").pipe(
    Stream.filter((event) => event.storageArea === localStorage && (event.key === sessionKey || event.key === null)),
    Stream.runForEach(() => Effect.sync(() => location.reload()))
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
  const layerDatabase = BrowserSqlite.layerWorker(() =>
    new Worker(new URL("./sqlite.worker.ts", import.meta.url), { type: "module", name: session.userId })
  )
  const layerSync = SyncClient.layerWebSocket({ url: syncUrl() })
  return ReplicaAtom.make(
    BrowserReplica.layer(Layer.merge(layerDatabase, layerSync), {
      name: `chat-${session.userId}`,
      definition,
      spaces: [spaceId],
      ephemerals,
      profiles
    }).pipe(
      Layer.provide(layerDomain),
      Layer.provide(Socket.layerWebSocketConstructorGlobal),
      Layer.provide(Authentication.layerCredentialProviderStatic(bearer)),
      Layer.provide(BrowserReplica.layerPlatformBrowser)
    )
  )
}

export type ChatClient = SharedChatClient<BrowserStorageError.BrowserStorageError>

const clients = new Map<UserId, ChatClient>()

export const clientFor = (session: LoginResponse): ChatClient => {
  const existing = clients.get(session.userId)
  if (existing !== undefined) return existing
  const client = makeChatClient(makeGraph(session), session.userId)
  clients.set(session.userId, client)
  return client
}
