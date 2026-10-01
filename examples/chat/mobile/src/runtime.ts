import type { LoginRequest, LoginResponse } from "@effect-local/example-chat-shared/auth"
import { makeChatClient } from "@effect-local/example-chat-shared/client"
import { layerSessionCredential, renewCredential } from "@effect-local/example-chat-shared/credential"
import { definition, spaceId, type UserId } from "@effect-local/example-chat-shared/domain"
import { layerDomain } from "@effect-local/example-chat-shared/handlers"
import { requestLogin, sessionKey, StoredSession } from "@effect-local/example-chat-shared/session"
import * as ExpoReplica from "@lucas-barake/effect-local-expo/ExpoReplica"
import * as ReactNativeSocket from "@lucas-barake/effect-local-expo/ReactNativeSocket"
import * as ReplicaAtom from "@lucas-barake/effect-local-rpc/ReplicaAtom"
import * as SyncClient from "@lucas-barake/effect-local-rpc/SyncClient"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as FetchHttpClient from "effect/http/FetchHttpClient"
import * as Layer from "effect/Layer"
import * as Atom from "effect/reactivity/Atom"
import type { SqlError } from "effect/sql/SqlError"
import { Platform } from "react-native"
import { layerSecureStore } from "./secureStore.js"

const defaultServerUrl = Platform.select({ android: "http://10.0.2.2:4100", default: "http://localhost:4100" })

const serverUrl = process.env.EXPO_PUBLIC_CHAT_SERVER_URL ?? defaultServerUrl

const syncUrl = `${serverUrl.replace(/^http/, "ws")}/sync`

const appRuntime = Atom.runtime(Layer.merge(layerSecureStore, FetchHttpClient.layer))

export const sessionAtom = Atom.kvs({
  runtime: appRuntime,
  key: sessionKey,
  schema: StoredSession,
  defaultValue: () => null,
  mode: "async"
})

export const loginAtom = appRuntime.fn<LoginRequest>()(
  Effect.fnUntraced(function*(credentials, get) {
    const session = yield* requestLogin(serverUrl, credentials)
    get.set(sessionAtom, session)
    return session
  })
)

export const logoutAtom = appRuntime.fn<void>()((_, get) => Effect.sync(() => get.set(sessionAtom, null)))

const makeGraph = (session: LoginResponse) => {
  const layerCredential = layerSessionCredential(session.token)
  return ReplicaAtom.make(
    ExpoReplica.layer({
      definition,
      database: { filename: `chat-${session.userId}.db` },
      initialSpaces: [spaceId]
    }).pipe(
      Layer.provideMerge(SyncClient.layerWebSocket({ url: syncUrl })),
      Layer.provide(ReactNativeSocket.layerWebSocketConstructor),
      Layer.provide(layerDomain),
      Layer.provideMerge(layerCredential)
    )
  )
}

const makeClient = (session: LoginResponse) => {
  const graph = makeGraph(session)
  return {
    ...makeChatClient<ReplicaError.ReplicaError | SqlError>(graph, session.userId),
    renewCredential: graph.runtime.fn<string>()(renewCredential)
  }
}

export type ChatClient = ReturnType<typeof makeClient>

const clients = new Map<UserId, ChatClient>()

export const clientFor = (session: LoginResponse): ChatClient => {
  const existing = clients.get(session.userId)
  if (existing !== undefined) return existing
  const client = makeClient(session)
  clients.set(session.userId, client)
  return client
}
