import { BrowserCrypto } from "@effect/platform-browser"
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as OwnershipCoordinator from "@lucas-barake/effect-local-browser/OwnershipCoordinator"
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { Atom } from "effect/unstable/reactivity"
import {
  ConversationSummaries,
  definition,
  Heartbeat,
  ListMessages,
  MarkDelivered,
  MarkRead,
  Messages,
  Presence,
  PresenceSnapshot,
  SendMessage,
  UndeliveredInbound
} from "./shared/domain.ts"
import { engineGeneration, userById } from "./shared/identities.ts"

const params = new URL(window.location.href).searchParams
export const me = userById(params.get("user") ?? "")

const workerName = `chat${engineGeneration}-${me.id}`

const opfsWorkerUrl = () => {
  const url = new URL("./opfs.worker.ts", import.meta.url)
  url.searchParams.set("user", me.id)
  return url
}

const OwnerInfo = Schema.Struct({
  replicaId: Schema.String,
  writerGeneration: Identity.WriterGeneration
})

declare global {
  interface Window {
    __chatOwnerError?: string
    __chatOwnerInfo?: {
      readonly ownerId: string
      readonly provider: boolean
      readonly replicaId: string
      readonly writerGeneration: number
    }
  }
}

const OwnershipLive = OwnershipCoordinator.layerTab({
  name: workerName,
  sharedWorker: () =>
    new SharedWorker(new URL("./replica.shared-worker.ts", import.meta.url), {
      // Vite otherwise prepends a static environment import before the entry can buffer `connect`.
      /* @vite-ignore */
      name: workerName,
      type: "module"
    }),
  databaseWorker: () =>
    new Worker(opfsWorkerUrl(), {
      // Vite otherwise prepends a static environment import before the entry can buffer its port.
      /* @vite-ignore */
      name: `${workerName}-opfs`,
      type: "module"
    }),
  infoSchema: OwnerInfo,
  onAttached: (attached) => {
    window.__chatOwnerInfo = {
      ownerId: attached.ownerId,
      provider: attached.provider,
      replicaId: attached.info.replicaId,
      writerGeneration: attached.info.writerGeneration
    }
  },
  onOwnerError: (message) => {
    window.__chatOwnerError = message
  }
})

const ReplicaLive = Layer.merge(
  BrowserReplica.layerWithReactivity(definition).pipe(
    Layer.provide(Layer.merge(OwnershipLive, BrowserCrypto.layer))
  ),
  BrowserCrypto.layer
)

const runtime = Atom.runtime(ReplicaLive)

export const conversationSummaries = ReplicaAtom.queryFamily(runtime, ConversationSummaries)
export const messages = ReplicaAtom.queryFamily(runtime, ListMessages)
export const presenceSnapshot = ReplicaAtom.queryFamily(runtime, PresenceSnapshot)
export const undeliveredInbound = ReplicaAtom.queryFamily(runtime, UndeliveredInbound)
export const commandDelivery = ReplicaAtom.commandDeliveryFamily(runtime)

export const relayConnectionStatus = ReplicaAtom.relayConnectionStatus(runtime)

/**
 * The message id IS the send command's uuid, so any tab of the sender can rebuild the first tick
 * (relay custody) for any message it rendered from SQL by resubscribing
 * `commandDelivery(commandIdOfMessage(id))` — including after a reload.
 */
export const commandIdOfMessage = (messageId: string) => Identity.CommandId.make(`cmd_${messageId}`)

export const sendMessage = runtime.fn<{
  readonly conversationId: Identity.DocumentId
  readonly body: string
}>()(
  ({ body, conversationId }) =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const commandId = yield* Identity.makeCommandId
      yield* replica.mutate(SendMessage, {
        commandId,
        documentId: conversationId,
        payload: { messageId: commandId.slice("cmd_".length), author: me.id, body }
      })
    }),
  { concurrent: true, reactivityKeys: [Messages.name] }
)

export const markDelivered = runtime.fn<{
  readonly conversationId: Identity.DocumentId
  readonly messageId: string
}>()(
  ({ conversationId, messageId }) =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      yield* replica.mutate(MarkDelivered, {
        commandId: yield* Identity.makeCommandId,
        documentId: conversationId,
        payload: { messageId }
      })
    }),
  { concurrent: true, reactivityKeys: [Messages.name] }
)

export const markRead = runtime.fn<{
  readonly conversationId: Identity.DocumentId
  readonly messageId: string
}>()(
  ({ conversationId, messageId }) =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      yield* replica.mutate(MarkRead, {
        commandId: yield* Identity.makeCommandId,
        documentId: conversationId,
        payload: { messageId }
      })
    }),
  { concurrent: true, reactivityKeys: [Messages.name] }
)

export const heartbeat = runtime.fn<{ readonly conversationId: Identity.DocumentId }>()(
  ({ conversationId }) =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      yield* replica.mutate(Heartbeat, {
        commandId: yield* Identity.makeCommandId,
        documentId: conversationId,
        payload: { userId: me.id }
      })
    }),
  { concurrent: true, reactivityKeys: [Presence.name] }
)
