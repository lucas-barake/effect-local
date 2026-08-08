import { BrowserCrypto } from "@effect/platform-browser"
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as OwnershipCoordinator from "@lucas-barake/effect-local-browser/OwnershipCoordinator"
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { AsyncResult, Atom } from "effect/unstable/reactivity"
import {
  Conversation,
  ConversationSummaries,
  definition,
  Heartbeat,
  ListMessages,
  MarkDelivered,
  MarkRead,
  Messages,
  PresenceSnapshot,
  SendMessage,
  UndeliveredInbound
} from "./shared/domain.ts"
import { type ChatUser, conversationIdFor, counterpartsOf, engineGeneration, userById } from "./shared/identities.ts"

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
    // The tab-side RPC error for a dead worker is information-free (the browser's worker error
    // event crosses the thread boundary without the original exception), so this report from the
    // owner is the only place the engine's actual failure reaches the page console.
    // eslint-disable-next-line no-console
    console.error("[engine]", message)
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
export const commandDelivery = ReplicaAtom.commandDeliveryFamily(runtime)
export const relayConnectionStatus = ReplicaAtom.relayConnectionStatus(runtime)

const undeliveredInbound = ReplicaAtom.queryFamily(runtime, UndeliveredInbound)

/**
 * The conversation ids resolve only once the replica actually holds each seeded conversation
 * document. Mutating before the seed restore lands would create the document with a fresh
 * genesis, and the restore would then silently shadow those writes — so this atom is the gate
 * everything mutating sits behind: the roster stays disabled and the daemons stay idle until it
 * succeeds.
 */
export const conversationIds = runtime.atom(
  Effect.gen(function*() {
    const replica = yield* Replica.Replica
    const entries = yield* Effect.forEach(counterpartsOf(me.id), (counterpart) =>
      Effect.gen(function*() {
        const conversationId = yield* Effect.promise(() => conversationIdFor(me.id, counterpart.id))
        yield* replica.get(Conversation, conversationId).pipe(
          Effect.retry({
            while: (error) => error.reason._tag === "DocumentNotFound",
            schedule: Schedule.spaced(Duration.millis(500))
          })
        )
        return [counterpart.id, conversationId] as const
      }))
    return new Map<string, Identity.DocumentId>(entries)
  })
)

/** The counterpart whose conversation is on screen. Written by the roster, read by the daemons. */
export const selectedCounterpartId = Atom.make<string | undefined>(undefined)

export const selectedConversation = Atom.make(
  (get): { readonly counterpart: ChatUser; readonly conversationId: Identity.DocumentId } | undefined => {
    const counterpartId = get(selectedCounterpartId)
    if (counterpartId === undefined) return undefined
    const conversationId = AsyncResult.getOrElse(get(conversationIds), () => undefined)?.get(counterpartId)
    return conversationId === undefined ? undefined : { counterpart: userById(counterpartId), conversationId }
  }
)

/** Re-renders presence labels as time passes without any component owning a timer. */
export const nowMillis = Atom.make(
  Stream.succeed(Date.now()).pipe(
    Stream.concat(Stream.tick(Duration.seconds(5)).pipe(Stream.map(() => Date.now())))
  )
)

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

const beatConversations = (conversationIdList: ReadonlyArray<Identity.DocumentId>) =>
  Effect.gen(function*() {
    const replica = yield* Replica.Replica
    yield* Effect.forEach(conversationIdList, (conversationId) =>
      Effect.flatMap(Identity.makeCommandId, (commandId) =>
        replica.mutate(Heartbeat, { commandId, documentId: conversationId, payload: { userId: me.id } })), {
      discard: true
    })
    // The reactive daemons cancel their previous run whenever a watched atom updates — which their
    // own commits cause. An interrupted mutate can have committed without ever reaching the relay
    // push stream, silently stranding the change on this replica, so a beat in flight always runs
    // to completion.
  }).pipe(Effect.uninterruptible)

/**
 * `runtime.atom` for the mounted daemons. The daemons are mounted fire-and-forget, so a failure
 * would otherwise be invisible: this logs the cause with its source and then lets it propagate,
 * so the atom's `AsyncResult` still reports the failure to anything that reads it.
 */
const daemonAtom = <A, E,>(
  label: string,
  create: (get: Atom.AtomContext) => Effect.Effect<A, E, Replica.Replica | Crypto.Crypto>
) =>
  runtime.atom((get) =>
    create(get).pipe(
      Effect.tapCause((cause) => Effect.logWarning(`${label} failed`, cause))
    )
  )

/**
 * Presence heartbeat: while mounted, an open app writes `presence:<me>` into every conversation on
 * an interval. Duplicate beats from several tabs collapse in the document — each user's presence
 * is one key only that user writes.
 */
export const presenceHeartbeat = daemonAtom("presence heartbeat", (get) =>
  Effect.gen(function*() {
    const ids = yield* get.result(conversationIds, { suspendOnWaiting: true })
    // Materialized once: `Effect.forever` re-runs the same beat, and a bare `ids.values()`
    // iterator would be exhausted after the first one, silently turning every later beat into a
    // no-op.
    const conversationIdList = [...ids.values()]
    yield* beatConversations(conversationIdList).pipe(
      Effect.andThen(Effect.sleep(Duration.seconds(15))),
      Effect.forever
    )
  }).pipe(
    // Nothing re-runs this atom (its one dependency never changes), so a transient beat failure
    // would end presence for the rest of the session. Restart the loop instead, on the same
    // cadence as the beat it replaces.
    Effect.tapCause((cause) => Effect.logWarning("presence heartbeat restarting", cause)),
    Effect.retry({ schedule: Schedule.spaced(Duration.seconds(15)) })
  ))

/**
 * Delivery receipts: any open tab of this user acknowledges inbound messages, whether or not their
 * conversation is on screen — the replica holds them durably, which is what two gray ticks mean.
 * Re-runs whenever the query updates; the set-if-null handler makes overlapping marks no-ops.
 */
export const deliveredReceipts = daemonAtom("delivery receipts", (get) => {
  const undelivered = get(undeliveredInbound({ me: me.id }))
  if (undelivered._tag !== "Success") return Effect.void
  return Effect.gen(function*() {
    const replica = yield* Replica.Replica
    yield* Effect.forEach(
      undelivered.value,
      (row) =>
        Effect.flatMap(Identity.makeCommandId, (commandId) =>
          replica.mutate(MarkDelivered, {
            commandId,
            documentId: row.conversationId,
            payload: { messageId: row.messageId }
          })),
      { discard: true }
    )
    // Same reason as the read receipts below: the batch must survive its own invalidations.
  }).pipe(Effect.uninterruptible)
})

/**
 * Read receipts: an inbound message is "read" once its conversation is the one on screen. The open
 * conversation and its rows both come from the graph, so selecting a conversation and receiving a
 * message while it is open re-run this without any component wiring.
 */
export const readReceipts = daemonAtom("read receipts", (get) => {
  const selected = get(selectedConversation)
  if (selected === undefined) return Effect.void
  const result = get(messages({ conversationId: selected.conversationId }))
  if (result._tag !== "Success") return Effect.void
  const unread = result.value.filter((row) => row.author !== me.id && row.readAtMillis === null)
  if (unread.length === 0) return Effect.void
  return Effect.gen(function*() {
    const replica = yield* Replica.Replica
    yield* Effect.forEach(
      unread,
      (row) =>
        Effect.flatMap(Identity.makeCommandId, (commandId) =>
          replica.mutate(MarkRead, {
            commandId,
            documentId: selected.conversationId,
            payload: { messageId: row.messageId }
          })),
      { discard: true }
    )
    // The daemon re-evaluates the moment its own commits invalidate the query it watches, which
    // cancels the previous run. An interrupted mutate can have committed without ever reaching
    // the relay push stream, silently stranding the receipt on this replica — so the batch always
    // runs to completion.
  }).pipe(Effect.uninterruptible)
})
