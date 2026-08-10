import { BrowserCrypto } from "@effect/platform-browser"
import * as BrowserReplica from "@lucas-barake/effect-local-browser/BrowserReplica"
import * as OwnershipCoordinator from "@lucas-barake/effect-local-browser/OwnershipCoordinator"
import * as ReplicaAtom from "@lucas-barake/effect-local-browser/ReplicaAtom"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as Transient from "@lucas-barake/effect-local/Transient"
import * as Clock from "effect/Clock"
import type * as Crypto from "effect/Crypto"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Atom } from "effect/unstable/reactivity"
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult"
import {
  Activity,
  Conversation,
  ConversationSummaries,
  definition,
  ListMessages,
  MarkDelivered,
  MarkRead,
  Messages,
  Present,
  SendMessage,
  Typing,
  UndeliveredInbound
} from "./shared/domain.ts"
import {
  type ChatUser,
  conversationIdFor,
  counterpartsOf,
  endpointFor,
  engineGeneration,
  userById
} from "./shared/identities.ts"

const params = new URL(window.location.href).searchParams
export const me = userById(params.get("user") ?? "")

const workerName = `chat${engineGeneration}-${me.id}`

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
    new Worker(new URL("./opfs.worker.ts", import.meta.url), {
      // Vite otherwise prepends a static environment import before the entry can buffer its port.
      /* @vite-ignore */
      name: me.id,
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

const conversationSummaries = ReplicaAtom.queryFamily(runtime, ConversationSummaries)
export const messages = ReplicaAtom.queryFamily(runtime, ListMessages)
export const commandDelivery = ReplicaAtom.commandDeliveryFamily(runtime)
export const relayConnectionStatus = ReplicaAtom.relayConnectionStatus(runtime)

const undeliveredInbound = ReplicaAtom.queryFamily(runtime, UndeliveredInbound)

const conversationIds = runtime.atom(
  Effect.forEach(
    counterpartsOf(me.id),
    (counterpart) =>
      Effect.promise(() => conversationIdFor(me.id, counterpart.id)).pipe(
        Effect.map((conversationId) => [counterpart.id, conversationId] as const)
      )
  ).pipe(Effect.map((entries) => new Map<string, Identity.DocumentId>(entries)))
)

const conversationDocument = ReplicaAtom.documentFamily(runtime, Conversation)

const conversations = Atom.readable((get) => {
  const ids = get(conversationIds)
  if (ids._tag !== "Success") {
    return new Map<string, { readonly conversationId: Identity.DocumentId; readonly ready: boolean }>()
  }
  const next = new Map(
    [...ids.value].map(([counterpartId, conversationId]) => [
      counterpartId,
      { conversationId, ready: get(conversationDocument(conversationId))._tag === "Success" }
    ])
  )
  const previous = get.self<typeof next>()
  if (
    Option.isSome(previous) &&
    previous.value.size === next.size &&
    [...next].every(([counterpartId, conversation]) => {
      const prior = previous.value.get(counterpartId)
      return prior?.conversationId === conversation.conversationId && prior.ready === conversation.ready
    })
  ) return previous.value
  return next
})

/**
 * The endpoint identity each counterpart presents on the channel it shares with me. A transient
 * message names its sender with the peer id the relay authenticated, so this is what turns an
 * inbound activity message into a roster row — the payload itself never claims a sender.
 */
const peerIdOf = (counterpartId: string) => endpointFor(counterpartId, me.id).principal.peerId

const counterpartByPeerId = new Map(
  counterpartsOf(me.id).map((counterpart) => [peerIdOf(counterpart.id), counterpart.id])
)

interface ActivityTarget {
  readonly conversationId: Identity.DocumentId
  readonly peerId: Identity.PeerId
}

const activityTargets = (
  conversationMap: ReadonlyMap<string, { readonly conversationId: Identity.DocumentId; readonly ready: boolean }>
): ReadonlyArray<ActivityTarget> =>
  [...conversationMap].flatMap(([counterpartId, conversation]) =>
    conversation.ready ? [{ conversationId: conversation.conversationId, peerId: peerIdOf(counterpartId) }] : []
  )

/** The counterpart whose conversation is on screen. Written by the roster, read by the daemons. */
export const selectedCounterpartId = Atom.make<string | undefined>(undefined)

export const selectedConversation = Atom.readable(
  (get): { readonly counterpart: ChatUser; readonly conversationId: Identity.DocumentId } | undefined => {
    const counterpartId = get(selectedCounterpartId)
    if (counterpartId === undefined) return undefined
    const conversation = get(conversations).get(counterpartId)
    return conversation?.ready !== true
      ? undefined
      : { counterpart: userById(counterpartId), conversationId: conversation.conversationId }
  }
)

/** Re-renders presence labels as time passes without any component owning a timer. */
const nowMillis = Atom.make(
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

export const draft = Atom.family((_conversationId: Identity.DocumentId) => Atom.make(""))

const clearDraft = Atom.family((conversationId: Identity.DocumentId) =>
  Atom.writable(
    () => undefined,
    (context, submittedDraft: string) => {
      const conversationDraft = draft(conversationId)
      if (context.get(conversationDraft) === submittedDraft) context.set(conversationDraft, "")
    }
  )
)

export const sendMessage = Atom.family((conversationId: Identity.DocumentId) =>
  runtime.fn<string>()(
    (submittedDraft, context) =>
      Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const commandId = yield* Identity.makeCommandId
        yield* replica.mutate(SendMessage, {
          commandId,
          documentId: conversationId,
          payload: { messageId: commandId.slice("cmd_".length), author: me.id, body: submittedDraft.trim() }
        })
        context.set(clearDraft(conversationId), submittedDraft)
      }),
    { concurrent: true, reactivityKeys: [Messages.name] }
  )
)

const latest = <A, E,>(result: AsyncResult.AsyncResult<A, E>, fallback: A): A =>
  AsyncResult.getOrElse(result, () => fallback)

const PRESENCE_BEAT_INTERVAL = Duration.seconds(5)
const TYPING_BEAT_INTERVAL = Duration.seconds(2)
/** Two missed beats. Long enough to ride out a reconnect, short enough that a closed tab goes dark. */
const ONLINE_WINDOW_MILLIS = 12_000
const TYPING_WINDOW_MILLIS = 5_000

interface Awareness {
  readonly presentAtMillis: number
  readonly typingAtMillis: number
}

const noAwareness: ReadonlyMap<string, Awareness> = new Map()

/**
 * What the counterparts are doing right now, accumulated from the transient activity channel.
 *
 * Nothing here is durable, and that is the point: a transient message has no outbox, no custody, no
 * receipt, and no replay, so it never enters the document, its Automerge history, or the relay's
 * store-and-forward inboxes. The cost is that the channel says nothing about an absent peer, so
 * expiry is this replica's own job — an entry is only believed for as long as beats keep arriving.
 */
export const awareness = runtime.atom((get) => {
  const targets = activityTargets(get(conversations))
  if (targets.length === 0) return Stream.empty
  return Activity.client.pipe(
    Effect.map((forConversation) =>
      Stream.mergeAll(
        targets.map((target) => forConversation(target.conversationId).messages),
        { concurrency: "unbounded" }
      )
    ),
    Stream.unwrap,
    Stream.mapEffect((message) =>
      Effect.map(
        Clock.currentTimeMillis,
        (atMillis) => ({ peerId: message.peerId, payload: message.payload, atMillis })
      )
    ),
    Stream.scan(noAwareness, (state, { atMillis, payload, peerId }) => {
      const counterpartId = counterpartByPeerId.get(peerId)
      if (counterpartId === undefined) return state
      const previous = state.get(counterpartId)
      return new Map(state).set(counterpartId, {
        presentAtMillis: atMillis,
        typingAtMillis: payload._tag === "Typing" ? atMillis : previous?.typingAtMillis ?? 0
      })
    }),
    Stream.tapCause((cause) => Effect.logWarning("activity channel failed", cause))
  )
}, { initialValue: noAwareness })

export const roster = Atom.readable((get) => {
  const summaries = latest(get(conversationSummaries({ me: me.id })), [])
  const activity = latest(get(awareness), noAwareness)
  const conversationMap = get(conversations)
  const now = latest(get(nowMillis), Date.now())
  const byConversation = new Map(summaries.map((summary) => [summary.conversationId, summary]))

  return counterpartsOf(me.id)
    .map((counterpart) => {
      const conversation = conversationMap.get(counterpart.id)
      const seen = activity.get(counterpart.id)
      return {
        counterpart,
        conversationId: conversation?.conversationId,
        ready: conversation?.ready === true,
        online: seen !== undefined && now - seen.presentAtMillis < ONLINE_WINDOW_MILLIS,
        typing: seen !== undefined && now - seen.typingAtMillis < TYPING_WINDOW_MILLIS,
        summary: conversation === undefined ? undefined : byConversation.get(conversation.conversationId)
      }
    })
    .toSorted((left, right) => (right.summary?.lastSentAtMillis ?? 0) - (left.summary?.lastSentAtMillis ?? 0))
})

/**
 * A counterpart that is not connected has no live relay route, and a burst of beats can outrun the
 * relay's per-session token bucket. Neither is a fault of this replica: awareness that cannot be
 * delivered is awareness the counterpart is right to not have.
 */
const publishQuietly = (publish: Effect.Effect<void, ReplicaError.ReplicaError>) =>
  publish.pipe(
    Effect.catchReason("ReplicaError", "StorageUnavailable", () => Effect.logDebug("no live route for the beat")),
    Effect.catchReason("ReplicaError", "QuotaExceeded", () => Effect.logDebug("relay paced the beat"))
  )

/**
 * `runtime.atom` for the mounted daemons. The daemons are mounted fire-and-forget, so a failure
 * would otherwise be invisible: this logs the cause with its source and then lets it propagate,
 * so the atom's `AsyncResult` still reports the failure to anything that reads it.
 */
const daemonAtom = <A, E,>(
  label: string,
  create: (get: Atom.AtomContext) => Effect.Effect<A, E, Replica.Replica | Crypto.Crypto | Transient.Transient>
) =>
  runtime.atom((get) =>
    create(get).pipe(
      Effect.tapCause((cause) => Effect.logWarning(`${label} failed`, cause))
    )
  )

/**
 * Presence heartbeat: while mounted, an open app announces itself to every counterpart on an
 * interval. Several tabs of one user beat independently and the counterpart cannot tell, because a
 * beat carries no state to conflict over.
 */
export const presenceHeartbeat = daemonAtom("presence heartbeat", (get) =>
  Effect.gen(function*() {
    const targets = activityTargets(get(conversations))
    if (targets.length === 0) return
    const forConversation = yield* Activity.client
    yield* Effect.forEach(
      targets,
      (target) => publishQuietly(forConversation(target.conversationId).publish(target.peerId, new Present())),
      { concurrency: "unbounded", discard: true }
    ).pipe(
      Effect.andThen(Effect.sleep(PRESENCE_BEAT_INTERVAL)),
      Effect.forever
    )
  }).pipe(
    // Readiness changes can restart this atom while the profile is being provisioned. Once ready,
    // the readiness map keeps its identity and the loop keeps its cadence — and unlike the durable
    // heartbeat this replaced, a beat commits nothing, so the loop can no longer restart itself.
    Effect.tapCause((cause) => Effect.logWarning("presence heartbeat restarting", cause)),
    Effect.retry({ schedule: Schedule.spaced(PRESENCE_BEAT_INTERVAL) })
  ))

/**
 * Typing indicator: beats while the open conversation has an unsent draft.
 *
 * The draft is read as a stream rather than through `get`, so a keystroke does not rebuild this
 * atom and the token bucket survives the whole typing burst. `enforce` drops what does not fit
 * instead of queueing it, which is also what makes the indicator end: the last keystroke before a
 * pause is usually dropped, and the counterpart's window simply expires.
 */
export const typingBeacon = runtime.atom((get) => {
  const selected = get(selectedConversation)
  if (selected === undefined) return Stream.empty
  return Activity.client.pipe(
    Effect.map((forConversation) =>
      Atom.toStream(draft(selected.conversationId)).pipe(
        Stream.filter((text) => text.trim().length > 0),
        Stream.throttle({ cost: () => 1, units: 1, duration: TYPING_BEAT_INTERVAL, strategy: "enforce" }),
        Stream.mapEffect(() =>
          publishQuietly(
            forConversation(selected.conversationId).publish(peerIdOf(selected.counterpart.id), new Typing())
          )
        )
      )
    ),
    Stream.unwrap,
    Stream.tapCause((cause) => Effect.logWarning("typing beacon failed", cause))
  )
})

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
