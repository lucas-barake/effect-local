import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import { absurd } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as SynchronizedRef from "effect/SynchronizedRef"

export interface State {
  readonly online: boolean
  readonly dropNextReceipt: boolean
  readonly duplicateNextPage: boolean
}

export type Event =
  | {
    readonly _tag: "ReceiptCommitted"
    readonly spaceId: Identity.SpaceId
    readonly receipt: Protocol.Receipt
  }
  | {
    readonly _tag: "ReceiptDropped"
    readonly spaceId: Identity.SpaceId
    readonly receipt: Protocol.Receipt
  }
  | {
    readonly _tag: "ReceiptReturned"
    readonly spaceId: Identity.SpaceId
    readonly receipt: Protocol.Receipt
  }
  | {
    readonly _tag: "PullCompletedAfterReceipt"
    readonly spaceId: Identity.SpaceId
  }
  | {
    readonly _tag: "RequestRejectedOffline"
    readonly spaceId: Identity.SpaceId
  }

type ReceiptCommitted = Extract<Event, { readonly _tag: "ReceiptCommitted" }>
type ReceiptDropped = Extract<Event, { readonly _tag: "ReceiptDropped" }>
type ReceiptReturned = Extract<Event, { readonly _tag: "ReceiptReturned" }>
type PullCompletedAfterReceipt = Extract<Event, { readonly _tag: "PullCompletedAfterReceipt" }>
type RequestRejectedOffline = Extract<Event, { readonly _tag: "RequestRejectedOffline" }>

interface FaultState extends State {
  readonly partitionAfterNextReceipt: boolean
  readonly withholdPullEvidence: boolean
  readonly postReceiptPullPending: boolean
}

interface EventQueues {
  readonly receiptCommitted: Queue.Queue<ReceiptCommitted>
  readonly receiptDropped: Queue.Queue<ReceiptDropped>
  readonly receiptReturned: Queue.Queue<ReceiptReturned>
  readonly pullCompletedAfterReceipt: Queue.Queue<PullCompletedAfterReceipt>
  readonly requestRejectedOffline: Queue.Queue<RequestRejectedOffline>
}

export interface Service {
  readonly state: (spaceId: Identity.SpaceId) => Effect.Effect<State>
  readonly partition: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly heal: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly dropNextReceipt: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly duplicateNextPage: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly partitionAfterNextReceipt: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly withholdPullEvidence: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly releasePullEvidence: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly holdNextReceipt: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly releaseHeldReceipt: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly takeDroppedReceipt: (spaceId: Identity.SpaceId) => Effect.Effect<boolean>
  readonly takeDuplicatePage: (spaceId: Identity.SpaceId) => Effect.Effect<boolean>
  readonly takePartitionAfterReceipt: (spaceId: Identity.SpaceId) => Effect.Effect<boolean>
  readonly shouldWithholdPullEvidence: (spaceId: Identity.SpaceId) => Effect.Effect<boolean>
  readonly awaitReceiptRelease: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly markReceiptReturned: (spaceId: Identity.SpaceId) => Effect.Effect<void>
  readonly takePostReceiptPull: (spaceId: Identity.SpaceId) => Effect.Effect<boolean>
  readonly emit: (event: Event) => Effect.Effect<void>
  readonly awaitReceiptCommitted: (spaceId: Identity.SpaceId) => Effect.Effect<ReceiptCommitted>
  readonly awaitReceiptDropped: (spaceId: Identity.SpaceId) => Effect.Effect<ReceiptDropped>
  readonly awaitReceiptReturned: (spaceId: Identity.SpaceId) => Effect.Effect<ReceiptReturned>
  readonly awaitPullCompletedAfterReceipt: (spaceId: Identity.SpaceId) => Effect.Effect<PullCompletedAfterReceipt>
  readonly awaitRequestRejectedOffline: (spaceId: Identity.SpaceId) => Effect.Effect<RequestRejectedOffline>
}

export class FaultInjection extends Context.Service<FaultInjection, Service>()(
  "@lucas-barake/effect-local-test/FaultInjection"
) {}

export const layer: Layer.Layer<FaultInjection> = Layer.effect(
  FaultInjection,
  Effect.gen(function*() {
    const initial = (): FaultState => ({
      online: true,
      dropNextReceipt: false,
      duplicateNextPage: false,
      partitionAfterNextReceipt: false,
      withholdPullEvidence: false,
      postReceiptPullPending: false
    })
    const state = yield* Ref.make(new Map<Identity.SpaceId, FaultState>())
    const receiptGates = yield* Ref.make(new Map<Identity.SpaceId, Deferred.Deferred<void>>())
    const eventQueues = yield* SynchronizedRef.make(new Map<Identity.SpaceId, EventQueues>())
    const queuesFor = (spaceId: Identity.SpaceId) =>
      SynchronizedRef.modifyEffect(
        eventQueues,
        Effect.fnUntraced(function*(spaces) {
          const existing = spaces.get(spaceId)
          if (existing !== undefined) return [existing, spaces] as const
          const queues: EventQueues = {
            receiptCommitted: yield* Queue.unbounded<ReceiptCommitted>(),
            receiptDropped: yield* Queue.unbounded<ReceiptDropped>(),
            receiptReturned: yield* Queue.unbounded<ReceiptReturned>(),
            pullCompletedAfterReceipt: yield* Queue.unbounded<PullCompletedAfterReceipt>(),
            requestRejectedOffline: yield* Queue.unbounded<RequestRejectedOffline>()
          }
          const next = new Map(spaces)
          next.set(spaceId, queues)
          return [queues, next] as const
        })
      )
    yield* Effect.addFinalizer(() =>
      SynchronizedRef.get(eventQueues).pipe(
        Effect.flatMap((spaces) =>
          Effect.forEach(spaces.values(), (queues) =>
            Effect.all([
              Queue.shutdown(queues.receiptCommitted),
              Queue.shutdown(queues.receiptDropped),
              Queue.shutdown(queues.receiptReturned),
              Queue.shutdown(queues.pullCompletedAfterReceipt),
              Queue.shutdown(queues.requestRejectedOffline)
            ], { discard: true }))
        ),
        Effect.asVoid
      )
    )
    const get = (spaceId: Identity.SpaceId) =>
      Ref.get(state).pipe(
        Effect.map((spaces) => {
          const { online, dropNextReceipt, duplicateNextPage } = spaces.get(spaceId) ?? initial()
          return { online, dropNextReceipt, duplicateNextPage }
        })
      )
    const set = (spaceId: Identity.SpaceId, patch: Partial<FaultState>) =>
      Ref.update(state, (spaces) => {
        const next = new Map(spaces)
        next.set(spaceId, { ...(spaces.get(spaceId) ?? initial()), ...patch })
        return next
      })
    const take = (
      spaceId: Identity.SpaceId,
      key: "dropNextReceipt" | "duplicateNextPage" | "partitionAfterNextReceipt" | "postReceiptPullPending"
    ) =>
      Ref.modify(state, (spaces) => {
        const current = spaces.get(spaceId) ?? initial()
        const next = new Map(spaces)
        next.set(spaceId, { ...current, [key]: false })
        return [current[key], next]
      })
    return FaultInjection.of({
      state: get,
      partition: (spaceId) => set(spaceId, { online: false }),
      heal: (spaceId) => set(spaceId, { online: true }),
      dropNextReceipt: (spaceId) => set(spaceId, { dropNextReceipt: true }),
      duplicateNextPage: (spaceId) => set(spaceId, { duplicateNextPage: true }),
      partitionAfterNextReceipt: (spaceId) => set(spaceId, { partitionAfterNextReceipt: true }),
      withholdPullEvidence: (spaceId) => set(spaceId, { withholdPullEvidence: true }),
      releasePullEvidence: (spaceId) => set(spaceId, { withholdPullEvidence: false }),
      holdNextReceipt: Effect.fnUntraced(function*(spaceId) {
        const gate = yield* Deferred.make<void>()
        yield* Ref.update(receiptGates, (gates) => {
          const next = new Map(gates)
          next.set(spaceId, gate)
          return next
        })
      }),
      releaseHeldReceipt: (spaceId) =>
        Ref.get(receiptGates).pipe(
          Effect.flatMap((gates) => {
            const gate = gates.get(spaceId)
            if (gate === undefined) return Effect.void
            return Deferred.succeed(gate, undefined).pipe(Effect.asVoid)
          })
        ),
      takeDroppedReceipt: (spaceId) => take(spaceId, "dropNextReceipt"),
      takeDuplicatePage: (spaceId) => take(spaceId, "duplicateNextPage"),
      takePartitionAfterReceipt: (spaceId) => take(spaceId, "partitionAfterNextReceipt"),
      shouldWithholdPullEvidence: (spaceId) =>
        Ref.get(state).pipe(
          Effect.map((spaces) => (spaces.get(spaceId) ?? initial()).withholdPullEvidence)
        ),
      awaitReceiptRelease: (spaceId) =>
        Ref.get(receiptGates).pipe(
          Effect.flatMap((gates) => {
            const gate = gates.get(spaceId)
            if (gate === undefined) return Effect.void
            return Deferred.await(gate).pipe(
              Effect.andThen(Ref.update(receiptGates, (current) => {
                if (current.get(spaceId) !== gate) return current
                const next = new Map(current)
                next.delete(spaceId)
                return next
              }))
            )
          })
        ),
      markReceiptReturned: (spaceId) => set(spaceId, { postReceiptPullPending: true }),
      takePostReceiptPull: (spaceId) => take(spaceId, "postReceiptPullPending"),
      emit: (event) => {
        switch (event._tag) {
          case "ReceiptCommitted":
            return queuesFor(event.spaceId).pipe(
              Effect.flatMap((queues) => Queue.offer(queues.receiptCommitted, event)),
              Effect.asVoid
            )
          case "ReceiptDropped":
            return queuesFor(event.spaceId).pipe(
              Effect.flatMap((queues) => Queue.offer(queues.receiptDropped, event)),
              Effect.asVoid
            )
          case "ReceiptReturned":
            return queuesFor(event.spaceId).pipe(
              Effect.flatMap((queues) => Queue.offer(queues.receiptReturned, event)),
              Effect.asVoid
            )
          case "PullCompletedAfterReceipt":
            return queuesFor(event.spaceId).pipe(
              Effect.flatMap((queues) => Queue.offer(queues.pullCompletedAfterReceipt, event)),
              Effect.asVoid
            )
          case "RequestRejectedOffline":
            return queuesFor(event.spaceId).pipe(
              Effect.flatMap((queues) => Queue.offer(queues.requestRejectedOffline, event)),
              Effect.asVoid
            )
          default:
            return absurd(event)
        }
      },
      awaitReceiptCommitted: (spaceId) =>
        queuesFor(spaceId).pipe(Effect.flatMap((queues) => Queue.take(queues.receiptCommitted))),
      awaitReceiptDropped: (spaceId) =>
        queuesFor(spaceId).pipe(Effect.flatMap((queues) => Queue.take(queues.receiptDropped))),
      awaitReceiptReturned: (spaceId) =>
        queuesFor(spaceId).pipe(Effect.flatMap((queues) => Queue.take(queues.receiptReturned))),
      awaitPullCompletedAfterReceipt: (spaceId) =>
        queuesFor(spaceId).pipe(Effect.flatMap((queues) => Queue.take(queues.pullCompletedAfterReceipt))),
      awaitRequestRejectedOffline: (spaceId) =>
        queuesFor(spaceId).pipe(Effect.flatMap((queues) => Queue.take(queues.requestRejectedOffline)))
    })
  })
)
