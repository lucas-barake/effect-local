import type * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import { absurd } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"

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

interface Controls {
  readonly partitionAfterNextReceipt: boolean
  readonly withholdPullEvidence: boolean
  readonly postReceiptPullPending: boolean
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
    const initial = (): State => ({ online: true, dropNextReceipt: false, duplicateNextPage: false })
    const state = yield* Ref.make(new Map<Identity.SpaceId, State>())
    const initialControls = (): Controls => ({
      partitionAfterNextReceipt: false,
      withholdPullEvidence: false,
      postReceiptPullPending: false
    })
    const controls = yield* Ref.make(new Map<Identity.SpaceId, Controls>())
    const receiptGates = yield* Ref.make(new Map<Identity.SpaceId, Deferred.Deferred<void>>())
    const receiptCommitted = yield* Effect.acquireRelease(Queue.unbounded<ReceiptCommitted>(), Queue.shutdown)
    const receiptDropped = yield* Effect.acquireRelease(Queue.unbounded<ReceiptDropped>(), Queue.shutdown)
    const receiptReturned = yield* Effect.acquireRelease(Queue.unbounded<ReceiptReturned>(), Queue.shutdown)
    const pullCompletedAfterReceipt = yield* Effect.acquireRelease(
      Queue.unbounded<PullCompletedAfterReceipt>(),
      Queue.shutdown
    )
    const requestRejectedOffline = yield* Effect.acquireRelease(
      Queue.unbounded<RequestRejectedOffline>(),
      Queue.shutdown
    )
    const get = (spaceId: Identity.SpaceId) =>
      Ref.get(state).pipe(Effect.map((spaces) => spaces.get(spaceId) ?? initial()))
    const set = (spaceId: Identity.SpaceId, patch: Partial<State>) =>
      Ref.update(state, (spaces) => {
        const next = new Map(spaces)
        next.set(spaceId, { ...(spaces.get(spaceId) ?? initial()), ...patch })
        return next
      })
    const take = (spaceId: Identity.SpaceId, key: "dropNextReceipt" | "duplicateNextPage") =>
      Ref.modify(state, (spaces) => {
        const current = spaces.get(spaceId) ?? initial()
        const next = new Map(spaces)
        next.set(spaceId, { ...current, [key]: false })
        return [current[key], next]
      })
    const setControl = (spaceId: Identity.SpaceId, patch: Partial<Controls>) =>
      Ref.update(controls, (spaces) => {
        const next = new Map(spaces)
        next.set(spaceId, { ...(spaces.get(spaceId) ?? initialControls()), ...patch })
        return next
      })
    const takeControl = (spaceId: Identity.SpaceId, key: "partitionAfterNextReceipt" | "postReceiptPullPending") =>
      Ref.modify(controls, (spaces) => {
        const current = spaces.get(spaceId) ?? initialControls()
        const next = new Map(spaces)
        next.set(spaceId, { ...current, [key]: false })
        return [current[key], next]
      })
    const awaitSpaceEvent = <A extends Event,>(queue: Queue.Dequeue<A>, spaceId: Identity.SpaceId) =>
      Effect.gen(function*() {
        while (true) {
          const event = yield* Queue.take(queue)
          if (event.spaceId === spaceId) return event
        }
      })
    return FaultInjection.of({
      state: get,
      partition: (spaceId) => set(spaceId, { online: false }),
      heal: (spaceId) => set(spaceId, { online: true }),
      dropNextReceipt: (spaceId) => set(spaceId, { dropNextReceipt: true }),
      duplicateNextPage: (spaceId) => set(spaceId, { duplicateNextPage: true }),
      partitionAfterNextReceipt: (spaceId) => setControl(spaceId, { partitionAfterNextReceipt: true }),
      withholdPullEvidence: (spaceId) => setControl(spaceId, { withholdPullEvidence: true }),
      releasePullEvidence: (spaceId) => setControl(spaceId, { withholdPullEvidence: false }),
      holdNextReceipt: (spaceId) =>
        Effect.gen(function*() {
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
      takePartitionAfterReceipt: (spaceId) => takeControl(spaceId, "partitionAfterNextReceipt"),
      shouldWithholdPullEvidence: (spaceId) =>
        Ref.get(controls).pipe(
          Effect.map((spaces) => (spaces.get(spaceId) ?? initialControls()).withholdPullEvidence)
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
      markReceiptReturned: (spaceId) => setControl(spaceId, { postReceiptPullPending: true }),
      takePostReceiptPull: (spaceId) => takeControl(spaceId, "postReceiptPullPending"),
      emit: (event) => {
        switch (event._tag) {
          case "ReceiptCommitted":
            return Queue.offer(receiptCommitted, event).pipe(Effect.asVoid)
          case "ReceiptDropped":
            return Queue.offer(receiptDropped, event).pipe(Effect.asVoid)
          case "ReceiptReturned":
            return Queue.offer(receiptReturned, event).pipe(Effect.asVoid)
          case "PullCompletedAfterReceipt":
            return Queue.offer(pullCompletedAfterReceipt, event).pipe(Effect.asVoid)
          case "RequestRejectedOffline":
            return Queue.offer(requestRejectedOffline, event).pipe(Effect.asVoid)
          default:
            return absurd(event)
        }
      },
      awaitReceiptCommitted: (spaceId) => awaitSpaceEvent(receiptCommitted, spaceId),
      awaitReceiptDropped: (spaceId) => awaitSpaceEvent(receiptDropped, spaceId),
      awaitReceiptReturned: (spaceId) => awaitSpaceEvent(receiptReturned, spaceId),
      awaitPullCompletedAfterReceipt: (spaceId) => awaitSpaceEvent(pullCompletedAfterReceipt, spaceId),
      awaitRequestRejectedOffline: (spaceId) => awaitSpaceEvent(requestRejectedOffline, spaceId)
    })
  })
)
