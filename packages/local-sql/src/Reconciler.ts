import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as LocalStore from "./LocalStore.js"
import * as SyncEngine from "./SyncEngine.js"

export interface Service {
  readonly sync: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly notify: Effect.Effect<void>
  readonly status: Effect.Effect<ReplicaStatus.ReplicaStatus>
}

export class Reconciler extends Context.Service<Reconciler, Service>()(
  "@lucas-barake/effect-local-sql/Reconciler"
) {}

export const layer = (options: {
  readonly spaceId: Identity.SpaceId
  readonly pageSize?: number
  readonly retryDelay?: Duration.Input
}): Layer.Layer<Reconciler, ReplicaError.InvalidConfiguration, LocalStore.Store | SyncEngine.SyncEngine> =>
  Layer.effect(
    Reconciler,
    Effect.gen(function*() {
      const pageSize = options.pageSize ?? 256
      if (!Number.isSafeInteger(pageSize) || pageSize <= 0 || pageSize > Protocol.maximumBatchEntries) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "pageSize",
          message: `pageSize must be between 1 and ${Protocol.maximumBatchEntries}`
        })
      }
      const retryDelay = yield* Option.match(
        Duration.fromInput(options.retryDelay ?? Duration.seconds(1)),
        {
          onNone: () =>
            Effect.fail(
              new ReplicaError.InvalidConfiguration({
                option: "retryDelay",
                message: "retryDelay must be a valid positive finite duration"
              })
            ),
          onSome: (duration) =>
            Duration.isPositive(duration) && Duration.isFinite(duration)
              ? Effect.succeed(duration)
              : Effect.fail(
                new ReplicaError.InvalidConfiguration({
                  option: "retryDelay",
                  message: "retryDelay must be a valid positive finite duration"
                })
              )
        }
      )
      const local = yield* LocalStore.Store
      const remote = yield* SyncEngine.SyncEngine
      const gate = yield* Semaphore.make(1)
      const wake = yield* Queue.sliding<void>(1)
      const status = yield* Ref.make<ReplicaStatus.ReplicaStatus>({ _tag: "Offline", pending: 0 })
      const setStatus = (value: ReplicaStatus.ReplicaStatus) =>
        Ref.set(status, value).pipe(
          Effect.andThen(local.invalidateStatus)
        )

      const catchUp = Effect.gen(function*() {
        let hasMore = true
        while (hasMore) {
          const cursor = yield* local.cursor
          const page = yield* remote.pull({ spaceId: options.spaceId, after: cursor, limit: pageSize })
          yield* local.applyEntries(page.entries)
          hasMore = page.hasMore
        }
      })

      const submitPending = Effect.gen(function*() {
        const pending = yield* local.pending
        for (const mutation of pending) {
          const receipt = yield* remote.submit(mutation.envelope)
          yield* local.applyReceipt(receipt)
          if (receipt._tag === "Accepted") yield* catchUp
        }
      })

      const sync = gate.withPermit(Effect.gen(function*() {
        const pendingBefore = yield* local.pendingCount
        yield* setStatus({ _tag: "Connecting", pending: pendingBefore })
        yield* catchUp
        yield* submitPending
        yield* catchUp
        const pendingAfter = yield* local.pendingCount
        const cursor = yield* local.cursor
        yield* setStatus({ _tag: "Online", pending: pendingAfter, cursor })
      })).pipe(
        Effect.tapError((error) => setStatus({ _tag: "Failed", pending: 0, message: error._tag })),
        Effect.withSpan("Reconciler.sync")
      )

      const worker = Effect.forever(
        Queue.take(wake).pipe(
          Effect.andThen(sync),
          Effect.catch((cause) =>
            Effect.logWarning("Reconciliation failed", cause).pipe(
              Effect.andThen(Effect.sleep(retryDelay)),
              Effect.andThen(Queue.offer(wake, undefined)),
              Effect.asVoid
            )
          )
        )
      )
      const workerFiber = yield* Effect.forkScoped(worker)
      yield* Effect.forkScoped(
        remote.watch(options.spaceId).pipe(
          Stream.runForEach(() => Queue.offer(wake, undefined)),
          Effect.catch((cause) => Effect.logWarning("Sync watch ended", cause)),
          Effect.ensuring(Queue.offer(wake, undefined))
        )
      )
      yield* Queue.offer(wake, undefined)
      yield* Effect.addFinalizer(() =>
        Fiber.interrupt(workerFiber).pipe(Effect.andThen(Queue.shutdown(wake)), Effect.asVoid)
      )

      return Reconciler.of({
        sync,
        notify: Queue.offer(wake, undefined).pipe(Effect.asVoid),
        status: Ref.get(status)
      })
    })
  )
