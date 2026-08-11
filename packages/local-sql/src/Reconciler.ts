import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as Configuration from "./internal/configuration.js"
import * as LocalStore from "./LocalStore.js"
import * as SyncEngine from "./SyncEngine.js"

export interface ReconciliationService {
  readonly sync: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly failed: (error: ReplicaError.ReplicaError) => Effect.Effect<void>
  readonly succeeded: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly status: Effect.Effect<ReplicaStatus.ReplicaStatus>
}

export class Reconciliation extends Context.Service<Reconciliation, ReconciliationService>()(
  "@lucas-barake/effect-local-sql/Reconciliation"
) {}

export interface Service {
  readonly sync: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly notify: Effect.Effect<void>
  readonly status: Effect.Effect<ReplicaStatus.ReplicaStatus>
}

export class Reconciler extends Context.Service<Reconciler, Service>()(
  "@lucas-barake/effect-local-sql/Reconciler"
) {}

export interface Options {
  readonly definition: Definition.Any
  readonly spaceId: Identity.SpaceId
  readonly pageSize?: number
  readonly retryDelay?: Duration.Input
}

export const layerOnePass = (
  options: Pick<Options, "definition" | "spaceId" | "pageSize">
): Layer.Layer<Reconciliation, ReplicaError.InvalidConfiguration, LocalStore.Store | SyncEngine.SyncEngine> =>
  Layer.effect(
    Reconciliation,
    Effect.gen(function*() {
      const pageSize = options.pageSize ?? 256
      if (!Number.isSafeInteger(pageSize) || pageSize <= 0 || pageSize > Protocol.maximumBatchEntries) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "pageSize",
          message: `pageSize must be between 1 and ${Protocol.maximumBatchEntries}`
        })
      }
      const local = yield* LocalStore.Store
      const remote = yield* SyncEngine.SyncEngine
      const gate = yield* Semaphore.make(1)
      const status = yield* Ref.make<ReplicaStatus.ReplicaStatus>({ _tag: "Offline", pending: 0 })
      const setStatus = (value: ReplicaStatus.ReplicaStatus) =>
        Ref.set(status, value).pipe(Effect.andThen(local.invalidateStatus))
      const failed = (error: ReplicaError.ReplicaError) =>
        setStatus({ _tag: "Failed", pending: 0, message: error._tag })
      const succeeded = Effect.gen(function*() {
        const pending = yield* local.pendingCount
        const cursor = yield* local.cursor
        yield* setStatus({ _tag: "Online", pending, cursor })
      })

      const catchUp = Effect.gen(function*() {
        let hasMore = true
        while (hasMore) {
          const cursor = yield* local.cursor
          const page = yield* remote.pull({
            spaceId: options.spaceId,
            schema: options.definition.schemaIdentity,
            after: cursor,
            limit: pageSize
          })
          yield* local.applyEntries(page.entries)
          hasMore = page.hasMore
        }
      })

      const submitPending = Effect.gen(function*() {
        const pending = yield* local.pending
        for (const mutation of pending) {
          yield* local.persistReceipt(
            yield* remote.submit({
              envelope: mutation.envelope,
              schema: options.definition.schemaIdentity
            })
          )
        }
        yield* local.settleReceipts
      })

      const sync = gate.withPermit(Effect.gen(function*() {
        const pending = yield* local.pendingCount
        yield* setStatus({ _tag: "Connecting", pending })
        yield* catchUp
        yield* submitPending
        yield* catchUp
        yield* succeeded
      })).pipe(
        Effect.tapError(failed),
        Effect.withSpan("Reconciliation.sync")
      )

      return Reconciliation.of({ sync, failed, succeeded, status: Ref.get(status) })
    })
  )

export const layerInMemoryScheduler = (
  options: Pick<Options, "definition" | "spaceId" | "retryDelay">
): Layer.Layer<
  Reconciler,
  ReplicaError.ReplicaError,
  LocalStore.Store | Reconciliation | SyncEngine.SyncEngine
> =>
  Layer.effect(
    Reconciler,
    Effect.gen(function*() {
      const retryDelayMillis = yield* Configuration.positiveFiniteDurationMillis(
        "retryDelay",
        options.retryDelay ?? Duration.seconds(1)
      )
      const local = yield* LocalStore.Store
      const reconciliation = yield* Reconciliation
      const remote = yield* SyncEngine.SyncEngine
      const wake = yield* Queue.sliding<void>(1)
      const notify = Queue.offer(wake, undefined).pipe(Effect.asVoid)
      const requestAndNotify = local.requestReconciliation.pipe(Effect.andThen(notify))
      const worker = Effect.forever(
        Queue.take(wake).pipe(
          Effect.andThen(local.reconciliationGenerations),
          Effect.flatMap((generations) =>
            generations.completed >= generations.requested
              ? Effect.void
              : reconciliation.sync.pipe(
                Effect.forkChild({ startImmediately: true }),
                Effect.flatMap(Fiber.await),
                Effect.flatMap((exit) =>
                  exit._tag === "Failure" && Cause.hasInterruptsOnly(exit.cause)
                    ? Effect.fail(new ReplicaError.ServerUnavailable())
                    : exit
                ),
                Effect.andThen(local.completeReconciliation(generations.requested)),
                Effect.andThen(reconciliation.succeeded)
              )
          ),
          Effect.catch((error) =>
            Effect.logWarning("Reconciliation failed", error).pipe(
              Effect.andThen(Effect.sleep(retryDelayMillis)),
              Effect.andThen(notify)
            )
          )
        )
      )
      const workerFiber = yield* Effect.forkScoped(worker)
      const watchFiber = yield* Effect.forkScoped(
        Effect.forever(
          remote.watch({ spaceId: options.spaceId, schema: options.definition.schemaIdentity }).pipe(
            Stream.runForEach(() => requestAndNotify),
            Effect.catch((error) => Effect.logWarning("Sync watch ended", error)),
            Effect.ensuring(
              requestAndNotify.pipe(
                Effect.catch((error) =>
                  Effect.logWarning("Could not persist a reconciliation wake", error).pipe(
                    Effect.andThen(notify)
                  )
                )
              )
            ),
            Effect.andThen(Effect.sleep(retryDelayMillis))
          )
        )
      )
      yield* requestAndNotify
      yield* Effect.addFinalizer(() =>
        Fiber.interruptAll([workerFiber, watchFiber]).pipe(
          Effect.andThen(Queue.shutdown(wake)),
          Effect.asVoid
        )
      )

      return Reconciler.of({ sync: reconciliation.sync, notify, status: reconciliation.status })
    })
  )

export const layer = (options: Options) =>
  layerInMemoryScheduler(options).pipe(Layer.provideMerge(layerOnePass(options)))
