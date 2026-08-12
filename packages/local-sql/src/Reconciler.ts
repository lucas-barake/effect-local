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
import * as FiberMap from "effect/FiberMap"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import type * as Scope from "effect/Scope"
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
  readonly shutdown: Effect.Effect<void>
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

export interface ManagedSpace {
  readonly spaceId: Identity.SpaceId
  readonly generation: number
  readonly definition: Definition.Any
  readonly local: Pick<
    LocalStore.Service,
    "requestReconciliation" | "reconciliationGenerations" | "completeReconciliation"
  >
  readonly reconciliation: ReconciliationService
  readonly retryDelay?: Duration.Input
}

export interface ManagerService {
  readonly register: (space: ManagedSpace) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly unregister: (spaceId: Identity.SpaceId, generation: number) => Effect.Effect<void>
  readonly sync: (spaceId: Identity.SpaceId) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly notify: (spaceId: Identity.SpaceId) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly status: (
    spaceId: Identity.SpaceId
  ) => Effect.Effect<ReplicaStatus.ReplicaStatus, ReplicaError.ReplicaError>
}

export class Manager extends Context.Service<Manager, ManagerService>()(
  "@lucas-barake/effect-local-sql/Reconciler/Manager"
) {}

interface ManagedState extends ManagedSpace {
  readonly retryDelayMillis: number
  queued: boolean
  running: boolean
  dirtyEpoch: number
}

interface Work {
  readonly spaceId: Identity.SpaceId
  readonly generation: number
}

const managedKey = (spaceId: Identity.SpaceId, generation: number) => `${spaceId}:${generation}`

export const makeManager = (options: {
  readonly concurrency?: number
} = {}): Effect.Effect<
  ManagerService,
  ReplicaError.InvalidConfiguration,
  SyncEngine.SyncEngine | Scope.Scope
> =>
  Effect.gen(function*() {
    const remote = yield* SyncEngine.SyncEngine
    const concurrency = options.concurrency ?? 8
    if (!Number.isSafeInteger(concurrency) || concurrency <= 0) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "reconciliationConcurrency",
        message: "reconciliationConcurrency must be a positive safe integer"
      })
    }
    const queue = yield* Effect.acquireRelease(
      Queue.unbounded<Work>(),
      Queue.shutdown
    )
    const turns = yield* FiberMap.make<string, void, never>()
    const watches = yield* FiberMap.make<string, void, never>()
    const spaces = new Map<Identity.SpaceId, ManagedState>()

    const lookup = (spaceId: Identity.SpaceId) =>
      Effect.suspend(() => {
        const space = spaces.get(spaceId)
        if (space === undefined) return Effect.fail(new ReplicaError.SpaceNotJoined({ spaceId }))
        return Effect.succeed(space)
      })

    const enqueue = (space: ManagedState) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function*() {
          const current = spaces.get(space.spaceId)
          if (current !== space) {
            yield* new ReplicaError.SpaceNotJoined({ spaceId: space.spaceId })
            return
          }
          yield* restore(current.local.requestReconciliation)
          const admitted = spaces.get(space.spaceId)
          if (admitted !== space) {
            yield* new ReplicaError.SpaceNotJoined({ spaceId: space.spaceId })
            return
          }
          admitted.dirtyEpoch += 1
          if (admitted.queued || admitted.running) return
          admitted.queued = true
          yield* Queue.offer(queue, { spaceId: admitted.spaceId, generation: admitted.generation })
        })
      )

    const notify = (spaceId: Identity.SpaceId) => lookup(spaceId).pipe(Effect.flatMap(enqueue))

    const runTurn = (space: ManagedState, epoch: number) =>
      Effect.gen(function*() {
        const generations = yield* space.local.reconciliationGenerations
        if (generations.completed >= generations.requested) return
        yield* space.reconciliation.sync
        yield* space.local.completeReconciliation(generations.requested)
        yield* space.reconciliation.succeeded
      }).pipe(
        Effect.catch((error) =>
          space.reconciliation.failed(error).pipe(
            Effect.andThen(Effect.sleep(space.retryDelayMillis)),
            Effect.andThen(enqueue(space)),
            Effect.catch(() => Effect.void)
          )
        ),
        Effect.ensuring(Effect.uninterruptible(Effect.gen(function*() {
          const current = spaces.get(space.spaceId)
          if (current !== space) return
          current.running = false
          if (current.dirtyEpoch <= epoch || current.queued) return
          current.queued = true
          yield* Queue.offer(queue, { spaceId: current.spaceId, generation: current.generation })
        })))
      )

    const selectWork = (work: Work) => {
      const current = spaces.get(work.spaceId)
      if (current === undefined || current.generation !== work.generation || !current.queued) return undefined
      current.queued = false
      if (current.running) return undefined
      current.running = true
      return { space: current, epoch: current.dirtyEpoch }
    }

    const worker = Effect.forever(Effect.gen(function*() {
      const work = yield* Queue.take(queue)
      const selected = selectWork(work)
      if (selected === undefined) return
      const fiber = yield* FiberMap.run(
        turns,
        managedKey(selected.space.spaceId, selected.space.generation),
        runTurn(selected.space, selected.epoch)
      )
      yield* Fiber.await(fiber)
    }))
    yield* Effect.forEach(
      Array.from({ length: concurrency }),
      () => Effect.forkScoped(worker),
      { discard: true }
    )

    const register = (space: ManagedSpace) =>
      Effect.gen(function*() {
        const retryDelayMillis = yield* Configuration.positiveFiniteDurationMillis(
          "retryDelay",
          space.retryDelay ?? Duration.seconds(1)
        )
        const state: ManagedState = {
          ...space,
          retryDelayMillis,
          queued: false,
          running: false,
          dirtyEpoch: 0
        }
        spaces.set(space.spaceId, state)
        yield* FiberMap.run(
          watches,
          managedKey(space.spaceId, space.generation),
          Effect.forever(
            remote.watch({ spaceId: space.spaceId, schema: space.definition.schemaIdentity }).pipe(
              Stream.runForEach(() => enqueue(state)),
              Effect.catch((error) =>
                state.reconciliation.failed(error).pipe(
                  Effect.andThen(enqueue(state)),
                  Effect.catch(() => Effect.void)
                )
              ),
              Effect.andThen(Effect.sleep(retryDelayMillis))
            )
          )
        )
        yield* enqueue(state)
      }).pipe(Effect.onError(() => unregister(space.spaceId, space.generation)))

    const unregister = (spaceId: Identity.SpaceId, generation: number) =>
      Effect.gen(function*() {
        const current = spaces.get(spaceId)
        if (current?.generation === generation) spaces.delete(spaceId)
        const key = managedKey(spaceId, generation)
        yield* FiberMap.remove(watches, key)
        yield* FiberMap.remove(turns, key)
      })

    const sync = (spaceId: Identity.SpaceId) =>
      lookup(spaceId).pipe(Effect.flatMap((space) => space.reconciliation.sync))
    const status = (spaceId: Identity.SpaceId) =>
      lookup(spaceId).pipe(Effect.flatMap((space) => space.reconciliation.status))

    return Manager.of({ register, unregister, sync, notify, status })
  })

export const layerManager: Layer.Layer<Manager, ReplicaError.InvalidConfiguration, SyncEngine.SyncEngine> = Layer
  .effect(
    Manager,
    makeManager()
  )

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
        local.pendingCount.pipe(
          Effect.catch(() => Effect.succeed(0)),
          Effect.flatMap((pending) => {
            if (error._tag === "AuthorizationDenied") {
              return setStatus({ _tag: "NeedsAuthentication", pending })
            }
            if (error._tag === "ServerUnavailable") return setStatus({ _tag: "Offline", pending })
            return setStatus({ _tag: "Failed", pending, message: error._tag })
          })
        )
      const succeeded = Effect.gen(function*() {
        const pending = yield* local.pendingCount
        const cursor = yield* local.cursor
        yield* setStatus({ _tag: "Online", pending, cursor })
      })

      const bootstrap = (
        manifest: Protocol.SnapshotManifest
      ): Effect.Effect<void, ReplicaError.ReplicaError> =>
        Effect.gen(function*() {
          let afterOrdinal = yield* local.prepareBootstrap(manifest)
          while (true) {
            const page = yield* remote.bootstrap({
              spaceId: options.spaceId,
              schema: options.definition.schemaIdentity,
              snapshotId: manifest.snapshotId,
              afterOrdinal,
              limit: pageSize
            })
            if (page.manifest.snapshotId !== manifest.snapshotId) {
              yield* bootstrap(page.manifest)
              return yield* Effect.void
            }
            const complete = yield* local.stageBootstrapPage(page)
            if (complete) {
              yield* local.installBootstrap(page.manifest)
              return yield* Effect.void
            }
            afterOrdinal += page.entities.length
          }
        })

      const bootstrapExpired = (receipt: Protocol.ExpiredReceipt) =>
        Effect.gen(function*() {
          const firstPage = yield* remote.bootstrap({
            spaceId: options.spaceId,
            schema: options.definition.schemaIdentity,
            snapshotId: receipt.snapshotId,
            afterOrdinal: -1,
            limit: pageSize
          })
          if (
            firstPage.manifest.sequence < receipt.snapshotSequence ||
            firstPage.manifest.terminalSequenceThrough < receipt.terminalSequenceThrough
          ) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: `Snapshot ${receipt.snapshotId} does not cover expired receipt ${receipt.mutationId}`
            })
          }
          let afterOrdinal = yield* local.prepareBootstrap(firstPage.manifest)
          if (afterOrdinal < 0) {
            const complete = yield* local.stageBootstrapPage(firstPage)
            if (complete) {
              yield* local.installBootstrap(firstPage.manifest)
              return yield* Effect.void
            }
            afterOrdinal = firstPage.entities.length - 1
          }
          while (true) {
            const page = yield* remote.bootstrap({
              spaceId: options.spaceId,
              schema: options.definition.schemaIdentity,
              snapshotId: firstPage.manifest.snapshotId,
              afterOrdinal,
              limit: pageSize
            })
            if (page.manifest.snapshotId !== firstPage.manifest.snapshotId) {
              yield* bootstrap(page.manifest)
              return yield* Effect.void
            }
            const complete = yield* local.stageBootstrapPage(page)
            if (complete) {
              yield* local.installBootstrap(page.manifest)
              return yield* Effect.void
            }
            afterOrdinal += page.entities.length
          }
        })

      const catchUp = Effect.gen(function*() {
        while (true) {
          const cursor = yield* local.cursor
          const result = yield* remote.pull({
            spaceId: options.spaceId,
            schema: options.definition.schemaIdentity,
            after: cursor,
            limit: pageSize
          })
          if ("_tag" in result) {
            yield* bootstrap(result.manifest)
            continue
          }
          yield* local.applyEntries(result.entries, { publishProjection: !result.hasMore })
          if (!result.hasMore) return
        }
      })

      const submitPending = Effect.gen(function*() {
        yield* local.settleReceipts
        while (true) {
          const pending = yield* local.pending
          let installedExpiredSnapshot = false
          for (const mutation of pending) {
            const receipt = yield* remote.submit({
              envelope: mutation.envelope,
              schema: options.definition.schemaIdentity
            })
            yield* local.persistReceipt(receipt)
            if (receipt._tag === "Expired") {
              yield* local.settleReceipts
              const unresolved = (yield* local.pending).some(
                (candidate) => candidate.envelope.mutationId === receipt.mutationId
              )
              if (!unresolved) continue
              yield* bootstrapExpired(receipt)
              installedExpiredSnapshot = true
              break
            }
          }
          if (installedExpiredSnapshot) continue
          yield* local.settleReceipts
          return
        }
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
          Effect.flatMap((generations) => {
            if (generations.completed >= generations.requested) return Effect.void
            return reconciliation.sync.pipe(
              Effect.forkChild({ startImmediately: true }),
              Effect.flatMap(Fiber.await),
              Effect.flatMap((exit) => {
                if (exit._tag === "Failure" && Cause.hasInterruptsOnly(exit.cause)) {
                  return Effect.fail(new ReplicaError.ServerUnavailable())
                }
                return exit
              }),
              Effect.andThen(local.completeReconciliation(generations.requested)),
              Effect.andThen(reconciliation.succeeded)
            )
          }),
          Effect.catch((error) => {
            if (error._tag === "StaleSchema") return reconciliation.failed(error)
            return Effect.logWarning("Reconciliation failed", error).pipe(
              Effect.andThen(Effect.sleep(retryDelayMillis)),
              Effect.andThen(notify)
            )
          })
        )
      )
      const workerFiber = yield* Effect.forkScoped(worker)
      const watchFiber = yield* Effect.forkScoped(
        Effect.forever(
          remote.watch({ spaceId: options.spaceId, schema: options.definition.schemaIdentity }).pipe(
            Stream.runForEach(() => requestAndNotify),
            Effect.matchEffect({
              onFailure: (error) => {
                if (error._tag === "StaleSchema") return Effect.fail(error)
                return Effect.logWarning("Sync watch ended", error).pipe(
                  Effect.andThen(requestAndNotify),
                  Effect.catch((wakeError) =>
                    Effect.logWarning("Could not persist a reconciliation wake", wakeError).pipe(
                      Effect.andThen(notify)
                    )
                  )
                )
              },
              onSuccess: () => requestAndNotify
            }),
            Effect.andThen(Effect.sleep(retryDelayMillis))
          )
        ).pipe(Effect.catchTag("StaleSchema", reconciliation.failed))
      )
      yield* requestAndNotify
      yield* Effect.addFinalizer(() =>
        Fiber.interruptAll([workerFiber, watchFiber]).pipe(
          Effect.andThen(Queue.shutdown(wake)),
          Effect.asVoid
        )
      )

      return Reconciler.of({
        sync: reconciliation.sync,
        notify,
        status: reconciliation.status,
        shutdown: Effect.void
      })
    })
  )

export const layer = (options: Options) =>
  layerInMemoryScheduler(options).pipe(Layer.provideMerge(layerOnePass(options)))
