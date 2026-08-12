import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
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
  readonly maximumRetryDelay?: Duration.Input
}

export interface ManagedSpace {
  readonly spaceId: Identity.SpaceId
  readonly generation: number
  readonly definition: Definition.Any
  readonly local: Pick<
    LocalStore.Service,
    "requestReconciliation" | "reconciliationGenerations" | "completeReconciliation" | "replicationState"
  >
  readonly reconciliation: ReconciliationService
  readonly retryDelay?: Duration.Input
  readonly maximumRetryDelay?: Duration.Input
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
  readonly maximumRetryDelayMillis: number
  queued: boolean
  running: boolean
  retryAttempt: number
  retrying: boolean
  halted: boolean
  authenticationGate: Deferred.Deferred<void> | undefined
  dirtyEpoch: number
}

interface Work {
  readonly spaceId: Identity.SpaceId
  readonly generation: number
}

const managedKey = (spaceId: Identity.SpaceId, generation: number) => `${spaceId}:${generation}`

const isTransientFailure = (error: ReplicaError.ReplicaError) =>
  error._tag === "AuthenticatorUnavailable" ||
  error._tag === "ServerUnavailable" ||
  error._tag === "OperationTimeout"

const cappedRetryDelay = (
  retryDelayMillis: number,
  maximumRetryDelayMillis: number,
  retryAttempt: number
) =>
  Math.min(
    maximumRetryDelayMillis,
    retryDelayMillis * 2 ** Math.min(retryAttempt, 52)
  )

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
    const retries = yield* FiberMap.make<string, void, never>()
    const authenticationWaiters = yield* FiberMap.make<string, void, never>()
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
          admitted.halted = false
          if (
            admitted.queued ||
            admitted.running ||
            admitted.retrying ||
            admitted.authenticationGate !== undefined
          ) return
          admitted.queued = true
          yield* Queue.offer(queue, { spaceId: admitted.spaceId, generation: admitted.generation })
        })
      )

    const notify = (spaceId: Identity.SpaceId) => lookup(spaceId).pipe(Effect.flatMap(enqueue))

    const pauseForCredential = (space: ManagedState, error: ReplicaError.CredentialRejected) =>
      Effect.gen(function*() {
        if (error.credentialGeneration === undefined) {
          space.halted = true
          return undefined
        }
        if (space.authenticationGate !== undefined) return space.authenticationGate
        const gate = yield* Deferred.make<void>()
        space.authenticationGate = gate
        const key = managedKey(space.spaceId, space.generation)
        yield* FiberMap.run(
          authenticationWaiters,
          key,
          remote.waitForCredentialChange(error.credentialGeneration).pipe(
            Effect.andThen(Effect.uninterruptible(Effect.gen(function*() {
              const current = spaces.get(space.spaceId)
              if (current !== space || current.authenticationGate !== gate) return
              current.authenticationGate = undefined
              current.retryAttempt = 0
              yield* Deferred.succeed(gate, undefined)
              yield* enqueue(current)
            }))),
            Effect.orDie,
            Effect.catchCause((cause) => {
              if (Cause.hasInterruptsOnly(cause)) return Effect.void
              return Effect.failCause(cause)
            })
          )
        )
        return gate
      })

    const scheduleRetry = (space: ManagedState) =>
      Effect.gen(function*() {
        const delay = cappedRetryDelay(
          space.retryDelayMillis,
          space.maximumRetryDelayMillis,
          space.retryAttempt
        )
        space.retryAttempt += 1
        space.retrying = true
        yield* FiberMap.run(
          retries,
          managedKey(space.spaceId, space.generation),
          Effect.sleep(delay).pipe(
            Effect.andThen(Effect.uninterruptible(Effect.gen(function*() {
              const current = spaces.get(space.spaceId)
              if (current !== space) return
              current.retrying = false
              yield* enqueue(current)
            }))),
            Effect.orDie,
            Effect.catchCause((cause) => {
              if (Cause.hasInterruptsOnly(cause)) return Effect.void
              return Effect.failCause(cause)
            })
          )
        )
      })

    const handleFailure = (space: ManagedState, error: ReplicaError.ReplicaError) => {
      let policy: Effect.Effect<void>
      if (error._tag === "CredentialRejected") {
        policy = pauseForCredential(space, error).pipe(Effect.asVoid)
      } else if (isTransientFailure(error)) {
        policy = scheduleRetry(space)
      } else {
        policy = Effect.sync(() => {
          space.halted = true
        })
      }
      return space.reconciliation.failed(error).pipe(Effect.andThen(policy))
    }

    const runTurn = (space: ManagedState, epoch: number) =>
      Effect.gen(function*() {
        const generations = yield* space.local.reconciliationGenerations
        if (generations.completed >= generations.requested) return
        yield* space.reconciliation.sync
        yield* space.local.completeReconciliation(generations.requested)
        yield* space.reconciliation.succeeded
        space.retryAttempt = 0
      }).pipe(
        Effect.catch((error) => handleFailure(space, error).pipe(Effect.catch(() => Effect.void))),
        Effect.ensuring(Effect.uninterruptible(Effect.gen(function*() {
          const current = spaces.get(space.spaceId)
          if (current !== space) return
          current.running = false
          if (
            current.dirtyEpoch <= epoch ||
            current.queued ||
            current.retrying ||
            current.halted ||
            current.authenticationGate !== undefined
          ) return
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
        const maximumRetryDelayMillis = yield* Configuration.positiveFiniteDurationMillis(
          "maximumRetryDelay",
          space.maximumRetryDelay ?? Duration.minutes(1)
        )
        if (maximumRetryDelayMillis < retryDelayMillis) {
          return yield* new ReplicaError.InvalidConfiguration({
            option: "maximumRetryDelay",
            message: "maximumRetryDelay must be greater than or equal to retryDelay"
          })
        }
        const state: ManagedState = {
          ...space,
          retryDelayMillis,
          maximumRetryDelayMillis,
          queued: false,
          running: false,
          retryAttempt: 0,
          retrying: false,
          halted: false,
          authenticationGate: undefined,
          dirtyEpoch: 0
        }
        spaces.set(space.spaceId, state)
        let watchAttempt = 0
        const watch = (): Effect.Effect<void> =>
          Effect.suspend(() => {
            const authenticationGate = state.authenticationGate
            if (authenticationGate !== undefined) {
              return Deferred.await(authenticationGate).pipe(Effect.andThen(watch()))
            }
            return Stream.unwrap(space.local.replicationState.pipe(
              Effect.map((replication) =>
                remote.watch({
                  spaceId: space.spaceId,
                  clientId: replication.clientId,
                  schema: space.definition.schemaIdentity,
                  scope: replication.scope,
                  scopeGeneration: replication.scopeGeneration,
                  cursor: replication.cursor
                })
              )
            )).pipe(
              Stream.runForEach(() => {
                watchAttempt = 0
                return enqueue(state)
              }),
              Effect.matchEffect({
                onSuccess: () => {
                  const delay = cappedRetryDelay(retryDelayMillis, maximumRetryDelayMillis, watchAttempt)
                  watchAttempt += 1
                  return Effect.sleep(delay).pipe(Effect.andThen(watch()))
                },
                onFailure: (error) => {
                  const activeAuthenticationGate = state.authenticationGate
                  if (activeAuthenticationGate !== undefined && error._tag !== "CredentialRejected") {
                    return Deferred.await(activeAuthenticationGate).pipe(Effect.andThen(watch()))
                  }
                  let policy: Effect.Effect<void>
                  if (error._tag === "CredentialRejected") {
                    policy = pauseForCredential(state, error).pipe(
                      Effect.flatMap((gate) => {
                        if (gate === undefined) return Effect.void
                        return Deferred.await(gate)
                      }),
                      Effect.andThen(watch())
                    )
                  } else if (isTransientFailure(error)) {
                    policy = Effect.suspend(() => {
                      const delay = cappedRetryDelay(retryDelayMillis, maximumRetryDelayMillis, watchAttempt)
                      watchAttempt += 1
                      return Effect.sleep(delay).pipe(Effect.andThen(watch()))
                    })
                  } else {
                    policy = Effect.void
                  }
                  return state.reconciliation.failed(error).pipe(Effect.andThen(policy))
                }
              })
            )
          })
        yield* FiberMap.run(
          watches,
          managedKey(space.spaceId, space.generation),
          watch()
        )
        return yield* enqueue(state)
      }).pipe(Effect.onError(() => unregister(space.spaceId, space.generation)))

    const unregister = (spaceId: Identity.SpaceId, generation: number) =>
      Effect.gen(function*() {
        const current = spaces.get(spaceId)
        if (current?.generation === generation) spaces.delete(spaceId)
        const key = managedKey(spaceId, generation)
        yield* FiberMap.remove(watches, key)
        yield* FiberMap.remove(turns, key)
        yield* FiberMap.remove(retries, key)
        yield* FiberMap.remove(authenticationWaiters, key)
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
      const updateAvailable = yield* Ref.make<Identity.SchemaIdentity | undefined>(undefined)
      const setStatus = (value: ReplicaStatus.ReplicaStatus) =>
        Ref.set(status, value).pipe(Effect.andThen(local.invalidateStatus))
      const failed = (error: ReplicaError.ReplicaError) =>
        local.pendingCount.pipe(
          Effect.catch(() => Effect.succeed(0)),
          Effect.flatMap((pending) => {
            if (error._tag === "CredentialRejected") {
              return setStatus({ _tag: "NeedsAuthentication", pending })
            }
            if (
              error._tag === "AuthenticatorUnavailable" ||
              error._tag === "ServerUnavailable" ||
              error._tag === "OperationTimeout"
            ) return setStatus({ _tag: "Offline", pending })
            return setStatus({ _tag: "Failed", pending, message: error._tag })
          })
        )
      const succeeded = Effect.gen(function*() {
        if ((yield* Ref.get(status))._tag === "NeedsAuthentication") return
        const pending = yield* local.pendingCount
        const cursor = yield* local.cursor
        const serverSchema = yield* Ref.get(updateAvailable)
        if (serverSchema !== undefined) {
          yield* setStatus({ _tag: "SchemaUpdateAvailable", pending, cursor, serverSchema })
        } else {
          yield* setStatus({ _tag: "Online", pending, cursor })
        }
      })
      const observeServerSchema = (serverSchema: Identity.SchemaIdentity) => {
        if (
          serverSchema.version === options.definition.schemaIdentity.version &&
          serverSchema.hash === options.definition.schemaIdentity.hash
        ) return Ref.set(updateAvailable, undefined)
        return Ref.set(updateAvailable, serverSchema)
      }

      const continueBootstrap = (
        manifest: Protocol.SnapshotManifest,
        initialAfterOrdinal: number
      ): Effect.Effect<void, ReplicaError.ReplicaError> =>
        Effect.gen(function*() {
          let afterOrdinal = initialAfterOrdinal
          while (true) {
            const state = yield* local.replicationState
            const page = yield* remote.bootstrap({
              spaceId: options.spaceId,
              clientId: state.clientId,
              schema: options.definition.schemaIdentity,
              scope: state.scope,
              scopeGeneration: state.scopeGeneration,
              cursor: manifest.cursor,
              snapshotId: manifest.snapshotId,
              afterOrdinal,
              limit: pageSize
            })
            yield* observeServerSchema(page.serverSchema)
            if (page.manifest.snapshotId !== manifest.snapshotId) {
              const nextAfterOrdinal = yield* local.prepareBootstrap(page.manifest)
              yield* continueBootstrap(page.manifest, nextAfterOrdinal)
              return yield* Effect.void
            }
            const complete = yield* local.stageBootstrapPage(page)
            if (complete) {
              yield* local.installBootstrap(page.manifest)
              return yield* Effect.void
            }
            afterOrdinal += page.entries.length
          }
        })

      const bootstrap = (
        manifest: Protocol.SnapshotManifest
      ): Effect.Effect<void, ReplicaError.ReplicaError> =>
        local.prepareBootstrap(manifest).pipe(
          Effect.flatMap((afterOrdinal) => continueBootstrap(manifest, afterOrdinal))
        )

      const bootstrapExpired = (receipt: Protocol.ExpiredReceipt) =>
        Effect.gen(function*() {
          const state = yield* local.replicationState
          if (state.cursor === null) {
            return yield* new ReplicaError.ProtocolInvalid({
              message: "Expired receipt recovery requires an installed replication view"
            })
          }
          const firstPage = yield* remote.bootstrap({
            spaceId: options.spaceId,
            clientId: state.clientId,
            schema: options.definition.schemaIdentity,
            scope: state.scope,
            scopeGeneration: state.scopeGeneration,
            cursor: state.cursor,
            snapshotId: receipt.snapshotId,
            afterOrdinal: -1,
            limit: pageSize
          })
          yield* observeServerSchema(firstPage.serverSchema)
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
            afterOrdinal = firstPage.entries.length - 1
          }
          return yield* continueBootstrap(firstPage.manifest, afterOrdinal)
        }).pipe(Effect.tapErrorTag("AuthorizationDenied", () => local.revokeReplication))

      const catchUp = Effect.gen(function*() {
        while (true) {
          const state = yield* local.replicationState
          const result = yield* remote.pull({
            spaceId: options.spaceId,
            clientId: state.clientId,
            schema: options.definition.schemaIdentity,
            scope: state.scope,
            scopeGeneration: state.scopeGeneration,
            cursor: state.cursor,
            limit: pageSize
          })
          yield* observeServerSchema(result.serverSchema)
          if ("_tag" in result) {
            yield* bootstrap(result.manifest)
            continue
          }
          yield* local.applyViewPage(result)
          if (!result.hasMore) return
        }
      }).pipe(Effect.tapErrorTag("AuthorizationDenied", () => local.revokeReplication))

      const submitPending = Effect.gen(function*() {
        yield* local.settleReceipts
        while (true) {
          const pending = yield* local.pendingToSubmit
          let installedExpiredSnapshot = false
          for (const mutation of pending) {
            const receipt = yield* Effect.gen(function*() {
              yield* local.markSubmitting(mutation.envelope.mutationId)
              const remoteReceipt = yield* remote.submit({
                envelope: mutation.envelope,
                schema: options.definition.schemaIdentity
              })
              yield* local.persistReceipt(remoteReceipt)
              return remoteReceipt
            }).pipe(Effect.tapError(() => local.markRetrying(mutation.envelope.mutationId)))
            if (receipt._tag === "Expired") {
              yield* local.settleReceipts
              const unresolved = (yield* local.pendingToSubmit).some(
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
        yield* local.settleReceipts
        yield* succeeded
      })).pipe(
        Effect.tapError(failed),
        Effect.withSpan("Reconciliation.sync")
      )

      return Reconciliation.of({ sync, failed, succeeded, status: Ref.get(status) })
    })
  )

export const layerInMemoryScheduler = (
  options: Pick<Options, "definition" | "spaceId" | "retryDelay" | "maximumRetryDelay">
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
      const maximumRetryDelayMillis = yield* Configuration.positiveFiniteDurationMillis(
        "maximumRetryDelay",
        options.maximumRetryDelay ?? Duration.minutes(1)
      )
      if (maximumRetryDelayMillis < retryDelayMillis) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "maximumRetryDelay",
          message: "maximumRetryDelay must be greater than or equal to retryDelay"
        })
      }
      const local = yield* LocalStore.Store
      const reconciliation = yield* Reconciliation
      const remote = yield* SyncEngine.SyncEngine
      const wake = yield* Queue.sliding<void>(1)
      const notify = Queue.offer(wake, undefined).pipe(Effect.asVoid)
      const requestAndNotify = local.requestReconciliation.pipe(Effect.andThen(notify))
      let retryAttempt = 0
      const retryAfterBackoff = Effect.suspend(() => {
        const delay = cappedRetryDelay(retryDelayMillis, maximumRetryDelayMillis, retryAttempt)
        retryAttempt += 1
        return Effect.sleep(delay).pipe(Effect.andThen(notify))
      })
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
              Effect.andThen(reconciliation.succeeded),
              Effect.tap(() =>
                Effect.sync(() => {
                  retryAttempt = 0
                })
              )
            )
          }),
          Effect.catch((error) => {
            if (error._tag === "CredentialRejected") {
              if (error.credentialGeneration === undefined) return reconciliation.failed(error)
              return reconciliation.failed(error).pipe(
                Effect.andThen(remote.waitForCredentialChange(error.credentialGeneration)),
                Effect.tap(() =>
                  Effect.sync(() => {
                    retryAttempt = 0
                  })
                ),
                Effect.andThen(notify)
              )
            }
            if (!isTransientFailure(error)) return reconciliation.failed(error)
            return reconciliation.failed(error).pipe(
              Effect.andThen(Effect.logWarning("Reconciliation failed", error)),
              Effect.andThen(retryAfterBackoff)
            )
          })
        )
      )
      const workerFiber = yield* Effect.forkScoped(worker)
      let watchAttempt = 0
      const watch = (): Effect.Effect<void> =>
        Effect.suspend(() =>
          Stream.unwrap(local.replicationState.pipe(
            Effect.map((state) =>
              remote.watch({
                spaceId: options.spaceId,
                clientId: state.clientId,
                schema: options.definition.schemaIdentity,
                scope: state.scope,
                scopeGeneration: state.scopeGeneration,
                cursor: state.cursor
              })
            )
          )).pipe(
            Stream.runForEach(() => {
              watchAttempt = 0
              return requestAndNotify
            }),
            Effect.matchEffect({
              onFailure: (error) => {
                if (error._tag === "CredentialRejected") {
                  if (error.credentialGeneration === undefined) return reconciliation.failed(error)
                  return reconciliation.failed(error).pipe(
                    Effect.andThen(remote.waitForCredentialChange(error.credentialGeneration)),
                    Effect.tap(() =>
                      Effect.sync(() => {
                        watchAttempt = 0
                      })
                    ),
                    Effect.andThen(watch())
                  )
                }
                if (!isTransientFailure(error)) return reconciliation.failed(error)
                const delay = cappedRetryDelay(retryDelayMillis, maximumRetryDelayMillis, watchAttempt)
                watchAttempt += 1
                return reconciliation.failed(error).pipe(
                  Effect.andThen(Effect.logWarning("Sync watch ended", error)),
                  Effect.andThen(Effect.sleep(delay)),
                  Effect.andThen(watch())
                )
              },
              onSuccess: () => {
                const delay = cappedRetryDelay(retryDelayMillis, maximumRetryDelayMillis, watchAttempt)
                watchAttempt += 1
                return Effect.sleep(delay).pipe(Effect.andThen(watch()))
              }
            })
          )
        )
      const watchFiber = yield* Effect.forkScoped(
        watch()
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
