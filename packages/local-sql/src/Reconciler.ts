import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FiberMap from "effect/FiberMap"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
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
  readonly watchFailed: (error: ReplicaError.ReplicaError) => Effect.Effect<void>
  readonly succeeded: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly status: Effect.Effect<ReplicaStatus.ReplicaStatus>
}

export class Reconciliation extends Context.Service<Reconciliation, ReconciliationService>()(
  "@lucas-barake/effect-local-sql/Reconciliation"
) {}

export interface Service {
  readonly sync: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly notify: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly status: Effect.Effect<ReplicaStatus.ReplicaStatus, ReplicaError.ReplicaError>
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
  readonly onStatusChange?: (status: ReplicaStatus.ReplicaStatus) => Effect.Effect<void>
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
  authenticationEpoch: number
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

export const makeManager = Effect.fnUntraced(function*(options: {
  readonly concurrency?: number
} = {}): Effect.fn.Return<
  ManagerService,
  ReplicaError.InvalidConfiguration,
  SyncEngine.SyncEngine | Scope.Scope
> {
  const remote = yield* SyncEngine.SyncEngine
  const concurrency = options.concurrency ?? 8
  if (!Number.isSafeInteger(concurrency) || concurrency <= 0) {
    return yield* new ReplicaError.InvalidConfiguration({
      option: "reconciliationConcurrency",
      message: "reconciliationConcurrency must be a positive safe integer"
    })
  }
  const queue = yield* Effect.acquireRelease(Queue.unbounded<Work>(), Queue.shutdown)
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
    Effect.uninterruptibleMask(Effect.fnUntraced(function*(restore) {
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
    }))

  const notify = (spaceId: Identity.SpaceId) => lookup(spaceId).pipe(Effect.flatMap(enqueue))

  const admitCredentialPause = Effect.fnUntraced(function*(
    space: ManagedState,
    error: ReplicaError.CredentialRejected
  ) {
    if (error.credentialGeneration === undefined) {
      space.halted = true
      return undefined
    }
    if (space.authenticationGate !== undefined) {
      return { gate: space.authenticationGate, generation: error.credentialGeneration, owner: false }
    }
    const gate = yield* Deferred.make<void>()
    space.authenticationGate = gate
    space.authenticationEpoch += 1
    return { gate, generation: error.credentialGeneration, owner: true }
  })

  const startCredentialWait = Effect.fnUntraced(function*(
    space: ManagedState,
    admission: { readonly gate: Deferred.Deferred<void>; readonly generation: number; readonly owner: boolean }
  ) {
    if (!admission.owner) return
    const gate = admission.gate
    const key = managedKey(space.spaceId, space.generation)
    const finishWait = Effect.gen(function*() {
      const current = spaces.get(space.spaceId)
      if (current !== space || current.authenticationGate !== gate) return
      current.authenticationGate = undefined
      current.retryAttempt = 0
      yield* Deferred.succeed(gate, undefined)
      yield* enqueue(current)
    }).pipe(Effect.uninterruptible)
    yield* FiberMap.run(
      authenticationWaiters,
      key,
      remote.waitForCredentialChange(admission.generation).pipe(
        Effect.andThen(finishWait),
        Effect.catchTags({
          StorageUnavailable: (error) => Effect.die(error),
          StorageCorrupt: (error) => Effect.die(error),
          CanonicalEncodeError: (error) => Effect.die(error),
          SpaceNotJoined: (error) => Effect.die(error),
          DefinitionMismatch: (error) => Effect.die(error),
          StaleSchema: (error) => Effect.die(error),
          SchemaGenerationConflict: (error) => Effect.die(error),
          SchemaEvolutionUnsupported: (error) => Effect.die(error),
          SchemaEvolutionFailed: (error) => Effect.die(error),
          StorageMigrationMismatch: (error) => Effect.die(error),
          SchemaKeyCollision: (error) => Effect.die(error),
          PendingMutationEvolutionRejected: (error) => Effect.die(error),
          ReplicaIdentityMismatch: (error) => Effect.die(error),
          SpaceUnavailable: (error) => Effect.die(error),
          EphemeralSessionUnavailable: (error) => Effect.die(error),
          MutationIdentityConflict: (error) => Effect.die(error),
          QuarantineResubmissionConflict: (error) => Effect.die(error),
          OutOfOrderMutation: (error) => Effect.die(error),
          CursorGap: (error) => Effect.die(error),
          StaleReplicationScope: (error) => Effect.die(error),
          SnapshotUnavailable: (error) => Effect.die(error),
          CapacityExceeded: (error) => Effect.die(error),
          InvalidConfiguration: (error) => Effect.die(error),
          UnknownCommitOutcome: (error) => Effect.die(error),
          ProtocolInvalid: (error) => Effect.die(error),
          UpgradeRequired: (error) => Effect.die(error),
          ProtocolVersionRejected: (error) => Effect.die(error),
          ServerUnavailable: (error) => Effect.die(error),
          CredentialRejected: (error) => Effect.die(error),
          AuthenticatorUnavailable: (error) => Effect.die(error),
          OperationTimeout: (error) => Effect.die(error),
          AuthorizationDenied: (error) => Effect.die(error)
        }),
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.void
          return Effect.failCause(cause)
        })
      )
    )
  })

  const scheduleRetry = Effect.fnUntraced(function*(space: ManagedState) {
    space.retryAttempt += 1
    const delay = Configuration.retryMillis(space, space.retryAttempt)
    space.retrying = true
    const key = managedKey(space.spaceId, space.generation)
    const finishRetry = Effect.gen(function*() {
      const current = spaces.get(space.spaceId)
      if (current !== space) return
      current.retrying = false
      yield* enqueue(current)
    }).pipe(Effect.uninterruptible)
    yield* FiberMap.run(
      retries,
      key,
      Effect.sleep(delay).pipe(
        Effect.andThen(finishRetry),
        Effect.catchTags({
          StorageUnavailable: (error) => Effect.die(error),
          StorageCorrupt: (error) => Effect.die(error),
          CanonicalEncodeError: (error) => Effect.die(error),
          SpaceNotJoined: (error) => Effect.die(error),
          DefinitionMismatch: (error) => Effect.die(error),
          StaleSchema: (error) => Effect.die(error),
          SchemaGenerationConflict: (error) => Effect.die(error),
          SchemaEvolutionUnsupported: (error) => Effect.die(error),
          SchemaEvolutionFailed: (error) => Effect.die(error),
          StorageMigrationMismatch: (error) => Effect.die(error),
          SchemaKeyCollision: (error) => Effect.die(error),
          PendingMutationEvolutionRejected: (error) => Effect.die(error),
          ReplicaIdentityMismatch: (error) => Effect.die(error),
          SpaceUnavailable: (error) => Effect.die(error),
          EphemeralSessionUnavailable: (error) => Effect.die(error),
          MutationIdentityConflict: (error) => Effect.die(error),
          QuarantineResubmissionConflict: (error) => Effect.die(error),
          OutOfOrderMutation: (error) => Effect.die(error),
          CursorGap: (error) => Effect.die(error),
          StaleReplicationScope: (error) => Effect.die(error),
          SnapshotUnavailable: (error) => Effect.die(error),
          CapacityExceeded: (error) => Effect.die(error),
          InvalidConfiguration: (error) => Effect.die(error),
          UnknownCommitOutcome: (error) => Effect.die(error),
          ProtocolInvalid: (error) => Effect.die(error),
          UpgradeRequired: (error) => Effect.die(error),
          ProtocolVersionRejected: (error) => Effect.die(error),
          ServerUnavailable: (error) => Effect.die(error),
          CredentialRejected: (error) => Effect.die(error),
          AuthenticatorUnavailable: (error) => Effect.die(error),
          OperationTimeout: (error) => Effect.die(error),
          AuthorizationDenied: (error) => Effect.die(error)
        }),
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.void
          return Effect.failCause(cause)
        })
      )
    )
  })

  const handleFailure = Effect.fnUntraced(function*(space: ManagedState, error: ReplicaError.ReplicaError) {
    if (error._tag === "CredentialRejected") {
      const admission = yield* admitCredentialPause(space, error)
      yield* space.reconciliation.failed(error)
      if (admission !== undefined) yield* startCredentialWait(space, admission)
      return
    }
    let policy: Effect.Effect<void>
    if (isTransientFailure(error)) {
      policy = scheduleRetry(space)
    } else {
      policy = Effect.sync(() => {
        space.halted = true
      })
    }
    yield* space.reconciliation.failed(error).pipe(Effect.andThen(policy))
  })

  const runTurn = Effect.fnUntraced(function*(space: ManagedState, epoch: number) {
    const turn = Effect.gen(function*() {
      const generations = yield* space.local.reconciliationGenerations
      if (generations.completed >= generations.requested) return
      yield* space.reconciliation.sync
      yield* space.local.completeReconciliation(generations.requested)
      yield* space.reconciliation.succeeded
      space.retryAttempt = 0
    })
    const finishTurn = Effect.gen(function*() {
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
    }).pipe(Effect.uninterruptible)
    yield* turn.pipe(
      Effect.catch((error) => handleFailure(space, error).pipe(Effect.catch(() => Effect.void))),
      Effect.ensuring(finishTurn)
    )
  })

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
  yield* Effect.forEach(Array.from({ length: concurrency }), () => Effect.forkScoped(worker), { discard: true })

  const register = Effect.fnUntraced(
    function*(space: ManagedSpace) {
      const retryTiming = yield* Configuration.retryTiming(space)
      const state: ManagedState = {
        ...space,
        ...retryTiming,
        queued: false,
        running: false,
        retryAttempt: 0,
        retrying: false,
        halted: false,
        authenticationGate: undefined,
        authenticationEpoch: 0,
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
          const watchEpoch = state.authenticationEpoch
          return Stream.unwrap(Effect.map(space.local.replicationState, (replication) =>
            remote.watch({
              spaceId: space.spaceId,
              clientId: replication.clientId,
              schema: space.definition.schemaIdentity,
              scope: replication.scope,
              scopeGeneration: replication.scopeGeneration,
              cursor: replication.cursor
            }))).pipe(
              Stream.runForEach(() => {
                watchAttempt = 0
                return enqueue(state)
              }),
              Effect.matchEffect({
                onSuccess: () => {
                  watchAttempt += 1
                  const delay = Configuration.retryMillis(retryTiming, watchAttempt)
                  return Effect.sleep(delay).pipe(Effect.andThen(watch()))
                },
                onFailure: Effect.fnUntraced(function*(error) {
                  if (watchEpoch !== state.authenticationEpoch) return yield* watch()
                  const activeAuthenticationGate = state.authenticationGate
                  if (activeAuthenticationGate !== undefined && error._tag !== "CredentialRejected") {
                    return yield* Deferred.await(activeAuthenticationGate).pipe(Effect.andThen(watch()))
                  }
                  let policy: Effect.Effect<void>
                  if (error._tag === "CredentialRejected") {
                    const admission = yield* admitCredentialPause(state, error)
                    yield* state.reconciliation.watchFailed(error)
                    if (admission === undefined) return yield* Effect.void
                    yield* startCredentialWait(state, admission)
                    yield* Deferred.await(admission.gate)
                    return yield* watch()
                  } else if (isTransientFailure(error)) {
                    policy = Effect.suspend(() => {
                      watchAttempt += 1
                      const delay = Configuration.retryMillis(retryTiming, watchAttempt)
                      return Effect.sleep(delay).pipe(Effect.andThen(watch()))
                    })
                  } else {
                    policy = Effect.void
                  }
                  return yield* state.reconciliation.watchFailed(error).pipe(Effect.andThen(policy))
                })
              })
            )
        })
      yield* FiberMap.run(watches, managedKey(space.spaceId, space.generation), watch())
      return yield* enqueue(state)
    },
    (effect, space) => effect.pipe(Effect.onError(() => unregister(space.spaceId, space.generation)))
  )

  const unregister = Effect.fnUntraced(function*(spaceId: Identity.SpaceId, generation: number) {
    const current = spaces.get(spaceId)
    if (current?.generation === generation) spaces.delete(spaceId)
    const key = managedKey(spaceId, generation)
    yield* FiberMap.remove(watches, key)
    yield* FiberMap.remove(turns, key)
    yield* FiberMap.remove(retries, key)
    yield* FiberMap.remove(authenticationWaiters, key)
  })

  const sync = (spaceId: Identity.SpaceId) => lookup(spaceId).pipe(Effect.flatMap((space) => space.reconciliation.sync))
  const status = (spaceId: Identity.SpaceId) =>
    lookup(spaceId).pipe(Effect.flatMap((space) => space.reconciliation.status))

  return Manager.of({ register, unregister, sync, notify, status })
})

export const layerManager: Layer.Layer<Manager, ReplicaError.InvalidConfiguration, SyncEngine.SyncEngine> = Layer
  .effect(Manager, makeManager())

export const layerOnePass = (
  options: Pick<Options, "definition" | "spaceId" | "pageSize" | "onStatusChange">
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
        Ref.set(status, value).pipe(
          Effect.andThen(local.invalidateStatus),
          Effect.andThen(options.onStatusChange?.(value) ?? Effect.void)
        )
      const reportFailure = (error: ReplicaError.ReplicaError, preserveConnecting: boolean) =>
        local.pendingCount.pipe(
          Effect.catch(() => Effect.succeed(0)),
          Effect.flatMap((pending) =>
            Ref.modify(
              status,
              (current): readonly [ReplicaStatus.ReplicaStatus | undefined, ReplicaStatus.ReplicaStatus] => {
                if (error._tag === "CredentialRejected") {
                  const next = { _tag: "NeedsAuthentication", pending } as const
                  return [next, next]
                }
                if (current._tag === "NeedsAuthentication") return [undefined, current]
                if (preserveConnecting && current._tag === "Connecting" && isTransientFailure(error)) {
                  return [undefined, current]
                }
                if (
                  error._tag === "AuthenticatorUnavailable" ||
                  error._tag === "ServerUnavailable" ||
                  error._tag === "OperationTimeout"
                ) {
                  const next = { _tag: "Offline", pending } as const
                  return [next, next]
                }
                const next = { _tag: "Failed", pending, message: error._tag } as const
                return [next, next]
              }
            ).pipe(
              Effect.flatMap((next) => {
                if (next === undefined) return Effect.void
                return local.invalidateStatus.pipe(
                  Effect.andThen(options.onStatusChange?.(next) ?? Effect.void)
                )
              })
            )
          )
        )
      const failed = (error: ReplicaError.ReplicaError) => reportFailure(error, false)
      const watchFailed = (error: ReplicaError.ReplicaError) => reportFailure(error, true)
      const succeeded = Effect.gen(function*() {
        if ((yield* Ref.get(status))._tag !== "Connecting") return
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

      const continueBootstrap = Effect.fnUntraced(function*(
        manifest: Protocol.SnapshotManifest,
        initialAfterOrdinal: number
      ): Effect.fn.Return<void, ReplicaError.ReplicaError> {
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

      const bootstrapExpired = Effect.fnUntraced(
        function*(receipt: Protocol.ExpiredReceipt) {
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
        },
        Effect.tapErrorTag("AuthorizationDenied", () => local.revokeReplication)
      )

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

      return Reconciliation.of({ sync, failed, watchFailed, succeeded, status: Ref.get(status) })
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
      const retryTiming = yield* Configuration.retryTiming(options)
      const local = yield* LocalStore.Store
      const reconciliation = yield* Reconciliation
      const remote = yield* SyncEngine.SyncEngine
      const wake = yield* Queue.sliding<void>(1)
      const notify = Queue.offer(wake, undefined).pipe(Effect.asVoid)
      const requestAndNotify = local.requestReconciliation.pipe(Effect.andThen(notify))
      const authenticationPause = yield* Ref.make<Option.Option<Deferred.Deferred<void>>>(Option.none())
      let authenticationEpoch = 0
      const awaitAuthenticationChange = Ref.get(authenticationPause).pipe(
        Effect.flatMap(Option.match({
          onNone: () => Effect.void,
          onSome: Deferred.await
        }))
      )
      const admitCredentialPause = Effect.uninterruptible(Effect.gen(function*() {
        const candidate = yield* Deferred.make<void>()
        const admission = yield* authenticationPause.pipe(
          Ref.modify(Option.match({
            onNone: () => [{ gate: candidate, owner: true }, Option.some(candidate)],
            onSome: (gate) => [{ gate, owner: false }, Option.some(gate)]
          }))
        )
        if (admission.owner) authenticationEpoch += 1
        return admission
      }))
      const startCredentialWait = Effect.fnUntraced(function*(
        generation: number,
        admission: { readonly gate: Deferred.Deferred<void>; readonly owner: boolean }
      ) {
        if (!admission.owner) return
        const finishWait = Effect.gen(function*() {
          const owned = yield* Ref.modify(authenticationPause, (current) => {
            if (Option.isSome(current) && current.value === admission.gate) {
              return [true, Option.none()] as const
            }
            return [false, current] as const
          })
          if (owned) yield* Deferred.succeed(admission.gate, undefined)
        }).pipe(Effect.uninterruptible)
        yield* remote.waitForCredentialChange(generation).pipe(
          Effect.andThen(finishWait),
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) return Effect.void
            return Effect.failCause(cause)
          }),
          Effect.forkScoped,
          Effect.asVoid
        )
      })
      let retryAttempt = 0
      const retryAfterBackoff = Effect.suspend(() => {
        retryAttempt += 1
        const delay = Configuration.retryMillis(retryTiming, retryAttempt)
        return Effect.sleep(delay).pipe(Effect.andThen(notify))
      })
      const worker = Effect.andThen(Queue.take(wake), awaitAuthenticationChange).pipe(
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
        Effect.catch(Effect.fnUntraced(function*(error) {
          if (error._tag === "CredentialRejected") {
            if (error.credentialGeneration === undefined) return yield* reconciliation.failed(error)
            const admission = yield* admitCredentialPause
            yield* reconciliation.failed(error)
            yield* startCredentialWait(error.credentialGeneration, admission)
            yield* Deferred.await(admission.gate)
            retryAttempt = 0
            return yield* notify
          }
          const pause = yield* Ref.get(authenticationPause)
          if (Option.isSome(pause)) {
            yield* Deferred.await(pause.value)
            return yield* notify
          }
          if (!isTransientFailure(error)) return yield* reconciliation.failed(error)
          return yield* reconciliation.failed(error).pipe(
            Effect.andThen(Effect.logWarning("Reconciliation failed", error)),
            Effect.andThen(retryAfterBackoff)
          )
        })),
        Effect.forever()
      )
      const workerFiber = yield* Effect.forkScoped(worker)
      let watchAttempt = 0
      const watch = (): Effect.Effect<void, never, Scope.Scope> =>
        Effect.suspend(() => {
          const watchEpoch = authenticationEpoch
          return awaitAuthenticationChange.pipe(Effect.andThen(
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
                onFailure: Effect.fnUntraced(function*(error) {
                  if (watchEpoch !== authenticationEpoch) return yield* watch()
                  if (error._tag === "CredentialRejected") {
                    if (error.credentialGeneration === undefined) return yield* reconciliation.failed(error)
                    const admission = yield* admitCredentialPause
                    yield* reconciliation.watchFailed(error)
                    yield* startCredentialWait(error.credentialGeneration, admission)
                    yield* Deferred.await(admission.gate)
                    watchAttempt = 0
                    return yield* watch()
                  }
                  const pause = yield* Ref.get(authenticationPause)
                  if (Option.isSome(pause)) {
                    yield* Deferred.await(pause.value)
                    return yield* watch()
                  }
                  if (!isTransientFailure(error)) return yield* reconciliation.watchFailed(error)
                  watchAttempt += 1
                  const delay = Configuration.retryMillis(retryTiming, watchAttempt)
                  return yield* reconciliation.watchFailed(error).pipe(
                    Effect.andThen(Effect.logWarning("Sync watch ended", error)),
                    Effect.andThen(Effect.sleep(delay)),
                    Effect.andThen(watch())
                  )
                }),
                onSuccess: () => {
                  watchAttempt += 1
                  const delay = Configuration.retryMillis(retryTiming, watchAttempt)
                  return Effect.sleep(delay).pipe(Effect.andThen(watch()))
                }
              })
            )
          ))
        })
      const watchFiber = yield* Effect.forkScoped(watch())
      yield* requestAndNotify
      yield* Effect.addFinalizer(() => {
        return Fiber.interruptAll([workerFiber, watchFiber]).pipe(
          Effect.andThen(Queue.shutdown(wake)),
          Effect.asVoid
        )
      })

      return Reconciler.of({
        sync: reconciliation.sync,
        notify,
        status: reconciliation.status,
        shutdown: Effect.void
      })
    })
  )

export const layer = (options: Options) => {
  return layerInMemoryScheduler(options).pipe(Layer.provideMerge(layerOnePass(options)))
}
