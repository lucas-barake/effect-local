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
import * as ConnectionLane from "./ConnectionLane.js"
import * as Configuration from "./internal/configuration.js"
import * as Errors from "./internal/errors.js"
import * as LosslessQueue from "./internal/losslessQueue.js"
import { backoff, credentialChange } from "./internal/transport.js"
import * as LocalStore from "./LocalStore.js"
import * as SyncEngine from "./SyncEngine.js"

export interface ReconciliationService {
  readonly sync: Effect.Effect<void, ReplicaError.ReplicaError>
  readonly generation: Effect.Effect<number>
  readonly failed: (error: ReplicaError.ReplicaError, observedGeneration: number) => Effect.Effect<void>
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
  readonly schedule: Effect.Effect<void, ReplicaError.ReplicaError>
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
  readonly onReconciled?: Effect.Effect<void>
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
  readonly schedule: (spaceId: Identity.SpaceId) => Effect.Effect<void, ReplicaError.ReplicaError>
  readonly status: (
    spaceId: Identity.SpaceId
  ) => Effect.Effect<ReplicaStatus.ReplicaStatus, ReplicaError.ReplicaError>
}

export class Manager extends Context.Service<Manager, ManagerService>()(
  "@lucas-barake/effect-local-sql/Reconciler/Manager"
) {}

interface ManagedState extends ManagedSpace {
  readonly requests: ReturnType<typeof makeReconciliationRequests>
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

const makeReconciliationRequests = (request: Effect.Effect<number, ReplicaError.ReplicaError>) => {
  let pending: Deferred.Deferred<boolean> | undefined
  const run: Effect.Effect<void, ReplicaError.ReplicaError> = Effect.suspend(() => {
    const shared = pending
    if (shared !== undefined) {
      return Deferred.await(shared).pipe(Effect.flatMap((written) => {
        if (written) return Effect.void
        return run
      }))
    }
    const own = Deferred.makeUnsafe<boolean>()
    pending = own
    return request.pipe(
      Effect.onExit((exit) => {
        if (exit._tag === "Failure" && pending === own) pending = undefined
        return Deferred.succeed(own, exit._tag === "Success")
      }),
      Effect.asVoid
    )
  })
  const observe = Effect.sync(() => {
    pending = undefined
  })
  return { run, observe }
}

type FailureClass = "Unreachable" | "Retryable" | "NeedsCredential" | "Terminal"

const capacityClasses: { readonly [Resource in ReplicaError.CapacityResource]: "Retryable" | "Terminal" } = {
  "read authorizations": "Retryable",
  "sync watchers": "Retryable",
  "sync watchers per principal": "Retryable",
  "server receipts": "Retryable",
  "server history": "Retryable",
  "bootstrap authorizations": "Retryable",
  "bootstrap pages": "Retryable",
  "ephemeral join verifications": "Retryable",
  "ephemeral watchers": "Retryable",
  "ephemeral watchers per principal": "Retryable",
  "ephemeral spaces": "Retryable",
  "ephemeral members": "Retryable",
  "ephemeral bytes per space": "Retryable",
  "ephemeral event keys per space": "Retryable",
  "ephemeral state keys per space": "Retryable",
  "ephemeral events": "Retryable",
  "pending mutations": "Retryable",
  "client receipts": "Terminal",
  "bootstrap entries": "Terminal",
  "bootstrap bytes": "Terminal",
  "bootstrap page bytes": "Terminal",
  "bootstrap entity bytes": "Terminal",
  "snapshot entities": "Terminal",
  "snapshot bytes": "Terminal",
  "scoped snapshot bytes": "Terminal",
  "replication page bytes": "Terminal",
  "mutation bytes": "Terminal",
  "receipt bytes": "Terminal",
  "mutation submission attempts": "Terminal",
  "schema evolution row bytes": "Terminal",
  "schema generations": "Terminal",
  "replication scope generations": "Terminal",
  "projection generation": "Terminal",
  "reconciliation generations": "Terminal",
  "local sequence": "Terminal",
  "terminal sequence": "Terminal",
  "server sequence": "Terminal",
  "ephemeral payload bytes": "Terminal",
  "ephemeral snapshot bytes": "Terminal",
  "ephemeral bytes per member": "Terminal",
  "ephemeral event keys per member": "Terminal",
  "ephemeral state keys per member": "Terminal"
}

const failureClasses: {
  readonly [Tag in Exclude<ReplicaError.ReplicaError["_tag"], "CapacityExceeded">]: FailureClass
} = {
  ServerUnavailable: "Unreachable",
  OperationTimeout: "Unreachable",
  AuthenticatorUnavailable: "Unreachable",
  StorageUnavailable: "Retryable",
  UnknownCommitOutcome: "Retryable",
  OwnerUnavailable: "Retryable",
  UnexpectedFailure: "Retryable",
  CredentialRejected: "NeedsCredential",
  StorageCorrupt: "Terminal",
  CanonicalEncodeError: "Terminal",
  DefinitionMismatch: "Terminal",
  StaleSchema: "Terminal",
  SchemaGenerationConflict: "Terminal",
  SchemaEvolutionUnsupported: "Terminal",
  SchemaEvolutionFailed: "Terminal",
  StorageMigrationMismatch: "Terminal",
  StorageMigrationPending: "Terminal",
  SchemaKeyCollision: "Terminal",
  PendingMutationEvolutionRejected: "Terminal",
  ReplicaIdentityMismatch: "Terminal",
  SpaceNotJoined: "Terminal",
  SpaceUnavailable: "Terminal",
  EphemeralSessionUnavailable: "Terminal",
  MutationIdentityConflict: "Terminal",
  QuarantineResubmissionConflict: "Terminal",
  OutOfOrderMutation: "Terminal",
  CursorGap: "Terminal",
  SettlementReplayTruncated: "Terminal",
  StaleReplicationScope: "Terminal",
  InvalidConfiguration: "Terminal",
  ProtocolInvalid: "Terminal",
  UpgradeRequired: "Terminal",
  ProtocolVersionRejected: "Terminal",
  AuthorizationDenied: "Terminal",
  BuildSuperseded: "Terminal"
}

const failureClass = (error: ReplicaError.ReplicaError): FailureClass => {
  if (error._tag === "CapacityExceeded") return capacityClasses[error.resource]
  return failureClasses[error._tag]
}

export const isTransientFailure = (error: ReplicaError.ReplicaError) => {
  const classified = failureClass(error)
  return classified === "Unreachable" || classified === "Retryable"
}

export const failureStatus = (error: ReplicaError.ReplicaError, pending: number): ReplicaStatus.ReplicaStatus => {
  const classified = failureClass(error)
  if (classified === "NeedsCredential") return { _tag: "NeedsAuthentication", pending }
  if (classified === "Unreachable") return { _tag: "Offline", pending }
  return { _tag: "Failed", pending, message: error._tag }
}

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

  const admit = (space: ManagedState) =>
    Effect.uninterruptible(Effect.suspend(() => {
      const admitted = spaces.get(space.spaceId)
      if (admitted !== space) return Effect.fail(new ReplicaError.SpaceNotJoined({ spaceId: space.spaceId }))
      admitted.dirtyEpoch += 1
      admitted.halted = false
      if (
        admitted.queued ||
        admitted.running ||
        admitted.retrying ||
        admitted.authenticationGate !== undefined
      ) return Effect.void
      admitted.queued = true
      return Queue.offer(queue, { spaceId: admitted.spaceId, generation: admitted.generation }).pipe(Effect.asVoid)
    }))

  const enqueue = (space: ManagedState) =>
    Effect.uninterruptibleMask(Effect.fnUntraced(function*(restore) {
      const current = spaces.get(space.spaceId)
      if (current !== space) {
        return yield* new ReplicaError.SpaceNotJoined({ spaceId: space.spaceId })
      }
      yield* restore(current.requests.run)
      return yield* admit(space)
    }))

  const notify = (spaceId: Identity.SpaceId) => lookup(spaceId).pipe(Effect.flatMap(enqueue))

  const schedule = (spaceId: Identity.SpaceId) => lookup(spaceId).pipe(Effect.flatMap(admit))

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
      yield* readmit(current)
    }).pipe(Effect.uninterruptible)
    yield* FiberMap.run(
      authenticationWaiters,
      key,
      credentialChange(remote, admission.generation, space.maximumRetryDelayMillis).pipe(
        Effect.annotateLogs({ "space.id": space.spaceId }),
        Effect.andThen(finishWait),
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.void
          return Effect.failCause(cause)
        })
      )
    )
  })

  const scheduleRetry = Effect.fnUntraced(function*(
    space: ManagedState,
    failure: ReplicaError.ReplicaError,
    transportGeneration: number
  ) {
    space.retryAttempt += 1
    const delay = Configuration.retryMillis(space, space.retryAttempt)
    space.retrying = true
    const key = managedKey(space.spaceId, space.generation)
    const finishRetry = Effect.gen(function*() {
      const current = spaces.get(space.spaceId)
      if (current !== space) return
      current.retrying = false
      yield* readmit(current)
    }).pipe(Effect.uninterruptible)
    yield* FiberMap.run(
      retries,
      key,
      backoff(remote, delay, failure, transportGeneration).pipe(
        Effect.catchCause((cause) =>
          Errors.logDefect("Retry backoff died", cause).pipe(
            Effect.annotateLogs({ "space.id": space.spaceId }),
            Effect.andThen(Effect.sleep(delay))
          )
        ),
        Effect.andThen(finishRetry),
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.void
          return Effect.failCause(cause)
        })
      )
    )
  })

  const handleFailure = Effect.fnUntraced(function*(
    space: ManagedState,
    error: ReplicaError.ReplicaError,
    transportGeneration: number,
    observedGeneration: number
  ) {
    if (error._tag === "CredentialRejected") {
      const admission = yield* admitCredentialPause(space, error)
      yield* space.reconciliation.failed(error, observedGeneration)
      if (admission !== undefined) yield* startCredentialWait(space, admission)
      return
    }
    let policy: Effect.Effect<void>
    if (isTransientFailure(error)) {
      policy = scheduleRetry(space, error, transportGeneration)
    } else {
      policy = Effect.sync(() => {
        space.halted = true
      })
    }
    yield* space.reconciliation.failed(error, observedGeneration).pipe(Effect.andThen(policy))
  })

  const readmit = (space: ManagedState): Effect.Effect<void> =>
    enqueue(space).pipe(
      Effect.catchCause((cause) => {
        if (Errors.causeKind(cause) === "Failure") return Effect.failCause(cause)
        const failure = Errors.iterationFailure("Reconciliation readmission died", cause)
        return Errors.logDefect("Reconciliation readmission died", cause).pipe(
          Effect.annotateLogs({ "space.id": space.spaceId }),
          Effect.andThen(Effect.fail(failure))
        )
      }),
      Effect.catch(Effect.fnUntraced(function*(error) {
        const transportGeneration = yield* remote.transportGeneration
        const observedGeneration = yield* space.reconciliation.generation
        yield* handleFailure(space, error, transportGeneration, observedGeneration).pipe(
          Effect.catch(() => Effect.void)
        )
      })),
      Effect.catchCause((cause) =>
        Errors.logDefect("Reconciliation failure handling died", cause).pipe(
          Effect.annotateLogs({ "space.id": space.spaceId })
        )
      )
    )

  const runTurn = (space: ManagedState, epoch: number): Effect.Effect<void> => {
    let transportGeneration = 0
    let observedGeneration = 0
    const turn = Effect.gen(function*() {
      transportGeneration = yield* remote.transportGeneration
      observedGeneration = yield* space.reconciliation.generation
      yield* space.requests.observe
      const generations = yield* space.local.reconciliationGenerations
      if (generations.completed >= generations.requested) return
      yield* space.reconciliation.sync
      observedGeneration = yield* space.reconciliation.generation
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
    return turn.pipe(
      Effect.catchCause((cause) => {
        if (Errors.causeKind(cause) === "Failure") return Effect.failCause(cause)
        const failure = Errors.iterationFailure("Reconciliation turn died", cause)
        return Errors.logDefect("Reconciliation turn died", cause).pipe(
          Effect.annotateLogs({ "space.id": space.spaceId }),
          Effect.andThen(space.reconciliation.generation),
          Effect.map((generation) => {
            observedGeneration = generation
          }),
          Effect.andThen(Effect.fail(failure))
        )
      }),
      Effect.catch((error) =>
        handleFailure(space, error, transportGeneration, observedGeneration).pipe(Effect.catch(() => Effect.void))
      ),
      Effect.catchCause((cause) =>
        Errors.logDefect("Reconciliation failure handling died", cause).pipe(
          Effect.annotateLogs({ "space.id": space.spaceId })
        )
      ),
      Effect.ensuring(finishTurn)
    )
  }

  const selectWork = (work: Work) => {
    const current = spaces.get(work.spaceId)
    if (current === undefined || current.generation !== work.generation || !current.queued) return undefined
    current.queued = false
    if (current.running) return undefined
    current.running = true
    return { space: current, epoch: current.dirtyEpoch }
  }

  const worker = Effect.forever(Effect.gen(function*() {
    const work = yield* LosslessQueue.take(queue)
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
    () => Effect.forkScoped(Effect.provideService(worker, ConnectionLane.Priority, "Background")),
    { discard: true }
  )

  const register = Effect.fnUntraced(
    function*(space: ManagedSpace) {
      const retryTiming = yield* Configuration.retryTiming(space)
      const state: ManagedState = {
        ...space,
        ...retryTiming,
        requests: makeReconciliationRequests(space.local.requestReconciliation),
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
      const watchBackoff = Configuration.makeWatchBackoff(retryTiming)
      const watch = (): Effect.Effect<void> =>
        Effect.suspend(() => {
          const authenticationGate = state.authenticationGate
          if (authenticationGate !== undefined) {
            return Deferred.await(authenticationGate).pipe(Effect.andThen(watch()))
          }
          const watchEpoch = state.authenticationEpoch
          const subscribed = Effect.andThen(watchBackoff.opened, remote.transportGeneration)
          return subscribed.pipe(Effect.flatMap((transportGeneration) =>
            Stream.unwrap(Effect.map(space.local.replicationState, (replication) =>
              remote.watch({
                spaceId: space.spaceId,
                clientId: replication.clientId,
                schema: space.definition.schemaIdentity,
                scope: replication.scope,
                scopeGeneration: replication.scopeGeneration,
                cursor: replication.cursor
              }))).pipe(
                Stream.runForEach(() => enqueue(state)),
                Effect.matchEffect({
                  onSuccess: () => watchBackoff.closed.pipe(Effect.flatMap(Effect.sleep), Effect.andThen(watch())),
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
                      policy = watchBackoff.closed.pipe(
                        Effect.flatMap((delay) => backoff(remote, delay, error, transportGeneration)),
                        Effect.andThen(readmit(state)),
                        Effect.andThen(watch())
                      )
                    } else {
                      policy = Effect.void
                    }
                    return yield* state.reconciliation.watchFailed(error).pipe(Effect.andThen(policy))
                  })
                })
              )
          ))
        })
      const superviseWatch = (): Effect.Effect<void> =>
        watch().pipe(
          Effect.catchCause((cause) => {
            const resubscribed = watchBackoff.closed.pipe(Effect.flatMap(Effect.sleep))
            if (Errors.causeKind(cause) !== "Defect") return Effect.andThen(resubscribed, superviseWatch())
            const died = Errors.unexpectedFailure("Sync watch died", cause)
            return Effect.logError("Sync watch died", cause).pipe(
              Effect.annotateLogs({ "space.id": space.spaceId }),
              Effect.andThen(state.reconciliation.watchFailed(died)),
              Effect.andThen(resubscribed),
              Effect.andThen(readmit(state)),
              Effect.andThen(superviseWatch())
            )
          })
        )
      yield* FiberMap.run(
        watches,
        managedKey(space.spaceId, space.generation),
        superviseWatch().pipe(Effect.provideService(ConnectionLane.Priority, "Background"))
      )
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

  return Manager.of({ register, unregister, sync, notify, schedule, status })
})

export const layerManager: Layer.Layer<Manager, ReplicaError.InvalidConfiguration, SyncEngine.SyncEngine> = Layer
  .effect(Manager, makeManager())

export const layerOnePass = (
  options: Pick<Options, "definition" | "spaceId" | "pageSize" | "onStatusChange" | "onReconciled">
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
      const status = yield* Ref.make<ReplicaStatus.ReplicaStatus>({ _tag: "Connecting", pending: 0 })
      let syncAttempted = false
      let syncing = false
      let failedSinceSyncStarted = false
      let syncGeneration = 0
      const updateAvailable = yield* Ref.make<Identity.SchemaIdentity | undefined>(undefined)
      const setStatus = (value: ReplicaStatus.ReplicaStatus) =>
        Ref.set(status, value).pipe(
          Effect.andThen(local.invalidateStatus),
          Effect.andThen(options.onStatusChange?.(value) ?? Effect.void)
        )
      const reportFailure = (
        error: ReplicaError.ReplicaError,
        preserveConnecting: boolean,
        observedGeneration: number
      ) =>
        local.pendingCount.pipe(
          Effect.catch(() => Effect.succeed(0)),
          Effect.catchCause((cause) =>
            Errors.logDefect("Pending count for a failure report died", cause).pipe(
              Effect.annotateLogs({ "space.id": options.spaceId }),
              Effect.as(0)
            )
          ),
          Effect.flatMap((pending) =>
            Ref.modify(
              status,
              (current): readonly [ReplicaStatus.ReplicaStatus | undefined, ReplicaStatus.ReplicaStatus] => {
                if (syncGeneration > observedGeneration) return [undefined, current]
                const next = failureStatus(error, pending)
                if (next._tag === "NeedsAuthentication") return [next, next]
                if (current._tag === "NeedsAuthentication" && failedSinceSyncStarted) return [undefined, current]
                if (preserveConnecting && (current._tag === "Connecting" || syncing) && next._tag === "Offline") {
                  return [undefined, current]
                }
                return [next, next]
              }
            ).pipe(
              Effect.flatMap((next) => {
                if (next === undefined) return Effect.void
                failedSinceSyncStarted = true
                return local.invalidateStatus.pipe(
                  Effect.andThen(options.onStatusChange?.(next) ?? Effect.void)
                )
              })
            )
          )
        )
      const failed = (error: ReplicaError.ReplicaError, observedGeneration: number) =>
        reportFailure(error, false, observedGeneration)
      const watchFailed = (error: ReplicaError.ReplicaError) =>
        Effect.suspend(() => {
          if (error._tag !== "AuthorizationDenied") return reportFailure(error, true, Number.POSITIVE_INFINITY)
          return gate.withPermit(
            local.revokeReplication.pipe(
              Effect.matchEffect({
                onFailure: (revokeError) => reportFailure(revokeError, false, Number.POSITIVE_INFINITY),
                onSuccess: () => reportFailure(error, true, Number.POSITIVE_INFINITY)
              })
            )
          )
        })
      const succeeded = Effect.gen(function*() {
        if (!syncAttempted || failedSinceSyncStarted) return
        const { cursor, pending } = yield* local.progress
        const serverSchema = yield* Ref.get(updateAvailable)
        const current = yield* Ref.get(status)
        if (serverSchema !== undefined) {
          if (
            current._tag === "SchemaUpdateAvailable" && current.pending === pending && current.cursor === cursor &&
            current.serverSchema.version === serverSchema.version && current.serverSchema.hash === serverSchema.hash
          ) {
            yield* options.onStatusChange?.(current) ?? Effect.void
            return
          }
          yield* setStatus({ _tag: "SchemaUpdateAvailable", pending, cursor, serverSchema })
        } else {
          if (current._tag === "Online" && current.pending === pending && current.cursor === cursor) {
            yield* options.onStatusChange?.(current) ?? Effect.void
            return
          }
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
            membershipIncarnation: local.membershipIncarnation,
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
            membershipIncarnation: local.membershipIncarnation,
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
            membershipIncarnation: local.membershipIncarnation,
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

      const validateBatchReceipts = (
        envelopes: ReadonlyArray<Protocol.MutationEnvelope>,
        receipts: ReadonlyArray<Protocol.Receipt>
      ) => {
        if (receipts.length === 0 || receipts.length > envelopes.length) {
          return Effect.fail(
            new ReplicaError.ProtocolInvalid({
              message: `SubmitBatch returned ${receipts.length} receipts for ${envelopes.length} mutations`
            })
          )
        }
        for (let index = 0; index < receipts.length; index++) {
          if (receipts[index].mutationId !== envelopes[index].mutationId) {
            return Effect.fail(
              new ReplicaError.ProtocolInvalid({
                message: `SubmitBatch receipt ${index} does not belong to mutation ${envelopes[index].mutationId}`
              })
            )
          }
        }
        return Effect.void
      }

      const submitPending = Effect.gen(function*() {
        while (true) {
          let installedExpiredSnapshot = false
          let after = 0
          let through: number | undefined
          let more = true
          while (more && !installedExpiredSnapshot) {
            const claim = yield* local.claimSubmitBatch({ after, through })
            through = claim.through
            const envelopes = claim.envelopes
            if (envelopes.length === 0) break
            const mutationIds = envelopes.map((envelope) => envelope.mutationId)
            const receipts = yield* Effect.gen(function*() {
              const result = yield* remote.submitBatch({ envelopes, schema: options.definition.schemaIdentity })
              yield* validateBatchReceipts(envelopes, result.receipts)
              yield* local.persistReceipts(result.receipts)
              if (result.receipts.length < mutationIds.length) {
                yield* local.markRetrying(mutationIds.slice(result.receipts.length))
              }
              return result.receipts
            }).pipe(Effect.tapError(() => local.markRetrying(mutationIds)))
            after = envelopes[receipts.length - 1].localSequence
            more = claim.more || receipts.length < envelopes.length
            for (const receipt of receipts) {
              if (receipt._tag !== "Expired") continue
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

      const sync = gate.withPermit(
        Effect.gen(function*() {
          syncGeneration += 1
          syncAttempted = true
          syncing = true
          failedSinceSyncStarted = false
          yield* catchUp
          yield* submitPending
          yield* catchUp
          syncing = false
          yield* succeeded
          yield* options.onReconciled ?? Effect.void
        }).pipe(
          Effect.ensuring(Effect.sync(() => {
            syncing = false
          })),
          Effect.tapError((error) => reportFailure(error, false, syncGeneration))
        )
      ).pipe(Effect.withSpan("Reconciliation.sync"))

      return Reconciliation.of({
        sync,
        generation: Effect.sync(() => syncGeneration),
        failed,
        watchFailed,
        succeeded,
        status: Ref.get(status)
      })
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
      const requests = makeReconciliationRequests(local.requestReconciliation)
      const requestAndNotify = requests.run.pipe(Effect.andThen(notify))
      const resyncAfterWatchFailure = requestAndNotify.pipe(
        Effect.catch((error) => reconciliation.watchFailed(error))
      )
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
        yield* credentialChange(remote, generation, retryTiming.maximumRetryDelayMillis).pipe(
          Effect.annotateLogs({ "space.id": options.spaceId }),
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
      const retryAfterBackoff = (error: ReplicaError.ReplicaError, transportGeneration: number) =>
        Effect.suspend(() => {
          retryAttempt += 1
          const delay = Configuration.retryMillis(retryTiming, retryAttempt)
          return backoff(remote, delay, error, transportGeneration).pipe(
            Effect.catchCause((cause) =>
              Errors.logDefect("Retry backoff died", cause).pipe(
                Effect.annotateLogs({ "space.id": options.spaceId }),
                Effect.andThen(Effect.sleep(delay))
              )
            ),
            Effect.andThen(notify)
          )
        })
      let turnTransportGeneration = 0
      let observedGeneration = 0
      const turn = Effect.gen(function*() {
        turnTransportGeneration = yield* remote.transportGeneration
        observedGeneration = yield* reconciliation.generation
        yield* requests.observe
        const generations = yield* local.reconciliationGenerations
        if (generations.completed >= generations.requested) return
        const exit = yield* reconciliation.sync.pipe(
          Effect.forkChild({ startImmediately: true }),
          Effect.flatMap(Fiber.await)
        )
        if (exit._tag === "Failure" && Cause.hasInterruptsOnly(exit.cause)) {
          observedGeneration = yield* reconciliation.generation
          yield* new ReplicaError.ServerUnavailable()
        }
        yield* exit
        observedGeneration = yield* reconciliation.generation
        yield* local.completeReconciliation(generations.requested)
        yield* reconciliation.succeeded
        retryAttempt = 0
      }).pipe(
        Effect.catchCause((cause) => {
          if (Errors.causeKind(cause) === "Failure") return Effect.failCause(cause)
          const failure = Errors.iterationFailure("Reconciliation turn died", cause)
          return Errors.logDefect("Reconciliation turn died", cause).pipe(
            Effect.annotateLogs({ "space.id": options.spaceId }),
            Effect.andThen(reconciliation.generation),
            Effect.map((generation) => {
              observedGeneration = generation
            }),
            Effect.andThen(Effect.fail(failure))
          )
        }),
        Effect.catch(Effect.fnUntraced(function*(error) {
          if (error._tag === "CredentialRejected") {
            if (error.credentialGeneration === undefined) {
              return yield* reconciliation.failed(error, observedGeneration)
            }
            const admission = yield* admitCredentialPause
            yield* reconciliation.failed(error, observedGeneration)
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
          if (!isTransientFailure(error)) return yield* reconciliation.failed(error, observedGeneration)
          return yield* reconciliation.failed(error, observedGeneration).pipe(
            Effect.andThen(Effect.logWarning("Reconciliation failed", error)),
            Effect.andThen(retryAfterBackoff(error, turnTransportGeneration))
          )
        }))
      )
      const worker = Effect.andThen(LosslessQueue.take(wake), awaitAuthenticationChange).pipe(
        Effect.andThen(turn),
        Effect.catchCause((cause) => {
          if (Errors.causeKind(cause) !== "Defect") return Effect.void
          return Effect.logError("Reconciliation failure handling died", cause).pipe(
            Effect.annotateLogs({ "space.id": options.spaceId }),
            Effect.andThen(reconciliation.generation),
            Effect.flatMap((generation) =>
              reconciliation.failed(
                Errors.unexpectedFailure("Reconciliation failure handling died", cause),
                generation
              )
            )
          )
        }),
        Effect.forever()
      )
      const workerFiber = yield* Effect.forkScoped(Effect.provideService(worker, ConnectionLane.Priority, "Background"))
      const watchBackoff = Configuration.makeWatchBackoff(retryTiming)
      const watch = (): Effect.Effect<void, never, Scope.Scope> =>
        Effect.suspend(() => {
          const watchEpoch = authenticationEpoch
          return awaitAuthenticationChange.pipe(
            Effect.andThen(watchBackoff.opened),
            Effect.andThen(remote.transportGeneration),
            Effect.flatMap((transportGeneration) =>
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
                Stream.runForEach(() => requestAndNotify),
                Effect.matchEffect({
                  onFailure: Effect.fnUntraced(function*(error) {
                    if (watchEpoch !== authenticationEpoch) return yield* watch()
                    if (error._tag === "CredentialRejected") {
                      if (error.credentialGeneration === undefined) return yield* reconciliation.watchFailed(error)
                      const admission = yield* admitCredentialPause
                      yield* reconciliation.watchFailed(error)
                      yield* startCredentialWait(error.credentialGeneration, admission)
                      yield* Deferred.await(admission.gate)
                      yield* watchBackoff.reset
                      return yield* watch()
                    }
                    const pause = yield* Ref.get(authenticationPause)
                    if (Option.isSome(pause)) {
                      yield* Deferred.await(pause.value)
                      return yield* watch()
                    }
                    if (!isTransientFailure(error)) return yield* reconciliation.watchFailed(error)
                    const delay = yield* watchBackoff.closed
                    return yield* reconciliation.watchFailed(error).pipe(
                      Effect.andThen(Effect.logWarning("Sync watch ended", error)),
                      Effect.andThen(backoff(remote, delay, error, transportGeneration)),
                      Effect.andThen(resyncAfterWatchFailure),
                      Effect.andThen(watch())
                    )
                  }),
                  onSuccess: () => watchBackoff.closed.pipe(Effect.flatMap(Effect.sleep), Effect.andThen(watch()))
                })
              )
            )
          )
        })
      const superviseWatch = (): Effect.Effect<void, never, Scope.Scope> =>
        watch().pipe(
          Effect.catchCause((cause) => {
            const resubscribed = watchBackoff.closed.pipe(Effect.flatMap(Effect.sleep))
            if (Errors.causeKind(cause) !== "Defect") return Effect.andThen(resubscribed, superviseWatch())
            const died = Errors.unexpectedFailure("Sync watch died", cause)
            return Effect.logError("Sync watch died", cause).pipe(
              Effect.annotateLogs({ "space.id": options.spaceId }),
              Effect.andThen(reconciliation.watchFailed(died)),
              Effect.andThen(resubscribed),
              Effect.andThen(resyncAfterWatchFailure),
              Effect.andThen(superviseWatch())
            )
          })
        )
      const watchFiber = yield* superviseWatch().pipe(
        Effect.provideService(ConnectionLane.Priority, "Background"),
        Effect.forkScoped
      )
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
        schedule: notify,
        status: reconciliation.status,
        shutdown: Effect.void
      })
    })
  )

export const layer = (options: Options) => {
  return layerInMemoryScheduler(options).pipe(Layer.provideMerge(layerOnePass(options)))
}
