import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Quarantine from "@lucas-barake/effect-local/Quarantine"
import * as ReactivityKey from "@lucas-barake/effect-local/ReactivityKey"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import type * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as Codec from "./internal/codec.js"
import * as Configuration from "./internal/configuration.js"
import * as MutationDescriptor from "./internal/mutationDescriptor.js"
import * as Rows from "./internal/rows.js"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as LocalStore from "./LocalStore.js"
import * as Migrations from "./Migrations.js"
import * as MutationRuntime from "./MutationRuntime.js"
import * as QueryExecutor from "./QueryExecutor.js"
import * as QueryReactivity from "./QueryReactivity.js"
import * as Reconciler from "./Reconciler.js"
import * as ReconciliationWorkflow from "./ReconciliationWorkflow.js"
import * as SyncEngine from "./SyncEngine.js"

export interface Options<D extends Definition.Any,> {
  readonly definition: D
  readonly clientId: Identity.ClientId
  readonly defaultScope: Protocol.ReplicationScope
  readonly maximumActiveSpaces: number
  readonly foregroundActiveSpaces: number
  readonly initialSpaces?: Iterable<Identity.SpaceId>
  readonly spaceId?: Identity.SpaceId
  readonly maximumPendingMutations?: number
  readonly evolution?: Evolution.Evolution
  readonly schemaEvolutionBatchSize?: number
  readonly schemaEvolutionBatchBytes?: number
  readonly retainedReceipts: number
  readonly maximumReceipts: number
  readonly retainedHistoryEntries: number
  readonly maximumBootstrapEntities: number
  readonly maximumBootstrapBytes: number
  readonly maximumBootstrapPageBytes: number
  readonly migration: Migrations.Options
  readonly pageSize?: number
  readonly reconciliationConcurrency?: number
  readonly foregroundReconciliationConcurrency?: number
  readonly retryDelay?: Duration.Input
  readonly maximumRetryDelay?: Duration.Input
  readonly maximumAttempts?: number
}

type BaseRequirements<D extends Definition.Any,> =
  | SqlClient.SqlClient
  | Crypto.Crypto
  | Reactivity.Reactivity
  | SyncEngine.SyncEngine
  | MutationRuntime.Handlers<D>
  | QueryExecutor.Handlers<D>

interface ActiveRuntime {
  readonly foreground: boolean
  readonly scope: Scope.Closeable
  readonly operationGate: Semaphore.Semaphore
  readonly local: LocalStore.Service
  readonly queries: QueryExecutor.Service
  readonly reconciler: Reconciler.Service
  readonly reconciliation: Reconciler.ReconciliationService
  readonly cancelReconciliation: Effect.Effect<void>
}

interface RememberedEntry {
  readonly spaceId: Identity.SpaceId
  handle: Replica.Space
  replicationScope: Protocol.ReplicationScope
  activation: Replica.Activation
  runtime: ActiveRuntime | undefined
  transition: Deferred.Deferred<void, ReplicaError.ReplicaError> | undefined
  foreground: boolean
  leases: number
  leaving: boolean
  leaveCompletion: Deferred.Deferred<void, ReplicaError.ReplicaError> | undefined
  workflowRegistration: ReconciliationWorkflow.RegistrationService | undefined
  summaryStatus: ReplicaStatus.ReplicaStatus
  retryAttempt: number
  retryVersion: number
}

type BackgroundWork =
  | { readonly _tag: "Sync"; readonly spaceId: Identity.SpaceId }
  | { readonly _tag: "Deactivate"; readonly entry: RememberedEntry; readonly runtime: ActiveRuntime }

interface RetryWork {
  readonly entry: RememberedEntry
  readonly version: number
  readonly readyAt: number
}

const RememberedRow = Schema.Struct({
  space_id: Identity.SpaceId,
  membership_incarnation: Identity.MembershipIncarnation,
  desired_scope_json: Schema.String,
  count: Schema.Int
})

const addressedStatus = (
  spaceId: Identity.SpaceId,
  status: ReplicaStatus.ReplicaStatus
): ReplicaStatus.SpaceStatus => ({ spaceId, ...status })

type AggregateCounts = ReplicaStatus.Aggregate["counts"]
type AggregateCategory = keyof AggregateCounts

const statusCategories: {
  readonly [Tag in ReplicaStatus.ReplicaStatus["_tag"]]: AggregateCategory
} = {
  Offline: "offline",
  Connecting: "connecting",
  Online: "online",
  SchemaUpdateAvailable: "online",
  NeedsAuthentication: "needsAuthentication",
  Failed: "failed"
}

const statusCategory = (status: ReplicaStatus.ReplicaStatus): AggregateCategory => statusCategories[status._tag]

const aggregateStatus = (
  spaces: number,
  totalPending: number,
  counts: AggregateCounts
): ReplicaStatus.Aggregate => {
  let state: ReplicaStatus.AggregateState = "Degraded"
  if (spaces === 0) state = "Idle"
  else if (counts.failed > 0) state = "Failed"
  else if (counts.needsAuthentication > 0) state = "NeedsAuthentication"
  else if (counts.online === spaces) state = "Online"
  else if (counts.offline === spaces) state = "Offline"
  else if (counts.connecting > 0) state = "Connecting"
  return { state, spaces, totalPending, counts }
}

const settledFor =
  <M extends Mutation.Any,>(mutation: M) => (settled: Replica.SettledMutation): settled is Replica.SettledMutation<M> =>
    settled.settlement.pending.envelope.name === mutation.name

const makeLayer = <D extends Definition.Any, R,>(
  options: Options<D>,
  workflowEngine: Effect.Effect<WorkflowEngine.WorkflowEngine["Service"] | undefined, never, R>
): Layer.Layer<
  Replica.Replica | QueryReactivity.QueryReactivity,
  ReplicaError.ReplicaError,
  BaseRequirements<D> | R
> => {
  const layerQueryReactivity = QueryReactivity.makeLayer()
  return Layer.effect(
    Replica.Replica,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const reactivity = yield* Reactivity.Reactivity
      const remote = yield* SyncEngine.SyncEngine
      const parentScope = yield* Effect.scope
      const rootContext = yield* Effect.context<BaseRequirements<D> | QueryReactivity.QueryReactivity | R>()
      const entries = new Map<Identity.SpaceId, RememberedEntry>()
      const joining = new Map<Identity.SpaceId, Deferred.Deferred<void>>()
      const foregroundResidents = new Map<Identity.SpaceId, RememberedEntry>()
      const aggregate = yield* Ref.make(aggregateStatus(0, 0, {
        offline: 0,
        connecting: 0,
        online: 0,
        needsAuthentication: 0,
        failed: 0
      }))
      let nextGeneration = 0
      const workflow = yield* workflowEngine
      if (
        !Number.isSafeInteger(options.maximumActiveSpaces) ||
        options.maximumActiveSpaces < 2
      ) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "maximumActiveSpaces",
          message: "maximumActiveSpaces must be a safe integer of at least 2"
        })
      }
      if (
        !Number.isSafeInteger(options.foregroundActiveSpaces) ||
        options.foregroundActiveSpaces <= 0 ||
        options.foregroundActiveSpaces >= options.maximumActiveSpaces
      ) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "foregroundActiveSpaces",
          message: "foregroundActiveSpaces must be positive and less than maximumActiveSpaces"
        })
      }
      const reconciliationConcurrency = options.reconciliationConcurrency ?? 8
      if (!Number.isSafeInteger(reconciliationConcurrency) || reconciliationConcurrency < 2) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "reconciliationConcurrency",
          message: "reconciliationConcurrency must be a safe integer of at least 2"
        })
      }
      const foregroundReconciliationConcurrency = options.foregroundReconciliationConcurrency ?? 1
      if (
        !Number.isSafeInteger(foregroundReconciliationConcurrency) ||
        foregroundReconciliationConcurrency <= 0 ||
        foregroundReconciliationConcurrency >= reconciliationConcurrency
      ) {
        return yield* new ReplicaError.InvalidConfiguration({
          option: "foregroundReconciliationConcurrency",
          message: "foregroundReconciliationConcurrency must be positive and less than reconciliationConcurrency"
        })
      }
      let manager: Reconciler.ManagerService | undefined
      if (workflow === undefined) {
        manager = yield* Reconciler.makeManager({ concurrency: foregroundReconciliationConcurrency })
      }
      const backgroundConcurrency = Math.min(
        options.maximumActiveSpaces - options.foregroundActiveSpaces,
        reconciliationConcurrency - foregroundReconciliationConcurrency
      )
      const backgroundActiveSpaces = yield* Semaphore.make(
        options.maximumActiveSpaces - options.foregroundActiveSpaces
      )
      const foregroundWorkflowTurns = yield* Semaphore.make(foregroundReconciliationConcurrency)
      const backgroundWorkflowTurns = yield* Semaphore.make(
        reconciliationConcurrency - foregroundReconciliationConcurrency
      )
      const retryTiming = yield* Configuration.retryTiming(options)
      const backgroundQueue = yield* Effect.acquireRelease(
        Queue.unbounded<BackgroundWork>(),
        Queue.shutdown
      )
      const retryQueue = yield* Effect.acquireRelease(
        Queue.unbounded<RetryWork>(),
        Queue.shutdown
      )
      const backgroundQueued = new Set<Identity.SpaceId>()
      const retrySchedule: Array<RetryWork> = []
      let capacityChanged = yield* Deferred.make<void>()

      yield* Migrations.client({
        definition: options.definition,
        clientId: options.clientId,
        migration: options.migration
      })

      const normalizedDefaultScope = yield* Protocol.validateReplicationScope(
        options.definition,
        options.defaultScope
      )
      const defaultScopeJson = yield* Codec.stringify(normalizedDefaultScope)
      const defaultScopeDigest = yield* Protocol.replicationScopeDigest(normalizedDefaultScope)

      const invalidateAggregate = reactivity.invalidate([ReactivityKey.aggregateStatus])
      const changeAggregate = (
        update: (current: ReplicaStatus.Aggregate) => ReplicaStatus.Aggregate,
        invalidate = true
      ) => {
        const changed = Ref.update(aggregate, update)
        if (!invalidate) return changed.pipe(Effect.uninterruptible)
        return changed.pipe(Effect.andThen(invalidateAggregate), Effect.uninterruptible)
      }
      const addContribution = (entry: RememberedEntry, invalidate = true) =>
        changeAggregate((current) => {
          const category = statusCategory(entry.summaryStatus)
          return aggregateStatus(
            current.spaces + 1,
            current.totalPending + entry.summaryStatus.pending,
            { ...current.counts, [category]: current.counts[category] + 1 }
          )
        }, invalidate)
      const removeContribution = (entry: RememberedEntry) =>
        changeAggregate((current) => {
          const category = statusCategory(entry.summaryStatus)
          return aggregateStatus(
            current.spaces - 1,
            current.totalPending - entry.summaryStatus.pending,
            { ...current.counts, [category]: current.counts[category] - 1 }
          )
        })
      const modifyContribution = (
        entry: RememberedEntry,
        update: (current: ReplicaStatus.ReplicaStatus) => ReplicaStatus.ReplicaStatus
      ) =>
        Effect.suspend(() => {
          if (entries.get(entry.spaceId) !== entry || entry.leaving) return Effect.void
          return Ref.modify(aggregate, (current): readonly [boolean, ReplicaStatus.Aggregate] => {
            const previous = entry.summaryStatus
            const next = update(previous)
            const previousCategory = statusCategory(previous)
            const nextCategory = statusCategory(next)
            if (previousCategory === nextCategory && previous.pending === next.pending) return [false, current]
            entry.summaryStatus = next
            let counts = current.counts
            if (previousCategory !== nextCategory) {
              counts = {
                ...counts,
                [previousCategory]: counts[previousCategory] - 1,
                [nextCategory]: counts[nextCategory] + 1
              }
            }
            return [
              true,
              aggregateStatus(
                current.spaces,
                current.totalPending + next.pending - previous.pending,
                counts
              )
            ]
          }).pipe(
            Effect.flatMap((changed) => {
              if (changed) return invalidateAggregate
              return Effect.void
            }),
            Effect.uninterruptible
          )
        })
      const updateContribution = (
        entry: RememberedEntry,
        next: ReplicaStatus.ReplicaStatus
      ) => modifyContribution(entry, () => next)
      const updatePendingContribution = (entry: RememberedEntry, pending: number) =>
        modifyContribution(entry, (current) => ({ ...current, pending }))
      const readMemberships = SqlSchema.findAll({
        Request: Schema.Void,
        Result: RememberedRow,
        execute: () =>
          sql`SELECT s.space_id, s.membership_incarnation, s.desired_scope_json, COUNT(p.mutation_id) AS count
          FROM effect_local_client_spaces AS s
          LEFT JOIN effect_local_client_pending_data AS p
            ON p.space_id = s.space_id AND p.schema_generation = s.active_schema_generation
          GROUP BY s.space_id, s.membership_incarnation, s.desired_scope_json
          ORDER BY s.space_id`
      })
      const readMembership = SqlSchema.findOneOption({
        Request: Identity.SpaceId,
        Result: RememberedRow,
        execute: (spaceId) =>
          sql`SELECT s.space_id, s.membership_incarnation, s.desired_scope_json, COUNT(p.mutation_id) AS count
          FROM effect_local_client_spaces AS s
          LEFT JOIN effect_local_client_pending_data AS p
            ON p.space_id = s.space_id AND p.schema_generation = s.active_schema_generation
          WHERE s.space_id = ${spaceId}
          GROUP BY s.space_id, s.membership_incarnation, s.desired_scope_json`
      })
      const pendingCount = SqlSchema.findOne({
        Request: Identity.SpaceId,
        Result: Rows.CountRow,
        execute: (spaceId) =>
          sql`SELECT COUNT(p.mutation_id) AS count
          FROM effect_local_client_spaces AS s
          LEFT JOIN effect_local_client_pending_data AS p
            ON p.space_id = s.space_id AND p.schema_generation = s.active_schema_generation
          WHERE s.space_id = ${spaceId}`
      })
      const decodeScope = (encoded: string) =>
        Codec.parse(encoded).pipe(
          Effect.flatMap((value) => Codec.decode(Protocol.ReplicationScope, value)),
          Effect.flatMap((value) => Protocol.validateReplicationScope(options.definition, value))
        )

      const signalCapacity = Effect.gen(function*() {
        const previous = capacityChanged
        capacityChanged = yield* Deferred.make<void>()
        yield* Deferred.succeed(previous, undefined)
      })

      const invalidateActivation = (spaceId: Identity.SpaceId) =>
        reactivity.invalidate([
          ReactivityKey.activation(spaceId),
          ReactivityKey.status(spaceId)
        ])

      const checkRuntime = (entry: RememberedEntry, runtime: ActiveRuntime) =>
        Effect.suspend(() => {
          const current = entries.get(entry.spaceId)
          if (current === entry && !entry.leaving && entry.runtime === runtime) return Effect.void
          return Effect.fail(new ReplicaError.SpaceUnavailable({ spaceId: entry.spaceId }))
        })

      const initialize = Effect.fnUntraced(function*(
        entry: RememberedEntry,
        generation: number,
        foreground: boolean
      ) {
        const spaceId = entry.spaceId
        const childScope = yield* Scope.fork(parentScope)
        return yield* Effect.gen(function*() {
          if (!foreground) {
            yield* Effect.acquireRelease(
              backgroundActiveSpaces.take(1),
              () => backgroundActiveSpaces.release(1)
            ).pipe(Scope.provide(childScope))
          }
          const layerMutationRuntime = MutationRuntime.layer(options.definition, options.evolution)
          const layerLocalStore = LocalStore.layer({
            ...options,
            scope: entry.replicationScope,
            spaceId
          }).pipe(Layer.provide(layerMutationRuntime))
          const layerQueryExecutor = QueryExecutor.layer(options.definition, spaceId)
          let local: LocalStore.Service
          let queries: QueryExecutor.Service
          let reconciler: Reconciler.Service
          let reconciliation: Reconciler.ReconciliationService
          let cancelReconciliation = Effect.void
          if (workflow !== undefined) {
            const workflowContext = Context.add(rootContext, WorkflowEngine.WorkflowEngine, workflow)
            const layerReconciliation = Reconciler.layerOnePass({
              ...options,
              spaceId,
              onStatusChange: (status) => updateContribution(entry, status)
            }).pipe(
              Layer.provide(layerLocalStore)
            )
            const runtime = yield* Layer.mergeAll(
              layerLocalStore,
              layerQueryExecutor,
              layerReconciliation
            ).pipe(
              Layer.buildWithScope(childScope),
              Effect.provide(workflowContext),
              Effect.tapError((error) => Scope.close(childScope, Exit.fail(error)))
            )
            local = Context.get(runtime, LocalStore.Store)
            queries = Context.get(runtime, QueryExecutor.QueryExecutor)
            reconciliation = Context.get(runtime, Reconciler.Reconciliation)
            if (foreground) {
              if (entry.workflowRegistration === undefined) {
                return yield* Effect.die("Workflow registration was not initialized")
              }
              const scheduler = yield* ReconciliationWorkflow.layerScheduler({ ...options, spaceId }).pipe(
                Layer.provide(Layer.succeed(LocalStore.Store, local)),
                Layer.provide(Layer.succeed(Reconciler.Reconciliation, reconciliation)),
                Layer.provide(
                  Layer.succeed(ReconciliationWorkflow.Registration, entry.workflowRegistration)
                ),
                Layer.buildWithScope(childScope),
                Effect.provide(workflowContext),
                Effect.tapError((error) => Scope.close(childScope, Exit.fail(error)))
              )
              reconciler = Context.get(scheduler, Reconciler.Reconciler)
            } else {
              reconciler = Reconciler.Reconciler.of({
                sync: reconciliation.sync,
                notify: local.requestReconciliation.pipe(Effect.asVoid),
                status: reconciliation.status,
                shutdown: Effect.void
              })
            }
            cancelReconciliation = reconciler.shutdown
          } else {
            if (manager === undefined) return yield* Effect.die("Reconciler manager was not initialized")
            const runtime = yield* Layer.mergeAll(
              layerLocalStore,
              layerQueryExecutor,
              Reconciler.layerOnePass({
                ...options,
                spaceId,
                onStatusChange: (status) => updateContribution(entry, status)
              }).pipe(Layer.provide(layerLocalStore))
            ).pipe(
              Layer.buildWithScope(childScope),
              Effect.provide(rootContext),
              Effect.tapError((error) => Scope.close(childScope, Exit.fail(error)))
            )
            local = Context.get(runtime, LocalStore.Store)
            queries = Context.get(runtime, QueryExecutor.QueryExecutor)
            reconciliation = Context.get(runtime, Reconciler.Reconciliation)
            if (foreground) {
              let managedSpace: Reconciler.ManagedSpace = {
                spaceId,
                generation,
                definition: options.definition,
                local,
                reconciliation
              }
              if (options.retryDelay !== undefined) {
                managedSpace = { ...managedSpace, retryDelay: options.retryDelay }
              }
              if (options.maximumRetryDelay !== undefined) {
                managedSpace = { ...managedSpace, maximumRetryDelay: options.maximumRetryDelay }
              }
              yield* Effect.acquireRelease(
                manager.register(managedSpace),
                () => manager.unregister(spaceId, generation)
              ).pipe(Scope.provide(childScope))
              reconciler = Reconciler.Reconciler.of({
                sync: manager.sync(spaceId),
                notify: manager.notify(spaceId),
                status: manager.status(spaceId),
                shutdown: Effect.void
              })
            } else {
              reconciler = Reconciler.Reconciler.of({
                sync: reconciliation.sync,
                notify: local.requestReconciliation.pipe(Effect.asVoid),
                status: reconciliation.status,
                shutdown: Effect.void
              })
            }
          }
          const operationGate = yield* Semaphore.make(1)
          return {
            foreground,
            scope: childScope,
            operationGate,
            local,
            queries,
            reconciler,
            reconciliation,
            cancelReconciliation
          } satisfies ActiveRuntime
        }).pipe(
          Effect.onExit((exit) => {
            if (Exit.isFailure(exit)) return Scope.close(childScope, exit)
            return Effect.void
          })
        )
      })

      const enqueueBackground = (entry: RememberedEntry, resetRetry = true) =>
        Effect.suspend(() => {
          if (entry.leaving || backgroundQueued.has(entry.spaceId)) return Effect.void
          if (resetRetry) {
            entry.retryAttempt = 0
            entry.retryVersion += 1
          }
          backgroundQueued.add(entry.spaceId)
          return Queue.offer(backgroundQueue, { _tag: "Sync", spaceId: entry.spaceId }).pipe(Effect.asVoid)
        })

      const scheduleBackgroundRetry = Effect.fnUntraced(function*(entry: RememberedEntry) {
        if (entries.get(entry.spaceId) !== entry || entry.leaving || entry.foreground) return
        entry.retryAttempt += 1
        entry.retryVersion += 1
        const readyAt = (yield* Clock.currentTimeMillis) + Configuration.retryMillis(retryTiming, entry.retryAttempt)
        yield* Queue.offer(retryQueue, { entry, version: entry.retryVersion, readyAt })
      })

      const retrySchedulerTurn = Effect.suspend(() => {
        const insert = (work: RetryWork) => {
          retrySchedule.push(work)
          retrySchedule.sort((left, right) => left.readyAt - right.readyAt)
        }
        if (retrySchedule.length === 0) {
          return Queue.take(retryQueue).pipe(
            Effect.tap((work) => {
              insert(work)
              return Effect.void
            }),
            Effect.asVoid
          )
        }
        return Clock.currentTimeMillis.pipe(
          Effect.flatMap((now) => {
            const next = retrySchedule[0]
            if (next.readyAt <= now) {
              retrySchedule.shift()
              if (
                entries.get(next.entry.spaceId) !== next.entry ||
                next.entry.leaving ||
                next.entry.foreground ||
                next.entry.retryVersion !== next.version
              ) return Effect.void
              return enqueueBackground(next.entry, false)
            }
            return Effect.raceFirst(
              Queue.take(retryQueue).pipe(Effect.map((work) => Option.some(work))),
              Effect.sleep(Duration.millis(next.readyAt - now)).pipe(Effect.as(Option.none<RetryWork>()))
            ).pipe(
              Effect.tap(Option.match({
                onNone: () => Effect.void,
                onSome: (work) => {
                  insert(work)
                  return Effect.void
                }
              })),
              Effect.asVoid
            )
          })
        )
      })

      const deactivate = (
        entry: RememberedEntry,
        explicit: boolean,
        expectedRuntime?: ActiveRuntime,
        enqueuePending = true
      ): Effect.Effect<boolean, ReplicaError.ReplicaError> =>
        Effect.uninterruptibleMask(Effect.fnUntraced(function*(restore) {
          if (entries.get(entry.spaceId) !== entry) {
            return yield* new ReplicaError.SpaceUnavailable({ spaceId: entry.spaceId })
          }
          if (entry.activation === "Inactive") return false
          if (entry.activation === "Activating" || entry.activation === "Deactivating") {
            const transition = entry.transition
            if (transition !== undefined) yield* restore(Deferred.await(transition))
            return yield* deactivate(entry, explicit, expectedRuntime, enqueuePending)
          }
          const runtime = entry.runtime
          if (runtime === undefined) {
            entry.activation = "Inactive"
            return false
          }
          if (
            expectedRuntime !== undefined &&
            (runtime !== expectedRuntime || runtime.foreground || entry.foreground)
          ) return false
          if (entry.leases > 0) {
            if (!explicit) return false
            const changed = capacityChanged
            yield* restore(Deferred.await(changed))
            return yield* deactivate(entry, explicit, expectedRuntime, enqueuePending)
          }
          const completion = yield* Deferred.make<void, ReplicaError.ReplicaError>()
          entry.activation = "Deactivating"
          entry.transition = completion
          entry.foreground = false
          foregroundResidents.delete(entry.spaceId)
          yield* invalidateActivation(entry.spaceId)
          const shutdown = Scope.close(runtime.scope, Exit.void)
          const result = yield* restore(
            runtime.operationGate.withPermit(shutdown)
          ).pipe(Effect.exit)
          entry.runtime = undefined
          entry.activation = "Inactive"
          entry.transition = undefined
          yield* Deferred.done(completion, result)
          yield* invalidateActivation(entry.spaceId)
          yield* signalCapacity
          if (Exit.isFailure(result)) {
            yield* result
            return false
          }
          const count = yield* pendingCount(entry.spaceId).pipe(
            Effect.catchTags({
              SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client membership row is corrupt",
                    cause
                  })
                ),
              NoSuchElementError: (cause) =>
                Effect.fail(
                  new ReplicaError.StorageCorrupt({
                    message: "Client membership row is missing",
                    cause
                  })
                )
            }),
            Effect.tapError(() => {
              if (enqueuePending) return enqueueBackground(entry)
              return Effect.void
            })
          )
          yield* updateContribution(entry, { _tag: "Offline", pending: count.count })
          if (enqueuePending && count.count > 0 && !entry.leaving) yield* enqueueBackground(entry)
          return true
        }))

      const ensureForegroundCapacity = (entry: RememberedEntry): Effect.Effect<void, ReplicaError.ReplicaError> =>
        Effect.suspend(() => {
          if (foregroundResidents.size <= options.foregroundActiveSpaces) return Effect.void
          let victim: RememberedEntry | undefined
          for (const candidate of foregroundResidents.values()) {
            if (candidate !== entry && candidate.activation === "Active" && candidate.leases === 0) {
              victim = candidate
              break
            }
          }
          if (victim !== undefined) return deactivate(victim, false).pipe(Effect.asVoid)
          const changed = capacityChanged
          return Deferred.await(changed).pipe(Effect.andThen(ensureForegroundCapacity(entry)))
        })

      const activate = (
        entry: RememberedEntry,
        foreground: boolean
      ): Effect.Effect<ActiveRuntime, ReplicaError.ReplicaError> =>
        Effect.uninterruptibleMask(Effect.fnUntraced(function*(restore) {
          if (entries.get(entry.spaceId) !== entry || entry.leaving) {
            return yield* new ReplicaError.SpaceUnavailable({ spaceId: entry.spaceId })
          }
          if (foreground && !entry.foreground) {
            entry.foreground = true
            foregroundResidents.delete(entry.spaceId)
            foregroundResidents.set(entry.spaceId, entry)
            yield* restore(ensureForegroundCapacity(entry)).pipe(
              Effect.onExit((exit) => {
                if (Exit.isSuccess(exit) || entry.activation === "Active") return Effect.void
                entry.foreground = false
                foregroundResidents.delete(entry.spaceId)
                return signalCapacity
              })
            )
          }
          if (foreground) {
            foregroundResidents.delete(entry.spaceId)
            foregroundResidents.set(entry.spaceId, entry)
          }
          if (entry.activation === "Active" && entry.runtime !== undefined) {
            if (!foreground || entry.runtime.foreground) return entry.runtime
            if (entry.leases > 0) {
              const changed = capacityChanged
              yield* restore(Deferred.await(changed))
              return yield* activate(entry, foreground)
            }
            yield* restore(deactivate(entry, false))
            entry.foreground = true
            return yield* activate(entry, foreground)
          }
          if (entry.activation === "Activating" || entry.activation === "Deactivating") {
            const transition = entry.transition
            if (transition !== undefined) yield* restore(Deferred.await(transition))
            return yield* activate(entry, foreground)
          }
          const completion = yield* Deferred.make<void, ReplicaError.ReplicaError>()
          const generation = ++nextGeneration
          entry.activation = "Activating"
          entry.transition = completion
          yield* invalidateActivation(entry.spaceId)
          const result = yield* restore(initialize(entry, generation, foreground)).pipe(Effect.exit)
          if (Exit.isSuccess(result)) {
            entry.runtime = result.value
            entry.activation = "Active"
            entry.transition = undefined
            yield* Deferred.succeed(completion, undefined)
            yield* invalidateActivation(entry.spaceId)
            yield* signalCapacity
            return result.value
          }
          entry.runtime = undefined
          entry.activation = "Inactive"
          entry.transition = undefined
          entry.foreground = false
          foregroundResidents.delete(entry.spaceId)
          yield* Deferred.done(completion, result)
          yield* invalidateActivation(entry.spaceId)
          yield* signalCapacity
          return yield* result
        }))

      const acquire = (entry: RememberedEntry, foreground: boolean) =>
        activate(entry, foreground).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              entry.leases += 1
              if (foreground) {
                foregroundResidents.delete(entry.spaceId)
                foregroundResidents.set(entry.spaceId, entry)
              }
            })
          )
        )

      const release = (entry: RememberedEntry) =>
        Effect.sync(() => {
          entry.leases = Math.max(0, entry.leases - 1)
        }).pipe(Effect.andThen(signalCapacity))

      const withActive = <A, E extends { readonly _tag: string },>(
        entry: RememberedEntry,
        use: (runtime: ActiveRuntime) => Effect.Effect<A, E>
      ): Effect.Effect<A, E | ReplicaError.ReplicaError> =>
        Effect.acquireUseRelease(
          acquire(entry, true),
          (runtime) => {
            const operation = checkRuntime(entry, runtime).pipe(Effect.andThen(use(runtime)))
            return runtime.operationGate.withPermit(operation)
          },
          () => release(entry)
        )

      const continueCancellation = Effect.fnUntraced(function*(
        runtime: ActiveRuntime,
        initial: Option.Option<Quarantine.QuarantinedMutation>
      ) {
        let canceled = initial
        while (Option.isSome(canceled)) {
          yield* runtime.reconciler.sync
          const canceledReceipt = yield* remote.discard({
            envelope: canceled.value.envelope,
            schema: runtime.local.schema
          })
          canceled = yield* runtime.local.resolveQuarantine(canceledReceipt, "Discard")
        }
      })

      const findReceipt = (runtime: ActiveRuntime, mutationId: Identity.MutationId) =>
        runtime.local.receipt(mutationId).pipe(
          Effect.flatMap(Option.match({
            onNone: () =>
              Effect.fail(
                new ReplicaError.ProtocolInvalid({
                  message: `Quarantined mutation ${mutationId} was not found or previously resolved`
                })
              ),
            onSome: Effect.succeed
          }))
        )

      const makeHandle = (entry: RememberedEntry): Replica.Space => {
        const runtimeLease = Effect.acquireRelease(acquire(entry, true), () => release(entry))
        return {
          spaceId: entry.spaceId,
          scope: Effect.suspend(() => {
            if (entries.get(entry.spaceId) !== entry || entry.leaving) {
              return Effect.fail(new ReplicaError.SpaceUnavailable({ spaceId: entry.spaceId }))
            }
            return Effect.succeed(entry.replicationScope)
          }),
          setScope: (nextScope) =>
            withActive(entry, (runtime) =>
              runtime.local.setScope(nextScope).pipe(
                Effect.andThen(runtime.local.replicationState),
                Effect.tap((state) =>
                  Effect.sync(() => {
                    entry.replicationScope = state.scope
                  })
                )
              )).pipe(
                Effect.flatMap(() => deactivate(entry, true)),
                Effect.andThen(activate(entry, true)),
                Effect.flatMap((runtime) => runtime.reconciler.notify),
                Effect.andThen(reactivity.invalidate([ReactivityKey.scope(entry.spaceId)]))
              ),
          activation: Effect.suspend(() => {
            if (entries.get(entry.spaceId) !== entry || entry.leaving) {
              return Effect.fail(new ReplicaError.SpaceUnavailable({ spaceId: entry.spaceId }))
            }
            return Effect.succeed(entry.activation)
          }),
          activate: activate(entry, true).pipe(Effect.asVoid),
          deactivate: deactivate(entry, true).pipe(Effect.asVoid),
          mutate: (mutation, payload) =>
            withActive(entry, (runtime) =>
              runtime.local.mutate(mutation, payload).pipe(
                Effect.tap(() =>
                  runtime.local.pendingCount.pipe(
                    Effect.flatMap((pending) => updatePendingContribution(entry, pending))
                  )
                ),
                Effect.tap(() => runtime.reconciler.notify)
              )),
          get: (model, key) => withActive(entry, (runtime) => runtime.local.get(model, key)),
          query: (query, payload) => withActive(entry, (runtime) => runtime.queries.execute(query, payload)),
          receipt: (mutation, mutationId) =>
            withActive(entry, (runtime) => runtime.local.receiptFor(mutation, mutationId)),
          pending: withActive(entry, (runtime) => runtime.local.pending),
          pendingFor: (mutation) =>
            withActive(entry, (runtime) =>
              MutationDescriptor.validate(options.definition, mutation).pipe(
                Effect.andThen(runtime.local.pending),
                Effect.map((pending) =>
                  pending.flatMap((item) => {
                    if (item.envelope.name !== mutation.name) return []
                    return [{ ...item } satisfies Replica.PendingMutation<typeof mutation>]
                  })
                )
              )),
          settlements: (settlementOptions) =>
            Stream.scoped(
              Stream.fromEffect(runtimeLease).pipe(
                Stream.flatMap((runtime) => runtime.local.settlements({ from: settlementOptions?.from ?? "live" }))
              )
            ),
          settlementsFor: (mutation, settlementOptions) =>
            Stream.scoped(
              Stream.fromEffect(runtimeLease).pipe(
                Stream.tap(() => MutationDescriptor.validate(options.definition, mutation)),
                Stream.flatMap((runtime) =>
                  runtime.local.settlements({
                    from: settlementOptions?.from ?? "live",
                    mutationName: mutation.name
                  }).pipe(Stream.filter(settledFor(mutation)))
                )
              )
            ),
          acknowledgeSettlements: (sequence) =>
            withActive(entry, (runtime) => runtime.local.acknowledgeSettlements(sequence)),
          quarantine: withActive(entry, (runtime) => runtime.local.quarantine),
          discardQuarantined: (mutationId) =>
            withActive(entry, (runtime) =>
              Effect.fnUntraced(function*() {
                const found = yield* runtime.local.quarantineByMutation(mutationId)
                if (Option.isNone(found)) {
                  const continuation = yield* runtime.local.quarantineCancellation(mutationId)
                  yield* continueCancellation(runtime, continuation)
                  return yield* findReceipt(runtime, mutationId)
                }
                const receipt = yield* remote.discard({
                  envelope: found.value.envelope,
                  schema: runtime.local.schema
                })
                const canceled = yield* runtime.local.resolveQuarantine(receipt, "Discard")
                yield* continueCancellation(runtime, canceled)
                return receipt
              })().pipe(
                Effect.exit,
                Effect.flatMap((exit) =>
                  runtime.local.pendingCount.pipe(
                    Effect.flatMap((pending) => updatePendingContribution(entry, pending)),
                    Effect.andThen(runtime.reconciler.notify),
                    Effect.andThen(exit)
                  )
                )
              )),
          resubmitQuarantined: <M extends Mutation.Any,>(
            mutationId: Identity.MutationId,
            mutation: M,
            payload: Mutation.Payload<M>
          ) =>
            withActive(entry, (runtime) =>
              Effect.fnUntraced(function*() {
                const found = yield* runtime.local.quarantineByMutation(mutationId)
                if (Option.isNone(found)) {
                  const continuation = yield* runtime.local.quarantineCancellation(mutationId)
                  yield* continueCancellation(runtime, continuation)
                  return Quarantine.AlreadyResolved.make({ receipt: yield* findReceipt(runtime, mutationId) })
                }
                const item = found.value
                if (mutation.name !== item.envelope.name) {
                  return yield* new ReplicaError.ProtocolInvalid({
                    message: `Resubmission mutation ${mutation.name} does not match ${item.envelope.name}`
                  })
                }
                const pending = yield* runtime.local.ensureQuarantineResubmission(mutationId, mutation, payload)
                const receipt = yield* remote.discard({ envelope: item.envelope, schema: runtime.local.schema })
                const canceled = yield* runtime.local.resolveQuarantine(receipt, "Resubmit")
                yield* continueCancellation(runtime, canceled)
                if (receipt._tag !== "Rejected" || receipt.origin !== "Quarantine") {
                  return Quarantine.AlreadyResolved.make({ receipt })
                }
                return Quarantine.Resubmitted.make({ pending })
              })().pipe(
                Effect.exit,
                Effect.flatMap((exit) =>
                  runtime.local.pendingCount.pipe(
                    Effect.flatMap((pending) => updatePendingContribution(entry, pending)),
                    Effect.andThen(runtime.reconciler.notify),
                    Effect.andThen(exit)
                  )
                )
              )),
          status: Effect.suspend(() => {
            const runtime = entry.runtime
            if (entry.activation === "Active" && runtime !== undefined) {
              return Effect.all([runtime.reconciler.status, runtime.local.pendingCount]).pipe(
                Effect.map(([status, pending]) => addressedStatus(entry.spaceId, { ...status, pending }))
              )
            }
            return pendingCount(entry.spaceId).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                SchemaError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Client membership row is corrupt",
                      cause
                    })
                  ),
                NoSuchElementError: (cause) =>
                  Effect.fail(
                    new ReplicaError.StorageCorrupt({
                      message: "Client membership row is missing",
                      cause
                    })
                  )
              }),
              Effect.map((row) => addressedStatus(entry.spaceId, { _tag: "Offline", pending: row.count }))
            )
          })
        }
      }

      const createEntry = Effect.fnUntraced(function*(row: typeof RememberedRow.Type) {
        const replicationScope = yield* decodeScope(row.desired_scope_json)
        let handle: Replica.Space | undefined
        const entry: RememberedEntry = {
          spaceId: row.space_id,
          get handle() {
            if (handle === undefined) handle = makeHandle(entry)
            return handle
          },
          replicationScope,
          activation: "Inactive",
          runtime: undefined,
          transition: undefined,
          foreground: false,
          leases: 0,
          leaving: false,
          leaveCompletion: undefined,
          workflowRegistration: undefined,
          summaryStatus: { _tag: "Offline", pending: row.count },
          retryAttempt: 0,
          retryVersion: 0
        }
        if (workflow !== undefined) {
          const lease = ReconciliationWorkflow.RuntimeLease.of({
            acquire: Effect.gen(function*() {
              const foreground = entry.foreground
              const leaseRuntime = yield* Effect.acquireRelease(
                acquire(entry, foreground),
                (runtime) =>
                  release(entry).pipe(
                    Effect.andThen(Queue.offer(backgroundQueue, { _tag: "Deactivate", entry, runtime })),
                    Effect.asVoid
                  )
              )
              return {
                local: leaseRuntime.local,
                reconciliation: leaseRuntime.reconciliation
              }
            }),
            admit: (effect) =>
              Effect.suspend(() => {
                let turns = backgroundWorkflowTurns
                if (entry.foreground) turns = foregroundWorkflowTurns
                return turns.withPermit(effect)
              })
          })
          const registrationContext = Context.add(
            Context.add(rootContext, WorkflowEngine.WorkflowEngine, workflow),
            ReconciliationWorkflow.RuntimeLease,
            lease
          )
          const built = yield* ReconciliationWorkflow.layerDetachedRegistration({
            ...options,
            spaceId: row.space_id,
            membershipIncarnation: row.membership_incarnation
          }).pipe(
            Layer.provide(Layer.succeed(ReconciliationWorkflow.RegistrationScope, parentScope)),
            Layer.buildWithScope(parentScope),
            Effect.provide(registrationContext)
          )
          entry.workflowRegistration = Context.get(built, ReconciliationWorkflow.Registration)
        }
        return entry
      })

      const insertMembership = (spaceId: Identity.SpaceId) =>
        sql`INSERT INTO effect_local_client_spaces
          (space_id, membership_incarnation, definition_hash, schema_version, schema_hash, schema_generation,
            active_schema_generation, active_projection_generation, projection_schema_generation,
            next_local_sequence, server_cursor, visible_revision, requested_generation, completed_generation,
            installed_snapshot_sequence, installed_snapshot_terminal_sequence, desired_scope_json,
            desired_scope_digest, scope_generation)
          VALUES (${spaceId},
            ('inc_' || lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
              substr(lower(hex(randomblob(2))), 2) || '-' ||
              substr('89ab', abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))), 2) || '-' ||
              lower(hex(randomblob(6)))), ${options.definition.hash},
            ${options.definition.schemaIdentity.version}, ${options.definition.schemaIdentity.hash}, 0, 0, 0, 0,
            1, 0, 0, 1, 0, 0, 0, ${defaultScopeJson}, ${defaultScopeDigest}, 1)
          ON CONFLICT (space_id) DO NOTHING`

      const join = (spaceId: Identity.SpaceId): Effect.Effect<Replica.Space, ReplicaError.ReplicaError> =>
        Effect.uninterruptibleMask(
          Effect.fnUntraced(function*(restore) {
            const current = entries.get(spaceId)
            if (current !== undefined && !current.leaving) return current.handle
            if (current?.leaveCompletion !== undefined) {
              yield* restore(Deferred.await(current.leaveCompletion))
              return yield* join(spaceId)
            }
            const activeJoin = joining.get(spaceId)
            if (activeJoin !== undefined) {
              yield* restore(Deferred.await(activeJoin))
              return yield* join(spaceId)
            }
            const completion = yield* Deferred.make<void>()
            joining.set(spaceId, completion)
            const result = yield* restore(
              sql.withTransaction(insertMembership(spaceId)).pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
                Effect.andThen(
                  readMembership(spaceId).pipe(
                    Effect.catchTags({
                      SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
                      SchemaError: (cause) =>
                        Effect.fail(
                          new ReplicaError.StorageCorrupt({
                            message: "Client membership row is corrupt",
                            cause
                          })
                        )
                    })
                  )
                ),
                Effect.flatMap(Option.match({
                  onNone: () =>
                    Effect.fail(
                      new ReplicaError.StorageCorrupt({
                        message: `Remembered space ${spaceId} was not persisted`
                      })
                    ),
                  onSome: createEntry
                })),
                Effect.tap((entry) => {
                  entries.set(spaceId, entry)
                  return addContribution(entry).pipe(
                    Effect.andThen(reactivity.invalidate([
                      ReactivityKey.membership(spaceId),
                      ReactivityKey.spaces
                    ]))
                  )
                })
              )
            ).pipe(Effect.exit)
            if (joining.get(spaceId) === completion) joining.delete(spaceId)
            yield* Deferred.succeed(completion, undefined)
            return yield* result.pipe(Effect.map((entry) => entry.handle))
          })
        )

      const leave = (spaceId: Identity.SpaceId): Effect.Effect<void, ReplicaError.ReplicaError> =>
        Effect.uninterruptibleMask(Effect.fnUntraced(function*(restore) {
          const activeJoin = joining.get(spaceId)
          if (activeJoin !== undefined) {
            yield* restore(Deferred.await(activeJoin))
            return yield* leave(spaceId)
          }
          const current = entries.get(spaceId)
          if (current?.leaveCompletion !== undefined) {
            return yield* restore(Deferred.await(current.leaveCompletion))
          }
          if (current === undefined) {
            return yield* restore(
              sql.withTransaction(
                sql`DELETE FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
              ).pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))),
                Effect.asVoid
              )
            )
          }
          const completion = yield* Deferred.make<void, ReplicaError.ReplicaError>()
          current.leaving = true
          current.leaveCompletion = completion
          const cleanup = Effect.suspend(() => current.runtime?.cancelReconciliation ?? Effect.void).pipe(
            Effect.andThen(deactivate(current, true)),
            Effect.andThen(
              sql.withTransaction(
                sql`DELETE FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
              ).pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(StorageUnavailable.make(cause))))
            ),
            Effect.tap(() =>
              removeContribution(current).pipe(
                Effect.andThen(Effect.sync(() => entries.delete(spaceId)))
              )
            ),
            Effect.tap(() =>
              reactivity.invalidate([
                ReactivityKey.membership(spaceId),
                ReactivityKey.spaces
              ])
            ),
            Effect.asVoid,
            Effect.tapError(() =>
              Effect.sync(() => {
                current.leaving = false
                current.leaveCompletion = undefined
              })
            ),
            Effect.exit,
            Effect.tap((exit) => Deferred.done(completion, exit)),
            Effect.asVoid
          )
          yield* Effect.forkIn(cleanup, parentScope, { startImmediately: true })
          return yield* restore(Deferred.await(completion))
        }))

      const space = (spaceId: Identity.SpaceId) =>
        Effect.suspend(() => {
          const entry = entries.get(spaceId)
          if (entry !== undefined && !entry.leaving) return Effect.succeed(entry.handle)
          return Effect.fail(new ReplicaError.SpaceNotJoined({ spaceId }))
        })

      const spaces = Effect.sync(() =>
        Array.from(entries.values())
          .filter((entry) => !entry.leaving)
          .toSorted((left, right) => left.spaceId.localeCompare(right.spaceId))
          .map((entry) => entry.handle)
      )

      const status = Ref.get(aggregate)

      const backgroundTurn = Effect.gen(function*() {
        const work = yield* Queue.take(backgroundQueue)
        if (work._tag === "Deactivate") {
          const result = yield* deactivate(work.entry, false, work.runtime, false).pipe(Effect.result)
          if (Result.isFailure(result)) yield* scheduleBackgroundRetry(work.entry)
          return
        }
        const spaceId = work.spaceId
        backgroundQueued.delete(spaceId)
        const entry = entries.get(spaceId)
        if (entry === undefined || entry.leaving) return
        let activeRuntime: ActiveRuntime | undefined
        const result = yield* Effect.acquireUseRelease(
          acquire(entry, false).pipe(Effect.tap((runtime) => {
            activeRuntime = runtime
            return Effect.void
          })),
          (runtime) => {
            if (workflow === undefined) return runtime.reconciler.sync
            return backgroundWorkflowTurns.withPermit(runtime.reconciler.sync)
          },
          () => release(entry)
        ).pipe(Effect.result)
        if (activeRuntime !== undefined) {
          const deactivation = yield* deactivate(
            entry,
            false,
            activeRuntime,
            Result.isSuccess(result)
          ).pipe(Effect.result)
          if (Result.isFailure(deactivation)) {
            yield* scheduleBackgroundRetry(entry)
            return
          }
        }
        if (Result.isFailure(result)) {
          yield* scheduleBackgroundRetry(entry)
        } else {
          entry.retryAttempt = 0
          entry.retryVersion += 1
        }
      })

      yield* Effect.forEach(
        Array.from({ length: backgroundConcurrency }),
        () => backgroundTurn.pipe(Effect.forever, Effect.forkScoped({ startImmediately: true })),
        { discard: true }
      )
      yield* retrySchedulerTurn.pipe(Effect.forever, Effect.forkScoped({ startImmediately: true }))

      const restored = yield* readMemberships(undefined).pipe(
        Effect.catchTags({
          SqlError: (cause) => Effect.fail(StorageUnavailable.make(cause)),
          SchemaError: (cause) =>
            Effect.fail(
              new ReplicaError.StorageCorrupt({
                message: "Client membership row is corrupt",
                cause
              })
            )
        })
      )
      for (const row of restored) {
        const entry = yield* createEntry(row)
        entries.set(row.space_id, entry)
        yield* addContribution(entry, false)
      }
      const configured: Array<Identity.SpaceId> = []
      if (options.initialSpaces !== undefined) configured.push(...options.initialSpaces)
      if (options.spaceId !== undefined) configured.push(options.spaceId)
      yield* Effect.forEach(Array.from(new Set(configured)).toSorted(), join, { discard: true })
      for (const row of restored) {
        const entry = entries.get(row.space_id)
        if (entry !== undefined && row.count > 0) yield* enqueueBackground(entry)
      }

      return Replica.Replica.of({ join, leave, spaces, space, status })
    })
  ).pipe(Layer.provideMerge(layerQueryReactivity))
}

export const layer = <D extends Definition.Any,>(options: Options<D>) =>
  makeLayer<D, never>(options, Effect.succeed(undefined))

export const layerWorkflow = <D extends Definition.Any,>(options: Options<D>) =>
  makeLayer<D, WorkflowEngine.WorkflowEngine>(options, WorkflowEngine.WorkflowEngine)
