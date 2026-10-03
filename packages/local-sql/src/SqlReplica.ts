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
import * as FiberMap from "effect/FiberMap"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlSchema from "effect/sql/SqlSchema"
import * as Stream from "effect/Stream"
import * as WorkflowEngine from "effect/workflow/WorkflowEngine"
import * as ConnectionLane from "./ConnectionLane.js"
import * as Codec from "./internal/codec.js"
import * as Configuration from "./internal/configuration.js"
import * as LosslessQueue from "./internal/losslessQueue.js"
import * as MutationDescriptor from "./internal/mutationDescriptor.js"
import * as Rows from "./internal/rows.js"
import * as SqliteIdentifier from "./internal/sqliteIdentifier.js"
import { isTransportFailure } from "./internal/transport.js"
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
  readonly clientId?: Identity.ClientId | undefined
  readonly defaultScope?: Protocol.ReplicationScope
  readonly maximumActiveSpaces?: number
  readonly foregroundActiveSpaces?: number
  readonly initialSpaces?: Iterable<Identity.SpaceId>
  readonly spaceId?: Identity.SpaceId
  readonly maximumPendingMutations?: number
  readonly evolution?: Evolution.Evolution
  readonly schemaEvolutionBatchSize?: number
  readonly schemaEvolutionBatchBytes?: number
  readonly retainedReceipts?: number
  readonly maximumReceipts?: number
  readonly retainedHistoryEntries?: number
  readonly maximumBootstrapEntities?: number
  readonly maximumBootstrapBytes?: number
  readonly maximumBootstrapPageBytes?: number
  readonly maximumSettlementSnapshotBytes?: number
  readonly retainedMutationIds?: number
  readonly migration?: Migrations.Options
  readonly pageSize?: number
  readonly receiptPersistBatchSize?: number
  readonly maximumBackgroundWait?: Duration.Input
  readonly reconciliationConcurrency?: number
  readonly foregroundReconciliationConcurrency?: number
  readonly retryDelay?: Duration.Input
  readonly maximumRetryDelay?: Duration.Input
  readonly maximumAttempts?: number
}

type ResolvedOptions<D extends Definition.Any,> =
  & Options<D>
  & {
    readonly defaultScope: Protocol.ReplicationScope
    readonly maximumActiveSpaces: number
    readonly foregroundActiveSpaces: number
    readonly retainedReceipts: number
    readonly maximumReceipts: number
    readonly retainedHistoryEntries: number
    readonly maximumBootstrapEntities: number
    readonly maximumBootstrapBytes: number
    readonly maximumBootstrapPageBytes: number
    readonly migration: Migrations.Options
  }

export const defaults = {
  maximumActiveSpaces: 16,
  foregroundActiveSpaces: 4,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 100_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  migration: { retryDelay: "100 millis", maximumAttempts: 8 }
} as const satisfies Partial<Options<Definition.Any>>

const resolveOptions = <D extends Definition.Any,>(input: Options<D>): ResolvedOptions<D> => ({
  ...defaults,
  defaultScope: Protocol.ReplicationScope.make({ models: input.definition.models.map((model) => model.name) }),
  ...input,
  migration: {
    retryDelay: input.migration?.retryDelay ?? defaults.migration.retryDelay,
    maximumAttempts: input.migration?.maximumAttempts ?? defaults.migration.maximumAttempts
  }
})

type BaseRequirements<D extends Definition.Any,> =
  | SqlClient.SqlClient
  | Crypto.Crypto
  | Reactivity.Reactivity
  | SyncEngine.SyncEngine
  | MutationRuntime.Handlers<D>
  | QueryExecutor.Handlers<D>

const operationPermits = Number.MAX_SAFE_INTEGER

interface ActiveRuntime {
  readonly foreground: boolean
  readonly scope: Scope.Closeable
  readonly operationGate: Semaphore.Semaphore
  readonly quarantineGate: Semaphore.Semaphore
  readonly preemption: Deferred.Deferred<void>
  readonly local: LocalStore.Service
  readonly queries: QueryExecutor.Service
  readonly reconciler: Reconciler.Service
  readonly reconciliation: Reconciler.ReconciliationService
  readonly cancelReconciliation: Effect.Effect<void>
}

interface RememberedEntry {
  readonly spaceId: Identity.SpaceId
  readonly membershipIncarnation: Identity.MembershipIncarnation
  handle: Replica.Space
  activation: Replica.Activation
  runtime: ActiveRuntime | undefined
  transition: Deferred.Deferred<void, ReplicaError.ReplicaError> | undefined
  foreground: boolean
  foregroundDemand: number
  settlementsRecorded: Deferred.Deferred<void>
  leases: number
  leaving: boolean
  dueWhileLeaving: boolean
  leaveCompletion: Deferred.Deferred<void, ReplicaError.ReplicaError> | undefined
  workflowRegistration: ReconciliationWorkflow.RegistrationService | undefined
  summaryStatus: ReplicaStatus.ReplicaStatus
  synced: boolean
  retryAttempt: number
  backgroundGeneration: number
  backgroundFailure: ReplicaError.ReplicaError | undefined
}

type BackgroundWork =
  | { readonly _tag: "Sync"; readonly spaceId: Identity.SpaceId }
  | { readonly _tag: "Deactivate"; readonly entry: RememberedEntry; readonly runtime: ActiveRuntime }

interface RetryWork {
  readonly entry: RememberedEntry
  readonly version: number
  readonly readyAt: number
  readonly transportGeneration: Option.Option<number>
}

const RememberedRow = Schema.Struct({
  space_id: Identity.SpaceId,
  membership_incarnation: Identity.MembershipIncarnation,
  desired_scope_json: Schema.String,
  replication_view_id: Schema.NullOr(Identity.ReplicationViewId),
  count: Schema.Int
})

const DesiredScopeRow = Schema.Struct({
  desired_scope_json: Schema.String
})

const addressedStatus = (
  spaceId: Identity.SpaceId,
  synced: boolean,
  status: ReplicaStatus.ReplicaStatus
): ReplicaStatus.SpaceStatus => ({ spaceId, synced, ...status })

const inactiveStatus = (entry: RememberedEntry, pending: number): ReplicaStatus.ReplicaStatus => {
  if (entry.backgroundFailure === undefined) return { _tag: "Idle", pending }
  return Reconciler.failureStatus(entry.backgroundFailure, pending)
}

type AggregateCounts = ReplicaStatus.Aggregate["counts"]
type AggregateCategory = keyof AggregateCounts

const statusCategories: {
  readonly [Tag in ReplicaStatus.ReplicaStatus["_tag"]]: AggregateCategory
} = {
  Idle: "idle",
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
  const synchronizing = spaces - counts.idle
  let state: ReplicaStatus.AggregateState = "Degraded"
  if (synchronizing === 0) state = "Idle"
  else if (counts.failed > 0) state = "Failed"
  else if (counts.needsAuthentication > 0) state = "NeedsAuthentication"
  else if (counts.online === synchronizing) state = "Online"
  else if (counts.offline === synchronizing) state = "Offline"
  else if (counts.connecting > 0) state = "Connecting"
  return { state, spaces, totalPending, counts }
}

const settledFor =
  <M extends Mutation.Any,>(mutation: M) => (settled: Replica.SettledMutation): settled is Replica.SettledMutation<M> =>
    settled.settlement.pending.envelope.name === mutation.name

const makeLayer = <D extends Definition.Any, R,>(
  input: Options<D>,
  workflowEngine: Effect.Effect<WorkflowEngine.WorkflowEngine["Service"] | undefined, never, R>
): Layer.Layer<
  Replica.Replica | QueryReactivity.QueryReactivity,
  ReplicaError.ReplicaError,
  BaseRequirements<D> | R
> => {
  const options = resolveOptions(input)
  const layerQueryReactivity = QueryReactivity.makeLayer()
  return Layer.effect(
    Replica.Replica,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const lane = yield* ConnectionLane.make({ maximumBackgroundWait: options.maximumBackgroundWait })
      const reactivity = yield* Reactivity.Reactivity
      const remote = yield* SyncEngine.SyncEngine
      const parentScope = yield* Effect.scope
      const rootContext = Context.add(
        yield* Effect.context<BaseRequirements<D> | QueryReactivity.QueryReactivity | R>(),
        ConnectionLane.ConnectionLane,
        lane
      )
      const entries = new Map<Identity.SpaceId, RememberedEntry>()
      const joining = new Map<Identity.SpaceId, Deferred.Deferred<void>>()
      const foregroundResidents = new Map<Identity.SpaceId, RememberedEntry>()
      const foregroundAdmitted = new Set<Identity.SpaceId>()
      const dropForegroundReservation = (entry: RememberedEntry) => {
        entry.foreground = false
        foregroundResidents.delete(entry.spaceId)
        foregroundAdmitted.delete(entry.spaceId)
      }
      const aggregate = yield* Ref.make(aggregateStatus(0, 0, {
        idle: 0,
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
      const credentialWaits = yield* FiberMap.make<Identity.MembershipIncarnation, void, never>()
      const retrySchedule: Array<RetryWork> = []
      let capacityChanged = yield* Deferred.make<void>()

      const clientId = yield* Migrations.client({
        definition: options.definition,
        clientId: options.clientId,
        migration: options.migration
      }).pipe(Effect.provideService(ConnectionLane.ConnectionLane, lane))

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
          sql`SELECT s.space_id, s.membership_incarnation, s.desired_scope_json, s.replication_view_id,
            COUNT(p.mutation_id) AS count
          FROM effect_local_client_spaces AS s
          LEFT JOIN effect_local_client_pending_data AS p
            ON p.space_id = s.space_id AND p.schema_generation = s.active_schema_generation
          GROUP BY s.space_id, s.membership_incarnation, s.desired_scope_json, s.replication_view_id
          ORDER BY s.space_id`
      })
      const readMembership = SqlSchema.findOneOption({
        Request: Identity.SpaceId,
        Result: RememberedRow,
        execute: (spaceId) =>
          sql`SELECT s.space_id, s.membership_incarnation, s.desired_scope_json, s.replication_view_id,
            COUNT(p.mutation_id) AS count
          FROM effect_local_client_spaces AS s
          LEFT JOIN effect_local_client_pending_data AS p
            ON p.space_id = s.space_id AND p.schema_generation = s.active_schema_generation
          WHERE s.space_id = ${spaceId}
          GROUP BY s.space_id, s.membership_incarnation, s.desired_scope_json, s.replication_view_id`
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
      const readDesiredScope = SqlSchema.findOneOption({
        Request: Identity.SpaceId,
        Result: DesiredScopeRow,
        execute: (spaceId) => sql`SELECT desired_scope_json FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
      })
      const durableScope = Effect.fnUntraced(function*(spaceId: Identity.SpaceId) {
        const row = yield* lane.withStatement(readDesiredScope(spaceId)).pipe(
          Effect.catchTags({
            SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.StorageCorrupt({
                  message: "Client membership row is corrupt",
                  cause
                })
              )
          })
        )
        if (Option.isNone(row)) return yield* new ReplicaError.SpaceUnavailable({ spaceId })
        return yield* decodeScope(row.value.desired_scope_json)
      })

      const signalCapacity = Effect.sync(() => {
        const previous = capacityChanged
        capacityChanged = Deferred.makeUnsafe<void>()
        Deferred.doneUnsafe(previous, Exit.void)
      })

      const recordReplicationView = (entry: RememberedEntry, installed: boolean) =>
        Effect.suspend(() => {
          if (entry.synced === installed) return Effect.void
          entry.synced = installed
          return reactivity.invalidate([ReactivityKey.status(entry.spaceId)])
        })

      const publishSettlements = (entry: RememberedEntry) =>
        Effect.suspend(() => {
          const recorded = entry.settlementsRecorded
          entry.settlementsRecorded = Deferred.makeUnsafe<void>()
          return Deferred.succeed(recorded, undefined)
        }).pipe(Effect.asVoid)

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

      const buildRuntime = Effect.fnUntraced(function*(
        entry: RememberedEntry,
        generation: number,
        foreground: boolean,
        childScope: Scope.Closeable
      ) {
        const spaceId = entry.spaceId
        if (!foreground) {
          yield* Effect.acquireRelease(
            backgroundActiveSpaces.take(1),
            () => backgroundActiveSpaces.release(1),
            { interruptible: true }
          ).pipe(Scope.provide(childScope))
        }
        const replicationScope = yield* durableScope(spaceId)
        const layerMutationRuntime = MutationRuntime.layer(options.definition, options.evolution)
        const reconcilerReady = yield* Deferred.make<Reconciler.Service>()
        const layerLocalStore = LocalStore.layer({
          ...options,
          clientId,
          scope: replicationScope,
          spaceId,
          onSettlementsRecorded: publishSettlements(entry),
          onReplicationView: (installed) => recordReplicationView(entry, installed),
          onMutationsCommitted: (pending) =>
            updatePendingContribution(entry, pending).pipe(
              Effect.andThen(Deferred.await(reconcilerReady)),
              Effect.flatMap((ready) => ready.schedule)
            )
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
            onStatusChange: (status) => updateContribution(entry, status),
            onReconciled: forgetBackgroundFailure(entry)
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
            const scheduler = yield* ReconciliationWorkflow.layerScheduler({ ...options, clientId, spaceId }).pipe(
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
              schedule: Effect.void,
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
              onStatusChange: (status) => updateContribution(entry, status),
              onReconciled: forgetBackgroundFailure(entry)
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
              schedule: manager.schedule(spaceId),
              status: manager.status(spaceId),
              shutdown: Effect.void
            })
          } else {
            reconciler = Reconciler.Reconciler.of({
              sync: reconciliation.sync,
              notify: local.requestReconciliation.pipe(Effect.asVoid),
              schedule: Effect.void,
              status: reconciliation.status,
              shutdown: Effect.void
            })
          }
        }
        yield* Deferred.succeed(reconcilerReady, reconciler)
        const operationGate = yield* Semaphore.make(operationPermits)
        const quarantineGate = yield* Semaphore.make(1)
        const preemption = yield* Deferred.make<void>()
        return {
          foreground,
          scope: childScope,
          operationGate,
          quarantineGate,
          preemption,
          local,
          queries,
          reconciler,
          reconciliation,
          cancelReconciliation
        } satisfies ActiveRuntime
      })

      const initialize = (
        entry: RememberedEntry,
        generation: number,
        foreground: boolean
      ): Effect.Effect<ActiveRuntime, ReplicaError.ReplicaError> =>
        Effect.uninterruptibleMask((restore) =>
          Scope.fork(parentScope).pipe(
            Effect.flatMap((childScope) =>
              restore(buildRuntime(entry, generation, foreground, childScope)).pipe(
                Effect.onExit((exit) => {
                  if (Exit.isFailure(exit)) return Scope.close(childScope, exit)
                  return Effect.void
                })
              )
            )
          )
        )

      const enqueueBackground = (entry: RememberedEntry, resetRetry = true) =>
        Effect.suspend(() => {
          if (entry.leaving) {
            entry.dueWhileLeaving = true
            return Effect.void
          }
          if (backgroundQueued.has(entry.spaceId)) return Effect.void
          if (resetRetry) {
            entry.retryAttempt = 0
            entry.backgroundGeneration += 1
          }
          backgroundQueued.add(entry.spaceId)
          return Queue.offer(backgroundQueue, { _tag: "Sync", spaceId: entry.spaceId }).pipe(Effect.asVoid)
        })

      const scheduleBackgroundRetry = Effect.fnUntraced(function*(
        entry: RememberedEntry,
        transportGeneration: Option.Option<number>
      ) {
        entry.retryAttempt += 1
        entry.backgroundGeneration += 1
        const readyAt = (yield* Clock.currentTimeMillis) + Configuration.retryMillis(retryTiming, entry.retryAttempt)
        yield* Queue.offer(retryQueue, { entry, version: entry.backgroundGeneration, readyAt, transportGeneration })
      })

      const forgetBackgroundFailure = (entry: RememberedEntry) =>
        Effect.suspend(() => {
          entry.backgroundGeneration += 1
          entry.backgroundFailure = undefined
          return FiberMap.remove(credentialWaits, entry.membershipIncarnation)
        })

      const reportBackgroundFailure = (entry: RememberedEntry, failure: ReplicaError.ReplicaError | undefined) =>
        Effect.suspend(() => {
          entry.backgroundFailure = failure
          let contribute = Effect.void
          if (entry.activation === "Inactive") {
            contribute = modifyContribution(entry, (current) => inactiveStatus(entry, current.pending))
          }
          return FiberMap.remove(credentialWaits, entry.membershipIncarnation).pipe(
            Effect.andThen(contribute),
            Effect.andThen(reactivity.invalidate([ReactivityKey.status(entry.spaceId)]))
          )
        })

      const settleBackgroundTurn = Effect.fnUntraced(function*(
        entry: RememberedEntry,
        generation: number,
        failure: ReplicaError.ReplicaError,
        transportGeneration: Option.Option<number>
      ) {
        if (entry.backgroundGeneration !== generation) return
        if (Reconciler.isTransientFailure(failure)) {
          yield* reportBackgroundFailure(entry, undefined)
          let retryTransport = Option.none<number>()
          if (isTransportFailure(failure)) retryTransport = transportGeneration
          yield* scheduleBackgroundRetry(entry, retryTransport)
          return
        }
        yield* reportBackgroundFailure(entry, failure)
        if (failure._tag !== "CredentialRejected" || failure.credentialGeneration === undefined) return
        const wait = remote.waitForCredentialChange(failure.credentialGeneration).pipe(
          Effect.andThen(enqueueBackground(entry))
        )
        yield* FiberMap.run(credentialWaits, entry.membershipIncarnation, wait)
      })

      const releaseTransportRetries = Effect.gen(function*() {
        const current = yield* remote.transportGeneration
        const now = yield* Clock.currentTimeMillis
        for (let index = 0; index < retrySchedule.length; index++) {
          const work = retrySchedule[index]
          if (Option.isSome(work.transportGeneration) && work.transportGeneration.value < current) {
            retrySchedule[index] = { ...work, readyAt: now, transportGeneration: Option.none() }
          }
        }
        retrySchedule.sort((left, right) => left.readyAt - right.readyAt)
      })

      const awaitTransportRetry = Effect.suspend(() => {
        let oldest: number | undefined
        for (const work of retrySchedule) {
          if (Option.isSome(work.transportGeneration)) {
            oldest = Math.min(oldest ?? work.transportGeneration.value, work.transportGeneration.value)
          }
        }
        if (oldest === undefined) return Effect.never
        return remote.waitForTransportChange(oldest)
      })

      const retrySchedulerTurn = Effect.suspend(() => {
        const insert = (work: RetryWork) => {
          retrySchedule.push(work)
          retrySchedule.sort((left, right) => left.readyAt - right.readyAt)
        }
        if (retrySchedule.length === 0) {
          return LosslessQueue.take(retryQueue).pipe(
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
              if (next.entry.backgroundGeneration !== next.version) return Effect.void
              return enqueueBackground(next.entry, false)
            }
            return Effect.raceAllFirst([
              LosslessQueue.take(retryQueue).pipe(Effect.map((work) => Option.some(work))),
              Effect.sleep(Duration.millis(next.readyAt - now)).pipe(Effect.as(Option.none<RetryWork>())),
              awaitTransportRetry.pipe(Effect.andThen(releaseTransportRetries), Effect.as(Option.none<RetryWork>()))
            ]).pipe(
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
            const pending = entry.transition
            if (pending !== undefined) yield* restore(Deferred.await(pending))
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
            yield* Deferred.succeed(runtime.preemption, undefined)
            yield* restore(Deferred.await(changed))
            return yield* deactivate(entry, explicit, expectedRuntime, enqueuePending)
          }
          const completion = Deferred.makeUnsafe<void, ReplicaError.ReplicaError>()
          entry.activation = "Deactivating"
          entry.transition = completion
          dropForegroundReservation(entry)
          yield* invalidateActivation(entry.spaceId)
          const shutdown = Scope.close(runtime.scope, Exit.void)
          const result = yield* runtime.operationGate.withPermits(operationPermits)(shutdown).pipe(Effect.exit)
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
          const count = yield* lane.withStatement(pendingCount(entry.spaceId)).pipe(
            Effect.catchTags({
              SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
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
          yield* updateContribution(entry, inactiveStatus(entry, count.count))
          if (enqueuePending && count.count > 0) yield* enqueueBackground(entry)
          return true
        }))

      const hasForegroundRuntime = (entry: RememberedEntry) =>
        entry.activation === "Active" && entry.runtime !== undefined && entry.runtime.foreground

      const ensureForegroundCapacity = (entry: RememberedEntry): Effect.Effect<void, ReplicaError.ReplicaError> =>
        Effect.suspend(() => {
          if (hasForegroundRuntime(entry)) return Effect.void
          let admitted = 0
          let victim: RememberedEntry | undefined
          for (const candidate of foregroundResidents.values()) {
            if (candidate === entry || !foregroundAdmitted.has(candidate.spaceId)) continue
            admitted += 1
            if (victim === undefined && candidate.activation === "Active" && candidate.leases === 0) {
              victim = candidate
            }
          }
          if (admitted < options.foregroundActiveSpaces) {
            foregroundAdmitted.add(entry.spaceId)
            return Effect.void
          }
          if (victim !== undefined) {
            return deactivate(victim, false).pipe(Effect.andThen(ensureForegroundCapacity(entry)))
          }
          const changed = capacityChanged
          return Deferred.await(changed).pipe(Effect.andThen(ensureForegroundCapacity(entry)))
        })

      const releaseForegroundReservation = (entry: RememberedEntry) =>
        Effect.suspend(() => {
          dropForegroundReservation(entry)
          const runtime = entry.runtime
          if (entry.activation !== "Active" || runtime === undefined || runtime.foreground || entry.leases > 0) {
            return signalCapacity
          }
          return signalCapacity.pipe(Effect.andThen(enqueueBackground(entry)))
        })

      const transition = (
        entry: RememberedEntry,
        foreground: boolean
      ): Effect.Effect<ActiveRuntime, ReplicaError.ReplicaError> =>
        Effect.uninterruptibleMask(Effect.fnUntraced(function*(restore) {
          if (entries.get(entry.spaceId) !== entry || entry.leaving) {
            return yield* new ReplicaError.SpaceUnavailable({ spaceId: entry.spaceId })
          }
          if (foreground && !hasForegroundRuntime(entry)) {
            entry.foreground = true
            foregroundResidents.delete(entry.spaceId)
            foregroundResidents.set(entry.spaceId, entry)
            yield* restore(ensureForegroundCapacity(entry))
          }
          if (foreground) {
            foregroundResidents.delete(entry.spaceId)
            foregroundResidents.set(entry.spaceId, entry)
          }
          let retiring: ActiveRuntime | undefined
          if (entry.activation === "Active" && entry.runtime !== undefined) {
            if (!foreground || entry.runtime.foreground) return entry.runtime
            if (entry.leases > 0) {
              const changed = capacityChanged
              yield* Deferred.succeed(entry.runtime.preemption, undefined)
              yield* restore(Deferred.await(changed))
              return yield* transition(entry, foreground)
            }
            retiring = entry.runtime
          } else if (entry.activation === "Activating" || entry.activation === "Deactivating") {
            const pending = entry.transition
            if (pending !== undefined) yield* restore(Deferred.await(pending))
            return yield* transition(entry, foreground)
          }
          if (foreground && !entry.foreground) return yield* transition(entry, foreground)
          const completion = Deferred.makeUnsafe<void, ReplicaError.ReplicaError>()
          const generation = ++nextGeneration
          entry.activation = "Activating"
          entry.transition = completion
          entry.runtime = undefined
          yield* modifyContribution(entry, (current) => ({ _tag: "Connecting", pending: current.pending }))
          yield* invalidateActivation(entry.spaceId)
          const startRuntime = restore(initialize(entry, generation, foreground))
          let start = startRuntime
          if (retiring !== undefined) {
            start = retiring.operationGate.withPermits(operationPermits)(Scope.close(retiring.scope, Exit.void)).pipe(
              Effect.andThen(restore(
                lane.withStatement(pendingCount(entry.spaceId)).pipe(
                  Effect.catchTags({
                    SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
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
                  Effect.flatMap((count) => updateContribution(entry, { _tag: "Connecting", pending: count.count }))
                )
              )),
              Effect.andThen(startRuntime)
            )
          }
          const result = yield* start.pipe(Effect.exit)
          if (Exit.isSuccess(result)) {
            entry.runtime = result.value
            entry.activation = "Active"
            entry.transition = undefined
            if (foreground) entry.backgroundGeneration += 1
            yield* Deferred.succeed(completion, undefined)
            yield* invalidateActivation(entry.spaceId)
            yield* signalCapacity
            return result.value
          }
          entry.activation = "Inactive"
          entry.transition = undefined
          dropForegroundReservation(entry)
          yield* modifyContribution(entry, (current) => inactiveStatus(entry, current.pending))
          if (Exit.hasInterrupts(result)) yield* Deferred.succeed(completion, undefined)
          else yield* Deferred.done(completion, result)
          yield* invalidateActivation(entry.spaceId)
          yield* signalCapacity
          if (retiring !== undefined) yield* enqueueBackground(entry)
          return yield* result
        }))

      const activate = (
        entry: RememberedEntry,
        foreground: boolean
      ): Effect.Effect<ActiveRuntime, ReplicaError.ReplicaError> => {
        if (!foreground) return transition(entry, false)
        return Effect.uninterruptibleMask((restore) => {
          entry.foregroundDemand += 1
          return restore(transition(entry, true)).pipe(
            Effect.onExit((exit) => {
              entry.foregroundDemand -= 1
              if (
                Exit.isSuccess(exit) ||
                entry.foregroundDemand > 0 ||
                !entry.foreground ||
                hasForegroundRuntime(entry)
              ) return Effect.void
              return releaseForegroundReservation(entry)
            })
          )
        })
      }

      const acquire = (
        entry: RememberedEntry,
        foreground: boolean,
        restore: (
          effect: Effect.Effect<ActiveRuntime, ReplicaError.ReplicaError>
        ) => Effect.Effect<ActiveRuntime, ReplicaError.ReplicaError>
      ): Effect.Effect<ActiveRuntime, ReplicaError.ReplicaError> =>
        restore(activate(entry, foreground)).pipe(
          Effect.flatMap((runtime) => {
            if (entry.activation !== "Active" || entry.runtime !== runtime) return acquire(entry, foreground, restore)
            entry.leases += 1
            if (foreground) {
              foregroundResidents.delete(entry.spaceId)
              foregroundResidents.set(entry.spaceId, entry)
            }
            return Effect.succeed(runtime)
          })
        )

      const release = (entry: RememberedEntry) =>
        Effect.sync(() => {
          entry.leases = Math.max(0, entry.leases - 1)
        }).pipe(Effect.andThen(signalCapacity))

      const withLease = <A, E extends { readonly _tag: string },>(
        entry: RememberedEntry,
        foreground: boolean,
        use: (runtime: ActiveRuntime) => Effect.Effect<A, E>
      ): Effect.Effect<A, E | ReplicaError.ReplicaError> =>
        Effect.uninterruptibleMask((restore) =>
          acquire(entry, foreground, restore).pipe(
            Effect.flatMap((runtime) => restore(use(runtime)).pipe(Effect.ensuring(release(entry))))
          )
        )

      const withActive = <A, E extends { readonly _tag: string },>(
        entry: RememberedEntry,
        use: (runtime: ActiveRuntime) => Effect.Effect<A, E>
      ): Effect.Effect<A, E | ReplicaError.ReplicaError> =>
        withLease(entry, true, (runtime) => {
          const operation = checkRuntime(entry, runtime).pipe(Effect.andThen(use(runtime)))
          return runtime.operationGate.withPermit(operation)
        })

      const withResidentRuntime = <A, E extends { readonly _tag: string },>(
        entry: RememberedEntry,
        use: (runtime: ActiveRuntime) => Effect.Effect<A, E>
      ): Effect.Effect<A, E | ReplicaError.ReplicaError> =>
        Effect.suspend(() => {
          const runtime = entry.runtime
          if (
            entries.get(entry.spaceId) !== entry || entry.leaving || entry.activation !== "Active" ||
            runtime === undefined
          ) return withActive(entry, use)
          return runtime.operationGate.withPermit(Effect.suspend(() => {
            if (entry.activation !== "Active" || entry.runtime !== runtime || entry.leaving) {
              return Effect.succeed(Option.none<A>())
            }
            return use(runtime).pipe(Effect.map(Option.some))
          })).pipe(
            Effect.flatMap(Option.match({
              onNone: () => withResidentRuntime(entry, use),
              onSome: Effect.succeed
            }))
          )
        })

      const settledStream = (
        entry: RememberedEntry,
        from: Replica.SettlementStart,
        mutationName?: string
      ): Stream.Stream<Replica.SettledMutation, ReplicaError.ReplicaError> =>
        Stream.unwrap(
          withResidentRuntime(entry, (runtime) => runtime.local.resolveSettlementStart(from)).pipe(
            Effect.map((start) =>
              Stream.paginate(
                start,
                Effect.fnUntraced(function*(cursor: number) {
                  while (true) {
                    const recorded = entry.settlementsRecorded
                    const settled = yield* withResidentRuntime(
                      entry,
                      (runtime) => runtime.local.readSettlements({ after: cursor, mutationName })
                    )
                    if (settled.length > 0) {
                      return [settled, Option.some<number>(settled[settled.length - 1].sequence)] as const
                    }
                    yield* Deferred.await(recorded)
                  }
                })
              )
            )
          )
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
        return {
          spaceId: entry.spaceId,
          scope: Effect.suspend(() => {
            if (entries.get(entry.spaceId) !== entry || entry.leaving) {
              return Effect.fail(new ReplicaError.SpaceUnavailable({ spaceId: entry.spaceId }))
            }
            return durableScope(entry.spaceId)
          }),
          setScope: (nextScope) =>
            withActive(entry, (runtime) => runtime.local.setScope(nextScope)).pipe(
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
          mutate: (mutation, payload, mutateOptions) =>
            withActive(entry, (runtime) => runtime.local.mutate(mutation, payload, mutateOptions)),
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
          settlements: (settlementOptions) => settledStream(entry, settlementOptions?.from ?? "live"),
          settlementsFor: (mutation, settlementOptions) =>
            Stream.unwrap(
              MutationDescriptor.validate(options.definition, mutation).pipe(
                Effect.as(
                  settledStream(entry, settlementOptions?.from ?? "live", mutation.name).pipe(
                    Stream.filter(settledFor(mutation))
                  )
                )
              )
            ),
          resolveSettlementStart: (from) => withActive(entry, (runtime) => runtime.local.resolveSettlementStart(from)),
          acknowledgeSettlements: (sequence) =>
            withActive(entry, (runtime) => runtime.local.acknowledgeSettlements(sequence)),
          quarantine: withActive(entry, (runtime) => runtime.local.quarantine),
          discardQuarantined: (mutationId) =>
            withActive(entry, (runtime) =>
              runtime.quarantineGate.withPermit(
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
                })()
              ).pipe(
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
              runtime.quarantineGate.withPermit(
                Effect.fnUntraced(function*() {
                  yield* MutationDescriptor.validate(options.definition, mutation)
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
                })()
              ).pipe(
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
                Effect.map(([status, pending]) => addressedStatus(entry.spaceId, entry.synced, { ...status, pending }))
              )
            }
            return lane.withStatement(pendingCount(entry.spaceId)).pipe(
              Effect.catchTags({
                SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
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
              Effect.map((row) => {
                if (entry.activation === "Activating") {
                  return addressedStatus(entry.spaceId, entry.synced, { _tag: "Connecting", pending: row.count })
                }
                return addressedStatus(entry.spaceId, entry.synced, inactiveStatus(entry, row.count))
              })
            )
          })
        }
      }

      const createEntry = Effect.fnUntraced(function*(row: typeof RememberedRow.Type) {
        yield* decodeScope(row.desired_scope_json)
        let handle: Replica.Space | undefined
        const entry: RememberedEntry = {
          spaceId: row.space_id,
          membershipIncarnation: row.membership_incarnation,
          get handle() {
            if (handle === undefined) handle = makeHandle(entry)
            return handle
          },
          activation: "Inactive",
          runtime: undefined,
          transition: undefined,
          foreground: false,
          foregroundDemand: 0,
          settlementsRecorded: Deferred.makeUnsafe<void>(),
          leases: 0,
          leaving: false,
          dueWhileLeaving: false,
          leaveCompletion: undefined,
          workflowRegistration: undefined,
          summaryStatus: { _tag: "Idle", pending: row.count },
          synced: row.replication_view_id !== null,
          retryAttempt: 0,
          backgroundGeneration: 0,
          backgroundFailure: undefined
        }
        if (workflow !== undefined) {
          const lease = ReconciliationWorkflow.RuntimeLease.of({
            acquire: Effect.gen(function*() {
              const foreground = entry.foreground
              const leaseRuntime = yield* Effect.uninterruptibleMask((restore) =>
                acquire(entry, foreground, restore).pipe(
                  Effect.tap((runtime) =>
                    Effect.addFinalizer(() =>
                      release(entry).pipe(
                        Effect.andThen(Queue.offer(backgroundQueue, { _tag: "Deactivate", entry, runtime })),
                        Effect.asVoid
                      )
                    )
                  )
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
            clientId,
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
            ${SqliteIdentifier.random(sql, "inc")}, ${options.definition.hash},
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
              lane.withTransaction(insertMembership(spaceId)).pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause }))),
                Effect.andThen(
                  lane.withStatement(readMembership(spaceId)).pipe(
                    Effect.catchTags({
                      SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
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
              lane.withTransaction(
                sql`DELETE FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
              ).pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause }))),
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
              lane.withTransaction(
                sql`DELETE FROM effect_local_client_spaces WHERE space_id = ${spaceId}`
              ).pipe(
                Effect.catchTag("SqlError", (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })))
              )
            ),
            Effect.tap(() =>
              removeContribution(current).pipe(
                Effect.andThen(Effect.sync(() => {
                  entries.delete(spaceId)
                  current.backgroundGeneration += 1
                })),
                Effect.andThen(FiberMap.remove(credentialWaits, current.membershipIncarnation)),
                Effect.andThen(publishSettlements(current))
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
              Effect.suspend(() => {
                current.leaving = false
                current.leaveCompletion = undefined
                if (!current.dueWhileLeaving) return Effect.void
                current.dueWhileLeaving = false
                return enqueueBackground(current)
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
          .sort((left, right) => left.spaceId.localeCompare(right.spaceId))
          .map((entry) => entry.handle)
      )

      const status = Ref.get(aggregate)

      const backgroundTurn = Effect.gen(function*() {
        const work = yield* LosslessQueue.take(backgroundQueue)
        if (work._tag === "Deactivate") {
          const result = yield* deactivate(work.entry, false, work.runtime, false).pipe(Effect.result)
          if (Result.isFailure(result)) {
            const generation = work.entry.backgroundGeneration
            yield* settleBackgroundTurn(work.entry, generation, result.failure, Option.none())
          }
          return
        }
        const spaceId = work.spaceId
        backgroundQueued.delete(spaceId)
        const entry = entries.get(spaceId)
        if (entry === undefined) return
        if (entry.leaving) {
          entry.dueWhileLeaving = true
          return
        }
        const generation = entry.backgroundGeneration
        let activeRuntime: ActiveRuntime | undefined
        const transportGeneration = yield* remote.transportGeneration
        const result = yield* withLease(entry, false, (runtime) => {
          activeRuntime = runtime
          let sync = runtime.reconciler.sync
          if (workflow !== undefined) sync = backgroundWorkflowTurns.withPermit(sync)
          return Effect.raceFirst(sync, Deferred.await(runtime.preemption))
        }).pipe(Effect.result)
        if (activeRuntime !== undefined) {
          if (activeRuntime.foreground) return
          const deactivation = yield* deactivate(
            entry,
            false,
            activeRuntime,
            Result.isSuccess(result)
          ).pipe(Effect.result)
          if (Result.isFailure(deactivation)) {
            yield* settleBackgroundTurn(entry, generation, deactivation.failure, Option.none())
            return
          }
        }
        if (Result.isFailure(result)) {
          yield* settleBackgroundTurn(entry, generation, result.failure, Option.some(transportGeneration))
        }
      })

      yield* Effect.forEach(
        Array.from({ length: backgroundConcurrency }),
        () =>
          backgroundTurn.pipe(
            Effect.forever,
            Effect.provideService(ConnectionLane.Priority, "Background"),
            Effect.forkScoped({ startImmediately: true })
          ),
        { discard: true }
      )
      yield* retrySchedulerTurn.pipe(
        Effect.forever,
        Effect.provideService(ConnectionLane.Priority, "Background"),
        Effect.forkScoped({ startImmediately: true })
      )

      const restored = yield* lane.withStatement(readMemberships(undefined)).pipe(
        Effect.catchTags({
          SqlError: (cause) => Effect.fail(new ReplicaError.StorageUnavailable({ cause })),
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
      yield* Effect.forEach(Array.from(new Set(configured)).sort(), join, { discard: true })
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
