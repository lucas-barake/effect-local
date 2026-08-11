import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Evolution from "@lucas-barake/effect-local/Evolution"
import type * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import type * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as StorageUnavailable from "./internal/storageUnavailable.js"
import * as LocalStore from "./LocalStore.js"
import * as Migrations from "./Migrations.js"
import * as MutationRuntime from "./MutationRuntime.js"
import * as QueryExecutor from "./QueryExecutor.js"
import * as Reconciler from "./Reconciler.js"
import * as ReconciliationWorkflow from "./ReconciliationWorkflow.js"
import type * as SyncEngine from "./SyncEngine.js"

export interface Options<D extends Definition.Any,> {
  readonly definition: D
  readonly clientId: Identity.ClientId
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

interface ActiveEntry {
  readonly _tag: "Active"
  readonly generation: number
  readonly scope: Scope.Closeable
  readonly operationGate: Semaphore.Semaphore
  readonly local: LocalStore.Service
  readonly queries: QueryExecutor.Service
  readonly reconciler: Reconciler.Service
  readonly interruptReconciliation: Effect.Effect<void>
  readonly handle: Replica.Space
}

interface JoiningEntry {
  readonly _tag: "Joining"
  readonly generation: number
  readonly completion: Deferred.Deferred<Replica.Space, ReplicaError.ReplicaError>
}

interface LeavingEntry {
  readonly _tag: "Leaving"
  readonly generation: number
  readonly completion: Deferred.Deferred<void, ReplicaError.ReplicaError>
}

type Entry = ActiveEntry | JoiningEntry | LeavingEntry

const addressedStatus = (
  spaceId: Identity.SpaceId,
  status: ReplicaStatus.ReplicaStatus
): ReplicaStatus.SpaceStatus => ({ spaceId, ...status })

const aggregateStatus = (spaces: ReadonlyArray<ReplicaStatus.SpaceStatus>): ReplicaStatus.Aggregate => {
  const counts = {
    offline: spaces.filter((status) => status._tag === "Offline").length,
    connecting: spaces.filter((status) => status._tag === "Connecting").length,
    online: spaces.filter((status) => status._tag === "Online").length,
    needsAuthentication: spaces.filter((status) => status._tag === "NeedsAuthentication").length,
    failed: spaces.filter((status) => status._tag === "Failed").length
  }
  let state: ReplicaStatus.AggregateState = "Degraded"
  if (spaces.length === 0) state = "Idle"
  else if (counts.failed > 0) state = "Failed"
  else if (counts.needsAuthentication > 0) state = "NeedsAuthentication"
  else if (counts.online === spaces.length) state = "Online"
  else if (counts.offline === spaces.length) state = "Offline"
  else if (counts.connecting > 0) state = "Connecting"
  return {
    state,
    spaces,
    totalPending: spaces.reduce((total, status) => total + status.pending, 0),
    counts
  }
}

const makeLayer = <D extends Definition.Any, R,>(
  options: Options<D>,
  workflowEngine: Effect.Effect<WorkflowEngine.WorkflowEngine["Service"] | undefined, never, R>
): Layer.Layer<Replica.Replica, ReplicaError.ReplicaError, BaseRequirements<D> | R> =>
  Layer.effect(
    Replica.Replica,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const reactivity = yield* Reactivity.Reactivity
      const parentScope = yield* Effect.scope
      const rootContext = yield* Effect.context<BaseRequirements<D> | R>()
      const membershipGate = yield* Semaphore.make(1)
      const entries = new Map<Identity.SpaceId, Entry>()
      let nextGeneration = 0
      const workflow = yield* workflowEngine
      let manager: Reconciler.ManagerService | undefined
      if (workflow === undefined) manager = yield* Reconciler.makeManager

      yield* Migrations.client({
        definition: options.definition,
        clientId: options.clientId,
        migration: options.migration
      })

      const checkActive = (spaceId: Identity.SpaceId, generation: number) =>
        membershipGate.withPermit(Effect.gen(function*() {
          const current = entries.get(spaceId)
          if (current?._tag === "Active" && current.generation === generation) return current
          return yield* new ReplicaError.SpaceUnavailable({ spaceId })
        }))

      const initialize = (spaceId: Identity.SpaceId, generation: number) =>
        Effect.gen(function*() {
          const childScope = yield* Scope.fork(parentScope)
          const mutationRuntime = MutationRuntime.layer(options.definition, options.evolution)
          const localLayer = LocalStore.layer({ ...options, spaceId }).pipe(Layer.provide(mutationRuntime))
          const queryLayer = QueryExecutor.layer(options.definition, spaceId)
          let local: LocalStore.Service
          let queries: QueryExecutor.Service
          let reconciler: Reconciler.Service
          let interruptReconciliation = Effect.void
          if (workflow !== undefined) {
            const workflowContext = Context.add(rootContext, WorkflowEngine.WorkflowEngine, workflow)
            const runtime = yield* Layer.buildWithScope(
              Layer.mergeAll(
                localLayer,
                queryLayer,
                ReconciliationWorkflow.layer({ ...options, spaceId }).pipe(
                  Layer.provide(localLayer),
                  Layer.provide(Layer.succeed(ReconciliationWorkflow.RegistrationScope, parentScope))
                )
              ),
              childScope
            ).pipe(
              Effect.provide(workflowContext),
              Effect.tapError((error) => Scope.close(childScope, Exit.fail(error)))
            )
            local = Context.get(runtime, LocalStore.Store)
            queries = Context.get(runtime, QueryExecutor.QueryExecutor)
            reconciler = Context.get(runtime, Reconciler.Reconciler)
            interruptReconciliation = Effect.gen(function*() {
              const generations = yield* local.reconciliationGenerations
              if (generations.completed >= generations.requested) return
              const payload = ReconciliationWorkflow.Payload.make({
                schemaIdentity:
                  `${options.definition.schemaIdentity.version}:${options.definition.schemaIdentity.hash}`,
                spaceId,
                clientId: options.clientId,
                membershipIncarnation: local.membershipIncarnation,
                generation: generations.requested
              })
              yield* workflow.interruptUnsafe(
                ReconciliationWorkflow.make(payload),
                yield* ReconciliationWorkflow.executionId(payload)
              )
            }).pipe(Effect.orDie)
          } else {
            if (manager === undefined) return yield* Effect.die("Reconciler manager was not initialized")
            const runtime = yield* Layer.buildWithScope(
              Layer.mergeAll(
                localLayer,
                queryLayer,
                Reconciler.layerOnePass({ ...options, spaceId }).pipe(Layer.provide(localLayer))
              ),
              childScope
            ).pipe(
              Effect.provide(rootContext),
              Effect.tapError((error) => Scope.close(childScope, Exit.fail(error)))
            )
            local = Context.get(runtime, LocalStore.Store)
            queries = Context.get(runtime, QueryExecutor.QueryExecutor)
            const reconciliation = Context.get(runtime, Reconciler.Reconciliation)
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
            yield* manager.register(managedSpace)
            yield* Scope.addFinalizer(childScope, manager.unregister(spaceId, generation))
            reconciler = Reconciler.Reconciler.of({
              sync: manager.sync(spaceId),
              notify: manager.notify(spaceId).pipe(Effect.orDie),
              status: manager.status(spaceId).pipe(Effect.orDie)
            })
          }
          const operationGate = yield* Semaphore.make(1)
          let active!: ActiveEntry
          const admit = <A, E,>(effect: Effect.Effect<A, E>) =>
            operationGate.withPermit(checkActive(spaceId, generation).pipe(Effect.andThen(effect)))
          const handle: Replica.Space = {
            spaceId,
            mutate: (mutation, payload) =>
              admit(local.mutate(mutation, payload).pipe(Effect.tap(() => reconciler.notify))),
            get: (model, key) => admit(local.get(model, key)),
            query: (query, payload) => admit(queries.execute(query, payload)),
            receipt: (mutationId) => admit(local.receipt(mutationId)),
            status: admit(
              Effect.all([reconciler.status, local.pendingCount]).pipe(
                Effect.map(([status, pending]) => addressedStatus(spaceId, { ...status, pending }))
              )
            )
          }
          active = {
            _tag: "Active",
            generation,
            scope: childScope,
            operationGate,
            local,
            queries,
            reconciler,
            interruptReconciliation,
            handle
          }
          return active
        })

      const join = (spaceId: Identity.SpaceId): Effect.Effect<Replica.Space, ReplicaError.ReplicaError> =>
        Effect.gen(function*() {
          const completion = yield* Deferred.make<Replica.Space, ReplicaError.ReplicaError>()
          const decision = yield* membershipGate.withPermit(Effect.sync(() => {
            const current = entries.get(spaceId)
            if (current?._tag === "Active") return { _tag: "Active" as const, entry: current }
            if (current?._tag === "Joining") return { _tag: "WaitJoin" as const, completion: current.completion }
            if (current?._tag === "Leaving") return { _tag: "WaitLeave" as const, completion: current.completion }
            const generation = ++nextGeneration
            entries.set(spaceId, { _tag: "Joining", generation, completion })
            return { _tag: "Initialize" as const, generation, completion }
          }))
          if (decision._tag === "Active") return decision.entry.handle
          if (decision._tag === "WaitJoin") return yield* Deferred.await(decision.completion)
          if (decision._tag === "WaitLeave") {
            yield* Deferred.await(decision.completion)
            return yield* join(spaceId)
          }
          const result = yield* initialize(spaceId, decision.generation).pipe(Effect.result)
          if (result._tag === "Success") {
            yield* membershipGate.withPermit(Effect.sync(() => entries.set(spaceId, result.success)))
            yield* Deferred.succeed(decision.completion, result.success.handle)
            yield* reactivity.invalidate([`effect-local:space:${spaceId}`, "effect-local:status"])
            return result.success.handle
          }
          yield* membershipGate.withPermit(Effect.sync(() => {
            const current = entries.get(spaceId)
            if (current?._tag === "Joining" && current.generation === decision.generation) entries.delete(spaceId)
          }))
          yield* Deferred.fail(decision.completion, result.failure)
          return yield* Effect.fail(result.failure)
        })

      const leave = (spaceId: Identity.SpaceId): Effect.Effect<void, ReplicaError.ReplicaError> =>
        Effect.gen(function*() {
          const completion = yield* Deferred.make<void, ReplicaError.ReplicaError>()
          const decision = yield* membershipGate.withPermit(Effect.sync(() => {
            const current = entries.get(spaceId)
            if (current === undefined) return { _tag: "Missing" as const }
            if (current._tag === "Joining") return { _tag: "WaitJoin" as const, completion: current.completion }
            if (current._tag === "Leaving") return { _tag: "WaitLeave" as const, completion: current.completion }
            entries.set(spaceId, {
              _tag: "Leaving",
              generation: current.generation,
              completion
            })
            return { _tag: "Evict" as const, entry: current, completion }
          }))
          if (decision._tag === "Missing") return yield* Effect.void
          if (decision._tag === "WaitJoin") {
            yield* Deferred.await(decision.completion)
            return yield* leave(spaceId)
          }
          if (decision._tag === "WaitLeave") {
            yield* Deferred.await(decision.completion)
            return yield* Effect.void
          }
          const result = yield* decision.entry.interruptReconciliation.pipe(
            Effect.andThen(Scope.close(decision.entry.scope, Exit.void)),
            Effect.andThen(decision.entry.operationGate.withPermit(
              sql.withTransaction(sql`DELETE FROM effect_local_client_spaces WHERE space_id = ${spaceId}`).pipe(
                Effect.asVoid
              )
            )),
            Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause))),
            Effect.result
          )
          if (result._tag === "Success") {
            yield* membershipGate.withPermit(Effect.sync(() => entries.delete(spaceId)))
            yield* Deferred.succeed(decision.completion, undefined)
            yield* reactivity.invalidate([`effect-local:space:${spaceId}`, "effect-local:status"])
            return yield* Effect.void
          }
          yield* membershipGate.withPermit(Effect.sync(() => entries.delete(spaceId)))
          yield* Deferred.fail(decision.completion, result.failure)
          return yield* Effect.fail(result.failure)
        })

      const space = (spaceId: Identity.SpaceId) =>
        membershipGate.withPermit(Effect.gen(function*() {
          const entry = entries.get(spaceId)
          if (entry?._tag === "Active") return entry.handle
          return yield* new ReplicaError.SpaceNotJoined({ spaceId })
        }))

      const spaces = membershipGate.withPermit(Effect.sync(() =>
        Array.from(entries.entries())
          .filter((entry): entry is [Identity.SpaceId, ActiveEntry] => entry[1]._tag === "Active")
          .toSorted(([left], [right]) => left.localeCompare(right))
          .map(([, entry]) => entry.handle)
      ))

      const status = Effect.gen(function*() {
        const handles = yield* spaces
        const statuses = yield* Effect.forEach(handles, (handle) => handle.status, { concurrency: "unbounded" })
        return aggregateStatus(statuses)
      })

      const restored = yield* sql<{ readonly space_id: Identity.SpaceId }>`
        SELECT space_id FROM effect_local_client_spaces ORDER BY space_id`.pipe(
        Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))
      )
      const configured: Array<Identity.SpaceId> = []
      if (options.initialSpaces !== undefined) configured.push(...options.initialSpaces)
      if (options.spaceId !== undefined) configured.push(options.spaceId)
      const initial = Array.from(new Set([...restored.map((row) => row.space_id), ...configured])).toSorted()
      yield* Effect.forEach(initial, join, { discard: true })

      return Replica.Replica.of({ join, leave, spaces, space, status })
    })
  )

export const layer = <D extends Definition.Any,>(options: Options<D>) =>
  makeLayer<D, never>(options, Effect.succeed(undefined))

export const layerWorkflow = <D extends Definition.Any,>(options: Options<D>) =>
  makeLayer<D, WorkflowEngine.WorkflowEngine>(options, WorkflowEngine.WorkflowEngine)
