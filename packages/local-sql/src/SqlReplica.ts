import type * as Definition from "@lucas-barake/effect-local/Definition"
import type * as Evolution from "@lucas-barake/effect-local/Evolution"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as Mutation from "@lucas-barake/effect-local/Mutation"
import type * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Quarantine from "@lucas-barake/effect-local/Quarantine"
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
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
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
  readonly scope: Protocol.ReplicationScope
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
    online: spaces.filter((status) => status._tag === "Online" || status._tag === "SchemaUpdateAvailable").length,
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
): Layer.Layer<
  Replica.Replica | QueryReactivity.QueryReactivity,
  ReplicaError.ReplicaError,
  BaseRequirements<D> | R
> => {
  const queryReactivity = QueryReactivity.makeLayer()
  return Layer.effect(
    Replica.Replica,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const reactivity = yield* Reactivity.Reactivity
      const remote = yield* SyncEngine.SyncEngine
      const parentScope = yield* Effect.scope
      const rootContext = yield* Effect.context<BaseRequirements<D> | QueryReactivity.QueryReactivity | R>()
      const entries = new Map<Identity.SpaceId, Entry>()
      let nextGeneration = 0
      const workflow = yield* workflowEngine
      let manager: Reconciler.ManagerService | undefined
      if (workflow === undefined) {
        if (options.reconciliationConcurrency === undefined) manager = yield* Reconciler.makeManager()
        else manager = yield* Reconciler.makeManager({ concurrency: options.reconciliationConcurrency })
      }

      yield* Migrations.client({
        definition: options.definition,
        clientId: options.clientId,
        migration: options.migration
      })

      const checkActive = (spaceId: Identity.SpaceId, generation: number) =>
        Effect.suspend(() => {
          const current = entries.get(spaceId)
          if (current?._tag === "Active" && current.generation === generation) return Effect.succeed(current)
          return Effect.fail(new ReplicaError.SpaceUnavailable({ spaceId }))
        })

      const initialize = (spaceId: Identity.SpaceId, generation: number) =>
        Effect.gen(function*() {
          const childScope = yield* Scope.fork(parentScope)
          return yield* Effect.gen(function*() {
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
              interruptReconciliation = reconciler.shutdown
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
              yield* Effect.acquireRelease(
                manager.register(managedSpace),
                () => manager.unregister(spaceId, generation)
              ).pipe(Scope.provide(childScope))
              reconciler = Reconciler.Reconciler.of({
                sync: manager.sync(spaceId),
                notify: manager.notify(spaceId).pipe(Effect.orDie),
                status: manager.status(spaceId).pipe(Effect.orDie),
                shutdown: Effect.void
              })
            }
            const operationGate = yield* Semaphore.make(1)
            const admit = <A, E,>(effect: Effect.Effect<A, E>) =>
              operationGate.withPermit(checkActive(spaceId, generation).pipe(Effect.andThen(effect)))
            const findReceipt = (mutationId: Identity.MutationId) =>
              local.receipt(mutationId).pipe(
                Effect.flatMap((found) => {
                  if (Option.isSome(found)) return Effect.succeed(found.value)
                  return Effect.fail(
                    new ReplicaError.ProtocolInvalid({
                      message: `Quarantined mutation ${mutationId} was not found or previously resolved`
                    })
                  )
                })
              )
            const continueCancellation = (initial: Option.Option<Quarantine.QuarantinedMutation>) =>
              Effect.gen(function*() {
                let canceled = initial
                while (Option.isSome(canceled)) {
                  yield* reconciler.sync
                  const canceledReceipt = yield* remote.discard({
                    envelope: canceled.value.envelope,
                    schema: local.schema
                  })
                  canceled = yield* local.resolveQuarantine(canceledReceipt, "Discard")
                }
              })
            const resolveDisposition = (
              receipt: Parameters<LocalStore.Service["resolveQuarantine"]>[0],
              disposition: LocalStore.QuarantineDisposition
            ) =>
              Effect.gen(function*() {
                const canceled = yield* local.resolveQuarantine(receipt, disposition)
                yield* continueCancellation(canceled)
              })
            const discardQuarantined = (mutationId: Identity.MutationId) =>
              Effect.gen(function*() {
                const found = yield* local.quarantineByMutation(mutationId)
                if (Option.isNone(found)) {
                  const continuation = yield* local.quarantineCancellation(mutationId)
                  yield* continueCancellation(continuation)
                  return yield* findReceipt(mutationId)
                }
                const receipt = yield* remote.discard({ envelope: found.value.envelope, schema: local.schema })
                yield* resolveDisposition(receipt, "Discard")
                return receipt
              }).pipe(Effect.ensuring(reconciler.notify))
            const handle: Replica.Space = {
              spaceId,
              mutate: (mutation, payload) =>
                admit(local.mutate(mutation, payload).pipe(Effect.tap(() => reconciler.notify))),
              get: (model, key) => admit(local.get(model, key)),
              query: (query, payload) => admit(queries.execute(query, payload)),
              receipt: (mutationId) => admit(local.receipt(mutationId)),
              quarantine: admit(local.quarantine),
              discardQuarantined: (mutationId) => admit(discardQuarantined(mutationId)),
              resubmitQuarantined: <M extends Mutation.Any,>(
                mutationId: Identity.MutationId,
                mutation: M,
                payload: Mutation.Payload<M>
              ) =>
                admit(
                  Effect.gen(function*() {
                    const found = yield* local.quarantineByMutation(mutationId)
                    if (Option.isNone(found)) {
                      const continuation = yield* local.quarantineCancellation(mutationId)
                      yield* continueCancellation(continuation)
                      return Quarantine.AlreadyResolved.make({ receipt: yield* findReceipt(mutationId) })
                    }
                    const item = found.value
                    if (mutation.name !== item.envelope.name) {
                      return yield* new ReplicaError.ProtocolInvalid({
                        message: `Resubmission mutation ${mutation.name} does not match ${item.envelope.name}`
                      })
                    }
                    const pending = yield* local.ensureQuarantineResubmission(mutationId, mutation, payload)
                    const receipt = yield* remote.discard({ envelope: item.envelope, schema: local.schema })
                    if (receipt._tag !== "Rejected" || receipt.origin !== "Quarantine") {
                      yield* resolveDisposition(receipt, "Resubmit")
                      return Quarantine.AlreadyResolved.make({ receipt })
                    }
                    yield* resolveDisposition(receipt, "Resubmit")
                    return Quarantine.Resubmitted.make({ pending })
                  }).pipe(Effect.ensuring(reconciler.notify))
                ),
              status: admit(
                Effect.all([reconciler.status, local.pendingCount]).pipe(
                  Effect.map(([status, pending]) => addressedStatus(spaceId, { ...status, pending }))
                )
              )
            }
            return {
              _tag: "Active",
              generation,
              scope: childScope,
              operationGate,
              local,
              queries,
              reconciler,
              interruptReconciliation,
              handle
            } satisfies ActiveEntry
          }).pipe(
            Effect.onExit((exit) => {
              if (Exit.isFailure(exit)) return Scope.close(childScope, exit)
              return Effect.void
            })
          )
        })

      const reserveJoin = (
        spaceId: Identity.SpaceId,
        completion: Deferred.Deferred<Replica.Space, ReplicaError.ReplicaError>
      ) => {
        const current = entries.get(spaceId)
        if (current?._tag === "Active") return { _tag: "Active" as const, entry: current }
        if (current?._tag === "Joining") return { _tag: "WaitJoin" as const, completion: current.completion }
        if (current?._tag === "Leaving") return { _tag: "WaitLeave" as const, completion: current.completion }
        const generation = ++nextGeneration
        entries.set(spaceId, { _tag: "Joining", generation, completion })
        return { _tag: "Initialize" as const, generation, completion }
      }

      const join = (spaceId: Identity.SpaceId): Effect.Effect<Replica.Space, ReplicaError.ReplicaError> =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function*() {
            const completion = yield* Deferred.make<Replica.Space, ReplicaError.ReplicaError>()
            const decision = reserveJoin(spaceId, completion)
            if (decision._tag === "Active") return decision.entry.handle
            if (decision._tag === "WaitJoin") return yield* restore(Deferred.await(decision.completion))
            if (decision._tag === "WaitLeave") {
              yield* restore(Deferred.await(decision.completion))
              return yield* join(spaceId)
            }
            const result = yield* restore(initialize(spaceId, decision.generation)).pipe(Effect.exit)
            if (result._tag === "Success") {
              entries.set(spaceId, result.value)
              yield* Deferred.succeed(decision.completion, result.value.handle)
              yield* reactivity.invalidate([`effect-local:space:${spaceId}`, "effect-local:status"])
              return result.value.handle
            }
            const current = entries.get(spaceId)
            if (current?._tag === "Joining" && current.generation === decision.generation) entries.delete(spaceId)
            const handleResult = Exit.map(result, (entry) => entry.handle)
            yield* Deferred.done(decision.completion, handleResult)
            return yield* handleResult
          })
        )

      const evictMembership = (spaceId: Identity.SpaceId) =>
        sql.withTransaction(sql`DELETE FROM effect_local_client_spaces WHERE space_id = ${spaceId}`).pipe(
          Effect.asVoid,
          Effect.catchIf(SqlError.isSqlError, (cause) => Effect.fail(StorageUnavailable.make(cause)))
        )

      const reserveLeave = (
        spaceId: Identity.SpaceId,
        completion: Deferred.Deferred<void, ReplicaError.ReplicaError>
      ) => {
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
      }

      const leave = (spaceId: Identity.SpaceId): Effect.Effect<void, ReplicaError.ReplicaError> =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function*() {
            const completion = yield* Deferred.make<void, ReplicaError.ReplicaError>()
            const decision = reserveLeave(spaceId, completion)
            if (decision._tag === "Missing") return yield* restore(evictMembership(spaceId))
            if (decision._tag === "WaitJoin") {
              yield* restore(Deferred.await(decision.completion))
              return yield* leave(spaceId)
            }
            if (decision._tag === "WaitLeave") {
              yield* restore(Deferred.await(decision.completion))
              return yield* Effect.void
            }
            let scopeClosed = false
            const result = yield* restore(decision.entry.operationGate.withPermit(
              decision.entry.interruptReconciliation.pipe(
                Effect.andThen(Effect.uninterruptible(
                  Scope.close(decision.entry.scope, Exit.void).pipe(
                    Effect.andThen(Effect.sync(() => {
                      scopeClosed = true
                    }))
                  )
                )),
                Effect.andThen(evictMembership(spaceId))
              )
            )).pipe(Effect.exit)
            if (result._tag === "Success") {
              entries.delete(spaceId)
              yield* Deferred.succeed(decision.completion, undefined)
              yield* reactivity.invalidate([`effect-local:space:${spaceId}`, "effect-local:status"])
              return yield* Effect.void
            }
            if (scopeClosed) entries.delete(spaceId)
            else entries.set(spaceId, decision.entry)
            yield* Deferred.done(decision.completion, result)
            return yield* result
          })
        )

      const space = (spaceId: Identity.SpaceId) =>
        Effect.suspend(() => {
          const entry = entries.get(spaceId)
          if (entry?._tag === "Active") return Effect.succeed(entry.handle)
          return Effect.fail(new ReplicaError.SpaceNotJoined({ spaceId }))
        })

      const spaces = Effect.sync(() =>
        Array.from(entries.entries())
          .filter((entry): entry is [Identity.SpaceId, ActiveEntry] => entry[1]._tag === "Active")
          .toSorted(([left], [right]) => left.localeCompare(right))
          .map(([, entry]) => entry.handle)
      )

      const status = Effect.gen(function*() {
        const active = Array.from(entries.values())
          .filter((entry): entry is ActiveEntry => entry._tag === "Active")
          .toSorted((left, right) => left.handle.spaceId.localeCompare(right.handle.spaceId))
        if (active.length === 0) return aggregateStatus([])
        const readPendingCounts = SqlSchema.findAll({
          Request: Schema.Array(Identity.SpaceId),
          Result: Rows.SpacePendingCountRow,
          execute: (spaceIds) =>
            sql`SELECT s.space_id, COUNT(p.mutation_id) AS count
            FROM effect_local_client_spaces AS s
            LEFT JOIN effect_local_client_pending_data AS p
              ON p.space_id = s.space_id AND p.schema_generation = s.active_schema_generation
            WHERE s.space_id IN ${sql.in(spaceIds)}
            GROUP BY s.space_id`
        })
        const counts = yield* readPendingCounts(active.map((entry) => entry.handle.spaceId)).pipe(
          Effect.mapError((cause) => {
            if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
            return new ReplicaError.StorageCorrupt({ message: "Client pending count row is corrupt", cause })
          })
        )
        const pendingBySpace = new Map(counts.map((row) => [row.space_id, row.count]))
        const statuses = yield* Effect.forEach(
          active,
          (entry) =>
            entry.reconciler.status.pipe(
              Effect.map((spaceStatus) =>
                addressedStatus(entry.handle.spaceId, {
                  ...spaceStatus,
                  pending: pendingBySpace.get(entry.handle.spaceId) ?? 0
                })
              )
            ),
          { concurrency: "unbounded" }
        )
        return aggregateStatus(statuses)
      })

      const readSpaces = SqlSchema.findAll({
        Request: Schema.Void,
        Result: Rows.SpaceIdRow,
        execute: () => sql`SELECT space_id FROM effect_local_client_spaces ORDER BY space_id`
      })
      const restored = yield* readSpaces(undefined).pipe(Effect.mapError((cause) => {
        if (SqlError.isSqlError(cause)) return StorageUnavailable.make(cause)
        return new ReplicaError.StorageCorrupt({ message: "Client membership row is corrupt", cause })
      }))
      const configured: Array<Identity.SpaceId> = []
      if (options.initialSpaces !== undefined) configured.push(...options.initialSpaces)
      if (options.spaceId !== undefined) configured.push(options.spaceId)
      const initial = Array.from(new Set([...restored.map((row) => row.space_id), ...configured])).toSorted()
      yield* Effect.forEach(initial, join, { discard: true })

      return Replica.Replica.of({ join, leave, spaces, space, status })
    })
  ).pipe(Layer.provideMerge(queryReactivity))
}

export const layer = <D extends Definition.Any,>(options: Options<D>) =>
  makeLayer<D, never>(options, Effect.succeed(undefined))

export const layerWorkflow = <D extends Definition.Any,>(options: Options<D>) =>
  makeLayer<D, WorkflowEngine.WorkflowEngine>(options, WorkflowEngine.WorkflowEngine)
