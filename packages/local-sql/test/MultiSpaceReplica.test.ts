import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Definition from "@lucas-barake/effect-local/Definition"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import type * as LocalStore from "../src/LocalStore.js"
import * as Reconciler from "../src/Reconciler.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceA = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const spaceB = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000002")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const clientHistory = {
  scope,
  retainedReceipts: 256,
  settlementCapacity: 64,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration: { retryDelay: "1 millis", maximumAttempts: 8 }
} satisfies Omit<SqlReplica.Options<typeof Domain.definition>, "definition" | "clientId">

const remoteService = SyncEngine.SyncEngine.of({
  waitForCredentialChange: () => Effect.never,
  submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
  discard: () => Effect.die("unexpected discard"),
  pull: () => Effect.never,
  bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
  watch: () => Stream.never
})
const layerRemote = Layer.succeed(SyncEngine.SyncEngine, remoteService)

const layerLive = SqlReplica.layer({
  ...clientHistory,
  definition: Domain.definition,
  clientId,
  initialSpaces: [spaceB, spaceA]
}).pipe(
  Layer.provide(Domain.layerHandlers),
  Layer.provide(layerRemote),
  Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
  Layer.provide(NodeCrypto.layer),
  Layer.provide(Reactivity.layer)
)

describe("multi space Replica", () => {
  it.effect(
    "isolates overlapping entity keys for two spaces in one database",
    Effect.fnUntraced(function*() {
      const context = yield* Layer.build(layerLive)
      const replica = Context.get(context, Replica.Replica)
      const spaces = yield* replica.spaces
      assert.deepStrictEqual(spaces.map((space) => space.spaceId), [spaceA, spaceB])
      const a = yield* replica.space(spaceA)
      const b = yield* replica.space(spaceB)

      yield* a.mutate(Domain.PutTodo, Domain.todo("same", "A"))
      yield* b.mutate(Domain.PutTodo, Domain.todo("same", "B"))

      assert.strictEqual(Option.getOrThrow(yield* a.get(Domain.Todo, "same")).title, "A")
      assert.strictEqual(Option.getOrThrow(yield* b.get(Domain.Todo, "same")).title, "B")
      const aggregate = yield* replica.status
      assert.deepStrictEqual(aggregate.spaces.map((status) => status.spaceId), [spaceA, spaceB])
      assert.strictEqual(aggregate.totalPending, 2)
    }, Effect.scoped)
  )

  it.effect(
    "evicts one space and leaves retained handles stale across rejoin",
    Effect.fnUntraced(function*() {
      const context = yield* Layer.build(layerLive)
      const replica = Context.get(context, Replica.Replica)
      const stale = yield* replica.space(spaceA)
      const b = yield* replica.space(spaceB)
      yield* stale.mutate(Domain.PutTodo, Domain.todo("same", "A"))
      yield* b.mutate(Domain.PutTodo, Domain.todo("same", "B"))

      yield* replica.leave(spaceA)
      const missingSpace = yield* replica.space(spaceA).pipe(Effect.result)
      assert.strictEqual(missingSpace._tag, "Failure")
      if (missingSpace._tag === "Failure") assert.strictEqual(missingSpace.failure._tag, "SpaceNotJoined")
      assert.strictEqual((yield* stale.get(Domain.Todo, "same").pipe(Effect.flip))._tag, "SpaceUnavailable")
      assert.strictEqual(Option.getOrThrow(yield* b.get(Domain.Todo, "same")).title, "B")

      const rejoined = yield* replica.join(spaceA)
      const rejoinedTodo = yield* rejoined.get(Domain.Todo, "same")
      const rejoinedTodoIsNone = Option.isNone(rejoinedTodo)
      assert.isTrue(rejoinedTodoIsNone)
      const staleMutation = yield* stale.mutate(Domain.PutTodo, Domain.todo("stale")).pipe(Effect.result)
      assert.strictEqual(staleMutation._tag, "Failure")
      if (staleMutation._tag === "Failure") assert.strictEqual(staleMutation.failure._tag, "SpaceUnavailable")
    }, Effect.scoped)
  )

  it.effect(
    "restores durable memberships when the Replica runtime restarts",
    Effect.fnUntraced(function*() {
      const layerDatabase = Layer.mergeAll(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer,
        Reactivity.layer
      )
      const databaseContext = yield* Layer.build(layerDatabase)
      const layerSql = Layer.succeed(SqlClient.SqlClient, Context.get(databaseContext, SqlClient.SqlClient))
      const layerCrypto = Layer.succeed(Crypto.Crypto, Context.get(databaseContext, Crypto.Crypto))
      const layerReactivity = Layer.succeed(
        Reactivity.Reactivity,
        Context.get(databaseContext, Reactivity.Reactivity)
      )
      const layerServices = Layer.mergeAll(
        layerSql,
        layerCrypto,
        layerReactivity
      )
      const replicaLayer = (initialSpaces?: Iterable<Identity.SpaceId>) => {
        const options = {
          ...clientHistory,
          definition: Domain.definition,
          clientId
        } satisfies SqlReplica.Options<typeof Domain.definition>
        let layerReplica = SqlReplica.layer(options)
        if (initialSpaces !== undefined) layerReplica = SqlReplica.layer({ ...options, initialSpaces })
        return layerReplica.pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(layerRemote),
          Layer.provide(layerServices)
        )
      }

      const firstScope = yield* Scope.make()
      const firstContext = yield* Layer.buildWithScope(replicaLayer([spaceA, spaceB]), firstScope)
      const first = Context.get(firstContext, Replica.Replica)
      yield* (yield* first.space(spaceA)).mutate(Domain.PutTodo, Domain.todo("restart", "retained"))
      yield* Scope.close(firstScope, Exit.void)

      const secondScope = yield* Scope.make()
      const secondContext = yield* Layer.buildWithScope(replicaLayer(), secondScope)
      const second = Context.get(secondContext, Replica.Replica)
      assert.deepStrictEqual((yield* second.spaces).map((space) => space.spaceId), [spaceA, spaceB])
      assert.strictEqual(
        Option.getOrThrow(yield* (yield* second.space(spaceA)).get(Domain.Todo, "restart")).title,
        "retained"
      )
      yield* Scope.close(secondScope, Exit.void)
    }, Effect.scoped)
  )

  it.effect(
    "releases a joining reservation when its owner is interrupted",
    Effect.fnUntraced(function*() {
      const engineContext = yield* Layer.build(WorkflowEngine.layerMemory)
      const engine = Context.get(engineContext, WorkflowEngine.WorkflowEngine)
      const firstRegistration = yield* Deferred.make<void>()
      let blockRegistration = true
      const register: typeof engine.register = (workflow, execute) => {
        if (!blockRegistration) return engine.register(workflow, execute)
        blockRegistration = false
        return Deferred.succeed(firstRegistration, undefined).pipe(Effect.andThen(Effect.never))
      }
      const interruptedEngine = new Proxy(engine, {
        get: (target, property, receiver) => {
          if (property === "register") return register
          return Reflect.get(target, property, receiver)
        }
      })
      const layerReplica = SqlReplica.layerWorkflow({
        ...clientHistory,
        definition: Domain.definition,
        clientId
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer),
        Layer.provide(Layer.succeed(WorkflowEngine.WorkflowEngine, interruptedEngine))
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const first = yield* Effect.forkChild(replica.join(spaceA), { startImmediately: true })
      yield* Deferred.await(firstRegistration)
      yield* Fiber.interrupt(first)

      assert.strictEqual((yield* replica.join(spaceA)).spaceId, spaceA)
    }, Effect.scoped)
  )

  it.effect(
    "releases manager registration when join is interrupted",
    Effect.fnUntraced(function*() {
      const requestBlocked = yield* Deferred.make<void>()
      const activeWatches = yield* Ref.make(0)
      const watchStarted = yield* Deferred.make<void>()
      const countedRemoteService = SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        discard: () => Effect.die("unexpected discard"),
        pull: () => Effect.never,
        bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        watch: () => {
          const acquire = Ref.update(activeWatches, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(watchStarted, undefined)),
            Effect.as(Stream.never)
          )
          return Effect.acquireRelease(
            acquire,
            () => Ref.update(activeWatches, (count) => count - 1)
          ).pipe(Stream.unwrap)
        }
      })
      const layerCountedRemote = Layer.succeed(SyncEngine.SyncEngine, countedRemoteService)
      const manager = yield* Reconciler.makeManager().pipe(Effect.provide(layerCountedRemote))
      const local = {
        requestReconciliation: Deferred.succeed(requestBlocked, undefined).pipe(Effect.andThen(Effect.never)),
        reconciliationGenerations: Effect.succeed({ requested: 0, completed: 0 }),
        replicationState: Effect.succeed({
          clientId,
          scope,
          scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
          cursor: null
        }),
        completeReconciliation: () => Effect.void
      } satisfies Pick<
        LocalStore.Service,
        "requestReconciliation" | "reconciliationGenerations" | "replicationState" | "completeReconciliation"
      >
      const reconciliation = Reconciler.Reconciliation.of({
        sync: Effect.void,
        failed: () => Effect.void,
        watchFailed: () => Effect.void,
        succeeded: Effect.void,
        status: Effect.succeed({ _tag: "Offline", pending: 0 })
      })
      const registration = yield* Effect.forkChild(
        manager.register({
          spaceId: spaceA,
          generation: 1,
          definition: Domain.definition,
          local,
          reconciliation
        }),
        { startImmediately: true }
      )
      yield* Deferred.await(watchStarted)
      yield* Deferred.await(requestBlocked)
      yield* Fiber.interrupt(registration)

      assert.strictEqual(yield* Ref.get(activeWatches), 0)
    }, Effect.scoped)
  )

  it.effect(
    "drains an admitted mutation before reconciliation teardown",
    Effect.fnUntraced(function*() {
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const definition = Definition.make({
        version: 1,
        models: [Domain.Todo],
        mutations: [Domain.PutTodo]
      })
      const layerHandlers = Domain.PutTodo.toLayer(({ payload, transaction }) =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.andThen(transaction.set(Domain.Todo, payload.id, payload)),
          Effect.as(payload)
        )
      )
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition,
        clientId,
        initialSpaces: [spaceA]
      }).pipe(
        Layer.provide(layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer)
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const space = yield* replica.space(spaceA)
      const mutate = space.mutate(Domain.PutTodo, Domain.todo("drain"))
      const mutation = yield* Effect.forkChild(
        mutate,
        { startImmediately: true }
      )
      yield* Deferred.await(entered)
      const leaving = yield* Effect.forkChild(replica.leave(spaceA), { startImmediately: true })
      yield* Effect.yieldNow
      const missingSpace = yield* replica.space(spaceA).pipe(Effect.result)
      assert.strictEqual(missingSpace._tag, "Failure")
      if (missingSpace._tag === "Failure") assert.strictEqual(missingSpace.failure._tag, "SpaceNotJoined")
      yield* Deferred.succeed(release, undefined)

      assert.strictEqual((yield* Fiber.join(mutation)).envelope.spaceId, spaceA)
      yield* Fiber.join(leaving)
    }, Effect.scoped)
  )

  it.effect(
    "releases a leaving reservation when its owner is interrupted",
    Effect.fnUntraced(function*() {
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const definition = Definition.make({
        version: 1,
        models: [Domain.Todo],
        mutations: [Domain.PutTodo]
      })
      const layerHandlers = Domain.PutTodo.toLayer(({ payload, transaction }) =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.andThen(transaction.set(Domain.Todo, payload.id, payload)),
          Effect.as(payload)
        )
      )
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition,
        clientId,
        initialSpaces: [spaceA]
      }).pipe(
        Layer.provide(layerHandlers),
        Layer.provide(layerRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer)
      )
      const context = yield* Layer.build(layerReplica)
      const replica = Context.get(context, Replica.Replica)
      const space = yield* replica.space(spaceA)
      const mutate = space.mutate(Domain.PutTodo, Domain.todo("interrupt-leave"))
      const mutation = yield* Effect.forkChild(
        mutate,
        { startImmediately: true }
      )
      yield* Deferred.await(entered)
      const leaving = yield* Effect.forkChild(replica.leave(spaceA), { startImmediately: true })
      yield* Effect.yieldNow
      const missingSpace = yield* replica.space(spaceA).pipe(Effect.result)
      assert.strictEqual(missingSpace._tag, "Failure")
      if (missingSpace._tag === "Failure") assert.strictEqual(missingSpace.failure._tag, "SpaceNotJoined")
      yield* Fiber.interrupt(leaving)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(mutation)

      assert.strictEqual((yield* replica.join(spaceA)).spaceId, spaceA)
    }, Effect.scoped)
  )

  it.effect(
    "retries durable eviction after a SQL failure",
    Effect.fnUntraced(function*() {
      const layerDatabase = Layer.mergeAll(
        SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
        NodeCrypto.layer,
        Reactivity.layer
      )
      const databaseContext = yield* Layer.build(layerDatabase)
      const sql = Context.get(databaseContext, SqlClient.SqlClient)
      let failDelete = true
      const failingSql = new Proxy(sql, {
        apply: (target, thisArg, args: Parameters<typeof sql>) => {
          const rawSource: unknown = args[0]
          let source: string
          if (Array.isArray(rawSource)) source = rawSource.join("")
          else source = String(rawSource)
          if (failDelete && source.includes("DELETE FROM effect_local_client_spaces")) {
            failDelete = false
            return Effect.fail(
              new SqlError.SqlError({
                reason: new SqlError.UnknownError({ cause: "injected leave failure" })
              })
            )
          }
          return Reflect.apply(target, thisArg, args)
        }
      })
      const services = (client: SqlClient.SqlClient) => {
        const layerSql = Layer.succeed(SqlClient.SqlClient, client)
        const layerCrypto = Layer.succeed(Crypto.Crypto, Context.get(databaseContext, Crypto.Crypto))
        const layerReactivity = Layer.succeed(
          Reactivity.Reactivity,
          Context.get(databaseContext, Reactivity.Reactivity)
        )
        return Layer.mergeAll(
          layerSql,
          layerCrypto,
          layerReactivity
        )
      }
      const replicaLayer = (client: SqlClient.SqlClient, initialSpaces?: Iterable<Identity.SpaceId>) =>
        SqlReplica.layer({
          ...clientHistory,
          definition: Domain.definition,
          clientId,
          initialSpaces: initialSpaces ?? []
        }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(layerRemote),
          Layer.provide(services(client))
        )

      const firstScope = yield* Scope.make()
      const firstContext = yield* Layer.buildWithScope(replicaLayer(failingSql, [spaceA]), firstScope)
      const first = Context.get(firstContext, Replica.Replica)
      const failedLeave = yield* first.leave(spaceA).pipe(Effect.result)
      assert.strictEqual(failedLeave._tag, "Failure")
      if (failedLeave._tag === "Failure") assert.strictEqual(failedLeave.failure._tag, "StorageUnavailable")
      yield* first.leave(spaceA)
      yield* Scope.close(firstScope, Exit.void)

      const secondScope = yield* Scope.make()
      const secondContext = yield* Layer.buildWithScope(replicaLayer(sql), secondScope)
      assert.deepStrictEqual(yield* Context.get(secondContext, Replica.Replica).spaces, [])
      yield* Scope.close(secondScope, Exit.void)
    }, Effect.scoped)
  )

  it.effect(
    "bounds reconciliation turns across many joined spaces",
    Effect.fnUntraced(function*() {
      const spaces = Array.from({ length: 6 }, (_, index) => {
        const suffix = String(index + 1).padStart(12, "0")
        return Identity.SpaceId.make(`spc_00000000-0000-4000-8000-${suffix}`)
      })
      const current = yield* Ref.make(0)
      const maximum = yield* Ref.make(0)
      const twoStarted = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const blockedRemoteService = SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        submit: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        discard: () => Effect.die("unexpected discard"),
        pull: () => {
          const acquire = Ref.updateAndGet(current, (count) => count + 1).pipe(
            Effect.tap((count) => Ref.update(maximum, (seen) => Math.max(seen, count))),
            Effect.tap((count) => {
              if (count === 2) return Deferred.succeed(twoStarted, undefined)
              return Effect.void
            })
          )
          const use = () =>
            Deferred.await(release).pipe(Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable())))
          return Effect.acquireUseRelease(
            acquire,
            use,
            () => Ref.update(current, (count) => count - 1)
          )
        },
        bootstrap: () => Effect.fail(new ReplicaError.ServerUnavailable()),
        watch: () => Stream.never
      })
      const layerBlockedRemote = Layer.succeed(SyncEngine.SyncEngine, blockedRemoteService)
      const layerReplica = SqlReplica.layer({
        ...clientHistory,
        definition: Domain.definition,
        clientId,
        initialSpaces: spaces,
        reconciliationConcurrency: 2
      }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(layerBlockedRemote),
        Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
        Layer.provide(NodeCrypto.layer),
        Layer.provide(Reactivity.layer)
      )
      const context = yield* Layer.build(layerReplica)
      yield* Deferred.await(twoStarted)
      yield* Context.get(context, Replica.Replica).status

      assert.isAtMost(yield* Ref.get(maximum), 2)
      yield* Deferred.succeed(release, undefined)
    }, Effect.scoped)
  )
})
