import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as ConnectionLane from "../src/ConnectionLane.js"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryExecutor from "../src/QueryExecutor.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import { gateStatements, type Phase } from "./fixtures/SqlGate.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const
const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const makeLane = Effect.fnUntraced(function*(options: ConnectionLane.Options = {}) {
  const context = yield* Layer.build(
    ConnectionLane.makeLayer(options).pipe(
      Layer.provideMerge(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
      Layer.provide(Reactivity.layer)
    )
  )
  const sql = Context.get(context, SqlClient.SqlClient)
  const lane = Context.get(context, ConnectionLane.ConnectionLane)
  const order: Array<string> = []
  const record = (name: string, priority: ConnectionLane.Priority) =>
    lane.withStatement(Effect.sync(() => order.push(name))).pipe(
      Effect.provideService(ConnectionLane.Priority, priority),
      Effect.forkChild({ startImmediately: true })
    )
  const hold = Effect.gen(function*() {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const holder = yield* lane.withTransaction(
      Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
    ).pipe(
      Effect.provideService(ConnectionLane.Priority, "Background"),
      Effect.forkChild({ startImmediately: true })
    )
    yield* Deferred.await(entered)
    return { holder, release: Deferred.succeed(release, undefined) }
  })
  const session = Effect.gen(function*() {
    const between = yield* Deferred.make<void>()
    const resume = yield* Deferred.make<void>()
    const fiber = yield* lane.withSession(Effect.gen(function*() {
      yield* lane.withStatement(Effect.sync(() => order.push("session-1")))
      yield* Deferred.succeed(between, undefined)
      yield* Deferred.await(resume)
      yield* lane.withStatement(Effect.sync(() => order.push("session-2")))
    })).pipe(
      Effect.provideService(ConnectionLane.Priority, "Background"),
      Effect.forkChild({ startImmediately: true })
    )
    yield* Deferred.await(between)
    return { fiber, resume: Deferred.succeed(resume, undefined) }
  })
  return { sql, lane, record, order, hold, session }
})

const receiptFor = (pending: Protocol.PendingMutation): Protocol.Receipt => ({
  _tag: "Rejected",
  name: pending.envelope.name,
  sourceSchema: pending.envelope.sourceSchema,
  mutationVersion: pending.envelope.mutationVersion,
  spaceId,
  clientId,
  membershipIncarnation: pending.envelope.membershipIncarnation,
  mutationId: pending.envelope.mutationId,
  localSequence: pending.envelope.localSequence,
  origin: "Authorization",
  rejection: "denied"
})

const makeStore = Effect.fnUntraced(function*(
  phasesOf: (statement: string) => ReadonlyArray<Phase>,
  receiptPersistBatchSize = 8
) {
  const actual = yield* SqliteClient.make({ filename: ":memory:", disableWAL: true }).pipe(
    Effect.provide(Reactivity.layer)
  )
  const gate = yield* gateStatements(actual, phasesOf)
  const layerDatabase = Layer.mergeAll(
    ConnectionLane.makeLayer().pipe(Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, gate.sql))),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
  const context = yield* Layer.build(
    Layer.mergeAll(
      LocalStore.layer({
        definition: Domain.definition,
        spaceId,
        clientId,
        scope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
        retainedReceipts: 256,
        maximumReceipts: 10_000,
        retainedHistoryEntries: 256,
        maximumBootstrapEntities: 10_000,
        maximumBootstrapBytes: 64 * 1024 * 1024,
        maximumBootstrapPageBytes: 4 * 1024 * 1024,
        receiptPersistBatchSize,
        migration
      }).pipe(Layer.provide(layerRuntime)),
      QueryExecutor.layer(Domain.definition, spaceId).pipe(Layer.provide(Domain.layerHandlers))
    ).pipe(Layer.provideMerge(layerDatabase))
  )
  return {
    local: Context.get(context, LocalStore.Store),
    queries: Context.get(context, QueryExecutor.QueryExecutor),
    lane: Context.get(context, ConnectionLane.ConnectionLane),
    pauses: gate.pauses
  }
})

const readCounts = { minimum: 0, direction: "asc" } as const

const inBackground = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.provideService(ConnectionLane.Priority, "Background"),
    Effect.forkChild({ startImmediately: true })
  )

const inForeground = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.provideService(ConnectionLane.Priority, "Foreground"),
    Effect.forkChild({ startImmediately: true })
  )

const isSettlementScan = (statement: string) =>
  statement.includes("l.mutation_id AS entry_mutation_id") &&
  statement.includes("effect_local_client_receipts_data AS r")

const isReceiptInsert = (statement: string) => statement.includes("INSERT INTO effect_local_client_receipts_data")

const recordStarts = (pauseFirstReceipt: boolean) => {
  const started: Array<string> = []
  let paused = !pauseFirstReceipt
  const phasesOf = (statement: string): ReadonlyArray<Phase> => {
    if (statement.startsWith("WITH RECURSIVE") && started.at(-1) !== "query") started.push("query")
    if (!isReceiptInsert(statement)) return []
    started.push("receipt")
    if (paused) return []
    paused = true
    return ["before"]
  }
  return { started, phasesOf }
}

describe("ConnectionLane", () => {
  it.effect(
    "serves waiting foreground work before waiting background work, each in arrival order",
    Effect.fnUntraced(function*() {
      const { hold, order, record } = yield* makeLane()
      const held = yield* hold
      const waiters = [
        yield* record("background-1", "Background"),
        yield* record("foreground-1", "Foreground"),
        yield* record("background-2", "Background"),
        yield* record("foreground-2", "Foreground")
      ]
      yield* held.release
      yield* Fiber.join(held.holder)
      yield* Fiber.joinAll(waiters)
      assert.deepStrictEqual(order, ["foreground-1", "foreground-2", "background-1", "background-2"])
    })
  )

  it.effect(
    "serves a background waiter in arrival order once it has waited the maximum background wait",
    Effect.fnUntraced(function*() {
      const { hold, order, record } = yield* makeLane({ maximumBackgroundWait: "100 millis" })
      const held = yield* hold
      const background = yield* record("background", "Background")
      yield* TestClock.adjust("100 millis")
      const foreground = yield* record("foreground", "Foreground")
      yield* held.release
      yield* Fiber.joinAll([held.holder, background, foreground])
      assert.deepStrictEqual(order, ["background", "foreground"])
    })
  )

  it.effect(
    "interrupting a waiting acquirer runs nothing and leaves the turn with the holder",
    Effect.fnUntraced(function*() {
      const { hold, lane, order, record } = yield* makeLane()
      const held = yield* hold
      const interrupted = yield* record("interrupted", "Foreground")
      const waiting = yield* record("waiting", "Foreground")
      yield* Fiber.interrupt(interrupted)
      yield* Effect.yieldNow
      assert.deepStrictEqual(order, [])
      assert.isTrue(yield* lane.foregroundWaiting)
      yield* held.release
      yield* Fiber.join(held.holder)
      yield* Fiber.join(waiting)
      assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(interrupted)))
      assert.isFalse(yield* lane.foregroundWaiting)
      assert.deepStrictEqual(order, ["waiting"])
    })
  )

  it.effect(
    "serves a deep queue of transactions on a synchronous driver without exhausting the stack",
    Effect.fnUntraced(function*() {
      const { hold, lane, sql } = yield* makeLane()
      const held = yield* hold
      const queued = yield* Effect.forEach(
        Array.from({ length: 3000 }, (_, index) => index),
        (index) => lane.withTransaction(sql`SELECT ${index} AS n`).pipe(Effect.forkChild({ startImmediately: true }))
      )
      yield* held.release
      yield* Fiber.join(held.holder)
      const exits = yield* Effect.forEach(queued, Fiber.await)
      assert.isTrue(exits.every(Exit.isSuccess))
    })
  )

  it.effect(
    "keeps a session turn across consecutive work while waiters have waited less than the maximum background wait",
    Effect.fnUntraced(function*() {
      const { order, record, session } = yield* makeLane({ maximumBackgroundWait: "100 millis" })
      const opened = yield* session
      const foreground = yield* record("foreground", "Foreground")
      yield* opened.resume
      yield* Fiber.joinAll([opened.fiber, foreground])
      assert.deepStrictEqual(order, ["session-1", "session-2", "foreground"])
    })
  )

  it.effect(
    "yields a session turn to a waiter that has waited the maximum background wait",
    Effect.fnUntraced(function*() {
      const { order, record, session } = yield* makeLane({ maximumBackgroundWait: "100 millis" })
      const opened = yield* session
      const foreground = yield* record("foreground", "Foreground")
      yield* TestClock.adjust("100 millis")
      yield* opened.resume
      yield* Fiber.joinAll([opened.fiber, foreground])
      assert.deepStrictEqual(order, ["session-1", "foreground", "session-2"])
    })
  )

  it.effect(
    "does not lend a session turn to fibers forked by its owner",
    Effect.fnUntraced(function*() {
      const { lane, order } = yield* makeLane()
      const child = yield* Deferred.make<Fiber.Fiber<void>>()
      yield* lane.withSession(Effect.gen(function*() {
        yield* lane.withStatement(Effect.sync(() => order.push("session-1")))
        const forked = yield* lane.withStatement(Effect.sync(() => order.push("child"))).pipe(
          Effect.asVoid,
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.succeed(child, forked)
        yield* lane.withStatement(Effect.sync(() => order.push("session-2")))
      }))
      yield* Fiber.join(yield* Deferred.await(child))
      assert.deepStrictEqual(order, ["session-1", "session-2", "child"])
    })
  )

  it.effect(
    "runs nested transactions and statements inside a transaction without taking another turn",
    Effect.fnUntraced(function*() {
      const { lane, sql } = yield* makeLane()
      yield* sql`CREATE TABLE lane_nested (name TEXT NOT NULL)`
      yield* lane.withTransaction(Effect.gen(function*() {
        yield* lane.withTransaction(sql`INSERT INTO lane_nested (name) VALUES ('savepoint')`)
        yield* lane.withStatement(sql`INSERT INTO lane_nested (name) VALUES ('statement')`)
      }))
      const rows = yield* SqlSchema.findAll({
        Request: Schema.Void,
        Result: Schema.Struct({ name: Schema.String }),
        execute: () => sql`SELECT name FROM lane_nested ORDER BY rowid`
      })(undefined)
      assert.deepStrictEqual(rows.map((row) => row.name), ["savepoint", "statement"])
    })
  )
})

describe("LocalStore on a connection lane", () => {
  it.effect(
    "starts a foreground query issued after a background receipt write queued before it",
    Effect.fnUntraced(function*() {
      const recorder = recordStarts(false)
      const { local, lane, queries } = yield* makeStore(recorder.phasesOf)
      const pending = yield* local.mutate(Domain.PutTodo, Domain.todo("queued"))
      yield* local.pendingToSubmit
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const holder = yield* inForeground(
        lane.withTransaction(Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))))
      )
      yield* Deferred.await(entered)
      const persist = yield* inBackground(local.persistReceipts([receiptFor(pending)]))
      const query = yield* inForeground(queries.execute(Domain.ReadCountIndex, readCounts))
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.joinAll([holder, persist, query])
      assert.deepStrictEqual(recorder.started, ["query", "receipt"])
    })
  )

  it.effect(
    "lets a foreground query run between receipt chunks of a background receipt batch",
    Effect.fnUntraced(function*() {
      const recorder = recordStarts(true)
      const { local, pauses, queries } = yield* makeStore(recorder.phasesOf, 1)
      const pending = yield* Effect.forEach(
        ["first", "second", "third"],
        (id) => local.mutate(Domain.PutTodo, Domain.todo(id))
      )
      const receipts = pending.map(receiptFor)
      const persist = yield* inBackground(local.persistReceipts(receipts))
      const first = yield* Queue.take(pauses)
      const query = yield* inForeground(queries.execute(Domain.ReadCountIndex, readCounts))
      yield* Deferred.succeed(first.release, undefined)
      yield* Fiber.joinAll([persist, query])
      assert.deepStrictEqual(recorder.started, ["receipt", "query", "receipt", "receipt"])
      for (const item of pending) {
        assert.isTrue(Option.isSome(yield* local.receipt(item.envelope.mutationId)))
      }
    })
  )

  it.effect(
    "keeps the committed receipt prefix and fails when a later chunk of a split batch is rejected",
    Effect.fnUntraced(function*() {
      const recorder = recordStarts(true)
      const { local, pauses, queries } = yield* makeStore(recorder.phasesOf, 1)
      const pending = yield* Effect.forEach(["kept-1", "kept-2"], (id) => local.mutate(Domain.PutTodo, Domain.todo(id)))
      const unknown = receiptFor({
        ...pending[1],
        envelope: {
          ...pending[1].envelope,
          mutationId: Identity.MutationId.make("mut_00000000-0000-4000-8000-00000000ffff"),
          localSequence: Identity.LocalSequence.make(pending[1].envelope.localSequence + 1)
        }
      })
      const persist = yield* inBackground(local.persistReceipts([...pending.map(receiptFor), unknown]))
      const first = yield* Queue.take(pauses)
      const query = yield* inForeground(queries.execute(Domain.ReadCountIndex, readCounts))
      yield* Deferred.succeed(first.release, undefined)
      const exit = yield* Fiber.await(persist)
      yield* Fiber.join(query)
      assert.deepStrictEqual(recorder.started, ["receipt", "query", "receipt"])
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        const failure = Cause.findErrorOption(exit.cause)
        assert.isTrue(Option.isSome(failure) && failure.value._tag === "ProtocolInvalid")
      }
      for (const item of pending) {
        assert.isTrue(Option.isSome(yield* local.receipt(item.envelope.mutationId)))
      }
      assert.isTrue(Option.isNone(yield* local.receipt(unknown.mutationId)))
    })
  )

  it.effect(
    "admits a foreground projection gate waiter before a background one that queued first",
    Effect.fnUntraced(function*() {
      const started: Array<string> = []
      let holderPaused = false
      const { local, pauses } = yield* makeStore((statement) => {
        if (isSettlementScan(statement)) {
          if (!holderPaused) {
            holderPaused = true
            return ["before"]
          }
          started.push("background")
        }
        if (statement.includes("DELETE FROM effect_local_client_canonical_entities_data")) started.push("foreground")
        return []
      })
      const holder = yield* inBackground(local.settleReceipts)
      const paused = yield* Queue.take(pauses)
      const background = yield* inBackground(local.settleReceipts)
      const foreground = yield* inForeground(local.revokeReplication)
      yield* Deferred.succeed(paused.release, undefined)
      yield* Fiber.joinAll([holder, background, foreground])
      assert.deepStrictEqual(started, ["foreground", "background"])
    })
  )

  it.effect(
    "serves a background projection gate holder ahead of queued reads once foreground work waits on the gate",
    Effect.fnUntraced(function*() {
      const started: Array<string> = []
      let observing = false
      const { lane, local, queries } = yield* makeStore((statement) => {
        if (observing && statement.startsWith("SELECT space_id, membership_incarnation, definition_hash")) {
          started.push("gate holder")
        }
        return []
      })
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const holder = yield* inForeground(
        lane.withTransaction(Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))))
      )
      yield* Deferred.await(entered)
      observing = true
      const gateHolder = yield* inBackground(local.applyEntries([]))
      const query = yield* inForeground(
        queries.execute(Domain.ReadCountIndex, readCounts).pipe(
          Effect.ensuring(Effect.sync(() => started.push("query")))
        )
      )
      const gateWaiter = yield* inForeground(local.revokeReplication)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.joinAll([holder, gateHolder, query, gateWaiter])
      assert.deepStrictEqual(started.slice(0, 2), ["gate holder", "query"])
    })
  )
})

const layerMemoryDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer,
  Reactivity.layer
)

const layerServer = ServerStore.layerTrusted({
  definition: Domain.definition,
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  maximumWatchersPerSpace: 1_024,
  readAuthorizationRefreshInterval: "30 seconds",
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  migration
}).pipe(Layer.provide(layerRuntime), Layer.provide(layerMemoryDatabase))

const replicaOptions = {
  definition: Domain.definition,
  clientId,
  defaultScope: Protocol.ReplicationScope.make({ models: [Domain.Todo.name] }),
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  retainedHistoryEntries: 256,
  maximumBootstrapEntities: 10_000,
  maximumBootstrapBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: 4 * 1024 * 1024,
  migration,
  retryDelay: "1 minute",
  maximumRetryDelay: "1 minute"
} as const

const recordingRemote = Effect.fnUntraced(function*(reachable: boolean) {
  const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
  const priorities: Array<ConnectionLane.Priority> = []
  const submitted = yield* Deferred.make<void>()
  const observe = Effect.fnUntraced(function*<A, E extends { readonly _tag: string }, R,>(
    effect: Effect.Effect<A, E, R>
  ) {
    priorities.push(yield* ConnectionLane.Priority)
    return yield* effect
  })
  const unreachable = Effect.fail(new ReplicaError.ServerUnavailable())
  const remote = SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Effect.never,
    submitBatch: (request) => {
      if (!reachable) return unreachable
      return observe(server.admitBatch(request, null)).pipe(Effect.tap(() => Deferred.succeed(submitted, undefined)))
    },
    discard: (request) => server.discard(request, null),
    pull: (request) => {
      if (!reachable) return unreachable
      return observe(server.pull(request))
    },
    bootstrap: (request) => observe(server.bootstrap(request)),
    watch: (request) => {
      if (!reachable) return Stream.fail(new ReplicaError.ServerUnavailable())
      const watched = Effect.succeed(server.watch(request))
      return Stream.unwrap(observe(watched))
    }
  })
  return { remote, priorities, submitted: Deferred.await(submitted) }
})

describe("SqlReplica priorities", () => {
  it.effect(
    "runs managed reconciliation of an active space as background work",
    Effect.fnUntraced(function*() {
      const { priorities, remote, submitted } = yield* recordingRemote(true)
      const context = yield* Layer.build(
        SqlReplica.layer({ ...replicaOptions, initialSpaces: [spaceId] }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.provide(layerMemoryDatabase)
        )
      )
      const space = yield* Context.get(context, Replica.Replica).space(spaceId)
      yield* space.mutate(Domain.PutTodo, Domain.todo("managed"))
      yield* submitted
      assert.isAbove(priorities.length, 1)
      assert.deepStrictEqual(new Set(priorities), new Set(["Background"]))
    })
  )

  it.effect(
    "runs workflow reconciliation of an active space as background work",
    Effect.fnUntraced(function*() {
      const { priorities, remote, submitted } = yield* recordingRemote(true)
      const context = yield* Layer.build(
        SqlReplica.layerWorkflow({ ...replicaOptions, initialSpaces: [spaceId] }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.provide(layerMemoryDatabase),
          Layer.provide(WorkflowEngine.layerMemory)
        )
      )
      const space = yield* Context.get(context, Replica.Replica).space(spaceId)
      yield* space.mutate(Domain.PutTodo, Domain.todo("workflow"))
      yield* submitted
      assert.isAbove(priorities.length, 1)
      assert.deepStrictEqual(new Set(priorities), new Set(["Background"]))
    })
  )

  it.effect(
    "syncs a remembered space with pending work in the background queue as background work",
    Effect.fnUntraced(function*() {
      const database = yield* Layer.build(layerMemoryDatabase)
      const offline = yield* recordingRemote(false)
      const firstScope = yield* Scope.make()
      const first = yield* SqlReplica.layer({ ...replicaOptions, initialSpaces: [spaceId] }).pipe(
        Layer.provide(Domain.layerHandlers),
        Layer.provide(Layer.succeed(SyncEngine.SyncEngine, offline.remote)),
        Layer.provide(Layer.succeedContext(database)),
        Layer.buildWithScope(firstScope)
      )
      const firstSpace = yield* Context.get(first, Replica.Replica).space(spaceId)
      yield* firstSpace.mutate(Domain.PutTodo, Domain.todo("remembered"))
      yield* Scope.close(firstScope, Exit.void)

      const { priorities, remote, submitted } = yield* recordingRemote(true)
      yield* Layer.build(
        SqlReplica.layer(replicaOptions).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote)),
          Layer.provide(Layer.succeedContext(database))
        )
      )
      yield* submitted
      assert.isAbove(priorities.length, 1)
      assert.deepStrictEqual(new Set(priorities), new Set(["Background"]))
    })
  )
})
