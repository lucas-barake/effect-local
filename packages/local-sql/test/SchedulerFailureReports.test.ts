import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import type * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Scheduler from "effect/Scheduler"
import * as SqlClient from "effect/sql/SqlClient"
import * as SqlError from "effect/sql/SqlError"
import * as Stream from "effect/Stream"
import * as WorkflowEngine from "effect/workflow/WorkflowEngine"
import * as ConnectionLane from "../src/ConnectionLane.js"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import * as VirtualTime from "./fixtures/DeterministicTime.js"
import { gateStatements } from "./fixtures/SqlGate.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-0000000000e1")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-0000000000e1")

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })

const layerServerDatabase = Layer.mergeAll(
  SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
  NodeCrypto.layer
)

const layerServer = ServerStore.layerTrusted({ definition: Domain.definition, migration }).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(layerServerDatabase)
)

const layerClientDatabase = Layer.mergeAll(
  ConnectionLane.makeLayer().pipe(Layer.provideMerge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))),
  NodeCrypto.layer,
  Reactivity.layer,
  QueryReactivity.layer
)

const completeStatement = "SET completed_generation"
const countStatement = "SELECT COUNT(*) AS count FROM effect_local_client_pending_data"
const claimStatement = "AND attempt_count >= "
const admissionStatement = "SET requested_generation = ?"

type PullMode = "Pass" | "Hold" | "Interrupt" | "Reject" | "FailWhenReleased"

const harness = Effect.fnUntraced(function*() {
  const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
  const database = yield* Layer.build(layerClientDatabase)
  const sql = Context.get(database, SqlClient.SqlClient)
  let failStatement: (statement: string) => boolean = () => false
  let pauseStatement: (statement: string) => boolean = () => false
  const injected = yield* Queue.unbounded<void>()
  const failingSql = new Proxy(sql, {
    apply: (target, thisArg, args: Parameters<typeof sql>) => {
      const source: unknown = args[0]
      if (Array.isArray(source) && failStatement(source.join("?"))) {
        return Queue.offer(injected, undefined).pipe(
          Effect.andThen(
            Effect.fail(new SqlError.SqlError({ reason: new SqlError.LockTimeoutError({ cause: "injected" }) }))
          )
        )
      }
      return Reflect.apply(target, thisArg, args)
    }
  })
  const gate = yield* gateStatements(failingSql, (statement) => {
    if (pauseStatement(statement)) return ["before"]
    return []
  })
  const watchFailure = yield* Deferred.make<ReplicaError.ReplicaError>()
  const watchEnd = yield* Deferred.make<void>()
  const credentialChange = yield* Deferred.make<void>()
  let pullMode: PullMode = "Pass"
  let pullFailure: ReplicaError.ReplicaError | undefined
  const heldPulls = yield* Queue.unbounded<void>()
  const heldPullReleased = yield* Deferred.make<void>()
  const transportWaits = yield* Queue.unbounded<void>()
  const watchStarts = yield* Queue.unbounded<void>()
  const watchTimes: Array<number> = []
  let watchWakes = false
  let watchAccepted = false
  let pulls = 0
  let acceptedAhead = false
  const liveWakes = yield* Queue.unbounded<Protocol.Wake>()
  const watchFailed = Effect.flip(Deferred.await(watchFailure))
  const watchOutcome = Effect.raceFirst(watchFailed, Deferred.await(watchEnd))
  const remote = SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Deferred.await(credentialChange),
    credentialGeneration: Effect.succeed(0),
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Queue.offer(transportWaits, undefined).pipe(Effect.andThen(Effect.never)),
    submitBatch: (request) => {
      if (!acceptedAhead) return server.admitBatch(request, null)
      return Effect.succeed(Protocol.SubmitBatchResult.make({
        receipts: request.envelopes.map((envelope) =>
          Protocol.AcceptedReceipt.make({
            ...envelope,
            serverSequence: Identity.ServerSequence.make(1_000),
            result: Domain.todo(envelope.mutationId, "accepted")
          })
        )
      }))
    },
    discard: (request) => server.discard(request, null),
    pull: (request) =>
      Effect.suspend(() => {
        pulls += 1
        if (pullMode === "Interrupt") {
          pullMode = "Pass"
          return Effect.interrupt
        }
        if (pullFailure !== undefined) {
          const failure = pullFailure
          pullFailure = undefined
          return Effect.fail(failure)
        }
        if (pullMode === "Reject") {
          pullMode = "Pass"
          return Effect.fail(new ReplicaError.CredentialRejected({ credentialGeneration: 1 }))
        }
        if (pullMode === "Hold") return Queue.offer(heldPulls, undefined).pipe(Effect.andThen(Effect.never))
        if (pullMode === "FailWhenReleased") {
          pullMode = "Pass"
          return Queue.offer(heldPulls, undefined).pipe(
            Effect.andThen(Deferred.await(heldPullReleased)),
            Effect.andThen(Effect.fail(new ReplicaError.ServerUnavailable()))
          )
        }
        return server.pull(request)
      }),
    bootstrap: server.bootstrap,
    watch: (request) => {
      const started = Clock.currentTimeMillis.pipe(
        Effect.tap((now) => Effect.sync(() => watchTimes.push(now))),
        Effect.andThen(Queue.offer(watchStarts, undefined))
      )
      const opened = Stream.fromEffect(started).pipe(Stream.drain)
      if (watchAccepted) return Stream.concat(opened, Stream.fromQueue(liveWakes))
      const outcome = Stream.fromEffect(Effect.andThen(started, watchOutcome)).pipe(Stream.drain)
      if (!watchWakes) return outcome
      const wake = Protocol.Wake.make({ spaceId: request.spaceId })
      return Stream.concat(Stream.succeed(wake), outcome)
    }
  })
  return {
    database: Context.add(database, SqlClient.SqlClient, gate.sql),
    layerRemote: Layer.succeed(SyncEngine.SyncEngine, remote),
    heldPulls,
    transportWaits,
    watchStarts,
    watchTimes,
    wakeOnWatch: Effect.sync(() => {
      watchWakes = true
    }),
    injected,
    paused: gate.pauses,
    failWatch: (error: ReplicaError.ReplicaError) => Deferred.succeed(watchFailure, error),
    acceptLaterWatches: Effect.sync(() => {
      watchAccepted = true
    }),
    wake: Queue.offer(liveWakes, Protocol.Wake.make({ spaceId })),
    pulls: () => pulls,
    acceptAheadOfTheView: Effect.sync(() => {
      acceptedAhead = true
    }),
    endWatch: Deferred.succeed(watchEnd, undefined),
    changeCredential: Deferred.succeed(credentialChange, undefined),
    releaseHeldPull: Deferred.succeed(heldPullReleased, undefined),
    failNextPull: (error: ReplicaError.ReplicaError) =>
      Effect.sync(() => {
        pullFailure = error
      }),
    setPullMode: (mode: PullMode) =>
      Effect.sync(() => {
        pullMode = mode
      }),
    failWhen: (decide: (statement: string) => boolean) =>
      Effect.sync(() => {
        failStatement = decide
      }),
    pauseWhen: (decide: (statement: string) => boolean) =>
      Effect.sync(() => {
        pauseStatement = decide
      })
  }
})

const failOnce = (matches: (statement: string) => boolean) => {
  let armed = true
  return (statement: string) => {
    if (!armed || !matches(statement)) return false
    armed = false
    return true
  }
}

const failAfter = (trigger: string, target: (statement: string) => boolean) => {
  let triggered = false
  return failOnce((statement) => {
    if (statement.includes(trigger)) triggered = true
    return triggered && target(statement)
  })
}

const failEach = (statements: ReadonlyArray<string>) => {
  let next = 0
  return (statement: string) => {
    const expected = statements[next]
    if (expected === undefined || !statement.includes(expected)) return false
    next += 1
    return true
  }
}

const awaitStatus = (
  reactivity: Reactivity.Reactivity,
  space: Replica.Space,
  predicate: (status: ReplicaStatus.SpaceStatus) => boolean
) =>
  reactivity.stream([`effect-local:space:${space.spaceId}:status`], space.status).pipe(
    Stream.filter(predicate),
    Stream.runHead,
    Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: Effect.succeed }))
  )

const isNotOnline = (status: ReplicaStatus.SpaceStatus) => status._tag !== "Online"

const replicaOptions = {
  definition: Domain.definition,
  clientId,
  initialSpaces: [spaceId],
  defaultScope: scope,
  migration,
  retryDelay: "1 minute",
  maximumRetryDelay: "1 minute"
} as const

const schedulerLayer = (constructor: "layer" | "layerWorkflow") => {
  if (constructor === "layer") return SqlReplica.layer(replicaOptions).pipe(Layer.provide(Domain.layerHandlers))
  return SqlReplica.layerWorkflow({ ...replicaOptions, maximumAttempts: 1 }).pipe(
    Layer.provide(Domain.layerHandlers),
    Layer.provide(WorkflowEngine.layerMemory)
  )
}

const activeSpace = Effect.fnUntraced(function*(
  controls: Effect.Success<ReturnType<typeof harness>>,
  layer: Layer.Layer<
    Replica.Replica | QueryReactivity.QueryReactivity,
    ReplicaError.ReplicaError,
    SyncEngine.SyncEngine | SqlClient.SqlClient | Crypto.Crypto | Reactivity.Reactivity
  >
) {
  const context = yield* Layer.build(
    layer.pipe(Layer.provide(controls.layerRemote), Layer.provide(Layer.succeedContext(controls.database)))
  )
  const reactivity = Context.get(controls.database, Reactivity.Reactivity)
  const space = yield* Context.get(context, Replica.Replica).space(spaceId)
  yield* space.activate
  yield* awaitStatus(reactivity, space, (status) => status._tag === "Online")
  return space
})

const statusAfterNextTurnStarts = Effect.fnUntraced(function*(
  controls: Effect.Success<ReturnType<typeof harness>>,
  space: Replica.Space
) {
  yield* controls.setPullMode("Hold")
  yield* space.mutate(Domain.PutTodo, Domain.todo("next"))
  yield* VirtualTime.advanceUntil(Queue.take(controls.heldPulls))
  return yield* space.status
})

const assertFailedWithStorage = (status: ReplicaStatus.ReplicaStatus) => {
  assert.strictEqual(status._tag, "Failed")
  if (status._tag === "Failed") assert.strictEqual(status.message, "StorageUnavailable")
}

describe("scheduler failure reports", () => {
  it.effect(
    "reports a completion failure after a successful sync (Manager)",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      const space = yield* activeSpace(
        controls,
        SqlReplica.layer(replicaOptions).pipe(Layer.provide(Domain.layerHandlers))
      )
      yield* controls.failWhen(failOnce((statement) => statement.includes(completeStatement)))
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* Queue.take(controls.injected)

      assertFailedWithStorage(yield* statusAfterNextTurnStarts(controls, space))
    }, VirtualTime.provide)
  )

  it.effect(
    "reports a failure of the final succeeded step (workflow)",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      const space = yield* activeSpace(
        controls,
        SqlReplica.layerWorkflow(replicaOptions).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(WorkflowEngine.layerMemory)
        )
      )
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 second"))
      yield* controls.failWhen(failAfter(completeStatement, (statement) => statement.includes(countStatement)))
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* Queue.take(controls.injected)

      assertFailedWithStorage(yield* statusAfterNextTurnStarts(controls, space))
    }, VirtualTime.provide)
  )

  it.effect(
    "drains a pending mutation after one lock timeout during a foreground sync (Manager)",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      const space = yield* activeSpace(
        controls,
        SqlReplica.layer(replicaOptions).pipe(Layer.provide(Domain.layerHandlers))
      )
      yield* controls.failWhen(failOnce((statement) => statement.includes(claimStatement)))
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* Queue.take(controls.injected)

      const onlineWithoutPending = awaitStatus(
        Context.get(controls.database, Reactivity.Reactivity),
        space,
        (status) => status._tag === "Online" && status.pending === 0
      )

      const drained = yield* VirtualTime.advanceUntil(onlineWithoutPending).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.provide)
  )

  it.effect(
    "watches again after the watch fails on unavailable storage (Manager)",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      yield* activeSpace(controls, SqlReplica.layer(replicaOptions).pipe(Layer.provide(Domain.layerHandlers)))
      yield* Queue.take(controls.watchStarts)
      yield* controls.failWatch(new ReplicaError.StorageUnavailable({ cause: "injected" }))

      const watchedAgain = yield* VirtualTime.advanceUntil(Queue.take(controls.watchStarts)).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(watchedAgain))
    }, VirtualTime.provide)
  )

  it.effect(
    "drains a pending mutation after one lock timeout during a foreground sync (workflow)",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      const space = yield* activeSpace(
        controls,
        SqlReplica.layerWorkflow(replicaOptions).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(WorkflowEngine.layerMemory)
        )
      )
      yield* controls.failWhen(failOnce((statement) => statement.includes(claimStatement)))
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* Queue.take(controls.injected)

      const onlineWithoutPending = awaitStatus(
        Context.get(controls.database, Reactivity.Reactivity),
        space,
        (status) => status._tag === "Online" && status.pending === 0
      )

      const drained = yield* VirtualTime.advanceUntil(onlineWithoutPending).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.provide)
  )

  it.effect.each([1, 2])(
    "drains a pending mutation after one lock timeout with %s workflow attempts per execution",
    Effect.fnUntraced(function*(maximumAttempts) {
      const controls = yield* harness()
      const space = yield* activeSpace(
        controls,
        SqlReplica.layerWorkflow({ ...replicaOptions, maximumAttempts }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(WorkflowEngine.layerMemory)
        )
      )
      let claims = 0
      const failFirstClaim = failOnce((statement) => statement.includes(claimStatement))
      yield* controls.failWhen((statement) => {
        if (statement.includes(claimStatement)) claims += 1
        return failFirstClaim(statement)
      })
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* Queue.take(controls.injected)
      assertFailedWithStorage(
        yield* awaitStatus(Context.get(controls.database, Reactivity.Reactivity), space, isNotOnline)
      )
      const onlineWithoutPending = awaitStatus(
        Context.get(controls.database, Reactivity.Reactivity),
        space,
        (status) => status._tag === "Online" && status.pending === 0
      )

      const drained = yield* VirtualTime.advanceUntil(onlineWithoutPending).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(drained))
      assert.strictEqual(claims, 2)
    }, VirtualTime.provide)
  )

  it.effect.each([1, 2])(
    "spaces every retry by the backoff while storage stays unavailable with %s workflow attempts per execution",
    Effect.fnUntraced(function*(maximumAttempts) {
      const controls = yield* harness()
      const space = yield* activeSpace(
        controls,
        SqlReplica.layerWorkflow({ ...replicaOptions, maximumAttempts }).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(WorkflowEngine.layerMemory)
        )
      )
      yield* controls.failWhen((statement) => statement.includes(claimStatement))
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* Queue.take(controls.injected)

      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("10 minutes"))

      const attempts = 1 + (yield* Queue.size(controls.injected))
      assert.isAtLeast(attempts, 6)
      assert.isAtMost(attempts, 11)
      assertFailedWithStorage(yield* space.status)
    }, VirtualTime.provide)
  )

  it.effect(
    "watches again after the watch fails on unavailable storage (workflow)",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      yield* activeSpace(
        controls,
        SqlReplica.layerWorkflow(replicaOptions).pipe(
          Layer.provide(Domain.layerHandlers),
          Layer.provide(WorkflowEngine.layerMemory)
        )
      )
      yield* Queue.take(controls.watchStarts)
      yield* controls.failWatch(new ReplicaError.StorageUnavailable({ cause: "injected" }))

      const watchedAgain = yield* VirtualTime.advanceUntil(Queue.take(controls.watchStarts)).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(watchedAgain))
    }, VirtualTime.provide)
  )

  it.effect.each(
    [
      ["layer", "on its own"],
      ["layer", "after another mutation"],
      ["layerWorkflow", "on its own"],
      ["layerWorkflow", "after another mutation"]
    ] as const
  )(
    "drains pending mutations after storage was still unavailable when the retry was admitted with %s %s",
    Effect.fnUntraced(function*([constructor, trigger]) {
      const controls = yield* harness()
      const space = yield* activeSpace(controls, schedulerLayer(constructor))
      yield* controls.failWhen(failEach([claimStatement, admissionStatement]))
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* Queue.take(controls.injected)
      yield* VirtualTime.advanceUntil(Queue.take(controls.injected))
      if (trigger === "after another mutation") yield* space.mutate(Domain.PutTodo, Domain.todo("second"))
      const onlineWithoutPending = awaitStatus(
        Context.get(controls.database, Reactivity.Reactivity),
        space,
        (status) => status._tag === "Online" && status.pending === 0
      )

      const drained = yield* VirtualTime.advanceUntil(onlineWithoutPending).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.provide)
  )

  it.effect.each(["layer", "layerWorkflow"] as const)(
    "drains pending mutations after storage was unavailable when a new credential was admitted with %s",
    Effect.fnUntraced(function*(constructor) {
      const controls = yield* harness()
      const space = yield* activeSpace(controls, schedulerLayer(constructor))
      const reactivity = Context.get(controls.database, Reactivity.Reactivity)
      yield* controls.setPullMode("Reject")
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* awaitStatus(reactivity, space, (status) => status._tag === "NeedsAuthentication")
      yield* controls.failWhen(failEach([admissionStatement]))
      yield* controls.changeCredential
      yield* Queue.take(controls.injected)
      const onlineWithoutPending = awaitStatus(
        reactivity,
        space,
        (status) => status._tag === "Online" && status.pending === 0
      )

      const drained = yield* VirtualTime.advanceUntil(onlineWithoutPending).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.provide)
  )

  it.effect(
    "watches again after storage was unavailable when an ended watch was admitted (workflow)",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      yield* activeSpace(controls, schedulerLayer("layerWorkflow"))
      yield* Queue.take(controls.watchStarts)
      yield* controls.failWhen(failEach([admissionStatement]))
      yield* controls.endWatch
      yield* Queue.take(controls.injected)
      yield* Queue.clear(controls.watchStarts)

      const watchedAgain = yield* VirtualTime.advanceUntil(Queue.take(controls.watchStarts)).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(watchedAgain))
    }, VirtualTime.provide)
  )
})

const inMemory = Effect.fnUntraced(function*(
  controls: Effect.Success<ReturnType<typeof harness>>,
  retryDelay: Duration.Input = "1 minute"
) {
  const statuses = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
  const context = yield* Layer.build(
    Reconciler.layer({
      definition: Domain.definition,
      spaceId,
      retryDelay,
      maximumRetryDelay: "1 minute",
      onStatusChange: (status) => Queue.offer(statuses, status).pipe(Effect.asVoid)
    }).pipe(
      Layer.provideMerge(
        LocalStore.layer({
          definition: Domain.definition,
          spaceId,
          clientId,
          scope,
          retainedReceipts: 256,
          maximumReceipts: 10_000,
          retainedHistoryEntries: 256,
          maximumBootstrapEntities: 10_000,
          maximumBootstrapBytes: 64 * 1024 * 1024,
          maximumBootstrapPageBytes: 4 * 1024 * 1024,
          migration
        }).pipe(Layer.provide(layerRuntime), Layer.provide(Layer.succeedContext(controls.database)))
      ),
      Layer.provide(controls.layerRemote)
    )
  )
  const initial = yield* Queue.take(statuses)
  assert.strictEqual(initial._tag, "Online")
  return {
    reconciler: Context.get(context, Reconciler.Reconciler),
    local: Context.get(context, LocalStore.Store),
    statuses
  }
})

describe("in-memory scheduler failure reports", () => {
  it.effect(
    "reports a completion failure after a successful sync",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      const { local, reconciler } = yield* inMemory(controls)
      yield* controls.failWhen(failOnce((statement) => statement.includes(completeStatement)))
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule
      yield* Queue.take(controls.injected)

      yield* controls.setPullMode("Hold")
      yield* local.mutate(Domain.PutTodo, Domain.todo("next"))
      yield* reconciler.schedule
      yield* VirtualTime.advanceUntil(Queue.take(controls.heldPulls))
      assertFailedWithStorage(yield* reconciler.status)
    }, VirtualTime.provide)
  )

  it.effect(
    "drains a pending mutation after one lock timeout during a sync",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      const { local, reconciler, statuses } = yield* inMemory(controls)
      yield* controls.failWhen(failOnce((statement) => statement.includes(claimStatement)))
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule
      yield* Queue.take(controls.injected)
      let reported = yield* Queue.take(statuses)
      while (reported._tag !== "Failed") reported = yield* Queue.take(statuses)
      assertFailedWithStorage(reported)
      const awaitDrained = Effect.gen(function*() {
        let status = yield* Queue.take(statuses)
        while (status._tag !== "Online" || status.pending !== 0) status = yield* Queue.take(statuses)
        return status
      })

      const drained = yield* VirtualTime.advanceUntil(awaitDrained).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.provide)
  )

  it.effect(
    "watches again after the watch fails on unavailable storage",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      yield* inMemory(controls)
      yield* Queue.take(controls.watchStarts)
      yield* controls.failWatch(new ReplicaError.StorageUnavailable({ cause: "injected" }))

      const watchedAgain = yield* VirtualTime.advanceUntil(Queue.take(controls.watchStarts)).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(watchedAgain))
    }, VirtualTime.provide)
  )

  it.effect.each(
    [
      new ReplicaError.UnknownCommitOutcome({ mutationId: "mutation", cause: "injected" }),
      new ReplicaError.CapacityExceeded({ resource: "read authorizations", limit: 1 }),
      new ReplicaError.OwnerUnavailable({ reason: "transport" })
    ]
  )(
    "drains a pending mutation after one sync fails with a retryable failure",
    Effect.fnUntraced(function*(failure) {
      const controls = yield* harness()
      const { local, reconciler, statuses } = yield* inMemory(controls)
      yield* controls.failNextPull(failure)
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule
      let reported = yield* Queue.take(statuses)
      while (reported._tag !== "Failed") reported = yield* Queue.take(statuses)
      assert.strictEqual(reported.message, failure._tag)
      const awaitDrained = Effect.gen(function*() {
        let status = yield* Queue.take(statuses)
        while (status._tag !== "Online" || status.pending !== 0) status = yield* Queue.take(statuses)
        return status
      })

      const drained = yield* VirtualTime.advanceUntil(awaitDrained).pipe(
        Effect.timeoutOption("10 minutes")
      )

      assert.isTrue(Option.isSome(drained))
    }, VirtualTime.provide)
  )

  it.effect.each(
    [
      new ReplicaError.StorageUnavailable({ cause: "injected" }),
      new ReplicaError.ServerUnavailable()
    ]
  )(
    "grows the delay between watch subscriptions when every watch wakes once and then fails",
    Effect.fnUntraced(function*(failure) {
      const controls = yield* harness()
      yield* inMemory(controls, "1 second")
      yield* Queue.take(controls.watchStarts)
      yield* controls.wakeOnWatch
      yield* controls.failWatch(failure)

      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("10 minutes"))

      const times = controls.watchTimes
      const gaps = times.slice(1).map((time, index) => (time - times[index]) / 1000)
      assert.deepStrictEqual(gaps, [1, 2, 4, 8, 16, 32, 60, 60, 60, 60, 60, 60, 60, 60])
    }, VirtualTime.provide),
    60_000
  )

  it.effect(
    "reports a sync that ends by interruption as Offline",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      const { local, reconciler } = yield* inMemory(controls)
      yield* controls.setPullMode("Interrupt")
      yield* local.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* reconciler.schedule
      yield* Queue.take(controls.transportWaits)

      assert.strictEqual((yield* reconciler.status)._tag, "Offline")
    }, VirtualTime.provide)
  )

  it.effect.each([22, 40, 42, 54, 55, 59, 60])(
    "keeps a rejected watch credential reported during a successful sync at a budget of %s",
    Effect.fnUntraced(function*(budget: number) {
      const controls = yield* harness()
      const { reconciler } = yield* inMemory(controls)
      yield* controls.pauseWhen(failOnce((statement) => statement.includes(countStatement)))
      yield* controls.failWatch(new ReplicaError.CredentialRejected({}))
      const reporting = yield* Queue.take(controls.paused)
      const syncing = yield* reconciler.sync.pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.succeed(reporting.release, undefined)

      yield* Fiber.join(syncing)
      const status = yield* reconciler.status
      assert.strictEqual(status._tag, "NeedsAuthentication", `the report survived at a budget of ${budget}`)
    }, (effect, budget) => VirtualTime.provide(effect).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, budget)))
  )

  it.effect(
    "keeps a rejected watch credential reported while a sync starts",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      const { reconciler } = yield* inMemory(controls)
      yield* controls.pauseWhen(failOnce((statement) => statement.includes(countStatement)))
      yield* controls.failWatch(new ReplicaError.CredentialRejected({}))
      const reporting = yield* Queue.take(controls.paused)
      const later = yield* reconciler.sync.pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.succeed(reporting.release, undefined)

      yield* Fiber.join(later)
      assert.strictEqual((yield* reconciler.status)._tag, "NeedsAuthentication")
    }, VirtualTime.provide)
  )
})

const schedulers = ["layer", "layerWorkflow", "in-memory"] as const

const onlineScheduler = Effect.fnUntraced(function*(
  controls: Effect.Success<ReturnType<typeof harness>>,
  scheduler: typeof schedulers[number]
) {
  if (scheduler === "in-memory") {
    const { local, reconciler } = yield* inMemory(controls)
    return {
      status: reconciler.status,
      mutate: local.mutate(Domain.PutTodo, Domain.todo("later")).pipe(Effect.andThen(reconciler.notify))
    }
  }
  let layerReplica = SqlReplica.layer(replicaOptions).pipe(Layer.provide(Domain.layerHandlers))
  if (scheduler === "layerWorkflow") {
    layerReplica = SqlReplica.layerWorkflow(replicaOptions).pipe(
      Layer.provide(Domain.layerHandlers),
      Layer.provide(WorkflowEngine.layerMemory)
    )
  }
  const space = yield* activeSpace(controls, layerReplica)
  return { status: space.status, mutate: space.mutate(Domain.PutTodo, Domain.todo("later")) }
})

const statusBecomes = <E extends { readonly _tag: string },>(
  status: Effect.Effect<ReplicaStatus.ReplicaStatus, E>,
  tag: ReplicaStatus.ReplicaStatus["_tag"]
) =>
  status.pipe(
    Effect.repeat({ until: (current) => current._tag === tag }),
    Effect.timeoutOption("1 hour"),
    VirtualTime.advanceUntil
  )

describe("a watch whose credential was rejected without a generation", () => {
  it.effect.each(schedulers)(
    "is subscribed again once a later sync succeeds, and live updates resume with %s",
    Effect.fnUntraced(function*(scheduler) {
      const controls = yield* harness()
      const running = yield* onlineScheduler(controls, scheduler)
      yield* Queue.takeAll(controls.watchStarts)
      yield* controls.acceptLaterWatches
      yield* controls.failWatch(new ReplicaError.CredentialRejected({}))
      const rejected = yield* statusBecomes(running.status, "NeedsAuthentication")

      yield* running.mutate
      const resubscribed = yield* Queue.take(controls.watchStarts).pipe(
        Effect.timeoutOption("1 hour"),
        VirtualTime.advanceUntil
      )
      const online = yield* statusBecomes(running.status, "Online")
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))
      const pullsBefore = controls.pulls()
      yield* controls.wake
      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 minute"))

      assert.isTrue(Option.isSome(rejected), "the rejection was reported")
      assert.isTrue(Option.isSome(resubscribed), "the watch was subscribed again")
      assert.isTrue(Option.isSome(online), "the space is online with its watch back")
      assert.isAbove(controls.pulls(), pullsBefore, "a change announced by the server was pulled")
    }, VirtualTime.provide)
  )

  it.effect.each(schedulers)(
    "stays reported while no sync has succeeded since with %s",
    Effect.fnUntraced(function*(scheduler) {
      const controls = yield* harness()
      const running = yield* onlineScheduler(controls, scheduler)
      yield* Queue.takeAll(controls.watchStarts)
      yield* controls.failWatch(new ReplicaError.CredentialRejected({}))
      yield* statusBecomes(running.status, "NeedsAuthentication")
      const watchesBefore = controls.watchTimes.length

      yield* VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption("1 hour"))

      assert.strictEqual((yield* running.status)._tag, "NeedsAuthentication")
      assert.strictEqual(controls.watchTimes.length, watchesBefore)
    }, VirtualTime.provide)
  )
})

const quiet = (duration: Duration.Input) => VirtualTime.advanceUntil(Effect.never).pipe(Effect.timeoutOption(duration))

describe("a sync that left accepted work pending and then failed terminally", () => {
  it.effect.each(schedulers)(
    "is not run again when the retry delay of the stalled sync elapses with %s",
    Effect.fnUntraced(function*(scheduler) {
      const controls = yield* harness()
      const running = yield* onlineScheduler(controls, scheduler)
      yield* controls.acceptAheadOfTheView
      yield* running.mutate
      yield* quiet("30 seconds")
      const stalled = yield* running.status
      yield* controls.failNextPull(new ReplicaError.ProtocolInvalid({ message: "injected" }))
      yield* running.mutate
      yield* statusBecomes(running.status, "Failed")
      const pullsWhenFailed = controls.pulls()

      yield* quiet("10 minutes")

      assert.deepStrictEqual([stalled._tag, stalled.pending], ["Online", 1], "the first sync left its mutation pending")
      assert.strictEqual(controls.pulls(), pullsWhenFailed, "server calls after the terminal failure")
      assert.strictEqual((yield* running.status)._tag, "Failed")
    }, VirtualTime.provide)
  )
})

describe("a new credential for a foreground space of the manager", () => {
  it.effect(
    "is used at once when a failure backoff was pending before the credential was rejected",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      const space = yield* activeSpace(controls, schedulerLayer("layer"))
      yield* controls.failNextPull(new ReplicaError.ServerUnavailable())
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* VirtualTime.quiet("1 second")
      const failed = (yield* space.status)._tag
      yield* controls.acceptLaterWatches
      yield* controls.failWatch(new ReplicaError.CredentialRejected({ credentialGeneration: 1 }))
      yield* VirtualTime.quiet("1 second")
      const paused = (yield* space.status)._tag

      yield* controls.changeCredential
      yield* VirtualTime.quiet("900 millis")
      const status = yield* space.status

      assert.deepStrictEqual([failed, paused], ["Offline", "NeedsAuthentication"])
      assert.deepStrictEqual([status._tag, status.pending], ["Online", 0], "synced within a second of the credential")
    }, VirtualTime.provide)
  )

  it.effect(
    "is used at once when a call failed while the space waited for it",
    Effect.fnUntraced(function*() {
      const controls = yield* harness()
      const space = yield* activeSpace(controls, schedulerLayer("layer"))
      yield* controls.setPullMode("FailWhenReleased")
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* VirtualTime.advanceUntil(Queue.take(controls.heldPulls))
      yield* controls.acceptLaterWatches
      yield* controls.failWatch(new ReplicaError.CredentialRejected({ credentialGeneration: 1 }))
      yield* VirtualTime.quiet("1 second")
      const paused = (yield* space.status)._tag
      yield* controls.releaseHeldPull
      yield* VirtualTime.quiet("1 second")

      yield* controls.changeCredential
      yield* VirtualTime.quiet("900 millis")
      const status = yield* space.status

      assert.strictEqual(paused, "NeedsAuthentication")
      assert.deepStrictEqual([status._tag, status.pending], ["Online", 0], "synced within a second of the credential")
    }, VirtualTime.provide)
  )
})
