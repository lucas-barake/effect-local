import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Context from "effect/Context"
import type * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
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

type PullMode = "Pass" | "Hold" | "Interrupt"

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
            Effect.fail(new SqlError.SqlError({ reason: new SqlError.UnknownError({ cause: "injected" }) }))
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
  let pullMode: PullMode = "Pass"
  const heldPulls = yield* Queue.unbounded<void>()
  const transportWaits = yield* Queue.unbounded<void>()
  const remote = SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Queue.offer(transportWaits, undefined).pipe(Effect.andThen(Effect.never)),
    submitBatch: (request) => server.admitBatch(request, null),
    discard: (request) => server.discard(request, null),
    pull: (request) =>
      Effect.suspend(() => {
        if (pullMode === "Interrupt") {
          pullMode = "Pass"
          return Effect.interrupt
        }
        if (pullMode === "Hold") return Queue.offer(heldPulls, undefined).pipe(Effect.andThen(Effect.never))
        return server.pull(request)
      }),
    bootstrap: server.bootstrap,
    watch: () => Stream.fromEffect(Deferred.await(watchFailure).pipe(Effect.flatMap(Effect.fail)))
  })
  return {
    database: Context.add(database, SqlClient.SqlClient, gate.sql),
    layerRemote: Layer.succeed(SyncEngine.SyncEngine, remote),
    heldPulls,
    transportWaits,
    injected,
    paused: gate.pauses,
    failWatch: (error: ReplicaError.ReplicaError) => Deferred.succeed(watchFailure, error),
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

const replicaOptions = {
  definition: Domain.definition,
  clientId,
  initialSpaces: [spaceId],
  defaultScope: scope,
  migration,
  retryDelay: "1 minute",
  maximumRetryDelay: "1 minute"
} as const

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
  yield* Queue.take(controls.heldPulls)
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
    })
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
      yield* controls.failWhen(failAfter(completeStatement, (statement) => statement.includes(countStatement)))
      yield* space.mutate(Domain.PutTodo, Domain.todo("first"))
      yield* Queue.take(controls.injected)

      assertFailedWithStorage(yield* statusAfterNextTurnStarts(controls, space))
    })
  )
})

const inMemory = Effect.fnUntraced(function*(controls: Effect.Success<ReturnType<typeof harness>>) {
  const statuses = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
  const context = yield* Layer.build(
    Reconciler.layer({
      definition: Domain.definition,
      spaceId,
      retryDelay: "1 minute",
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
    local: Context.get(context, LocalStore.Store)
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
      yield* Queue.take(controls.heldPulls)
      assertFailedWithStorage(yield* reconciler.status)
    })
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
    })
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
    })
  )
})
