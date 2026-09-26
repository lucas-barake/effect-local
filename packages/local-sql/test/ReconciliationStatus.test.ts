import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import { gateStatements } from "./fixtures/SqlGate.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")

const database = () =>
  Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const

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
}).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(database())
)

const layerDirectSync = Layer.effect(
  SyncEngine.SyncEngine,
  Effect.gen(function*() {
    const server = yield* ServerStore.ServerStore
    return SyncEngine.SyncEngine.of({
      waitForCredentialChange: () => Effect.never,
      transportGeneration: Effect.succeed(0),
      waitForTransportChange: () => Effect.never,
      submitBatch: (request) => server.admitBatch(request, null),
      discard: (request) => server.discard(request, null),
      pull: server.pull,
      bootstrap: server.bootstrap,
      watch: server.watch
    })
  })
).pipe(Layer.provide(layerServer))

const layerClientDatabase = database()
const localOptions = {
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
  migration
} satisfies LocalStore.Options
const layerLocal = LocalStore.layer(localOptions).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(layerClientDatabase)
)
const layerReconciliation = Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
  Layer.provide(layerLocal),
  Layer.provide(layerDirectSync)
)

describe("reconciliation status", () => {
  it.effect(
    "starts Connecting and reports Online only after its own sync succeeds",
    Effect.fnUntraced(function*() {
      const reconciliation = Context.get(yield* Layer.build(layerReconciliation), Reconciler.Reconciliation)
      assert.strictEqual((yield* reconciliation.status)._tag, "Connecting")

      yield* reconciliation.succeeded
      assert.strictEqual((yield* reconciliation.status)._tag, "Connecting")

      yield* reconciliation.sync
      assert.strictEqual((yield* reconciliation.status)._tag, "Online")
    })
  )

  it.effect(
    "reports an installed view even when the pass is interrupted while installing it",
    Effect.fnUntraced(function*() {
      const clientDatabase = yield* Layer.build(database())
      const sql = Context.get(clientDatabase, SqlClient.SqlClient)
      const gate = yield* gateStatements(sql, (statement) => {
        if (statement.includes("replication_view_id = ?") && statement.includes("installed_snapshot_id = ?")) {
          return ["after"]
        }
        return []
      })
      const reports = yield* Ref.make<ReadonlyArray<boolean>>([])
      const gatedDatabase = Context.add(clientDatabase, SqlClient.SqlClient, gate.sql)
      const layerGatedLocal = LocalStore.layer({
        ...localOptions,
        onReplicationView: (installed) => Ref.update(reports, (previous) => [...previous, installed])
      }).pipe(
        Layer.provide(layerRuntime),
        Layer.provide(Layer.succeedContext(gatedDatabase))
      )
      const reconciliation = Context.get(
        yield* Layer.build(
          Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(layerGatedLocal),
            Layer.provide(layerDirectSync)
          )
        ),
        Reconciler.Reconciliation
      )
      const pass = yield* reconciliation.sync.pipe(Effect.forkChild({ startImmediately: true }))
      const installing = yield* Queue.take(gate.pauses)
      const interrupting = yield* Fiber.interrupt(pass).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.succeed(installing.release, undefined)
      yield* Fiber.join(interrupting)

      const view = yield* sql<
        { readonly installed: number }
      >`SELECT COUNT(*) AS installed FROM effect_local_client_spaces
        WHERE space_id = ${spaceId} AND replication_view_id IS NOT NULL`
      assert.strictEqual(view[0]?.installed, 1)
      assert.strictEqual((yield* Ref.get(reports)).at(-1), true)
    })
  )

  it.effect(
    "records one durable reconciliation request for a burst of wakes that arrive during one turn",
    Effect.fnUntraced(function*() {
      const clientDatabase = yield* Layer.build(database())
      const local = Context.get(
        yield* Layer.build(
          LocalStore.layer(localOptions).pipe(
            Layer.provide(layerRuntime),
            Layer.provide(Layer.succeedContext(clientDatabase))
          )
        ),
        LocalStore.Store
      )
      const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
      const pullEntered = yield* Deferred.make<void>()
      const pullRelease = yield* Deferred.make<void>()
      const wakesDrained = yield* Deferred.make<void>()
      const emitWakes = yield* Deferred.make<void>()
      const wakes = 20
      const remote = SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        transportGeneration: Effect.succeed(0),
        waitForTransportChange: () => Effect.never,
        submitBatch: (request) => server.admitBatch(request, null),
        discard: (request) => server.discard(request, null),
        pull: (request) =>
          Deferred.succeed(pullEntered, undefined).pipe(
            Effect.andThen(Deferred.await(pullRelease)),
            Effect.andThen(server.pull(request))
          ),
        bootstrap: server.bootstrap,
        watch: (request) =>
          Stream.fromEffect(Deferred.await(emitWakes)).pipe(
            Stream.flatMap(() =>
              Stream.fromIterable(Array.from({ length: wakes }, () => ({ spaceId: request.spaceId })))
            ),
            Stream.concat(Stream.fromEffect(Deferred.succeed(wakesDrained, undefined)).pipe(Stream.drain)),
            Stream.concat(Stream.never)
          )
      })
      const reconciliation = Context.get(
        yield* Layer.build(
          Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
            Layer.provide(Layer.succeed(LocalStore.Store, local)),
            Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
          )
        ),
        Reconciler.Reconciliation
      )
      const manager = yield* Reconciler.makeManager().pipe(Effect.provideService(SyncEngine.SyncEngine, remote))
      yield* manager.register({ spaceId, generation: 1, definition: Domain.definition, local, reconciliation })
      yield* Deferred.await(pullEntered)
      const observed = yield* local.reconciliationGenerations

      yield* Deferred.succeed(emitWakes, undefined)
      yield* Deferred.await(wakesDrained)

      assert.strictEqual((yield* local.reconciliationGenerations).requested, observed.requested + 1)
      yield* Deferred.succeed(pullRelease, undefined)
    }, Effect.scoped)
  )
})
