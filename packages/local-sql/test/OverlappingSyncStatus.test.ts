import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as SqlClient from "effect/sql/SqlClient"
import * as Stream from "effect/Stream"
import * as ConnectionLane from "../src/ConnectionLane.js"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"
import { gateStatements } from "./fixtures/SqlGate.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-0000000000c1")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-0000000000c1")

const database = () =>
  Layer.mergeAll(
    ConnectionLane.makeLayer().pipe(Layer.provideMerge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const

const layerServer = ServerStore.layerTrusted({ definition: Domain.definition, migration }).pipe(
  Layer.provide(layerRuntime),
  Layer.provide(database())
)

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

const harness = Effect.fnUntraced(function*() {
  const server = Context.get(yield* Layer.build(layerServer), ServerStore.ServerStore)
  const clientDatabase = yield* Layer.build(database())
  let pauseNextPendingCount = false
  const gate = yield* gateStatements(Context.get(clientDatabase, SqlClient.SqlClient), (statement) => {
    if (
      !pauseNextPendingCount ||
      !statement.includes("SELECT COUNT(*) AS count FROM effect_local_client_pending_data")
    ) {
      return []
    }
    pauseNextPendingCount = false
    return ["before"]
  })
  const gatedDatabase = Context.add(clientDatabase, SqlClient.SqlClient, gate.sql)
  let nextPull: { readonly fail: boolean; readonly release: Deferred.Deferred<void> } | undefined
  const heldPulls = yield* Queue.unbounded<void>()
  const remote = SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    transportGeneration: Effect.succeed(0),
    waitForTransportChange: () => Effect.never,
    submitBatch: (request) => server.admitBatch(request, null),
    discard: (request) => server.discard(request, null),
    pull: (request) =>
      Effect.suspend(() => {
        const held = nextPull
        if (held === undefined) return server.pull(request)
        nextPull = undefined
        return Queue.offer(heldPulls, undefined).pipe(
          Effect.andThen(Deferred.await(held.release)),
          Effect.andThen(Effect.suspend(() => {
            if (held.fail) return Effect.fail(new ReplicaError.ServerUnavailable())
            return server.pull(request)
          }))
        )
      }),
    bootstrap: server.bootstrap,
    watch: () => Stream.never
  })
  const reconciliation = Context.get(
    yield* Layer.build(
      Reconciler.layerOnePass({ definition: Domain.definition, spaceId }).pipe(
        Layer.provide(
          LocalStore.layer(localOptions).pipe(
            Layer.provide(layerRuntime),
            Layer.provide(Layer.succeedContext(gatedDatabase))
          )
        ),
        Layer.provide(Layer.succeed(SyncEngine.SyncEngine, remote))
      )
    ),
    Reconciler.Reconciliation
  )
  const holdNextPull = Effect.fnUntraced(function*(fail: boolean) {
    const release = yield* Deferred.make<void>()
    nextPull = { fail, release }
    return release
  })
  return {
    reconciliation,
    heldPulls,
    pendingCountPauses: gate.pauses,
    holdNextPull,
    pauseNextPendingCount: Effect.sync(() => {
      pauseNextPendingCount = true
    })
  }
})

describe("overlapping sync passes", () => {
  it.effect(
    "reports Online after a pass that starts while an earlier pass reports its failure",
    Effect.fnUntraced(function*() {
      const { heldPulls, holdNextPull, pauseNextPendingCount, pendingCountPauses, reconciliation } = yield* harness()
      yield* reconciliation.sync
      assert.strictEqual((yield* reconciliation.status)._tag, "Online")

      const releaseFailure = yield* holdNextPull(true)
      const earlier = yield* reconciliation.sync.pipe(Effect.forkChild({ startImmediately: true }))
      yield* Queue.take(heldPulls)
      yield* pauseNextPendingCount
      yield* Deferred.succeed(releaseFailure, undefined)
      const reporting = yield* Queue.take(pendingCountPauses)
      const later = yield* reconciliation.sync.pipe(Effect.forkChild({ startImmediately: true }))
      yield* Deferred.succeed(reporting.release, undefined)

      yield* Fiber.join(later)
      yield* Fiber.await(earlier)
      assert.strictEqual((yield* reconciliation.status)._tag, "Online")
    })
  )

  it.effect(
    "keeps a later pass Online when a scheduler reports the earlier pass failure again",
    Effect.fnUntraced(function*() {
      const { heldPulls, holdNextPull, reconciliation } = yield* harness()
      yield* reconciliation.sync

      const releaseFailure = yield* holdNextPull(true)
      yield* Deferred.succeed(releaseFailure, undefined)
      const earlierFailure = yield* reconciliation.sync.pipe(
        Effect.andThen(Effect.die("expected the earlier pass to fail")),
        Effect.catchTag("ServerUnavailable", Effect.succeed)
      )
      yield* Queue.take(heldPulls)
      assert.strictEqual((yield* reconciliation.status)._tag, "Offline")

      const releaseLater = yield* holdNextPull(false)
      const later = yield* reconciliation.sync.pipe(Effect.forkChild({ startImmediately: true }))
      yield* Queue.take(heldPulls)
      yield* reconciliation.failed(earlierFailure)
      yield* Deferred.succeed(releaseLater, undefined)

      yield* Fiber.join(later)
      assert.strictEqual((yield* reconciliation.status)._tag, "Online")
    })
  )
})
