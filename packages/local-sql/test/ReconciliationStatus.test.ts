import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as LocalStore from "../src/LocalStore.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

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
      submit: server.submit,
      discard: (request) => server.discard(request, null),
      pull: server.pull,
      bootstrap: server.bootstrap,
      watch: server.watch
    })
  })
).pipe(Layer.provide(layerServer))

const layerClientDatabase = database()
const layerLocal = LocalStore.layer({
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
}).pipe(
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
})
