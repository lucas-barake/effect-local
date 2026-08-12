import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import type * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import { afterAll, assert, bench, describe } from "vitest"
import * as LocalStore from "../src/LocalStore.js"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as Reconciler from "../src/Reconciler.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "../test/Domain.js"

/* oxlint-disable effect/noTestLifecycleHooks, effect/noAsyncFunction, no-await-in-loop -- Vitest owns benchmark fixture setup, timing callbacks, and teardown. */

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const writerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const readerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options

const envelope = (name: string, payload: typeof Protocol.MutationEnvelope.Type["payload"], sequence: number) =>
  Effect.gen(function*() {
    const identity = {
      spaceId,
      clientId: writerId,
      mutationId: Identity.MutationId.make(
        `mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`
      ),
      localSequence: Identity.LocalSequence.make(sequence),
      basis: Identity.ServerSequence.make(0),
      name,
      payload,
      digestVersion: 3 as const,
      membershipIncarnation,
      sourceSchema: Domain.definition.schemaIdentity,
      mutationVersion: Identity.SchemaVersion.make(1)
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
  })

interface Environment {
  readonly runtime: ManagedRuntime.ManagedRuntime<
    Crypto.Crypto | LocalStore.Store | Reconciler.Reconciliation | ServerStore.ServerStore,
    unknown
  >
  nextSequence: number
}

const makeEnvironment = (entityCount: number): Environment => {
  const database = Layer.mergeAll(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer,
    Reactivity.layer,
    QueryReactivity.layer
  )
  const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))
  const serverLayer = ServerStore.layer({
    definition: Domain.definition,
    wakeCapacity: 16,
    maximumWatchersPerSpace: 1_024,
    maximumConcurrentReadAuthorizations: 64,
    maximumPendingReadAuthorizations: 4_096,
    readAuthorizationCacheCapacity: 4_096,
    retainedHistoryEntries: 1_024,
    maximumHistoryEntries: 1_000_000,
    retainedReceipts: 256,
    maximumReceipts: 1_000_000,
    maximumSnapshotEntities: 200_000,
    maximumSnapshotBytes: 256 * 1024 * 1024,
    maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
    pruneBatchSize: 1_000,
    retainedSnapshots: 2,
    maintenanceConcurrency: 1,
    maintenanceSpaceBatchSize: 128,
    migration,
    readAuthorizationRefreshInterval: "1 second",
    authorizeAccess: () => Effect.void,
    authorizeMutation: () => Effect.void,
    authorizeRead: () => Effect.void
  }).pipe(Layer.provide(runtime), Layer.provide(database))
  const remote = Layer.effect(
    SyncEngine.SyncEngine,
    Effect.gen(function*() {
      const server = yield* ServerStore.ServerStore
      return SyncEngine.SyncEngine.of({
        waitForCredentialChange: () => Effect.never,
        discard: (request) => server.discard(request, "reader"),
        submit: server.submit,
        pull: (request) => server.pullAuthorized(request, "reader"),
        bootstrap: (request) => server.bootstrapAuthorized(request, "reader"),
        watch: (request) => Stream.unwrap(server.watchAuthorized(request, "reader"))
      })
    })
  ).pipe(Layer.provide(serverLayer))
  const localLayer = LocalStore.layer({
    settlementCapacity: 64,
    retainedReceipts: 256,
    maximumReceipts: 1_000_000,
    retainedHistoryEntries: 1_024,
    maximumBootstrapEntities: 200_000,
    maximumBootstrapBytes: 256 * 1024 * 1024,
    maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
    migration,
    definition: Domain.definition,
    spaceId,
    clientId: readerId,
    scope
  }).pipe(Layer.provide(runtime), Layer.provide(database))
  const reconcilerLayer = Reconciler.layerOnePass({
    definition: Domain.definition,
    spaceId
  }).pipe(Layer.provide(localLayer), Layer.provide(remote))
  return {
    runtime: ManagedRuntime.make(
      Layer.mergeAll(localLayer, reconcilerLayer, serverLayer, database)
    ),
    nextSequence: 1
  }
}

const seed = (environment: Environment, entityCount: number) =>
  environment.runtime.runPromise(Effect.gen(function*() {
    const server = yield* ServerStore.ServerStore
    const reconciler = yield* Reconciler.Reconciliation
    const local = yield* LocalStore.Store
    yield* server.submit(
      yield* envelope(Domain.PutManyTodos.name, { count: entityCount }, environment.nextSequence++)
    )
    yield* reconciler.sync
    assert.isTrue(Option.isSome(yield* local.get(Domain.Todo, "bulk-0")))
    assert.isTrue(Option.isSome(yield* local.get(Domain.Todo, `bulk-${entityCount - 1}`)))
  }))

const syncOneMessage = (environment: Environment) =>
  environment.runtime.runPromise(Effect.gen(function*() {
    const server = yield* ServerStore.ServerStore
    const reconciler = yield* Reconciler.Reconciliation
    const local = yield* LocalStore.Store
    const sequence = environment.nextSequence++
    const id = `message-${String(sequence).padStart(8, "0")}`
    yield* server.submit(yield* envelope(Domain.PutTodo.name, Domain.todo(id), sequence))
    yield* reconciler.sync
    assert.isTrue(Option.isSome(yield* local.get(Domain.Todo, id)))
  }))

const configurations = [
  { entityCount: 1_000, iterations: 10 },
  { entityCount: 10_000, iterations: 5 },
  { entityCount: 100_000, iterations: 3 }
]

const environments = new Map<number, Environment>()

afterAll(async () => {
  for (const environment of environments.values()) {
    await environment.runtime.dispose()
  }
})

describe("per-message sync cost by space size", () => {
  for (const { entityCount, iterations } of configurations) {
    bench(`space of ${entityCount} entities`, async () => {
      let environment = environments.get(entityCount)
      if (environment === undefined) {
        environment = makeEnvironment(entityCount)
        environments.set(entityCount, environment)
        await seed(environment, entityCount)
      }
      await syncOneMessage(environment)
    }, { iterations, time: 0, warmupIterations: 1, warmupTime: 0, throws: true })
  }
})
