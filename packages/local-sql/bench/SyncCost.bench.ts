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
const fullScope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })
const windowedScope = Protocol.ReplicationScope.make({
  models: [],
  windows: [Protocol.ReplicationWindow.make({ model: Domain.Message.name, index: "byChat", count: 50 })]
})
const chats = 100
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

const makeEnvironment = (scope: Protocol.ReplicationScope): Environment => {
  const database = SqliteClient.layer({ filename: ":memory:", disableWAL: true }).pipe(
    (sqlite) => Layer.mergeAll(sqlite, NodeCrypto.layer, Reactivity.layer, QueryReactivity.layer)
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
  const remote = Effect.gen(function*() {
    const server = yield* ServerStore.ServerStore
    return SyncEngine.SyncEngine.of({
      waitForCredentialChange: () => Effect.never,
      discard: (request) => server.discard(request, "reader"),
      submit: server.submit,
      pull: (request) => server.pullAuthorized(request, "reader"),
      bootstrap: (request) => server.bootstrapAuthorized(request, "reader"),
      watch: (request) => server.watchAuthorized(request, "reader").pipe(Stream.unwrap)
    })
  }).pipe(Layer.effect(SyncEngine.SyncEngine), Layer.provide(serverLayer))
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
    runtime: Layer.mergeAll(localLayer, reconcilerLayer, serverLayer, database).pipe((layer) =>
      ManagedRuntime.make(layer)
    ),
    nextSequence: 1
  }
}

const seedTodos = (environment: Environment, entityCount: number) =>
  Effect.gen(function*() {
    const server = yield* ServerStore.ServerStore
    const reconciler = yield* Reconciler.Reconciliation
    const local = yield* LocalStore.Store
    yield* server.submit(
      yield* envelope(Domain.PutManyTodos.name, { count: entityCount }, environment.nextSequence++)
    )
    yield* reconciler.sync
    const lastTodo = yield* local.get(Domain.Todo, `bulk-${entityCount - 1}`)
    lastTodo.pipe(Option.isSome, (present) => assert.isTrue(present))
  }).pipe((effect) => environment.runtime.runPromise(effect))

const seedMessages = (environment: Environment, entityCount: number) =>
  Effect.gen(function*() {
    const server = yield* ServerStore.ServerStore
    const reconciler = yield* Reconciler.Reconciliation
    const local = yield* LocalStore.Store
    yield* server.submit(
      yield* envelope(Domain.PutManyMessages.name, { count: entityCount, chats }, environment.nextSequence++)
    )
    yield* reconciler.sync
    const lastMessage = yield* local.get(Domain.Message, `bulk-${entityCount - 1}`)
    lastMessage.pipe(Option.isSome, (present) => assert.isTrue(present))
  }).pipe((effect) => environment.runtime.runPromise(effect))

const syncOneTodo = (environment: Environment) =>
  Effect.gen(function*() {
    const server = yield* ServerStore.ServerStore
    const reconciler = yield* Reconciler.Reconciliation
    const local = yield* LocalStore.Store
    const sequence = environment.nextSequence++
    const id = `message-${String(sequence).padStart(8, "0")}`
    const todo = Domain.todo(id)
    yield* server.submit(yield* envelope(Domain.PutTodo.name, todo, sequence))
    yield* reconciler.sync
    const syncedTodo = yield* local.get(Domain.Todo, id)
    syncedTodo.pipe(Option.isSome, (present) => assert.isTrue(present))
  }).pipe((effect) => environment.runtime.runPromise(effect))

const syncOneMessage = (environment: Environment) =>
  Effect.gen(function*() {
    const server = yield* ServerStore.ServerStore
    const reconciler = yield* Reconciler.Reconciliation
    const local = yield* LocalStore.Store
    const sequence = environment.nextSequence++
    const id = `live-${String(sequence).padStart(8, "0")}`
    yield* server.submit(
      yield* envelope(
        Domain.PutMessage.name,
        { id, chatId: "chat-0", sentAt: 10_000_000 + sequence, body: `body-${id}` },
        sequence
      )
    )
    yield* reconciler.sync
    const syncedMessage = yield* local.get(Domain.Message, id)
    syncedMessage.pipe(Option.isSome, (present) => assert.isTrue(present))
  }).pipe((effect) => environment.runtime.runPromise(effect))

const configurations = [
  { label: "full scope, space of 1000 entities", scope: fullScope, entityCount: 1_000, iterations: 10 },
  { label: "full scope, space of 10000 entities", scope: fullScope, entityCount: 10_000, iterations: 5 },
  { label: "full scope, space of 100000 entities", scope: fullScope, entityCount: 100_000, iterations: 3 },
  { label: "windowed scope, space of 1000 entities", scope: windowedScope, entityCount: 1_000, iterations: 10 },
  { label: "windowed scope, space of 10000 entities", scope: windowedScope, entityCount: 10_000, iterations: 10 },
  { label: "windowed scope, space of 100000 entities", scope: windowedScope, entityCount: 100_000, iterations: 10 }
]

const environments = new Map<string, Environment>()

afterAll(async () => {
  for (const environment of environments.values()) {
    await environment.runtime.dispose()
  }
})

describe("per-message sync cost by space size", () => {
  for (const { entityCount, iterations, label, scope } of configurations) {
    const windowed = scope === windowedScope
    bench(label, async () => {
      let environment = environments.get(label)
      if (environment === undefined) {
        environment = makeEnvironment(scope)
        environments.set(label, environment)
        if (windowed) await seedMessages(environment, entityCount)
        else await seedTodos(environment, entityCount)
      }
      if (windowed) await syncOneMessage(environment)
      else await syncOneTodo(environment)
    }, { iterations, time: 0, warmupIterations: 1, warmupTime: 0, throws: true })
  }
})
