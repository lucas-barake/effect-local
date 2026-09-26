import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as MutationRuntime from "../src/MutationRuntime.js"
import * as ServerStore from "../src/ServerStore.js"
import * as SqlReplica from "../src/SqlReplica.js"
import * as SyncEngine from "../src/SyncEngine.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000301")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000301")
const mutationId = Identity.MutationId.make("mut_00000000-0000-4000-8000-000000000301")
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } as const

const layerDatabase = () =>
  Layer.mergeAll(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)

const layerRuntime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.layerHandlers))

const layerServer = ServerStore.layerTrusted({
  definition: Domain.definition,
  readAuthorizationRefreshInterval: "30 seconds",
  maximumWatchersPerSpace: 1_024,
  maximumConcurrentReadAuthorizations: 64,
  maximumPendingReadAuthorizations: 4_096,
  readAuthorizationCacheCapacity: 4_096,
  retainedHistoryEntries: 256,
  maximumHistoryEntries: 10_000,
  retainedReceipts: 256,
  maximumReceipts: 10_000,
  maximumSnapshotEntities: 10_000,
  maximumSnapshotBytes: 64 * 1024 * 1024,
  maximumBootstrapPageBytes: Protocol.maximumBatchBytes,
  pruneBatchSize: 1_000,
  retainedSnapshots: 2,
  maintenanceConcurrency: 1,
  maintenanceSpaceBatchSize: 128,
  migration
}).pipe(Layer.provide(layerRuntime), Layer.provide(layerDatabase()))

const layerConnectedSync = Effect.gen(function*() {
  const store = yield* ServerStore.ServerStore
  return SyncEngine.SyncEngine.of({
    waitForCredentialChange: () => Effect.never,
    submit: store.submit,
    discard: (request) => store.discard(request, null),
    pull: store.pull,
    bootstrap: store.bootstrap,
    watch: store.watch
  })
}).pipe(Layer.effect(SyncEngine.SyncEngine), Layer.provide(layerServer))

const layerDisconnectedSync = Layer.succeed(SyncEngine.SyncEngine, {
  waitForCredentialChange: () => Effect.never,
  submit: () => Effect.never,
  discard: () => Effect.never,
  pull: () => Effect.never,
  bootstrap: () => Effect.never,
  watch: () => Stream.never
})

const layerReplica = <E,>(
  layerSync: Layer.Layer<SyncEngine.SyncEngine, E>,
  options: { readonly retainedReceipts?: number; readonly retainedMutationIds?: number } = {}
) =>
  SqlReplica.layer({
    definition: Domain.definition,
    clientId,
    initialSpaces: [spaceId],
    retryDelay: "10 millis",
    ...options
  }).pipe(
    Layer.provide(layerSync),
    Layer.provide(Domain.layerHandlers),
    Layer.provide(layerDatabase()),
    Layer.provide(Reactivity.layer)
  )

const space = Replica.Replica.use((replica) => replica.space(spaceId))
const provideOffline = Effect.provide(layerReplica(layerDisconnectedSync))
const provideOnline = Effect.provide(layerReplica(layerConnectedSync))
const provideOnlineWithoutReceipts = Effect.provide(layerReplica(layerConnectedSync, { retainedReceipts: 0 }))
const provideOnlineWithOneRetiredId = Effect.provide(
  layerReplica(layerConnectedSync, { retainedReceipts: 0, retainedMutationIds: 1 })
)

describe("caller-minted mutation ids", () => {
  it.effect(
    "returns the recorded pending mutation when the id is reused before it settles",
    Effect.fnUntraced(function*() {
      const target = yield* space
      const first = yield* target.mutate(Domain.PutTodo, Domain.todo("todo-1"), { mutationId })
      const second = yield* target.mutate(Domain.PutTodo, Domain.todo("todo-1"), { mutationId })
      assert.strictEqual(first.envelope.mutationId, mutationId)
      assert.deepStrictEqual(second.envelope, first.envelope)
      const pending = yield* target.pending
      assert.deepStrictEqual(pending.map((entry) => entry.envelope.mutationId), [mutationId])
    }, provideOffline)
  )

  it.effect(
    "fails with MutationIdentityConflict when the id is reused for a different mutation",
    Effect.fnUntraced(function*() {
      const target = yield* space
      yield* target.mutate(Domain.PutTodo, Domain.todo("todo-1"), { mutationId })
      const outcome = yield* target.mutate(Domain.PutTodo, Domain.todo("todo-2"), { mutationId }).pipe(
        Effect.as("mutated" as const),
        Effect.catchTag("MutationIdentityConflict", () => Effect.succeed("conflict" as const))
      )
      assert.strictEqual(outcome, "conflict")
      const pending = yield* target.pending
      assert.strictEqual(pending.length, 1)
    }, provideOffline)
  )

  it.effect(
    "returns the settled mutation when the id is reused after the server accepted it",
    Effect.fnUntraced(function*() {
      const target = yield* space
      const first = yield* target.mutate(Domain.PutTodo, Domain.todo("todo-1"), { mutationId })
      const settled = yield* target.settlements({ from: 0 }).pipe(Stream.runHead)
      assert.isTrue(Option.isSome(settled))
      const replayed = yield* target.mutate(Domain.PutTodo, Domain.todo("todo-1"), { mutationId })
      assert.deepStrictEqual(replayed.envelope, first.envelope)
      assert.deepStrictEqual(yield* target.pending, [])
    }, provideOnline)
  )

  it.effect(
    "fails with MutationIdentityConflict when the id is reused after its receipt was pruned",
    Effect.fnUntraced(function*() {
      const target = yield* space
      yield* target.mutate(Domain.PutTodo, Domain.todo("todo-1"), { mutationId })
      yield* target.settlements({ from: 0 }).pipe(Stream.take(1), Stream.runDrain)
      yield* target.mutate(Domain.PutTodo, Domain.todo("todo-2"))
      yield* target.settlements({ from: 1 }).pipe(Stream.take(1), Stream.runDrain)
      const outcome = yield* target.mutate(Domain.PutTodo, Domain.todo("todo-1"), { mutationId }).pipe(
        Effect.as("executed again" as const),
        Effect.catchTag("MutationIdentityConflict", () => Effect.succeed("conflict" as const))
      )
      assert.strictEqual(outcome, "conflict")
      assert.deepStrictEqual(yield* target.pending, [])
    }, provideOnlineWithoutReceipts)
  )

  it.effect(
    "accepts a retired id as a new mutation once retainedMutationIds newer mutations were issued",
    Effect.fnUntraced(function*() {
      const target = yield* space
      yield* target.mutate(Domain.PutTodo, Domain.todo("todo-1"), { mutationId })
      yield* target.settlements({ from: 0 }).pipe(Stream.take(1), Stream.runDrain)
      yield* target.mutate(Domain.PutTodo, Domain.todo("todo-2"))
      yield* target.settlements({ from: 1 }).pipe(Stream.take(1), Stream.runDrain)
      yield* target.mutate(Domain.PutTodo, Domain.todo("todo-3"))
      yield* target.settlements({ from: 2 }).pipe(Stream.take(1), Stream.runDrain)
      const replayed = yield* target.mutate(Domain.PutTodo, Domain.todo("todo-1"), { mutationId })
      assert.strictEqual(replayed.envelope.mutationId, mutationId)
      assert.strictEqual(replayed.envelope.localSequence, 4)
    }, provideOnlineWithOneRetiredId)
  )
})
