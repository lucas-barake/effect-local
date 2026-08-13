import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import type * as Migrations from "../src/Migrations.js"
import * as MutationRuntime from "../src/MutationRuntime.js"
import type * as OfflineWake from "../src/OfflineWake.js"
import * as QueryReactivity from "../src/QueryReactivity.js"
import * as ServerStore from "../src/ServerStore.js"
import * as Domain from "./Domain.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const writerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000001")
const readerId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })

const database = SqliteClient.layer({ filename: ":memory:", disableWAL: true }).pipe((sqlite) =>
  Layer.mergeAll(sqlite, NodeCrypto.layer, Reactivity.layer, QueryReactivity.layer)
)
const runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.handlers))
const migration = { retryDelay: "1 millis", maximumAttempts: 8 } satisfies Migrations.Options
const serverOptions = {
  definition: Domain.definition,
  readAuthorizationRefreshInterval: "1 second" as const,
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
  migration,
  authorizeAccess: () => Effect.void,
  authorizeMutation: () => Effect.void,
  authorizeRead: () => Effect.void
} satisfies ServerStore.Options

const wakeTiming = {
  coalescingWindow: "1 second",
  pollInterval: "1 second",
  retryDelay: "1 second",
  maximumRetryDelay: "1 minute",
  claimLeaseDuration: "30 seconds",
  hookTimeout: "10 seconds",
  presenceLeaseDuration: "30 seconds",
  presenceHeartbeatInterval: "10 seconds",
  claimBatchSize: 32,
  maximumConcurrentRecipientResolutions: 8,
  maximumConcurrentDeliveries: 8,
  maximumRecipientsPerSpace: 1_000
} as const

class TestWakeError extends Schema.TaggedErrorClass<TestWakeError, Schema.JsonObject>("test/TestWakeError")(
  "TestWakeError",
  { reason: Schema.String }
) {}

const service = <I, S, E extends { readonly _tag: string }, R,>(
  tag: Context.Service<I, S>,
  layer: Layer.Layer<I, E, R>
) => Layer.build(layer).pipe(Effect.map(Context.get(tag)))

const makeServer = (
  offlineWake: OfflineWake.Options,
  databaseLayer: typeof database = database
) => {
  const storeLayer = ServerStore.layer({ ...serverOptions, offlineWake }).pipe(
    Layer.provide(runtime),
    Layer.provide(databaseLayer)
  )
  return service(ServerStore.ServerStore, storeLayer)
}

const envelope = (sequence: number) =>
  Effect.gen(function*() {
    const identity = {
      spaceId,
      clientId: writerId,
      mutationId: Identity.MutationId.make(
        `mut_00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`
      ),
      localSequence: Identity.LocalSequence.make(sequence),
      basis: Identity.ServerSequence.make(0),
      name: Domain.PutTodo.name,
      payload: pipe(sequence, String, Domain.todo),
      digestVersion: 3 as const,
      membershipIncarnation,
      sourceSchema: Domain.definition.schemaIdentity,
      mutationVersion: Domain.PutTodo.version
    }
    return Protocol.MutationEnvelope.make({ ...identity, digest: yield* Protocol.mutationDigest(identity) })
  })

const pullRequest = (cursor: Protocol.ReplicationCursor | null = null): Protocol.PullRequest =>
  Protocol.PullRequest.make({
    spaceId,
    clientId: readerId,
    schema: Domain.definition.schemaIdentity,
    scope,
    scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
    cursor,
    limit: 10
  })

const watchRequest = (): Protocol.WatchRequest =>
  Protocol.WatchRequest.make({
    spaceId,
    clientId: readerId,
    schema: Domain.definition.schemaIdentity,
    scope,
    scopeGeneration: Identity.ReplicationScopeGeneration.make(1),
    cursor: null
  })

const incremental = (result: Protocol.PullResult): Protocol.PullPage => {
  if ("_tag" in result) assert.fail("expected an incremental pull page")
  return result
}

const submit = (server: ServerStore.Service, sequence: number) =>
  envelope(sequence).pipe(
    Effect.provide(NodeCrypto.layer),
    Effect.flatMap(server.submit)
  )

const pull = (server: ServerStore.Service, cursor: Protocol.ReplicationCursor | null = null) =>
  pipe(cursor, pullRequest, server.pull)

describe("offline wake delivery", () => {
  it.effect("delivers one durable content free wake for a disconnected member", () =>
    Effect.gen(function*() {
      const deliveries = yield* Ref.make<ReadonlyArray<OfflineWake.Delivery>>([])
      const offlineWake = {
        recipients: () => Effect.succeed([readerId]),
        deliver: (wake: OfflineWake.Delivery) => Ref.update(deliveries, (current) => [...current, wake]),
        ...wakeTiming
      } satisfies OfflineWake.Options
      const server = yield* makeServer(offlineWake)

      const submitted = yield* envelope(1).pipe(Effect.provide(NodeCrypto.layer))
      const receipt = yield* server.submit(submitted)
      assert.strictEqual(receipt._tag, "Accepted")
      yield* TestClock.adjust("2 seconds")
      yield* Effect.yieldNow

      const delivered = yield* Ref.get(deliveries)
      assert.lengthOf(delivered, 1)
      assert.strictEqual(delivered[0].spaceId, spaceId)
      assert.strictEqual(delivered[0].clientId, readerId)
      const isWakeId = Schema.is(Identity.WakeId)
      const wakeIdValid = isWakeId(delivered[0].wakeId)
      assert.isTrue(wakeIdValid)

      yield* TestClock.adjust("10 seconds")
      yield* Effect.yieldNow
      const laterDeliveries = yield* Ref.get(deliveries)
      assert.lengthOf(laterDeliveries, 1)
    }).pipe(Effect.scoped))

  it.effect("redelivers one stable wake identity after the delivery hook fails", () =>
    Effect.gen(function*() {
      const attempts = yield* Queue.unbounded<OfflineWake.Delivery>()
      const attemptCount = yield* Ref.make(0)
      const offlineWake = {
        recipients: () => Effect.succeed([readerId]),
        deliver: (wake: OfflineWake.Delivery) =>
          Effect.gen(function*() {
            yield* Queue.offer(attempts, wake)
            const attempt = yield* Ref.updateAndGet(attemptCount, (value) => value + 1)
            if (attempt === 1) return yield* new TestWakeError({ reason: "provider unavailable" })
            return yield* Effect.void
          }),
        ...wakeTiming
      } satisfies OfflineWake.Options
      const server = yield* makeServer(offlineWake)

      const receipt = yield* submit(server, 1)
      assert.strictEqual(receipt._tag, "Accepted")
      yield* TestClock.adjust("1 second")
      const first = yield* Queue.take(attempts)
      assert.strictEqual(yield* Ref.get(attemptCount), 1)

      yield* TestClock.adjust("1 second")
      const second = yield* Queue.take(attempts)
      assert.deepStrictEqual(second, first)
      assert.strictEqual(yield* Ref.get(attemptCount), 2)
    }).pipe(Effect.scoped))

  it.effect("coalesces repeated accepted mutations behind one high water fence", () =>
    Effect.gen(function*() {
      const deliveries = yield* Ref.make<ReadonlyArray<OfflineWake.Delivery>>([])
      const offlineWake = {
        recipients: () => Effect.succeed([readerId]),
        deliver: (wake: OfflineWake.Delivery) => Ref.update(deliveries, (current) => [...current, wake]),
        ...wakeTiming
      } satisfies OfflineWake.Options
      const server = yield* makeServer(offlineWake)

      for (let sequence = 1; sequence <= 3; sequence++) {
        const receipt = yield* submit(server, sequence)
        assert.strictEqual(receipt._tag, "Accepted")
      }
      yield* TestClock.adjust("2 seconds")
      yield* Effect.yieldNow
      const delivered = yield* Ref.get(deliveries)
      assert.lengthOf(delivered, 1)
    }).pipe(Effect.scoped))

  it.effect("retires a durable wake when the client acknowledges its pull", () =>
    Effect.gen(function*() {
      const deliveries = yield* Ref.make<ReadonlyArray<OfflineWake.Delivery>>([])
      const offlineWake = {
        recipients: () => Effect.succeed([readerId]),
        deliver: (wake: OfflineWake.Delivery) => Ref.update(deliveries, (current) => [...current, wake]),
        ...wakeTiming
      } satisfies OfflineWake.Options
      const server = yield* makeServer(offlineWake)

      const bootstrap = yield* pull(server)
      if (!("_tag" in bootstrap)) assert.fail("expected bootstrap metadata")
      const firstPage = yield* pull(server, bootstrap.manifest.cursor)
      let page = incremental(firstPage)
      const secondPage = yield* pull(server, page.cursor)
      page = incremental(secondPage)

      const receipt = yield* submit(server, 1)
      assert.strictEqual(receipt._tag, "Accepted")
      const mutationPage = yield* pull(server, page.cursor)
      page = incremental(mutationPage)
      assert.strictEqual(page.serverSequence, 1)
      yield* pull(server, page.cursor)

      yield* TestClock.adjust("10 seconds")
      yield* Effect.yieldNow
      const delivered = yield* Ref.get(deliveries)
      assert.lengthOf(delivered, 0)
    }).pipe(Effect.scoped))

  it.effect("does not call the delivery hook while the client has a live Watch", () =>
    Effect.gen(function*() {
      const deliveries = yield* Queue.unbounded<OfflineWake.Delivery>()
      const watchReady = yield* Deferred.make<void>()
      const offlineWake = {
        recipients: () => Effect.succeed([readerId]),
        deliver: (wake: OfflineWake.Delivery) => Queue.offer(deliveries, wake),
        ...wakeTiming
      } satisfies OfflineWake.Options
      const server = yield* makeServer(offlineWake)
      const request = watchRequest()
      const watcher = yield* server.watch(request).pipe(
        Stream.tap(() => Deferred.succeed(watchReady, undefined)),
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(watchReady)

      const receipt = yield* submit(server, 1)
      assert.strictEqual(receipt._tag, "Accepted")
      yield* TestClock.adjust("10 seconds")
      yield* Effect.yieldNow
      assert.strictEqual(yield* Queue.size(deliveries), 0)

      yield* Fiber.interrupt(watcher)
      yield* Effect.yieldNow
      const delivered = yield* Queue.take(deliveries)
      assert.strictEqual(delivered.clientId, readerId)
    }).pipe(Effect.scoped))

  it.effect("coordinates Watch presence across server runtimes sharing the database", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const filename = `${directory}/offline-wake.sqlite`
      const deliveries = yield* Queue.unbounded<OfflineWake.Delivery>()
      const watchReady = yield* Deferred.make<void>()
      const offlineWake = {
        recipients: () => Effect.succeed([readerId]),
        deliver: (wake: OfflineWake.Delivery) => Queue.offer(deliveries, wake),
        ...wakeTiming
      } satisfies OfflineWake.Options
      const persistentDatabase = () =>
        SqliteClient.layer({ filename, disableWAL: true }).pipe((sqlite) =>
          Layer.mergeAll(sqlite, NodeCrypto.layer, Reactivity.layer, QueryReactivity.layer)
        )
      const buildServer = () => {
        const databaseLayer = persistentDatabase()
        return makeServer(offlineWake, databaseLayer)
      }
      const connectedServer = yield* buildServer()
      const submittingServer = yield* buildServer()
      const request = watchRequest()
      const watcher = yield* connectedServer.watch(request).pipe(
        Stream.tap(() => Deferred.succeed(watchReady, undefined)),
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(watchReady)

      const receipt = yield* submit(submittingServer, 1)
      assert.strictEqual(receipt._tag, "Accepted")
      yield* TestClock.adjust("10 seconds")
      yield* Effect.yieldNow
      assert.strictEqual(yield* Queue.size(deliveries), 0)

      yield* Fiber.interrupt(watcher)
      yield* Effect.yieldNow
      const delivered = yield* Queue.take(deliveries)
      assert.strictEqual(delivered.clientId, readerId)
    }).pipe(Effect.provide(NodeFileSystem.layer), Effect.scoped))

  it.effect("registers Watch presence only after an in flight delivery releases its claim", () =>
    Effect.gen(function*() {
      const deliveryStarted = yield* Deferred.make<void>()
      const releaseDelivery = yield* Deferred.make<void>()
      const watchReady = yield* Deferred.make<void>()
      const offlineWake = {
        recipients: () => Effect.succeed([readerId]),
        deliver: () => {
          const released = Deferred.await(releaseDelivery)
          return Deferred.succeed(deliveryStarted, undefined).pipe(Effect.andThen(released))
        },
        ...wakeTiming
      } satisfies OfflineWake.Options
      const server = yield* makeServer(offlineWake)

      const receipt = yield* submit(server, 1)
      assert.strictEqual(receipt._tag, "Accepted")
      yield* TestClock.adjust("1 second")
      yield* Deferred.await(deliveryStarted)
      const request = watchRequest()
      const watcher = yield* server.watch(request).pipe(
        Stream.tap(() => Deferred.succeed(watchReady, undefined)),
        Stream.runDrain,
        Effect.forkChild({ startImmediately: true })
      )
      yield* Effect.yieldNow
      assert.isFalse(yield* Deferred.isDone(watchReady))

      yield* Deferred.succeed(releaseDelivery, undefined)
      yield* TestClock.adjust("1 second")
      yield* Deferred.await(watchReady)
      yield* Fiber.interrupt(watcher)
    }).pipe(Effect.scoped))
})
