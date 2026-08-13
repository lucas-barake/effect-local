import { NodeCrypto, NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Protocol from "@lucas-barake/effect-local/Protocol"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import { pipe } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
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
const sentinelId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000003")
const membershipIncarnation = Identity.MembershipIncarnation.make(
  "inc_00000000-0000-4000-8000-000000000001"
)
const scope = Protocol.ReplicationScope.make({ models: [Domain.Todo.name] })

const database = (filename: string) =>
  SqliteClient.layer({ filename, disableWAL: true }).pipe((sqlite) =>
    Layer.mergeAll(sqlite, NodeCrypto.layer, Reactivity.layer, QueryReactivity.Layer)
  )
const Database = database(":memory:")
const Runtime = MutationRuntime.layer(Domain.definition).pipe(Layer.provide(Domain.Handlers))
const provideNodeFileSystem = Effect.provide(NodeFileSystem.layer)
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
  databaseLayer: typeof Database = Database
) => {
  const StoreLayer = ServerStore.layer({ ...serverOptions, offlineWake }).pipe(
    Layer.provide(Runtime),
    Layer.provide(databaseLayer)
  )
  return service(ServerStore.ServerStore, StoreLayer)
}

const makeServerInScope = (
  offlineWake: OfflineWake.Options,
  databaseLayer: typeof Database,
  owner: Scope.Scope
) => {
  const StoreLayer = ServerStore.layer({ ...serverOptions, offlineWake }).pipe(
    Layer.provide(Runtime),
    Layer.provide(databaseLayer)
  )
  return Layer.buildWithScope(StoreLayer, owner).pipe(Effect.map(Context.get(ServerStore.ServerStore)))
}

const envelope = Effect.fnUntraced(function*(sequence: number) {
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

const startWatch = (server: ServerStore.Service, ready: Deferred.Deferred<void>) =>
  server.watch(watchRequest()).pipe(
    Stream.tap(() => Deferred.succeed(ready, undefined)),
    Stream.runDrain,
    Effect.forkChild({ startImmediately: true })
  )

const makeInspectionSql = (filename: string) => SqliteClient.make({ filename }).pipe(Effect.provide(Reactivity.layer))

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
  it.effect(
    "delivers one durable content free wake for a disconnected member",
    Effect.fnUntraced(function*() {
      const deliveries = yield* Queue.bounded<OfflineWake.Delivery>(2).pipe(
        (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
      )
      const offlineWake = {
        recipients: () => Effect.succeed([readerId]),
        deliver: (wake: OfflineWake.Delivery) => Queue.offer(deliveries, wake).pipe(Effect.as("Delivered" as const)),
        ...wakeTiming
      } satisfies OfflineWake.Options
      const server = yield* makeServer(offlineWake)

      const submitted = yield* envelope(1).pipe(Effect.provide(NodeCrypto.layer))
      const receipt = yield* server.submit(submitted)
      assert.strictEqual(receipt._tag, "Accepted")
      yield* TestClock.adjust("2 seconds")

      const delivered = yield* Queue.take(deliveries)
      assert.strictEqual(delivered.spaceId, spaceId)
      assert.strictEqual(delivered.clientId, readerId)
      const deliveryKeys = Object.keys(delivered)
      const sortedDeliveryKeys = deliveryKeys.toSorted()
      assert.deepStrictEqual(sortedDeliveryKeys, ["clientId", "spaceId", "wakeId"])
      const isWakeId = Schema.is(Identity.WakeId)
      const wakeId = delivered.wakeId
      const wakeIdValid = isWakeId(wakeId)
      assert.isTrue(wakeIdValid)
    }, Effect.scoped)
  )

  it.effect(
    "recovers a durable wake after the accepting server runtime closes",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/offline-wake-restart.sqlite`
        const delivery = yield* Deferred.make<OfflineWake.Delivery>()
        const offlineWake = {
          recipients: () => Effect.succeed([readerId]),
          deliver: (wake: OfflineWake.Delivery) =>
            Deferred.succeed(delivery, wake).pipe(Effect.as("Delivered" as const)),
          ...wakeTiming,
          coalescingWindow: "5 seconds"
        } satisfies OfflineWake.Options
        const acceptingScope = yield* Scope.make()
        const AcceptingDatabase = database(filename)
        const acceptingServer = yield* makeServerInScope(offlineWake, AcceptingDatabase, acceptingScope)
        const receipt = yield* submit(acceptingServer, 1)
        assert.strictEqual(receipt._tag, "Accepted")
        yield* Scope.close(acceptingScope, Exit.void)

        const recoveringScope = yield* Scope.make()
        const RecoveringDatabase = database(filename)
        yield* makeServerInScope(offlineWake, RecoveringDatabase, recoveringScope)
        yield* TestClock.adjust("5 seconds")
        const recovered = yield* Deferred.await(delivery)
        assert.strictEqual(recovered.clientId, readerId)
        yield* Scope.close(recoveringScope, Exit.void)
      },
      provideNodeFileSystem,
      Effect.scoped
    )
  )

  it.effect(
    "accepts submillisecond timing without stranding durable rows",
    Effect.fnUntraced(function*() {
      const delivery = yield* Deferred.make<OfflineWake.Delivery>()
      const offlineWake = {
        recipients: () => Effect.succeed([readerId]),
        deliver: (wake: OfflineWake.Delivery) => Deferred.succeed(delivery, wake).pipe(Effect.as("Delivered" as const)),
        ...wakeTiming,
        coalescingWindow: "500 micros",
        pollInterval: "500 micros"
      } satisfies OfflineWake.Options
      const server = yield* makeServer(offlineWake)
      const receipt = yield* submit(server, 1)
      assert.strictEqual(receipt._tag, "Accepted")
      yield* TestClock.adjust("1 millis")
      const delivered = yield* Deferred.await(delivery)
      assert.strictEqual(delivered.clientId, readerId)
    }, Effect.scoped)
  )

  it.effect(
    "redelivers one stable wake identity after the delivery hook fails",
    Effect.fnUntraced(function*() {
      const attempts = yield* Queue.bounded<OfflineWake.Delivery>(2).pipe(
        (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
      )
      const attemptCount = yield* Ref.make(0)
      const offlineWake = {
        recipients: () => Effect.succeed([readerId]),
        deliver: Effect.fnUntraced(function*(wake: OfflineWake.Delivery) {
          yield* Queue.offer(attempts, wake)
          const attempt = yield* Ref.updateAndGet(attemptCount, (value) => value + 1)
          if (attempt === 1) return yield* new TestWakeError({ reason: "provider unavailable" })
          return "Delivered" as const
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
    }, Effect.scoped)
  )

  it.effect(
    "coalesces repeated accepted mutations behind one high water fence",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/offline-wake-coalescing.sqlite`
        const deliveries = yield* Queue.bounded<OfflineWake.Delivery>(2).pipe(
          (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
        )
        const cycleCompleted = yield* Deferred.make<void>()
        const offlineWake = {
          recipients: () => Effect.succeed([readerId, sentinelId]),
          deliver: (wake: OfflineWake.Delivery) => {
            if (wake.clientId === sentinelId) {
              return Deferred.succeed(cycleCompleted, undefined).pipe(Effect.as("Delivered" as const))
            }
            return Queue.offer(deliveries, wake).pipe(Effect.as("Delivered" as const))
          },
          ...wakeTiming,
          maximumConcurrentDeliveries: 1
        } satisfies OfflineWake.Options
        const server = yield* makeServer(offlineWake, database(filename))

        for (let sequence = 1; sequence <= 3; sequence++) {
          const receipt = yield* submit(server, sequence)
          assert.strictEqual(receipt._tag, "Accepted")
        }
        yield* TestClock.adjust("2 seconds")
        yield* Queue.take(deliveries)
        yield* Deferred.await(cycleCompleted)
        const inspectionSql = yield* makeInspectionSql(filename)
        const fences = yield* inspectionSql<{
          readonly high_water_sequence: number
          readonly notified_sequence: number
        }>`SELECT high_water_sequence, notified_sequence
        FROM effect_local_server_offline_wakes
        WHERE space_id = ${spaceId} AND client_id = ${readerId}`
        assert.deepStrictEqual(fences, [{ high_water_sequence: 3, notified_sequence: 3 }])
        assert.strictEqual(yield* Queue.size(deliveries), 0)
      },
      provideNodeFileSystem,
      Effect.scoped
    )
  )

  it.effect(
    "retires a durable wake when the client acknowledges its pull",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/offline-wake-ack.sqlite`
        const cycleCompleted = yield* Deferred.make<void>()
        const watchReady = yield* Deferred.make<void>()
        const offlineWake = {
          recipients: () => Effect.succeed([readerId, sentinelId]),
          deliver: (wake: OfflineWake.Delivery) => {
            if (wake.clientId !== sentinelId) return Effect.die("connected reader wake reached delivery")
            return Deferred.succeed(cycleCompleted, undefined).pipe(Effect.as("Delivered" as const))
          },
          ...wakeTiming,
          maximumConcurrentDeliveries: 1
        } satisfies OfflineWake.Options
        const server = yield* makeServer(offlineWake, database(filename))

        const bootstrap = yield* pull(server)
        if (!("_tag" in bootstrap)) assert.fail("expected bootstrap metadata")
        const firstPage = yield* pull(server, bootstrap.manifest.cursor)
        let page = incremental(firstPage)
        const secondPage = yield* pull(server, page.cursor)
        page = incremental(secondPage)
        const watcher = yield* startWatch(server, watchReady)
        yield* Deferred.await(watchReady)

        const receipt = yield* submit(server, 1)
        assert.strictEqual(receipt._tag, "Accepted")
        yield* TestClock.adjust("2 seconds")
        yield* Deferred.await(cycleCompleted)
        const inspectionSql = yield* makeInspectionSql(filename)
        const pending = yield* inspectionSql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_server_offline_wakes
        WHERE space_id = ${spaceId} AND client_id = ${readerId}`
        assert.deepStrictEqual(pending, [{ count: 1 }])
        const mutationPage = yield* pull(server, page.cursor)
        page = incremental(mutationPage)
        assert.strictEqual(page.serverSequence, 1)
        yield* pull(server, page.cursor)

        const rows = yield* inspectionSql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_server_offline_wakes
        WHERE space_id = ${spaceId} AND client_id = ${readerId}`
        assert.deepStrictEqual(rows, [{ count: 0 }])
        yield* Fiber.interrupt(watcher)
      },
      provideNodeFileSystem,
      Effect.scoped
    )
  )

  it.effect(
    "does not call the delivery hook while the client has a live Watch",
    Effect.fnUntraced(function*() {
      const deliveries = yield* Queue.bounded<OfflineWake.Delivery>(1).pipe(
        (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
      )
      const watchReady = yield* Deferred.make<void>()
      const cycleCompleted = yield* Deferred.make<void>()
      const offlineWake = {
        recipients: () => Effect.succeed([readerId, sentinelId]),
        deliver: (wake: OfflineWake.Delivery) => {
          if (wake.clientId === sentinelId) {
            return Deferred.succeed(cycleCompleted, undefined).pipe(Effect.as("Delivered" as const))
          }
          return Queue.offer(deliveries, wake).pipe(Effect.as("Delivered" as const))
        },
        ...wakeTiming,
        maximumConcurrentDeliveries: 1
      } satisfies OfflineWake.Options
      const server = yield* makeServer(offlineWake)
      const watcher = yield* startWatch(server, watchReady)
      yield* Deferred.await(watchReady)

      const receipt = yield* submit(server, 1)
      assert.strictEqual(receipt._tag, "Accepted")
      yield* TestClock.adjust("10 seconds")
      yield* Deferred.await(cycleCompleted)
      assert.strictEqual(yield* Queue.size(deliveries), 0)

      yield* Fiber.interrupt(watcher)
      yield* TestClock.adjust("1 second")
      const delivered = yield* Queue.take(deliveries)
      assert.strictEqual(delivered.clientId, readerId)
    }, Effect.scoped)
  )

  it.effect(
    "coordinates Watch presence across server runtimes sharing the database",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/offline-wake.sqlite`
        const deliveries = yield* Queue.bounded<OfflineWake.Delivery>(1).pipe(
          (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
        )
        const watchReady = yield* Deferred.make<void>()
        const cycleCompleted = yield* Deferred.make<void>()
        const offlineWake = {
          recipients: () => Effect.succeed([readerId, sentinelId]),
          deliver: (wake: OfflineWake.Delivery) => {
            if (wake.clientId === sentinelId) {
              return Deferred.succeed(cycleCompleted, undefined).pipe(Effect.as("Delivered" as const))
            }
            return Queue.offer(deliveries, wake).pipe(Effect.as("Delivered" as const))
          },
          ...wakeTiming,
          maximumConcurrentDeliveries: 1
        } satisfies OfflineWake.Options
        const buildServer = () => makeServer(offlineWake, database(filename))
        const connectedServer = yield* buildServer()
        const submittingServer = yield* buildServer()
        const watcher = yield* startWatch(connectedServer, watchReady)
        yield* Deferred.await(watchReady)

        const receipt = yield* submit(submittingServer, 1)
        assert.strictEqual(receipt._tag, "Accepted")
        yield* TestClock.adjust("10 seconds")
        yield* Deferred.await(cycleCompleted)
        assert.strictEqual(yield* Queue.size(deliveries), 0)

        yield* Fiber.interrupt(watcher)
        yield* TestClock.adjust("1 second")
        const delivered = yield* Queue.take(deliveries)
        assert.strictEqual(delivered.clientId, readerId)
      },
      provideNodeFileSystem,
      Effect.scoped
    )
  )

  it.effect(
    "restores a live Watch presence row after lease cleanup",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/offline-wake-presence.sqlite`
        const deliveries = yield* Queue.bounded<OfflineWake.Delivery>(1).pipe(
          (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
        )
        const watchReady = yield* Deferred.make<void>()
        const resolved = yield* Deferred.make<void>()
        const cycleCompleted = yield* Deferred.make<void>()
        const offlineWake = {
          recipients: () => Deferred.succeed(resolved, undefined).pipe(Effect.as([readerId, sentinelId])),
          deliver: (wake: OfflineWake.Delivery) => {
            if (wake.clientId === sentinelId) {
              return Deferred.succeed(cycleCompleted, undefined).pipe(Effect.as("Delivered" as const))
            }
            return Queue.offer(deliveries, wake).pipe(Effect.as("Delivered" as const))
          },
          ...wakeTiming,
          maximumConcurrentDeliveries: 1
        } satisfies OfflineWake.Options
        const server = yield* makeServer(offlineWake, database(filename))
        const watcher = yield* startWatch(server, watchReady)
        yield* Deferred.await(watchReady)
        const inspectionSql = yield* makeInspectionSql(filename)
        yield* inspectionSql`DELETE FROM effect_local_server_watch_runtimes`

        yield* TestClock.adjust(wakeTiming.presenceHeartbeatInterval)
        const presence = yield* inspectionSql<{ readonly count: number }>`SELECT COUNT(*) AS count
        FROM effect_local_server_watch_presence
        WHERE space_id = ${spaceId} AND client_id = ${readerId}`
        assert.deepStrictEqual(presence, [{ count: 1 }])
        const receipt = yield* submit(server, 1)
        assert.strictEqual(receipt._tag, "Accepted")
        yield* TestClock.adjust("2 seconds")
        yield* Deferred.await(resolved)
        yield* Deferred.await(cycleCompleted)
        assert.strictEqual(yield* Queue.size(deliveries), 0)
        yield* Fiber.interrupt(watcher)
      },
      provideNodeFileSystem,
      Effect.scoped
    )
  )

  it.effect(
    "registers a new Watch after runtime lease cleanup",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/offline-wake-new-watch-after-cleanup.sqlite`
        const offlineWake = {
          recipients: () => Effect.succeed([readerId]),
          deliver: () => Effect.succeed("Delivered" as const),
          ...wakeTiming
        } satisfies OfflineWake.Options
        const server = yield* makeServer(offlineWake, database(filename))
        const inspectionSql = yield* makeInspectionSql(filename)
        yield* inspectionSql`DELETE FROM effect_local_server_watch_runtimes`
        const watchReady = yield* Deferred.make<void>()

        const watcher = yield* startWatch(server, watchReady)
        yield* Deferred.await(watchReady)

        const presence = yield* inspectionSql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_server_watch_presence
          WHERE space_id = ${spaceId} AND client_id = ${readerId}`
        assert.deepStrictEqual(presence, [{ count: 1 }])
        yield* Fiber.interrupt(watcher)
      },
      provideNodeFileSystem,
      Effect.scoped
    )
  )

  it.effect(
    "reconciles durable presence after transient Watch cleanup failure",
    Effect.fnUntraced(
      function*() {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped()
        const filename = `${directory}/offline-wake-cleanup-recovery.sqlite`
        const delivered = yield* Deferred.make<OfflineWake.Delivery>()
        const offlineWake = {
          recipients: () => Effect.succeed([readerId]),
          deliver: (wake: OfflineWake.Delivery) =>
            Deferred.succeed(delivered, wake).pipe(Effect.as("Delivered" as const)),
          ...wakeTiming
        } satisfies OfflineWake.Options
        const server = yield* makeServer(offlineWake, database(filename))
        const inspectionSql = yield* makeInspectionSql(filename)
        const watcherScope = yield* Scope.make()
        yield* server.watch(watchRequest()).pipe(Stream.take(1), Stream.runDrain, Scope.provide(watcherScope))
        yield* inspectionSql.unsafe(`CREATE TRIGGER fail_presence_delete
          BEFORE DELETE ON effect_local_server_watch_presence
          BEGIN SELECT RAISE(ABORT, 'transient cleanup failure'); END`)
        yield* Scope.close(watcherScope, Exit.void)
        yield* inspectionSql.unsafe("DROP TRIGGER fail_presence_delete")

        const receipt = yield* submit(server, 1)
        assert.strictEqual(receipt._tag, "Accepted")
        yield* TestClock.adjust("10 seconds")
        const wake = yield* Deferred.await(delivered)
        assert.strictEqual(wake.clientId, readerId)
      },
      provideNodeFileSystem,
      Effect.scoped
    )
  )

  it.effect(
    "does not retry delivery after recipient membership is revoked",
    Effect.fnUntraced(function*() {
      const member = yield* Ref.make(true)
      const attempts = yield* Queue.bounded<OfflineWake.Delivery>(2).pipe(
        (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
      )
      const attemptCount = yield* Ref.make(0)
      const membershipDecided = yield* Deferred.make<void>()
      const retryCompleted = yield* Deferred.make<void>()
      const sentinelAttempts = yield* Ref.make(0)
      const offlineWake = {
        ...wakeTiming,
        recipients: () => Effect.succeed([readerId, sentinelId]),
        deliver: Effect.fnUntraced(function*(wake: OfflineWake.Delivery) {
          if (wake.clientId === sentinelId) {
            const attempt = yield* Ref.updateAndGet(sentinelAttempts, (value) => value + 1)
            if (attempt === 1) return yield* new TestWakeError({ reason: "sentinel retry" })
            yield* Deferred.succeed(retryCompleted, undefined)
            return "Delivered" as const
          }
          yield* Ref.update(attemptCount, (value) => value + 1)
          if (!(yield* Ref.get(member))) {
            yield* Deferred.succeed(membershipDecided, undefined)
            return "NotRecipient" as const
          }
          yield* Queue.offer(attempts, wake)
          return yield* new TestWakeError({ reason: "provider unavailable" })
        }),
        maximumConcurrentDeliveries: 1
      } satisfies OfflineWake.Options
      const server = yield* makeServer(offlineWake)
      const receipt = yield* submit(server, 1)
      assert.strictEqual(receipt._tag, "Accepted")
      yield* TestClock.adjust("1 second")
      yield* Queue.take(attempts)

      yield* Ref.set(member, false)
      yield* TestClock.adjust("1 second")
      yield* Deferred.await(membershipDecided)
      yield* Deferred.await(retryCompleted)
      assert.strictEqual(yield* Queue.size(attempts), 0)
      assert.strictEqual(yield* Ref.get(attemptCount), 2)
    }, Effect.scoped)
  )
})
