import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Context from "effect/Context"
import * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Logger from "effect/Logger"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Scope from "effect/Scope"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as OfflineWakeRuntime from "../src/internal/offlineWake.js"
import * as Migrations from "../src/Migrations.js"
import type * as OfflineWake from "../src/OfflineWake.js"

const spaceId = Identity.SpaceId.make("spc_00000000-0000-4000-8000-000000000001")
const clientId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000002")
const sentinelId = Identity.ClientId.make("cli_00000000-0000-4000-8000-000000000003")
const provideNodeCrypto = Effect.provide(NodeCrypto.layer)

const options = {
  recipients: () => Effect.succeed([clientId]),
  coalescingWindow: "1 second",
  pollInterval: "1 second",
  retryDelay: "1 second",
  maximumRetryDelay: "1 second",
  claimLeaseDuration: "30 seconds",
  hookTimeout: "10 seconds",
  presenceLeaseDuration: "30 seconds",
  presenceHeartbeatInterval: "10 seconds",
  claimBatchSize: 4,
  maximumConcurrentRecipientResolutions: 1,
  maximumConcurrentDeliveries: 1,
  maximumRecipientsPerSpace: 4
} as const

const withSqlDefect = (
  actualSql: SqlClient.SqlClient,
  shouldFail: (source: string) => boolean
): { readonly sql: SqlClient.SqlClient; readonly injected: () => boolean } => {
  let pending = true
  const sql = new Proxy(actualSql, {
    apply: (target, thisArg, args: Parameters<SqlClient.SqlClient>) => {
      const rawSource: unknown = args[0]
      let source: string
      if (Array.isArray(rawSource)) source = rawSource.join("")
      else source = String(rawSource)
      if (pending && shouldFail(source)) {
        pending = false
        return Effect.die("injected SQL defect")
      }
      return Reflect.apply(target, thisArg, args)
    }
  })
  return { sql, injected: () => !pending }
}

const makeMigratedSql = Effect.fnUntraced(function*(owner: Scope.Scope) {
  const sql = yield* SqliteClient.make({ filename: ":memory:", disableWAL: true }).pipe(
    Effect.provide(Reactivity.layer),
    Scope.provide(owner)
  )
  yield* Migrations.server().pipe(Effect.provideService(SqlClient.SqlClient, sql))
  return sql
})

const makeService = Effect.fnUntraced(function*(
  sql: SqlClient.SqlClient,
  owner: Scope.Scope,
  deliveries: Queue.Queue<OfflineWake.Delivery>
) {
  const crypto = yield* Crypto.Crypto
  return yield* OfflineWakeRuntime.make({
    ...options,
    deliver: (wake) => Queue.offer(deliveries, wake).pipe(Effect.as("Delivered" as const))
  }, Context.empty()).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.provideService(Crypto.Crypto, crypto),
    Scope.provide(owner)
  )
})

describe("offline wake worker recovery", () => {
  it.effect(
    "does not log opaque hook defect details",
    Effect.fnUntraced(
      function*() {
        const owner = yield* Scope.make()
        const sql = yield* makeMigratedSql(owner)
        const logs = yield* Queue.unbounded<string>().pipe(
          (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
        )
        const cycleCompleted = yield* Deferred.make<void>()
        const logger = Logger.make<unknown, void>((event) => {
          Queue.offerUnsafe(logs, Logger.formatJson.log(event))
        })
        const crypto = yield* Crypto.Crypto
        const service = yield* OfflineWakeRuntime.make({
          ...options,
          recipients: () => Effect.succeed([clientId, sentinelId]),
          deliver: (wake) => {
            if (wake.clientId === sentinelId) {
              return Deferred.succeed(cycleCompleted, undefined).pipe(Effect.as("Delivered" as const))
            }
            return Effect.die("provider token must stay private")
          }
        }, Context.empty()).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.provide(Logger.layer([logger])),
          Scope.provide(owner)
        )

        yield* service.enqueue(spaceId, Identity.ServerSequence.make(1))
        yield* service.notify
        yield* TestClock.adjust("2 seconds")
        yield* Deferred.await(cycleCompleted)
        const captured = yield* Queue.takeAll(logs)
        assert.isTrue(captured.some((log) => log.includes("Offline wake delivery failed")))
        assert.isFalse(captured.some((log) => log.includes("provider token must stay private")))
        yield* Scope.close(owner, Exit.void)
      },
      provideNodeCrypto,
      Effect.scoped
    )
  )

  it.effect(
    "continues dispatch after a worker defect",
    Effect.fnUntraced(
      function*() {
        const owner = yield* Scope.make()
        const actualSql = yield* makeMigratedSql(owner)
        const fault = withSqlDefect(
          actualSql,
          (source) =>
            source.includes("UPDATE effect_local_server_offline_wake_spaces") && source.includes("WHERE rowid IN")
        )
        const deliveries = yield* Queue.bounded<OfflineWake.Delivery>(1).pipe(
          (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
        )
        const service = yield* makeService(fault.sql, owner, deliveries)

        yield* service.enqueue(spaceId, Identity.ServerSequence.make(1))
        yield* service.notify
        yield* TestClock.adjust("1 second")
        yield* service.notify
        yield* TestClock.adjust("2 seconds")
        const delivery = yield* Queue.take(deliveries)
        assert.isTrue(fault.injected())
        assert.strictEqual(delivery.clientId, clientId)
        yield* Scope.close(owner, Exit.void)
      },
      provideNodeCrypto,
      Effect.scoped
    )
  )

  it.effect(
    "keeps refreshing a live Watch lease after a heartbeat defect",
    Effect.fnUntraced(
      function*() {
        const owner = yield* Scope.make()
        const actualSql = yield* makeMigratedSql(owner)
        const fault = withSqlDefect(
          actualSql,
          (source) => source.includes("UPDATE effect_local_server_watch_runtimes SET expires_at")
        )
        const deliveries = yield* Queue.bounded<OfflineWake.Delivery>(1).pipe(
          (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
        )
        const service = yield* makeService(fault.sql, owner, deliveries)
        const watchScope = yield* Scope.make()
        yield* service.registerWatch(spaceId, clientId).pipe(Scope.provide(watchScope))

        yield* TestClock.adjust("10 seconds")
        yield* service.enqueue(spaceId, Identity.ServerSequence.make(1))
        yield* service.notify
        yield* TestClock.adjust("25 seconds")
        const rows = yield* actualSql<{ readonly count: number }>`SELECT COUNT(*) AS count
          FROM effect_local_server_watch_runtimes WHERE expires_at > 30_000`
        assert.deepStrictEqual(rows, [{ count: 1 }])
        assert.isTrue(fault.injected())
        assert.strictEqual(yield* Queue.size(deliveries), 0)
        yield* Scope.close(watchScope, Exit.void)
        yield* Scope.close(owner, Exit.void)
      },
      provideNodeCrypto,
      Effect.scoped
    )
  )

  it.effect(
    "reconciles durable presence after a Watch cleanup defect",
    Effect.fnUntraced(
      function*() {
        const owner = yield* Scope.make()
        const actualSql = yield* makeMigratedSql(owner)
        const fault = withSqlDefect(
          actualSql,
          (source) => source.includes("DELETE FROM effect_local_server_watch_presence")
        )
        const targetDeliveries = yield* Queue.bounded<OfflineWake.Delivery>(1).pipe(
          (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
        )
        const cycleCompleted = yield* Deferred.make<void>()
        const crypto = yield* Crypto.Crypto
        const service = yield* OfflineWakeRuntime.make({
          ...options,
          recipients: () => Effect.succeed([clientId, sentinelId]),
          deliver: (wake) => {
            if (wake.clientId === sentinelId) {
              return Deferred.succeed(cycleCompleted, undefined).pipe(Effect.as("Delivered" as const))
            }
            return Queue.offer(targetDeliveries, wake).pipe(Effect.as("Delivered" as const))
          }
        }, Context.empty()).pipe(
          Effect.provideService(SqlClient.SqlClient, fault.sql),
          Effect.provideService(Crypto.Crypto, crypto),
          Scope.provide(owner)
        )
        const watchScope = yield* Scope.make()
        yield* service.registerWatch(spaceId, clientId).pipe(Scope.provide(watchScope))
        const closed = yield* Scope.close(watchScope, Exit.void).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(closed))
        assert.isTrue(fault.injected())

        yield* service.enqueue(spaceId, Identity.ServerSequence.make(1))
        yield* service.notify
        yield* TestClock.adjust("2 seconds")
        yield* Deferred.await(cycleCompleted)
        assert.strictEqual(yield* Queue.size(targetDeliveries), 0)
        yield* TestClock.adjust("10 seconds")
        assert.strictEqual(yield* Queue.size(targetDeliveries), 1)
        yield* Scope.close(owner, Exit.void)
      },
      provideNodeCrypto,
      Effect.scoped
    )
  )

  it.effect(
    "preserves a newer wake when an older delivery reports revoked membership",
    Effect.fnUntraced(
      function*() {
        const crypto = yield* Crypto.Crypto
        const owner = yield* Scope.make()
        const sql = yield* makeMigratedSql(owner)
        const membership = yield* Ref.make(true)
        const firstStarted = yield* Deferred.make<void>()
        const decideFirst = yield* Deferred.make<void>()
        const firstDecided = yield* Deferred.make<void>()
        const finishFirst = yield* Deferred.make<void>()
        const laterDelivery = yield* Queue.bounded<OfflineWake.Delivery>(1).pipe(
          (acquire) => Effect.acquireRelease(acquire, Queue.shutdown)
        )
        const attempts = yield* Ref.make(0)
        const raceOptions = {
          ...options,
          hookTimeout: "20 seconds",
          recipients: Effect.fnUntraced(function*() {
            if (yield* Ref.get(membership)) return [clientId]
            return []
          }),
          deliver: Effect.fnUntraced(function*(wake: OfflineWake.Delivery) {
            const attempt = yield* Ref.updateAndGet(attempts, (value) => value + 1)
            if (attempt > 1) {
              yield* Queue.offer(laterDelivery, wake)
              return "Delivered" as const
            }
            yield* Deferred.succeed(firstStarted, undefined)
            yield* Deferred.await(decideFirst)
            const member = yield* Ref.get(membership)
            yield* Deferred.succeed(firstDecided, undefined)
            yield* Deferred.await(finishFirst)
            if (member) return "Delivered" as const
            return "NotRecipient" as const
          })
        } satisfies OfflineWake.Options
        const make = (scope: Scope.Scope) =>
          OfflineWakeRuntime.make(raceOptions, Context.empty()).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
            Effect.provideService(Crypto.Crypto, crypto),
            Scope.provide(scope)
          )
        const firstOwner = yield* Scope.make()
        const secondOwner = yield* Scope.make()
        const first = yield* make(firstOwner)
        const second = yield* make(secondOwner)

        yield* first.enqueue(spaceId, Identity.ServerSequence.make(1))
        yield* first.notify
        yield* TestClock.adjust("2 seconds")
        yield* Deferred.await(firstStarted)
        yield* Ref.set(membership, false)
        yield* Deferred.succeed(decideFirst, undefined)
        yield* Deferred.await(firstDecided)
        yield* Ref.set(membership, true)
        yield* second.enqueue(spaceId, Identity.ServerSequence.make(2))
        yield* second.notify
        yield* TestClock.adjust("2 seconds")
        const rows = yield* sql<{ readonly high_water_sequence: number }>`SELECT high_water_sequence
          FROM effect_local_server_offline_wakes
          WHERE space_id = ${spaceId} AND client_id = ${clientId}`
        assert.deepStrictEqual(rows, [{ high_water_sequence: 2 }])
        yield* Deferred.succeed(finishFirst, undefined)
        yield* TestClock.adjust("2 seconds")
        const delivery = yield* Queue.take(laterDelivery)
        assert.strictEqual(delivery.clientId, clientId)
        yield* Scope.close(firstOwner, Exit.void)
        yield* Scope.close(secondOwner, Exit.void)
        yield* Scope.close(owner, Exit.void)
      },
      provideNodeCrypto,
      Effect.scoped
    )
  )
})
