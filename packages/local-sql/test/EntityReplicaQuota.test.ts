import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as CommandOutcome from "@lucas-barake/effect-local/CommandOutcome"
import type * as Conflict from "@lucas-barake/effect-local/Conflict"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import type * as Crypto from "effect/Crypto"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as DurableRuntime from "../src/DurableRuntime.js"
import * as EntityReplica from "../src/EntityReplica.js"
import * as SqlReplica from "../src/SqlReplica.js"

describe("EntityReplica in-flight command limit", () => {
  class DirectReplica extends Context.Service<DirectReplica, Replica.Replica["Service"]>()(
    "@lucas-barake/effect-local-sql/test/DirectReplica"
  ) {}

  const Task = Document.make("Task", { schema: Schema.Struct({ title: Schema.String }), version: 1 })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const limits: ReplicaLimits.Values = {
    maxBackupBytes: 1_000_000,
    maxChunkBytes: 64_000,
    maxArchiveRecords: 1_000,
    maxJsonDepth: 32,
    maxConflictDepth: 64,
    maxConflictNodes: 100_000,
    maxConflictAlternatives: 10_000,
    maxConflictPathSegments: 128,
    maxConflictValueBytes: 16 * 1024 * 1024,
    maxConflictSourceChanges: 100_000,
    maxConflictSourceOperations: 100_000,
    maxConflictSourceBytes: 64 * 1024 * 1024,
    maxSyncMessageBytes: 64_000,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1_000,
    maxSyncOperationsPerMessage: 1_000,
    maxPendingBytesPerDocument: 1_000_000,
    maxPendingBytesPerPeer: 1_000_000,
    maxPendingBytesPerReplica: 2_000_000,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1_000,
    maxPendingChangesPerPeer: 1_000,
    maxPendingChangesPerReplica: 2_000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 20_000,
    maxSessions: 8,
    maxStreamsPerSession: 4,
    maxInFlightPerSession: 16,
    maxQueuedRpc: 1,
    maxQueuedPermits: 1,
    maxActiveRestores: 1,
    maxRestoresPerSession: 16,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  }

  const database = Layer.merge(
    SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
    NodeCrypto.layer
  )

  const databaseWithTransactionProbe = (
    probe: { armed: boolean },
    onTransaction: Effect.Effect<void>
  ) => {
    const baseDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
    const instrumentedDatabase = Layer.effect(
      SqlClient.SqlClient,
      Effect.gen(function*() {
        const sql = yield* SqlClient.SqlClient
        const instrumentedSql: SqlClient.SqlClient = (...args: Array<any>) => sql(...args)
        return Object.assign(instrumentedSql, sql, {
          withTransaction: <R, E, A,>(effect: Effect.Effect<A, E, R>) =>
            Effect.serviceOption(sql.transactionService).pipe(
              Effect.flatMap((transaction) =>
                sql.withTransaction(effect).pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      if (!probe.armed || Option.isSome(transaction)) return false
                      probe.armed = false
                      return true
                    }).pipe(
                      Effect.flatMap((pause) => {
                        if (pause) return onTransaction
                        return Effect.void
                      })
                    )
                  )
                )
              )
            )
        })
      })
    ).pipe(Layer.provideMerge(baseDatabase))
    return Layer.merge(instrumentedDatabase, NodeCrypto.layer)
  }

  const productionLive = (
    databaseLayer: Layer.Layer<SqlClient.SqlClient | Crypto.Crypto>
  ) =>
    Layer.merge(
      SqlReplica.layerWithBindings(definition, { projections: [] }).pipe(
        Layer.provide(Layer.merge(databaseLayer, ReplicaLimits.layer(limits)))
      ),
      databaseLayer
    )

  const productionLiveWithDirectReplica = (
    databaseLayer: Layer.Layer<SqlClient.SqlClient | Crypto.Crypto>
  ) => {
    const services = SqlReplica.servicesLayerWithBindings(definition, { projections: [] })
    const direct = Layer.effect(DirectReplica, Replica.Replica).pipe(
      Layer.provide(SqlReplica.layerFromServices(definition).pipe(Layer.provideMerge(services)))
    )
    const durable = DurableRuntime.layer(definition).pipe(Layer.provideMerge(services))
    const entity = EntityReplica.layer(definition).pipe(Layer.provideMerge(durable))
    return Layer.merge(entity, direct).pipe(
      Layer.provideMerge(Layer.merge(databaseLayer, ReplicaLimits.layer(limits)))
    )
  }

  it.effect("rejects a concurrent distinct command beyond the in-flight limit", () =>
    Effect.gen(function*() {
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const probe = { armed: false }
      const probedDatabase = databaseWithTransactionProbe(
        probe,
        Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)))
      )
      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const sql = yield* SqlClient.SqlClient
        const firstId = yield* Identity.makeCommandId
        const secondId = yield* Identity.makeCommandId
        probe.armed = true
        const first = yield* replica.create(Task, { commandId: firstId, value: { title: "first" } }).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(started)
        const rejected = yield* Effect.flip(replica.create(Task, { commandId: secondId, value: { title: "second" } }))
        assert.strictEqual(rejected.reason._tag, "QuotaExceeded")
        // The gate also raises QuotaExceeded now, so pin the resource to keep this assertion unambiguous.
        if (rejected.reason._tag === "QuotaExceeded") {
          assert.strictEqual(rejected.reason.resource, "in-flight commands")
          assert.strictEqual(rejected.reason.limit, limits.maxQueuedRpc)
        }
        yield* Deferred.succeed(release, undefined)
        // Joining without a flip is the assertion: the admitted command commits rather than failing.
        const documentId = yield* Fiber.join(first)
        const rows = yield* sql<{
          readonly documents: number
          readonly receipts: number
        }>`SELECT
          (SELECT COUNT(*) FROM effect_local_documents
            WHERE document_id = ${documentId}) AS documents,
          (SELECT COUNT(*) FROM effect_local_command_receipts
            WHERE command_id IN (${firstId}, ${secondId})) AS receipts`
        assert.deepStrictEqual(rows[0], { documents: 1, receipts: 1 })
      }).pipe(Effect.provide(productionLive(probedDatabase)))
    }).pipe(TestClock.withLive))

  it.effect("does not publish a durably rejected resolution and replays the persisted result", () =>
    Effect.gen(function*() {
      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const sql = yield* SqlClient.SqlClient
        const documentId = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "one" }
        })
        const commandId = yield* Identity.makeCommandId
        const resolution: Conflict.Resolution = {
          heads: [],
          path: { parents: [], target: { _tag: "Key", key: "title" } },
          choice: { _tag: "DeleteValue" }
        }
        yield* sql`UPDATE effect_local_commit_outbox
          SET published = 0, invalidation_keys = 'invalid-json'
          WHERE commit_sequence = (
            SELECT MAX(commit_sequence) FROM effect_local_commit_outbox
          )`

        const first = yield* Effect.flip(replica.resolveConflict(Task, { commandId, documentId, resolution }))
        if (first._tag !== "StaleConflictResolution") {
          assert.fail(`Expected StaleConflictResolution, got ${first._tag}`)
        }
        const replayed = yield* Effect.flip(replica.resolveConflict(Task, { commandId, documentId, resolution }))
        if (replayed._tag !== "StaleConflictResolution") {
          assert.fail(`Expected StaleConflictResolution, got ${replayed._tag}`)
        }
        assert.deepStrictEqual(
          yield* replica.lookupConflictResolution(Task, { commandId, documentId, resolution }),
          CommandOutcome.rejected(commandId, first)
        )
        const rows = yield* sql<{
          readonly published: number
          readonly receipts: number
        }>`SELECT
          (SELECT published FROM effect_local_commit_outbox
            ORDER BY commit_sequence DESC LIMIT 1) AS published,
          (SELECT COUNT(*) FROM effect_local_command_receipts
            WHERE command_id = ${commandId}) AS receipts`
        assert.deepStrictEqual(rows[0], { published: 0, receipts: 1 })
      }).pipe(Effect.provide(productionLive(database)))
    }).pipe(TestClock.withLive))

  it.effect("shares an exact resolution retry without admitting a distinct command", () =>
    Effect.gen(function*() {
      const release = yield* Deferred.make<void>()
      const retryAdmitted = yield* Deferred.make<void>()
      const probe = { armed: false }
      const probedDatabase = databaseWithTransactionProbe(
        probe,
        Deferred.succeed(retryAdmitted, undefined).pipe(Effect.andThen(Deferred.await(release)))
      )
      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const direct = yield* DirectReplica
        const sql = yield* SqlClient.SqlClient
        const documentId = yield* direct.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "one" }
        })
        const commandId = yield* Identity.makeCommandId
        const resolution: Conflict.Resolution = {
          heads: [],
          path: { parents: [], target: { _tag: "Key", key: "title" } },
          choice: { _tag: "DeleteValue" }
        }
        const first = yield* Effect.flip(direct.resolveConflict(Task, { commandId, documentId, resolution }))
        if (first._tag !== "StaleConflictResolution") {
          assert.fail(`Expected StaleConflictResolution, got ${first._tag}`)
        }

        probe.armed = true
        const exact = yield* Effect.flip(
          replica.resolveConflict(Task, { commandId, documentId, resolution })
        ).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        yield* Deferred.await(retryAdmitted)
        const distinctCommandId = yield* Identity.makeCommandId
        const distinct = yield* Effect.flip(replica.resolveConflict(Task, {
          commandId: distinctCommandId,
          documentId,
          resolution
        }))
        if (!Schema.is(ReplicaError.ReplicaError)(distinct)) {
          assert.fail(`Expected ReplicaError, got ${distinct._tag}`)
        }
        assert.strictEqual(distinct.reason._tag, "QuotaExceeded")
        if (distinct.reason._tag === "QuotaExceeded") {
          assert.strictEqual(distinct.reason.resource, "in-flight commands")
          assert.strictEqual(distinct.reason.limit, limits.maxQueuedRpc)
        }
        yield* Deferred.succeed(release, undefined)
        const replayed = yield* Fiber.join(exact)
        if (replayed._tag !== "StaleConflictResolution") {
          assert.fail(`Expected StaleConflictResolution, got ${replayed._tag}`)
        }
        assert.deepStrictEqual(
          yield* replica.lookupConflictResolution(Task, { commandId, documentId, resolution }),
          CommandOutcome.rejected(commandId, first)
        )
        const rows = yield* sql<{ readonly receipts: number }>`
          SELECT COUNT(*) AS receipts
          FROM effect_local_command_receipts
          WHERE command_id IN (${commandId}, ${distinctCommandId})`
        assert.deepStrictEqual(rows[0], { receipts: 1 })
      }).pipe(Effect.provide(productionLiveWithDirectReplica(probedDatabase)))
    }).pipe(TestClock.withLive))
})
