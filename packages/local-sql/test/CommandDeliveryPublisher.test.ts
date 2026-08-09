import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlConnection from "effect/unstable/sql/SqlConnection"
import * as CommandDeliveryPublisher from "../src/CommandDeliveryPublisher.js"
import * as CommandDeliveryStore from "../src/CommandDeliveryStore.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import * as ReplicaOperationScheduler from "../src/ReplicaOperationScheduler.js"

describe("CommandDeliveryPublisher", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const definition = ReplicaDefinition.make({
    name: "command-delivery-publisher",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const limits: ReplicaLimits.Values = {
    maxBackupBytes: 1_000_000,
    maxChunkBytes: 64_000,
    maxArchiveRecords: 1_000,
    maxJsonDepth: 64,
    maxConflictDepth: 128,
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
    maxSyncOperationsPerMessage: 10_000,
    maxPendingBytesPerDocument: 1_000_000,
    maxPendingBytesPerPeer: 1_000_000,
    maxPendingBytesPerReplica: 1_000_000,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1_000,
    maxPendingChangesPerPeer: 1_000,
    maxPendingChangesPerReplica: 1_000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 10_000,
    maxSessions: 10,
    maxStreamsPerSession: 10,
    maxInFlightPerSession: 1,
    maxQueuedRpc: 100,
    maxQueuedPermits: 100,
    maxActiveRestores: 10,
    maxRestoresPerSession: 1,
    maxRestoreMillis: 30_000,
    maxRestorePullMillis: 10_000,
    maxRestoreCoalesceMillis: 25,
    maxRestoreErrorBytes: 4_096
  }
  const layer = (() => {
    const database = Layer.merge(
      SqliteClient.layer({ filename: ":memory:", disableWAL: true }),
      NodeCrypto.layer
    )
    const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provideMerge(database))
    const base = Layer.merge(bootstrap, ReplicaLimits.layer(limits))
    const gate = ReplicaGate.layer.pipe(Layer.provideMerge(base))
    const scheduler = ReplicaOperationScheduler.layer.pipe(Layer.provideMerge(base))
    const store = CommandDeliveryStore.layer.pipe(Layer.provideMerge(gate))
    const publisher = CommandDeliveryPublisher.layer(CommandDeliveryPublisher.defaultOptions).pipe(
      Layer.provideMerge(Layer.merge(store, scheduler))
    )
    return Layer.mergeAll(database, bootstrap, gate, scheduler, store, publisher)
  })()

  it.effect("delivers a full pending batch to a subscriber without evicting the oldest events", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const gate = yield* ReplicaGate.ReplicaGate
      const publisher = yield* CommandDeliveryPublisher.CommandDeliveryPublisher
      const permit = yield* gate.current
      const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
      const events = Array.from({ length: 512 }, () => ({
        replica_incarnation: permit.incarnation,
        command_id: null,
        document_id: documentId,
        published: 0
      }))
      for (let offset = 0; offset < events.length; offset += 100) {
        yield* sql`INSERT INTO effect_local_command_delivery_events ${sql.insert(events.slice(offset, offset + 100))}`
      }

      const first = yield* Effect.scoped(Effect.gen(function*() {
        const subscription = yield* publisher.subscribe
        yield* publisher.publishPending
        return yield* Stream.runHead(subscription.events)
      }))
      assert.strictEqual(Option.getOrNull(first)?.sequence, 1)
    }).pipe(Effect.provide(layer)))

  it.effect("drains a full event batch before waiting for the next poll", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const gate = yield* ReplicaGate.ReplicaGate
      const permit = yield* gate.current
      const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
      const events = Array.from({ length: 600 }, () => ({
        replica_incarnation: permit.incarnation,
        command_id: null,
        document_id: documentId,
        published: 0
      }))
      for (let offset = 0; offset < events.length; offset += 100) {
        yield* sql`INSERT INTO effect_local_command_delivery_events ${sql.insert(events.slice(offset, offset + 100))}`
      }

      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow
      assert.deepStrictEqual(
        yield* sql`SELECT COUNT(*) AS count
          FROM effect_local_command_delivery_events
          WHERE published = 0`,
        [{ count: 0 }]
      )
    }).pipe(Effect.provide(layer)))

  it.effect("retries polling after a typed storage failure", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const gate = yield* ReplicaGate.ReplicaGate
      const permit = yield* gate.current
      const documentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")

      yield* sql`INSERT INTO effect_local_command_delivery_events (
        replica_incarnation, command_id, document_id, published
      ) VALUES (${permit.incarnation}, NULL, ${documentId}, 0)`
      yield* sql`PRAGMA query_only = ON`
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow

      assert.deepStrictEqual(
        yield* sql`SELECT COUNT(*) AS count
          FROM effect_local_command_delivery_events
          WHERE published = 0`,
        [{ count: 1 }]
      )

      yield* sql`PRAGMA query_only = OFF`
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow

      assert.deepStrictEqual(
        yield* sql`SELECT COUNT(*) AS count
          FROM effect_local_command_delivery_events
          WHERE published = 0`,
        [{ count: 0 }]
      )
    }).pipe(Effect.provide(layer)))

  it.effect("stops polling when a transaction commit defects", () => {
    let failNextCommit = false
    let commitAttempts = 0
    const baseDatabase = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
    const instrumentedDatabase = Layer.effect(
      SqlClient.SqlClient,
      Effect.gen(function*() {
        const sql = yield* SqlClient.SqlClient
        return Object.assign(
          ((...args: ReadonlyArray<unknown>) => (sql as any)(...args)) as SqlClient.SqlClient,
          sql,
          {
            withTransaction: <R, E, A,>(effect: Effect.Effect<A, E, R>) =>
              Effect.scoped(Effect.gen(function*() {
                const transaction = yield* Effect.serviceOption(sql.transactionService)
                if (Option.isSome(transaction)) {
                  return yield* sql.withTransaction(effect)
                }
                const connection = yield* sql.reserve
                const instrumentedConnection: SqlConnection.Connection = {
                  ...connection,
                  executeUnprepared: (statement, params, transformRows) => {
                    const execution = connection.executeUnprepared(statement, params, transformRows)
                    if (statement !== "COMMIT") return execution
                    commitAttempts++
                    if (!failNextCommit) return execution
                    failNextCommit = false
                    return execution.pipe(
                      Effect.andThen(Effect.die(new Error("simulated commit defect")))
                    )
                  }
                }
                return yield* sql.withTransaction(effect).pipe(
                  Effect.provideService(sql.transactionService, [instrumentedConnection, -1])
                )
              }))
          }
        )
      })
    ).pipe(Layer.provideMerge(baseDatabase))
    const database = Layer.merge(instrumentedDatabase, NodeCrypto.layer)
    const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provideMerge(database))
    const base = Layer.merge(bootstrap, ReplicaLimits.layer(limits))
    const gate = ReplicaGate.layer.pipe(Layer.provideMerge(base))
    const scheduler = ReplicaOperationScheduler.layer.pipe(Layer.provideMerge(base))
    const store = CommandDeliveryStore.layer.pipe(Layer.provideMerge(gate))
    const publisher = CommandDeliveryPublisher.layer(CommandDeliveryPublisher.defaultOptions).pipe(
      Layer.provideMerge(Layer.merge(store, scheduler))
    )
    const testLayer = Layer.mergeAll(database, bootstrap, gate, scheduler, store, publisher)

    return Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const gate = yield* ReplicaGate.ReplicaGate
      const permit = yield* gate.current
      const firstDocumentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
      const secondDocumentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")
      const commitAttemptsBeforeDefect = commitAttempts

      failNextCommit = true
      yield* sql`INSERT INTO effect_local_command_delivery_events (
        replica_incarnation, command_id, document_id, published
      ) VALUES (${permit.incarnation}, NULL, ${firstDocumentId}, 0)`
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow

      yield* sql`INSERT INTO effect_local_command_delivery_events (
        replica_incarnation, command_id, document_id, published
      ) VALUES (${permit.incarnation}, NULL, ${secondDocumentId}, 0)`
      yield* TestClock.adjust("1 second")
      yield* Effect.yieldNow

      assert.deepStrictEqual(
        yield* sql`SELECT COUNT(*) AS count
          FROM effect_local_command_delivery_events
          WHERE published = 0`,
        [{ count: 1 }]
      )
      assert.strictEqual(commitAttempts - commitAttemptsBeforeDefect, 1)
    }).pipe(Effect.provide(testLayer))
  })
})
