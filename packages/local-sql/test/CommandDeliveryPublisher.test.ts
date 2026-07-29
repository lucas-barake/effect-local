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
import * as Schema from "effect/Schema"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as CommandDeliveryPublisher from "../src/CommandDeliveryPublisher.js"
import * as CommandDeliveryStore from "../src/CommandDeliveryStore.js"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

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
    const store = CommandDeliveryStore.layer.pipe(Layer.provideMerge(gate))
    const publisher = CommandDeliveryPublisher.layer(CommandDeliveryPublisher.defaultOptions).pipe(
      Layer.provideMerge(store)
    )
    return Layer.mergeAll(database, bootstrap, gate, store, publisher)
  })()

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

  it.effect("continues polling after a transaction commit defect", () => {
    let failNextCommit = false
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
              sql.withTransaction(effect).pipe(
                Effect.tap(() =>
                  Effect.suspend(() => {
                    if (!failNextCommit) return Effect.void
                    failNextCommit = false
                    return Effect.die(new Error("simulated commit defect"))
                  })
                )
              )
          }
        )
      })
    ).pipe(Layer.provideMerge(baseDatabase))
    const database = Layer.merge(instrumentedDatabase, NodeCrypto.layer)
    const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provideMerge(database))
    const base = Layer.merge(bootstrap, ReplicaLimits.layer(limits))
    const gate = ReplicaGate.layer.pipe(Layer.provideMerge(base))
    const store = CommandDeliveryStore.layer.pipe(Layer.provideMerge(gate))
    const publisher = CommandDeliveryPublisher.layer(CommandDeliveryPublisher.defaultOptions).pipe(
      Layer.provideMerge(store)
    )
    const testLayer = Layer.mergeAll(database, bootstrap, gate, store, publisher)

    return Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const gate = yield* ReplicaGate.ReplicaGate
      const permit = yield* gate.current
      const firstDocumentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000001")
      const secondDocumentId = Identity.DocumentId.make("doc_00000000-0000-4000-8000-000000000002")

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
        [{ count: 0 }]
      )
    }).pipe(Effect.provide(testLayer))
  })
})
