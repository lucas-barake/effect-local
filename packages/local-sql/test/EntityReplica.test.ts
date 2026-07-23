import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Latch from "effect/Latch"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as CommitPublisher from "../src/CommitPublisher.js"
import * as SqlReplica from "../src/SqlReplica.js"

describe("EntityReplica", () => {
  const Task = Document.make("Task", {
    schema: Schema.Struct({ title: Schema.String }),
    version: 1
  })
  const definition = ReplicaDefinition.make({
    name: "entity-replica",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const limits: ReplicaLimits.Values = {
    maxBackupBytes: 1024 * 1024,
    maxChunkBytes: 64 * 1024,
    maxArchiveRecords: 1000,
    maxJsonDepth: 32,
    maxSyncMessageBytes: 64 * 1024,
    maxPeerSendMillis: 1_000,
    maxSyncChangesPerMessage: 100,
    maxSyncDependencyEdgesPerMessage: 1000,
    maxSyncOperationsPerMessage: 10_000,
    maxPendingBytesPerDocument: 1024 * 1024,
    maxPendingBytesPerPeer: 1024 * 1024,
    maxPendingBytesPerReplica: 1024 * 1024,
    maxPendingAgeMillis: 60_000,
    maxPendingChangesPerDocument: 1000,
    maxPendingChangesPerPeer: 1000,
    maxPendingChangesPerReplica: 1000,
    maxPendingDependencyEdgesPerDocument: 10_000,
    maxPendingDependencyEdgesPerPeer: 10_000,
    maxPendingDependencyEdgesPerReplica: 10_000,
    maxSessions: 8,
    maxStreamsPerSession: 8,
    maxInFlightPerSession: 32,
    maxQueuedRpc: 128
  }

  it.effect("invalidates subscribers when interruption arrives after a restore commits", () =>
    Effect.gen(function*() {
      const committed = yield* Deferred.make<void>()
      const release = yield* Latch.make()
      let armed = false
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
                Effect.serviceOption(sql.transactionService).pipe(
                  Effect.flatMap((transaction) =>
                    sql.withTransaction(effect).pipe(
                      Effect.tap(() =>
                        armed && Option.isNone(transaction)
                          ? Deferred.succeed(committed, undefined).pipe(Effect.andThen(release.await))
                          : Effect.void
                      )
                    )
                  )
                )
            }
          )
        })
      ).pipe(Layer.provideMerge(baseDatabase))
      const database = Layer.merge(instrumentedDatabase, NodeCrypto.layer)
      const live = SqlReplica.layerWithBindings(definition, { projections: [] }).pipe(
        Layer.provide(Layer.merge(database, ReplicaLimits.layer(limits)))
      )

      yield* Effect.gen(function*() {
        const replica = yield* Replica.Replica
        const publisher = yield* CommitPublisher.CommitPublisher
        const initial = yield* publisher.subscribe
        assert.strictEqual(initial.refreshGeneration, 0)
        const first = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "first" }
        })
        assert.strictEqual(first._tag, "DurablyCommittedLocal")
        if (first._tag !== "DurablyCommittedLocal") return
        const backup = yield* replica.exportBackup({ maxBytes: limits.maxBackupBytes }).pipe(Stream.runCollect)
        const second = yield* replica.create(Task, {
          commandId: yield* Identity.makeCommandId,
          value: { title: "second" }
        })
        assert.strictEqual(second._tag, "DurablyCommittedLocal")
        if (second._tag !== "DurablyCommittedLocal") return

        armed = true
        const restore = yield* replica.restoreBackup({
          expectedDefinitionHash: definition.hash,
          maxBytes: limits.maxBackupBytes,
          mode: "replace",
          source: Stream.fromIterable(backup)
        }).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Deferred.await(committed)
        const interrupt = yield* Fiber.interrupt(restore).pipe(Effect.forkChild({ startImmediately: true }))
        yield* Effect.yieldNow
        yield* release.open
        yield* Fiber.join(interrupt)

        assert.strictEqual((yield* replica.get(Task, first.value)).value.title, "first")
        assert.strictEqual((yield* Effect.exit(replica.get(Task, second.value)))._tag, "Failure")
        assert.strictEqual((yield* publisher.subscribe).refreshGeneration, 1)
      }).pipe(Effect.scoped, Effect.provide(live), Effect.provide(database), TestClock.withLive)
    }))
})
