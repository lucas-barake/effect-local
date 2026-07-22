import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"

describe("ReplicaGate", () => {
  const Task = Document.make("Task", { schema: Schema.String, version: 1 })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })
  const Database = Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)
  const Bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(Database))
  const Dependencies = Layer.merge(Database, Bootstrap)
  const Gate = Layer.merge(Dependencies, ReplicaGate.layer.pipe(Layer.provide(Dependencies)))

  it.effect("blocks exclusive restore while shared work is active", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const initial = yield* gate.current
      const sharedAcquired = yield* Deferred.make<void>()
      const releaseShared = yield* Deferred.make<void>()
      const exclusiveAcquired = yield* Deferred.make<ReplicaGate.Permit>()
      const shared = yield* Effect.forkChild(Effect.scoped(Effect.gen(function*() {
        yield* gate.shared
        yield* Deferred.succeed(sharedAcquired, undefined)
        yield* Deferred.await(releaseShared)
      })))
      yield* Deferred.await(sharedAcquired)
      const exclusive = yield* Effect.forkChild(gate.claim((permit) => Deferred.succeed(exclusiveAcquired, permit)))
      yield* Effect.yieldNow
      assert.isTrue(Option.isNone(yield* Deferred.poll(exclusiveAcquired)))
      yield* Deferred.succeed(releaseShared, undefined)
      const permit = yield* Deferred.await(exclusiveAcquired)
      assert.strictEqual(permit.incarnation, initial.incarnation + 1)
      assert.strictEqual(permit.writerGeneration, initial.writerGeneration + 1)
      yield* Fiber.join(shared)
      yield* Fiber.join(exclusive)
      assert.deepStrictEqual(yield* gate.current, permit)
      assert.strictEqual((yield* Effect.exit(gate.validate(initial)))._tag, "Failure")
      yield* gate.validate(permit)
    }).pipe(Effect.provide(Gate)))

  it.effect("releases exclusive permits when epoch advancement fails", () =>
    Effect.scoped(Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      yield* sql`CREATE TRIGGER fail_epoch_update
        BEFORE UPDATE OF replica_incarnation ON effect_local_metadata
        BEGIN SELECT RAISE(ABORT, 'epoch update failed'); END`
      const result = yield* Effect.exit(gate.claim(() => Effect.void))
      assert.strictEqual(result._tag, "Failure")
      yield* sql`DROP TRIGGER fail_epoch_update`
      assert.strictEqual((yield* gate.shared).replicaId, (yield* gate.current).replicaId)
    })).pipe(Effect.provide(Gate)))

  it.effect("publishes a claimed epoch after the caller commits", () =>
    Effect.scoped(Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const claimed = yield* gate.claim(Effect.succeed)
      const current = yield* gate.current
      const rows = yield* sql<{
        readonly replica_incarnation: number
        readonly writer_generation: number
      }>`SELECT replica_incarnation, writer_generation FROM effect_local_metadata WHERE singleton = 1`
      assert.deepStrictEqual(rows, [{
        replica_incarnation: current.incarnation,
        writer_generation: current.writerGeneration
      }])
      assert.strictEqual(current.incarnation, 1)
      assert.strictEqual(current.writerGeneration, 2)
      assert.deepStrictEqual(current, claimed)
      assert.strictEqual((yield* gate.shared).writerGeneration, 2)
    })).pipe(Effect.provide(Gate)))

  it.effect("rolls back a claimed epoch when the caller transaction fails", () =>
    Effect.scoped(Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const initial = yield* gate.current
      const result = yield* Effect.exit(gate.claim(() =>
        Effect.gen(function*() {
          yield* sql`INSERT INTO effect_local_metadata (
          singleton, storage_format_version, replica_id, replica_incarnation,
          writer_generation, definition_hash, commit_sequence
        ) SELECT singleton, storage_format_version, replica_id, replica_incarnation,
          writer_generation, definition_hash, commit_sequence
          FROM effect_local_metadata WHERE singleton = 1`
        })
      ))
      assert.strictEqual(result._tag, "Failure")
      const current = yield* gate.current
      assert.strictEqual(current.replicaId, initial.replicaId)
      assert.strictEqual(current.incarnation, initial.incarnation)
      assert.strictEqual(current.writerGeneration, initial.writerGeneration)
    })).pipe(Effect.provide(Gate)))

  it.effect("reports corrupt replica metadata through the typed error channel", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      yield* sql`UPDATE effect_local_metadata SET replica_id = 'invalid' WHERE singleton = 1`
      const result = yield* Effect.result(gate.refresh)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure.reason._tag, "StorageCorrupt")
    }).pipe(Effect.provide(Gate)))
})
