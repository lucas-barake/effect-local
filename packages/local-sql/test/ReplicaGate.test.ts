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

  it.effect("does not admit new shared work ahead of a waiting restore", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const firstAcquired = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const restoreAcquired = yield* Deferred.make<void>()
      const releaseRestore = yield* Deferred.make<void>()
      const lateReaderAcquired = yield* Deferred.make<void>()
      const releaseLateReader = yield* Deferred.make<void>()
      const first = yield* Effect.forkChild(Effect.scoped(Effect.gen(function*() {
        yield* gate.shared
        yield* Deferred.succeed(firstAcquired, undefined)
        yield* Deferred.await(releaseFirst)
      })))
      yield* Deferred.await(firstAcquired)
      const restore = yield* Effect.forkChild(gate.claim(() =>
        Deferred.succeed(restoreAcquired, undefined).pipe(
          Effect.andThen(Deferred.await(releaseRestore))
        )
      ))
      yield* Effect.yieldNow
      const lateReader = yield* Effect.forkChild(Effect.scoped(Effect.gen(function*() {
        yield* gate.shared
        yield* Deferred.succeed(lateReaderAcquired, undefined)
        yield* Deferred.await(releaseLateReader)
      })))
      yield* Effect.yieldNow
      assert.isTrue(Option.isNone(yield* Deferred.poll(lateReaderAcquired)))
      yield* Deferred.succeed(releaseFirst, undefined)
      yield* Deferred.await(restoreAcquired)
      assert.isTrue(Option.isNone(yield* Deferred.poll(lateReaderAcquired)))
      yield* Deferred.succeed(releaseRestore, undefined)
      yield* Deferred.await(lateReaderAcquired)
      yield* Deferred.succeed(releaseLateReader, undefined)
      yield* Fiber.join(first)
      yield* Fiber.join(restore)
      yield* Fiber.join(lateReader)
    }).pipe(Effect.provide(Gate)))

  it.effect("preserves fiber reentrancy inside an exclusive claim", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const claimed = yield* gate.claim(() => Effect.scoped(gate.shared))
      assert.strictEqual(claimed.incarnation, 0)
      assert.strictEqual((yield* gate.current).incarnation, 1)
    }).pipe(Effect.provide(Gate)))

  it.live("reenters a shared scope ahead of a waiting restore", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const reentrant = yield* Effect.scoped(Effect.gen(function*() {
        const permit = yield* gate.shared
        const restore = yield* gate.claim(() => Effect.never).pipe(
          Effect.forkChild({ startImmediately: true })
        )
        return yield* Effect.scoped(gate.shared).pipe(
          Effect.ensuring(Fiber.interrupt(restore)),
          Effect.map((nested) => ({ nested, permit }))
        )
      })).pipe(Effect.forkChild({ startImmediately: true }))
      const { nested, permit } = yield* Fiber.join(reentrant).pipe(
        Effect.timeout("100 millis"),
        Effect.ensuring(Fiber.interrupt(reentrant))
      )
      assert.deepStrictEqual(nested, permit)
    }).pipe(Effect.provide(Gate)))

  it.effect("allows concurrent shared scopes", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const firstAcquired = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const secondAcquired = yield* Deferred.make<void>()
      const first = yield* Effect.forkChild(Effect.scoped(Effect.gen(function*() {
        yield* gate.shared
        yield* Deferred.succeed(firstAcquired, undefined)
        yield* Deferred.await(releaseFirst)
      })))
      yield* Deferred.await(firstAcquired)
      const second = yield* Effect.forkChild(Effect.scoped(
        gate.shared.pipe(Effect.andThen(Deferred.succeed(secondAcquired, undefined)))
      ))
      yield* Deferred.await(secondAcquired)
      yield* Deferred.succeed(releaseFirst, undefined)
      yield* Fiber.join(first)
      yield* Fiber.join(second)
    }).pipe(Effect.provide(Gate)))

  it.effect("preserves nested exclusive reentrancy", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const nested = yield* gate.claim(() => gate.claim(Effect.succeed))
      assert.strictEqual(nested.incarnation, 2)
      assert.strictEqual((yield* gate.current).incarnation, 2)
    }).pipe(Effect.provide(Gate)))

  it.effect("does not publish a nested claim when the outer transaction rolls back", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const initial = yield* gate.current
      const result = yield* Effect.exit(
        gate.claim(() =>
          gate.claim(Effect.succeed).pipe(
            Effect.andThen(Effect.fail("rollback"))
          )
        )
      )
      assert.strictEqual(result._tag, "Failure")
      assert.deepStrictEqual(yield* gate.current, initial)
      const rows = yield* sql<{
        readonly replica_incarnation: number
        readonly writer_generation: number
      }>`SELECT replica_incarnation, writer_generation FROM effect_local_metadata WHERE singleton = 1`
      assert.deepStrictEqual(rows, [{
        replica_incarnation: initial.incarnation,
        writer_generation: initial.writerGeneration
      }])
    }).pipe(Effect.provide(Gate)))

  it.effect("does not publish a claim nested in a caller transaction that rolls back", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const initial = yield* gate.current
      const result = yield* Effect.exit(
        sql.withTransaction(
          gate.claim(Effect.succeed).pipe(
            Effect.andThen(Effect.fail("rollback"))
          )
        )
      )
      assert.strictEqual(result._tag, "Failure")
      assert.deepStrictEqual(yield* gate.current, initial)
      const rows = yield* sql<{
        readonly replica_incarnation: number
        readonly writer_generation: number
      }>`SELECT replica_incarnation, writer_generation FROM effect_local_metadata WHERE singleton = 1`
      assert.deepStrictEqual(rows, [{
        replica_incarnation: initial.incarnation,
        writer_generation: initial.writerGeneration
      }])
    }).pipe(Effect.provide(Gate)))

  it.effect("removes an interrupted request waiting for admission", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const claimAcquired = yield* Deferred.make<void>()
      const claim = yield* gate.claim(() =>
        Deferred.succeed(claimAcquired, undefined).pipe(Effect.andThen(Effect.never))
      ).pipe(Effect.forkChild)
      yield* Deferred.await(claimAcquired)
      const waiting = yield* Effect.scoped(gate.shared).pipe(Effect.forkChild)
      yield* Effect.yieldNow
      yield* Fiber.interrupt(waiting)
      yield* Fiber.interrupt(claim)
      yield* Effect.scoped(gate.shared)
    }).pipe(Effect.provide(Gate)))

  it.effect("retires an interrupted writer before admitting later readers", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const firstAcquired = yield* Deferred.make<void>()
      const releaseFirst = yield* Deferred.make<void>()
      const lateReaderAcquired = yield* Deferred.make<void>()
      const releaseLateReader = yield* Deferred.make<void>()
      const first = yield* Effect.forkChild(Effect.scoped(Effect.gen(function*() {
        yield* gate.shared
        yield* Deferred.succeed(firstAcquired, undefined)
        yield* Deferred.await(releaseFirst)
      })))
      yield* Deferred.await(firstAcquired)
      const writer = yield* Effect.forkChild(gate.claim(() => Effect.never))
      yield* Effect.yieldNow
      const lateReader = yield* Effect.forkChild(Effect.scoped(Effect.gen(function*() {
        yield* gate.shared
        yield* Deferred.succeed(lateReaderAcquired, undefined)
        yield* Deferred.await(releaseLateReader)
      })))
      yield* Effect.yieldNow
      yield* Fiber.interrupt(writer)
      yield* Deferred.await(lateReaderAcquired)
      yield* Deferred.succeed(releaseFirst, undefined)
      yield* Deferred.succeed(releaseLateReader, undefined)
      yield* Fiber.join(first)
      yield* Fiber.join(lateReader)
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

  it.effect("reports the generation observed from a concurrent writer", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const initial = yield* gate.current
      yield* sql`UPDATE effect_local_metadata
        SET writer_generation = writer_generation + 1
        WHERE singleton = 1`
      const result = yield* Effect.result(gate.validate(initial))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "ReplicaFenced")
        if (result.failure.reason._tag === "ReplicaFenced") {
          assert.strictEqual(result.failure.reason.expectedGeneration, initial.writerGeneration)
          assert.strictEqual(result.failure.reason.observedGeneration, initial.writerGeneration + 1)
        }
      }
    }).pipe(Effect.provide(Gate)))

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
