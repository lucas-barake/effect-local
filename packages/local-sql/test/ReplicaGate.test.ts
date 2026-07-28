import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Result from "effect/Result"
import * as Schema from "effect/Schema"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import { gateLimits, withGateLimits } from "./fixtures/limits.js"

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
  const Gate = Layer.merge(Dependencies, ReplicaGate.layer.pipe(withGateLimits, Layer.provide(Dependencies)))

  const gateWith = (maxQueuedPermits: number) => {
    const database = Layer.merge(SqliteClient.layer({ filename: ":memory:", disableWAL: true }), NodeCrypto.layer)
    const bootstrap = ReplicaBootstrap.layer(definition).pipe(Layer.provide(database))
    const dependencies = Layer.merge(database, bootstrap)
    return Layer.merge(
      dependencies,
      ReplicaGate.layer.pipe(
        Layer.provide(ReplicaLimits.layer({ ...gateLimits, maxQueuedPermits })),
        Layer.provide(dependencies)
      )
    )
  }

  type Admission = Result.Result<ReplicaGate.Permit, ReplicaError.ReplicaError>

  const holdWriter = (gate: ReplicaGate.ReplicaGate["Service"]) =>
    Effect.gen(function*() {
      const release = yield* Deferred.make<void>()
      const acquired = yield* Deferred.make<void>()
      const fiber = yield* Effect.forkChild(
        gate.claim(() => Deferred.succeed(acquired, undefined).pipe(Effect.andThen(Deferred.await(release))))
      )
      yield* Deferred.await(acquired)
      return { release, fiber } as const
    })

  const probeAdmission = (gate: ReplicaGate.ReplicaGate["Service"]) =>
    Effect.forkChild(Effect.result(Effect.scoped(gate.admit))).pipe(Effect.tap(() => Effect.yieldNow))

  const holdAdmission = (gate: ReplicaGate.ReplicaGate["Service"], release: Deferred.Deferred<void>) =>
    Effect.gen(function*() {
      const acquired = yield* Deferred.make<void>()
      const fiber = yield* Effect.forkChild(
        Effect.scoped(Effect.gen(function*() {
          yield* gate.admit
          yield* Deferred.succeed(acquired, undefined)
          yield* Deferred.await(release)
        }))
      )
      yield* Effect.yieldNow
      return { acquired, fiber } as const
    })

  const assertQuotaExceeded = (admission: Admission, limit: number) => {
    assert.isTrue(Result.isFailure(admission))
    if (Result.isFailure(admission)) {
      assert.strictEqual(admission.failure.reason._tag, "QuotaExceeded")
      if (admission.failure.reason._tag === "QuotaExceeded") {
        assert.strictEqual(admission.failure.reason.resource, "queued permits")
        assert.strictEqual(admission.failure.reason.limit, limit)
      }
    }
  }

  it.effect("rejects admitted work beyond the queued permit bound while a writer holds the gate", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const writer = yield* holdWriter(gate)

      const releaseQueued = yield* Deferred.make<void>()
      const first = yield* holdAdmission(gate, releaseQueued)
      const second = yield* holdAdmission(gate, releaseQueued)
      assert.isTrue(Option.isNone(yield* Deferred.poll(first.acquired)))
      assert.isTrue(Option.isNone(yield* Deferred.poll(second.acquired)))

      // Awaiting before the writer is released proves the probe was shed rather than queued.
      assertQuotaExceeded(yield* Fiber.join(yield* probeAdmission(gate)), 2)

      yield* Deferred.succeed(writer.release, undefined)
      yield* Fiber.join(writer.fiber)
      yield* Deferred.await(first.acquired)
      yield* Deferred.await(second.acquired)
      yield* Deferred.succeed(releaseQueued, undefined)
      yield* Fiber.join(first.fiber)
      yield* Fiber.join(second.fiber)
    }).pipe(Effect.provide(gateWith(2))))

  it.effect("sheds bounded acquirers while a writer waits behind live readers", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      // The realistic restore shape: a reader holds the read lock, so the writer takes the gate but then
      // blocks on the write lock, and a backlog forms behind it.
      const releaseReader = yield* Deferred.make<void>()
      const reader = yield* holdAdmission(gate, releaseReader)
      yield* Deferred.await(reader.acquired)

      const claimed = yield* Deferred.make<void>()
      const writer = yield* Effect.forkChild(gate.claim(() => Deferred.succeed(claimed, undefined)))
      yield* Effect.yieldNow
      assert.isTrue(Option.isNone(yield* Deferred.poll(claimed)))

      const releaseQueued = yield* Deferred.make<void>()
      const queued = yield* holdAdmission(gate, releaseQueued)
      assertQuotaExceeded(yield* Fiber.join(yield* probeAdmission(gate)), 1)

      yield* Deferred.succeed(releaseReader, undefined)
      yield* Fiber.join(reader.fiber)
      yield* Deferred.await(claimed)
      yield* Fiber.join(writer)
      yield* Deferred.await(queued.acquired)
      yield* Deferred.succeed(releaseQueued, undefined)
      yield* Fiber.join(queued.fiber)
    }).pipe(Effect.provide(gateWith(1))))

  it.effect("frees a queued permit slot when a waiting acquirer is interrupted", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const writer = yield* holdWriter(gate)

      const releaseQueued = yield* Deferred.make<void>()
      const queued = yield* holdAdmission(gate, releaseQueued)
      assertQuotaExceeded(yield* Fiber.join(yield* probeAdmission(gate)), 1)

      yield* Fiber.interrupt(queued.fiber)
      const replacement = yield* holdAdmission(gate, releaseQueued)
      yield* Deferred.succeed(writer.release, undefined)
      yield* Fiber.join(writer.fiber)
      yield* Deferred.await(replacement.acquired)
      yield* Deferred.succeed(releaseQueued, undefined)
      yield* Fiber.join(replacement.fiber)
    }).pipe(Effect.provide(gateWith(1))))

  it.effect("keeps the gate exclusive after admission rejections", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const writer = yield* holdWriter(gate)

      const releaseQueued = yield* Deferred.make<void>()
      const queued = yield* holdAdmission(gate, releaseQueued)
      for (let attempt = 0; attempt < 3; attempt++) {
        assertQuotaExceeded(yield* Fiber.join(yield* probeAdmission(gate)), 1)
      }
      yield* Deferred.succeed(writer.release, undefined)
      yield* Fiber.join(writer.fiber)
      yield* Deferred.await(queued.acquired)
      yield* Deferred.succeed(releaseQueued, undefined)
      yield* Fiber.join(queued.fiber)

      const secondWriter = yield* holdWriter(gate)
      const excluded = yield* holdAdmission(gate, releaseQueued)
      assert.isTrue(Option.isNone(yield* Deferred.poll(excluded.acquired)))
      yield* Deferred.succeed(secondWriter.release, undefined)
      yield* Fiber.join(secondWriter.fiber)
      yield* Deferred.await(excluded.acquired)
      yield* Fiber.join(excluded.fiber)
    }).pipe(Effect.provide(gateWith(1))))

  it.effect("admits reentrant work inside a claim while the queued permit bound is saturated", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const releaseQueued = yield* Deferred.make<void>()
      yield* gate.claim(() =>
        Effect.gen(function*() {
          const queued = yield* holdAdmission(gate, releaseQueued)
          assertQuotaExceeded(yield* Fiber.join(yield* probeAdmission(gate)), 1)

          yield* Effect.scoped(gate.admit)
          yield* gate.claim(() => Effect.void)

          yield* Fiber.interrupt(queued.fiber)
        })
      )
    }).pipe(Effect.provide(gateWith(1))))

  it.effect("admits a nested shared acquisition while the queued permit bound is saturated", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const releaseQueued = yield* Deferred.make<void>()
      const reader = yield* Deferred.make<void>()
      const nested = yield* Deferred.make<void>()
      const holder = yield* Effect.forkChild(
        Effect.scoped(Effect.gen(function*() {
          yield* gate.admit
          yield* Deferred.succeed(reader, undefined)
          yield* Deferred.await(releaseQueued)
          yield* Effect.scoped(gate.admit)
          yield* Deferred.succeed(nested, undefined)
        }))
      )
      yield* Deferred.await(reader)

      // A writer blocks behind the open read lock, so a further acquirer queues and saturates the bound.
      const writer = yield* Effect.forkChild(gate.claim(() => Effect.void))
      yield* Effect.yieldNow
      const queued = yield* holdAdmission(gate, releaseQueued)
      assertQuotaExceeded(yield* Fiber.join(yield* probeAdmission(gate)), 1)

      yield* Deferred.succeed(releaseQueued, undefined)
      yield* Deferred.await(nested)
      yield* Fiber.join(holder)
      yield* Fiber.join(writer)
      yield* Fiber.join(queued.fiber)
    }).pipe(Effect.provide(gateWith(1))))

  it.effect("admits an exclusive claim while the queued permit bound is saturated", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const writer = yield* holdWriter(gate)

      const releaseQueued = yield* Deferred.make<void>()
      const queued = yield* holdAdmission(gate, releaseQueued)
      assertQuotaExceeded(yield* Fiber.join(yield* probeAdmission(gate)), 1)

      // The claim must be queued rather than shed. It waits behind the already-queued acquirer in FIFO
      // order, so that acquirer has to finish before the claim can be granted.
      const secondClaimed = yield* Deferred.make<void>()
      const secondWriter = yield* Effect.forkChild(gate.claim(() => Deferred.succeed(secondClaimed, undefined)))
      yield* Effect.yieldNow
      assert.isTrue(Option.isNone(yield* Deferred.poll(secondClaimed)))

      yield* Deferred.succeed(writer.release, undefined)
      yield* Fiber.join(writer.fiber)
      yield* Deferred.await(queued.acquired)
      yield* Deferred.succeed(releaseQueued, undefined)
      yield* Fiber.join(queued.fiber)
      yield* Deferred.await(secondClaimed)
      yield* Fiber.join(secondWriter)
    }).pipe(Effect.provide(gateWith(1))))

  it.effect("admits infrastructure shared work while the queued permit bound is saturated", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const writer = yield* holdWriter(gate)

      const releaseQueued = yield* Deferred.make<void>()
      const queued = yield* holdAdmission(gate, releaseQueued)
      assertQuotaExceeded(yield* Fiber.join(yield* probeAdmission(gate)), 1)

      const infrastructure = yield* Effect.forkChild(Effect.result(Effect.scoped(gate.shared)))
      yield* Effect.yieldNow
      assert.isUndefined(infrastructure.pollUnsafe())

      yield* Deferred.succeed(writer.release, undefined)
      yield* Fiber.join(writer.fiber)
      assert.isTrue(Result.isSuccess(yield* Fiber.join(infrastructure)))
      yield* Deferred.await(queued.acquired)
      yield* Deferred.succeed(releaseQueued, undefined)
      yield* Fiber.join(queued.fiber)
    }).pipe(Effect.provide(gateWith(1))))

  it.effect("preserves grant order across cancelled waiters", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const order = yield* Ref.make<ReadonlyArray<string>>([])
      const writer = yield* holdWriter(gate)

      const releaseQueued = yield* Deferred.make<void>()
      const queue = (label: string) =>
        Effect.gen(function*() {
          const fiber = yield* Effect.forkChild(
            Effect.scoped(Effect.gen(function*() {
              yield* gate.admit
              yield* Ref.update(order, (labels) => [...labels, label])
              yield* Deferred.await(releaseQueued)
            }))
          )
          yield* Effect.yieldNow
          return fiber
        })

      const a = yield* queue("A")
      const b = yield* queue("B")
      const c = yield* queue("C")
      const d = yield* queue("D")
      yield* Fiber.interrupt(b)
      yield* Fiber.interrupt(c)

      yield* Deferred.succeed(writer.release, undefined)
      yield* Fiber.join(writer.fiber)
      yield* Deferred.succeed(releaseQueued, undefined)
      yield* Fiber.join(a)
      yield* Fiber.join(d)
      assert.deepStrictEqual(yield* Ref.get(order), ["A", "D"])
    }).pipe(Effect.provide(gateWith(4))))

  it.effect("blocks rather than sheds a live waiter while a cancelled waiter is reaped", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const writer = yield* holdWriter(gate)

      const releaseQueued = yield* Deferred.make<void>()
      const live = yield* holdAdmission(gate, releaseQueued)
      const cancelled = yield* holdAdmission(gate, releaseQueued)
      yield* Fiber.interrupt(cancelled.fiber)
      yield* Effect.yieldNow

      assert.isTrue(Option.isNone(yield* Deferred.poll(live.acquired)))

      yield* Deferred.succeed(writer.release, undefined)
      yield* Fiber.join(writer.fiber)
      yield* Deferred.await(live.acquired)
      yield* Deferred.succeed(releaseQueued, undefined)
      yield* Fiber.join(live.fiber)
    }).pipe(Effect.provide(gateWith(2))))

  it.effect("admits a concurrent burst while no exclusive holder blocks the gate", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const outcomes = yield* Effect.all(
        Array.from({ length: 8 }, () => Effect.result(Effect.scoped(gate.admit))),
        { concurrency: "unbounded" }
      )
      const shed = outcomes.filter(Result.isFailure).map((outcome) => outcome.failure.reason._tag)
      assert.deepStrictEqual(shed, [])
    }).pipe(Effect.provide(gateWith(2))))

  // A claim that is interrupted while still QUEUED is the only path that leaves an entry in the arbiter's
  // exclusive-waiter index without granting it. Nothing else drains that index, so failing to reap it
  // would make the shed condition permanently true and every later acquirer would be shed with
  // QuotaExceeded on a completely idle replica.
  it.effect("keeps admitting on an idle gate after a queued claim is interrupted", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const releaseReader = yield* Deferred.make<void>()
      // The reader pins the read lock, so the first claim takes the gate but blocks on the write lock and
      // a second claim can only queue behind it.
      const reader = yield* holdAdmission(gate, releaseReader)
      yield* Deferred.await(reader.acquired)
      const first = yield* Effect.forkChild(gate.claim(() => Effect.void))
      yield* Effect.yieldNow
      const queued = yield* Effect.forkChild(gate.claim(() => Effect.void))
      yield* Effect.yieldNow
      yield* Fiber.interrupt(queued)

      yield* Deferred.succeed(releaseReader, undefined)
      yield* Fiber.join(reader.fiber)
      yield* Fiber.join(first)

      const outcomes = yield* Effect.all(
        Array.from({ length: 8 }, () => Effect.result(Effect.scoped(gate.admit))),
        { concurrency: "unbounded" }
      )
      assert.deepStrictEqual(
        outcomes.filter(Result.isFailure).map((outcome) => outcome.failure.reason._tag),
        []
      )
    }).pipe(Effect.provide(gateWith(1))))

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

  // A missing singleton is replica-wide, not one document's bytes, so it must not arrive as
  // `StorageCorrupt`: `ReplicaEvolution` quarantines a document on that reason and `BackupStore`
  // reports it as an invalid backup.
  it.effect("reports a missing metadata singleton as a typed replica-wide failure", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      const permit = yield* gate.current
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`
      const result = yield* Effect.result(gate.validate(permit))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
      }
    }).pipe(Effect.provide(Gate)))

  it.effect("reports a missing metadata singleton from refresh as well", () =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`
      const result = yield* Effect.result(gate.refresh)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
      }
    }).pipe(Effect.provide(Gate)))

  // `claim` reads the singleton twice inside its own transaction, right after an UPDATE that matches
  // zero rows. The exclusive permit must still be released, or every later acquirer deadlocks.
  it.effect("reports a missing metadata singleton from claim and still releases the gate", () =>
    Effect.scoped(Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const sql = yield* SqlClient.SqlClient
      yield* sql`DELETE FROM effect_local_metadata WHERE singleton = 1`
      const result = yield* Effect.result(gate.claim(() => Effect.void))
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result) && result.failure._tag === "ReplicaError") {
        assert.strictEqual(result.failure.reason._tag, "ReplicaMetadataMissing")
      }
      const generations = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_local_writer_generations`
      assert.strictEqual(generations[0]?.count, 1)
      // Proves the exclusive permit was released rather than stranded.
      yield* Effect.scoped(gate.shared)
    })).pipe(Effect.provide(Gate)))
})
