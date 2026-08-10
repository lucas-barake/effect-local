import { NodeCrypto } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Document from "@lucas-barake/effect-local/Document"
import * as DocumentSet from "@lucas-barake/effect-local/DocumentSet"
import * as ReplicaDefinition from "@lucas-barake/effect-local/ReplicaDefinition"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Scheduler from "effect/Scheduler"
import * as Schema from "effect/Schema"
import * as ReplicaBootstrap from "../src/ReplicaBootstrap.js"
import * as ReplicaGate from "../src/ReplicaGate.js"
import { gateLimits } from "./fixtures/limits.js"

describe("ReplicaGate grant boundary", () => {
  const Task = Document.make("Task", { schema: Schema.String, version: 1 })
  const definition = ReplicaDefinition.make({
    name: "tasks",
    documents: DocumentSet.make(Task),
    mutations: [],
    projections: [],
    queries: []
  })

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

  // The probes are forked and the deadline is put on `Fiber.await`, not on the probe itself. A stranded
  // write lock makes every later `acquireRead` retry forever, and `readLock` acquires uninterruptibly, so
  // a deadline wrapped around the probe could never fire in exactly the failure this exists to catch --
  // the suite would hit the vitest timeout with no indication of which case broke. `forkDetach` rather
  // than `forkChild` so a wedged probe cannot re-hang the parent when its scope closes.
  const assertGateUsable = (label: string) =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const admitProbe = yield* Effect.forkDetach(Effect.scoped(gate.admit).pipe(Effect.asVoid))
      const admitted = yield* Fiber.await(admitProbe).pipe(Effect.timeoutOption("2 seconds"))
      const claimProbe = yield* Effect.forkDetach(gate.claim(() => Effect.void))
      const claimed = yield* Fiber.await(claimProbe).pipe(Effect.timeoutOption("2 seconds"))
      assert.strictEqual(
        `admit=${admitted._tag} claim=${claimed._tag}`,
        "admit=Some claim=Some",
        `${label}: gate was left permanently occupied`
      )
    })

  /**
   * Interrupting from a forked fiber spends a scheduling slot of its own, which pushes the landing point
   * past the few steps that follow the grant. `interruptUnsafe` delivers it inline, pinning it to the
   * exact step the parent stopped on, which is the only way to reach the window after the acquirer's
   * cleanup handler is popped but before the bracket that owes the `Release` is installed.
   */
  const round = (maxOps: number, offset: number, deliver: "inline" | "forked") =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const label = `maxOps ${maxOps} offset ${offset} ${deliver}`
      const writerHeld = yield* Deferred.make<void>()
      const releaseWriter = yield* Deferred.make<void>()
      const writer = yield* Effect.forkChild(
        gate.claim(() => Deferred.succeed(writerHeld, undefined).pipe(Effect.andThen(Deferred.await(releaseWriter))))
      )
      yield* Deferred.await(writerHeld)

      const waiter = yield* Effect.forkChild(
        Effect.result(Effect.scoped(gate.admit)).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, maxOps))
      )
      yield* Effect.yieldNow

      yield* Deferred.succeed(releaseWriter, undefined)
      for (let turn = 0; turn < offset; turn++) yield* Effect.yieldNow
      if (deliver === "inline") {
        yield* Effect.sync(() => waiter.interruptUnsafe())
      } else {
        yield* Effect.forkChild(Fiber.interrupt(waiter)).pipe(Effect.asVoid)
      }
      const waiterExit = yield* Fiber.await(waiter).pipe(Effect.timeoutOption("5 seconds"))
      assert.strictEqual(waiterExit._tag, "Some", `${label}: waiter never terminated`)
      const writerExit = yield* Fiber.await(writer).pipe(Effect.timeoutOption("5 seconds"))
      assert.strictEqual(writerExit._tag, "Some", `${label}: writer never terminated`)

      yield* assertGateUsable(label)
    })

  // A fiber the arbiter has already granted must hand the gate back even when the interrupt is observed
  // after its acquisition returned. The grid is contiguous on purpose: the earlier hand-picked list
  // skipped `maxOps` 5, and that one value exposed a permanent gate deadlock. A grid whose members were
  // chosen because they pass is not evidence. `MaxOpsBeforeYield` only changes scheduling granularity;
  // it does not change any Effect semantics.
  it.live("an interrupt landing right after the grant never strands the gate", () =>
    Effect.gen(function*() {
      for (let maxOps = 2; maxOps <= 18; maxOps++) {
        for (let offset = 0; offset < 16; offset++) {
          yield* round(maxOps, offset, "forked")
          yield* round(maxOps, offset, "inline")
        }
      }
    }).pipe(Effect.provide(gateWith(4))), 300_000)

  // An interrupted `claim` strands the TxReentrantLock write lock unless the acquisition compensates,
  // and because `readLock` acquires uninterruptibly the readers that pile up behind it cannot be
  // cancelled out. The reader forces the claim to block in `acquireWrite`, which is where the window is.
  const claimRound = (maxOps: number, offset: number) =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const label = `maxOps ${maxOps} offset ${offset}`
      const readerHeld = yield* Deferred.make<void>()
      const releaseReader = yield* Deferred.make<void>()
      const reader = yield* Effect.forkChild(
        Effect.scoped(Effect.gen(function*() {
          yield* gate.admit
          yield* Deferred.succeed(readerHeld, undefined)
          yield* Deferred.await(releaseReader)
        }))
      )
      yield* Deferred.await(readerHeld)

      const claimer = yield* Effect.forkChild(
        gate.claim(() => Effect.void).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, maxOps))
      )
      yield* Effect.yieldNow
      yield* Deferred.succeed(releaseReader, undefined)
      for (let turn = 0; turn < offset; turn++) yield* Effect.yieldNow
      yield* Effect.sync(() => claimer.interruptUnsafe())

      const claimExit = yield* Fiber.await(claimer).pipe(Effect.timeoutOption("5 seconds"))
      assert.strictEqual(claimExit._tag, "Some", `${label}: claim never terminated`)
      const readerExit = yield* Fiber.await(reader).pipe(Effect.timeoutOption("5 seconds"))
      assert.strictEqual(readerExit._tag, "Some", `${label}: reader never terminated`)

      yield* assertGateUsable(label)
    })

  it.live("an interrupted claim never strands the write lock", () =>
    Effect.gen(function*() {
      for (let maxOps = 2; maxOps <= 18; maxOps++) {
        for (let offset = 0; offset < 16; offset++) {
          yield* claimRound(maxOps, offset)
        }
      }
    }).pipe(Effect.provide(gateWith(4))), 300_000)

  // The sweep above only interrupts a claim that is BLOCKED in `acquireWrite` behind a reader. With no
  // reader the acquire never retries, so the interrupt lands inside the epoch transaction or just after
  // the scope finalizer was registered instead: the window where a spurious compensation would release a
  // lock the finalizer also releases.
  const idleClaimRound = (maxOps: number, offset: number) =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const label = `idle claim maxOps ${maxOps} offset ${offset}`
      const claimer = yield* Effect.forkChild(
        gate.claim(() => Effect.void).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, maxOps))
      )
      for (let turn = 0; turn < offset; turn++) yield* Effect.yieldNow
      yield* Effect.sync(() => claimer.interruptUnsafe())
      const claimExit = yield* Fiber.await(claimer).pipe(Effect.timeoutOption("5 seconds"))
      assert.strictEqual(claimExit._tag, "Some", `${label}: claim never terminated`)
      yield* assertGateUsable(label)
    })

  it.live("an interrupted unobstructed claim never strands the write lock", () =>
    Effect.gen(function*() {
      for (let maxOps = 2; maxOps <= 18; maxOps++) {
        for (let offset = 0; offset < 20; offset++) {
          yield* idleClaimRound(maxOps, offset)
        }
      }
    }).pipe(Effect.provide(gateWith(4))), 300_000)

  // A write lock the compensation failed to release is unrecoverable rather than merely slow, because
  // `readLock` acquires uninterruptibly: a reader arriving afterwards can never be cancelled out of
  // `acquireRead`. The trailing reader queues behind the claim's exclusive hold so it is granted only
  // once the interrupted claim has retired.
  const readerBehindInterruptedClaimRound = (maxOps: number, offset: number) =>
    Effect.gen(function*() {
      const gate = yield* ReplicaGate.ReplicaGate
      const label = `reader behind claim maxOps ${maxOps} offset ${offset}`
      const readerHeld = yield* Deferred.make<void>()
      const releaseReader = yield* Deferred.make<void>()
      const reader = yield* Effect.forkChild(
        Effect.scoped(Effect.gen(function*() {
          yield* gate.admit
          yield* Deferred.succeed(readerHeld, undefined)
          yield* Deferred.await(releaseReader)
        }))
      )
      yield* Deferred.await(readerHeld)

      const claimer = yield* Effect.forkChild(
        gate.claim(() => Effect.void).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, maxOps))
      )
      yield* Effect.yieldNow
      const trailing = yield* Effect.forkChild(Effect.result(Effect.scoped(gate.admit)))
      yield* Effect.yieldNow

      yield* Deferred.succeed(releaseReader, undefined)
      for (let turn = 0; turn < offset; turn++) yield* Effect.yieldNow
      yield* Effect.sync(() => claimer.interruptUnsafe())

      const claimExit = yield* Fiber.await(claimer).pipe(Effect.timeoutOption("5 seconds"))
      assert.strictEqual(claimExit._tag, "Some", `${label}: claim never terminated`)
      const readerExit = yield* Fiber.await(reader).pipe(Effect.timeoutOption("5 seconds"))
      assert.strictEqual(readerExit._tag, "Some", `${label}: reader never terminated`)
      const trailingExit = yield* Fiber.await(trailing).pipe(Effect.timeoutOption("5 seconds"))
      assert.strictEqual(trailingExit._tag, "Some", `${label}: trailing reader never terminated`)
      yield* assertGateUsable(label)
    })

  it.live("a reader behind an interrupted claim is never wedged on the write lock", () =>
    Effect.gen(function*() {
      for (let maxOps = 2; maxOps <= 18; maxOps++) {
        for (let offset = 0; offset < 20; offset++) {
          yield* readerBehindInterruptedClaimRound(maxOps, offset)
        }
      }
    }).pipe(Effect.provide(gateWith(8))), 300_000)
})
