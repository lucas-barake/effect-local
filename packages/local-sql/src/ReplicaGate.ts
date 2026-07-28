import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Context from "effect/Context"
import * as DateTime from "effect/DateTime"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as LogLevel from "effect/LogLevel"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as TxReentrantLock from "effect/TxReentrantLock"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import { metadataMissing } from "./internal/errors.js"
import * as ReplicaBootstrap from "./ReplicaBootstrap.js"

export interface Permit {
  readonly replicaId: Identity.ReplicaId
  readonly incarnation: Identity.ReplicaIncarnation
  readonly writerGeneration: Identity.WriterGeneration
}

/**
 * The arbiter grants by succeeding this, sheds by failing it, and an interrupted acquirer interrupts it
 * itself. Those are the three outcomes, so they are carried on `Deferred`'s three channels rather than
 * re-encoded as success values that every consumer would have to re-dispatch on.
 */
type Admission = Deferred.Deferred<void, ReplicaError.ReplicaError>

/**
 * How one acquirer asked for the gate. `exclusive` (`claim`) holds it long enough to build a real
 * backlog; `bounded` (`admit`) is the only kind the arbiter is ever allowed to shed.
 */
type Acquirer = "shared" | "exclusive" | "bounded"

export class ReplicaGate extends Context.Service<ReplicaGate, {
  readonly current: Effect.Effect<Permit>
  /**
   * Whether an exclusive claim owns the replica right now. `current` lags the database for as long as a
   * claim is running, because the claim bumps `writer_generation` inside its transaction and republishes
   * the matching permit only once that transaction has committed. An observer that compares the two has to
   * know that the disagreement belongs to this process, not to a foreign writer that fenced the replica.
   */
  readonly claiming: Effect.Effect<boolean>
  readonly shared: Effect.Effect<Permit, never, Scope.Scope>
  readonly admit: Effect.Effect<Permit, ReplicaError.ReplicaError, Scope.Scope>
  readonly claim: <A, E, R,>(
    use: (permit: Permit) => Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E | ReplicaError.ReplicaError | SqlError.SqlError, R>
  readonly refresh: Effect.Effect<Permit, ReplicaError.ReplicaError>
  readonly validate: (expected: Permit) => Effect.Effect<void, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/ReplicaGate") {}

export const layer: Layer.Layer<
  ReplicaGate,
  never,
  ReplicaBootstrap.ReplicaBootstrap | SqlClient.SqlClient | ReplicaLimits.ReplicaLimits
> = Layer
  .effect(
    ReplicaGate,
    Effect.gen(function*() {
      const bootstrap = yield* ReplicaBootstrap.ReplicaBootstrap
      const sql = yield* SqlClient.SqlClient
      const limits = yield* ReplicaLimits.ReplicaLimits
      const state = yield* Ref.make<Permit>(bootstrap)
      const lock = yield* TxReentrantLock.make()
      const writer = yield* Ref.make<number | null>(null)
      const readers = yield* Ref.make(new Map<number, number>())
      const requests = yield* Effect.acquireRelease(
        Queue.unbounded<
          | { readonly _tag: "Acquire"; readonly granted: Admission; readonly kind: Acquirer }
          | { readonly _tag: "Interrupt"; readonly granted: Admission }
          | { readonly _tag: "Release" }
        >(),
        Queue.shutdown
      )
      // The arbiter fiber owns `waiters` for the Layer scope. An interrupted acquirer reports itself with
      // `Interrupt` so its entry is dropped immediately; without that, entries could only be reaped by the
      // grant loop below, which cannot run while a writer holds the gate. That reaping, not the cap, is
      // what actually bounds memory during a long restore.
      yield* Effect.gen(function*() {
        const waiters = new Set<Admission>()
        const exclusiveWaiters = new Set<Admission>()
        // A fresh `waiters.values()` restarts at slot 0 and steps over every slot a previous grant deleted,
        // because a `Set` deletion leaves a tombstone until the table is rehashed. Building one iterator per
        // grant makes draining a backlog of N waiters O(N^2). One long-lived cursor yields the same
        // sequence: a `Set` iterator visits entries added after it was created and skips entries deleted
        // before it reaches them, which is exactly the FIFO-with-cancellation order this loop wants.
        let cursor = waiters.values()
        const takeNext = (): Admission => {
          let step = cursor.next()
          // An exhausted iterator stays done, so it has to be replaced once `waiters` refills.
          if (step.done === true) {
            cursor = waiters.values()
            step = cursor.next()
          }
          return step.value!
        }
        let holder: "shared" | "exclusive" | null = null
        while (true) {
          const request = yield* Queue.take(requests)
          switch (request._tag) {
            case "Acquire":
              // Only the arbiter knows the real waiter set, so admission is decided here. A shared
              // acquirer holds the gate only long enough to take the read lock, so a queue of shared
              // acquirers always drains on its own: shedding it would reject work that a fully idle
              // replica can serve. Only a backlog behind an exclusive holder can grow without bound,
              // so that is the sole condition under which a bounded acquirer is shed.
              if (
                request.kind === "bounded" &&
                (holder === "exclusive" || exclusiveWaiters.size > 0) &&
                waiters.size >= limits.maxQueuedPermits
              ) {
                yield* Deferred.fail(
                  request.granted,
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.QuotaExceeded({
                      resource: "queued permits",
                      limit: limits.maxQueuedPermits
                    })
                  })
                )
              } else {
                waiters.add(request.granted)
                if (request.kind === "exclusive") exclusiveWaiters.add(request.granted)
              }
              break
            case "Interrupt":
              waiters.delete(request.granted)
              exclusiveWaiters.delete(request.granted)
              break
            case "Release":
              holder = null
              break
          }
          while (holder === null && waiters.size > 0) {
            const next = takeNext()
            waiters.delete(next)
            const nextExclusive = exclusiveWaiters.delete(next)
            if (yield* Deferred.succeed(next, undefined)) holder = nextExclusive ? "exclusive" : "shared"
          }
        }
      }).pipe(Effect.forkScoped({ startImmediately: true }))
      const release = Queue.offer(requests, { _tag: "Release" }).pipe(Effect.asVoid)
      // Only `Deferred.await` may be interrupted, and the cleanup handler is installed INSIDE the
      // surrounding uninterruptible region on purpose.
      //
      // `Effect.onInterrupt` is an on-exit frame that is also popped on the success path, and a frame
      // popped while the fiber is still interruptible re-arms interruptibility behind itself: `onExit`
      // runs its continuation uninterruptibly and pushes a restore marker UNDER the frame it just
      // consumed. That marker is an interrupt-raising point which now sits after the handler is gone but
      // before `Effect.acquireUseRelease` has installed its release. An interrupt landing there granted
      // the gate to a fiber that then died without ever offering `Release`, so the gate stayed held
      // forever and every later acquirer deadlocked. Popped from an already-uninterruptible region, no
      // marker is pushed and the grant flows straight into the bracket that owes the release.
      //
      // Callers must therefore run this uninterruptibly, which every caller does: it is always the
      // `acquire` of `Effect.acquireUseRelease`. That is also why `Effect.interruptible` is applied
      // directly to the await instead of restored from an `Effect.uninterruptibleMask` -- inside that
      // bracket the fiber is already uninterruptible, so `uninterruptibleMask` would hand back `identity`
      // and a queued waiter could never be cancelled at all.
      const requestAdmission = (kind: Acquirer) =>
        Deferred.make<void, ReplicaError.ReplicaError>().pipe(
          Effect.flatMap((granted) =>
            Effect.uninterruptible(
              Effect.gen(function*() {
                yield* Queue.offer(requests, { _tag: "Acquire", granted, kind })
                return yield* Effect.interruptible(Deferred.await(granted))
              }).pipe(
                Effect.onInterrupt(() =>
                  Deferred.interrupt(granted).pipe(
                    Effect.flatMap((interrupted) =>
                      interrupted
                        ? Queue.offer(requests, { _tag: "Interrupt", granted }).pipe(Effect.asVoid)
                        // The arbiter decided first. Only a grant leaves the gate held, so a shed
                        // acquirer must not offer a `Release` it never earned.
                        : Deferred.await(granted).pipe(Effect.andThen(release), Effect.ignore)
                    )
                  )
                )
              )
            )
          )
        )
      // The arbiter only sheds `bounded` requests, so `shared` and `claim` can never be rejected. State
      // that with `orDie` rather than discarding the failure: silently treating a rejection as a grant
      // would let the caller take the read lock without holding the gate, breaking mutual exclusion.
      const acquire = requestAdmission("shared").pipe(Effect.orDie)
      const acquireExclusive = requestAdmission("exclusive").pipe(Effect.orDie)
      // `Effect.annotateLogs` copies the fiber context whenever it runs and is not level-guarded, so it
      // must not execute when Debug is disabled. Rejections are emitted exactly when the replica is
      // already saturated, which is the worst moment to spend work on a discarded log line.
      const rejectionLog = Effect.logDebug("ReplicaGate admission rejected").pipe(
        Effect.annotateLogs({ resource: "queued permits", limit: limits.maxQueuedPermits }),
        Effect.when(LogLevel.isEnabled("Debug"))
      )
      const acquireBounded = requestAdmission("bounded").pipe(Effect.tapError(() => rejectionLog))
      const readLock = Effect.withFiber((fiber) =>
        Effect.acquireRelease(
          TxReentrantLock.acquireRead(lock).pipe(
            Effect.tap(() =>
              Ref.update(readers, (current) => {
                const next = new Map(current)
                next.set(fiber.id, (current.get(fiber.id) ?? 0) + 1)
                return next
              })
            )
          ),
          () =>
            TxReentrantLock.releaseRead(lock).pipe(
              Effect.andThen(Ref.update(readers, (current) => {
                const next = new Map(current)
                const count = current.get(fiber.id) ?? 0
                if (count <= 1) next.delete(fiber.id)
                else next.set(fiber.id, count - 1)
                return next
              }))
            )
          // No `{ interruptible: true }`: `TxReentrantLock.acquireRead` commits its `TxRef` before returning,
          // so an interrupt observed between the commit and the finalizer registration would leak the read
          // lock and block every future writer forever. This matches `TxReentrantLock.readLock` upstream.
          // Safe to acquire uninterruptibly here because the arbiter already serialises readers against the
          // writer, so `acquireRead` never has to retry.
        )
      )
      // `acquireWrite` has to stay interruptible: it blocks until every reader drains, and an interrupted
      // restore must be able to abandon that wait. But `Effect.tx` commits its journal before leaving its
      // own mask, so an interrupt recorded during the commit is raised only after the lock is already
      // held, `Effect.acquireRelease` never reaches its finalizer registration, and the write lock is
      // stranded. Every later `acquireRead` then retries forever, and because `readLock` acquires
      // uninterruptibly those readers cannot even be cancelled out of it.
      //
      // Compensate on interrupt, but only when this attempt actually took the lock. `writeLocks` reports
      // the reentrancy count of whichever fiber is the writer, so comparing it across the attempt is what
      // distinguishes "my acquire committed" from "someone else already held it". Releasing
      // unconditionally would be wrong for a REENTRANT nested `claim`: that fiber is already the writer at
      // count 1, so an interrupt before its nested acquire commits would release the OUTER claim's lock.
      // The comparison is sound because only one fiber can be inside `writeLock` at a time -- the
      // non-reentrant path reaches it only after the arbiter granted the gate exclusively, and the
      // reentrant path is by construction the same fiber.
      const writeLock = Effect.uninterruptibleMask((restore) =>
        TxReentrantLock.writeLocks(lock).pipe(
          Effect.flatMap((before) =>
            Effect.acquireRelease(
              restore(TxReentrantLock.acquireWrite(lock)).pipe(
                Effect.onInterrupt(() =>
                  TxReentrantLock.writeLocks(lock).pipe(
                    Effect.flatMap((after) => after > before ? TxReentrantLock.releaseWrite(lock) : Effect.void)
                  )
                )
              ),
              () => TxReentrantLock.releaseWrite(lock)
            )
          )
        )
      )
      const findState = SqlSchema.findOne({
        Request: Schema.Void,
        Result: Schema.Struct({
          replica_id: Identity.ReplicaId,
          replica_incarnation: Identity.ReplicaIncarnation,
          writer_generation: Identity.WriterGeneration
        }),
        execute: () =>
          sql`SELECT replica_id, replica_incarnation, writer_generation
            FROM effect_local_metadata WHERE singleton = 1`
      })
      // The documented boundary where a missing metadata singleton stops being a defect.
      //
      // It was left as a defect deliberately, on the reasoning that a typed failure inside a
      // `ClusterSchema.Persisted` entity handler would be recorded as a terminal reply and replayed
      // for that primary key forever. Measured against the real cluster, that does not happen: every
      // `DocumentEntity` RPC is also annotated `ClusterSchema.WithTransaction`, which makes the reply
      // write share the handler's transaction, so a failing handler rolls the reply back and the
      // message stays unprocessed. The defect, meanwhile, is not "retry until repaired" either --
      // nothing recreates this row at runtime, so the entity rebuilds on a backoff forever while the
      // caller waits for a reply that is never written.
      //
      // The reason is deliberately NOT `StorageCorrupt`. That one means a single document's stored
      // bytes are unusable, and consumers act on it per document: `ReplicaEvolution` quarantines the
      // document it was reading and `BackupStore` reports an invalid backup. A lost replica identity
      // is neither.
      const readState = (operation: string) =>
        findState(undefined).pipe(
          Effect.map((row): Permit => ({
            replicaId: row.replica_id,
            incarnation: row.replica_incarnation,
            writerGeneration: row.writer_generation
          })),
          Effect.catchTags({
            NoSuchElementError: () => Effect.fail(metadataMissing(operation)),
            SchemaError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageCorrupt({
                    cause
                  })
                })
              ),
            SqlError: (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: new ReplicaError.StorageUnavailable({
                    cause
                  })
                })
              )
          })
        )
      const validateState = SqlSchema.findAll({
        Request: Schema.Struct({
          incarnation: Identity.ReplicaIncarnation,
          writerGeneration: Identity.WriterGeneration
        }),
        Result: Schema.Struct({
          replica_incarnation: Identity.ReplicaIncarnation,
          writer_generation: Identity.WriterGeneration
        }),
        execute: (expected) =>
          sql`UPDATE effect_local_metadata SET writer_generation = writer_generation
            WHERE singleton = 1
              AND replica_incarnation = ${expected.incarnation}
              AND writer_generation = ${expected.writerGeneration}
            RETURNING replica_incarnation, writer_generation`
      })
      // A shed acquirer never reaches `readLock` or `release`, because `Effect.acquireUseRelease` installs
      // its release only after `acquire` succeeds. So the gate's own occupancy is untouched by a shed.
      const sharedWith = <E,>(admission: Effect.Effect<void, E>) => {
        const enter = Effect.acquireUseRelease(admission, () => readLock, () => release)
        return Effect.withFiber((fiber) =>
          Effect.all({ readers: Ref.get(readers), writer: Ref.get(writer) }).pipe(
            Effect.flatMap(({ readers, writer }) =>
              writer === fiber.id || (readers.get(fiber.id) ?? 0) > 0
                ? readLock
                : enter
            ),
            Effect.andThen(Ref.get(state))
          )
        )
      }
      return {
        current: Ref.get(state),
        // `writer` is set before `run` and cleared only after `run` has republished `state`, so a `false`
        // here proves `state` already carries the generation of the last claim that touched the database.
        claiming: Ref.get(writer).pipe(Effect.map((owner) => owner !== null)),
        refresh: readState("ReplicaGate.refresh").pipe(Effect.tap((next) => Ref.set(state, next))),
        shared: sharedWith(acquire),
        admit: sharedWith(acquireBounded),
        claim: (use) =>
          Effect.withFiber((fiber) => {
            const run = (publish: boolean) =>
              Effect.scoped(
                writeLock.pipe(
                  Effect.andThen(
                    Effect.uninterruptibleMask((restore) =>
                      sql.withTransaction(Effect.gen(function*() {
                        yield* sql`UPDATE effect_local_metadata SET
                      replica_incarnation = replica_incarnation + 1,
                      writer_generation = writer_generation + 1
                      WHERE singleton = 1`
                        const permit = yield* readState("ReplicaGate.claim")
                        yield* sql`INSERT INTO effect_local_writer_generations (generation, claimed_at)
                      VALUES (${permit.writerGeneration}, ${DateTime.formatIso(yield* DateTime.now)})`
                        const result = yield* restore(use(permit))
                        return [result, yield* readState("ReplicaGate.claim")] as const
                      })).pipe(
                        Effect.flatMap(([result, permit]) =>
                          publish ? Ref.set(state, permit).pipe(Effect.as(result)) : Effect.succeed(result)
                        )
                      )
                    )
                  )
                )
              )
            return Ref.get(writer).pipe(
              Effect.flatMap((owner) =>
                owner === fiber.id
                  ? run(false)
                  : Effect.serviceOption(sql.transactionService).pipe(
                    Effect.flatMap((transaction) =>
                      transaction._tag === "Some"
                        ? Effect.die(new Error("ReplicaGate.claim cannot run inside an existing SQL transaction"))
                        : Effect.acquireUseRelease(
                          acquireExclusive,
                          () => Ref.set(writer, fiber.id).pipe(Effect.andThen(run(true))),
                          () => Ref.set(writer, null).pipe(Effect.andThen(release))
                        )
                    )
                  )
              )
            )
          }),
        validate: (expected) =>
          validateState(expected).pipe(
            Effect.flatMap((rows) =>
              rows.length === 1 ? Effect.void : readState("ReplicaGate.validate").pipe(
                Effect.flatMap((observed) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: new ReplicaError.ReplicaFenced({
                        expectedGeneration: expected.writerGeneration,
                        observedGeneration: observed.writerGeneration
                      })
                    })
                  )
                )
              )
            ),
            Effect.catchTags({
              SchemaError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageCorrupt({
                      cause
                    })
                  })
                ),
              SqlError: (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: new ReplicaError.StorageUnavailable({
                      cause
                    })
                  })
                )
            })
          )
      }
    })
  )
