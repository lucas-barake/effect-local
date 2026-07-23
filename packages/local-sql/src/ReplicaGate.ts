import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as DateTime from "effect/DateTime"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as TxReentrantLock from "effect/TxReentrantLock"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as ReplicaBootstrap from "./ReplicaBootstrap.js"

export interface Permit {
  readonly replicaId: Identity.ReplicaId
  readonly incarnation: Identity.ReplicaIncarnation
  readonly writerGeneration: Identity.WriterGeneration
}

export class ReplicaGate extends Context.Service<ReplicaGate, {
  readonly current: Effect.Effect<Permit>
  readonly shared: Effect.Effect<Permit, never, Scope.Scope>
  readonly claim: <A, E, R,>(
    use: (permit: Permit) => Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E | ReplicaError.ReplicaError | SqlError.SqlError, R>
  readonly refresh: Effect.Effect<Permit, ReplicaError.ReplicaError>
  readonly validate: (expected: Permit) => Effect.Effect<void, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/ReplicaGate") {}

export const layer: Layer.Layer<ReplicaGate, never, ReplicaBootstrap.ReplicaBootstrap | SqlClient.SqlClient> = Layer
  .effect(
    ReplicaGate,
    Effect.gen(function*() {
      const bootstrap = yield* ReplicaBootstrap.ReplicaBootstrap
      const sql = yield* SqlClient.SqlClient
      const state = yield* Ref.make<Permit>(bootstrap)
      const lock = yield* TxReentrantLock.make()
      const writer = yield* Ref.make<number | null>(null)
      const readers = yield* Ref.make(new Map<number, number>())
      const requests = yield* Effect.acquireRelease(
        Queue.unbounded<
          | {
            readonly _tag: "Acquire"
            readonly granted: Deferred.Deferred<boolean>
          }
          | { readonly _tag: "Release" }
        >(),
        Queue.shutdown
      )
      yield* Effect.gen(function*() {
        const pending: Array<Deferred.Deferred<boolean>> = []
        let occupied = false
        while (true) {
          const request = yield* Queue.take(requests)
          if (request._tag === "Acquire") pending.push(request.granted)
          else occupied = false
          while (!occupied && pending.length > 0) {
            occupied = yield* Deferred.succeed(pending.shift()!, true)
          }
        }
      }).pipe(Effect.forkScoped({ startImmediately: true }))
      const release = Queue.offer(requests, { _tag: "Release" }).pipe(Effect.asVoid)
      const acquire = Effect.gen(function*() {
        const granted = yield* Deferred.make<boolean>()
        yield* Queue.offer(requests, { _tag: "Acquire", granted })
        yield* Deferred.await(granted).pipe(
          Effect.onInterrupt(() =>
            Deferred.succeed(granted, false).pipe(
              Effect.flatMap((cancelled) => cancelled ? Effect.void : release)
            )
          )
        )
      }).pipe(Effect.interruptible)
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
            ),
          { interruptible: true }
        )
      )
      const writeLock = Effect.acquireRelease(
        TxReentrantLock.acquireWrite(lock),
        () => TxReentrantLock.releaseWrite(lock),
        { interruptible: true }
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
      const readState = findState(undefined).pipe(
        Effect.map((row): Permit => ({
          replicaId: row.replica_id,
          incarnation: row.replica_incarnation,
          writerGeneration: row.writer_generation
        })),
        Effect.catchTags({
          NoSuchElementError: () => Effect.die(new Error("Replica metadata was not initialized")),
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
      return {
        current: Ref.get(state),
        refresh: readState.pipe(Effect.tap((next) => Ref.set(state, next))),
        shared: Effect.withFiber((fiber) =>
          Effect.all({ readers: Ref.get(readers), writer: Ref.get(writer) }).pipe(
            Effect.flatMap(({ readers, writer }) =>
              writer === fiber.id || (readers.get(fiber.id) ?? 0) > 0
                ? readLock
                : Effect.acquireUseRelease(
                  acquire,
                  () => readLock,
                  () => release
                )
            ),
            Effect.andThen(Ref.get(state))
          )
        ),
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
                        const permit = yield* readState
                        yield* sql`INSERT INTO effect_local_writer_generations (generation, claimed_at)
                      VALUES (${permit.writerGeneration}, ${DateTime.formatIso(yield* DateTime.now)})`
                        const result = yield* restore(use(permit))
                        return [result, yield* readState] as const
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
                          acquire,
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
              rows.length === 1 ? Effect.void : readState.pipe(
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
