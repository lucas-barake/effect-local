import * as Identity from "@lucas-barake/effect-local/Identity"
import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as ReplicaBootstrap from "./ReplicaBootstrap.js"

export interface Permit {
  readonly replicaId: Identity.ReplicaId
  readonly incarnation: Identity.ReplicaIncarnation
  readonly writerGeneration: Identity.WriterGeneration
}

export class ReplicaGate extends Context.Service<ReplicaGate, {
  readonly current: Effect.Effect<Permit>
  readonly shared: Effect.Effect<Permit, never, Scope.Scope>
  readonly exclusive: Effect.Effect<Permit, ReplicaError.ReplicaError, Scope.Scope>
  readonly refresh: Effect.Effect<Permit, ReplicaError.ReplicaError>
  readonly validate: (expected: Permit) => Effect.Effect<void, ReplicaError.ReplicaError>
}>()("@lucas-barake/effect-local-sql/ReplicaGate") {}

const permits = 1_000_000

const decodePermit = (row: {
  readonly replica_id: string
  readonly replica_incarnation: number
  readonly writer_generation: number
}) =>
  Effect.try({
    try: (): Permit => ({
      replicaId: Identity.ReplicaId.make(row.replica_id),
      incarnation: Identity.ReplicaIncarnation.make(row.replica_incarnation),
      writerGeneration: Identity.WriterGeneration.make(row.writer_generation)
    }),
    catch: (cause) =>
      new ReplicaError.ReplicaError({
        reason: {
          _tag: "StorageCorrupt",
          cause: { _tag: "SchemaCause", message: String(cause), path: [] }
        }
      })
  })

export const layer: Layer.Layer<ReplicaGate, never, ReplicaBootstrap.ReplicaBootstrap | SqlClient.SqlClient> = Layer
  .effect(
    ReplicaGate,
    Effect.gen(function*() {
      const bootstrap = yield* ReplicaBootstrap.ReplicaBootstrap
      const sql = yield* SqlClient.SqlClient
      const semaphore = yield* Semaphore.make(permits)
      const state = yield* Ref.make<Permit>(bootstrap)
      const readState = sql<{
        readonly replica_id: string
        readonly replica_incarnation: number
        readonly writer_generation: number
      }>`SELECT replica_id, replica_incarnation, writer_generation
        FROM effect_local_metadata WHERE singleton = 1`.pipe(
        Effect.flatMap((rows) =>
          rows[0] === undefined
            ? Effect.die(new Error("Replica metadata was not initialized"))
            : decodePermit(rows[0])
        ),
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(
            new ReplicaError.ReplicaError({
              reason: {
                _tag: "StorageUnavailable",
                cause: { _tag: "SqlCause", message: String(cause), code: null }
              }
            })
          ))
      )
      return {
        current: Ref.get(state),
        refresh: readState.pipe(Effect.tap((next) => Ref.set(state, next))),
        shared: Effect.acquireRelease(
          semaphore.take(1).pipe(Effect.andThen(Ref.get(state))),
          () => semaphore.release(1)
        ),
        exclusive: Effect.acquireRelease(
          semaphore.take(permits),
          () => semaphore.release(permits)
        ).pipe(
          Effect.andThen(
            sql.withTransaction(Effect.gen(function*() {
              yield* sql`UPDATE effect_local_metadata SET
              replica_incarnation = replica_incarnation + 1,
              writer_generation = writer_generation + 1
              WHERE singleton = 1`
              const rows = yield* sql<{
                readonly replica_id: string
                readonly replica_incarnation: number
                readonly writer_generation: number
              }>`SELECT replica_id, replica_incarnation, writer_generation
              FROM effect_local_metadata WHERE singleton = 1`
              const row = rows[0]
              if (row === undefined) return yield* Effect.die(new Error("Replica metadata was not initialized"))
              yield* sql`INSERT INTO effect_local_writer_generations (generation, claimed_at)
              VALUES (${row.writer_generation}, ${new Date().toISOString()})`
              return yield* decodePermit(row)
            })).pipe(
              Effect.catchTag("SqlError", (cause) =>
                Effect.fail(
                  new ReplicaError.ReplicaError({
                    reason: {
                      _tag: "StorageUnavailable",
                      cause: { _tag: "SqlCause", message: String(cause), code: null }
                    }
                  })
                )),
              Effect.tap((next) => Ref.set(state, next)),
              Effect.uninterruptible
            )
          )
        ),
        validate: (expected) =>
          sql<{
            readonly replica_incarnation: number
            readonly writer_generation: number
          }>`UPDATE effect_local_metadata SET writer_generation = writer_generation
            WHERE singleton = 1
              AND replica_incarnation = ${expected.incarnation}
              AND writer_generation = ${expected.writerGeneration}
            RETURNING replica_incarnation, writer_generation`.pipe(
            Effect.flatMap((rows) =>
              rows.length === 1 ? Effect.void : Ref.get(state).pipe(
                Effect.flatMap((observed) =>
                  Effect.fail(
                    new ReplicaError.ReplicaError({
                      reason: {
                        _tag: "ReplicaFenced",
                        expectedGeneration: expected.writerGeneration,
                        observedGeneration: observed.writerGeneration
                      }
                    })
                  )
                )
              )
            ),
            Effect.catchTag("SqlError", (cause) =>
              Effect.fail(
                new ReplicaError.ReplicaError({
                  reason: {
                    _tag: "StorageUnavailable",
                    cause: { _tag: "SqlCause", message: String(cause), code: null }
                  }
                })
              ))
          )
      }
    })
  )
