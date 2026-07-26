import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlConnection from "effect/unstable/sql/SqlConnection"
import type * as SqlError from "effect/unstable/sql/SqlError"

export class NestedPeerRelayTransactionError extends Schema.TaggedErrorClass<NestedPeerRelayTransactionError>(
  "@lucas-barake/effect-local-rpc/internal/peerRelaySqliteTransaction/NestedPeerRelayTransactionError"
)("NestedPeerRelayTransactionError", {}) {}

export interface Options {
  readonly maxAcquireAttempts: number
  readonly acquireRetryBaseDelayMillis: number
  readonly acquireRetryMaximumDelayMillis: number
}

const execute = (connection: SqlConnection.Connection, statement: string) =>
  connection.executeUnprepared(statement, [], undefined).pipe(Effect.asVoid)

export const cleanupAcquisition = (
  original: Cause.Cause<SqlError.SqlError>,
  scope: Scope.Closeable | undefined,
  connection: SqlConnection.Connection | undefined,
  began: boolean
): Effect.Effect<never, SqlError.SqlError> =>
  Effect.uninterruptible(
    Effect.gen(function*() {
      let combined: Cause.Cause<SqlError.SqlError> = original
      if (began && connection !== undefined) {
        const rollback = yield* execute(connection, "ROLLBACK").pipe(Effect.exit)
        if (Exit.isFailure(rollback)) {
          combined = Cause.combine(combined, rollback.cause)
        }
      }
      if (scope !== undefined) {
        const closed = yield* Scope.close(scope, Exit.failCause(combined)).pipe(Effect.exit)
        if (Exit.isFailure(closed)) {
          combined = Cause.combine(combined, closed.cause)
        }
      }
      return yield* Effect.failCause(combined)
    })
  )

const acquireImmediate = (
  sql: SqlClient.SqlClient,
  options: Options
): Effect.Effect<
  readonly [Scope.Closeable, SqlConnection.Connection],
  SqlError.SqlError
> =>
  Effect.interruptibleMask((restore) => {
    const attempt = (
      remaining: number,
      attemptNumber: number
    ): Effect.Effect<
      readonly [Scope.Closeable, SqlConnection.Connection],
      SqlError.SqlError
    > =>
      Effect.suspend(() => {
        let began = false
        let scope: Scope.Closeable | undefined
        let connection: SqlConnection.Connection | undefined
        const acquire = Effect.gen(function*() {
          scope = yield* Scope.make("sequential")
          const reserved = yield* sql.reserve.pipe(
            Effect.provideService(Scope.Scope, scope)
          )
          connection = reserved
          return yield* restore(
            execute(reserved, "BEGIN IMMEDIATE").pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  began = true
                })
              ),
              Effect.as([scope, reserved] as const)
            )
          )
        }).pipe(
          Effect.catchCause((cause) => cleanupAcquisition(cause, scope, connection, began))
        )
        return acquire.pipe(
          Effect.catchCause((cause) => {
            const only = cause.reasons.length === 1 ? cause.reasons[0] : undefined
            const lockTimeout = only !== undefined &&
              Cause.isFailReason(only) &&
              only.error._tag === "SqlError" &&
              only.error.reason._tag === "LockTimeoutError"
            if (!lockTimeout || remaining <= 1) {
              return Effect.failCause(cause)
            }
            return Effect.sleep(Math.min(
              options.acquireRetryMaximumDelayMillis,
              options.acquireRetryBaseDelayMillis * 2 ** attemptNumber
            )).pipe(
              Effect.andThen(attempt(remaining - 1, attemptNumber + 1))
            )
          })
        )
      })
    return attempt(options.maxAcquireAttempts, 0)
  })

export const make = (
  sql: SqlClient.SqlClient,
  options: Options
) => {
  return <A, E, R,>(
    effect: Effect.Effect<A, E, R>
  ): Effect.Effect<
    A,
    E | SqlError.SqlError | NestedPeerRelayTransactionError,
    R
  > =>
    Effect.gen(function*() {
      const active = yield* Effect.serviceOption(sql.transactionService)
      if (Option.isSome(active)) {
        return yield* new NestedPeerRelayTransactionError()
      }
      return yield* Effect.suspend(() => {
        let bodyCause: Cause.Cause<E> | undefined
        let rollbackCause: Cause.Cause<SqlError.SqlError> | undefined
        const withTransaction = SqlClient.makeWithTransaction({
          transactionService: sql.transactionService,
          spanAttributes: [["db.system", "sqlite"]],
          acquireConnection: acquireImmediate(sql, options),
          begin: () => Effect.void,
          savepoint: () => Effect.die(new Error("Nested relay transactions are forbidden")),
          commit: (connection) => execute(connection, "COMMIT"),
          rollback: (connection) =>
            execute(connection, "ROLLBACK").pipe(
              Effect.exit,
              Effect.tap((exit) =>
                Effect.sync(() => {
                  if (Exit.isFailure(exit)) {
                    rollbackCause = exit.cause
                  }
                })
              ),
              Effect.asVoid
            ),
          rollbackSavepoint: () => Effect.die(new Error("Nested relay transactions are forbidden"))
        })
        const observed = effect.pipe(
          Effect.catchCause((cause) =>
            Effect.sync(() => {
              bodyCause = cause
            }).pipe(Effect.andThen(Effect.failCause(cause)))
          )
        )
        return withTransaction(observed).pipe(
          Effect.catchCause((cause) => {
            let combined = bodyCause === undefined
              ? cause
              : Cause.combine(bodyCause, cause)
            if (rollbackCause !== undefined) {
              combined = Cause.combine(combined, rollbackCause)
            }
            return Effect.failCause(combined)
          })
        )
      })
    })
}
