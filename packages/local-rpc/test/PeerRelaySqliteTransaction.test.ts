import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Result from "effect/Result"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlConnection from "effect/unstable/sql/SqlConnection"
import * as SqlError from "effect/unstable/sql/SqlError"
import { cleanupAcquisition, make } from "../src/internal/peerRelaySqliteTransaction.js"

describe("peerRelaySqliteTransaction", () => {
  it.effect("preserves interruption together with rollback failure during acquisition", () =>
    Effect.gen(function*() {
      const rollbackDefect = new Error("rollback failed")
      const connection = {
        executeUnprepared: (statement: string) => {
          if (statement === "ROLLBACK") {
            return Effect.die(rollbackDefect)
          }
          return Effect.succeed([])
        }
      } as unknown as SqlConnection.Connection
      const scope = yield* Scope.make("sequential")
      const exit = yield* cleanupAcquisition(
        Cause.interrupt(99_023),
        scope,
        connection,
        true
      ).pipe(Effect.exit)
      assert.strictEqual(Exit.isFailure(exit), true)
      if (Exit.isFailure(exit)) {
        assert.strictEqual(Cause.hasInterrupts(exit.cause), true)
        assert.strictEqual(Result.getOrThrow(Cause.findDefect(exit.cause)), rollbackDefect)
      }
    }))

  it.effect("does not retry LockTimeoutError when Scope release also defects", () =>
    Effect.gen(function*() {
      const closeDefect = new Error("scope close failed")
      const lock = new SqlError.SqlError({
        reason: new SqlError.LockTimeoutError({ cause: new Error("busy") })
      })
      let begins = 0
      const connection = {
        executeUnprepared: (statement: string) => {
          if (statement === "BEGIN IMMEDIATE") {
            begins++
            return Effect.fail(lock)
          }
          return Effect.succeed([])
        }
      } as unknown as SqlConnection.Connection
      const reserve = Effect.gen(function*() {
        const scope = yield* Scope.Scope
        yield* Scope.addFinalizer(scope, Effect.die(closeDefect))
        return connection
      })
      const sql = {
        reserve,
        transactionService: SqlClient.TransactionConnection(999_024)
      } as unknown as SqlClient.SqlClient
      const exit = yield* make(sql, {
        maxAcquireAttempts: 2,
        acquireRetryBaseDelayMillis: 0,
        acquireRetryMaximumDelayMillis: 0
      })(Effect.void).pipe(Effect.exit)
      assert.strictEqual(begins, 1)
      assert.strictEqual(Exit.isFailure(exit), true)
      if (Exit.isFailure(exit)) {
        assert.strictEqual(Result.getOrThrow(Cause.findDefect(exit.cause)), closeDefect)
        const failure = Result.getOrThrow(Cause.findError(exit.cause))
        assert.strictEqual(failure, lock)
      }
    }))

  it.effect("preserves a body failure together with rollback failure", () =>
    Effect.gen(function*() {
      const bodyFailure = new Error("body failed")
      const rollbackDefect = new Error("rollback failed")
      const connection = {
        executeUnprepared: (statement: string) =>
          statement === "ROLLBACK"
            ? Effect.die(rollbackDefect)
            : Effect.succeed([])
      } as unknown as SqlConnection.Connection
      const sql = {
        reserve: Effect.succeed(connection),
        transactionService: SqlClient.TransactionConnection(999_025)
      } as unknown as SqlClient.SqlClient
      const exit = yield* make(sql, {
        maxAcquireAttempts: 1,
        acquireRetryBaseDelayMillis: 0,
        acquireRetryMaximumDelayMillis: 0
      })(Effect.fail(bodyFailure)).pipe(Effect.exit)
      assert.strictEqual(Exit.isFailure(exit), true)
      if (Exit.isFailure(exit)) {
        assert.strictEqual(Result.getOrThrow(Cause.findError(exit.cause)), bodyFailure)
        assert.strictEqual(Result.getOrThrow(Cause.findDefect(exit.cause)), rollbackDefect)
      }
    }))

  it.effect("preserves a body interruption together with rollback failure", () =>
    Effect.gen(function*() {
      const rollbackDefect = new Error("rollback failed")
      const connection = {
        executeUnprepared: (statement: string) =>
          statement === "ROLLBACK"
            ? Effect.die(rollbackDefect)
            : Effect.succeed([])
      } as unknown as SqlConnection.Connection
      const sql = {
        reserve: Effect.succeed(connection),
        transactionService: SqlClient.TransactionConnection(999_026)
      } as unknown as SqlClient.SqlClient
      const exit = yield* make(sql, {
        maxAcquireAttempts: 1,
        acquireRetryBaseDelayMillis: 0,
        acquireRetryMaximumDelayMillis: 0
      })(Effect.interrupt).pipe(Effect.exit)
      assert.strictEqual(Exit.isFailure(exit), true)
      if (Exit.isFailure(exit)) {
        assert.strictEqual(Cause.hasInterrupts(exit.cause), true)
        assert.strictEqual(Result.getOrThrow(Cause.findDefect(exit.cause)), rollbackDefect)
      }
    }))

  it.effect("preserves body, rollback, and transaction scope close failures", () =>
    Effect.gen(function*() {
      const bodyFailure = new Error("body failed")
      const rollbackDefect = new Error("rollback failed")
      const closeDefect = new Error("scope close failed")
      const connection = {
        executeUnprepared: (statement: string) =>
          statement === "ROLLBACK"
            ? Effect.die(rollbackDefect)
            : Effect.succeed([])
      } as unknown as SqlConnection.Connection
      const reserve = Effect.gen(function*() {
        const scope = yield* Scope.Scope
        yield* Scope.addFinalizer(scope, Effect.die(closeDefect))
        return connection
      })
      const sql = {
        reserve,
        transactionService: SqlClient.TransactionConnection(999_027)
      } as unknown as SqlClient.SqlClient
      const exit = yield* make(sql, {
        maxAcquireAttempts: 1,
        acquireRetryBaseDelayMillis: 0,
        acquireRetryMaximumDelayMillis: 0
      })(Effect.fail(bodyFailure)).pipe(Effect.exit)
      assert.strictEqual(Exit.isFailure(exit), true)
      if (Exit.isFailure(exit)) {
        assert.strictEqual(Result.getOrThrow(Cause.findError(exit.cause)), bodyFailure)
        const defects = new Set(
          exit.cause.reasons
            .filter(Cause.isDieReason)
            .map((reason) => reason.defect)
        )
        assert.strictEqual(defects.has(rollbackDefect), true)
        assert.strictEqual(defects.has(closeDefect), true)
      }
    }))
})
