import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Result from "effect/Result"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlConnection from "effect/unstable/sql/SqlConnection"
import * as SqlError from "effect/unstable/sql/SqlError"
import { cleanupAcquisition, makeSqlite as make } from "../src/internal/peerRelaySqlTransaction.js"

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

  it.effect("rolls back a deferred constraint commit failure before reusing the connection", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const transaction = make(sql, {
        maxAcquireAttempts: 1,
        acquireRetryBaseDelayMillis: 0,
        acquireRetryMaximumDelayMillis: 0
      })
      yield* sql`PRAGMA foreign_keys = ON`
      yield* sql`CREATE TABLE parents (
        parent_id INTEGER PRIMARY KEY
      )`
      yield* sql`CREATE TABLE children (
        child_id INTEGER PRIMARY KEY,
        parent_id INTEGER NOT NULL,
        FOREIGN KEY (parent_id) REFERENCES parents(parent_id)
          DEFERRABLE INITIALLY DEFERRED
      )`

      const failed = yield* transaction(
        sql`INSERT INTO children (child_id, parent_id) VALUES (1, 99)`
      ).pipe(Effect.exit)
      assert.strictEqual(Exit.isFailure(failed), true)

      const succeeded = yield* transaction(
        sql`INSERT INTO parents (parent_id) VALUES (1)`
      ).pipe(Effect.exit)
      assert.strictEqual(Exit.isSuccess(succeeded), true)

      const children = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM children
      `
      const parents = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM parents
      `
      assert.strictEqual(children[0]?.count, 0)
      assert.strictEqual(parents[0]?.count, 1)
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))))

  it.effect("preserves commit, rollback, and transaction scope close failures", () =>
    Effect.gen(function*() {
      const commitDefect = new Error("commit failed")
      const rollbackDefect = new Error("rollback failed")
      const closeDefect = new Error("scope close failed")
      const connection = {
        executeUnprepared: (statement: string) => {
          if (statement === "COMMIT") {
            return Effect.die(commitDefect)
          }
          if (statement === "ROLLBACK") {
            return Effect.die(rollbackDefect)
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
        transactionService: SqlClient.TransactionConnection(999_028)
      } as unknown as SqlClient.SqlClient
      const exit = yield* make(sql, {
        maxAcquireAttempts: 1,
        acquireRetryBaseDelayMillis: 0,
        acquireRetryMaximumDelayMillis: 0
      })(Effect.void).pipe(Effect.exit)
      assert.strictEqual(Exit.isFailure(exit), true)
      if (Exit.isFailure(exit)) {
        const defects = new Set(
          exit.cause.reasons
            .filter(Cause.isDieReason)
            .map((reason) => reason.defect)
        )
        assert.strictEqual(defects.has(commitDefect), true)
        assert.strictEqual(defects.has(rollbackDefect), true)
        assert.strictEqual(defects.has(closeDefect), true)
      }
    }))

  it.effect("does not duplicate a typed commit failure as a defect", () =>
    Effect.gen(function*() {
      const commitFailure = new SqlError.SqlError({
        reason: new SqlError.UnknownError({ cause: new Error("commit failed") })
      })
      const connection = {
        executeUnprepared: (statement: string) =>
          statement === "COMMIT"
            ? Effect.fail(commitFailure)
            : Effect.succeed([])
      } as unknown as SqlConnection.Connection
      const sql = {
        reserve: Effect.succeed(connection),
        transactionService: SqlClient.TransactionConnection(999_029)
      } as unknown as SqlClient.SqlClient
      const exit = yield* make(sql, {
        maxAcquireAttempts: 1,
        acquireRetryBaseDelayMillis: 0,
        acquireRetryMaximumDelayMillis: 0
      })(Effect.void).pipe(Effect.exit)
      assert.strictEqual(Exit.isFailure(exit), true)
      if (Exit.isFailure(exit)) {
        assert.strictEqual(Result.getOrThrow(Cause.findError(exit.cause)), commitFailure)
        assert.strictEqual(
          exit.cause.reasons.some((reason) => Cause.isDieReason(reason) && reason.defect === commitFailure),
          false
        )
      }
    }))
})
