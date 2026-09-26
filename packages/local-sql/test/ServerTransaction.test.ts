import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlError from "effect/unstable/sql/SqlError"
import * as StorageUnavailable from "../src/internal/storageUnavailable.js"
import * as Transaction from "../src/internal/transaction.js"

const layerSqlite = SqliteClient.layer({ filename: ":memory:" })

const provideSqlite = Effect.provide(layerSqlite)

const failingOnce = (failure: ReplicaError.StorageUnavailable) => {
  let attempts = 0
  const effect = Effect.suspend(() => {
    attempts += 1
    if (attempts === 1) return Effect.fail(failure)
    return Effect.succeed("committed")
  })
  return { effect, attempts: () => attempts }
}

describe("server transactions", () => {
  it.effect(
    "retries a transaction whose deadlock surfaced as StorageUnavailable",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      const deadlock = new SqlError.SqlError({ reason: new SqlError.DeadlockError({ cause: "deadlock detected" }) })
      const probe = failingOnce(StorageUnavailable.make(deadlock))
      assert.strictEqual(yield* Transaction.withServerTransaction(sql, probe.effect), "committed")
      assert.strictEqual(probe.attempts(), 2)
    }, provideSqlite)
  )

  it.effect(
    "does not retry a StorageUnavailable caused by anything but a transient conflict",
    Effect.fnUntraced(function*() {
      const sql = yield* SqlClient.SqlClient
      const probe = failingOnce(StorageUnavailable.make("disk full"))
      const outcome = yield* Transaction.withServerTransaction(sql, probe.effect).pipe(
        Effect.catch((error) => Effect.succeed(error._tag))
      )
      assert.strictEqual(outcome, "StorageUnavailable")
      assert.strictEqual(probe.attempts(), 1)
    }, provideSqlite)
  )
})
