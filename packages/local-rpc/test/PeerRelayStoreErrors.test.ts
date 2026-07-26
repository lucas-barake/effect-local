import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as SqlError from "effect/unstable/sql/SqlError"
import { mapStoreErrors } from "../src/internal/peerRelayStoreErrors.js"

describe("PeerRelayStore errors", () => {
  it.effect("maps typed SQL failures while preserving rollback defects", () =>
    Effect.gen(function*() {
      const sqlFailure = new SqlError.SqlError({
        reason: new SqlError.UnknownError({ cause: new Error("commit failed") })
      })
      const rollbackDefect = new Error("rollback failed")
      const exit = yield* mapStoreErrors(
        Effect.failCause(
          Cause.combine(
            Cause.fail(sqlFailure),
            Cause.die(rollbackDefect)
          )
        )
      ).pipe(Effect.exit)
      assert.strictEqual(Exit.isFailure(exit), true)
      if (Exit.isFailure(exit)) {
        const failures = exit.cause.reasons.filter(Cause.isFailReason)
        assert.strictEqual(failures.length, 1)
        const mapped = failures[0]!.error
        assert.strictEqual(mapped._tag, "ReplicaError")
        if (mapped._tag === "ReplicaError") {
          assert.strictEqual(mapped.reason._tag, "StorageUnavailable")
          if (mapped.reason._tag === "StorageUnavailable") {
            assert.strictEqual(mapped.reason.cause, sqlFailure)
          }
        }
        assert.strictEqual(
          exit.cause.reasons.some((reason) => Cause.isDieReason(reason) && reason.defect === rollbackDefect),
          true
        )
      }
    }))
})
