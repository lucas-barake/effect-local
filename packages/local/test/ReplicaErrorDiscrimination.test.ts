import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Identity from "../src/Identity.js"
import * as ReplicaError from "../src/ReplicaError.js"

describe("ReplicaError discrimination", () => {
  const error = new ReplicaError.ReplicaError({
    reason: new ReplicaError.QuotaExceeded({ resource: "documents", limit: 10 })
  })

  it("exposes the reason as message and cause", () => {
    assert.strictEqual(error.message, "QuotaExceeded")
    assert.strictEqual(error.cause, error.reason)
  })

  it.effect("is recovered by catchTag and discriminated by reason tag", () =>
    Effect.gen(function*() {
      const reasonTag = yield* Effect.fail(error).pipe(
        Effect.catchTag("ReplicaError", (failure) => Effect.succeed(failure.reason._tag))
      )
      assert.strictEqual(reasonTag, "QuotaExceeded")
    }))

  it("round trips a fenced reason carrying branded generations", () => {
    const fenced = new ReplicaError.ReplicaError({
      reason: new ReplicaError.ReplicaFenced({
        expectedGeneration: Identity.WriterGeneration.make(1),
        observedGeneration: Identity.WriterGeneration.make(2)
      })
    })
    const encoded = Schema.encodeSync(ReplicaError.ReplicaError)(fenced)
    assert.deepStrictEqual(Schema.decodeUnknownSync(ReplicaError.ReplicaError)(encoded), fenced)
  })
})
