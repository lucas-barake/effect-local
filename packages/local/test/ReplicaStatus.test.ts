import { assert, describe, it } from "@effect/vitest"
import * as Schema from "effect/Schema"
import * as ReplicaStatus from "../src/ReplicaStatus.js"

describe("ReplicaStatus", () => {
  it("represents every public lifecycle state", () => {
    const values: ReadonlyArray<ReplicaStatus.ReplicaStatus> = [
      { _tag: "Starting", phase: "migrations" },
      { _tag: "Ready", pendingCommands: 0 },
      { _tag: "ReadOnly", reason: "newer schema" },
      { _tag: "Degraded", reason: "projection retry" },
      { _tag: "ProjectionBlocked", projection: "TaskRows", reason: "invalid row" },
      { _tag: "Restoring", processedBytes: 128 },
      { _tag: "Failed", message: "storage unavailable" }
    ]
    for (const value of values) {
      assert.deepStrictEqual(Schema.decodeUnknownSync(ReplicaStatus.ReplicaStatus)(value), value)
    }
  })
})
