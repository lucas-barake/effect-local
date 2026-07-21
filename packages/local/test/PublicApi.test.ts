import { assert, describe, it } from "@effect/vitest"
import * as Local from "../src/index.js"

describe("public API", () => {
  it("exports the supported namespaces", () => {
    assert.deepStrictEqual(Object.keys(Local).toSorted(), [
      "Backup",
      "Canonical",
      "CommandOutcome",
      "Commit",
      "Document",
      "DocumentSet",
      "Identity",
      "Mutation",
      "PeerTransport",
      "Projection",
      "Query",
      "Replica",
      "ReplicaDefinition",
      "ReplicaError",
      "ReplicaLimits",
      "ReplicaStatus",
      "Snapshot"
    ])
  })
})
