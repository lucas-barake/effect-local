import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as ReplicaWorkflow from "../src/ReplicaWorkflow.js"

describe("ReplicaWorkflow", () => {
  it.effect("derives stable execution ids from the replica incarnation and operation id", () =>
    Effect.gen(function*() {
      const payload = {
        replicaIncarnation: Identity.ReplicaIncarnation.make(2),
        operationId: ReplicaWorkflow.OperationId.make("backup-2026-07-21")
      }
      const first = yield* ReplicaWorkflow.CreateBackup.executionId(payload)
      const second = yield* ReplicaWorkflow.CreateBackup.executionId(payload)
      assert.strictEqual(first, second)
      assert.notStrictEqual(
        first,
        yield* ReplicaWorkflow.CreateBackup.executionId({
          ...payload,
          replicaIncarnation: Identity.ReplicaIncarnation.make(3)
        })
      )
      assert.notStrictEqual(
        first,
        yield* ReplicaWorkflow.CreateBackup.executionId({
          ...payload,
          operationId: ReplicaWorkflow.OperationId.make("backup-2026-07-22")
        })
      )
    }))

  it.effect("keeps projection rebuild identities specific to the projection", () =>
    Effect.gen(function*() {
      const payload = {
        replicaIncarnation: Identity.ReplicaIncarnation.make(1),
        operationId: ReplicaWorkflow.OperationId.make("rebuild"),
        projection: "Tasks"
      }
      assert.notStrictEqual(
        yield* ReplicaWorkflow.ProjectionRebuild.executionId(payload),
        yield* ReplicaWorkflow.ProjectionRebuild.executionId({ ...payload, projection: "Labels" })
      )
    }))
})
