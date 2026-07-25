import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import { TestClock } from "effect/testing"
import * as TestReplica from "../src/TestReplica.js"
import { definition, Rename, Task } from "./fixtures.js"

it.layer(NodeCrypto.layer)("TestReplica limits threading", (it) => {
  const Handler = Rename.toLayer(({ draft, payload }) => {
    draft.title = payload
    return undefined
  })

  it.effect("layerWithSyncAndLimits exposes the provided limits rather than defaults", () =>
    Effect.gen(function*() {
      const custom = { ...TestReplica.defaultLimits, maxSessions: 3, maxQueuedRpc: 7 }
      assert.notStrictEqual(TestReplica.defaultLimits.maxSessions, custom.maxSessions)
      const limits = yield* ReplicaLimits.ReplicaLimits.pipe(
        Effect.provide(
          TestReplica.layerWithSyncAndLimits(definition, { projections: [], limits: custom }).pipe(
            Layer.provide(Handler)
          )
        )
      )
      assert.strictEqual(limits.maxSessions, 3)
      assert.strictEqual(limits.maxQueuedRpc, 7)
    }).pipe(TestClock.withLive))

  it.effect("commits a concurrent write burst on an idle replica", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const commandIds = yield* Effect.all(
        Array.from({ length: 8 }, () => Identity.makeCommandId)
      )
      const outcomes = yield* Effect.all(
        commandIds.map((commandId) => Effect.result(replica.create(Task, { commandId, value: { title: commandId } }))),
        { concurrency: "unbounded" }
      )
      const shed = outcomes.filter(Result.isFailure).map((outcome) => outcome.failure.reason._tag)
      assert.deepStrictEqual(shed, [])
      const stored = yield* Effect.all(
        commandIds.map((commandId) => replica.get(Task, Identity.documentIdFromCommandId(commandId)))
      )
      assert.deepStrictEqual(stored.map((snapshot) => snapshot.value.title), [...commandIds])
    }).pipe(
      Effect.provide(
        TestReplica.layerWithSyncAndLimits(definition, {
          projections: [],
          limits: { ...TestReplica.defaultLimits, maxQueuedPermits: 2 }
        }).pipe(Layer.provide(Handler))
      ),
      TestClock.withLive
    ))
})
