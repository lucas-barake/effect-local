import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Replica from "@lucas-barake/effect-local/Replica"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import type * as ReplicaStatus from "@lucas-barake/effect-local/ReplicaStatus"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Result from "effect/Result"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as TestReplica from "../src/TestReplica.js"
import { definition, Rename, Task } from "./fixtures.js"

it.layer(NodeCrypto.layer)("TestReplica limits threading", (test) => {
  const Handler = Rename.toLayer(({ draft, payload }) => {
    draft.title = payload
    return undefined
  })

  test.effect("layerWithSyncAndLimits exposes the provided limits rather than defaults", () =>
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

  test.effect("layerWithSyncAndLimits forwards the configured health sampling interval", () =>
    Effect.gen(function*() {
      const replica = yield* Replica.Replica
      const sql = yield* SqlClient.SqlClient
      const seen = yield* Queue.unbounded<ReplicaStatus.ReplicaStatus>()
      yield* replica.status.pipe(
        Stream.runForEach((status) => Queue.offer(seen, status)),
        Effect.forkChild
      )
      assert.deepStrictEqual(yield* Queue.take(seen), { _tag: "Ready", pendingCommands: 0 })
      yield* sql`UPDATE effect_local_metadata SET writer_generation = writer_generation + 1
        WHERE singleton = 1`
      yield* TestClock.adjust("1 second")
      assert.deepStrictEqual(yield* Queue.poll(seen), Option.none())
      yield* TestClock.adjust("4 seconds")
      assert.deepStrictEqual(yield* Queue.take(seen), {
        _tag: "ReadOnly",
        reason: "Another writer generation owns this replica"
      })
    }).pipe(
      Effect.provide(
        TestReplica.layerWithSyncAndLimits(definition, {
          health: { sampleInterval: "5 seconds" },
          projections: [],
          limits: TestReplica.defaultLimits
        }).pipe(Layer.provide(Handler))
      )
    ))

  test.effect("commits a concurrent write burst on an idle replica", () =>
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
