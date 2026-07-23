import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as ReplicaLimits from "@lucas-barake/effect-local/ReplicaLimits"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { TestClock } from "effect/testing"
import * as TestReplica from "../src/TestReplica.js"
import { definition, Rename } from "./fixtures.js"

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
})
