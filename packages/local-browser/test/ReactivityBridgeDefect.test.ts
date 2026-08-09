import { assert, describe, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as ReplicaAtom from "../src/ReplicaAtom.js"
import * as ReplicaClient from "../src/ReplicaClient.js"
import { peerConnectionStatus, relayConnectionStatus, replica } from "./fixtures.js"

describe("ReplicaAtom reactivity bridge", () => {
  it.effect("resubscribes after the invalidation stream dies", () =>
    Effect.gen(function*() {
      const reactivity = yield* Reactivity.make
      const consumed = yield* Deferred.make<void>()
      const died = yield* Deferred.make<void>()
      let subscriptions = 0
      let invalidations = 0
      reactivity.registerUnsafe(["defect-key"], () => invalidations++)
      const client: ReplicaClient.ReplicaClient["Service"] = {
        ...replica,
        ownerEpoch: "owner",
        peerConnectionStatus,
        relayConnectionStatus,
        invalidations: Stream.unwrap(Effect.sync(() => {
          subscriptions++
          return subscriptions < 2
            ? Stream.fromEffect(Deferred.succeed(died, undefined)).pipe(
              Stream.flatMap(() => Stream.die(new TypeError("owner defect")))
            )
            : Stream.make({
              _tag: "Invalidation" as const,
              ownerEpoch: "owner",
              sequence: Identity.CommitSequence.make(1),
              keys: ["defect-key"]
            }).pipe(Stream.tap(() => Deferred.succeed(consumed, undefined)))
        }))
      }
      yield* Effect.scoped(
        Effect.gen(function*() {
          yield* Layer.build(ReplicaAtom.layerReactivity)
          yield* Deferred.await(died)
          yield* TestClock.adjust(1_000)
          assert.strictEqual(subscriptions, 2, "the bridge must resubscribe after a defect")
          yield* Deferred.await(consumed)
          assert.strictEqual(invalidations, 1)
        }).pipe(
          Effect.provideService(ReplicaClient.ReplicaClient, client),
          Effect.provideService(Reactivity.Reactivity, reactivity)
        )
      )
    }))
})
