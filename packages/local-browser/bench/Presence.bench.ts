import { NodeCrypto } from "@effect/platform-node"
import * as Identity from "@lucas-barake/effect-local/Identity"
import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { bench } from "vitest"
import * as Presence from "../src/Presence.js"

// Per-update cost must not grow with the number of tracked peers. Compare the two reported timings: a
// copy-on-write map makes the 4000 peer case scale with the resident count, an in place map does not. The
// deterministic guard for this property is test/PresenceScaling.test.ts, which counts map operations; this
// file is for measuring the wall clock effect.
const Payload = Schema.Struct({ cursor: Schema.Number })
const residentCounts = [250, 4_000] satisfies ReadonlyArray<number>
const iterations = 500
const warmupPasses = [0, 1, 2, 3, 4]

const churns = Effect.runSync(
  Effect.forEach(residentCounts, (residents) =>
    Effect.gen(function*() {
      const presence = yield* Presence.make(Payload, { timeToLive: "1 hour" })
      for (let index = 0; index < residents; index++) {
        yield* presence.receive(yield* Identity.makePeerId, { cursor: 0 })
      }
      const target = yield* Identity.makePeerId
      const scoped = yield* Identity.makePeerId
      return [
        residents,
        Effect.gen(function*() {
          for (let index = 0; index < iterations; index++) {
            yield* presence.receive(target, { cursor: index })
            yield* presence.remove(target)
            yield* Effect.scoped(presence.publish(scoped, { cursor: index }))
          }
        })
      ] satisfies readonly [number, Effect.Effect<void, ReplicaError.ReplicaError>]
    })).pipe(Effect.provide(NodeCrypto.layer))
)

// Warm every case before any case is sampled. vitest runs benches in declaration order in one process, so
// warming each case only against itself makes the first one pay JIT tiering for the whole Effect and Schema
// stack, and the reported ratio between the cases becomes an artifact of that ordering.
Effect.runSync(
  Effect.forEach(warmupPasses, () => Effect.forEach(churns, ([, churn]) => churn))
)

for (const [residents, churn] of churns) {
  bench(
    `presence set, remove, and scoped publish churn with ${residents} resident peers`,
    () => Effect.runPromise(churn),
    {
      iterations: 40,
      time: 3_000,
      warmupIterations: 10,
      warmupTime: 500
    }
  )
}
