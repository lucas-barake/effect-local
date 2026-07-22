import { NodeCrypto } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as FaultInjection from "../src/FaultInjection.js"

it.layer(NodeCrypto.layer)("FaultInjection", (it) => {
  it.effect("replays the final decision after a deterministic sequence", () =>
    Effect.gen(function*() {
      const faults = yield* FaultInjection.FaultInjection
      const packet = {
        sequence: 0,
        from: (yield* Identity.makePeerId),
        to: (yield* Identity.makePeerId),
        payload: Uint8Array.of(1)
      }
      assert.isTrue((yield* faults.decide(packet)).drop)
      assert.isFalse((yield* faults.decide({ ...packet, sequence: 1 })).drop)
      assert.isFalse((yield* faults.decide({ ...packet, sequence: 2 })).drop)
    }).pipe(Effect.provide(FaultInjection.layerSequence([
      { drop: true, copies: 1, delay: 0, reorder: false },
      { drop: false, copies: 1, delay: 0, reorder: false }
    ]))))
})
