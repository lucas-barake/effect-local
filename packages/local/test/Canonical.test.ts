import { NodeCrypto } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Canonical from "../src/Canonical.js"

describe("Canonical", () => {
  it("orders object keys independently of declaration order", () => {
    assert.strictEqual(Canonical.stringify({ b: 2, a: 1 }), Canonical.stringify({ a: 1, b: 2 }))
    assert.strictEqual(Canonical.hash({ b: 2, a: 1 }), Canonical.hash({ a: 1, b: 2 }))
  })

  it.effect("computes a stable SHA-256 digest", () =>
    Effect.gen(function*() {
      const first = yield* Canonical.digest({ b: 2, a: 1 })
      const second = yield* Canonical.digest({ a: 1, b: 2 })
      assert.strictEqual(first, second)
      assert.strictEqual(first.length, 64)
    }).pipe(Effect.provide(NodeCrypto.layer)))
})
