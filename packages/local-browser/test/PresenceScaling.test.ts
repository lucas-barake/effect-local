import { NodeCrypto } from "@effect/platform-node"
import * as Identity from "@lucas-barake/effect-local/Identity"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { assert, describe, it } from "vitest"

// Per-update cost must not grow with the number of tracked peers. Timing that is too noisy to assert, so
// this counts the peer map operations a fixed churn performs instead, which is exact and machine
// independent. A copy-on-write peer map makes the count grow with the resident count; an in place map does
// not. The counting subclass has to stay installed for the whole run, because a copy-on-write map replaces
// itself on every write; `counting` gates the counter so only the measured churn is recorded, and this file
// runs in its own vitest worker, which holds because vitest defaults to `pool: "forks"` with
// `isolate: true` and neither is overridden in any vitest config here.
const NativeMap = globalThis.Map
let counting = false
let operations = 0

class CountingMap<K, V,> extends NativeMap<K, V> {
  override set(key: K, value: V): this {
    if (counting) operations++
    return super.set(key, value) as this
  }
  override delete(key: K): boolean {
    if (counting) operations++
    return super.delete(key)
  }
  override get(key: K): V | undefined {
    if (counting) operations++
    return super.get(key)
  }
  // `new Map(current)` copies by iterating the source, so this is what makes a full-map clone visible.
  override [Symbol.iterator](): MapIterator<[K, V]> {
    const inner = super[Symbol.iterator]()
    return {
      [Symbol.iterator]() {
        return this
      },
      next: () => {
        if (counting) operations++
        return inner.next()
      }
    } as MapIterator<[K, V]>
  }
}

const Presence = await import("../src/Presence.js")

const Payload = Schema.Struct({ cursor: Schema.Number })
const writes = 200

const seed = (residents: number) =>
  Effect.gen(function*() {
    const presence = yield* Presence.make(Payload, { timeToLive: "1 hour" })
    for (let index = 0; index < residents; index++) {
      yield* presence.receive(yield* Identity.makePeerId, { cursor: 0 })
    }
    const target = yield* Identity.makePeerId
    const scoped = yield* Identity.makePeerId
    return Effect.gen(function*() {
      for (let index = 0; index < writes; index++) {
        yield* presence.receive(target, { cursor: index })
        yield* presence.remove(target)
        yield* Effect.scoped(presence.publish(scoped, { cursor: index }))
      }
    })
  })
;(globalThis as { Map: unknown }).Map = CountingMap

const mapOperationsForChurn = Effect.gen(function*() {
  const churns = yield* Effect.forEach([250, 4_000] as const, seed)
  const counted: Array<number> = []
  for (const churn of churns) {
    yield* churn
    operations = 0
    counting = true
    yield* churn
    counting = false
    counted.push(operations)
  }
  return counted
}).pipe(Effect.provide(NodeCrypto.layer))

describe("Presence write cost", () => {
  it("keeps peer map work per write independent of the resident peer count", async () => {
    const [small, large] = await Effect.runPromise(mapOperationsForChurn)
    // Guards against the counter silently observing nothing, which would make the equality vacuous.
    assert.isAbove(small!, 0)
    assert.strictEqual(
      large,
      small,
      `growing residents 250 -> 4000 changed peer map work for ${writes} writes: ${small} -> ${large}`
    )
  })
})
