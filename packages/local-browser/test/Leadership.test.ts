import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import { TestClock } from "effect/testing"
import * as leadership from "../src/internal/leadership.js"
import * as Wire from "../src/internal/multiTabWire.js"
import * as testKit from "./multiTabKit.js"

interface Promotion {
  readonly tab: string
  readonly epoch: number
}

const startTab = Effect.fnUntraced(function*(
  kit: testKit.MemoryPlatform,
  tab: string,
  promotions: Queue.Queue<Promotion>,
  options?: {
    readonly demotions?: Queue.Queue<string>
    readonly build?: Effect.Effect<boolean>
  }
) {
  const connection = yield* kit.tabChannel.open("leadership-test")
  const control = yield* leadership.makeLeadership({
    name: "db",
    tabId: Wire.TabId.make(tab),
    connection,
    retryDelay: 500,
    whileLeader: Effect.fnUntraced(function*(epoch) {
      const built = yield* options?.build ?? Effect.succeed(true)
      if (!built) return false
      yield* Queue.offer(promotions, { tab, epoch })
      if (options?.demotions !== undefined) {
        const demotions = options.demotions
        yield* Effect.addFinalizer(() => Queue.offer(demotions, tab))
      }
      return true
    })
  }).pipe(Effect.provide(kit.layerAll))
  return control
})

describe("leadership", () => {
  it.effect(
    "the first tab acquires leadership with the initial epoch",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const promotions = yield* Queue.make<Promotion>()
      yield* startTab(kit, "a", promotions)
      const first = yield* Queue.take(promotions)
      assert.deepStrictEqual(first, { tab: "a", epoch: 1 })
    }, Effect.scoped)
  )

  it.effect(
    "a queued tab promotes with a higher epoch when the leader scope closes",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const promotions = yield* Queue.make<Promotion>()
      const scopeA = yield* Scope.make()
      yield* Scope.provide(startTab(kit, "a", promotions), scopeA)
      const first = yield* Queue.take(promotions)
      assert.deepStrictEqual(first, { tab: "a", epoch: 1 })
      yield* startTab(kit, "b", promotions)
      yield* Scope.close(scopeA, Exit.void)
      const second = yield* Queue.take(promotions)
      assert.deepStrictEqual(second, { tab: "b", epoch: 2 })
    }, Effect.scoped)
  )

  it.effect(
    "a steal preempts a live leader and tears its lease down",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const promotions = yield* Queue.make<Promotion>()
      const demotions = yield* Queue.make<string>()
      yield* startTab(kit, "a", promotions, { demotions })
      const first = yield* Queue.take(promotions)
      assert.strictEqual(first.tab, "a")
      const b = yield* startTab(kit, "b", promotions, { demotions })
      yield* b.requestSteal
      const second = yield* Queue.take(promotions)
      assert.deepStrictEqual(second, { tab: "b", epoch: 2 })
      const demoted = yield* Queue.take(demotions)
      assert.strictEqual(demoted, "a")
    }, Effect.scoped)
  )

  it.effect(
    "a failed owner build releases leadership to the next tab",
    Effect.fnUntraced(function*() {
      const kit = yield* testKit.makeMemoryPlatform
      const promotions = yield* Queue.make<Promotion>()
      yield* startTab(kit, "a", promotions, { build: Effect.succeed(false) })
      yield* startTab(kit, "b", promotions)
      yield* TestClock.adjust(1)
      const promoted = yield* Queue.take(promotions)
      assert.strictEqual(promoted.tab, "b")
    }, Effect.scoped)
  )
})
