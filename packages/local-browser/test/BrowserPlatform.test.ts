import { assert, describe, it } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Scheduler from "effect/Scheduler"
import * as Stream from "effect/Stream"
import * as BrowserReplica from "../src/BrowserReplica.js"
import * as platform from "../src/internal/platform.js"

describe("browser platform", () => {
  it.effect(
    "delivers the tab visibility change a subscription registers at every scheduler yield budget",
    Effect.fnUntraced(function*() {
      const visibility = Context.get(yield* Layer.build(BrowserReplica.layerPlatformBrowser), platform.TabVisibility)
      const lost: Array<number> = []
      for (let budget = 3; budget <= 64; budget++) {
        const subscriber = yield* visibility.changes.pipe(
          Stream.runHead,
          Effect.provideService(Scheduler.MaxOpsBeforeYield, budget),
          Effect.forkChild({ startImmediately: true })
        )
        for (let step = 0; step < 200 && subscriber.pollUnsafe() === undefined; step++) yield* Effect.yieldNow
        const exit = subscriber.pollUnsafe()
        yield* Fiber.interrupt(subscriber)
        if (exit === undefined) lost.push(budget)
        else assert.isTrue(Exit.isSuccess(exit) && Option.isSome(exit.value))
      }
      assert.deepStrictEqual(lost, [])
    })
  )
})
