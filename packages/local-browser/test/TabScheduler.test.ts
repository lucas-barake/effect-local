import { assert, describe, it, vi } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import type * as Scheduler from "effect/Scheduler"
import * as Scope from "effect/Scope"
import * as TabScheduler from "../src/internal/tabScheduler.js"

const dispatch = (scheduler: Scheduler.Scheduler, onRun: () => void) =>
  Effect.callback<string>((resume) => {
    scheduler.makeDispatcher().scheduleTask(() => {
      onRun()
      resume(Effect.succeed("ran"))
    }, 0)
  })

describe("TabScheduler", () => {
  it.effect(
    "runs dispatched fiber work while the page's timers do not fire",
    Effect.fnUntraced(function*() {
      const scheduler = yield* TabScheduler.make
      vi.useFakeTimers({ toFake: ["setTimeout", "setImmediate", "setInterval"] })
      const outcome = yield* dispatch(scheduler, () => vi.useRealTimers()).pipe(
        Effect.ensuring(Effect.sync(() => vi.useRealTimers()))
      )
      assert.strictEqual(outcome, "ran")
    }, Effect.scoped)
  )

  it.effect(
    "keeps dispatching after its scope closes",
    Effect.fnUntraced(function*() {
      const scope = yield* Scope.make()
      const scheduler = yield* TabScheduler.make.pipe(Scope.provide(scope))
      yield* Scope.close(scope, Exit.void)
      assert.strictEqual(yield* dispatch(scheduler, () => undefined), "ran")
    })
  )
})
