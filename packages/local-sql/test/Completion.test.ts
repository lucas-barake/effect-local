import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Logger from "effect/Logger"
import * as Completion from "../src/internal/completion.js"

const captureErrors = () => {
  const messages: Array<string> = []
  const logger = Logger.make<unknown, void>((entry) => {
    if (entry.logLevel !== "Error") return
    let message: unknown = entry.message
    if (Array.isArray(message)) message = message[0]
    messages.push(String(message))
  })
  return { layerLogs: Logger.layer([logger]), messages: () => messages }
}

const forkWaiter = <A,>(completion: Completion.Completion<A>) =>
  Completion.wait(completion).pipe(Effect.forkChild({ startImmediately: true }))

describe("a completion", () => {
  it.effect(
    "forgets a waiter that is interrupted and settles the others",
    Effect.fnUntraced(function*() {
      const completion = Completion.make<number>()
      const interrupted = yield* forkWaiter(completion)
      const kept = yield* forkWaiter(completion)
      const registered = completion.waiters.size
      yield* Fiber.interrupt(interrupted)
      const afterInterrupt = completion.waiters.size
      yield* Completion.settle(completion, Exit.succeed(1))

      assert.deepStrictEqual({ registered, afterInterrupt, left: completion.waiters.size }, {
        registered: 2,
        afterInterrupt: 1,
        left: 0
      })
      assert.strictEqual(yield* Fiber.join(kept), 1)
    })
  )

  it.effect(
    "gives the first exit to a waiter that arrives after it settled, and ignores a second settle",
    Effect.fnUntraced(function*() {
      const completion = Completion.make<number>()
      const early = yield* forkWaiter(completion)
      yield* Completion.settle(completion, Exit.succeed(1))
      yield* Completion.settle(completion, Exit.succeed(2))
      const late = yield* Completion.wait(completion)

      assert.deepStrictEqual([yield* Fiber.join(early), late], [1, 1])
    })
  )

  it.effect(
    "resumes every waiter in registration order when the callbacks of some throw",
    Effect.fnUntraced(function*() {
      const logs = captureErrors()
      const completion = Completion.make<void>()
      const resumed: Array<number> = []
      for (let index = 0; index < 10; index++) {
        const waiter = yield* Completion.wait(completion).pipe(
          Effect.map(() => resumed.push(index)),
          Effect.forkChild({ startImmediately: true })
        )
        waiter.addObserver(() => {
          if (index % 3 === 0) decodeURIComponent("%")
        })
      }
      yield* Completion.settle(completion, Exit.void).pipe(Effect.provide(logs.layerLogs))

      assert.deepStrictEqual(resumed, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      assert.deepStrictEqual(logs.messages(), Array.from({ length: 4 }, () => "Completion callback died"))
    })
  )

  it.effect(
    "resumes every waiter when the fiber that settles it is interrupted after the first waiter resumed",
    Effect.fnUntraced(function*() {
      const completion = Completion.make<void>()
      const settler: { fiber: Fiber.Fiber<void> | undefined } = { fiber: undefined }
      const first = yield* Completion.wait(completion).pipe(
        Effect.map(() => settler.fiber?.interruptUnsafe()),
        Effect.forkChild({ startImmediately: true })
      )
      const others = yield* Effect.forEach(Array.from({ length: 5 }), () => forkWaiter(completion))
      settler.fiber = yield* Effect.forkChild(Completion.settle(completion, Exit.void))
      yield* Fiber.await(settler.fiber)

      const stranded = [first, ...others].filter((waiter) => waiter.pollUnsafe() === undefined).length

      assert.strictEqual(stranded, 0)
      assert.strictEqual(completion.waiters.size, 0)
    })
  )
})
