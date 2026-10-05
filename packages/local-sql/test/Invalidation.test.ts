import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Logger from "effect/Logger"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as References from "effect/References"
import * as Invalidation from "../src/internal/invalidation.js"

const captureLogs = () => {
  const errors: Array<{ readonly message: unknown; readonly key: unknown; readonly defect: boolean }> = []
  const logger = Logger.make<unknown, void>((entry) => {
    if (entry.logLevel !== "Error") return
    let message: unknown = entry.message
    if (Array.isArray(message)) message = message[0]
    errors.push({
      message,
      key: entry.fiber.getRef(References.CurrentLogAnnotations)["reactivity.key"],
      defect: Cause.hasDies(entry.cause)
    })
  })
  return { layerLogs: Logger.layer([logger]), errors: () => errors }
}

const provideReactivity = Effect.provide(Reactivity.layer)

describe("Invalidation.notify", () => {
  it.effect(
    "completes, logs the defect once with its key, and still notifies the other keys when a subscriber throws",
    Effect.fnUntraced(function*() {
      const logs = captureLogs()
      const notified: Array<string> = []
      const exit = yield* Effect.gen(function*() {
        const reactivity = yield* Reactivity.Reactivity
        reactivity.registerUnsafe(["first"], () => {
          notified.push("first, registered before the subscriber that throws")
        })
        reactivity.registerUnsafe(["first"], () => {
          decodeURIComponent("%")
        })
        reactivity.registerUnsafe(["second"], () => {
          notified.push("second")
        })
        reactivity.registerUnsafe(["third"], () => {
          notified.push("third")
        })
        return yield* Invalidation.notify(reactivity, ["first", "second", "third"]).pipe(Effect.exit)
      }).pipe(Effect.provide(Layer.merge(Reactivity.layer, logs.layerLogs)))

      assert.isTrue(Exit.isSuccess(exit), "the notification completed")
      assert.deepStrictEqual(notified, ["first, registered before the subscriber that throws", "second", "third"])
      assert.deepStrictEqual(logs.errors(), [{ message: "Reactivity subscriber died", key: "first", defect: true }])
    })
  )

  it.effect(
    "delivers each key once when a batch of the caller ends",
    Effect.fnUntraced(function*() {
      const reactivity = yield* Reactivity.Reactivity
      let notifications = 0
      reactivity.registerUnsafe(["key"], () => {
        notifications += 1
      })

      const insideBatch = yield* Invalidation.notify(reactivity, ["key"]).pipe(
        Effect.andThen(Invalidation.notify(reactivity, ["key"])),
        Effect.andThen(reactivity.invalidate(["key"])),
        Effect.map(() => notifications),
        reactivity.withBatch
      )

      assert.strictEqual(insideBatch, 0)
      assert.strictEqual(notifications, 1)
    }, provideReactivity)
  )

  it.effect(
    "leaves the defect of a subscriber to the batch of the caller that delivers it",
    Effect.fnUntraced(function*() {
      const logs = captureLogs()
      const outcome = yield* Effect.gen(function*() {
        const reactivity = yield* Reactivity.Reactivity
        reactivity.registerUnsafe(["key"], () => {
          decodeURIComponent("%")
        })
        let notified: Exit.Exit<void> | undefined
        const batch = yield* Invalidation.notify(reactivity, ["key"]).pipe(
          Effect.exit,
          Effect.map((exit) => {
            notified = exit
          }),
          reactivity.withBatch,
          Effect.exit
        )
        return { notified, batch }
      }).pipe(Effect.provide(Layer.merge(Reactivity.layer, logs.layerLogs)))

      assert.isTrue(outcome.notified !== undefined && Exit.isSuccess(outcome.notified), "the notification completed")
      assert.isTrue(Exit.isFailure(outcome.batch), "the batch of the caller ended with the defect")
      assert.deepStrictEqual(logs.errors(), [])
    })
  )

  it.effect(
    "flush delivers before a batch of the caller ends",
    Effect.fnUntraced(function*() {
      const reactivity = yield* Reactivity.Reactivity
      let notifications = 0
      reactivity.registerUnsafe(["key"], () => {
        notifications += 1
      })

      const insideBatch = yield* Invalidation.flush(reactivity, ["key"]).pipe(
        Effect.map(() => notifications),
        reactivity.withBatch
      )

      assert.strictEqual(insideBatch, 1)
      assert.strictEqual(notifications, 1)
    }, provideReactivity)
  )

  it.effect(
    "flush completes and logs the defect with its key when a subscriber throws",
    Effect.fnUntraced(function*() {
      const logs = captureLogs()
      const notified: Array<string> = []
      const exit = yield* Effect.gen(function*() {
        const reactivity = yield* Reactivity.Reactivity
        reactivity.registerUnsafe(["first"], () => {
          decodeURIComponent("%")
        })
        reactivity.registerUnsafe(["second"], () => {
          notified.push("second")
        })
        return yield* Invalidation.flush(reactivity, ["first", "second", "second"]).pipe(Effect.exit)
      }).pipe(Effect.provide(Layer.merge(Reactivity.layer, logs.layerLogs)))

      assert.isTrue(Exit.isSuccess(exit), "the notification completed")
      assert.deepStrictEqual(notified, ["second"])
      assert.deepStrictEqual(logs.errors(), [{ message: "Reactivity subscriber died", key: "first", defect: true }])
    })
  )

  it.effect(
    "notifies a key that is listed twice once",
    Effect.fnUntraced(function*() {
      const reactivity = yield* Reactivity.Reactivity
      let notifications = 0
      reactivity.registerUnsafe(["key"], () => {
        notifications += 1
      })

      yield* Invalidation.notify(reactivity, ["key", "key"])

      assert.strictEqual(notifications, 1)
    }, provideReactivity)
  )

  it.effect(
    "stops when the caller is interrupted while a notification is being delivered",
    Effect.fnUntraced(function*() {
      const base = yield* Reactivity.make
      const reached = yield* Deferred.make<void>()
      const reactivity: Reactivity.Reactivity = {
        ...base,
        invalidate: (keys) => {
          if (!Array.isArray(keys) || !keys.includes("second")) return base.invalidate(keys)
          return Effect.andThen(Deferred.succeed(reached, undefined), Effect.never)
        }
      }
      const notified: Array<string> = []
      for (const key of ["first", "second", "third"]) {
        reactivity.registerUnsafe([key], () => {
          notified.push(key)
        })
      }
      const notifying = yield* Invalidation.notify(reactivity, ["first", "second", "third"]).pipe(
        Effect.forkChild({ startImmediately: true })
      )
      yield* Deferred.await(reached)

      const exit = yield* Fiber.interrupt(notifying).pipe(Effect.andThen(Fiber.await(notifying)))

      assert.isTrue(Exit.hasInterrupts(exit), "the notification ended with the interruption")
      assert.deepStrictEqual(notified, ["first"])
    })
  )

  it.effect(
    "ends with the interruption and logs nothing when the Reactivity service interrupts the notification",
    Effect.fnUntraced(function*() {
      const logs = captureLogs()
      const base = yield* Reactivity.make
      const reactivity: Reactivity.Reactivity = { ...base, invalidate: () => Effect.interrupt }

      const exit = yield* Invalidation.notify(reactivity, ["key"]).pipe(Effect.exit, Effect.provide(logs.layerLogs))

      assert.isTrue(Exit.hasInterrupts(exit), "the notification ended with the interruption")
      assert.deepStrictEqual(logs.errors(), [])
    })
  )
})
