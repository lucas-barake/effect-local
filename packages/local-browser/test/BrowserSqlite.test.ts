import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as BrowserSqlite from "../src/BrowserSqlite.js"

/**
 * A stand in for the browser Worker: the test owns the far end of a Node
 * MessageChannel and speaks the SqliteClient wire protocol (ready handshake,
 * empty query results, close).
 */
const makeFakeWorker = Effect.fnUntraced(function*(startup: "ready" | "silent" = "ready") {
  const channel = new MessageChannel()
  const closed = yield* Deferred.make<void>()
  const spawnedLatch = yield* Deferred.make<void>()
  const errorListenedLatch = yield* Deferred.make<void>()
  const respawnedLatch = yield* Deferred.make<void>()
  let spawned = 0
  const releases: Array<"close" | "terminate"> = []
  channel.port2.addEventListener("message", (event) => {
    const message: unknown = event.data
    if (!Array.isArray(message)) return
    if (message[0] === "close") {
      Deferred.doneUnsafe(closed, Exit.void)
      return
    }
    if (typeof message[0] === "number") channel.port2.postMessage([message[0], undefined, []])
  })
  channel.port2.start()
  if (startup === "ready") channel.port2.postMessage(["ready"])
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      channel.port1.close()
      channel.port2.close()
    })
  )
  const worker: Worker = {
    onmessage: null,
    onmessageerror: null,
    onerror: null,
    postMessage: (message: unknown, transferOrOptions?: Array<Transferable> | StructuredSerializeOptions) => {
      if (Array.isArray(message) && message[0] === "close") releases.push("close")
      if (Array.isArray(transferOrOptions)) {
        channel.port1.postMessage(message, transferOrOptions)
      } else {
        channel.port1.postMessage(message, transferOrOptions)
      }
    },
    terminate: () => {
      releases.push("terminate")
    },
    addEventListener: (
      type: string,
      listener: EventListenerOrEventListenerObject,
      options?: AddEventListenerOptions | boolean
    ) => {
      channel.port1.addEventListener(type, listener, options)
      channel.port1.start()
      if (type === "error") Deferred.doneUnsafe(errorListenedLatch, Exit.void)
    },
    removeEventListener: (
      type: string,
      listener: EventListenerOrEventListenerObject,
      options?: EventListenerOptions | boolean
    ) => {
      channel.port1.removeEventListener(type, listener, options)
    },
    dispatchEvent: (event: Event) => channel.port1.dispatchEvent(event)
  }
  const spawn = () => {
    spawned += 1
    Deferred.doneUnsafe(spawnedLatch, Exit.void)
    if (spawned > 1) Deferred.doneUnsafe(respawnedLatch, Exit.void)
    return worker
  }
  return {
    spawn,
    closed,
    spawned: Deferred.await(spawnedLatch),
    errorListened: Deferred.await(errorListenedLatch),
    respawned: Deferred.await(respawnedLatch),
    ready: Effect.sync(() => channel.port2.postMessage(["ready"])),
    fail: Effect.sync(() => channel.port1.dispatchEvent(new Event("error"))),
    spawnCount: () => spawned,
    releases
  }
})

describe("BrowserSqlite.layerWorker", () => {
  it.effect(
    "fails the database layer with a SqlError when the worker errors before it is ready",
    Effect.fnUntraced(function*() {
      const fake = yield* makeFakeWorker("silent")
      const building = yield* Layer.build(BrowserSqlite.layerWorker(fake.spawn)).pipe(
        Effect.scoped,
        Effect.exit,
        Effect.forkChild
      )
      yield* fake.errorListened
      yield* fake.fail
      const exit = yield* Fiber.join(building)
      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        const failure = Cause.findErrorOption(exit.cause)
        assert.isTrue(Option.isSome(failure) && failure.value._tag === "SqlError")
      }
      assert.deepStrictEqual(fake.releases, ["terminate"])
    }, Effect.scoped)
  )

  it.effect(
    "hands worker errors to the client once the worker is ready",
    Effect.fnUntraced(function*() {
      const fake = yield* makeFakeWorker("silent")
      const building = yield* Layer.build(BrowserSqlite.layerWorker(fake.spawn)).pipe(Effect.forkChild)
      yield* fake.spawned
      yield* fake.ready
      yield* Fiber.join(building)
      yield* fake.fail
      yield* fake.respawned
      assert.strictEqual(fake.spawnCount(), 2)
    }, Effect.scoped)
  )

  it.effect(
    "spawns the worker once, completes the ready handshake, and closes then terminates on release",
    Effect.fnUntraced(function*() {
      const fake = yield* makeFakeWorker()
      const built = yield* Effect.scoped(
        Effect.gen(function*() {
          const context = yield* Layer.build(BrowserSqlite.layerWorker(fake.spawn))
          assert.strictEqual(fake.spawnCount(), 1)
          assert.deepStrictEqual(fake.releases, [])
          return context.mapUnsafe.has(SqliteClient.SqliteClient.key)
        })
      )
      assert.isTrue(built)
      yield* Deferred.await(fake.closed)
      assert.strictEqual(fake.spawnCount(), 1)
      // The close frame must leave before the worker is killed, or the driver
      // never gets the chance to shut the database down cleanly.
      assert.deepStrictEqual(fake.releases, ["close", "terminate"])
    }, Effect.scoped)
  )
})
