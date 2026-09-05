import * as SqliteClient from "@effect/sql-sqlite-wasm/SqliteClient"
import { assert, describe, it } from "@effect/vitest"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as BrowserSqlite from "../src/BrowserSqlite.js"

/**
 * A stand in for the browser Worker: the test owns the far end of a Node
 * MessageChannel and speaks the SqliteClient wire protocol (ready handshake,
 * empty query results, close).
 */
const makeFakeWorker = Effect.fnUntraced(function*() {
  const channel = new MessageChannel()
  const closed = yield* Deferred.make<void>()
  let spawned = 0
  let terminated = 0
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
  channel.port2.postMessage(["ready"])
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
      if (Array.isArray(transferOrOptions)) {
        channel.port1.postMessage(message, transferOrOptions)
      } else {
        channel.port1.postMessage(message, transferOrOptions)
      }
    },
    terminate: () => {
      terminated += 1
    },
    addEventListener: (
      type: string,
      listener: EventListenerOrEventListenerObject,
      options?: AddEventListenerOptions | boolean
    ) => {
      channel.port1.addEventListener(type, listener, options)
      channel.port1.start()
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
    return worker
  }
  return {
    spawn,
    closed,
    spawnCount: () => spawned,
    terminateCount: () => terminated
  }
})

describe("BrowserSqlite.layerWorker", () => {
  it.effect(
    "spawns the worker once, completes the ready handshake, and closes then terminates on release",
    Effect.fnUntraced(function*() {
      const fake = yield* makeFakeWorker()
      const scope = yield* Effect.scope
      const built = yield* Effect.scoped(
        Effect.gen(function*() {
          const context = yield* Layer.build(BrowserSqlite.layerWorker(fake.spawn))
          assert.strictEqual(fake.spawnCount(), 1)
          assert.strictEqual(fake.terminateCount(), 0)
          return context.mapUnsafe.has(SqliteClient.SqliteClient.key)
        })
      )
      assert.isTrue(built)
      yield* Deferred.await(fake.closed)
      assert.strictEqual(fake.spawnCount(), 1)
      assert.strictEqual(fake.terminateCount(), 1)
      void scope
    }, Effect.scoped)
  )
})
