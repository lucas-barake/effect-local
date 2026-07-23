import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as BrowserSqlite from "../src/BrowserSqlite.js"

const makeDatabaseChannel = () => {
  const channel = new MessageChannel()
  const databasePort = channel.port1
  const workerPort = channel.port2

  let startCalls = 0
  const originalStart = databasePort.start.bind(databasePort)
  databasePort.start = () => {
    startCalls += 1
    originalStart()
  }

  let messageListeners = 0
  const originalAddEventListener = databasePort.addEventListener.bind(databasePort)
  databasePort.addEventListener = ((type: string, ...rest: Array<unknown>) => {
    if (type === "message") messageListeners += 1
    return (originalAddEventListener as (...args: Array<unknown>) => unknown)(type, ...rest)
  }) as MessagePort["addEventListener"]

  workerPort.addEventListener("message", () => {})
  workerPort.postMessage(["ready", undefined, undefined])

  return {
    databasePort,
    startCalls: () => startCalls,
    messageListeners: () => messageListeners
  }
}

describe("BrowserSqlite", () => {
  it.effect("starts the provided database port", () =>
    Effect.gen(function*() {
      const db = makeDatabaseChannel()
      yield* Effect.scoped(Layer.build(BrowserSqlite.layerMessagePort(db.databasePort)))
      assert.isAtLeast(db.startCalls(), 1)
    }))

  it.effect("binds each composition to its own database port", () =>
    Effect.gen(function*() {
      const first = makeDatabaseChannel()
      const second = makeDatabaseChannel()
      const combined = Layer.merge(
        BrowserSqlite.layerMessagePort(first.databasePort),
        BrowserSqlite.layerMessagePort(second.databasePort)
      )
      yield* Effect.scoped(Layer.build(combined))
      assert.isAtLeast(first.messageListeners(), 1)
      assert.isAtLeast(second.messageListeners(), 1)
    }))
})
