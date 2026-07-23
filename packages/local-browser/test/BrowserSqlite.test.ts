import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as BrowserSqlite from "../src/BrowserSqlite.js"

type WorkerRequest =
  | readonly [id: number, sql: string, params: ReadonlyArray<unknown>]
  | readonly [operation: string, ...args: ReadonlyArray<unknown>]

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
    close: () => {
      databasePort.close()
      workerPort.close()
    },
    startCalls: () => startCalls,
    messageListeners: () => messageListeners
  }
}

describe("BrowserSqlite", () => {
  it.effect("binds boolean query parameters as SQLite integers", () => {
    const channel = new MessageChannel()
    const requests: Array<WorkerRequest> = []
    channel.port2.addEventListener("message", (event) => {
      const request = event.data as WorkerRequest
      requests.push(request)
      if (typeof request[0] !== "number") return
      const [id, , params] = request
      channel.port2.postMessage([
        id,
        undefined,
        [["truthy", "falsy", "textValue", "numberValue", "nullValue"], [params]]
      ])
    })
    channel.port2.start()
    channel.port2.postMessage(["ready", undefined, undefined])

    return Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const rows = yield* sql`
        SELECT ${true} AS truthy,
               ${false} AS falsy,
               ${"text"} AS textValue,
               ${42} AS numberValue,
               ${null} AS nullValue
      `
      assert.deepStrictEqual(rows, [{
        truthy: 1,
        falsy: 0,
        textValue: "text",
        numberValue: 42,
        nullValue: null
      }])
      assert.deepStrictEqual(requests[0]?.[2], [1, 0, "text", 42, null])
    }).pipe(
      Effect.provide(BrowserSqlite.layerMessagePort(channel.port1)),
      Effect.ensuring(Effect.sync(() => {
        channel.port1.close()
        channel.port2.close()
      }))
    )
  })

  it.effect("starts the provided database port", () =>
    Effect.gen(function*() {
      const db = makeDatabaseChannel()
      yield* Effect.scoped(Layer.build(BrowserSqlite.layerMessagePort(db.databasePort))).pipe(
        Effect.ensuring(Effect.sync(db.close))
      )
      assert.strictEqual(db.startCalls(), 1)
    }))

  it.effect("binds each composition to its own database port", () =>
    Effect.gen(function*() {
      const first = makeDatabaseChannel()
      const second = makeDatabaseChannel()
      const combined = Layer.merge(
        BrowserSqlite.layerMessagePort(first.databasePort),
        BrowserSqlite.layerMessagePort(second.databasePort)
      )
      yield* Effect.scoped(Layer.build(combined)).pipe(
        Effect.ensuring(Effect.sync(() => {
          first.close()
          second.close()
        }))
      )
      assert.isAtLeast(first.messageListeners(), 1)
      assert.isAtLeast(second.messageListeners(), 1)
    }))
})
