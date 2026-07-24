import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as BrowserSqlite from "../src/BrowserSqlite.js"

type WorkerRequest =
  | readonly [id: number, sql: string, params: ReadonlyArray<unknown>]
  | readonly [operation: string, ...args: ReadonlyArray<unknown>]

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
})
