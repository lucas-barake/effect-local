import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Result from "effect/Result"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as BrowserSqlite from "../src/BrowserSqlite.js"

type WorkerRequest =
  | readonly [id: number, sql: string, params: ReadonlyArray<unknown>]
  | readonly [operation: string, ...args: ReadonlyArray<unknown>]

class FirstSql extends Context.Service<FirstSql, SqlClient.SqlClient>()("test/BrowserSqlite/FirstSql") {}
class SecondSql extends Context.Service<SecondSql, SqlClient.SqlClient>()("test/BrowserSqlite/SecondSql") {}

const makeDatabaseChannel = (database: string = "database") => {
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

  let removedMessageListeners = 0
  const originalRemoveEventListener = databasePort.removeEventListener.bind(databasePort)
  databasePort.removeEventListener = ((type: string, ...rest: Array<unknown>) => {
    if (type === "message") removedMessageListeners += 1
    return (originalRemoveEventListener as (...args: Array<unknown>) => unknown)(type, ...rest)
  }) as MessagePort["removeEventListener"]

  let resolveClosed!: () => void
  const closed = new Promise<void>((resolve) => {
    resolveClosed = resolve
  })
  workerPort.addEventListener("message", (event) => {
    const request = event.data as WorkerRequest
    if (request[0] === "close") {
      resolveClosed()
      return
    }
    if (typeof request[0] !== "number") return
    workerPort.postMessage([
      request[0],
      undefined,
      [["database", "sql"], [[database, request[1]]]]
    ])
  })
  workerPort.start()
  workerPort.postMessage(["ready", undefined, undefined])

  return {
    databasePort,
    closed,
    close: () => {
      databasePort.close()
      workerPort.close()
    },
    removedMessageListeners: () => removedMessageListeners,
    startCalls: () => startCalls,
    messageListeners: () => messageListeners
  }
}

const captureClients = <E, R,>(database: Layer.Layer<SqlClient.SqlClient, E, R>) =>
  Layer.merge(
    Layer.effect(FirstSql, SqlClient.SqlClient).pipe(Layer.provide(database)),
    Layer.effect(SecondSql, SqlClient.SqlClient).pipe(Layer.provide(database))
  )

const assertReusedLayer = <E, R,>(
  database: Layer.Layer<SqlClient.SqlClient, E, R>,
  db: ReturnType<typeof makeDatabaseChannel>
) =>
  Effect.gen(function*() {
    const first = yield* FirstSql
    const second = yield* SecondSql
    const [firstRows, secondRows] = yield* Effect.all([
      first`SELECT 1 AS value`,
      second`SELECT 2 AS value`
    ], { concurrency: "unbounded" })

    assert.strictEqual(first, second)
    assert.deepStrictEqual(firstRows, [{ database: "shared", sql: "SELECT 1 AS value" }])
    assert.deepStrictEqual(secondRows, [{ database: "shared", sql: "SELECT 2 AS value" }])
    assert.strictEqual(db.startCalls(), 1)
    assert.strictEqual(db.messageListeners(), 1)
  }).pipe(
    Effect.provide(captureClients(database)),
    Effect.ensuring(Effect.sync(db.close))
  )

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

  it.effect("binds each client to its own database port", () => {
    const first = makeDatabaseChannel("first")
    const second = makeDatabaseChannel("second")
    const clients = Layer.merge(
      Layer.effect(FirstSql, SqlClient.SqlClient).pipe(
        Layer.provide(BrowserSqlite.layerMessagePort(first.databasePort))
      ),
      Layer.effect(SecondSql, SqlClient.SqlClient).pipe(
        Layer.provide(BrowserSqlite.layerMessagePort(second.databasePort))
      )
    )

    return Effect.gen(function*() {
      const firstSql = yield* FirstSql
      const secondSql = yield* SecondSql
      const [firstRows, secondRows] = yield* Effect.all([
        firstSql`SELECT 1 AS value`,
        secondSql`SELECT 2 AS value`
      ], { concurrency: "unbounded" })

      assert.deepStrictEqual(firstRows, [{ database: "first", sql: "SELECT 1 AS value" }])
      assert.deepStrictEqual(secondRows, [{ database: "second", sql: "SELECT 2 AS value" }])
    }).pipe(
      Effect.provide(clients),
      Effect.ensuring(Effect.sync(() => {
        first.close()
        second.close()
      }))
    )
  })

  it.effect("memoizes a reused layerMessagePort value", () => {
    const db = makeDatabaseChannel("shared")
    return assertReusedLayer(BrowserSqlite.layerMessagePort(db.databasePort), db)
  })

  it.effect("memoizes a reused public layer value", () => {
    const db = makeDatabaseChannel("shared")
    const database = BrowserSqlite.layer.pipe(
      Layer.provide(Layer.succeed(BrowserSqlite.DatabasePort, db.databasePort))
    )
    return assertReusedLayer(database, db)
  })

  it.effect("exposes database port startup failures", () => {
    const channel = new MessageChannel()
    const failure = new Error("start failed")
    channel.port1.start = () => {
      throw failure
    }

    return Effect.gen(function*() {
      const exit = yield* Effect.scoped(
        Layer.build(BrowserSqlite.layerMessagePort(channel.port1))
      ).pipe(Effect.exit)

      assert.isTrue(Exit.isFailure(exit))
      if (Exit.isFailure(exit)) {
        assert.strictEqual(Result.getOrThrow(Cause.findDefect(exit.cause)), failure)
      }
    }).pipe(
      Effect.ensuring(Effect.sync(() => {
        channel.port1.close()
        channel.port2.close()
      }))
    )
  })

  it.effect("releases the driver listener and closes the worker", () =>
    Effect.gen(function*() {
      const db = makeDatabaseChannel()
      yield* Effect.gen(function*() {
        yield* Effect.scoped(Layer.build(BrowserSqlite.layerMessagePort(db.databasePort)))
        yield* Effect.promise(() => db.closed)
        assert.strictEqual(db.removedMessageListeners(), 1)
      }).pipe(Effect.ensuring(Effect.sync(db.close)))
    }))
})
