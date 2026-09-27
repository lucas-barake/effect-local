import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as ConnectionLane from "../src/ConnectionLane.js"

const makeLane = Effect.fnUntraced(function*(options: ConnectionLane.Options = {}) {
  const context = yield* Layer.build(
    ConnectionLane.makeLayer(options).pipe(
      Layer.provideMerge(SqliteClient.layer({ filename: ":memory:", disableWAL: true })),
      Layer.provide(Reactivity.layer)
    )
  )
  const sql = Context.get(context, SqlClient.SqlClient)
  const lane = Context.get(context, ConnectionLane.ConnectionLane)
  const order: Array<string> = []
  const record = (name: string, priority: ConnectionLane.Priority) =>
    lane.withStatement(Effect.sync(() => order.push(name))).pipe(
      Effect.provideService(ConnectionLane.Priority, priority),
      Effect.forkChild({ startImmediately: true })
    )
  const hold = Effect.gen(function*() {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const holder = yield* lane.withTransaction(
      Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
    ).pipe(
      Effect.provideService(ConnectionLane.Priority, "Background"),
      Effect.forkChild({ startImmediately: true })
    )
    yield* Deferred.await(entered)
    return { holder, release: Deferred.succeed(release, undefined) }
  })
  return { sql, lane, record, order, hold }
})

describe("ConnectionLane", () => {
  it.effect(
    "serves waiting foreground work before waiting background work, each in arrival order",
    Effect.fnUntraced(function*() {
      const { hold, order, record } = yield* makeLane()
      const held = yield* hold
      const waiters = [
        yield* record("background-1", "Background"),
        yield* record("foreground-1", "Foreground"),
        yield* record("background-2", "Background"),
        yield* record("foreground-2", "Foreground")
      ]
      yield* held.release
      yield* Fiber.join(held.holder)
      yield* Fiber.joinAll(waiters)
      assert.deepStrictEqual(order, ["foreground-1", "foreground-2", "background-1", "background-2"])
    })
  )

  it.effect(
    "serves a background waiter in arrival order once it has waited the maximum background wait",
    Effect.fnUntraced(function*() {
      const { hold, order, record } = yield* makeLane({ maximumBackgroundWait: "100 millis" })
      const held = yield* hold
      const background = yield* record("background", "Background")
      yield* TestClock.adjust("100 millis")
      const foreground = yield* record("foreground", "Foreground")
      yield* held.release
      yield* Fiber.joinAll([held.holder, background, foreground])
      assert.deepStrictEqual(order, ["background", "foreground"])
    })
  )

  it.effect(
    "interrupting a waiting acquirer runs nothing and leaves the turn with the holder",
    Effect.fnUntraced(function*() {
      const { hold, lane, order, record } = yield* makeLane()
      const held = yield* hold
      const interrupted = yield* record("interrupted", "Foreground")
      const waiting = yield* record("waiting", "Foreground")
      yield* Fiber.interrupt(interrupted)
      yield* Effect.yieldNow
      assert.deepStrictEqual(order, [])
      assert.isTrue(yield* lane.foregroundWaiting)
      yield* held.release
      yield* Fiber.join(held.holder)
      yield* Fiber.join(waiting)
      assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(interrupted)))
      assert.isFalse(yield* lane.foregroundWaiting)
      assert.deepStrictEqual(order, ["waiting"])
    })
  )

  it.effect(
    "runs nested transactions and statements inside a transaction without taking another turn",
    Effect.fnUntraced(function*() {
      const { lane, sql } = yield* makeLane()
      yield* sql`CREATE TABLE lane_nested (name TEXT NOT NULL)`
      yield* lane.withTransaction(Effect.gen(function*() {
        yield* lane.withTransaction(sql`INSERT INTO lane_nested (name) VALUES ('savepoint')`)
        yield* lane.withStatement(sql`INSERT INTO lane_nested (name) VALUES ('statement')`)
      }))
      const rows = yield* SqlSchema.findAll({
        Request: Schema.Void,
        Result: Schema.Struct({ name: Schema.String }),
        execute: () => sql`SELECT name FROM lane_nested ORDER BY rowid`
      })(undefined)
      assert.deepStrictEqual(rows.map((row) => row.name), ["savepoint", "statement"])
    })
  )
})
