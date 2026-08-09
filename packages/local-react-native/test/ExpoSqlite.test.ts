import { assert, describe, it } from "@effect/vitest"
import * as Config from "effect/Config"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Result from "effect/Result"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Client from "effect/unstable/sql/SqlClient"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as ExpoSqlite from "../src/ExpoSqlite.js"
import * as ExpoSqliteNode from "./helpers/ExpoSqliteNode.js"

describe("ExpoSqlite", () => {
  const memory = ExpoSqlite.layer({ databaseName: ":memory:" })

  it.effect("roundtrips rows through the generic SqlClient", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, done INTEGER NOT NULL)`
      yield* sql`INSERT INTO tasks (title, done) VALUES (${"write tests"}, ${true})`
      const rows = yield* sql<{ readonly id: number; readonly title: string; readonly done: number }>`
        SELECT id, title, done FROM tasks
      `
      assert.deepStrictEqual(rows, [{ id: 1, title: "write tests", done: 1 }])
    }).pipe(Effect.provide(memory)))

  it.effect("binds booleans, undefined, and Uint8Array blobs through the production param path", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE binds (flag INTEGER, note TEXT, payload BLOB)`
      yield* sql`INSERT INTO binds (flag, note, payload) VALUES (${false}, ${undefined}, ${new Uint8Array([1, 2, 3])})`
      const rows = yield* sql<{ readonly flag: number; readonly note: string | null; readonly payload: Uint8Array }>`
        SELECT flag, note, payload FROM binds
      `
      assert.strictEqual(rows.length, 1)
      assert.strictEqual(rows[0].flag, 0)
      assert.strictEqual(rows[0].note, null)
      assert.deepStrictEqual([...rows[0].payload], [1, 2, 3])
    }).pipe(Effect.provide(memory)))

  it.effect("rejects bigint binds outside the safe integer range with SqlError", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      const result = yield* Effect.result(sql`SELECT ${BigInt("9007199254740993")}`)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "SqlError")
    }).pipe(Effect.provide(memory)))

  it.effect("returns raw value arrays through values and valuesUnprepared", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE pairs (a INTEGER, b TEXT)`
      yield* sql`INSERT INTO pairs (a, b) VALUES (${1}, ${"one"}), (${2}, ${"two"})`
      const values = yield* sql`SELECT a, b FROM pairs ORDER BY a`.values
      assert.deepStrictEqual(values, [[1, "one"], [2, "two"]])
      const unprepared = yield* sql`SELECT a, b FROM pairs ORDER BY a DESC`.valuesUnprepared
      assert.deepStrictEqual(unprepared, [[2, "two"], [1, "one"]])
    }).pipe(Effect.provide(memory)))

  it.effect("streams rows through executeStream", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE numbers (n INTEGER)`
      yield* sql`INSERT INTO numbers (n) VALUES (1), (2), (3)`
      const rows = yield* Stream.runCollect(sql`SELECT n FROM numbers ORDER BY n`.stream)
      assert.deepStrictEqual([...rows], [{ n: 1 }, { n: 2 }, { n: 3 }])
    }).pipe(Effect.provide(memory)))

  it.effect("commits successful transactions and rolls back failed ones", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE events (id INTEGER)`
      yield* sql.withTransaction(sql`INSERT INTO events (id) VALUES (1)`)
      const committed = yield* sql<{ readonly id: number }>`SELECT id FROM events`
      assert.deepStrictEqual(committed, [{ id: 1 }])
      const result = yield* Effect.result(sql.withTransaction(Effect.gen(function*() {
        yield* sql`INSERT INTO events (id) VALUES (2)`
        return yield* Effect.fail(new SimulatedFailure())
      })))
      assert.isTrue(Result.isFailure(result))
      const after = yield* sql<{ readonly id: number }>`SELECT id FROM events`
      assert.deepStrictEqual(after, [{ id: 1 }])
    }).pipe(Effect.provide(memory)))

  it.effect("rolls back only the inner savepoint of a nested transaction", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE events (id INTEGER)`
      const result = yield* Effect.result(sql.withTransaction(Effect.gen(function*() {
        yield* sql`INSERT INTO events (id) VALUES (1)`
        return yield* Effect.result(sql.withTransaction(Effect.gen(function*() {
          yield* sql`INSERT INTO events (id) VALUES (2)`
          return yield* Effect.fail(new SimulatedFailure())
        })))
      })))
      assert.isTrue(Result.isSuccess(result))
      if (Result.isSuccess(result)) assert.isTrue(Result.isFailure(result.success))
      const rows = yield* sql<{ readonly id: number }>`SELECT id FROM events`
      assert.deepStrictEqual(rows, [{ id: 1 }])
    }).pipe(Effect.provide(memory)))

  it.effect("pins the connection for the whole transaction scope", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE events (id INTEGER)`
      const insideTransaction = yield* Deferred.make<void>()
      const commit = yield* Deferred.make<void>()
      const outsiderQueried = yield* Deferred.make<void>()

      const transaction = yield* sql.withTransaction(Effect.gen(function*() {
        yield* sql`INSERT INTO events (id) VALUES (1)`
        yield* Deferred.succeed(insideTransaction, undefined)
        yield* Deferred.await(commit)
      })).pipe(Effect.forkChild)
      yield* Deferred.await(insideTransaction)
      // This query must wait on the single connection permit until the
      // transaction releases it; if it ran early it would observe zero rows.
      // `started` proves the outsider fiber is mid-query before we poll, so a
      // None result means it is blocked on the permit, not merely unscheduled.
      const started = yield* Deferred.make<void>()
      const outsider = yield* Effect.gen(function*() {
        yield* Deferred.succeed(started, undefined)
        const rows = yield* sql<{ readonly id: number }>`SELECT id FROM events`
        yield* Deferred.succeed(outsiderQueried, undefined)
        return rows
      }).pipe(Effect.forkChild)
      yield* Deferred.await(started)
      yield* Effect.yieldNow
      yield* Effect.yieldNow
      const queriedEarly = yield* Deferred.poll(outsiderQueried)
      assert.isTrue(Option.isNone(queriedEarly))

      yield* Deferred.succeed(commit, undefined)
      yield* Fiber.join(transaction)
      const rows = yield* Fiber.join(outsider)
      assert.deepStrictEqual(rows, [{ id: 1 }])
    }).pipe(Effect.provide(memory)))

  it.effect("serializes ordinary statements for the lifetime of native execution", () =>
    Effect.gen(function*() {
      const original = ExpoSqliteNode.SQLiteDatabase.prototype.getAllAsync
      const firstEntered = Deferred.makeUnsafe<void>()
      let releaseQuery!: () => void
      const queryGate = new Promise<void>((resolve) => {
        releaseQuery = resolve
      })
      let active = 0
      let maxActive = 0
      ExpoSqliteNode.SQLiteDatabase.prototype.getAllAsync = async function<T,>(
        source: string,
        ...params: ReadonlyArray<unknown>
      ): Promise<Array<T>> {
        active++
        maxActive = Math.max(maxActive, active)
        Deferred.doneUnsafe(firstEntered, Effect.void)
        await queryGate
        try {
          return await original.call(this, source, ...params) as Array<T>
        } finally {
          active--
        }
      }
      try {
        const sql = yield* Client.SqlClient
        const first = yield* sql`SELECT 1 AS value`.pipe(Effect.forkChild)
        yield* Deferred.await(firstEntered)
        const second = Effect.runFork(sql`SELECT 2 AS value`)
        assert.strictEqual(maxActive, 1)
        releaseQuery()
        yield* Fiber.join(first)
        yield* Fiber.join(second)
      } finally {
        releaseQuery()
        ExpoSqliteNode.SQLiteDatabase.prototype.getAllAsync = original
      }
    }).pipe(Effect.provide(memory)))

  it.effect("closes the database when the layer scope closes", () =>
    Effect.gen(function*() {
      const before = ExpoSqliteNode.openHandleCount()
      const scope = yield* Scope.make()
      yield* Scope.provide(Layer.build(ExpoSqlite.layer({ databaseName: ":memory:" })), scope)
      assert.strictEqual(ExpoSqliteNode.openHandleCount(), before + 1)
      yield* Scope.close(scope, Exit.void)
      assert.strictEqual(ExpoSqliteNode.openHandleCount(), before)
    }))

  it.effect("closes a database whose open finishes after layer interruption", () =>
    Effect.gen(function*() {
      const before = ExpoSqliteNode.openHandleCount()
      const openEntered = Deferred.makeUnsafe<void>()
      const openFinished = Deferred.makeUnsafe<void>()
      let releaseOpen!: () => void
      const openGate = new Promise<void>((resolve) => {
        releaseOpen = resolve
      })
      ExpoSqliteNode.setOpenProbe({
        beforeOpen: () => {
          Deferred.doneUnsafe(openEntered, Effect.void)
          return openGate
        },
        afterOpen: () => {
          Deferred.doneUnsafe(openFinished, Effect.void)
        }
      })
      try {
        const build = yield* Layer.build(ExpoSqlite.layer({ databaseName: ":memory:" })).pipe(Effect.forkChild)
        yield* Deferred.await(openEntered)
        build.interruptUnsafe()
        releaseOpen()
        yield* Deferred.await(openFinished)
        yield* Fiber.await(build)
        assert.strictEqual(ExpoSqliteNode.openHandleCount(), before)
      } finally {
        releaseOpen()
        ExpoSqliteNode.setOpenProbe(undefined)
      }
    }))

  it.effect("streams exact big integers under SafeIntegers", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE ids (id INTEGER PRIMARY KEY, v INTEGER)`
      const big = "211882234062311423"
      yield* sql`INSERT INTO ids (id, v) VALUES (${big}, ${big})`
      const rows = yield* Stream.runCollect(
        sql`SELECT id, v FROM ids`.stream.pipe(Stream.provideService(Client.SafeIntegers, true))
      )
      assert.deepStrictEqual([...rows], [{ id: big, v: big }])
    }).pipe(Effect.provide(memory)))

  it.effect("rejects unsafe bigint binds on streamed reads", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      const result = yield* sql`SELECT ${BigInt("9007199254740993")} AS id`.stream.pipe(
        Stream.runCollect,
        Effect.result
      )
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "SqlError")
    }).pipe(Effect.provide(memory)))

  it.effect("returns exact big integers as text under SafeIntegers and preserves row order", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE ids (id INTEGER PRIMARY KEY, v INTEGER)`
      const big = "211882234062311423"
      const bigger = "311882234062311423"
      yield* sql`INSERT INTO ids (id, v) VALUES (${big}, ${big}), (${bigger}, ${bigger}), (1, 1)`
      const rows = yield* sql<{ readonly id: number | string; readonly v: number | string }>`
        SELECT id, v FROM ids ORDER BY id DESC
      `.pipe(Effect.provideService(Client.SafeIntegers, true))
      assert.deepStrictEqual(rows, [
        { id: bigger, v: bigger },
        { id: big, v: big },
        { id: 1, v: 1 }
      ])
    }).pipe(Effect.provide(memory)))

  it.effect("exposes run metadata through executeRaw", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)`
      yield* sql`INSERT INTO events (name) VALUES (${"a"})`
      const result = yield* sql`INSERT INTO events (name) VALUES (${"b"})`.raw
      const metadata = result as { readonly changes: number; readonly lastInsertRowId: number }
      assert.strictEqual(metadata.changes, 1)
      assert.strictEqual(metadata.lastInsertRowId, 2)
    }).pipe(Effect.provide(memory)))

  it.effect("applies transformResultNames to result rows", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      const rows = yield* sql<{ readonly sourceDocumentId: string }>`
        SELECT ${"doc_1"} AS source_document_id
      `
      assert.deepStrictEqual(rows, [{ sourceDocumentId: "doc_1" }])
    }).pipe(
      Effect.provide(ExpoSqlite.layer({
        databaseName: ":memory:",
        transformResultNames: (name) => name.replaceAll(/_([a-z])/g, (_, letter) => letter.toUpperCase())
      }))
    ))

  it.effect("normalizes DataView and non-Uint8Array binds to blobs", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      yield* sql`CREATE TABLE blobs (payload BLOB)`
      const bytes = new Uint8Array([9, 8, 7])
      const view = new DataView(bytes.buffer)
      yield* sql`INSERT INTO blobs (payload) VALUES (${view})`
      const rows = yield* sql<{ readonly payload: Uint8Array }>`SELECT payload FROM blobs`
      assert.deepStrictEqual([...rows[0].payload], [9, 8, 7])
    }).pipe(Effect.provide(memory)))

  it.effect("maps native failures to SqlError", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      const result = yield* Effect.result(sql`SELECT FROM definitely not sql`)
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) assert.strictEqual(result.failure._tag, "SqlError")
    }).pipe(Effect.provide(memory)))

  it.effect("builds from an Effect Config through layerConfig", () =>
    Effect.gen(function*() {
      const sql = yield* Client.SqlClient
      const rows = yield* sql`SELECT ${1} AS one`
      assert.deepStrictEqual(rows, [{ one: 1 }])
    }).pipe(Effect.provide(ExpoSqlite.layerConfig({ databaseName: Config.succeed(":memory:") }))))

  it.effect("enables WAL on file-backed databases unless disableWAL is set", () =>
    Effect.gen(function*() {
      const directory = yield* Effect.sync(() => mkdtempSync(join(tmpdir(), "expo-sqlite-")))
      const walLayer = ExpoSqlite.layer({ databaseName: "wal.db", directory })
      const walMode = yield* Effect.gen(function*() {
        const sql = yield* Client.SqlClient
        const rows = yield* sql<{ readonly journal_mode: string }>`PRAGMA journal_mode`
        return rows[0].journal_mode
      }).pipe(Effect.provide(walLayer))
      assert.strictEqual(walMode, "wal")
      const plainLayer = ExpoSqlite.layer({ databaseName: "plain.db", directory, disableWAL: true })
      const plainMode = yield* Effect.gen(function*() {
        const sql = yield* Client.SqlClient
        const rows = yield* sql<{ readonly journal_mode: string }>`PRAGMA journal_mode`
        return rows[0].journal_mode
      }).pipe(Effect.provide(plainLayer))
      assert.strictEqual(plainMode, "delete")
    }))
})

class SimulatedFailure {
  readonly _tag = "SimulatedFailure"
}
