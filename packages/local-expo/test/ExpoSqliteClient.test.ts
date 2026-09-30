import { NodeFileSystem } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Config from "effect/Config"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FileSystem from "effect/FileSystem"
import * as Reactivity from "effect/reactivity/Reactivity"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as SqlClient from "effect/sql/SqlClient"
import type * as SqlError from "effect/sql/SqlError"
import * as Stream from "effect/Stream"
import * as ExpoSqliteClient from "../src/ExpoSqliteClient.js"
import { probe } from "./fixtures/nativeSqlite.js"

const memory = { filename: ":memory:", disableWAL: true } as const

class Abort extends Schema.TaggedError<Abort>("@lucas-barake/effect-local-expo/test/Abort")("Abort", {}) {}

const resetProbe = Effect.sync(() => probe.reset())
const provideReactivity = Effect.provide(Reactivity.layer)
const provideReactivityAndFileSystem = Effect.provide([Reactivity.layer, NodeFileSystem.layer])

const client = (config: ExpoSqliteClient.ExpoSqliteClientConfig = memory) =>
  resetProbe.pipe(Effect.andThen(ExpoSqliteClient.make(config)))

const temporaryDirectory = FileSystem.FileSystem.use((fs) => fs.makeTempDirectoryScoped())

const nativeCalls = (method: string) => probe.calls.filter((call) => call === method).length

const held = (method: string) => {
  const hold = probe.hold(method)
  return {
    entered: Effect.promise(() => hold.entered),
    release: Effect.sync(hold.release)
  }
}

const failureReason = <A,>(exit: Exit.Exit<A, SqlError.SqlError>) => {
  assert.isTrue(Exit.isFailure(exit))
  if (Exit.isSuccess(exit)) return undefined
  const error = exit.cause.reasons.find((reason) => reason._tag === "Fail")
  if (error?._tag !== "Fail") return undefined
  return error.error.reason._tag
}

describe("ExpoSqliteClient", () => {
  it.effect(
    "reads rows, value arrays, RETURNING, booleans and blobs through expo-sqlite",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        yield* sql`CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, flag INTEGER, data BLOB)`
        const inserted = yield* sql<{ readonly id: number }>`INSERT INTO t (id, name, flag, data)
        VALUES (${1}, ${"a"}, ${true}, ${new Uint8Array([1, 2, 3])}), (${2}, ${"b"}, ${false}, ${null})
        RETURNING id`
        assert.deepStrictEqual(inserted, [{ id: 1 }, { id: 2 }])
        const rows = yield* sql<{ readonly id: number; readonly flag: number; readonly data: Uint8Array | null }>`
        SELECT id, flag, data FROM t ORDER BY id`
        assert.deepStrictEqual(rows, [
          { id: 1, flag: 1, data: new Uint8Array([1, 2, 3]) },
          { id: 2, flag: 0, data: null }
        ])
        assert.deepStrictEqual(yield* sql`SELECT id, name FROM t ORDER BY id`.values, [[1, "a"], [2, "b"]])
        assert.deepStrictEqual(yield* sql`SELECT ${1} AS a, ${2} AS a`.values, [[1, 2]])
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "commits, rolls back and nests transactions on its own connection",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        yield* sql`CREATE TABLE t (id INTEGER PRIMARY KEY)`
        yield* sql.withTransaction(sql`INSERT INTO t (id) VALUES (${1})`)
        const rolledBack = yield* sql.withTransaction(
          sql`INSERT INTO t (id) VALUES (${2})`.pipe(Effect.andThen(Effect.fail(new Abort())))
        ).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(rolledBack))
        yield* sql.withTransaction(Effect.gen(function*() {
          yield* sql`INSERT INTO t (id) VALUES (${3})`
          yield* sql.withTransaction(
            sql`INSERT INTO t (id) VALUES (${4})`.pipe(Effect.andThen(Effect.fail(new Abort())))
          ).pipe(Effect.exit)
        }))
        assert.deepStrictEqual(yield* sql`SELECT id FROM t ORDER BY id`.values, [[1], [3]])
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "keeps a statement from another fiber off the connection while a transaction holds it",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        yield* sql`CREATE TABLE t (id INTEGER PRIMARY KEY)`
        const insert = held("runAsync")
        const transaction = yield* sql.withTransaction(
          sql`INSERT INTO t (id) VALUES (${1})`.pipe(Effect.andThen(sql`INSERT INTO t (id) VALUES (${2})`))
        ).pipe(Effect.forkChild)
        yield* insert.entered
        const prepared = nativeCalls("prepareAsync")
        const started = yield* Deferred.make<void>()
        const reader = yield* Deferred.succeed(started, undefined).pipe(
          Effect.andThen(sql`SELECT COUNT(*) AS count FROM t`.values),
          Effect.forkChild
        )
        yield* Deferred.await(started)
        assert.strictEqual(nativeCalls("prepareAsync"), prepared)
        yield* insert.release
        yield* Fiber.join(transaction)
        assert.deepStrictEqual(yield* Fiber.join(reader), [[2]])
        assert.strictEqual(probe.maxInFlight, 1)
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "never runs two native calls at once for concurrent statements",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        yield* sql`CREATE TABLE t (id INTEGER PRIMARY KEY)`
        yield* Effect.forEach(
          Array.from({ length: 20 }, (_, index) => index),
          (id) => sql`INSERT INTO t (id) VALUES (${id})`,
          { concurrency: "unbounded", discard: true }
        )
        assert.deepStrictEqual(yield* sql`SELECT COUNT(*) FROM t`.values, [[20]])
        assert.strictEqual(probe.maxInFlight, 1)
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "holds the connection until an interrupted statement's native call settles",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        yield* sql`CREATE TABLE t (id INTEGER PRIMARY KEY)`
        const insert = held("runAsync")
        const writer = yield* sql`INSERT INTO t (id) VALUES (${1})`.pipe(Effect.forkChild)
        yield* insert.entered
        const interruption = yield* Fiber.interrupt(writer).pipe(Effect.forkChild)
        const prepared = nativeCalls("prepareAsync")
        const started = yield* Deferred.make<void>()
        const reader = yield* Deferred.succeed(started, undefined).pipe(
          Effect.andThen(sql`SELECT COUNT(*) FROM t`.values),
          Effect.forkChild
        )
        yield* Deferred.await(started)
        assert.strictEqual(nativeCalls("prepareAsync"), prepared)
        yield* insert.release
        yield* Fiber.join(interruption)
        assert.deepStrictEqual(yield* Fiber.join(reader), [[1]])
        assert.strictEqual(probe.maxInFlight, 1)
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "closes the database only after in flight native work settles",
    Effect.fnUntraced(function*() {
      probe.reset()
      const scope = yield* Scope.make()
      const sql = yield* ExpoSqliteClient.make(memory).pipe(Scope.provide(scope))
      const read = held("getAllAsync")
      const reader = yield* sql`SELECT 1`.values.pipe(Effect.forkChild)
      yield* read.entered
      const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild)
      yield* read.release
      yield* Fiber.join(closing)
      yield* Fiber.join(reader)
      assert.strictEqual(probe.openDatabases, 0)
      assert.isBelow(probe.calls.indexOf("getAllAsync"), probe.calls.indexOf("closeAsync"))
      assert.strictEqual(probe.maxInFlight, 1)
    }, provideReactivity)
  )

  it.effect(
    "closes a database whose open was interrupted",
    Effect.fnUntraced(function*() {
      probe.reset()
      const open = held("initAsync")
      const opening = yield* ExpoSqliteClient.make(memory).pipe(Effect.scoped, Effect.forkChild)
      yield* open.entered
      const interruption = yield* Fiber.interrupt(opening).pipe(Effect.forkChild)
      yield* open.release
      yield* Fiber.join(interruption)
      assert.strictEqual(probe.opened.length, 1)
      assert.strictEqual(probe.openDatabases, 0)
    }, provideReactivity)
  )

  it.effect(
    "opens its own native connection with WAL enabled by default",
    Effect.fnUntraced(
      function*() {
        const directory = yield* temporaryDirectory
        const sql = yield* client({ filename: "replica.db", directory })
        assert.deepStrictEqual(probe.opened, [{ path: `${directory}/replica.db`, useNewConnection: true }])
        assert.deepStrictEqual(yield* sql`PRAGMA journal_mode`.values, [["wal"]])
      },
      Effect.scoped,
      provideReactivityAndFileSystem
    )
  )

  it.effect(
    "classifies native constraint failures from the iOS and Android error formats",
    Effect.fnUntraced(
      function*() {
        for (const platform of ["ios", "android"] as const) {
          const sql = yield* client()
          probe.platform = platform
          yield* sql`CREATE TABLE t (id INTEGER PRIMARY KEY)`
          yield* sql`INSERT INTO t (id) VALUES (${1})`
          const duplicate = yield* sql`INSERT INTO t (id) VALUES (${1})`.pipe(Effect.exit)
          assert.strictEqual(failureReason(duplicate), "ConstraintError", platform)
          const syntax = yield* sql`SELEKT 1`.pipe(Effect.exit)
          assert.strictEqual(failureReason(syntax), "UnknownError", platform)
        }
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "rolls back a failed COMMIT so the connection stays usable",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        yield* sql`PRAGMA foreign_keys = ON`
        yield* sql`CREATE TABLE parent (id INTEGER PRIMARY KEY)`
        yield* sql`CREATE TABLE child (id INTEGER PRIMARY KEY,
        parent INTEGER REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED)`
        const commit = yield* sql.withTransaction(sql`INSERT INTO child (id, parent) VALUES (${1}, ${99})`).pipe(
          Effect.exit
        )
        assert.isTrue(Exit.isFailure(commit))
        if (Exit.isFailure(commit)) assert.match(Cause.pretty(commit.cause), /foreign key constraint failed/i)
        yield* sql`INSERT INTO parent (id) VALUES (${1})`
        yield* sql.withTransaction(sql`INSERT INTO child (id, parent) VALUES (${2}, ${1})`)
        assert.deepStrictEqual(yield* sql`SELECT id FROM child`.values, [[2]])
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "refuses what expo-sqlite cannot represent exactly before reaching native code",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        assert.deepStrictEqual(yield* sql`SELECT ${42n} AS value`.values, [[42]])
        const prepared = nativeCalls("prepareAsync")
        const oversized = yield* sql`SELECT ${2n ** 63n - 1n} AS value`.pipe(Effect.exit)
        assert.strictEqual(failureReason(oversized), "UnknownError")
        const safeIntegers = yield* sql`SELECT 1`.pipe(
          Effect.provideService(SqlClient.SafeIntegers, true),
          Effect.exit
        )
        assert.strictEqual(failureReason(safeIntegers), "UnknownError")
        assert.strictEqual(nativeCalls("prepareAsync"), prepared)
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "streams rows and keeps other statements off the connection until the stream ends",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        yield* sql`CREATE TABLE t (id INTEGER PRIMARY KEY)`
        yield* sql`INSERT INTO t (id) VALUES (${1}), (${2}), (${3})`
        const writer = yield* sql`INSERT INTO t (id) VALUES (${4})`.pipe(Effect.forkChild)
        const streamed = yield* sql<{ readonly id: number }>`SELECT id FROM t WHERE id < 4 ORDER BY id`.stream.pipe(
          Stream.runCollect
        )
        yield* Fiber.join(writer)
        assert.deepStrictEqual(Array.from(streamed), [{ id: 1 }, { id: 2 }, { id: 3 }])
        assert.deepStrictEqual(yield* sql`SELECT COUNT(*) FROM t`.values, [[4]])
        assert.strictEqual(probe.maxInFlight, 1)
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "applies configured result name transforms to rows and streams",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client({ ...memory, transformResultNames: (name) => name.toUpperCase() })
        assert.deepStrictEqual(yield* sql`SELECT 1 AS id`, [{ ID: 1 }])
        const streamed = yield* sql`SELECT 1 AS id`.stream.pipe(Stream.runCollect)
        assert.deepStrictEqual(Array.from(streamed), [{ ID: 1 }])
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "prepares repeated SQL once and runs each execution in one native call",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        yield* sql`CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)`
        probe.calls = []
        for (let id = 0; id < 100; id++) yield* sql`INSERT INTO t (id, name) VALUES (${id}, ${"n"})`
        assert.deepStrictEqual(yield* sql`SELECT COUNT(*) FROM t`.values, [[100]])
        assert.strictEqual(nativeCalls("prepareAsync"), 2)
        assert.strictEqual(nativeCalls("runAsync"), 101)
        assert.strictEqual(nativeCalls("finalizeAsync"), 0)
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "prepares again after a failed prepare and finalizes every prepared statement before close",
    Effect.fnUntraced(function*() {
      yield* resetProbe
      const scope = yield* Scope.make()
      const sql = yield* ExpoSqliteClient.make(memory).pipe(Scope.provide(scope))
      const missing = yield* Effect.exit(sql`SELECT id FROM later`)
      assert.isTrue(Exit.isFailure(missing))
      yield* sql`CREATE TABLE later (id INTEGER)`
      yield* sql`INSERT INTO later (id) VALUES (${1})`
      assert.deepStrictEqual(yield* sql`SELECT id FROM later`, [{ id: 1 }])
      const duplicate = yield* Effect.exit(
        sql`INSERT INTO later (id) VALUES (${1})`.pipe(Effect.andThen(sql`CREATE UNIQUE INDEX u ON later (id)`))
      )
      assert.isTrue(Exit.isFailure(duplicate))
      yield* Scope.close(scope, Exit.void)
      assert.strictEqual(probe.openDatabases, 0)
      assert.strictEqual(probe.maxInFlight, 1)
      assert.strictEqual(nativeCalls("prepareAsync"), nativeCalls("finalizeAsync") + 1)
      assert.isBelow(probe.calls.lastIndexOf("finalizeAsync"), probe.calls.indexOf("closeAsync"))
    }, provideReactivity)
  )

  it.effect(
    "reports a constraint violation as a recoverable SqlError after finalizing its statement",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        yield* sql`CREATE TABLE t (id INTEGER PRIMARY KEY)`
        yield* sql`INSERT INTO t (id) VALUES (${1})`
        const duplicate = yield* sql`INSERT INTO t (id) VALUES (${1})`.pipe(Effect.exit)
        assert.strictEqual(failureReason(duplicate), "ConstraintError")
        if (Exit.isFailure(duplicate)) assert.isFalse(duplicate.cause.reasons.some((reason) => reason._tag === "Die"))
        const recovered = yield* sql`INSERT INTO t (id) VALUES (${1})`.pipe(
          Effect.as("inserted"),
          Effect.catchTag("SqlError", () => Effect.succeed("recovered"))
        )
        assert.strictEqual(recovered, "recovered")
        yield* sql`INSERT INTO t (id) VALUES (${2})`
        assert.deepStrictEqual(yield* sql`SELECT id FROM t ORDER BY id`.values, [[1], [2]])
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "returns each concurrent execution of the same SQL inside a transaction its own rows",
    Effect.fnUntraced(
      function*() {
        const sql = yield* client()
        yield* sql`CREATE TABLE t (id INTEGER PRIMARY KEY)`
        yield* sql`INSERT INTO t (id) VALUES (${1}), (${2}), (${3})`
        assert.deepStrictEqual(yield* sql`SELECT id FROM t WHERE id >= ${2} ORDER BY id`.values, [[2], [3]])
        const results = yield* sql.withTransaction(
          Effect.all([
            sql`SELECT id FROM t WHERE id >= ${1} ORDER BY id`.values,
            sql`SELECT id FROM t WHERE id >= ${3} ORDER BY id`.values
          ], { concurrency: "unbounded" })
        )
        assert.deepStrictEqual(results, [[[1], [2], [3]], [[3]]])
      },
      Effect.scoped,
      provideReactivity
    )
  )

  it.effect(
    "reads configuration through layerConfig",
    Effect.fnUntraced(function*() {
      yield* resetProbe
      const rows = yield* SqlClient.SqlClient.use((sql) => sql`SELECT 1 AS one`.values).pipe(
        Effect.provide(
          ExpoSqliteClient.layerConfig({ filename: Config.succeed(":memory:"), disableWAL: Config.succeed(true) })
        )
      )
      assert.deepStrictEqual(rows, [[1]])
      assert.deepStrictEqual(probe.opened, [{ path: ":memory:", useNewConnection: true }])
    }, Effect.scoped)
  )
})
