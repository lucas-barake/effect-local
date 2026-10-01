import * as Config from "effect/Config"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import { identity } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Reactivity from "effect/reactivity/Reactivity"
import type * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Client from "effect/sql/SqlClient"
import type { Connection } from "effect/sql/SqlConnection"
import { classifySqliteError, SqlError, UnknownError } from "effect/sql/SqlError"
import * as Statement from "effect/sql/Statement"
import * as Stream from "effect/Stream"
import * as SQLite from "expo-sqlite"

const ATTR_DB_SYSTEM_NAME = "db.system.name"

export const TypeId: TypeId = "~@lucas-barake/effect-local-expo/ExpoSqliteClient"

export type TypeId = "~@lucas-barake/effect-local-expo/ExpoSqliteClient"

export interface ExpoSqliteClient extends Client.SqlClient {
  readonly [TypeId]: TypeId
  readonly config: ExpoSqliteClientConfig
}

export const ExpoSqliteClient = Context.Service<ExpoSqliteClient>("@lucas-barake/effect-local-expo/ExpoSqliteClient")

export interface ExpoSqliteClientConfig {
  readonly filename: string
  readonly directory?: string | undefined
  readonly prepareCacheSize?: number | undefined
  readonly disableWAL?: boolean | undefined
  readonly spanAttributes?: Record<string, unknown> | undefined
  readonly transformResultNames?: ((str: string) => string) | undefined
  readonly transformQueryNames?: ((str: string) => string) | undefined
}

const nativeErrorCode = /Error code (\d+|[\s\S]):/

const sqliteCause = (cause: unknown): unknown => {
  if (typeof cause !== "object" || cause === null || !("message" in cause) || typeof cause.message !== "string") {
    return cause
  }
  const match = nativeErrorCode.exec(cause.message)
  if (match === null) return cause
  const [, code] = match
  let errno = Number(code)
  if (!/^\d+$/.test(code)) errno = code.charCodeAt(0)
  return Object.assign(cause, { errno })
}

const bindValue = (value: unknown): SQLite.SQLiteBindValue | undefined => {
  if (value === undefined || value === null) return null
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return value
  if (typeof value === "bigint") {
    if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER)) return undefined
    return Number(value)
  }
  if (value instanceof Uint8Array) return value
  if (value instanceof ArrayBuffer) return new Uint8Array(value)
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength))
  }
  return undefined
}

const bindParams = (params: ReadonlyArray<unknown>): Effect.Effect<Array<SQLite.SQLiteBindValue>, SqlError> => {
  const bound: Array<SQLite.SQLiteBindValue> = []
  for (const [index, param] of params.entries()) {
    const value = bindValue(param)
    if (value === undefined) {
      return Effect.fail(
        new SqlError({
          reason: new UnknownError({
            cause: param,
            message: `expo-sqlite cannot bind parameter ${index + 1} exactly`,
            operation: "bind"
          })
        })
      )
    }
    bound.push(value)
  }
  return Effect.succeed(bound)
}

const rejectSafeIntegers = Effect.withFiber<void, SqlError>((fiber) => {
  if (!fiber.getRef(Client.SafeIntegers)) return Effect.void
  return Effect.fail(
    new SqlError({
      reason: new UnknownError({
        cause: "SafeIntegers",
        message: "expo-sqlite returns INTEGER columns as JavaScript numbers and cannot read 64-bit integers exactly",
        operation: "execute"
      })
    })
  )
})

export const make: (
  options: ExpoSqliteClientConfig
) => Effect.Effect<ExpoSqliteClient, SqlError, Scope.Scope | Reactivity.Reactivity> = Effect.fnUntraced(
  function*(options) {
    const compiler = Statement.makeCompilerSqlite(options.transformQueryNames)
    let resultTransform: (<A extends object,>(row: ReadonlyArray<A>) => ReadonlyArray<A>) | undefined
    if (options.transformResultNames) {
      resultTransform = Statement.defaultTransforms(options.transformResultNames).array
    }
    const semaphore = yield* Semaphore.make(1)

    const database = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: () => SQLite.openDatabaseAsync(options.filename, { useNewConnection: true }, options.directory),
        catch: (cause) =>
          new SqlError({
            reason: classifySqliteError(sqliteCause(cause), {
              message: "Failed to open database",
              operation: "openDatabase"
            })
          })
      }),
      (opened) =>
        Effect.tryPromise({
          try: () => opened.closeAsync(),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(sqliteCause(cause), {
                message: "Failed to close database",
                operation: "close"
              })
            })
        }).pipe(
          Effect.uninterruptible,
          semaphore.withPermits(1),
          Effect.catchTag("SqlError", (error) => Effect.die(error))
        )
    )

    const finalizeStatement = (statement: SQLite.SQLiteStatement) =>
      Effect.tryPromise({
        try: () => statement.finalizeAsync(),
        catch: (cause) =>
          new SqlError({
            reason: classifySqliteError(sqliteCause(cause), {
              message: "Failed to finalize statement",
              operation: "finalize"
            })
          })
      }).pipe(Effect.uninterruptible)

    const prepareStatement = (sql: string) =>
      Effect.tryPromise({
        try: () => database.prepareAsync(sql),
        catch: (cause) =>
          new SqlError({
            reason: classifySqliteError(sqliteCause(cause), {
              message: "Failed to prepare statement",
              operation: "prepare"
            })
          })
      }).pipe(Effect.uninterruptible)

    const prepareCacheSize = options.prepareCacheSize ?? 200
    const prepared = new Map<string, SQLite.SQLiteStatement>()
    const running = new Set<SQLite.SQLiteStatement>()

    const discard = (statement: SQLite.SQLiteStatement) =>
      finalizeStatement(statement).pipe(Effect.catchTag("SqlError", () => Effect.void))

    const evict = (sql: string) =>
      Effect.suspend(() => {
        const statement = prepared.get(sql)
        if (statement === undefined) return Effect.void
        prepared.delete(sql)
        if (running.has(statement)) return Effect.void
        return finalizeStatement(statement).pipe(Effect.catchTag("SqlError", (error) => Effect.die(error)))
      })

    const oldest = () => {
      for (const sql of prepared.keys()) return sql
      return undefined
    }

    const checkout = (sql: string) =>
      Effect.suspend(() => {
        const statement = prepared.get(sql)
        if (statement !== undefined && !running.has(statement)) {
          prepared.delete(sql)
          prepared.set(sql, statement)
          running.add(statement)
          return Effect.succeed(statement)
        }
        return prepareStatement(sql).pipe(
          Effect.tap((fresh) => {
            running.add(fresh)
            if (prepared.has(sql)) return Effect.void
            prepared.set(sql, fresh)
            const evicted = oldest()
            if (prepared.size <= prepareCacheSize || evicted === undefined) return Effect.void
            return evict(evicted)
          }),
          Effect.uninterruptible
        )
      })

    const checkin = (sql: string, statement: SQLite.SQLiteStatement, failed: boolean) =>
      Effect.suspend(() => {
        running.delete(statement)
        const cachedStatement = prepared.get(sql) === statement
        if (failed) {
          if (cachedStatement) prepared.delete(sql)
          return discard(statement)
        }
        if (cachedStatement) return Effect.void
        return finalizeStatement(statement).pipe(Effect.catchTag("SqlError", (error) => Effect.die(error)))
      })

    const runStatement = (statement: SQLite.SQLiteStatement, bound: Array<SQLite.SQLiteBindValue>, values: boolean) => {
      let executed: Promise<SQLite.SQLiteExecuteAsyncResult<any>>
      if (values) executed = statement.executeForRawResultAsync<any>(bound)
      else executed = statement.executeAsync<any>(bound)
      return executed.then((result) => result.getAllAsync())
    }

    const query = (sql: string, params: ReadonlyArray<unknown>, values: boolean) =>
      Effect.flatMap(bindParams(params), (bound) =>
        rejectSafeIntegers.pipe(
          Effect.andThen(
            checkout(sql).pipe(
              Effect.flatMap((statement) =>
                Effect.tryPromise({
                  try: () => runStatement(statement, bound, values),
                  catch: (cause) =>
                    new SqlError({
                      reason: classifySqliteError(sqliteCause(cause), {
                        message: "Failed to execute statement",
                        operation: "execute"
                      })
                    })
                }).pipe(Effect.onExit((exit) => checkin(sql, statement, Exit.isFailure(exit))))
              ),
              Effect.uninterruptible
            )
          )
        ))

    const rows = (result: SQLite.SQLiteExecuteAsyncResult<any>) =>
      Stream.paginate(undefined, () =>
        Effect.tryPromise({
          try: () => result.next(),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(sqliteCause(cause), { message: "Failed to read row", operation: "stream" })
            })
        }).pipe(
          Effect.uninterruptible,
          Effect.map((next): readonly [ReadonlyArray<any>, Option.Option<undefined>] => {
            if (next.done === true) return [[], Option.none()]
            return [[next.value], Option.some(undefined)]
          })
        ))

    const stream = (sql: string, params: ReadonlyArray<unknown>) =>
      Stream.unwrap(Effect.gen(function*() {
        const bound = yield* bindParams(params)
        yield* rejectSafeIntegers
        let stepFailed = false
        const markStepFailed = Effect.sync(() => {
          stepFailed = true
        })
        const statement = yield* Effect.acquireRelease(prepareStatement(sql), (created) => {
          if (stepFailed) return discard(created)
          return finalizeStatement(created).pipe(Effect.catchTag("SqlError", (error) => Effect.die(error)))
        })
        const result = yield* Effect.tryPromise({
          try: () => statement.executeAsync<any>(bound),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(sqliteCause(cause), {
                message: "Failed to execute statement",
                operation: "stream"
              })
            })
        }).pipe(Effect.uninterruptible, Effect.tapError(() => markStepFailed))
        return rows(result).pipe(Stream.tapError(() => markStepFailed))
      }))

    const connection = identity<Connection>({
      execute(sql, params, transformRows) {
        if (transformRows) return Effect.map(query(sql, params, false), transformRows)
        return query(sql, params, false)
      },
      executeRaw(sql, params) {
        return query(sql, params, false)
      },
      executeValues(sql, params) {
        return query(sql, params, true)
      },
      executeValuesUnprepared(sql, params) {
        return query(sql, params, true)
      },
      executeUnprepared(sql, params, transformRows) {
        return this.execute(sql, params, transformRows)
      },
      executeStream(sql, params, transformRows) {
        if (transformRows) {
          return Stream.flatMap(stream(sql, params), (row) => Stream.fromIterable(transformRows([row])))
        }
        return stream(sql, params)
      }
    })

    if (options.disableWAL !== true) yield* connection.execute("PRAGMA journal_mode = WAL", [], undefined)

    const { onCommitFailure, transactionAcquirer } = Client.makeSqliteAcquirers({
      connection: Effect.succeed(connection),
      semaphore
    })

    const client = yield* Client.make({
      acquirer: transactionAcquirer,
      compiler,
      transactionAcquirer,
      onCommitFailure,
      releaseSavepoint: (name) => `RELEASE SAVEPOINT ${name}`,
      spanAttributes: [
        ...Object.entries(options.spanAttributes ?? {}),
        [ATTR_DB_SYSTEM_NAME, "sqlite"]
      ],
      transformRows: resultTransform
    })
    return Object.assign(client, { [TypeId]: TypeId, config: options })
  }
)

const toContext = (client: ExpoSqliteClient) =>
  Context.make(ExpoSqliteClient, client).pipe(Context.add(Client.SqlClient, client))

export const layerConfig = (
  config: Config.Wrap<ExpoSqliteClientConfig>
): Layer.Layer<ExpoSqliteClient | Client.SqlClient, Config.ConfigError | SqlError> =>
  Config.unwrap(config).pipe(
    Effect.flatMap(make),
    Effect.map(toContext),
    Layer.effectContext,
    Layer.provide(Reactivity.layer)
  )

export const layer = (
  config: ExpoSqliteClientConfig
): Layer.Layer<ExpoSqliteClient | Client.SqlClient, SqlError> =>
  make(config).pipe(
    Effect.map(toContext),
    Layer.effectContext,
    Layer.provide(Reactivity.layer)
  )
