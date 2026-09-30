import * as Config from "effect/Config"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
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

const classifyError = (cause: unknown, message: string, operation: string) =>
  classifySqliteError(sqliteCause(cause), { message, operation })

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

const native = <A,>(operation: string, message: string, evaluate: () => Promise<A>) =>
  Effect.uninterruptible(
    Effect.tryPromise({
      try: evaluate,
      catch: (cause) => new SqlError({ reason: classifyError(cause, message, operation) })
    })
  )

interface ExpoSqliteConnection extends Connection {}

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
      native(
        "openDatabase",
        "Failed to open database",
        () => SQLite.openDatabaseAsync(options.filename, { useNewConnection: true }, options.directory)
      ),
      (opened) =>
        semaphore.withPermits(1)(native("close", "Failed to close database", () => opened.closeAsync())).pipe(
          Effect.catchTag("SqlError", (error) => Effect.die(error))
        )
    )

    const runStatement = (statement: SQLite.SQLiteStatement, bound: Array<SQLite.SQLiteBindValue>, values: boolean) => {
      let executed: Promise<SQLite.SQLiteExecuteAsyncResult<any>>
      if (values) executed = statement.executeForRawResultAsync<any>(bound)
      else executed = statement.executeAsync<any>(bound)
      return executed.then((result) => result.getAllAsync()).finally(() => statement.finalizeAsync())
    }

    const query = (sql: string, params: ReadonlyArray<unknown>, values: boolean) =>
      Effect.flatMap(bindParams(params), (bound) =>
        rejectSafeIntegers.pipe(
          Effect.andThen(native(
            "execute",
            "Failed to execute statement",
            () => database.prepareAsync(sql).then((statement) => runStatement(statement, bound, values))
          ))
        ))

    const finalize = (prepared: SQLite.SQLiteStatement) =>
      native("stream", "Failed to finalize statement", () => prepared.finalizeAsync()).pipe(
        Effect.catchTag("SqlError", (error) => Effect.die(error))
      )

    const rows = (result: SQLite.SQLiteExecuteAsyncResult<any>) =>
      Stream.paginate(undefined, () =>
        native("stream", "Failed to read row", () => result.next()).pipe(
          Effect.map((next): readonly [ReadonlyArray<any>, Option.Option<undefined>] => {
            if (next.done === true) return [[], Option.none()]
            return [[next.value], Option.some(undefined)]
          })
        ))

    const stream = (sql: string, params: ReadonlyArray<unknown>) =>
      Stream.unwrap(Effect.gen(function*() {
        const bound = yield* bindParams(params)
        yield* rejectSafeIntegers
        const prepared = yield* Effect.acquireRelease(
          native("stream", "Failed to prepare statement", () => database.prepareAsync(sql)),
          finalize
        )
        const result = yield* native(
          "stream",
          "Failed to execute statement",
          () => prepared.executeAsync<any>(bound)
        )
        return rows(result)
      }))

    const connection = identity<ExpoSqliteConnection>({
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
