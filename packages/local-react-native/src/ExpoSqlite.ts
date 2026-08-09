/**
 * Connects Effect SQL to SQLite on React Native using `expo-sqlite`.
 *
 * This module opens an on-device SQLite database through expo's asynchronous API
 * and exposes it as both `ExpoSqliteClient` and the generic Effect SQL client.
 * All access is serialized through a single connection permit, transactions pin
 * that connection for their whole scope (nested transactions become savepoints,
 * handled by `SqlClient` core), and streaming queries run through expo-sqlite's
 * async cursor. The synchronous `*Sync` APIs are deliberately not used: they
 * block the only JS thread a React Native app has.
 *
 * Bind values follow expo-sqlite's own rules (`string | number | null | boolean
 * | Uint8Array | ArrayBuffer`); other typed arrays and `DataView` are normalized
 * to `Uint8Array`, and `bigint` binds convert to `number` when they fit a safe
 * integer and fail with `SqlError` otherwise.
 *
 * @since 0.1.0
 */
import * as Config from "effect/Config"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import { identity } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as Client from "effect/unstable/sql/SqlClient"
import type { Connection } from "effect/unstable/sql/SqlConnection"
import { classifySqliteError, SqlError } from "effect/unstable/sql/SqlError"
import * as Statement from "effect/unstable/sql/Statement"
import * as SQLite from "expo-sqlite"

const ATTR_DB_SYSTEM_NAME = "db.system.name"

/**
 * Runtime identifier attached to expo-sqlite client values.
 *
 * @category type IDs
 * @since 0.1.0
 */
export const TypeId: TypeId = "~@lucas-barake/effect-local-react-native/ExpoSqliteClient"

/**
 * Type-level identifier for expo-sqlite client values.
 *
 * @category type IDs
 * @since 0.1.0
 */
export type TypeId = "~@lucas-barake/effect-local-react-native/ExpoSqliteClient"

/**
 * expo-sqlite client service interface, extending `SqlClient` with its
 * configuration and marking `updateValues` as unsupported for SQLite.
 *
 * @category models
 * @since 0.1.0
 */
export interface ExpoSqliteClient extends Client.SqlClient {
  readonly [TypeId]: TypeId
  readonly config: ExpoSqliteClientConfig

  /** Not supported in sqlite */
  readonly updateValues: never
}

/**
 * Service tag for the expo-sqlite client.
 *
 * @category services
 * @since 0.1.0
 */
export const ExpoSqliteClient = Context.Service<ExpoSqliteClient>(
  "@lucas-barake/effect-local-react-native/ExpoSqliteClient"
)

/**
 * Configuration for an expo-sqlite client.
 *
 * `databaseName` is passed to `SQLite.openDatabaseAsync`; `":memory:"` gives a
 * transient database. `directory` overrides the default on-device database
 * directory. WAL mode is enabled after open unless `disableWAL` is set, because
 * Effect Local's replica issues reads and writes from many fibers and WAL is
 * what keeps a single-writer database responsive under that pattern; set
 * `disableWAL` for databases that must stay rollback-journaled.
 *
 * @category models
 * @since 0.1.0
 */
export interface ExpoSqliteClientConfig {
  readonly databaseName: string
  readonly options?: SQLite.SQLiteOpenOptions | undefined
  readonly directory?: string | undefined
  readonly disableWAL?: boolean | undefined
  readonly spanAttributes?: Record<string, unknown> | undefined
  readonly transformResultNames?: ((str: string) => string) | undefined
  readonly transformQueryNames?: ((str: string) => string) | undefined
}

// expo-sqlite binds only Uint8Array | ArrayBuffer for blobs; every other binary
// view the Statement compiler can emit is copied into a Uint8Array here.
const normalizeParam = (param: unknown): unknown => {
  if (typeof param === "bigint") {
    const asNumber = Number(param)
    return Number.isSafeInteger(asNumber) ? asNumber : param
  }
  return ArrayBuffer.isView(param) && !(param instanceof Uint8Array)
    ? new Uint8Array(param.buffer, param.byteOffset, param.byteLength)
    : param
}

const normalizeParams = (params: ReadonlyArray<unknown>): ReadonlyArray<unknown> =>
  params.some((param) => typeof param === "bigint" || (ArrayBuffer.isView(param) && !(param instanceof Uint8Array)))
    ? params.map(normalizeParam)
    : params

const hasUnsafeBigint = (params: ReadonlyArray<unknown>): boolean =>
  params.some((param) => typeof param === "bigint" && !Number.isSafeInteger(Number(param)))

interface ExpoSqliteConnection extends Connection {}

const SAFE_INTEGER_MAX = "9007199254740991"

// expo-sqlite bridges every INTEGER column into a JS number, so values above 2^53 —
// cluster snowflake ids are the load-bearing case — would come back rounded and corrupt
// every request id read back from storage. Reads running under `SqlClient.SafeIntegers`
// (cluster message storage) are wrapped so huge integers cross the bridge as exact TEXT;
// `messageFromRow` and friends pass them through `String(...)`, which preserves the digits.
// Small integers keep arriving as numbers, so local schemas are unaffected.
const isSafeIntegersRead = (sql: string): boolean => /^\s*(?:--[^\n]*\n|\/\*[^]*?\*\/\s*)*\s*(select|with)\b/i.test(sql)

const wrapSelectForExactIntegers = (sql: string, columnNames: ReadonlyArray<string>): string => {
  const expressions = columnNames.map((name) => {
    const quoted = `"${name.replaceAll("\"", "\"\"")}"`
    return `CASE WHEN typeof(${quoted}) = 'integer' AND (${quoted} > ${SAFE_INTEGER_MAX} OR ${quoted} < -${SAFE_INTEGER_MAX}) THEN CAST(${quoted} AS TEXT) ELSE ${quoted} END AS ${quoted}`
  })
  return `SELECT ${expressions.join(", ")} FROM (${sql})`
}

// Unwrapped reads resolve duplicate column names last-wins (expo-sqlite's composeRow
// assigns in order); a wrapped outer SELECT would resolve them first-wins. Statements
// with duplicates fall back to unwrapped rather than silently changing which row wins.
const hasDuplicateNames = (names: ReadonlyArray<string>): boolean => new Set(names).size !== names.length

/**
 * Creates a scoped expo-sqlite client from the supplied configuration, using a
 * single serialized connection.
 *
 * @category constructors
 * @since 0.1.0
 */
export const make = (
  options: ExpoSqliteClientConfig
): Effect.Effect<ExpoSqliteClient, SqlError, Scope.Scope | Reactivity.Reactivity> =>
  Effect.gen(function*() {
    const compiler = Statement.makeCompilerSqlite(options.transformQueryNames)
    const transformRows = options.transformResultNames ?
      Statement.defaultTransforms(options.transformResultNames).array :
      undefined

    const makeConnection = Effect.gen(function*() {
      const db = yield* Effect.tryPromise({
        try: () => SQLite.openDatabaseAsync(options.databaseName, options.options, options.directory),
        catch: (cause) =>
          new SqlError({
            reason: classifySqliteError(cause, { message: "Failed to open database", operation: "open" })
          })
      })
      yield* Effect.addFinalizer(() =>
        Effect.tryPromise({
          try: () => db.closeAsync(),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(cause, { message: "Failed to close database", operation: "close" })
            })
        }).pipe(Effect.orDie)
      )
      if (options.disableWAL !== true) {
        yield* Effect.tryPromise({
          try: () => db.execAsync("PRAGMA journal_mode = WAL"),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(cause, { message: "Failed to enable WAL mode", operation: "pragma" })
            })
        })
      }

      const columnNamesFor = (sql: string) =>
        Effect.acquireUseRelease(
          Effect.tryPromise({
            try: () => db.prepareAsync(sql),
            catch: (cause) =>
              new SqlError({
                reason: classifySqliteError(cause, { message: "Failed to prepare statement", operation: "prepare" })
              })
          }),
          (statement) =>
            Effect.tryPromise({
              try: () => statement.getColumnNamesAsync(),
              catch: (cause) =>
                new SqlError({
                  reason: classifySqliteError(cause, { message: "Failed to read column names", operation: "prepare" })
                })
            }),
          (statement) =>
            Effect.tryPromise({
              try: () => statement.finalizeAsync(),
              catch: (cause) =>
                new SqlError({
                  reason: classifySqliteError(cause, {
                    message: "Failed to finalize statement",
                    operation: "finalize"
                  })
                })
            }).pipe(Effect.orDie)
        )

      // If the wrap cannot be built for a statement, run it unwrapped: the read
      // degrades to expo-sqlite's double precision instead of failing a query that
      // would otherwise work.
      const maybeWrap = (sql: string) =>
        Effect.withFiber<string, SqlError>((fiber) => {
          const trimmed = sql.replace(/;+\s*$/, "")
          if (!Context.get(fiber.context, Client.SafeIntegers) || !isSafeIntegersRead(trimmed)) {
            return Effect.succeed(sql)
          }
          return Effect.map(
            columnNamesFor(trimmed),
            (names) => names.length === 0 || hasDuplicateNames(names) ? sql : wrapSelectForExactIntegers(trimmed, names)
          ).pipe(
            Effect.catchTag("SqlError", () => Effect.succeed(sql))
          )
        })

      const guardParams = (sql: string, params: ReadonlyArray<unknown>) =>
        hasUnsafeBigint(params)
          ? Effect.fail(
            new SqlError({
              reason: classifySqliteError(new Error(`BigInt bind parameter outside the safe integer range: ${sql}`), {
                message: "Failed to bind parameters",
                operation: "bind"
              })
            })
          )
          : Effect.void

      const run = (
        sql: string,
        params: ReadonlyArray<unknown> = []
      ): Effect.Effect<ReadonlyArray<any>, SqlError> =>
        guardParams(sql, params).pipe(
          Effect.andThen(maybeWrap(sql)),
          Effect.andThen((statement) =>
            Effect.tryPromise({
              try: () => db.getAllAsync(statement, normalizeParams(params) as Array<any>),
              catch: (cause) =>
                new SqlError({
                  reason: classifySqliteError(cause, { message: "Failed to execute statement", operation: "execute" })
                })
            })
          )
        )

      const runValues = (
        sql: string,
        params: ReadonlyArray<unknown> = []
      ): Effect.Effect<ReadonlyArray<ReadonlyArray<unknown>>, SqlError> =>
        guardParams(sql, params).pipe(
          Effect.andThen(maybeWrap(sql)),
          Effect.andThen((statementSql) =>
            Effect.acquireUseRelease(
              Effect.tryPromise({
                try: () => db.prepareAsync(statementSql),
                catch: (cause) =>
                  new SqlError({
                    reason: classifySqliteError(cause, { message: "Failed to prepare statement", operation: "prepare" })
                  })
              }),
              (statement) =>
                Effect.tryPromise({
                  try: async () => {
                    const result = await statement.executeForRawResultAsync(normalizeParams(params) as Array<any>)
                    return await result.getAllAsync()
                  },
                  catch: (cause) =>
                    new SqlError({
                      reason: classifySqliteError(cause, {
                        message: "Failed to execute statement for raw values",
                        operation: "execute"
                      })
                    })
                }),
              (statement) =>
                Effect.tryPromise({
                  try: () => statement.finalizeAsync(),
                  catch: (cause) =>
                    new SqlError({
                      reason: classifySqliteError(cause, {
                        message: "Failed to finalize statement",
                        operation: "finalize"
                      })
                    })
                }).pipe(Effect.orDie)
            )
          )
        )

      return identity<ExpoSqliteConnection>({
        execute(sql, params, transformRows) {
          return transformRows
            ? Effect.map(run(sql, params), transformRows)
            : run(sql, params)
        },
        executeRaw(sql, params) {
          return guardParams(sql, params).pipe(
            Effect.andThen(
              Effect.tryPromise({
                try: () => db.runAsync(sql, normalizeParams(params) as Array<any>),
                catch: (cause) =>
                  new SqlError({
                    reason: classifySqliteError(cause, {
                      message: "Failed to execute raw statement",
                      operation: "executeRaw"
                    })
                  })
              })
            )
          )
        },
        executeValues(sql, params) {
          return runValues(sql, params)
        },
        executeValuesUnprepared(sql, params) {
          return runValues(sql, params)
        },
        executeUnprepared(sql, params, transformRows) {
          return this.execute(sql, params, transformRows)
        },
        executeStream(sql, params, transformRows) {
          return Stream.unwrap(
            guardParams(sql, params).pipe(
              Effect.andThen(maybeWrap(sql)),
              Effect.map((statementSql) => {
                const iterable: AsyncIterableIterator<any> = db.getEachAsync(
                  statementSql,
                  normalizeParams(params) as Array<any>
                )
                return Stream.fromAsyncIterable(
                  iterable,
                  (cause) =>
                    new SqlError({
                      reason: classifySqliteError(cause, { message: "Failed to stream statement", operation: "stream" })
                    })
                )
              })
            )
          ).pipe(
            transformRows ? Stream.mapArray((chunk) => transformRows(chunk) as any) : identity
          )
        }
      })
    })

    const semaphore = yield* Semaphore.make(1)
    const connection = yield* makeConnection

    const acquirer = semaphore.withPermits(1)(Effect.succeed(connection))
    const transactionAcquirer = Effect.uninterruptibleMask((restore) => {
      const fiber = Fiber.getCurrent()!
      const scope = Context.getUnsafe(fiber.context, Scope.Scope)
      return Effect.as(
        Effect.tap(
          restore(semaphore.take(1)),
          () => Scope.addFinalizer(scope, semaphore.release(1))
        ),
        connection
      )
    })

    return Object.assign(
      (yield* Client.make({
        acquirer,
        compiler,
        transactionAcquirer,
        spanAttributes: [
          ...(options.spanAttributes ? Object.entries(options.spanAttributes) : []),
          [ATTR_DB_SYSTEM_NAME, "sqlite"]
        ],
        transformRows
      })) as ExpoSqliteClient,
      {
        [TypeId]: TypeId,
        config: options
      }
    )
  })

/**
 * Builds a layer from an Effect `Config` value, providing both the
 * `ExpoSqliteClient` service and the generic `SqlClient` service.
 *
 * @category layers
 * @since 0.1.0
 */
export const layerConfig = (
  config: Config.Wrap<ExpoSqliteClientConfig>
): Layer.Layer<ExpoSqliteClient | Client.SqlClient, Config.ConfigError | SqlError> =>
  Layer.effectContext(
    Config.unwrap(config).pipe(
      Effect.flatMap(make),
      Effect.map((client) =>
        Context.make(ExpoSqliteClient, client).pipe(
          Context.add(Client.SqlClient, client)
        )
      )
    )
  ).pipe(Layer.provide(Reactivity.layer))

/**
 * Builds a layer from an expo-sqlite client configuration, providing both
 * `ExpoSqliteClient` and the generic `SqlClient` service.
 *
 * @category layers
 * @since 0.1.0
 */
export const layer = (
  config: ExpoSqliteClientConfig
): Layer.Layer<ExpoSqliteClient | Client.SqlClient, SqlError> =>
  Layer.effectContext(
    Effect.map(make(config), (client) =>
      Context.make(ExpoSqliteClient, client).pipe(
        Context.add(Client.SqlClient, client)
      ))
  ).pipe(Layer.provide(Reactivity.layer))
