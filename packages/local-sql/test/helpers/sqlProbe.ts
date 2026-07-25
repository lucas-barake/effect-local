import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Scope from "effect/Scope"
import * as SqlClient from "effect/unstable/sql/SqlClient"

/**
 * A pass-through decorator over the real `SqlClient`.
 *
 * Every statement is forwarded to the underlying driver with `Reflect.apply`, so
 * production wiring is untouched: this observes a true external boundary rather
 * than replacing one. It exists so tests can assert how much durable work a code
 * path performs, and so they can inject the class of driver fault documented at
 * `packages/local-browser/src/BrowserSqlite.ts:28`.
 */
export interface SqlProbe {
  /** Normalized text of every statement issued through the decorated client. */
  readonly statements: Array<string>
  /** Runs before a matching statement, on the same connection. */
  before: ((text: string) => Effect.Effect<unknown, unknown, never> | undefined) | undefined
  /** Rewrites the interpolated parameters of a matching statement. */
  mapParams: ((text: string, params: Array<unknown>) => Array<unknown>) | undefined
  reset(): void
  /**
   * Number of reads of a document's entire retained change history, i.e. the
   * reconstruction reads `Recovery` issues. Matched positively on that query's
   * ordering so that a reshaped reconstruction stops being counted and the
   * assertion fails, rather than silently counting something else.
   */
  countHistoryReads(): number
}

/**
 * Installs fault hooks for the duration of a scope. Assigning them directly
 * leaks them into later tests whenever a test fails before its cleanup runs,
 * which makes failures cascade and can also mask a missing fault.
 */
export const withFault = (probe: SqlProbe, fault: {
  readonly before?: SqlProbe["before"]
  readonly mapParams?: SqlProbe["mapParams"]
}): Effect.Effect<void, never, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.sync(() => {
      probe.before = fault.before
      probe.mapParams = fault.mapParams
    }),
    () => Effect.sync(() => probe.reset())
  )

export const makeProbe = (): SqlProbe => {
  const statements: Array<string> = []
  return {
    statements,
    before: undefined,
    mapParams: undefined,
    reset() {
      statements.length = 0
      this.before = undefined
      this.mapParams = undefined
    },
    countHistoryReads() {
      return statements.filter((text) =>
        text.includes("FROM effect_local_changes") &&
        text.includes("ORDER BY commit_sequence, sequence, change_hash")
      ).length
    }
  }
}

const normalize = (strings: ReadonlyArray<string>): string => strings.join("?").replace(/\s+/g, " ").trim()

/**
 * Wraps whatever `SqlClient` is already in context. Provide the real driver layer
 * underneath, e.g. `probeLayer(probe).pipe(Layer.provide(SqliteClient.layer(...)))`.
 */
export const probeLayer = (probe: SqlProbe): Layer.Layer<SqlClient.SqlClient, never, SqlClient.SqlClient> =>
  Layer.effect(
    SqlClient.SqlClient,
    Effect.map(SqlClient.SqlClient, (sql) =>
      new Proxy(sql, {
        apply(target, thisArg, args: Array<unknown>) {
          const strings = args[0]
          if (!Array.isArray(strings)) return Reflect.apply(target as never, thisArg, args)
          const text = normalize(strings as ReadonlyArray<string>)
          probe.statements.push(text)
          const params = probe.mapParams === undefined
            ? args.slice(1)
            : probe.mapParams(text, args.slice(1))
          const statement = Reflect.apply(target as never, thisArg, [strings, ...params]) as Effect.Effect<
            unknown,
            unknown,
            never
          >
          const before = probe.before?.(text)
          return before === undefined ? statement : Effect.andThen(before, statement)
        }
      }) as typeof sql)
  )
