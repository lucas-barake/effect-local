/**
 * Node-backed fake of the `expo-sqlite` JS API surface, used only in tests via
 * the vitest module alias. expo-sqlite's own Node entry is an explicit dummy
 * stub, so this module re-implements the exact JS-level contract the driver
 * consumes on top of `node:sqlite`:
 *
 * - param normalization mirrors expo-sqlite's `paramUtils` (booleans become
 *   1/0, `undefined` becomes `null`, ArrayBuffer becomes a blob view) and
 *   rejects `bigint`, which the real native binding cannot transport while
 *   node:sqlite would silently accept it
 * - `runAsync` renames node:sqlite's `lastInsertRowid` to `lastInsertRowId`
 *   and coerces bigint metadata to numbers, matching the native module
 * - every call is async even though node:sqlite is synchronous, because the
 *   real module always crosses the native bridge
 */
import { mkdirSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { DatabaseSync, type StatementSync } from "node:sqlite"

let openHandles = 0
let preparedStatements = 0

let openProbe:
  | {
    readonly beforeOpen?: (() => Promise<void>) | undefined
    readonly afterOpen?: (() => void) | undefined
  }
  | undefined

export const openHandleCount = (): number => openHandles

export const preparedStatementCount = (): number => preparedStatements

export const resetPreparedStatementCount = (): void => {
  preparedStatements = 0
}

export const setOpenProbe = (probe: typeof openProbe): void => {
  openProbe = probe
}

export interface SQLiteRunResult {
  readonly lastInsertRowId: number
  readonly changes: number
}

export interface SQLiteOpenOptions {
  readonly enableChangeListener?: boolean
  readonly useNewConnection?: boolean
}

const normalizeValue = (value: unknown): null | number | string | Uint8Array => {
  if (typeof value === "boolean") return value ? 1 : 0
  if (typeof value === "bigint") {
    throw new Error("unsupported bind parameter type: bigint")
  }
  if (value === undefined || value === null) return null
  if (value instanceof ArrayBuffer) return new Uint8Array(value)
  if (value instanceof Uint8Array) return value
  if (typeof value === "string" || typeof value === "number") return value
  throw new Error(`unsupported bind parameter type: ${typeof value}`)
}

const normalizeParams = (params: ReadonlyArray<unknown>): Array<null | number | string | Uint8Array> => {
  const flat = params.length === 1 && Array.isArray(params[0]) ? params[0] : params
  return flat.map(normalizeValue)
}

// expo-sqlite's native bridge marshals every INTEGER column into a JS number (double),
// so values above 2^53 come back rounded in production. The fake mirrors exactly that,
// which is what keeps the driver's SafeIntegers CAST-wrap test honest: an unwrapped huge
// integer fails the exact-text assertions the same way on both sides of the boundary.
const readValue = (value: unknown): unknown => typeof value === "bigint" ? Number(value) : value

const readRow = <T,>(row: T): T => {
  if (Array.isArray(row)) return row.map(readValue) as T
  if (row !== null && typeof row === "object") {
    return Object.fromEntries(Object.entries(row).map(([key, value]) => [key, readValue(value)])) as T
  }
  return row
}

class SQLiteStatement {
  private finalized = false
  constructor(private readonly statement: StatementSync) {}

  private result<T,>(rows: Array<T>): AsyncIterableIterator<T> & { getAllAsync: () => Promise<Array<T>> } {
    let index = 0
    let drained = false
    return {
      [Symbol.asyncIterator]() {
        return this
      },
      next: async () => {
        if (drained || index >= rows.length) return { done: true, value: undefined }
        return { done: false, value: rows[index++] }
      },
      getAllAsync: () => {
        if (drained || index > 0) return Promise.reject(new Error("The statement cursor has already been consumed"))
        drained = true
        return Promise.resolve(rows)
      }
    }
  }

  executeAsync<T,>(...params: ReadonlyArray<unknown>): Promise<
    AsyncIterableIterator<T> & {
      getAllAsync: () => Promise<Array<T>>
    }
  > {
    if (this.finalized) return Promise.reject(new Error("statement is finalized"))
    const normalized = normalizeParams(params)
    return Promise.resolve().then(() => {
      this.statement.setReadBigInts(true)
      if (this.statement.columns().length === 0) {
        this.statement.run(...normalized)
        return this.result<T>([])
      }
      return this.result((this.statement.all(...normalized) as Array<T>).map(readRow))
    })
  }

  executeForRawResultAsync(
    ...params: ReadonlyArray<unknown>
  ): Promise<{ getAllAsync: () => Promise<Array<Array<unknown>>> }> {
    if (this.finalized) return Promise.reject(new Error("statement is finalized"))
    const statement = this.statement
    const normalized = normalizeParams(params)
    // The real implementation executes eagerly at this call, so errors surface here
    // and the result cursor can be drained exactly once.
    return Promise.resolve().then(() => {
      statement.setReadBigInts(true)
      statement.setReturnArrays(true)
      let rows: Array<Array<unknown>>
      try {
        rows = (statement.all(...normalized) as unknown as Array<Array<unknown>>).map(readRow)
      } finally {
        statement.setReturnArrays(false)
      }
      return this.result(rows)
    })
  }

  getColumnNamesAsync(): Promise<Array<string>> {
    if (this.finalized) return Promise.reject(new Error("statement is finalized"))
    return Promise.resolve(this.statement.columns().map((column) => column.name))
  }

  finalizeAsync(): Promise<void> {
    this.finalized = true
    return Promise.resolve()
  }
}

export class SQLiteDatabase {
  constructor(private readonly db: DatabaseSync) {}

  private prepare(source: string): StatementSync {
    preparedStatements++
    return this.db.prepare(source)
  }

  getAllAsync<T,>(source: string, ...params: ReadonlyArray<unknown>): Promise<Array<T>> {
    const normalized = normalizeParams(params)
    return Promise.resolve().then(() => {
      const statement = this.prepare(source)
      statement.setReadBigInts(true)
      if (statement.columns().length > 0) {
        return (statement.all(...normalized) as Array<T>).map(readRow)
      }
      statement.run(...normalized)
      return [] as Array<T>
    })
  }

  runAsync(source: string, ...params: ReadonlyArray<unknown>): Promise<SQLiteRunResult> {
    const normalized = normalizeParams(params)
    return Promise.resolve().then(() => {
      const result = this.prepare(source).run(...normalized)
      return { changes: Number(result.changes), lastInsertRowId: Number(result.lastInsertRowid) }
    })
  }

  prepareAsync(source: string): Promise<SQLiteStatement> {
    return Promise.resolve().then(() => new SQLiteStatement(this.prepare(source)))
  }

  execAsync(source: string): Promise<void> {
    return Promise.resolve().then(() => this.db.exec(source))
  }

  closeAsync(): Promise<void> {
    return Promise.resolve().then(() => {
      this.db.close()
      openHandles--
    })
  }

  async *getEachAsync<T,>(source: string, ...params: ReadonlyArray<unknown>): AsyncIterableIterator<T> {
    const statement = this.prepare(source)
    statement.setReadBigInts(true)
    const normalized = normalizeParams(params)
    // node:sqlite statements need no explicit finalization, so an early break only ends
    // iteration here; the real module finalizes the native statement in that case.
    for (const row of statement.iterate(...normalized)) {
      yield readRow(row as T)
    }
  }
}

export const openDatabaseAsync = (
  databaseName: string,
  _options?: SQLiteOpenOptions,
  directory?: string
): Promise<SQLiteDatabase> =>
  Promise.resolve().then(async () => {
    await openProbe?.beforeOpen?.()
    const filename = databaseName === ":memory:"
      ? ":memory:"
      : join(directory ?? tmpdir(), databaseName)
    if (filename !== ":memory:") mkdirSync(join(filename, ".."), { recursive: true })
    openHandles++
    const database = new SQLiteDatabase(new DatabaseSync(filename))
    openProbe?.afterOpen?.()
    return database
  })
