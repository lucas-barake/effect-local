import { DatabaseSync, type StatementSync } from "node:sqlite"

type Platform = "ios" | "android"
type Row = ReadonlyArray<unknown>

interface Signal {
  readonly promise: Promise<void>
  readonly resolve: () => void
}

const signal = (): Signal => {
  let resolve = () => {}
  const promise = new Promise<void>((settle) => {
    resolve = settle
  })
  return { promise, resolve }
}

interface Hold {
  readonly method: string
  readonly entered: Signal
  readonly release: Signal
}

interface Probe {
  platform: Platform
  inFlight: number
  maxInFlight: number
  calls: Array<string>
  opened: Array<{ readonly path: string; readonly useNewConnection: boolean }>
  openDatabases: number
  holds: Array<Hold>
  reset: () => void
  hold: (method: string) => { readonly entered: Promise<void>; readonly release: () => void }
}

export const probe: Probe = {
  platform: "ios",
  inFlight: 0,
  maxInFlight: 0,
  calls: [],
  opened: [],
  openDatabases: 0,
  holds: [],
  reset() {
    probe.platform = "ios"
    probe.inFlight = 0
    probe.maxInFlight = 0
    probe.calls = []
    probe.opened = []
    probe.openDatabases = 0
    probe.holds = []
  },
  hold(method: string) {
    const hold: Hold = { method, entered: signal(), release: signal() }
    probe.holds.push(hold)
    return { entered: hold.entered.promise, release: () => hold.release.resolve() }
  }
}

const nativeError = (method: string, cause: unknown): Error => {
  let code = 1
  let message = String(cause)
  if (cause instanceof Error) {
    message = cause.message
    const errcode = Reflect.get(cause, "errcode")
    if (typeof errcode === "number") code = errcode & 0xff
  }
  let text = `Calling the '${method}' function has failed\n→ Caused by: Error code ${code}: ${message}`
  if (probe.platform === "android") {
    text = `Call to function 'ExpoSQLite.${method}' has been rejected.\n→ Caused by: Error code ${
      String.fromCharCode(code)
    }: ${message}`
  }
  return Object.assign(new Error(text), { code: "ERR_INTERNAL_SQLITE_ERROR" })
}

const native = <A,>(method: string, evaluate: () => A): Promise<A> => {
  probe.calls.push(method)
  probe.inFlight++
  probe.maxInFlight = Math.max(probe.maxInFlight, probe.inFlight)
  const index = probe.holds.findIndex((hold) => hold.method === method)
  let gate = Promise.resolve()
  if (index !== -1) {
    const [hold] = probe.holds.splice(index, 1)
    hold.entered.resolve()
    gate = hold.release.promise
  }
  return gate
    .then(() => new Promise<void>((resolve) => setImmediate(resolve)))
    .then(() => {
      try {
        return evaluate()
      } catch (cause) {
        throw nativeError(method, cause)
      }
    })
    .finally(() => {
      probe.inFlight--
    })
}

const toNativeValue = (value: unknown): unknown => {
  if (typeof value === "bigint") return Number(value)
  if (value instanceof Uint8Array) return value.slice().buffer
  return value
}

const toNativeRow = (row: unknown): Row => {
  if (!Array.isArray(row)) throw new Error("node:sqlite returned a row object; expected setReturnArrays(true)")
  return row.map(toNativeValue)
}

class NativeDatabase {
  readonly databasePath: string
  readonly options: Record<string, unknown>
  database: DatabaseSync | undefined
  readonly statements = new Set<NativeStatement>()
  constructor(databasePath: string, options: Record<string, unknown>) {
    this.databasePath = databasePath
    this.options = options
  }
  initAsync() {
    return native("initAsync", () => {
      this.database = new DatabaseSync(this.databasePath)
      probe.openDatabases++
      probe.opened.push({ path: this.databasePath, useNewConnection: this.options.useNewConnection === true })
    })
  }
  isInTransactionAsync() {
    return native("isInTransactionAsync", () => this.open().isTransaction)
  }
  closeAsync() {
    return native("closeAsync", () => {
      for (const statement of this.statements) statement.finalizeNow()
      this.open().close()
      this.database = undefined
      probe.openDatabases--
    })
  }
  execAsync(source: string) {
    return native("execAsync", () => this.open().exec(source))
  }
  prepareAsync(statement: NativeStatement, source: string) {
    return native("prepareAsync", () => {
      statement.prepared = this.open().prepare(source)
      statement.prepared.setReadBigInts(true)
      statement.prepared.setReturnArrays(true)
      this.statements.add(statement)
      statement.owner = this
    })
  }
  open(): DatabaseSync {
    if (this.database === undefined) throw new Error("Access to closed resource")
    return this.database
  }
}

const longMin = -(2n ** 63n)
const longMax = 2n ** 63n - 1n

const bindValue = (value: unknown) => {
  if (probe.platform !== "android" || typeof value !== "number" || !Number.isInteger(value)) return value
  const long = BigInt(value)
  if (long < longMin) return longMin
  if (long > longMax) return longMax
  return long
}

class NativeStatement {
  prepared: StatementSync | undefined
  owner: NativeDatabase | undefined
  iterator: Iterator<unknown> | undefined
  args: Array<any> = []
  halted = false
  failure: unknown
  runAsync(
    _database: NativeDatabase,
    bindParams: Record<string, unknown>,
    bindBlobParams: Record<string, unknown>,
    shouldPassAsArray: boolean
  ) {
    return native("runAsync", () => {
      const statement = this.statement()
      const merged: Record<string, unknown> = { ...bindParams }
      for (const [key, value] of Object.entries(bindBlobParams)) {
        if (value instanceof ArrayBuffer) merged[key] = new Uint8Array(value)
        else merged[key] = value
      }
      let args: Array<any> = []
      if (shouldPassAsArray) {
        args = Array.from({ length: Object.keys(merged).length }, (_, index) => bindValue(merged[String(index)]))
      }
      this.failure = undefined
      this.args = args
      this.halted = false
      this.iterator = statement.iterate(...args)
      const first = this.step()
      const counters = this.owner!.open().prepare("SELECT changes() AS changes, last_insert_rowid() AS id").get()
      const changes = counters?.changes
      const id = counters?.id
      let firstRowValues: Row = []
      if (first.done !== true) firstRowValues = toNativeRow(first.value)
      return { lastInsertRowId: Number(id), changes: Number(changes), firstRowValues }
    })
  }
  stepAsync(_database: NativeDatabase) {
    return native("stepAsync", () => {
      const next = this.step()
      if (next.done === true) return null
      return toNativeRow(next.value)
    })
  }
  getAllAsync(_database: NativeDatabase) {
    return native("getAllAsync", () => {
      const rows: Array<Row> = []
      for (let next = this.step(); next.done !== true; next = this.step()) rows.push(toNativeRow(next.value))
      return rows
    })
  }
  getColumnNamesAsync() {
    return native("getColumnNamesAsync", () => this.statement().columns().map((column) => column.name))
  }
  resetAsync(_database: NativeDatabase) {
    return native("resetAsync", () => {
      this.iterator?.return?.()
      this.iterator = undefined
      this.halted = false
      const failure = this.failure
      this.failure = undefined
      if (failure !== undefined) throw failure
    })
  }
  finalizeAsync(_database: NativeDatabase) {
    return native("finalizeAsync", () => {
      const failure = this.failure
      this.finalizeNow()
      if (failure !== undefined) throw failure
    })
  }
  finalizeNow() {
    this.iterator?.return?.()
    this.iterator = undefined
    this.owner?.statements.delete(this)
    this.prepared = undefined
  }
  statement(): StatementSync {
    if (this.prepared === undefined) throw new Error("Access to closed resource")
    return this.prepared
  }
  step(): IteratorResult<unknown> {
    try {
      if (this.halted) {
        this.halted = false
        this.iterator = this.statement().iterate(...this.args)
      }
      const next = this.cursor().next()
      if (next.done === true) this.halted = true
      return next
    } catch (cause) {
      this.failure = cause
      throw cause
    }
  }
  cursor(): Iterator<unknown> {
    if (this.iterator === undefined) throw new Error("The statement has not been run")
    return this.iterator
  }
}

export const ExpoSQLite = {
  defaultDatabaseDirectory: ".",
  bundledExtensions: {},
  ensureDatabasePathExistsAsync: (_path: string) => native("ensureDatabasePathExistsAsync", () => undefined),
  NativeDatabase,
  NativeStatement
}
