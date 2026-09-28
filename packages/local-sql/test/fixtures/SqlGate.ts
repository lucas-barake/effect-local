import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import type * as SqlClient from "effect/unstable/sql/SqlClient"
import * as SqlSchema from "effect/unstable/sql/SqlSchema"
import * as Statement from "effect/unstable/sql/Statement"

export type Phase = "before" | "after"

export interface Pause {
  readonly statement: string
  readonly phase: Phase
  readonly rows: ReadonlyArray<unknown>
  readonly release: Deferred.Deferred<void>
}

const isStatement = (value: unknown): value is Statement.Statement<unknown> =>
  Statement.isFragment(value) && Effect.isEffect(value)

export const gateStatements = Effect.fnUntraced(function*(
  sql: SqlClient.SqlClient,
  phasesOf: (statement: string) => ReadonlyArray<Phase>
) {
  const pauses = yield* Queue.unbounded<Pause>()
  const pause = (statement: string, phase: Phase, rows: ReadonlyArray<unknown>) =>
    Deferred.make<void>().pipe(
      Effect.tap((release) => Queue.offer(pauses, { statement, phase, rows, release })),
      Effect.flatMap(Deferred.await)
    )
  const gatedStatement = Effect.fnUntraced(function*(
    text: string,
    phases: ReadonlyArray<Phase>,
    statement: Statement.Statement<unknown>
  ) {
    if (phases.includes("before")) yield* pause(text, "before", [])
    const rows = yield* statement
    if (phases.includes("after")) yield* pause(text, "after", rows)
    return rows
  })
  const gated = new Proxy(sql, {
    apply: (target, thisArg, args: ReadonlyArray<unknown>) => {
      const statement: unknown = Reflect.apply(target, thisArg, args)
      const source = args[0]
      if (!Array.isArray(source) || !isStatement(statement)) return statement
      const text = source.join("?")
      const phases = phasesOf(text)
      if (phases.length === 0) return statement
      return gatedStatement(text, phases, statement)
    }
  })
  return { sql: gated, pauses }
})

const WaiterRow = Schema.Struct({ waiters: Schema.Number })

export const lockWaiters = (observer: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Schema.Void,
    Result: WaiterRow,
    execute: () =>
      observer`SELECT COUNT(*)::int AS waiters FROM pg_stat_activity
        WHERE datname = current_database() AND wait_event_type = 'Lock'`
  })(undefined).pipe(Effect.map((row) => row.waiters))

export const awaitLockWaiters = (observer: SqlClient.SqlClient, waiters: number) =>
  lockWaiters(observer).pipe(Effect.repeat({ until: (count) => count >= waiters }))
