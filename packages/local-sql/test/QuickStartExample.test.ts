import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { DomainLive, ListTasks } from "../examples/domain.js"

describe("quick start example", () => {
  const DatabaseLive = SqliteClient.layer({ filename: ":memory:", disableWAL: true })
  const HandlerLive = DomainLive.pipe(Layer.provide(DatabaseLive))

  it.effect("keeps query storage failures in the typed error channel", () =>
    Effect.gen(function*() {
      const handler = yield* ListTasks.handler
      const exit = yield* Effect.exit(handler({ search: "" }))

      if (!Exit.isFailure(exit)) return assert.fail("the missing projection table must fail")
      const failure = Cause.findErrorOption(exit.cause)
      if (Option.isNone(failure)) return assert.fail("expected a typed failure")
    }).pipe(Effect.provide(HandlerLive)))
})
