import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as SqlClient from "effect/unstable/sql/SqlClient"
import type * as SqlError from "effect/unstable/sql/SqlError"
import * as Configuration from "./internal/configuration.js"
import * as PriorityLock from "./internal/priorityLock.js"

export type Priority = "Foreground" | "Background"

export const Priority = Context.Reference<Priority>("@lucas-barake/effect-local-sql/ConnectionLane/Priority", {
  defaultValue: () => "Foreground"
})

export const defaultMaximumBackgroundWait: Duration.Input = "50 millis"

export interface Options {
  readonly maximumBackgroundWait?: Duration.Input | undefined
}

export interface Service {
  readonly withTransaction: <A, E extends { readonly _tag: string }, R,>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E | SqlError.SqlError, R>
  readonly withStatement: <A, E extends { readonly _tag: string }, R,>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E, R>
  readonly foregroundWaiting: Effect.Effect<boolean>
}

export class ConnectionLane extends Context.Service<ConnectionLane, Service>()(
  "@lucas-barake/effect-local-sql/ConnectionLane"
) {}

export const make = Effect.fnUntraced(function*(
  options: Options = {}
): Effect.fn.Return<Service, ReplicaError.InvalidConfiguration, SqlClient.SqlClient> {
  const sql = yield* SqlClient.SqlClient
  const maximumBackgroundWaitMillis = yield* Configuration.positiveFiniteDurationMillis(
    "maximumBackgroundWait",
    options.maximumBackgroundWait ?? defaultMaximumBackgroundWait
  )
  const lock = PriorityLock.make(maximumBackgroundWaitMillis)
  const withStatement = <A, E extends { readonly _tag: string }, R,>(
    effect: Effect.Effect<A, E, R>
  ): Effect.Effect<A, E, R> =>
    Effect.withFiber((fiber) => {
      if (Option.isSome(Context.getOption(fiber.context, sql.transactionService))) return effect
      const foreground = fiber.getRef(Priority) === "Foreground"
      return lock.withPermit(foreground, fiber.getRef(PriorityLock.Urgency), effect)
    })
  return ConnectionLane.of({
    withTransaction: (effect) => withStatement(sql.withTransaction(effect)),
    withStatement,
    foregroundWaiting: Effect.withFiberSucceed((fiber) => {
      const now = fiber.getRef(Clock.Clock).currentTimeMillisUnsafe()
      return lock.foregroundWaiting(now)
    })
  })
})

export const makeLayer = (
  options: Options = {}
): Layer.Layer<ConnectionLane, ReplicaError.InvalidConfiguration, SqlClient.SqlClient> =>
  Layer.effect(ConnectionLane, make(options))
