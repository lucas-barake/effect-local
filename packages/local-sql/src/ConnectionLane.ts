import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import type * as Fiber from "effect/Fiber"
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
  readonly withSession: <A, E extends { readonly _tag: string }, R,>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E, R>
  readonly foregroundWaiting: Effect.Effect<boolean>
}

interface Session {
  holding: boolean
  since: number
}

interface Turn {
  readonly lock: PriorityLock.PriorityLock
  readonly owner: Fiber.Fiber<unknown, unknown>
  readonly session: Session | undefined
}

const CurrentTurns = Context.Reference<ReadonlyArray<Turn>>(
  "@lucas-barake/effect-local-sql/ConnectionLane/CurrentTurns",
  { defaultValue: () => [] }
)

const now = (fiber: Fiber.Fiber<unknown, unknown>) => fiber.getRef(Clock.Clock).currentTimeMillisUnsafe()

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
  const inTransaction = (fiber: Fiber.Fiber<unknown, unknown>) =>
    Option.isSome(Context.getOption(fiber.context, sql.transactionService))
  const ownedTurn = (fiber: Fiber.Fiber<unknown, unknown>) => {
    const turn = fiber.getRef(CurrentTurns).findLast((candidate) => candidate.lock === lock)
    if (turn === undefined || turn.owner !== fiber) return undefined
    return turn
  }
  const withTurn = <A, E extends { readonly _tag: string }, R,>(
    fiber: Fiber.Fiber<unknown, unknown>,
    session: Session | undefined,
    effect: Effect.Effect<A, E, R>
  ) => Effect.provideService(effect, CurrentTurns, [...fiber.getRef(CurrentTurns), { lock, owner: fiber, session }])
  const takeTurn = (fiber: Fiber.Fiber<unknown, unknown>, restore: PriorityLock.Restore) =>
    lock.take(fiber.getRef(Priority) === "Foreground", fiber.getRef(PriorityLock.Urgency), restore)
  const inSession = <A, E extends { readonly _tag: string }, R,>(
    session: Session,
    fiber: Fiber.Fiber<unknown, unknown>,
    effect: Effect.Effect<A, E, R>
  ) =>
    Effect.uninterruptibleMask((restore) => {
      const take = takeTurn(fiber, restore).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            session.holding = true
            session.since = now(fiber)
          })
        )
      )
      const turn = Effect.suspend(() => {
        if (!session.holding) return take
        const at = now(fiber)
        if (at - session.since < maximumBackgroundWaitMillis || !lock.foregroundWaiting(at)) return Effect.void
        session.holding = false
        return lock.release.pipe(Effect.andThen(take))
      })
      return turn.pipe(Effect.andThen(restore(effect)))
    })
  const withStatement = <A, E extends { readonly _tag: string }, R,>(
    effect: Effect.Effect<A, E, R>
  ): Effect.Effect<A, E, R> =>
    Effect.withFiber((fiber) => {
      if (inTransaction(fiber)) return effect
      const turn = ownedTurn(fiber)
      const held = withTurn(fiber, undefined, effect)
      if (turn === undefined) {
        const foreground = fiber.getRef(Priority) === "Foreground"
        return lock.withPermit(foreground, fiber.getRef(PriorityLock.Urgency), held)
      }
      if (turn.session === undefined) return effect
      return inSession(turn.session, fiber, held)
    })
  const withSession = <A, E extends { readonly _tag: string }, R,>(
    effect: Effect.Effect<A, E, R>
  ): Effect.Effect<A, E, R> =>
    Effect.withFiber((fiber) => {
      if (inTransaction(fiber) || ownedTurn(fiber) !== undefined) return effect
      const session: Session = { holding: false, since: 0 }
      const end = Effect.suspend(() => {
        if (!session.holding) return Effect.void
        session.holding = false
        return lock.release
      })
      return withTurn(fiber, session, effect).pipe(Effect.ensuring(end))
    })
  return ConnectionLane.of({
    withTransaction: (effect) => withStatement(sql.withTransaction(effect)),
    withStatement,
    withSession,
    foregroundWaiting: Effect.withFiberSucceed((fiber) => lock.foregroundWaiting(now(fiber)))
  })
})

export const makeLayer = (
  options: Options = {}
): Layer.Layer<ConnectionLane, ReplicaError.InvalidConfiguration, SqlClient.SqlClient> =>
  Layer.effect(ConnectionLane, make(options))
