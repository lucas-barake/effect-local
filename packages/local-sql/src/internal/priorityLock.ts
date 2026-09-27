import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import type * as Fiber from "effect/Fiber"
import { constFalse } from "effect/Function"

export const Urgency = Context.Reference<(now: number) => boolean>(
  "@lucas-barake/effect-local-sql/internal/priorityLock/Urgency",
  { defaultValue: () => constFalse }
)

interface Waiter {
  readonly foreground: boolean
  readonly urgent: (now: number) => boolean
  readonly since: number
  granted: boolean
  resume: ((effect: Effect.Effect<void>) => void) | undefined
}

type Restore = <A, E extends { readonly _tag: string }, R,>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>

export interface PriorityLock {
  readonly take: (foreground: boolean, urgent: (now: number) => boolean, restore: Restore) => Effect.Effect<void>
  readonly release: Effect.Effect<void>
  readonly withPermit: <A, E extends { readonly _tag: string }, R,>(
    foreground: boolean,
    urgent: (now: number) => boolean,
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E, R>
  readonly foregroundWaiting: (now: number) => boolean
}

const now = (fiber: Fiber.Fiber<unknown, unknown>) => fiber.getRef(Clock.Clock).currentTimeMillisUnsafe()

export const make = (maximumBackgroundWaitMillis: number): PriorityLock => {
  let held = false
  const waiters: Array<Waiter> = []
  const preferred = (waiter: Waiter, at: number) =>
    waiter.foreground || waiter.urgent(at) || at - waiter.since >= maximumBackgroundWaitMillis

  const releaseUnsafe = (fiber: Fiber.Fiber<unknown, unknown>) => {
    const at = now(fiber)
    let index = waiters.findIndex((waiter) => preferred(waiter, at))
    if (index < 0) index = 0
    const next = waiters[index]
    if (next === undefined) {
      held = false
      return
    }
    waiters.splice(index, 1)
    next.granted = true
    next.resume?.(Effect.void)
  }

  const release = Effect.withFiber((fiber) => {
    releaseUnsafe(fiber)
    return Effect.void
  })

  const abandon = (waiter: Waiter) =>
    Effect.withFiber((fiber) => {
      if (waiter.granted) {
        releaseUnsafe(fiber)
      } else {
        const index = waiters.indexOf(waiter)
        if (index >= 0) waiters.splice(index, 1)
      }
      return Effect.void
    })

  const take = (foreground: boolean, urgent: (now: number) => boolean, restore: Restore) =>
    Effect.withFiber((fiber) => {
      if (!held) {
        held = true
        return Effect.void
      }
      const waiter: Waiter = { foreground, urgent, since: now(fiber), granted: false, resume: undefined }
      waiters.push(waiter)
      return restore(Effect.callback<void>((resume) => {
        if (waiter.granted) {
          resume(Effect.void)
          return
        }
        waiter.resume = resume
      })).pipe(
        Effect.onExit((exit) => {
          if (Exit.isSuccess(exit)) return Effect.void
          return abandon(waiter)
        })
      )
    })

  const withPermit = <A, E extends { readonly _tag: string }, R,>(
    foreground: boolean,
    urgent: (now: number) => boolean,
    effect: Effect.Effect<A, E, R>
  ) =>
    Effect.uninterruptibleMask((restore) => {
      const guarded = restore(effect).pipe(Effect.onExit(() => release))
      return take(foreground, urgent, restore).pipe(Effect.andThen(guarded))
    })

  return {
    take,
    release,
    withPermit,
    foregroundWaiting: (at) => waiters.some((waiter) => preferred(waiter, at))
  }
}
