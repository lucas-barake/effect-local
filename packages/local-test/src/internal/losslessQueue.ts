import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Scheduler from "effect/Scheduler"

export const take = <A, E extends { readonly _tag: string } = never,>(
  queue: Queue.Dequeue<A, E>
): Effect.Effect<A, E> =>
  Effect.suspend(() =>
    Queue.takeUnsafe(queue) ??
      Queue.take(queue).pipe(Effect.provideService(Scheduler.PreventSchedulerYield, true))
  )
