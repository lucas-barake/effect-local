import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Scheduler from "effect/Scheduler"

export const take = <A,>(queue: Queue.Dequeue<A>): Effect.Effect<A> =>
  Effect.suspend(() =>
    Queue.takeUnsafe(queue) ??
      Queue.take(queue).pipe(Effect.provideService(Scheduler.PreventSchedulerYield, true))
  )
