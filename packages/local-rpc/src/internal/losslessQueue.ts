import type * as Cause from "effect/Cause"
import * as Channel from "effect/Channel"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Scheduler from "effect/Scheduler"
import * as Stream from "effect/Stream"

export const stream = <A, E extends { readonly _tag: string },>(
  queue: Queue.Dequeue<A, E>
): Stream.Stream<A, Exclude<E, Cause.Done>> =>
  Queue.takeAll(queue).pipe(
    Effect.provideService(Scheduler.PreventSchedulerYield, true),
    Effect.succeed,
    Channel.fromPull,
    Stream.fromChannel
  )
