import type * as Cause from "effect/Cause"
import * as Channel from "effect/Channel"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Scheduler from "effect/Scheduler"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"

const withoutYield = Effect.provideService(Scheduler.PreventSchedulerYield, true)

export const take = <A,>(queue: Queue.Dequeue<A>): Effect.Effect<A> =>
  Effect.suspend(() => Queue.takeUnsafe(queue) ?? withoutYield(Queue.take(queue)))

export const stream = <A, E extends Cause.Done = never,>(
  queue: Queue.Dequeue<A, E>
): Stream.Stream<A, Exclude<E, Cause.Done>> =>
  Queue.takeAll(queue).pipe(withoutYield, Effect.succeed, Channel.fromPull, Stream.fromChannel)

export const callback = <A,>(
  register: (queue: Queue.Queue<A, Cause.Done>) => Effect.Effect<unknown, never, Scope.Scope>,
  options: { readonly bufferSize: number; readonly strategy: "sliding" | "dropping" | "suspend" }
): Stream.Stream<A> =>
  Stream.unwrap(Effect.gen(function*() {
    const queue = yield* Queue.make<A, Cause.Done>({ capacity: options.bufferSize, strategy: options.strategy })
    yield* Effect.addFinalizer(() => Queue.shutdown(queue))
    yield* Effect.forkScoped(register(queue))
    return stream(queue)
  }))
