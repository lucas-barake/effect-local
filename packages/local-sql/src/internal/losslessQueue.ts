import type * as Arr from "effect/Array"
import type * as Cause from "effect/Cause"
import * as Channel from "effect/Channel"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Scheduler from "effect/Scheduler"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"

const withoutYield = Effect.provideService(Scheduler.PreventSchedulerYield, true)

export const take = <A, E extends { readonly _tag: string } = never,>(
  queue: Queue.Dequeue<A, E>
): Effect.Effect<A, E> => Effect.suspend(() => Queue.takeUnsafe(queue) ?? withoutYield(Queue.take(queue)))

export const merge = <A, E extends { readonly _tag: string }, R, A2, E2 extends { readonly _tag: string }, R2,>(
  left: Stream.Stream<A, E, R>,
  right: Stream.Stream<A2, E2, R2>
): Stream.Stream<A | A2, E | E2, R | R2> => {
  const transform = Effect.fnUntraced(function*(
    _upstream: unknown,
    _scope: Scope.Scope,
    forkedScope: Scope.Scope
  ) {
    const queue = yield* Queue.bounded<Arr.NonEmptyReadonlyArray<A | A2>, E | E2 | Cause.Done>(0)
    yield* Scope.addFinalizer(forkedScope, Queue.shutdown(queue))
    let finished = 0
    const sides: ReadonlyArray<Stream.Stream<A | A2, E | E2, R | R2>> = [left, right]
    for (const side of sides) {
      yield* Stream.toChannel(side).pipe(
        Channel.runForEach((chunk) => Queue.offer(queue, chunk).pipe(withoutYield, Effect.andThen(Effect.yieldNow))),
        Effect.onExit((exit) => {
          if (exit._tag === "Failure") return Queue.failCause(queue, exit.cause)
          finished++
          if (finished === sides.length) return Queue.end(queue)
          return Effect.void
        }),
        Effect.forkIn(forkedScope)
      )
    }
    return take(queue)
  })
  return Stream.fromChannel(Channel.fromTransformBracket(transform))
}
