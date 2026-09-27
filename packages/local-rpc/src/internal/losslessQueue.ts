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

export const stream = <A, E extends { readonly _tag: string },>(
  queue: Queue.Dequeue<A, E>
): Stream.Stream<A, Exclude<E, Cause.Done>> =>
  Queue.takeAll(queue).pipe(withoutYield, Effect.succeed, Channel.fromPull, Stream.fromChannel)

const mergeHalting = <A, E extends { readonly _tag: string }, R,>(
  sides: ReadonlyArray<Stream.Stream<A, E, R>>,
  halts: (side: number, finished: number) => boolean
): Stream.Stream<A, E, R> => {
  const transform = Effect.fnUntraced(function*(
    _upstream: unknown,
    _scope: Scope.Scope,
    forkedScope: Scope.Scope
  ) {
    const queue = yield* Queue.bounded<Arr.NonEmptyReadonlyArray<A>, E | Cause.Done>(0)
    yield* Scope.addFinalizer(forkedScope, Queue.shutdown(queue))
    let finished = 0
    for (let side = 0; side < sides.length; side++) {
      yield* Stream.toChannel(sides[side]).pipe(
        Channel.runForEach((chunk) => Queue.offer(queue, chunk).pipe(withoutYield, Effect.andThen(Effect.yieldNow))),
        Effect.onExit((exit) => {
          if (exit._tag === "Failure") return Queue.failCause(queue, exit.cause)
          finished++
          if (halts(side, finished)) return Queue.end(queue)
          return Effect.void
        }),
        Effect.forkIn(forkedScope)
      )
    }
    return take(queue)
  })
  return Stream.fromChannel(Channel.fromTransformBracket(transform))
}

export const merge = <A, E extends { readonly _tag: string }, R, A2, E2 extends { readonly _tag: string }, R2,>(
  left: Stream.Stream<A, E, R>,
  right: Stream.Stream<A2, E2, R2>
): Stream.Stream<A | A2, E | E2, R | R2> =>
  mergeHalting<A | A2, E | E2, R | R2>([left, right], (_side, finished) => finished === 2)

export const mergeEffect = <A, E extends { readonly _tag: string }, R, X, E2 extends { readonly _tag: string }, R2,>(
  self: Stream.Stream<A, E, R>,
  effect: Effect.Effect<X, E2, R2>
): Stream.Stream<A, E | E2, R | R2> =>
  mergeHalting<A, E | E2, R | R2>([self, Stream.drain(Stream.fromEffect(effect))], (side) => side === 0)

export const interruptWhen = <A, E extends { readonly _tag: string }, R, X, E2 extends { readonly _tag: string }, R2,>(
  self: Stream.Stream<A, E, R>,
  effect: Effect.Effect<X, E2, R2>
): Stream.Stream<A, E | E2, R | R2> =>
  mergeHalting<A, E | E2, R | R2>([self, Stream.drain(Stream.fromEffect(effect))], () => true)
