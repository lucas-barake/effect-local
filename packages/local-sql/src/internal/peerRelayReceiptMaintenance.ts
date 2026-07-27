import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"

export interface Options<E, R,> {
  readonly prune: Effect.Effect<number, E, R>
  readonly intervalMillis: number
  readonly pruneRowsPerSecond: number
  readonly wakeup: Queue.Dequeue<void>
}

const rateDelay = (rows: number, pruneRowsPerSecond: number): number => Math.ceil((rows / pruneRowsPerSecond) * 1_000)

export const run = <E, R,>(
  options: Options<E, R>
): Effect.Effect<never, E, R> =>
  Effect.gen(function*() {
    while (true) {
      yield* Queue.take(options.wakeup).pipe(
        Effect.raceFirst(Effect.sleep(options.intervalMillis))
      )
      const rows: number = yield* options.prune
      const delay = rateDelay(rows, options.pruneRowsPerSecond)
      if (delay > 0) yield* Effect.sleep(delay)
    }
  })
