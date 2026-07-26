import * as Effect from "effect/Effect"

export interface Options<E, R,> {
  readonly prune: Effect.Effect<number, E, R>
  readonly intervalMillis: number
  readonly pruneRowsPerSecond: number
}

const delayAfter = (
  rows: number,
  intervalMillis: number,
  pruneRowsPerSecond: number
): number =>
  Math.max(
    intervalMillis,
    Math.ceil((rows / pruneRowsPerSecond) * 1_000)
  )

export const run = <E, R,>(
  options: Options<E, R>
): Effect.Effect<never, E, R> =>
  Effect.gen(function*() {
    while (true) {
      const rows: number = yield* options.prune
      yield* Effect.sleep(
        delayAfter(rows, options.intervalMillis, options.pruneRowsPerSecond)
      )
    }
  })
