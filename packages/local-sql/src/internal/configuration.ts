import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import { pipe } from "effect/Function"
import * as Option from "effect/Option"

export const positiveFiniteDurationMillis = (
  option: string,
  input: Duration.Input
): Effect.Effect<number, ReplicaError.InvalidConfiguration> =>
  Duration.fromInput(input).pipe(Option.match({
    onNone: () =>
      Effect.fail(
        new ReplicaError.InvalidConfiguration({
          option,
          message: `${option} must be a valid positive finite duration`
        })
      ),
    onSome: (duration) => {
      if (Duration.isPositive(duration) && Duration.isFinite(duration)) {
        return pipe(Duration.toMillis(duration), Effect.succeed)
      }
      return Effect.fail(
        new ReplicaError.InvalidConfiguration({
          option,
          message: `${option} must be a valid positive finite duration`
        })
      )
    }
  }))

export interface RetryTiming {
  readonly retryDelayMillis: number
  readonly maximumRetryDelayMillis: number
}

export const retryTiming = (options: {
  readonly retryDelay?: Duration.Input
  readonly maximumRetryDelay?: Duration.Input
}): Effect.Effect<RetryTiming, ReplicaError.InvalidConfiguration> =>
  Effect.gen(function*() {
    const retryDelayMillis = yield* positiveFiniteDurationMillis(
      "retryDelay",
      options.retryDelay ?? Duration.seconds(1)
    )
    const maximumRetryDelayMillis = yield* positiveFiniteDurationMillis(
      "maximumRetryDelay",
      options.maximumRetryDelay ?? Duration.minutes(1)
    )
    if (maximumRetryDelayMillis < retryDelayMillis) {
      return yield* new ReplicaError.InvalidConfiguration({
        option: "maximumRetryDelay",
        message: "maximumRetryDelay must be greater than or equal to retryDelay"
      })
    }
    return { retryDelayMillis, maximumRetryDelayMillis }
  })

export const retryMillis = (timing: RetryTiming, attempt: number) =>
  Math.min(
    timing.maximumRetryDelayMillis,
    timing.retryDelayMillis * 2 ** Math.min(attempt - 1, 52)
  )
