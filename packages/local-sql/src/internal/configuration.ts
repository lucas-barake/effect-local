import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"

export const positiveFiniteDurationMillis = (
  option: string,
  input: Duration.Input
): Effect.Effect<number, ReplicaError.InvalidConfiguration> =>
  Option.match(Duration.fromInput(input), {
    onNone: () =>
      Effect.fail(
        new ReplicaError.InvalidConfiguration({
          option,
          message: `${option} must be a valid positive finite duration`
        })
      ),
    onSome: (duration) => {
      if (Duration.isPositive(duration) && Duration.isFinite(duration)) {
        return Effect.succeed(Duration.toMillis(duration))
      }
      return Effect.fail(
        new ReplicaError.InvalidConfiguration({
          option,
          message: `${option} must be a valid positive finite duration`
        })
      )
    }
  })

export const positiveIntegerDurationMillis = (
  option: string,
  input: Duration.Input
): Effect.Effect<number, ReplicaError.InvalidConfiguration> =>
  positiveFiniteDurationMillis(option, input).pipe(
    Effect.map((millis) => Math.max(1, Math.ceil(millis)))
  )

export const positiveSafeInteger = (
  option: string,
  value: number
): Effect.Effect<number, ReplicaError.InvalidConfiguration> => {
  if (Number.isSafeInteger(value) && value > 0) return Effect.succeed(value)
  return Effect.fail(
    new ReplicaError.InvalidConfiguration({
      option,
      message: `${option} must be a positive safe integer`
    })
  )
}

export interface RetryTiming {
  readonly retryDelayMillis: number
  readonly maximumRetryDelayMillis: number
}

export const retryTiming = Effect.fnUntraced(function*(options: {
  readonly retryDelay?: Duration.Input
  readonly maximumRetryDelay?: Duration.Input
}, optionPrefix = ""): Effect.fn.Return<RetryTiming, ReplicaError.InvalidConfiguration> {
  const retryDelayMillis = yield* positiveFiniteDurationMillis(
    `${optionPrefix}retryDelay`,
    options.retryDelay ?? Duration.seconds(1)
  )
  const maximumRetryDelayMillis = yield* positiveFiniteDurationMillis(
    `${optionPrefix}maximumRetryDelay`,
    options.maximumRetryDelay ?? Duration.minutes(1)
  )
  if (maximumRetryDelayMillis < retryDelayMillis) {
    return yield* new ReplicaError.InvalidConfiguration({
      option: `${optionPrefix}maximumRetryDelay`,
      message: `${optionPrefix}maximumRetryDelay must be greater than or equal to ${optionPrefix}retryDelay`
    })
  }
  return { retryDelayMillis, maximumRetryDelayMillis }
})

export const retryMillis = (timing: RetryTiming, attempt: number) =>
  Math.min(
    timing.maximumRetryDelayMillis,
    timing.retryDelayMillis * 2 ** Math.min(attempt - 1, 52)
  )
