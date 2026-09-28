import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Random from "effect/Random"
import * as Schedule from "effect/Schedule"
import { invalidConfiguration } from "./errors.js"

export const positiveFiniteDurationMillis = (
  option: string,
  input: Duration.Input
): Effect.Effect<number, ReplicaError.InvalidConfiguration> =>
  Option.match(Duration.fromInput(input), {
    onNone: () =>
      Effect.fail(
        invalidConfiguration(option, `${option} must be a valid positive finite duration`)
      ),
    onSome: (duration) => {
      if (Duration.isPositive(duration) && Duration.isFinite(duration)) {
        const millis = Math.ceil(Duration.toMillis(duration))
        if (Number.isSafeInteger(millis)) return Effect.succeed(millis)
      }
      return Effect.fail(
        invalidConfiguration(option, `${option} must be a valid positive finite duration`)
      )
    }
  })

export const reconnectPolicy = Schedule.min([
  Schedule.exponential(250, 2),
  Schedule.spaced(2000)
]).pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.map(Random.next, (random) => Duration.times(duration, 0.5 + random / 2))
  )
)
