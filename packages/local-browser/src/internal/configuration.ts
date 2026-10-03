import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import { invalidConfiguration } from "./errors.js"

export const positiveFiniteDurationMillis = (
  option: string,
  input: Duration.Input
): Effect.Effect<number, ReplicaError.InvalidConfiguration> =>
  Option.match(Duration.fromInput(input), {
    onNone: () => Effect.fail(invalidConfiguration(option, `${option} must be a valid positive finite duration`)),
    onSome: (duration) => {
      if (Duration.isPositive(duration) && Duration.isFinite(duration)) {
        return Effect.succeed(Duration.toMillis(duration))
      }
      return Effect.fail(invalidConfiguration(option, `${option} must be a valid positive finite duration`))
    }
  })

export const boundedTtlMillis = Effect.fnUntraced(function*(
  input: Duration.Input,
  minimum: number,
  maximum: number
) {
  const millis = Math.ceil(yield* positiveFiniteDurationMillis("ttl", input))
  if (!Number.isSafeInteger(millis)) {
    return yield* invalidConfiguration("ttl", "ttl must be a valid positive finite duration")
  }
  if (millis >= minimum && millis <= maximum) return millis
  return yield* invalidConfiguration(
    "ttl",
    `ttl must resolve to between ${minimum} and ${maximum} milliseconds`
  )
})
