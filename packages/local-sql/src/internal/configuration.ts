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
    onSome: (duration) =>
      Duration.isPositive(duration) && Duration.isFinite(duration)
        ? Effect.succeed(Duration.toMillis(duration))
        : Effect.fail(
          new ReplicaError.InvalidConfiguration({
            option,
            message: `${option} must be a valid positive finite duration`
          })
        )
  })
