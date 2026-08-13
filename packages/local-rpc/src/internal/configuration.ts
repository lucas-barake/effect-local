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
        const millis = pipe(Duration.toMillis(duration), Math.ceil)
        if (Number.isSafeInteger(millis)) return Effect.succeed(millis)
      }
      return Effect.fail(
        new ReplicaError.InvalidConfiguration({
          option,
          message: `${option} must be a valid positive finite duration`
        })
      )
    }
  }))
