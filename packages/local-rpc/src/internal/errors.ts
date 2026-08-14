import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"

export const failInvalidConfiguration = Effect.fnUntraced(function*(option: string, message: string) {
  return yield* new ReplicaError.InvalidConfiguration({ option, message })
})

export const failCapacityExceeded = Effect.fnUntraced(function*(resource: string, limit: number) {
  return yield* new ReplicaError.CapacityExceeded({ resource, limit })
})
