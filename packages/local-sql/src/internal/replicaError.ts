import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"

export const metadataMissing = () =>
  Effect.fail(
    new ReplicaError.ReplicaError({
      reason: new ReplicaError.ReplicaMetadataMissing()
    })
  )
