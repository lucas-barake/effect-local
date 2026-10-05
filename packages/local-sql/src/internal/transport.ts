import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import type * as SyncEngine from "../SyncEngine.js"
import * as Errors from "./errors.js"

export const isTransportFailure = (error: ReplicaError.ReplicaError) =>
  error._tag === "ServerUnavailable" || error._tag === "OperationTimeout"

export const backoff = (
  remote: SyncEngine.Service,
  delay: number,
  error: ReplicaError.ReplicaError,
  transportGeneration: number
) => {
  if (!isTransportFailure(error)) return Effect.sleep(delay)
  return Effect.raceFirst(Effect.sleep(delay), remote.waitForTransportChange(transportGeneration))
}

export const credentialChange = (
  remote: SyncEngine.Service,
  rejectedGeneration: number,
  retryDelayMillis: number
): Effect.Effect<void> => {
  let reported = false
  const wait: Effect.Effect<void> = Effect.suspend(() => remote.waitForCredentialChange(rejectedGeneration)).pipe(
    Effect.catchCause((cause) => {
      let logged = Effect.void
      if (!reported && Errors.causeKind(cause) === "Defect") {
        reported = true
        logged = Effect.logError("Credential wait died", cause)
      }
      return logged.pipe(Effect.andThen(Effect.sleep(retryDelayMillis)), Effect.andThen(wait))
    })
  )
  return wait
}
