import type * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import type * as SyncEngine from "../SyncEngine.js"

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
