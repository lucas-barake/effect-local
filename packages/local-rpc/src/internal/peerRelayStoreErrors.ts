import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import type * as PlatformError from "effect/PlatformError"
import type * as Schema from "effect/Schema"
import type * as SqlError from "effect/unstable/sql/SqlError"
import type { NestedPeerRelayTransactionError } from "./peerRelaySqliteTransaction.js"

export type StoreBoundaryError =
  | ReplicaError.ReplicaError
  | NestedPeerRelayTransactionError
  | SqlError.SqlError
  | Schema.SchemaError
  | PlatformError.PlatformError
  | Cause.NoSuchElementError

const storageUnavailable = (cause: unknown) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.StorageUnavailable({ cause })
  })

const storageCorrupt = (cause: unknown) =>
  new ReplicaError.ReplicaError({
    reason: new ReplicaError.StorageCorrupt({ cause })
  })

export const mapStoreErrors = <A, R,>(
  effect: Effect.Effect<A, StoreBoundaryError, R>
) =>
  Effect.catchCause(
    effect,
    (cause) =>
      Effect.failCause(Cause.map(cause, (error) => {
        switch (error._tag) {
          case "SqlError":
          case "PlatformError":
            return storageUnavailable(error)
          case "SchemaError":
          case "NoSuchElementError":
            return storageCorrupt(error)
          default:
            return error
        }
      }))
  )
