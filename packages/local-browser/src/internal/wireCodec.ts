import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

export type Json = typeof Schema.Json.Type

export type WireCodec = Schema.Top & Schema.ConstraintCodec<unknown, Json>

export const encodeJson = (
  schema: WireCodec,
  value: unknown
): Effect.Effect<Json, ReplicaError.StorageCorrupt> =>
  Schema.encodeUnknownEffect(schema)(value).pipe(
    Effect.catchTag("SchemaError", (error) =>
      Effect.fail(new ReplicaError.StorageCorrupt({ message: "multi-tab wire codec failure", cause: error })))
  )

export const decodeWith = <S extends WireCodec,>(
  schema: S,
  value: unknown
): Effect.Effect<S["Type"], ReplicaError.StorageCorrupt> =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.catchTag("SchemaError", (error) =>
      Effect.fail(new ReplicaError.StorageCorrupt({ message: "multi-tab wire codec failure", cause: error })))
  )
