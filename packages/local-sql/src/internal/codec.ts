import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

export const decode = <S extends Schema.Top,>(
  schema: S,
  value: unknown
): Effect.Effect<S["Type"], ReplicaError.StorageCorrupt> =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError((cause) =>
      new ReplicaError.StorageCorrupt({ message: "Stored value failed Schema decoding", cause })
    )
  ) as Effect.Effect<S["Type"], ReplicaError.StorageCorrupt>

export const encode = <S extends Schema.Top,>(
  schema: S,
  value: S["Type"]
): Effect.Effect<S["Encoded"], ReplicaError.StorageCorrupt> =>
  Schema.encodeEffect(schema)(value).pipe(
    Effect.mapError((cause) => new ReplicaError.StorageCorrupt({ message: "Value failed Schema encoding", cause }))
  ) as Effect.Effect<S["Encoded"], ReplicaError.StorageCorrupt>

export const parse = (value: string): Effect.Effect<unknown, ReplicaError.StorageCorrupt> =>
  Effect.try({
    try: () => JSON.parse(value) as unknown,
    catch: (cause) => new ReplicaError.StorageCorrupt({ message: "Stored JSON is invalid", cause })
  })

export const stringify = (value: unknown): Effect.Effect<string, ReplicaError.StorageCorrupt> =>
  Effect.try({
    try: () => JSON.stringify(value),
    catch: (cause) => new ReplicaError.StorageCorrupt({ message: "Value cannot be encoded as JSON", cause })
  })
