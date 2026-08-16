import * as ReplicaError from "@lucas-barake/effect-local/ReplicaError"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

export type Json = typeof Schema.Json.Type

export const encodeJson = (schema: Schema.Top, value: unknown): Effect.Effect<Json, ReplicaError.StorageCorrupt> =>
  // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion, effect-local/noUnknownEffectChannels -- Every wire definition schema encodes to Json without services; the generic Schema.Top erases both facts.
  Schema.encodeUnknownEffect(schema)(value).pipe(
    Effect.catchTag("SchemaError", (error) =>
      Effect.fail(new ReplicaError.StorageCorrupt({ message: "multi-tab wire codec failure", cause: error })))
  ) as Effect.Effect<Json, ReplicaError.StorageCorrupt>

export const decodeWith = (schema: Schema.Top, value: unknown): Effect.Effect<unknown, ReplicaError.StorageCorrupt> =>
  // oxlint-disable-next-line effect/noAs, typescript/no-unsafe-type-assertion, effect-local/noUnknownEffectChannels -- Every wire definition schema decodes without services; the generic Schema.Top erases that.
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.catchTag("SchemaError", (error) =>
      Effect.fail(new ReplicaError.StorageCorrupt({ message: "multi-tab wire codec failure", cause: error })))
  ) as Effect.Effect<unknown, ReplicaError.StorageCorrupt>
