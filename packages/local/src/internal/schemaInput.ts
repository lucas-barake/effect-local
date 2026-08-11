import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"

export type WireSchema = Schema.Codec<any, typeof Schema.Json.Type>

export interface Fields {
  readonly [key: string]: WireSchema
  readonly [key: symbol]: WireSchema
}

export const Void = Schema.Null.pipe(Schema.decodeTo(Schema.Void, {
  decode: SchemaGetter.transform(() => undefined),
  encode: SchemaGetter.transform(() => null)
})).annotate({ identifier: "EffectLocalWireVoid" })

export type Input = WireSchema | Fields

export type Normalized<S extends Input,> = S extends Fields ? Schema.Struct<S> : S

export type Valid<S extends Input,> = Normalized<S> extends WireSchema ? S : never

export type Wire<S extends Input,> = Extract<Normalized<S>, WireSchema>

export function normalize<S extends Input,>(input: Valid<S>): Wire<S>
export function normalize(input: Input): WireSchema {
  if (Schema.isSchema(input)) return input
  return Schema.Struct(input)
}
