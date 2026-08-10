import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"

export type WireSchema = Schema.Codec<any, typeof Schema.Json.Type, never, never>

export const Void = Schema.Null.pipe(Schema.decodeTo(Schema.Void, {
  decode: SchemaGetter.transform(() => undefined),
  encode: SchemaGetter.transform(() => null)
})).annotate({ identifier: "EffectLocalWireVoid" })

export type Input = WireSchema | Schema.Struct.Fields

export type Normalized<S extends Input,> = S extends Schema.Struct.Fields ? Schema.Struct<S> : S

export type Valid<S extends Input,> = Normalized<S> extends WireSchema ? S : never

export type Wire<S extends Input,> = Extract<Normalized<S>, WireSchema>

export const normalize = <S extends Input,>(input: Valid<S>): Wire<S> =>
  (Schema.isSchema(input) ? input : Schema.Struct(input)) as Wire<S>
