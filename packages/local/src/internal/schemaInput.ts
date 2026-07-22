import * as Schema from "effect/Schema"
import type * as Document from "../Document.js"

export type Input = Document.WireSchema | Schema.Struct.Fields

export type Normalized<S extends Input,> = S extends Schema.Struct.Fields ? Schema.Struct<S> : S

export type Valid<S extends Input,> = Normalized<S> extends Document.WireSchema ? S : never

export type Wire<S extends Input,> = Extract<Normalized<S>, Document.WireSchema>

export const normalize = <S extends Input,>(input: Valid<S>): Wire<S> =>
  (Schema.isSchema(input) ? input : Schema.Struct(input)) as Wire<S>
