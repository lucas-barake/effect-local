import type * as Schema from "effect/Schema"
import * as Identity from "./Identity.js"
import * as Defect from "./internal/defect.js"
import type * as SchemaInput from "./internal/schemaInput.js"

export interface Model<Name extends string, KeySchema extends Schema.Top, ValueSchema extends Schema.Top,> {
  readonly name: Name
  readonly version: Identity.SchemaVersion
  readonly key: KeySchema
  readonly schema: ValueSchema
}

export interface Any {
  readonly name: string
  readonly version: Identity.SchemaVersion
  readonly key: SchemaInput.WireSchema
  readonly schema: SchemaInput.WireSchema
}

export const make = <
  const Name extends string,
  KeySchema extends SchemaInput.WireSchema,
  ValueSchema extends SchemaInput.WireSchema,
>(
  name: Name,
  options: {
    readonly version: number
    readonly key: KeySchema
    readonly schema: ValueSchema
  }
): Model<Name, KeySchema, ValueSchema> => {
  if (name.length === 0) return Defect.invalid("Model name must be nonempty")
  if (name.startsWith("$")) return Defect.invalid(`Model name must not start with $: ${name}`)
  return Object.freeze({ name, ...options, version: Identity.SchemaVersion.make(options.version) })
}

export type Key<M extends Any,> = M["key"]["Type"]
export type Value<M extends Any,> = M["schema"]["Type"]
