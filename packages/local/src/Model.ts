import type * as Schema from "effect/Schema"
import type * as SchemaInput from "./internal/schemaInput.js"

export interface Model<Name extends string, Key extends Schema.Top, Value extends Schema.Top,> {
  readonly name: Name
  readonly key: Key
  readonly schema: Value
}

export interface Any {
  readonly name: string
  readonly key: Schema.Top
  readonly schema: Schema.Top
}

export const make = <
  const Name extends string,
  Key extends SchemaInput.WireSchema,
  Value extends SchemaInput.WireSchema,
>(
  name: Name,
  options: {
    readonly key: Key
    readonly schema: Value
  }
): Model<Name, Key, Value> => {
  if (name.length === 0) throw new TypeError("Model name must be nonempty")
  if (name.startsWith("$")) throw new TypeError(`Model name must not start with $: ${name}`)
  return Object.freeze({ name, ...options })
}

export type Key<M extends Any,> = M["key"]["Type"]
export type Value<M extends Any,> = M["schema"]["Type"]
