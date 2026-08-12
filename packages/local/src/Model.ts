import type * as Schema from "effect/Schema"
import * as Identity from "./Identity.js"
import * as Defect from "./internal/defect.js"
import type * as SchemaInput from "./internal/schemaInput.js"
import * as SecondaryIndex from "./SecondaryIndex.js"

export interface Model<
  Name extends string,
  KeySchema extends Schema.Top,
  ValueSchema extends Schema.Top,
  ModelIndexes extends SecondaryIndex.AnyRecord = {},
> {
  readonly name: Name
  readonly version: Identity.SchemaVersion
  readonly key: KeySchema
  readonly schema: ValueSchema
  readonly indexes: ModelIndexes
}

export interface Any {
  readonly name: string
  readonly version: Identity.SchemaVersion
  readonly key: SchemaInput.WireSchema
  readonly schema: SchemaInput.WireSchema
  readonly indexes: SecondaryIndex.AnyRecord
}

export function make<
  const Name extends string,
  KeySchema extends SchemaInput.WireSchema,
  ValueSchema extends SchemaInput.WireSchema,
  const ModelIndexes extends SecondaryIndex.Inputs<ValueSchema["Type"]>,
>(
  name: Name,
  options: {
    readonly version: number
    readonly key: KeySchema
    readonly schema: ValueSchema
    readonly indexes: ModelIndexes
  }
): Model<Name, KeySchema, ValueSchema, ModelIndexes>
export function make<
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
): Model<Name, KeySchema, ValueSchema>
export function make(
  name: string,
  options: {
    readonly version: number
    readonly key: SchemaInput.WireSchema
    readonly schema: SchemaInput.WireSchema
    readonly indexes?: SecondaryIndex.AnyRecord
  }
): Any {
  if (name.length === 0) return Defect.invalid("Model name must be nonempty")
  if (name.startsWith("$")) return Defect.invalid(`Model name must not start with $: ${name}`)
  const indexes = options.indexes ?? {}
  for (const [indexName, index] of Object.entries(indexes)) {
    SecondaryIndex.validate(indexName, index)
    for (const component of [...index.partition, ...index.sort]) Object.freeze(component)
    Object.freeze(index.partition)
    Object.freeze(index.sort)
    Object.freeze(index)
  }
  return Object.freeze({
    name,
    version: Identity.SchemaVersion.make(options.version),
    key: options.key,
    schema: options.schema,
    indexes: Object.freeze(indexes)
  })
}

export type Key<M extends Any,> = M["key"]["Type"]
export type Value<M extends Any,> = M["schema"]["Type"]
export type Indexes<M extends Any,> = M["indexes"]
