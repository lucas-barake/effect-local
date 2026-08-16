import type * as Schema from "effect/Schema"
import * as Identity from "./Identity.js"
import * as Defect from "./internal/defect.js"

export type Affinity = "text" | "real" | "integer"
export type Direction = "asc" | "desc"

export interface TextComponentInput<Value, Name extends string = string, T = unknown,> {
  readonly name: Name
  readonly affinity: "text"
  readonly schema: Schema.Codec<T, string>
  readonly extract: (value: Value) => T
}

export interface RealComponentInput<Value, Name extends string = string, T = unknown,> {
  readonly name: Name
  readonly affinity: "real"
  readonly schema: Schema.Codec<T, number>
  readonly extract: (value: Value) => T
}

export interface IntegerComponentInput<Value, Name extends string = string, T = unknown,> {
  readonly name: Name
  readonly affinity: "integer"
  readonly schema: Schema.Codec<T, number | boolean>
  readonly extract: (value: Value) => T
}

export type ComponentInput<Value = any,> =
  | TextComponentInput<Value>
  | RealComponentInput<Value>
  | IntegerComponentInput<Value>

export interface Input<
  Value = any,
  Partition extends ReadonlyArray<ComponentInput<Value>> = ReadonlyArray<ComponentInput<Value>>,
  Sort extends ReadonlyArray<ComponentInput<Value>> = ReadonlyArray<ComponentInput<Value>>,
> {
  readonly version: number
  readonly partition: Partition
  readonly sort: Sort
}

export type Inputs<Value,> = Readonly<Record<string, Input<Value>>>
export type Any = Input
export type AnyRecord = Readonly<Record<string, Any>>

const validName = (kind: string, name: string): void => {
  if (name.length === 0) return Defect.invalid(`${kind} name must be nonempty`)
  if (name.startsWith("$")) return Defect.invalid(`${kind} name must not start with $: ${name}`)
}

export const validate = (name: string, input: Any): void => {
  validName("Index", name)
  Identity.SchemaVersion.make(input.version)
  if (input.partition.length + input.sort.length === 0) {
    return Defect.invalid(`Index ${name} must declare at least one component`)
  }
  const names = new Set<string>()
  for (const component of [...input.partition, ...input.sort]) {
    validName("Index component", component.name)
    if (names.has(component.name)) return Defect.invalid(`Duplicate index component name: ${component.name}`)
    names.add(component.name)
  }
}
