import type * as Effect from "effect/Effect"
import type * as Schema from "effect/Schema"
import type * as Stream from "effect/Stream"
import * as Identity from "./Identity.js"
import * as Defect from "./internal/defect.js"
import type * as ReplicaError from "./ReplicaError.js"

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

type ComponentName<C,> = C extends { readonly name: infer Name extends string } ? Name : never
type ComponentValue<C,> = C extends { readonly schema: Schema.Top } ? C["schema"]["Type"] : never
type ComponentsRecord<Components extends ReadonlyArray<ComponentInput>,> = {
  readonly [Component in Components[number] as ComponentName<Component>]: ComponentValue<Component>
}

type First<Components extends ReadonlyArray<ComponentInput>,> = Components extends readonly [
  infer Head extends ComponentInput,
  ...ReadonlyArray<ComponentInput>
] ? Head :
  never

export interface Bounds<A,> {
  readonly gt?: A
  readonly gte?: A
  readonly lt?: A
  readonly lte?: A
}

type RangeRecord<Components extends ReadonlyArray<ComponentInput>,> = [First<Components>] extends [never] ? {}
  : { readonly [Name in ComponentName<First<Components>>]?: Bounds<ComponentValue<First<Components>>> }

export type Where<I extends Any,> = ComponentsRecord<I["partition"]> & RangeRecord<I["sort"]>

export interface Cursor<ModelName extends string, IndexName extends string,> {
  readonly model: ModelName
  readonly index: IndexName
  readonly token: string
}

export interface Page<A, C,> {
  readonly items: ReadonlyArray<A>
  readonly next: C | undefined
}

export interface Builder<ModelName extends string, IndexName extends string, Value, I extends Any,> {
  readonly where: (where: Where<I>) => Builder<ModelName, IndexName, Value, I>
  readonly order: (direction: Direction) => Builder<ModelName, IndexName, Value, I>
  readonly limit: (limit: number) => Builder<ModelName, IndexName, Value, I>
  readonly after: (cursor: Cursor<ModelName, IndexName> | string) => Builder<ModelName, IndexName, Value, I>
  readonly page: () => Effect.Effect<Page<Value, Cursor<ModelName, IndexName>>, ReplicaError.StorageError>
  readonly stream: () => Stream.Stream<Value, ReplicaError.StorageError>
}
