import * as Schema from "effect/Schema"
import * as Defect from "./internal/defect.js"
import * as SchemaInput from "./internal/schemaInput.js"
import * as Protocol from "./Protocol.js"

export class EncodeError extends Schema.TaggedErrorClass<EncodeError>(
  "@lucas-barake/effect-local/EphemeralEncodeError"
)("EphemeralEncodeError", { definition: Schema.String, cause: Schema.Defect() }) {}

export class DecodeError extends Schema.TaggedErrorClass<DecodeError>(
  "@lucas-barake/effect-local/EphemeralDecodeError"
)("EphemeralDecodeError", { definition: Schema.String, cause: Schema.Defect() }) {}

export type KeySchema = Schema.Codec<any, string>

export interface Event<Name extends string, P extends Schema.Top,> {
  readonly kind: "event"
  readonly name: Name
  readonly payloadSchema: P
}

export interface State<Name extends string, K extends KeySchema, P extends Schema.Top,> {
  readonly kind: "state"
  readonly name: Name
  readonly keySchema: K
  readonly payloadSchema: P
}

export interface AnyEvent {
  readonly kind: "event"
  readonly name: string
  readonly payloadSchema: SchemaInput.WireSchema
}

export interface AnyState {
  readonly kind: "state"
  readonly name: string
  readonly keySchema: KeySchema
  readonly payloadSchema: SchemaInput.WireSchema
}

export type Any = AnyEvent | AnyState

export function make<
  const Name extends string,
  P extends SchemaInput.Input = typeof SchemaInput.Void,
>(name: Name, options: {
  readonly kind: "event"
  readonly payload?: SchemaInput.Valid<P>
}): Event<Name, SchemaInput.Wire<P>>
export function make<
  const Name extends string,
  K extends KeySchema,
  P extends SchemaInput.Input = typeof SchemaInput.Void,
>(name: Name, options: {
  readonly kind: "state"
  readonly key: K
  readonly payload?: SchemaInput.Valid<P>
}): State<Name, K, SchemaInput.Wire<P>>
export function make(name: string, options: {
  readonly kind: "event" | "state"
  readonly key?: KeySchema
  readonly payload?: SchemaInput.Input
}): Any {
  if (name.length === 0) return Defect.invalid("Ephemeral name must be nonempty")
  if (name.startsWith("$")) return Defect.invalid(`Ephemeral name must not start with $: ${name}`)
  if (name.length > Protocol.maximumEphemeralChannelLength) {
    return Defect.invalid(
      `Ephemeral name must be at most ${Protocol.maximumEphemeralChannelLength} characters: ${name}`
    )
  }
  let payloadSchema: SchemaInput.WireSchema = SchemaInput.Void
  if (options.payload !== undefined) payloadSchema = SchemaInput.normalize(options.payload)
  if (options.kind === "state") {
    if (options.key === undefined) return Defect.invalid(`Ephemeral state definition requires a key codec: ${name}`)
    return { kind: "state", name, keySchema: options.key, payloadSchema }
  }
  return { kind: "event", name, payloadSchema }
}

export interface Member<P extends Schema.Top,> {
  readonly kind: "member"
  readonly payloadSchema: P
}

export interface AnyMember {
  readonly kind: "member"
  readonly payloadSchema: SchemaInput.WireSchema
}

export function member<P extends SchemaInput.Input = typeof SchemaInput.Void,>(
  payload?: SchemaInput.Valid<P>
): Member<SchemaInput.Wire<P>>
export function member(payload?: SchemaInput.Input): AnyMember {
  let payloadSchema: SchemaInput.WireSchema = SchemaInput.Void
  if (payload !== undefined) payloadSchema = SchemaInput.normalize(payload)
  return { kind: "member", payloadSchema }
}

export interface Group<Definitions extends ReadonlyArray<Any>,> {
  readonly definitions: Definitions
  readonly byName: ReadonlyMap<string, Any>
}

export const group = <const Definitions extends ReadonlyArray<Any>,>(
  definitions: Definitions
): Group<Definitions> => {
  const byName = new Map<string, Any>()
  for (const definition of definitions) {
    if (byName.has(definition.name)) {
      return Defect.invalid(`Duplicate ephemeral name: ${definition.name}`)
    }
    byName.set(definition.name, definition)
  }
  return Object.freeze({ definitions, byName })
}

export type Payload<D extends Any | AnyMember,> = D["payloadSchema"]["Type"]
export type Key<D extends AnyState,> = D["keySchema"]["Type"]
