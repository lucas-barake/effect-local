import * as Schema from "effect/Schema"
import * as SchemaAST from "effect/SchemaAST"
import * as SchemaGetter from "effect/SchemaGetter"
import * as Defect from "./defect.js"

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

const acceptsSymbolKeys = (parameter: SchemaAST.AST): boolean => {
  if (parameter._tag === "Symbol" || parameter._tag === "UniqueSymbol") return true
  if (parameter._tag === "Union") return parameter.types.some(acceptsSymbolKeys)
  return false
}

const rejectSymbolKeys = (ast: SchemaAST.AST, visited: Set<SchemaAST.AST>): void => {
  if (visited.has(ast)) return
  visited.add(ast)
  switch (ast._tag) {
    case "Objects":
      for (const property of ast.propertySignatures) {
        if (typeof property.name === "symbol") {
          return Defect.invalid(`Wire schemas cannot declare symbol keyed fields: ${String(property.name)}`)
        }
        rejectSymbolKeys(property.type, visited)
      }
      for (const signature of ast.indexSignatures) {
        if (acceptsSymbolKeys(signature.parameter)) {
          return Defect.invalid("Wire schemas cannot declare symbol keyed records")
        }
        rejectSymbolKeys(signature.type, visited)
      }
      return
    case "Arrays":
      for (const element of [...ast.elements, ...ast.rest]) rejectSymbolKeys(element, visited)
      return
    case "Union":
      for (const member of ast.types) rejectSymbolKeys(member, visited)
      return
    case "Declaration":
      for (const parameter of ast.typeParameters) rejectSymbolKeys(parameter, visited)
      return
    case "Suspend":
      return rejectSymbolKeys(ast.thunk(), visited)
  }
}

export const requireStringKeys = (schema: Schema.Top): void =>
  rejectSymbolKeys(SchemaAST.toEncoded(schema.ast), new Set())

export function normalize<S extends Input,>(input: Valid<S>): Wire<S>
export function normalize(input: Input): WireSchema {
  let Normalized: WireSchema
  if (Schema.isSchema(input)) Normalized = input
  else Normalized = Schema.Struct(input)
  requireStringKeys(Normalized)
  return Normalized
}
