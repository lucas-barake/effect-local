import type * as Schema from "effect/Schema"
import * as SchemaAST from "effect/SchemaAST"

export type Descriptor =
  | string
  | number
  | boolean
  | null
  | ReadonlyArray<Descriptor>
  | { readonly [key: string]: Descriptor }

interface State {
  readonly stack: Map<SchemaAST.AST, number>
  readonly ancestors: WeakSet<object>
}

const fromUnknown = (value: unknown, state: State): Descriptor => {
  if (value === null || value === undefined) return null
  switch (typeof value) {
    case "string":
    case "boolean":
      return value
    case "number":
      return Number.isFinite(value) ? value : String(value)
    case "bigint":
      return { _tag: "BigInt", value: value.toString() }
    case "symbol":
    case "function":
      return String(value)
  }
  if (SchemaAST.isAST(value)) return fromAST(value, state)
  if (value instanceof RegExp) return { _tag: "RegExp", source: value.source, flags: value.flags }
  if (value instanceof Date) return { _tag: "Date", value: value.toISOString() }
  if (state.ancestors.has(value)) return "[Circular]"
  state.ancestors.add(value)
  const result: Descriptor = Array.isArray(value)
    ? value.map((item) => fromUnknown(item, state))
    : Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, fromUnknown(item, state)])
    )
  state.ancestors.delete(value)
  return result
}

const fromAnnotations = (
  annotations: Schema.Annotations.Annotations | undefined,
  state: State
): Descriptor | undefined => {
  if (annotations === undefined) return undefined
  const result: Record<string, Descriptor> = {}
  if (typeof annotations.identifier === "string") result.identifier = annotations.identifier
  if (Array.isArray(annotations.brands)) {
    result.brands = annotations.brands.map((brand) => String(brand))
  }
  if (annotations.meta !== undefined) result.meta = fromUnknown(annotations.meta, state)
  return Object.keys(result).length === 0 ? undefined : result
}

const fromCheck = (check: SchemaAST.Check<any>, state: State): Descriptor => {
  const result: Record<string, Descriptor> = { _tag: check._tag }
  const annotations = fromAnnotations(check.annotations, state)
  if (annotations !== undefined) result.annotations = annotations
  if (check._tag === "FilterGroup") {
    result.checks = check.checks.map((inner) => fromCheck(inner, state))
  }
  return result
}

const fromChecks = (checks: SchemaAST.Checks | undefined, state: State): Descriptor | undefined =>
  checks === undefined ? undefined : checks.map((check) => fromCheck(check, state))

const propertyName = (name: PropertyKey): string => typeof name === "string" ? name : String(name)

const fromAST = (ast: SchemaAST.AST, state: State): Descriptor => {
  const ancestor = state.stack.get(ast)
  if (ancestor !== undefined) return { _tag: "Cycle", back: state.stack.size - ancestor }
  state.stack.set(ast, state.stack.size)
  const node: Record<string, Descriptor> = { _tag: ast._tag }
  const annotations = fromAnnotations(ast.annotations, state)
  if (annotations !== undefined) node.annotations = annotations
  const checks = fromChecks(ast.checks, state)
  if (checks !== undefined) node.checks = checks
  if (ast.encoding !== undefined) {
    node.encoding = ast.encoding.map((link) => ({
      to: fromAST(link.to, state),
      transformation: link.transformation._tag
    }))
  }
  if (ast.context !== undefined) {
    node.context = { isOptional: ast.context.isOptional, isMutable: ast.context.isMutable }
  }
  switch (ast._tag) {
    case "Declaration": {
      node.typeParameters = ast.typeParameters.map((parameter) => fromAST(parameter, state))
      const encodingChecks = fromChecks(ast.encodingChecks, state)
      if (encodingChecks !== undefined) node.encodingChecks = encodingChecks
      break
    }
    case "Literal": {
      node.literal = fromUnknown(ast.literal, state)
      break
    }
    case "UniqueSymbol": {
      node.symbol = String(ast.symbol)
      break
    }
    case "Enum": {
      node.enums = ast.enums.map(([name, value]) => [name, value])
      break
    }
    case "TemplateLiteral": {
      node.parts = ast.parts.map((part) => fromAST(part, state))
      break
    }
    case "Arrays": {
      node.isMutable = ast.isMutable
      node.elements = ast.elements.map((element) => fromAST(element, state))
      node.rest = ast.rest.map((element) => fromAST(element, state))
      const encodingChecks = fromChecks(ast.encodingChecks, state)
      if (encodingChecks !== undefined) node.encodingChecks = encodingChecks
      break
    }
    case "Objects": {
      node.propertySignatures = Object.fromEntries(
        ast.propertySignatures.map((property) => [
          propertyName(property.name),
          fromAST(property.type, state)
        ])
      )
      if (ast.indexSignatures.length > 0) {
        node.indexSignatures = ast.indexSignatures.map((signature) => ({
          parameter: fromAST(signature.parameter, state),
          type: fromAST(signature.type, state),
          merge: signature.merge !== undefined
        }))
      }
      const encodingChecks = fromChecks(ast.encodingChecks, state)
      if (encodingChecks !== undefined) node.encodingChecks = encodingChecks
      break
    }
    case "Union": {
      node.mode = ast.mode
      node.types = ast.types.map((member) => fromAST(member, state))
      const encodingChecks = fromChecks(ast.encodingChecks, state)
      if (encodingChecks !== undefined) node.encodingChecks = encodingChecks
      break
    }
    case "Suspend": {
      node.suspended = fromAST(ast.thunk(), state)
      break
    }
  }
  state.stack.delete(ast)
  return node
}

export const make = (schema: Schema.Constraint): Descriptor =>
  fromAST(schema.ast, { stack: new Map(), ancestors: new WeakSet() })
